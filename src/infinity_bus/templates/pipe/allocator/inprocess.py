"""进程内管道实现：共享双向通道上的一对端点，以及对应分配器。"""

import asyncio
import uuid
from typing import Any

from pydantic import BaseModel

from ..base import Pipe, PipeClosedError, cancel_tasks
from .base import PipeAllocator


class InProcessPipe[PayloadType: BaseModel](Pipe[PayloadType]):
    """进程内管道端点：写入的数据由**对端**读走，两个方向各自独立缓冲。"""

    def __init__(
        self,
        in_queue: asyncio.Queue[PayloadType],
        out_queue: asyncio.Queue[PayloadType],
        in_closed: asyncio.Event,
        out_closed: asyncio.Event,
    ) -> None:
        self._inbound: asyncio.Queue[PayloadType] = in_queue
        self._outbound: asyncio.Queue[PayloadType] = out_queue
        self._inbound_closed: asyncio.Event = in_closed
        self._outbound_closed: asyncio.Event = out_closed

    async def send(self, data: PayloadType) -> None:
        """写入对端缓冲区；本端或对端已关闭时抛出 PipeClosedError。"""
        if self._inbound_closed.is_set() or self._outbound_closed.is_set():
            raise PipeClosedError('Pipe is closed')

        put_task: asyncio.Task[None] = asyncio.create_task(self._outbound.put(data))
        in_closed_task: asyncio.Task[bool] = asyncio.create_task(self._inbound_closed.wait())
        out_closed_task: asyncio.Task[bool] = asyncio.create_task(self._outbound_closed.wait())
        tasks: tuple[asyncio.Task[Any], ...] = (put_task, in_closed_task, out_closed_task)
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            await cancel_tasks(tasks)
            raise

        await cancel_tasks(pending)
        if put_task in done:
            return
        raise PipeClosedError('Pipe is closed')

    async def receive(self) -> PayloadType:
        """从入站缓冲区读取；本端或对端已关闭且缓冲区耗尽时抛出 PipeClosedError。"""
        while True:
            try:
                data: PayloadType = self._inbound.get_nowait()
            except asyncio.QueueEmpty:
                pass
            else:
                self._inbound.task_done()
                return data

            if self._inbound_closed.is_set() or self._outbound_closed.is_set():
                raise PipeClosedError('Pipe is closed')

            get_task: asyncio.Task[PayloadType] = asyncio.create_task(self._inbound.get())
            in_closed_task: asyncio.Task[bool] = asyncio.create_task(self._inbound_closed.wait())
            out_closed_task: asyncio.Task[bool] = asyncio.create_task(self._outbound_closed.wait())
            tasks: tuple[asyncio.Task[Any], ...] = (get_task, in_closed_task, out_closed_task)
            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            except BaseException:
                await cancel_tasks(tasks)
                raise

            await cancel_tasks(pending)

            if get_task in done:
                data = get_task.result()
                self._inbound.task_done()
                return data
            # 关闭事件先到：回到循环顶部，先排空缓冲区再抛错。

    async def open(self) -> None:
        """重置本端关闭标记，并清空残留的入站数据。"""
        if self._outbound_closed.is_set():
            self._outbound_closed.clear()
            while True:
                try:
                    self._inbound.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._inbound.task_done()

    async def close(self) -> None:
        """关闭本端：通知对端「不会再有新数据」，并禁止后续读写。"""
        self._outbound_closed.set()


def create_pair[PayloadType: BaseModel](
    pipe_type: type[InProcessPipe[PayloadType]],
    maxsize: int | None = None,
) -> tuple[InProcessPipe[PayloadType], InProcessPipe[PayloadType]]:
    """创建共享同一双向通道的一对端点（方向相反、缓冲独立）。"""
    to_first: asyncio.Queue[PayloadType] = asyncio.Queue(maxsize=maxsize or 0)
    to_second: asyncio.Queue[PayloadType] = asyncio.Queue(maxsize=maxsize or 0)
    first_closed = asyncio.Event()
    second_closed = asyncio.Event()

    first = pipe_type(to_first, to_second, second_closed, first_closed)
    second = pipe_type(to_second, to_first, first_closed, second_closed)
    return first, second


class InProcessPipeAllocator[PayloadType: BaseModel = BaseModel](PipeAllocator[PayloadType]):
    """进程内管道分配器：``allocate()`` 创建共享同一双向通道的端点对。

    分配方端点由 ``allocate()`` 直接返回；借用方端点通过 ``checkout()`` 领取、
    ``checkin()`` 归还；``release()`` 关闭并注销整条管道。
    """

    def __init__(
        self,
        pipe_type: type[InProcessPipe[PayloadType]] = InProcessPipe,
    ) -> None:
        self._local_pipes: dict[str, InProcessPipe[PayloadType]] = {}
        self._peer_pipes: dict[str, InProcessPipe[PayloadType]] = {}
        self._active: dict[str, InProcessPipe[PayloadType]] = {}
        self._pipe_type: type[InProcessPipe[PayloadType]] = pipe_type

    async def allocate(
        self,
        **kwargs: Any,
    ) -> tuple[str, Pipe[PayloadType]]:
        """创建一对端点，返回 ``(pipe_id, 分配方本端端点)``。"""
        pipe_id: str = uuid.uuid4().hex
        if pipe_id in self._local_pipes:
            raise ValueError(f'Pipe with id {pipe_id} already exists')

        local_pipe, peer_pipe = create_pair(self._pipe_type, **kwargs)
        self._local_pipes[pipe_id] = local_pipe
        self._peer_pipes[pipe_id] = peer_pipe
        return pipe_id, local_pipe

    async def release(self, pipe_id: str) -> None:
        """关闭两端端点并注销；重复调用无副作用。"""
        local_pipe = self._local_pipes.pop(pipe_id, None)
        peer_pipe = self._peer_pipes.pop(pipe_id, None)
        active_pipe = self._active.pop(pipe_id, None)
        for pipe in (local_pipe, peer_pipe, active_pipe):
            if pipe is not None:
                await pipe.close()

    async def checkout(self, pipe_id: str) -> Pipe[PayloadType] | None:
        """返回借用方（服务端）端点；已领取或不存在时返回 None。"""
        pipe = self._peer_pipes.pop(pipe_id, None)
        if pipe is None:
            return None
        self._active[pipe_id] = pipe
        return pipe

    async def checkin(self, pipe_id: str) -> None:
        """归还借用方端点；不销毁分配方资源，重复调用无副作用。"""
        peer_pipe = self._active.pop(pipe_id, None)
        if peer_pipe is None:
            peer_pipe = self._peer_pipes.pop(pipe_id, None)
        if peer_pipe is not None:
            await peer_pipe.close()
