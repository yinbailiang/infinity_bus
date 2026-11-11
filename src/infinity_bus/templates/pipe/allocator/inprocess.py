"""进程内管道实现：共享双向通道上的一对端点，以及对应分配器。"""

import asyncio
import uuid
from typing import Any, Literal

from pydantic import BaseModel

from ..base import Pipe, PipeClosedError
from .base import PipeAllocator


def _make_queue(maxsize: int | None) -> asyncio.Queue[BaseModel]:
    """按 maxsize 创建队列，``None`` 表示无限容量。"""
    return asyncio.Queue() if maxsize is None else asyncio.Queue(maxsize=maxsize)


class InProcessPipe(Pipe):
    """进程内管道端点：写入的数据由**对端**读走，两个方向各自独立缓冲。

    两种构造方式：

    - ``InProcessPipe.create_pair(maxsize=...)``：创建共享同一双向通道的一对端点，
      ``InProcessPipeAllocator`` 使用这种方式；
    - ``InProcessPipe(maxsize=...)``：自环端点，写入的数据由自身 ``receive()`` 读回，
      仅适合单端测试，不表达两端通信语义。
    """

    def __init__(self, maxsize: int | None = None) -> None:
        queue = _make_queue(maxsize)
        closed = asyncio.Event()
        # 自环：入站与出站是同一个队列，关闭标记也只有一份
        self._outbound: asyncio.Queue[BaseModel] = queue
        self._inbound: asyncio.Queue[BaseModel] = queue
        self._closed = closed
        self._peer_closed = closed

    @classmethod
    def create_pair(cls, maxsize: int | None = None) -> tuple['InProcessPipe', 'InProcessPipe']:
        """创建共享同一双向通道的一对端点（方向相反、缓冲独立）。"""
        to_first = _make_queue(maxsize)
        to_second = _make_queue(maxsize)
        first_closed = asyncio.Event()
        second_closed = asyncio.Event()

        first = cls._build(to_second, to_first, first_closed, second_closed)
        second = cls._build(to_first, to_second, second_closed, first_closed)
        return first, second

    @classmethod
    def _build(
        cls,
        outbound: asyncio.Queue[BaseModel],
        inbound: asyncio.Queue[BaseModel],
        closed: asyncio.Event,
        peer_closed: asyncio.Event,
    ) -> 'InProcessPipe':
        """绕过 ``__init__`` 组装端点，使两个端点共享通道与关闭信号。"""
        pipe: InProcessPipe = cls.__new__(cls)
        pipe._outbound = outbound
        pipe._inbound = inbound
        pipe._closed = closed
        pipe._peer_closed = peer_closed
        return pipe

    async def send(self, data: BaseModel) -> None:
        """写入对端缓冲区；本端或对端已关闭时抛出 PipeClosedError。"""
        if self._closed.is_set() or self._peer_closed.is_set():
            raise PipeClosedError('Pipe is closed')
        await self._outbound.put(data)

    async def receive(self) -> BaseModel:
        """从入站缓冲区读取；对端已关闭且缓冲区为空时抛出 PipeClosedError。"""
        get_task: asyncio.Task[BaseModel] = asyncio.create_task(self._inbound.get())
        wait_task: asyncio.Task[Literal[True]] = asyncio.create_task(self._peer_closed.wait())
        done, _ = await asyncio.wait([get_task, wait_task], return_when=asyncio.FIRST_COMPLETED)

        if get_task in done:
            wait_task.cancel()
            try:
                await wait_task
            except asyncio.CancelledError:
                pass
            data: BaseModel = get_task.result()
            self._inbound.task_done()
            return data

        get_task.cancel()
        try:
            await get_task
        except asyncio.CancelledError:
            pass
        raise PipeClosedError('Pipe is closed')

    async def open(self) -> None:
        """打开端点。若本端曾关闭，重置关闭标记并清空残留的入站数据。"""
        if self._closed.is_set():
            self._closed.clear()
            while not self._inbound.empty():
                try:
                    self._inbound.get_nowait()
                    self._inbound.task_done()
                except asyncio.QueueEmpty:
                    break

    async def close(self) -> None:
        """关闭本端：通知对端「不会再有新数据」，并禁止后续写入。"""
        if not self._closed.is_set():
            self._closed.set()


class InProcessPipeAllocator(PipeAllocator):
    """进程内管道分配器：``allocate()`` 创建共享同一双向通道的端点对。

    分配方端点由 ``allocate()`` 直接返回；借用方端点通过 ``checkout()`` 领取、
    ``checkin()`` 归还；``release()`` 关闭并注销整条管道。
    """

    def __init__(
        self,
        pipe_type: type[InProcessPipe] = InProcessPipe,
    ) -> None:
        self._local_pipes: dict[str, InProcessPipe] = {}
        self._peer_pipes: dict[str, InProcessPipe] = {}
        self._pipe_type = pipe_type

    async def allocate(
        self,
        **kwargs: Any,
    ) -> tuple[str, Pipe]:
        """创建一对端点，返回 ``(pipe_id, 分配方本端端点)``。"""
        pipe_id: str = uuid.uuid4().hex
        if pipe_id in self._local_pipes:
            raise ValueError(f'Pipe with id {pipe_id} already exists')

        local_pipe, peer_pipe = self._pipe_type.create_pair(**kwargs)
        self._local_pipes[pipe_id] = local_pipe
        self._peer_pipes[pipe_id] = peer_pipe
        return pipe_id, local_pipe

    async def release(self, pipe_id: str) -> None:
        """关闭两端端点并注销；重复调用无副作用。"""
        local_pipe = self._local_pipes.pop(pipe_id, None)
        peer_pipe = self._peer_pipes.pop(pipe_id, None)
        for pipe in (local_pipe, peer_pipe):
            if pipe is not None:
                await pipe.close()

    async def checkout(self, pipe_id: str) -> Pipe | None:
        """返回借用方（服务端）端点，不存在时返回 None。"""
        return self._peer_pipes.get(pipe_id)

    async def checkin(self, pipe_id: str) -> None:
        """归还借用方端点；不销毁分配方资源，重复调用无副作用。"""
        peer_pipe = self._peer_pipes.pop(pipe_id, None)
        if peer_pipe is not None:
            await peer_pipe.close()
