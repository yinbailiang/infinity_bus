"""进程间管道：基于 ``multiprocessing.connection`` 的 Listener / Client。

控制面仍走事件总线，数据面由 ``multiprocessing.connection`` 建立：

- 客户端 ``allocate()`` 直连 ``Listener`` 地址并先发一帧 ``pipe_id`` 作为 hello；
- 服务端 accept 线程读取 hello，按 ``pipe_id`` 寄存连接；
- ``expect_pipe`` 通过 ``checkout(pipe_id)`` 领取该连接，``checkin()`` 归还。

``Connection`` 是阻塞 API，且 ``close()`` 不会唤醒其他线程中阻塞的 ``read``
（只是 ``os.close(fd)``，还有 fd 复用的风险）。因此接收侧不用读线程，而是在事件
循环里用 ``poll(0)`` + 短间隔轮询；发送侧仍在 ``asyncio.to_thread`` 中执行。
每条等待中的管道每 ``POLL_INTERVAL`` 秒唤醒一次，适合少量严格 P2P 连接。
"""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from multiprocessing import AuthenticationError
from multiprocessing.connection import Client, Connection, Listener
from typing import Any

from pydantic import BaseModel

from ..base import Pipe, PipeClosedError, PipeHandshakeError
from ..codec import decode_payload, encode_payload
from .base import SingleRoleAllocator

logger = logging.getLogger(__name__)

POLL_INTERVAL = 0.01
"""接收轮询间隔（秒）：等待中的 ``receive()`` 每 10ms 检查一次连接可读性。"""

_STOP_SENTINEL = b'__infinity_bus_pipe_stop__'
"""停止 accept 线程的哨兵 hello（``Listener.close()`` 无法唤醒阻塞中的 ``accept()``）。"""


class ProcessPipe[PayloadType: BaseModel](Pipe[PayloadType]):
    """基于 ``multiprocessing.connection.Connection`` 的管道端点。

    读写两端都按构建时传入的 ``payload_type`` 做 JSON 编解码；对端关闭（EOF）后，
    ``receive()`` 抛 ``PipeClosedError``。
    """

    def __init__(self, conn: Connection, payload_type: type[PayloadType]) -> None:
        self._conn = conn
        self._payload_type = payload_type
        self._closed = asyncio.Event()

    async def open(self) -> None:
        """打开端点（连接已由分配器建立，这里仅重置关闭标记）。"""
        self._closed.clear()

    async def send(self, data: PayloadType) -> None:
        """写入一条负载；已关闭或对端断开时抛 ``PipeClosedError``。"""
        if self._closed.is_set():
            raise PipeClosedError('Pipe is closed')
        try:
            await asyncio.to_thread(self._conn.send_bytes, encode_payload(data))
        except (BrokenPipeError, OSError, ValueError) as e:
            self._closed.set()
            raise PipeClosedError('Pipe is closed') from e

    async def receive(self) -> PayloadType:
        """读取一条负载；对端关闭时抛 ``PipeClosedError``。"""
        while True:
            if self._closed.is_set():
                raise PipeClosedError('Pipe is closed')
            try:
                readable = self._conn.poll(0)
            except (OSError, ValueError) as e:
                self._closed.set()
                raise PipeClosedError('Pipe is closed') from e
            if readable:
                try:
                    raw = self._conn.recv_bytes()
                except (EOFError, OSError, ValueError) as e:
                    self._closed.set()
                    raise PipeClosedError('Pipe is closed') from e
                return decode_payload(raw, self._payload_type)
            await asyncio.sleep(POLL_INTERVAL)

    async def close(self) -> None:
        """关闭底层连接；重复调用无副作用。"""
        self._closed.set()
        try:
            self._conn.close()
        except OSError, ValueError:  # pragma: no cover - 已失效的连接
            pass


class ProcessPipeServerAllocator[PayloadType: BaseModel](SingleRoleAllocator[PayloadType]):
    """服务端分配器：持有 ``Listener``，按首帧 ``pipe_id`` 配对连接。"""

    def __init__(
        self,
        address: str | tuple[str, int],
        payload_type: type[PayloadType],
        *,
        authkey: bytes | None = None,
        accept_timeout: float = 5.0,
        hello_timeout: float = 2.0,
        max_pending: int = 64,
    ) -> None:
        self._address = address
        self._payload_type = payload_type
        self._authkey = authkey
        self._accept_timeout = accept_timeout
        self._hello_timeout = hello_timeout
        self._max_pending = max_pending
        self._listener: Listener | None = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stopping = threading.Event()
        self._pending: dict[str, ProcessPipe[PayloadType]] = {}
        self._active: dict[str, ProcessPipe[PayloadType]] = {}
        self._waiters: dict[str, asyncio.Future[ProcessPipe[PayloadType]]] = {}

    @property
    def address(self) -> str | tuple[str, int]:
        """实际监听地址（端口 0 时为系统分配的端口）。"""
        return self._listener.address if self._listener is not None else self._address

    async def start(self) -> None:
        """创建 ``Listener`` 并启动 accept 线程（重复调用无副作用）。"""
        if self._listener is not None:
            return
        self._loop = asyncio.get_running_loop()
        self._stopping.clear()
        self._listener = Listener(self._address, authkey=self._authkey)
        self._thread = threading.Thread(target=self._accept_loop, name='infinity-bus-process-pipe', daemon=True)
        self._thread.start()

    def _accept_loop(self) -> None:
        """accept 线程：读取 hello 后把 ``(pipe_id, pipe)`` 交回事件循环。"""
        listener = self._listener
        loop = self._loop
        if listener is None or loop is None:  # pragma: no cover - start() 保证
            return

        while not self._stopping.is_set():
            try:
                conn = listener.accept()
            except AuthenticationError:
                # 单个客户端鉴权失败不应终结监听器
                logger.warning('进程管道拒绝了鉴权失败的连接')
                continue
            except OSError, EOFError:
                break

            try:
                self._handle_hello(conn, loop)
            except Exception:  # 单条坏连接不能终结 accept 线程
                logger.exception('进程管道处理 hello 失败')
                conn.close()

    def _handle_hello(self, conn: Connection, loop: asyncio.AbstractEventLoop) -> None:
        """读取并校验首帧；超时、非法帧或 EOF 时关闭连接。"""
        try:
            if not conn.poll(self._hello_timeout):
                logger.warning('进程管道 hello 超时，关闭连接')
                conn.close()
                return
            hello = conn.recv_bytes()
        except EOFError, OSError, ValueError:
            conn.close()
            return

        if hello == _STOP_SENTINEL:
            conn.close()
            self._stopping.set()
            return

        try:
            pipe_id = hello.decode('utf-8')
        except UnicodeDecodeError:
            logger.warning('进程管道收到非法 UTF-8 hello，关闭连接')
            conn.close()
            return

        pipe = ProcessPipe(conn, self._payload_type)
        loop.call_soon_threadsafe(self._register, pipe_id, pipe)

    def _register(self, pipe_id: str, pipe: ProcessPipe[PayloadType]) -> None:
        """在事件循环线程内登记已配对的连接。"""
        waiter = self._waiters.pop(pipe_id, None)
        if waiter is not None and not waiter.done():
            waiter.set_result(pipe)
            return
        if waiter is not None:
            # checkout 已超时或取消：关闭连接，避免无人认领的端点泄漏
            asyncio.get_running_loop().create_task(pipe.close())
            return
        if len(self._pending) >= self._max_pending:
            oldest_id, oldest_pipe = next(iter(self._pending.items()))
            self._pending.pop(oldest_id)
            asyncio.get_running_loop().create_task(oldest_pipe.close())
            logger.warning('进程管道 pending 达到上限，丢弃最旧连接: %s', oldest_id)
        self._pending[pipe_id] = pipe

    async def checkout(self, pipe_id: str) -> Pipe[PayloadType] | None:
        """领取指定 ``pipe_id`` 的端点；``accept_timeout`` 内未到达则返回 ``None``。"""
        pipe = self._pending.pop(pipe_id, None)
        if pipe is None:
            waiter: asyncio.Future[ProcessPipe[PayloadType]] = asyncio.get_running_loop().create_future()
            self._waiters[pipe_id] = waiter
            try:
                pipe = await asyncio.wait_for(waiter, timeout=self._accept_timeout)
            except asyncio.TimeoutError:
                self._waiters.pop(pipe_id, None)
                return None

        self._active[pipe_id] = pipe
        return pipe

    async def checkin(self, pipe_id: str) -> None:
        """归还并关闭端点；重复调用无副作用。"""
        waiter = self._waiters.pop(pipe_id, None)
        if waiter is not None and not waiter.done():
            waiter.cancel()

        pipe = self._active.pop(pipe_id, None) or self._pending.pop(pipe_id, None)
        if pipe is not None:
            await pipe.close()

    async def stop(self) -> None:
        """停止 accept 线程、关闭监听器，并清理所有未领走的连接。"""
        self._stopping.set()

        for waiter in list(self._waiters.values()):
            if not waiter.done():
                waiter.set_exception(PipeHandshakeError('Allocator stopped'))
        self._waiters.clear()

        if self._thread is not None:
            try:
                sentinel = await asyncio.wait_for(
                    asyncio.to_thread(Client, self.address, authkey=self._authkey),
                    timeout=2.0,
                )
                await asyncio.to_thread(sentinel.send_bytes, _STOP_SENTINEL)
                sentinel.close()
            except asyncio.TimeoutError, AuthenticationError, OSError, EOFError:
                # 监听器已不可用 / accept 线程已退出：直接关闭监听器即可
                pass
            # accept 线程可能正阻塞在 hello poll 上，等待其超时后再关闭监听器
            await asyncio.to_thread(self._thread.join, self._hello_timeout + 1.0)
            if self._thread.is_alive():
                logger.warning('进程管道 accept 线程未在期限内退出')
            self._thread = None

        if self._listener is not None:
            self._listener.close()
            self._listener = None

        for pipe in [*self._pending.values(), *self._active.values()]:
            try:
                await pipe.close()
            except Exception:
                logger.exception('关闭进程管道失败')
        self._pending.clear()
        self._active.clear()


class ProcessPipeClientAllocator[PayloadType: BaseModel](SingleRoleAllocator[PayloadType]):
    """客户端分配器：``allocate()`` 连接服务端并发送 hello。"""

    def __init__(
        self,
        address: str | tuple[str, int],
        payload_type: type[PayloadType],
        *,
        authkey: bytes | None = None,
        connect_timeout: float = 5.0,
    ) -> None:
        self._address = address
        self._payload_type = payload_type
        self._authkey = authkey
        self._connect_timeout = connect_timeout
        self._pipes: dict[str, ProcessPipe[PayloadType]] = {}

    async def allocate(self, **kwargs: Any) -> tuple[str, Pipe[PayloadType]]:
        """建立数据面连接，发送 ``pipe_id`` 后返回 ``(pipe_id, 本端端点)``。"""
        pipe_id = uuid.uuid4().hex
        conn: Connection | None = None
        try:
            conn = await asyncio.wait_for(
                asyncio.to_thread(Client, self._address, authkey=self._authkey),
                timeout=self._connect_timeout,
            )
            await asyncio.to_thread(conn.send_bytes, pipe_id.encode('utf-8'))
        except (asyncio.TimeoutError, AuthenticationError, OSError, EOFError, ValueError) as e:
            if conn is not None:
                conn.close()
            raise PipeHandshakeError(f'Pipe connect failed: {e}') from e

        pipe = ProcessPipe(conn, self._payload_type)
        self._pipes[pipe_id] = pipe
        return pipe_id, pipe

    async def release(self, pipe_id: str) -> None:
        """关闭并移除本端端点；重复调用无副作用。"""
        pipe = self._pipes.pop(pipe_id, None)
        if pipe is not None:
            await pipe.close()

    async def stop(self) -> None:
        """关闭所有本端端点（便于进程退出 / 测试收尾）。"""
        for pipe in list(self._pipes.values()):
            await pipe.close()
        self._pipes.clear()
