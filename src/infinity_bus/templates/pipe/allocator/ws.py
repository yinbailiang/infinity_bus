"""WebSocket 管道：基于 aiohttp 的双向端点与分配器（惰性依赖 aiohttp）。

控制面仍走事件总线，数据面由 WebSocket 建立：

- 服务端分配器自持一个 aiohttp 监听器（进程内只需一个），WS handler 读首帧
  ``{"pipe_id": ...}`` 后按 id 寄存连接；
- 客户端 ``allocate()`` 连接 ``url`` 并发送同一帧，随后返回本端端点；
- ``expect_pipe`` 通过 ``checkout(pipe_id)`` 领取连接，``checkin()`` 归还。

依赖：``aiohttp``（``pip install infinity_bus[ws]``）。模块惰性导入，缺失时在创建
分配器/端点时抛 ``ImportError``。
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Literal

from pydantic import BaseModel

from ..base import Pipe, PipeClosedError, PipeHandshakeError
from ..codec import PayloadType, decode_payload, encode_payload
from .base import SingleRoleAllocator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# aiohttp 惰性导入 —— 仅 WsPipe 收发与分配器启动时需要
# ---------------------------------------------------------------------------
_aiohttp: Any = None
_aiohttp_web: Any = None
_aiohttp_import_error: ImportError | None = None

try:
    import aiohttp as _aiohttp
    from aiohttp import web as _aiohttp_web
except ImportError as _e:  # pragma: no cover - 依赖缺失路径
    _aiohttp_import_error = _e


def _require_aiohttp() -> Any:
    """返回 aiohttp 模块；缺失时抛出带安装提示的 ImportError。"""
    if _aiohttp is None:
        raise ImportError('WsPipe 需要 aiohttp，请安装 infinity_bus[ws]') from _aiohttp_import_error
    return _aiohttp


def _require_web() -> Any:
    """返回 aiohttp.web 子模块；缺失时抛出带安装提示的 ImportError。"""
    if _aiohttp_web is None:
        raise ImportError('WsPipe 需要 aiohttp，请安装 infinity_bus[ws]') from _aiohttp_import_error
    return _aiohttp_web


class WsPipe(Pipe):
    """WebSocket 双向管道端点，aiohttp 的客户端与服务端连接通用。

    每个端点持有一个入站队列与一个后台泵任务：泵从 WebSocket 读取帧并解码入队，
    关闭帧/异常时结束。服务端 handler 因此也能通过泵完成关闭握手与心跳处理。
    """

    def __init__(self, ws: Any, payload_type: PayloadType, maxsize: int | None = None) -> None:
        self._ws = ws
        self._payload_type = payload_type
        self._inbound: asyncio.Queue[BaseModel] = asyncio.Queue() if maxsize is None else asyncio.Queue(maxsize=maxsize)
        self._closed = False
        self._closed_event = asyncio.Event()
        self._pump: asyncio.Task[None] | None = None

    async def open(self) -> None:
        """启动后台泵（幂等）。"""
        if self._pump is None or self._pump.done():
            self._closed = False
            self._closed_event.clear()
            self._pump = asyncio.create_task(self._pump_loop())

    async def _pump_loop(self) -> None:
        """读取 WebSocket 帧并解码入队；关闭帧或异常时结束。"""
        aiohttp = _require_aiohttp()
        try:
            while True:
                msg = await self._ws.receive()
                if msg.type == aiohttp.WSMsgType.BINARY:
                    await self._inbound.put(decode_payload(msg.data, self._payload_type))
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    await self._inbound.put(decode_payload(msg.data.encode('utf-8'), self._payload_type))
                else:
                    break
        except (ConnectionError, RuntimeError):
            pass
        finally:
            self._mark_closed()

    def _mark_closed(self) -> None:
        self._closed = True
        self._closed_event.set()

    async def wait_closed(self) -> None:
        """等待端点关闭（由 ``close()``、关闭帧或泵异常触发）。"""
        await self._closed_event.wait()

    async def send(self, data: BaseModel) -> None:
        """发送一条负载；连接已关闭时抛 ``PipeClosedError``。"""
        if self._closed or self._ws.closed:
            raise PipeClosedError('Pipe is closed')
        try:
            await self._ws.send_bytes(encode_payload(data))
        except (ConnectionError, RuntimeError) as e:
            self._mark_closed()
            raise PipeClosedError('Pipe is closed') from e

    async def receive(self) -> BaseModel:
        """读取一条负载；连接关闭且缓冲区耗尽时抛 ``PipeClosedError``。"""
        get_task: asyncio.Task[BaseModel] = asyncio.create_task(self._inbound.get())
        wait_task: asyncio.Task[Literal[True]] = asyncio.create_task(self._closed_event.wait())
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

    async def close(self) -> None:
        """停止泵并关闭 WebSocket；重复调用无副作用。"""
        self._mark_closed()
        pump, self._pump = self._pump, None
        if pump is not None:
            pump.cancel()
            try:
                await pump
            except asyncio.CancelledError:
                pass
        try:
            await asyncio.wait_for(self._ws.close(), timeout=0.5)
        except (asyncio.TimeoutError, ConnectionError, RuntimeError):
            pass


class WsPipeServerAllocator(SingleRoleAllocator):
    """服务端分配器：自持一个 WebSocket 监听器，按首帧 ``pipe_id`` 配对连接。"""

    def __init__(
        self,
        host: str,
        port: int,
        payload_type: PayloadType,
        *,
        path: str = '/pipe',
        accept_timeout: float = 5.0,
    ) -> None:
        self._host = host
        self._port = port
        self._path = path if path.startswith('/') else f'/{path}'
        self._payload_type = payload_type
        self._accept_timeout = accept_timeout
        self._app: Any = None
        self._runner: Any = None
        self._site: Any = None
        self._pending: dict[str, WsPipe] = {}
        self._active: dict[str, WsPipe] = {}
        self._waiters: dict[str, asyncio.Future[WsPipe]] = {}

    @property
    def port(self) -> int:
        """实际监听端口（构造时传 0 则为系统分配）。"""
        return self._port

    async def start(self) -> None:
        """启动 WebSocket 监听器（重复调用无副作用）。"""
        if self._site is not None:
            return
        web = _require_web()
        self._app = web.Application()
        self._app.router.add_get(self._path, self._handle_ws)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()

        server = getattr(self._site, '_server', None)
        sockets = list(getattr(server, 'sockets', []) or [])
        if sockets:
            self._port = sockets[0].getsockname()[1]

    async def _handle_ws(self, request: Any) -> Any:
        """WS handler：读 hello 帧 → 寄存 → 保持请求到连接关闭。"""
        aiohttp = _require_aiohttp()
        web = _require_web()
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        try:
            hello = await ws.receive()
            if hello.type != aiohttp.WSMsgType.TEXT:
                await ws.close()
                return ws
            pipe_id = str(json.loads(hello.data)['pipe_id'])
        except Exception:  # 握手帧非法：拒绝连接
            logger.warning('拒绝无效的管道握手帧', exc_info=True)
            await ws.close()
            return ws

        pipe = WsPipe(ws, self._payload_type)
        await pipe.open()
        self._register(pipe_id, pipe)
        try:
            await pipe.wait_closed()
        finally:
            await pipe.close()
        return ws

    def _register(self, pipe_id: str, pipe: WsPipe) -> None:
        waiter = self._waiters.pop(pipe_id, None)
        if waiter is not None and not waiter.done():
            waiter.set_result(pipe)
        else:
            self._pending[pipe_id] = pipe

    async def checkout(self, pipe_id: str) -> Pipe | None:
        """领取指定 ``pipe_id`` 的端点；``accept_timeout`` 内未到达则返回 ``None``。"""
        pipe = self._pending.pop(pipe_id, None)
        if pipe is None:
            waiter: asyncio.Future[WsPipe] = asyncio.get_running_loop().create_future()
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
        """停止监听器并清理所有未领走的连接。"""
        # 先关管道，handler 才会从 wait_closed() 返回，runner 才能干净退出
        for pipe in [*self._pending.values(), *self._active.values()]:
            await pipe.close()
        self._pending.clear()
        self._active.clear()

        if self._site is not None:
            await self._site.stop()
            self._site = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None


class WsPipeClientAllocator(SingleRoleAllocator):
    """客户端分配器：``allocate()`` 连接服务端并发送 hello。"""

    def __init__(
        self,
        url: str,
        payload_type: PayloadType,
        *,
        connect_timeout: float = 5.0,
    ) -> None:
        self._url = url
        self._payload_type = payload_type
        self._connect_timeout = connect_timeout
        self._session: Any = None
        self._pipes: dict[str, WsPipe] = {}

    async def allocate(self, **kwargs: Any) -> tuple[str, Pipe]:
        """连接服务端 WebSocket，发送 ``pipe_id`` 后返回 ``(pipe_id, 本端端点)``。"""
        aiohttp = _require_aiohttp()
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

        pipe_id = uuid.uuid4().hex
        ws: Any = None
        try:
            ws = await asyncio.wait_for(self._session.ws_connect(self._url), timeout=self._connect_timeout)
            await ws.send_json({'pipe_id': pipe_id})
        except Exception as e:
            if ws is not None:
                await ws.close()
            raise PipeHandshakeError(f'Pipe connect failed: {e}') from e

        pipe = WsPipe(ws, self._payload_type)
        self._pipes[pipe_id] = pipe
        return pipe_id, pipe

    async def release(self, pipe_id: str) -> None:
        """关闭并移除本端端点；重复调用无副作用。"""
        pipe = self._pipes.pop(pipe_id, None)
        if pipe is not None:
            await pipe.close()

    async def stop(self) -> None:
        """关闭内部 HTTP 会话。"""
        if self._session is not None:
            await self._session.close()
            self._session = None
