"""握手模板：``open_pipe`` / ``expect_pipe``（控制面走总线，数据面交给 Pipe 实现）。"""

import asyncio
import logging
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from pydantic import BaseModel

from ... import Event, EventBus
from ..expect import expect
from ..request import ResponseProtocol, request
from .allocator import PipeAllocator
from .base import Pipe, PipeHandshakeError
from .protocol import PipeLinkedResponse, PipeOpenRequest

logger = logging.getLogger(__name__)


@asynccontextmanager
async def open_pipe[PayloadType: BaseModel](
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    allocator: PipeAllocator[PayloadType],
    *,
    handshake_timeout: float = 5.0,
    session_id: str | None = None,
    pipe_kwargs: dict[str, Any] | None = None,
) -> AsyncGenerator[Pipe[PayloadType], None]:
    """客户端管道握手上下文管理器。

    分配管道 → 发送握手请求 → 等待握手响应 → yield 已就绪的 Pipe。
    """
    session_id = session_id or uuid.uuid4().hex

    pipe_id, pipe = await allocator.allocate(**(pipe_kwargs or {}))
    logger.debug(f'Pipe allocated with id={pipe_id}')

    try:
        try:
            resp: ResponseProtocol = await request(
                bus_proxy=bus_proxy,
                req_event=req_event,
                req_data={
                    'pipe_id': pipe_id,
                },
                resp_event=resp_event,
                session_id=session_id,
                timeout=handshake_timeout,
            )
        except asyncio.TimeoutError as e:
            raise PipeHandshakeError('Handshake timeout') from e
        except Exception as e:
            raise PipeHandshakeError(f'Handshake failed: {e}') from e

        if not isinstance(resp, PipeLinkedResponse):
            raise PipeHandshakeError(f'Handshake failed: expect PipeLinkedResponse but {resp.__class__.__name__}')
        if not resp.success:
            raise PipeHandshakeError(f'Handshake failed: {resp.error_msg}')

        logger.debug(f'Pipe handshake successful for id={pipe_id}')

        async with pipe:
            yield pipe

    finally:
        try:
            await allocator.release(pipe_id)
        except Exception:
            logger.exception('Pipe 释放失败: %s', pipe_id)
        logger.debug(f'Pipe {pipe_id} released from allocator')


@asynccontextmanager
async def expect_pipe[PayloadType: BaseModel](
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    allocator: PipeAllocator[PayloadType],
    session_id: str | None = None,
    timeout: float = 5.0,
) -> AsyncGenerator[Pipe[PayloadType], None]:
    """等待一个管道连接请求，返回已建立的 Pipe 实例。"""

    def request_filter(event: Event) -> bool:
        """仅匹配指定 session_id 的握手请求。"""
        if not isinstance(event.data, PipeOpenRequest):
            return False
        if session_id is not None and event.data.session_id != session_id:
            return False
        return True

    try:
        async with expect(bus_proxy, req_event, request_filter) as future:
            req_event_obj: Event = await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError as e:
        raise PipeHandshakeError('Handshake timeout') from e

    req_data: BaseModel | None = req_event_obj.data
    if not isinstance(req_data, PipeOpenRequest):
        raise PipeHandshakeError('Invalid request payload type')

    pipe_id: str = req_data.pipe_id
    pipe: Pipe[PayloadType] | None = await allocator.checkout(pipe_id)
    if pipe is None:
        error_resp = PipeLinkedResponse(
            session_id=req_data.session_id,
            request_id=req_data.request_id,
            success=False,
            error_msg=f'Pipe {pipe_id} not found',
        )
        try:
            await bus_proxy.publish(resp_event, error_resp.model_dump())
        except Exception:
            logger.exception('发送管道握手失败响应失败')
        raise PipeHandshakeError(f'Pipe {pipe_id} not found')

    success_resp = PipeLinkedResponse(session_id=req_data.session_id, request_id=req_data.request_id, success=True)

    try:
        try:
            await bus_proxy.publish(resp_event, success_resp.model_dump())
        except Exception as e:
            raise PipeHandshakeError(f'Handshake failed: {e}') from e
        logger.debug(f'Pipe accepted: {pipe_id}')

        async with pipe:
            yield pipe
    finally:
        try:
            await allocator.checkin(pipe_id)
        except Exception:
            logger.exception('Pipe 归还失败: %s', pipe_id)
        logger.debug(f'Pipe {pipe_id} checked in')
