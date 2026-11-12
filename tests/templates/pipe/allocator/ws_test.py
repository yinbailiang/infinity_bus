"""``WsPipe`` 与 WebSocket 分配器测试（aiohttp）。"""

import asyncio
from collections.abc import AsyncGenerator

import pytest
from pydantic import BaseModel

from infinity_bus.templates.pipe import PipeClosedError
from infinity_bus.templates.pipe.allocator.ws import (
    WsPipe,
    WsPipeClientAllocator,
    WsPipeServerAllocator,
)

aiohttp = pytest.importorskip('aiohttp')


class Payload(BaseModel):
    """测试用负载模型。"""

    value: int
    msg: str = 'default'


@pytest.fixture
async def ws_pair() -> AsyncGenerator[
    tuple[WsPipeClientAllocator, WsPipeServerAllocator, str, WsPipe, WsPipe],
    None,
]:
    """建立一个本地 aiohttp 服务端与已配对的 WS 管道端点。"""
    server = WsPipeServerAllocator('127.0.0.1', 0, Payload)
    await server.start()
    client = WsPipeClientAllocator(f'http://127.0.0.1:{server.port}/pipe', Payload)

    pipe_id, client_pipe = await client.allocate()
    server_pipe = await server.checkout(pipe_id)
    assert server_pipe is not None
    assert isinstance(client_pipe, WsPipe)
    assert isinstance(server_pipe, WsPipe)

    await client_pipe.open()
    await server_pipe.open()
    try:
        yield client, server, pipe_id, client_pipe, server_pipe
    finally:
        await client.release(pipe_id)
        await client.stop()
        await server.stop()


@pytest.mark.asyncio
async def test_duplex_roundtrip(
    ws_pair: tuple[WsPipeClientAllocator, WsPipeServerAllocator, str, WsPipe, WsPipe],
) -> None:
    """两端可双向收发且互不串台。"""
    _, _, _, client_pipe, server_pipe = ws_pair

    await client_pipe.send(Payload(value=1))
    await client_pipe.send(Payload(value=2))
    first = await server_pipe.receive()
    assert isinstance(first, Payload)
    assert first.value == 1

    await server_pipe.send(Payload(value=100))
    reply = await client_pipe.receive()
    assert isinstance(reply, Payload)
    assert reply.value == 100

    rest = await server_pipe.receive()
    assert isinstance(rest, Payload)
    assert rest.value == 2


@pytest.mark.asyncio
async def test_release_propagates_close(
    ws_pair: tuple[WsPipeClientAllocator, WsPipeServerAllocator, str, WsPipe, WsPipe],
) -> None:
    """客户端 release 后，服务端 receive 应抛 PipeClosedError。"""
    client, _, pipe_id, _, server_pipe = ws_pair

    await client.release(pipe_id)

    with pytest.raises(PipeClosedError):
        await asyncio.wait_for(server_pipe.receive(), timeout=2.0)


@pytest.mark.asyncio
async def test_checkin_propagates_close(
    ws_pair: tuple[WsPipeClientAllocator, WsPipeServerAllocator, str, WsPipe, WsPipe],
) -> None:
    """服务端 checkin 后，客户端 receive 应抛 PipeClosedError。"""
    _, server, pipe_id, client_pipe, _ = ws_pair

    await server.checkin(pipe_id)

    with pytest.raises(PipeClosedError):
        await asyncio.wait_for(client_pipe.receive(), timeout=2.0)


@pytest.mark.asyncio
async def test_send_after_close_raises(
    ws_pair: tuple[WsPipeClientAllocator, WsPipeServerAllocator, str, WsPipe, WsPipe],
) -> None:
    """关闭后写入抛 PipeClosedError。"""
    _, _, _, client_pipe, _ = ws_pair

    await client_pipe.close()

    with pytest.raises(PipeClosedError):
        await client_pipe.send(Payload(value=1))


@pytest.mark.asyncio
async def test_checkout_timeout_returns_none() -> None:
    """指定 pipe_id 未在超时内到达时 checkout 返回 None。"""
    server = WsPipeServerAllocator('127.0.0.1', 0, Payload, accept_timeout=0.1)
    await server.start()
    try:
        assert await server.checkout('missing') is None
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_single_role_methods_raise() -> None:
    """客户端分配器不支持 checkout，服务端分配器不支持 allocate。"""
    server = WsPipeServerAllocator('127.0.0.1', 0, Payload)
    client = WsPipeClientAllocator('http://127.0.0.1:1/pipe', Payload)

    with pytest.raises(NotImplementedError):
        await server.allocate()
    with pytest.raises(NotImplementedError):
        await client.checkout('x')


@pytest.mark.asyncio
async def test_cancelled_receive_does_not_steal_message(
    ws_pair: tuple[WsPipeClientAllocator, WsPipeServerAllocator, str, WsPipe, WsPipe],
) -> None:
    """取消 receive 后，后续消息仍应能被下一次 receive 取到。"""
    _, _, _, client_pipe, server_pipe = ws_pair

    recv_task = asyncio.create_task(server_pipe.receive())
    await asyncio.sleep(0.05)
    recv_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await recv_task

    await client_pipe.send(Payload(value=42))
    received = await asyncio.wait_for(server_pipe.receive(), timeout=0.5)
    assert received.value == 42


@pytest.mark.asyncio
async def test_silent_hello_does_not_block_stop() -> None:
    """握手前不发 hello 的连接不应拖住 stop。"""
    server = WsPipeServerAllocator('127.0.0.1', 0, Payload, hello_timeout=0.2, shutdown_timeout=1.0)
    await server.start()
    session = aiohttp.ClientSession()
    ws = await session.ws_connect(f'http://127.0.0.1:{server.port}/pipe')
    try:
        await asyncio.sleep(0.05)
        await asyncio.wait_for(server.stop(), timeout=3.0)
    finally:
        await ws.close()
        await session.close()


@pytest.mark.asyncio
async def test_invalid_frame_does_not_raise_on_stop() -> None:
    """解码失败的帧只终止管道，不应让 stop 抛错。"""
    server = WsPipeServerAllocator('127.0.0.1', 0, Payload, accept_timeout=1.0)
    await server.start()
    session = aiohttp.ClientSession()
    ws = await session.ws_connect(f'http://127.0.0.1:{server.port}/pipe')
    try:
        await ws.send_json({'pipe_id': 'p1'})
        pipe = await server.checkout('p1')
        assert pipe is not None
        await ws.send_bytes(b'not-json')
        await asyncio.sleep(0.1)
        await asyncio.wait_for(server.stop(), timeout=3.0)
    finally:
        await ws.close()
        await session.close()
