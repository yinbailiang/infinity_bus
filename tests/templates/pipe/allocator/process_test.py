"""``ProcessPipe`` 与进程间分配器测试（基于 ``multiprocessing.connection``）。"""

import asyncio
from collections.abc import AsyncGenerator

import pytest
from pydantic import BaseModel

from infinity_bus.templates.pipe import PipeClosedError, PipeHandshakeError
from infinity_bus.templates.pipe.allocator.process import (
    ProcessPipe,
    ProcessPipeClientAllocator,
    ProcessPipeServerAllocator,
)


class Payload(BaseModel):
    """测试用负载模型。"""

    value: int
    msg: str = 'default'


AUTHKEY = b'infinity-bus-test'


@pytest.fixture
async def process_pair() -> AsyncGenerator[
    tuple[ProcessPipeClientAllocator, ProcessPipeServerAllocator, str, ProcessPipe, ProcessPipe],
    None,
]:
    """建立一对已配对的进程间管道端点。"""
    server = ProcessPipeServerAllocator(('127.0.0.1', 0), Payload, authkey=AUTHKEY)
    await server.start()

    client = ProcessPipeClientAllocator(server.address, Payload, authkey=AUTHKEY)
    pipe_id, client_pipe = await client.allocate()
    server_pipe = await server.checkout(pipe_id)
    assert server_pipe is not None
    assert isinstance(client_pipe, ProcessPipe)
    assert isinstance(server_pipe, ProcessPipe)

    await client_pipe.open()
    await server_pipe.open()
    try:
        yield client, server, pipe_id, client_pipe, server_pipe
    finally:
        await client.release(pipe_id)
        await server.stop()


@pytest.mark.asyncio
async def test_duplex_roundtrip(
    process_pair: tuple[ProcessPipeClientAllocator, ProcessPipeServerAllocator, str, ProcessPipe, ProcessPipe],
) -> None:
    """两端可双向收发且互不串台。"""
    _, _, _, client_pipe, server_pipe = process_pair

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
async def test_release_propagates_eof(
    process_pair: tuple[ProcessPipeClientAllocator, ProcessPipeServerAllocator, str, ProcessPipe, ProcessPipe],
) -> None:
    """客户端 release 后，服务端 receive 应抛 PipeClosedError（而不是挂起）。"""
    client, _, pipe_id, _, server_pipe = process_pair

    await client.release(pipe_id)

    with pytest.raises(PipeClosedError):
        await asyncio.wait_for(server_pipe.receive(), timeout=2.0)


@pytest.mark.asyncio
async def test_checkin_propagates_eof(
    process_pair: tuple[ProcessPipeClientAllocator, ProcessPipeServerAllocator, str, ProcessPipe, ProcessPipe],
) -> None:
    """服务端 checkin 后，客户端 receive 应抛 PipeClosedError。"""
    _, server, pipe_id, client_pipe, _ = process_pair

    await server.checkin(pipe_id)

    with pytest.raises(PipeClosedError):
        await asyncio.wait_for(client_pipe.receive(), timeout=2.0)


@pytest.mark.asyncio
async def test_send_after_close_raises(
    process_pair: tuple[ProcessPipeClientAllocator, ProcessPipeServerAllocator, str, ProcessPipe, ProcessPipe],
) -> None:
    """关闭后写入抛 PipeClosedError。"""
    _, _, _, client_pipe, _ = process_pair

    await client_pipe.close()

    with pytest.raises(PipeClosedError):
        await client_pipe.send(Payload(value=1))


@pytest.mark.asyncio
async def test_authkey_mismatch_raises_handshake_error() -> None:
    """authkey 不匹配时 allocate() 抛 PipeHandshakeError。"""
    server = ProcessPipeServerAllocator(('127.0.0.1', 0), Payload, authkey=AUTHKEY)
    await server.start()
    try:
        client = ProcessPipeClientAllocator(server.address, Payload, authkey=b'wrong')
        with pytest.raises(PipeHandshakeError):
            await client.allocate()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_checkout_timeout_returns_none() -> None:
    """指定 pipe_id 未在超时内到达时 checkout 返回 None。"""
    server = ProcessPipeServerAllocator(('127.0.0.1', 0), Payload, authkey=AUTHKEY, accept_timeout=0.1)
    await server.start()
    try:
        assert await server.checkout('missing') is None
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_single_role_methods_raise() -> None:
    """客户端分配器不支持 checkout，服务端分配器不支持 allocate。"""
    server = ProcessPipeServerAllocator(('127.0.0.1', 0), Payload, authkey=AUTHKEY)
    client = ProcessPipeClientAllocator(('127.0.0.1', 1), Payload, authkey=AUTHKEY)

    with pytest.raises(NotImplementedError):
        await server.allocate()
    with pytest.raises(NotImplementedError):
        await client.checkout('x')
