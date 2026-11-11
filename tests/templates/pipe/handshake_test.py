"""``open_pipe`` / ``expect_pipe`` 握手模板测试。"""

import asyncio
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from pydantic import BaseModel

from infinity_bus import (
    EventBus,
    EventDeclaration,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
)
from infinity_bus.templates.pipe import (
    Pipe,
    PipeHandshakeError,
    PipeLinkedResponse,
    PipeOpenRequest,
    expect_pipe,
    open_pipe,
)
from infinity_bus.templates.pipe.allocator import InProcessPipeAllocator


# ------------------------------------------------------------------------------
# 测试所需的事件声明与负载模型
# ------------------------------------------------------------------------------
class TestPipeOpenRequest(EventDeclaration):
    """测试用的管道打开请求事件声明"""

    name = 'test.pipe.open'
    payload_type = PipeOpenRequest


class TestPipeLinkedResponse(EventDeclaration):
    """测试用的管道连接成功响应事件声明"""

    name = 'test.pipe.linked'
    payload_type = PipeLinkedResponse


class TestPipeOpenFailRequest(EventDeclaration):
    """测试握手失败场景的请求事件"""

    name = 'test.open.fail'
    payload_type = PipeOpenRequest


class TestPipeLinkedFailResponse(EventDeclaration):
    """测试握手失败场景的响应事件"""

    name = 'test.linked.fail'
    payload_type = PipeLinkedResponse


class SimplePayload(BaseModel):
    """测试用负载模型。"""

    value: int
    msg: str = 'default'


# ------------------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------------------
@pytest.fixture
async def event_bus() -> AsyncGenerator[EventBus, None]:
    """提供一个已启动的事件总线实例。"""
    reg = EventRegistry()

    reg.register(TestPipeOpenRequest)
    reg.register(TestPipeLinkedResponse)
    reg.register(TestPipeOpenFailRequest)
    reg.register(TestPipeLinkedFailResponse)

    hreg = EventHandlerRegistry()
    bus = EventBus(reg, hreg, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=100)))
    await bus.start()
    yield bus
    await bus.stop()


@pytest.fixture
def allocator() -> InProcessPipeAllocator:
    """每个测试独立的进程内分配器（模板不再提供默认实例）。"""
    return InProcessPipeAllocator()


class _RecordingAllocator(InProcessPipeAllocator):
    """记录分配 / 释放 / 借还调用的分配器。"""

    def __init__(self) -> None:
        super().__init__()
        self.allocated: list[str] = []
        self.released: list[str] = []
        self.checked_in: list[str] = []

    async def allocate(self, **kwargs: Any) -> tuple[str, Pipe]:
        pipe_id, pipe = await super().allocate(**kwargs)
        self.allocated.append(pipe_id)
        return pipe_id, pipe

    async def release(self, pipe_id: str) -> None:
        self.released.append(pipe_id)
        await super().release(pipe_id)

    async def checkin(self, pipe_id: str) -> None:
        self.checked_in.append(pipe_id)
        await super().checkin(pipe_id)


# ------------------------------------------------------------------------------
# 握手成功路径
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_open_and_expect_pipe_roundtrip(event_bus: EventBus, allocator: InProcessPipeAllocator) -> None:
    """握手成功后两端各持一个端点，可双向收发且互不串台。"""
    req_event = 'test.pipe.open'
    resp_event = 'test.pipe.linked'

    async def server() -> None:
        async with expect_pipe(
            bus_proxy=event_bus.proxy('server'),
            req_event=req_event,
            resp_event=resp_event,
            allocator=allocator,
            timeout=2.0,
        ) as server_pipe:
            received = await server_pipe.receive()
            assert isinstance(received, SimplePayload)
            assert received.value == 42
            assert received.msg == 'hello'
            await server_pipe.send(SimplePayload(value=7, msg='reply'))

    server_task = asyncio.create_task(server())
    await asyncio.sleep(0)  # 让服务端先注册 expect

    async with open_pipe(
        bus_proxy=event_bus.proxy('client'),
        req_event=req_event,
        resp_event=resp_event,
        allocator=allocator,
        handshake_timeout=2.0,
        pipe_kargs={'maxsize': 10},
    ) as client_pipe:
        await client_pipe.send(SimplePayload(value=42, msg='hello'))
        reply = await client_pipe.receive()
        assert isinstance(reply, SimplePayload)
        assert reply.value == 7
        assert reply.msg == 'reply'

    await server_task


@pytest.mark.asyncio
async def test_expect_pipe_with_session_id(event_bus: EventBus, allocator: InProcessPipeAllocator) -> None:
    """expect_pipe 指定 session_id 时仅匹配对应会话的管道请求。"""
    req_event = 'test.pipe.open'
    resp_event = 'test.pipe.linked'
    target_session = 'my-custom-session'

    async def server() -> None:
        async with expect_pipe(
            bus_proxy=event_bus.proxy('server'),
            req_event=req_event,
            resp_event=resp_event,
            allocator=allocator,
            session_id=target_session,
            timeout=2.0,
        ) as server_pipe:
            received = await server_pipe.receive()
            assert isinstance(received, SimplePayload)
            assert received.value == 42
            await server_pipe.send(SimplePayload(value=1, msg='ack'))

    server_task = asyncio.create_task(server())
    await asyncio.sleep(0)

    async with open_pipe(
        bus_proxy=event_bus.proxy('client'),
        req_event=req_event,
        resp_event=resp_event,
        allocator=allocator,
        session_id=target_session,
        handshake_timeout=2.0,
        pipe_kargs={'maxsize': 10},
    ) as client_pipe:
        await client_pipe.send(SimplePayload(value=42, msg='session_match'))
        ack = await client_pipe.receive()
        assert isinstance(ack, SimplePayload)
        assert ack.msg == 'ack'

    await server_task


# ------------------------------------------------------------------------------
# 握手失败路径
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_expect_pipe_timeout(event_bus: EventBus, allocator: InProcessPipeAllocator) -> None:
    """测试 expect_pipe 在超时后抛出 PipeHandshakeError。"""
    with pytest.raises(PipeHandshakeError, match='Handshake timeout'):
        async with expect_pipe(
            bus_proxy=event_bus.proxy('server'),
            req_event='nonexistent.event',
            resp_event='nonexistent.resp',
            allocator=allocator,
            timeout=0.1,
        ):
            pass


@pytest.mark.asyncio
async def test_open_pipe_handshake_failure(event_bus: EventBus, allocator: InProcessPipeAllocator) -> None:
    """测试 open_pipe 在无人应答时抛出异常。"""
    with pytest.raises(PipeHandshakeError):
        async with open_pipe(
            bus_proxy=event_bus.proxy('client'),
            req_event='test.open.fail',
            resp_event='test.linked.fail',
            allocator=allocator,
            handshake_timeout=0.1,
        ):
            pass


@pytest.mark.asyncio
async def test_expect_pipe_request_filter_rejects_non_pipe_payload(
    event_bus: EventBus,
    allocator: InProcessPipeAllocator,
) -> None:
    """request_filter 中 isinstance 检查失败时返回 False，expect_pipe 应超时。"""
    with pytest.raises(PipeHandshakeError, match='Handshake timeout'):
        async with expect_pipe(
            bus_proxy=event_bus.proxy('server'),
            req_event='test.pipe.no_payload',
            resp_event='test.pipe.linked',
            allocator=allocator,
            timeout=0.3,
        ):
            # 发布无 payload 的事件：isinstance(None, PipeOpenRequest) 为 False
            await event_bus.proxy('test').publish('test.pipe.no_payload', None)
            await asyncio.sleep(0.1)


# ------------------------------------------------------------------------------
# 分配器生命周期
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_expect_pipe_releases_from_allocator(event_bus: EventBus) -> None:
    """expect_pipe 退出时应释放分配器中的管道，避免跨进程时服务端泄漏条目。"""
    allocator = _RecordingAllocator()
    server_done = asyncio.Event()

    async def server() -> None:
        async with expect_pipe(
            bus_proxy=event_bus.proxy('server'),
            req_event='test.pipe.open',
            resp_event='test.pipe.linked',
            allocator=allocator,
            timeout=2.0,
        ):
            await asyncio.sleep(0.05)
        server_done.set()

    task = asyncio.create_task(server())
    await asyncio.sleep(0)  # 让服务端先注册 expect

    async with open_pipe(
        bus_proxy=event_bus.proxy('client'),
        req_event='test.pipe.open',
        resp_event='test.pipe.linked',
        allocator=allocator,
        handshake_timeout=2.0,
    ):
        await asyncio.wait_for(server_done.wait(), timeout=1.0)
        assert allocator.allocated, '客户端应已分配管道'
        assert allocator.checked_in == allocator.allocated, 'expect_pipe 退出时应归还端点'
        assert await allocator.checkout(allocator.allocated[0]) is None, '归还后不应再能领取端点'

    await task
    assert allocator.released == allocator.allocated, 'open_pipe 退出时应释放资源'
