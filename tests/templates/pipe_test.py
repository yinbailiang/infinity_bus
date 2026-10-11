import asyncio
from typing import AsyncGenerator, List, Optional, Tuple

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
    InProcessPipe,
    InProcessPipeAllocator,
    Pipe,
    PipeClosedError,
    PipeHandshakeError,
    PipeLinkedResponse,
    PipeOpenRequest,
    expect_pipe,
    open_pipe,
)


# ------------------------------------------------------------------------------
# 测试所需的事件声明类
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


class NoPayloadPipeEvent(EventDeclaration):
    name = 'test.pipe.no_payload'
    payload_type = None


# ------------------------------------------------------------------------------
# 测试用的 Payload 模型
# ------------------------------------------------------------------------------
class SimplePayload(BaseModel):
    value: int
    msg: str = 'default'


class AnotherPayload(BaseModel):
    data: list[int]


# ------------------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------------------
@pytest.fixture
async def event_bus() -> AsyncGenerator[EventBus, None]:
    """提供一个已启动的事件总线实例。"""
    reg = EventRegistry()

    # 注册测试事件
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
async def pipe_pair(event_bus: EventBus) -> AsyncGenerator[Tuple[Pipe, Pipe], None]:
    """
    通过 open_pipe 和 expect_pipe 建立一对连接好的管道，
    返回 (client_pipe, server_pipe)。
    """
    req_event = 'test.pipe.open'
    resp_event = 'test.pipe.linked'

    # 服务端：等待连接
    server_task = asyncio.create_task(_expect_pipe_async(event_bus, req_event, resp_event))

    # 客户端：发起连接
    async with open_pipe(
        bus_proxy=event_bus.proxy('client'),
        req_event=req_event,
        resp_event=resp_event,
        handshake_timeout=2.0,
        pipe_kargs={'maxsize': 10},
    ) as client_pipe:
        server_pipe = await server_task
        yield client_pipe, server_pipe


async def _expect_pipe_async(bus: EventBus, req_event: str, resp_event: str) -> Pipe:
    """辅助函数，便于在 fixture 中等待管道。"""
    async with expect_pipe(
        bus_proxy=bus.proxy('server'),
        req_event=req_event,
        resp_event=resp_event,
        timeout=2.0,
    ) as pipe:
        return pipe


async def _expect_pipe_with_session(bus: EventBus, req_event: str, resp_event: str, session_id: str) -> Pipe:
    """辅助函数：使用指定 session_id 等待管道。"""
    async with expect_pipe(
        bus_proxy=bus.proxy('server'),
        req_event=req_event,
        resp_event=resp_event,
        session_id=session_id,
        timeout=2.0,
    ) as pipe:
        return pipe


# ------------------------------------------------------------------------------
# 基础功能测试
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_send_receive_basic(pipe_pair: tuple[Pipe, Pipe]) -> None:
    """测试基本的发送和接收。"""
    client, server = pipe_pair

    payload = SimplePayload(value=42, msg='hello')
    await client.send(payload)

    received = await server.receive()
    assert isinstance(received, SimplePayload)
    assert received.value == 42
    assert received.msg == 'hello'


@pytest.mark.asyncio
async def test_send_multiple_items(pipe_pair: tuple[Pipe, Pipe]) -> None:
    """测试发送多条消息，保持 FIFO 顺序。"""
    client, server = pipe_pair

    values: list[int] = [1, 2, 3, 4, 5]
    for v in values:
        await client.send(SimplePayload(value=v))

    for expected in values:
        received = await server.receive()
        assert isinstance(received, SimplePayload)
        assert received.value == expected


@pytest.mark.asyncio
async def test_receive_after_close(pipe_pair: tuple[Pipe, Pipe]) -> None:
    """测试管道关闭后，接收方会收到 PipeClosedError。"""
    client, server = pipe_pair

    # 发送一条消息
    await client.send(SimplePayload(value=1))

    # 关闭客户端管道（这将触发两阶段关闭）
    await client.close()

    # 服务器应该先收到已存在的消息
    received: BaseModel = await server.receive()
    assert isinstance(received, SimplePayload)
    assert received.value == 1

    # 下一次接收应该抛出关闭异常
    with pytest.raises(PipeClosedError):
        await server.receive()


@pytest.mark.asyncio
async def test_send_after_close(pipe_pair: tuple[Pipe, Pipe]) -> None:
    """测试管道关闭后，发送方无法再发送新数据。"""
    client, _ = pipe_pair

    await client.close()

    with pytest.raises(PipeClosedError):
        await client.send(SimplePayload(value=999))


@pytest.mark.asyncio
async def test_context_manager_closes_pipe() -> None:
    """测试使用 async with 上下文时，管道在退出时自动关闭。"""
    pipe = InProcessPipe(maxsize=5)
    async with pipe:
        await pipe.send(SimplePayload(value=1))
        # 退出块时自动调用 close

    # 关闭后发送应失败
    with pytest.raises(PipeClosedError):
        await pipe.send(SimplePayload(value=2))


# ------------------------------------------------------------------------------
# 背压与队列满测试
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_backpressure_send_blocks_when_full(pipe_pair: tuple[Pipe, Pipe]) -> None:
    """测试当队列满时，send 会阻塞直到有空位。"""
    client, server = pipe_pair
    # maxsize=10 (来自 fixture)

    # 快速填满队列
    for i in range(10):
        await client.send(SimplePayload(value=i))

    # 启动一个后台发送任务（第 11 个，会阻塞）
    send_task = asyncio.create_task(client.send(SimplePayload(value=100)))

    # 给一点时间让 send_task 挂起
    await asyncio.sleep(0.05)
    assert not send_task.done(), '第11个发送应该阻塞'

    # 从服务器取走一条消息，释放队列空位
    received: BaseModel = await server.receive()
    assert isinstance(received, SimplePayload)
    assert received.value == 0

    # 此时阻塞的 send 应该完成
    await asyncio.wait_for(send_task, timeout=1.0)

    # 验证第 11 条消息最终被接收
    # 需要消费掉队列中剩余的 9 条，然后再接收第 11 条
    for i in range(1, 10):
        r: BaseModel = await server.receive()
        assert isinstance(r, SimplePayload)
        assert r.value == i
    final: BaseModel = await server.receive()
    assert isinstance(final, SimplePayload)
    assert final.value == 100


@pytest.mark.asyncio
async def test_drain_on_close_does_not_lose_data(pipe_pair: tuple[Pipe, Pipe]) -> None:
    """测试关闭时排空队列：已入队的数据必须全部被消费。"""
    client, server = pipe_pair

    # 发送 5 条消息
    for i in range(5):
        await client.send(SimplePayload(value=i))

    # 关闭客户端（会等待队列排空）
    close_task: asyncio.Task[None] = asyncio.create_task(client.close())

    # 服务端应该能收到全部 5 条消息
    received_values: List[int] = []
    for _ in range(5):
        try:
            r: BaseModel = await server.receive()
            assert isinstance(r, SimplePayload)
            received_values.append(r.value)
        except PipeClosedError:
            break

    assert received_values == [0, 1, 2, 3, 4]

    # 关闭任务应该顺利完成
    await close_task

    # 之后接收应抛出异常
    with pytest.raises(PipeClosedError):
        await server.receive()


# ------------------------------------------------------------------------------
# 并发与竞态测试
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_close_while_receive_blocking(pipe_pair: tuple[Pipe, Pipe]) -> None:
    """测试在 receive 阻塞等待数据时，close 能立即唤醒并抛出异常。"""
    client, server = pipe_pair

    # 启动一个接收协程，它会因为队列空而阻塞
    recv_task: asyncio.Task[BaseModel] = asyncio.create_task(server.receive())

    await asyncio.sleep(0.05)
    assert not recv_task.done()

    # 关闭管道
    await client.close()

    # 阻塞的 receive 应该以 PipeClosedError 结束
    with pytest.raises(PipeClosedError):
        await recv_task


@pytest.mark.asyncio
async def test_close_while_send_blocking(pipe_pair: tuple[Pipe, Pipe]) -> None:
    """
    测试在 send 阻塞（队列满）时，close 能正确执行排空，
    阻塞的 send 最终成功，数据不丢失。
    """
    client, server = pipe_pair

    # 填满队列（maxsize=10）
    for i in range(10):
        await client.send(SimplePayload(value=i))

    # 启动一个会阻塞的 send
    blocked_send = asyncio.create_task(client.send(SimplePayload(value=999)))
    await asyncio.sleep(0.05)
    assert not blocked_send.done()

    # 关闭管道，这会先禁用新发送，然后等待排空
    close_task = asyncio.create_task(client.close())

    # 服务端消费一条消息，释放空位，让阻塞的 send 完成
    r = await server.receive()
    assert isinstance(r, SimplePayload)
    assert r.value == 0

    # 阻塞的 send 应该很快完成
    await asyncio.wait_for(blocked_send, timeout=1.0)

    # 继续消费剩余消息，直到关闭完成
    for expected in range(1, 10):
        r = await server.receive()
        assert isinstance(r, SimplePayload)
        assert r.value == expected
    final = await server.receive()
    assert isinstance(final, SimplePayload)
    assert final.value == 999

    # 关闭任务应完成
    await close_task

    # 后续接收抛异常
    with pytest.raises(PipeClosedError):
        await server.receive()


# ------------------------------------------------------------------------------
# 握手与异常流程测试
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_expect_pipe_timeout(event_bus: EventBus) -> None:
    """测试 expect_pipe 在超时后抛出 PipeHandshakeError。"""
    with pytest.raises(PipeHandshakeError, match='Handshake timeout'):
        async with expect_pipe(
            bus_proxy=event_bus.proxy('server'),
            req_event='nonexistent.event',
            resp_event='nonexistent.resp',
            timeout=0.1,
        ):
            pass


@pytest.mark.asyncio
async def test_open_pipe_handshake_failure(event_bus: EventBus) -> None:
    """测试 open_pipe 在无人应答时抛出异常。"""
    with pytest.raises(PipeHandshakeError):
        async with open_pipe(
            bus_proxy=event_bus.proxy('client'),
            req_event='test.open.fail',
            resp_event='test.linked.fail',
            handshake_timeout=0.1,
        ):
            pass


@pytest.mark.asyncio
async def test_expect_pipe_with_session_id(event_bus: EventBus) -> None:
    """expect_pipe 指定 session_id 时仅匹配对应会话的管道请求"""
    req_event = 'test.pipe.open'
    resp_event = 'test.pipe.linked'
    target_session = 'my-custom-session'

    # 服务端：用指定 session_id 等待管道
    server_task = asyncio.create_task(_expect_pipe_with_session(event_bus, req_event, resp_event, target_session))

    # 客户端：发起连接，携带匹配的 session_id
    async with open_pipe(
        bus_proxy=event_bus.proxy('client'),
        req_event=req_event,
        resp_event=resp_event,
        session_id=target_session,
        handshake_timeout=2.0,
        pipe_kargs={'maxsize': 10},
    ) as client_pipe:
        server_pipe = await server_task
        await client_pipe.send(SimplePayload(value=42, msg='session_match'))
        received = await server_pipe.receive()
        assert isinstance(received, SimplePayload)
        assert received.value == 42


@pytest.mark.asyncio
async def test_expect_pipe_request_filter_rejects_non_pipe_payload(
    event_bus: EventBus,
) -> None:
    """request_filter 中 isinstance 检查失败时返回 False，expect_pipe 应超时"""
    # 注册一个无 payload 类型的事件用于测试

    with pytest.raises(PipeHandshakeError, match='Handshake timeout'):
        async with expect_pipe(
            bus_proxy=event_bus.proxy('server'),
            req_event='test.pipe.no_payload',
            resp_event='test.pipe.linked',
            timeout=0.3,
        ):
            # 发布一个无 payload 的事件 → request_filter 中
            # isinstance(None, PipeOpenRequest) 为 False → return False
            await event_bus.proxy('test').publish('test.pipe.no_payload', None)
            # give some time for the event to be dispatched and filtered
            await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_pipe_id_collision_prevention(event_bus: EventBus) -> None:
    """测试 InProcessPipeAllocator 分配管道并正常获取/释放。"""

    allocator = InProcessPipeAllocator(pipe_type=InProcessPipe)

    # 分配一个管道
    pipe_id: str = await allocator.allocate()
    pipe: Optional[Pipe] = await allocator.get(pipe_id)
    assert pipe is not None
    assert isinstance(pipe, InProcessPipe)

    # 发送一条消息验证管道可用
    await pipe.send(SimplePayload(value=42))
    received = await pipe.receive()
    assert isinstance(received, SimplePayload)
    assert received.value == 42

    # 释放管道
    await allocator.release(pipe_id)
    assert await allocator.get(pipe_id) is None


# ------------------------------------------------------------------------------
# 边界条件测试
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_receive_after_remote_close_with_remaining_data(
    pipe_pair: tuple[Pipe, Pipe],
) -> None:
    """对端关闭后，本端仍能读取缓冲区剩余数据，之后再抛异常。"""
    client, server = pipe_pair

    await client.send(SimplePayload(value=1))
    await client.send(SimplePayload(value=2))

    # 关闭客户端
    await client.close()

    # 服务端先读到两条数据
    r1: BaseModel = await server.receive()
    assert isinstance(r1, SimplePayload)
    assert r1.value == 1

    r2: BaseModel = await server.receive()
    assert isinstance(r2, SimplePayload)
    assert r2.value == 2

    # 第三次接收应该抛异常
    with pytest.raises(PipeClosedError):
        await server.receive()
