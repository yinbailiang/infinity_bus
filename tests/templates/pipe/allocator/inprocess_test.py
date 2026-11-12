"""``InProcessPipe`` 与 ``InProcessPipeAllocator`` 单元测试。"""

import asyncio

import pytest
from pydantic import BaseModel

from infinity_bus.templates.pipe import PipeClosedError
from infinity_bus.templates.pipe.allocator import InProcessPipe, InProcessPipeAllocator, create_pair


class Payload(BaseModel):
    """测试用负载模型。"""

    value: int
    msg: str = 'default'


# ------------------------------------------------------------------------------
# InProcessPipe：双端点收发语义
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_send_receive_basic() -> None:
    """A 端写入的数据由 B 端读出。"""
    first, second = create_pair(InProcessPipe)

    await first.send(Payload(value=42, msg='hello'))
    received = await second.receive()

    assert isinstance(received, Payload)
    assert received.value == 42
    assert received.msg == 'hello'


@pytest.mark.asyncio
async def test_send_multiple_items_keeps_fifo() -> None:
    """同一方向的多个消息保持 FIFO 顺序。"""
    first, second = create_pair(InProcessPipe)
    values = [1, 2, 3, 4, 5]

    for value in values:
        await first.send(Payload(value=value))

    for expected in values:
        received = await second.receive()
        assert isinstance(received, Payload)
        assert received.value == expected


@pytest.mark.asyncio
async def test_duplex_no_crosstalk() -> None:
    """双向同时收发时，各端只读到对端写入的数据。"""
    first, second = create_pair(InProcessPipe)

    await first.send(Payload(value=1))
    await first.send(Payload(value=2))

    # 对端消费第一条并回写
    received = await second.receive()
    assert isinstance(received, Payload)
    assert received.value == 1
    await second.send(Payload(value=100))

    # 本端应读回对端写入的 100，而不是自己尚未被消费的 2
    reply = await first.receive()
    assert isinstance(reply, Payload)
    assert reply.value == 100

    # 对端随后仍能按序读到本端的 2
    rest = await second.receive()
    assert isinstance(rest, Payload)
    assert rest.value == 2


@pytest.mark.asyncio
async def test_endpoints_are_distinct_objects() -> None:
    """配对端点必须是两个不同对象，否则无法区分方向。"""
    first, second = create_pair(InProcessPipe)
    assert first is not second


@pytest.mark.asyncio
async def test_direct_construction_wires_loopback() -> None:
    """直接构造端点时，入站/出站队列与关闭事件由调用方负责接线。"""
    queue: asyncio.Queue[Payload] = asyncio.Queue()
    closed = asyncio.Event()
    pipe = InProcessPipe(queue, queue, closed, closed)

    await pipe.send(Payload(value=1))
    received = await pipe.receive()

    assert isinstance(received, Payload)
    assert received.value == 1


@pytest.mark.asyncio
async def test_context_manager_closes_pipe() -> None:
    """使用 async with 上下文时，管道在退出时自动关闭。"""
    first, _ = create_pair(InProcessPipe, maxsize=5)
    async with first:
        await first.send(Payload(value=1))

    with pytest.raises(PipeClosedError):
        await first.send(Payload(value=2))


# ------------------------------------------------------------------------------
# InProcessPipe：关闭语义
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_receive_after_peer_close_drains_buffer() -> None:
    """对端关闭后仍能读取缓冲区数据，耗尽后抛 PipeClosedError。"""
    first, second = create_pair(InProcessPipe)

    await first.send(Payload(value=1))
    await first.close()

    received = await second.receive()
    assert isinstance(received, Payload)
    assert received.value == 1

    with pytest.raises(PipeClosedError):
        await second.receive()


@pytest.mark.asyncio
async def test_receive_after_peer_close_with_multiple_buffered_items() -> None:
    """对端关闭后缓冲区内的多条消息按序读完，再抛异常。"""
    first, second = create_pair(InProcessPipe)

    await first.send(Payload(value=1))
    await first.send(Payload(value=2))
    await first.close()

    first_item = await second.receive()
    second_item = await second.receive()
    assert isinstance(first_item, Payload) and first_item.value == 1
    assert isinstance(second_item, Payload) and second_item.value == 2

    with pytest.raises(PipeClosedError):
        await second.receive()


@pytest.mark.asyncio
async def test_send_after_close_raises() -> None:
    """本端关闭后不能写入新数据。"""
    first, _ = create_pair(InProcessPipe)
    await first.close()

    with pytest.raises(PipeClosedError):
        await first.send(Payload(value=999))


@pytest.mark.asyncio
async def test_send_after_peer_close_raises() -> None:
    """对端关闭后本端写入应失败（数据已无人读取）。"""
    first, second = create_pair(InProcessPipe)
    await second.close()

    with pytest.raises(PipeClosedError):
        await first.send(Payload(value=1))


@pytest.mark.asyncio
async def test_close_while_receive_blocking() -> None:
    """receive 阻塞等待数据时，对端 close 能唤醒并抛出异常。"""
    first, second = create_pair(InProcessPipe)

    recv_task: asyncio.Task[BaseModel] = asyncio.create_task(second.receive())
    await asyncio.sleep(0.05)
    assert not recv_task.done()

    await first.close()

    with pytest.raises(PipeClosedError):
        await recv_task


@pytest.mark.asyncio
async def test_close_wakes_local_receive() -> None:
    """本端 close 是终态，应立即唤醒本端阻塞的 receive。"""
    first, _ = create_pair(InProcessPipe)

    recv_task: asyncio.Task[BaseModel] = asyncio.create_task(first.receive())
    await asyncio.sleep(0.05)
    assert not recv_task.done()

    await first.close()

    with pytest.raises(PipeClosedError):
        await recv_task


@pytest.mark.asyncio
async def test_close_is_idempotent() -> None:
    """重复 close 不抛异常。"""
    first, _ = create_pair(InProcessPipe)
    await first.close()
    await first.close()


@pytest.mark.asyncio
async def test_reopen_after_close_clears_inbound_buffer() -> None:
    """重新 open() 会清空残留的入站数据并重置本端关闭标记。"""
    first, second = create_pair(InProcessPipe)

    await second.send(Payload(value=1))
    await first.close()
    await first.open()

    await second.send(Payload(value=2))
    received = await first.receive()
    assert isinstance(received, Payload)
    assert received.value == 2


# ------------------------------------------------------------------------------
# InProcessPipe：背压
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_backpressure_send_blocks_when_full() -> None:
    """队列满时 send 阻塞，直到接收方取走数据。"""
    first, second = create_pair(InProcessPipe, maxsize=10)

    for i in range(10):
        await first.send(Payload(value=i))

    send_task = asyncio.create_task(first.send(Payload(value=100)))
    await asyncio.sleep(0.05)
    assert not send_task.done(), '第 11 个发送应该阻塞'

    received = await second.receive()
    assert isinstance(received, Payload)
    assert received.value == 0

    await asyncio.wait_for(send_task, timeout=1.0)


@pytest.mark.asyncio
async def test_backpressure_is_per_direction() -> None:
    """一个方向被填满不影响另一个方向继续发送。"""
    first, second = create_pair(InProcessPipe, maxsize=1)

    await first.send(Payload(value=1))
    blocked = asyncio.create_task(first.send(Payload(value=2)))
    await asyncio.sleep(0.05)
    assert not blocked.done()

    # 反方向不受影响
    await asyncio.wait_for(second.send(Payload(value=100)), timeout=1.0)
    reply = await first.receive()
    assert isinstance(reply, Payload)
    assert reply.value == 100

    blocked.cancel()


# ------------------------------------------------------------------------------
# InProcessPipeAllocator：生命周期
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_allocate_returns_local_endpoint() -> None:
    """allocate() 返回 (pipe_id, 分配方端点)，checkout() 返回借用方端点。"""
    allocator = InProcessPipeAllocator()

    pipe_id, client_pipe = await allocator.allocate()
    server_pipe = await allocator.checkout(pipe_id)
    assert isinstance(client_pipe, InProcessPipe)
    assert isinstance(server_pipe, InProcessPipe)
    assert client_pipe is not server_pipe

    await client_pipe.send(Payload(value=42))
    received = await server_pipe.receive()
    assert isinstance(received, Payload)
    assert received.value == 42

    await server_pipe.send(Payload(value=43))
    reply = await client_pipe.receive()
    assert isinstance(reply, Payload)
    assert reply.value == 43


@pytest.mark.asyncio
async def test_release_closes_both_endpoints() -> None:
    """release() 关闭两端端点并注销管道。"""
    allocator = InProcessPipeAllocator()
    pipe_id, client_pipe = await allocator.allocate()
    server_pipe = await allocator.checkout(pipe_id)
    assert server_pipe is not None

    await allocator.release(pipe_id)

    with pytest.raises(PipeClosedError):
        await client_pipe.send(Payload(value=1))
    with pytest.raises(PipeClosedError):
        await server_pipe.send(Payload(value=2))
    assert await allocator.checkout(pipe_id) is None


@pytest.mark.asyncio
async def test_release_is_idempotent() -> None:
    """重复 release 不抛异常。"""
    allocator = InProcessPipeAllocator()
    pipe_id, _ = await allocator.allocate()
    await allocator.release(pipe_id)
    await allocator.release(pipe_id)


@pytest.mark.asyncio
async def test_checkin_returns_peer_endpoint() -> None:
    """checkin() 只归还借用方端点，不影响后续 release。"""
    allocator = InProcessPipeAllocator()
    pipe_id, client_pipe = await allocator.allocate()
    assert await allocator.checkout(pipe_id) is not None

    await allocator.checkin(pipe_id)

    assert await allocator.checkout(pipe_id) is None
    # 对端已归还，分配方不能再写入
    with pytest.raises(PipeClosedError):
        await client_pipe.send(Payload(value=1))

    await allocator.release(pipe_id)  # 资源由分配方收尾


@pytest.mark.asyncio
async def test_checkin_is_idempotent() -> None:
    """重复 checkin 不抛异常。"""
    allocator = InProcessPipeAllocator()
    pipe_id, _ = await allocator.allocate()
    await allocator.checkin(pipe_id)
    await allocator.checkin(pipe_id)


@pytest.mark.asyncio
async def test_allocate_uses_configured_pipe_type() -> None:
    """构造参数 pipe_type 决定 allocate() 创建的端点类型。"""
    allocator = InProcessPipeAllocator(pipe_type=InProcessPipe)

    _, client_pipe = await allocator.allocate()
    assert isinstance(client_pipe, InProcessPipe)


@pytest.mark.asyncio
async def test_allocate_passes_kwargs_to_pipe() -> None:
    """allocate(**kwargs) 透传给端点构造（maxsize 生效）。"""
    allocator = InProcessPipeAllocator()

    pipe_id, client_pipe = await allocator.allocate(maxsize=1)
    server_pipe = await allocator.checkout(pipe_id)
    assert server_pipe is not None

    await client_pipe.send(Payload(value=1))
    blocked = asyncio.create_task(client_pipe.send(Payload(value=2)))
    await asyncio.sleep(0.05)
    assert not blocked.done(), 'maxsize=1 时第二个 send 应阻塞'

    await server_pipe.receive()
    await asyncio.wait_for(blocked, timeout=1.0)


@pytest.mark.asyncio
async def test_allocated_ids_are_unique() -> None:
    """每次 allocate() 返回唯一标识符。"""
    allocator = InProcessPipeAllocator()

    results = [await allocator.allocate() for _ in range(100)]
    assert len({pipe_id for pipe_id, _ in results}) == 100


@pytest.mark.asyncio
async def test_release_unknown_id_is_noop() -> None:
    """释放不存在的 pipe_id 不抛异常。"""
    allocator = InProcessPipeAllocator()
    await allocator.release('missing')


@pytest.mark.asyncio
async def test_checkin_unknown_id_is_noop() -> None:
    """归还不存在的 pipe_id 不抛异常。"""
    allocator = InProcessPipeAllocator()
    await allocator.checkin('missing')


@pytest.mark.asyncio
async def test_checkout_is_single_borrow() -> None:
    """同一个 pipe_id 只能被 checkout 一次。"""
    allocator = InProcessPipeAllocator()
    pipe_id, _ = await allocator.allocate()

    first = await allocator.checkout(pipe_id)
    assert first is not None
    assert await allocator.checkout(pipe_id) is None

    await allocator.checkin(pipe_id)
    assert await allocator.checkout(pipe_id) is None


@pytest.mark.asyncio
async def test_send_blocked_when_peer_closes() -> None:
    """队列满时对端 close 必须唤醒阻塞的 send 并抛错。"""
    first, second = create_pair(InProcessPipe, maxsize=1)
    await first.send(Payload(value=1))

    send_task = asyncio.create_task(first.send(Payload(value=2)))
    await asyncio.sleep(0.05)
    assert not send_task.done()

    await second.close()

    with pytest.raises(PipeClosedError):
        await asyncio.wait_for(send_task, timeout=1.0)


@pytest.mark.asyncio
async def test_cancelled_receive_does_not_consume_next_message() -> None:
    """取消 receive 后，后续消息仍应能被下一次 receive 取到。"""
    first, second = create_pair(InProcessPipe)

    recv_task = asyncio.create_task(second.receive())
    await asyncio.sleep(0.05)
    recv_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await recv_task

    await first.send(Payload(value=42))
    received = await asyncio.wait_for(second.receive(), timeout=0.5)
    assert received.value == 42
