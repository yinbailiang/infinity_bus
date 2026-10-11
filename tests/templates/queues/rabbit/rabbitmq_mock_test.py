"""RabbitFanoutQueue 单元测试（mock aio-pika 对象，无需 broker）。

用 AsyncMock 伪造 exchange/channel/queue/connection，直接实例化
``RabbitFanoutQueue`` 验证其控制逻辑（put/get/ack/join 双策略/resume/close），
让无 broker 的默认跑批也能覆盖 rabbit 后端（CI 与 pre-commit 覆盖率门禁）。
真实 broker 集成由 ``rabbitmq_queue_test.py``（slow）覆盖。
"""

import asyncio
from types import SimpleNamespace
from typing import Any, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import BusTestPayload, TestEventDecl

import infinity_bus.templates.queues.rabbit.queue as rabbit_queue
from infinity_bus import Event, EventRegistry
from infinity_bus.templates.queues import EventCodec
from infinity_bus.templates.queues.rabbit.queue import RabbitFanoutQueue

pytestmark = pytest.mark.asyncio


def _registry() -> EventRegistry:
    reg = EventRegistry()
    reg.register(TestEventDecl)
    return reg


def _make_q(strategy: str = 'restart') -> Tuple[RabbitFanoutQueue, Any, Any, Any, Any]:
    """构造注入 fake 的队列实例：返回 (q, exchange, queue, channel, conn)。"""
    codec = EventCodec(_registry())
    exchange: Any = AsyncMock()
    queue: Any = AsyncMock()
    queue.consume.return_value = 'tag-1'
    channel: Any = AsyncMock()
    qobj: Any = MagicMock()
    qobj.declaration_result = SimpleNamespace(message_count=0)
    channel.get_queue = AsyncMock(return_value=qobj)
    conn: Any = AsyncMock()
    q = RabbitFanoutQueue(conn, channel, queue, exchange, 'member', codec, strategy=strategy)  # type: ignore[arg-type]
    q._consumer_tag = 'tag-1'
    q._consuming = True
    return q, exchange, queue, channel, conn


def _payload_event() -> Event:
    return Event(name='test.event', data=BusTestPayload(value=1), sources=['s'])


async def test_put_publishes_encoded_message() -> None:
    """put 经 exchange.publish 发送 codec.encode(event) 的持久消息。"""
    q, exchange, _, _, _ = _make_q()
    ev = _payload_event()
    await q.put(ev)
    exchange.publish.assert_awaited_once()
    message = exchange.publish.await_args.args[0]
    assert message.body == q._codec.encode(ev)


async def test_get_moves_message_to_unacked() -> None:
    """get 从 inbound 取事件并把 message 移入 unacked。"""
    q, _, _, _, _ = _make_q()
    msg: Any = AsyncMock()
    ev = _payload_event()
    await q._inbound.put((msg, ev))
    got = await q.get()
    assert got is ev
    assert q._unacked == [msg]


async def test_task_done_acks_message() -> None:
    """task_done 对 unacked 队首 message 发送 basic_ack。"""
    q, _, _, _, _ = _make_q()
    msg: Any = AsyncMock()
    q._unacked.append(msg)
    q.task_done()
    await asyncio.sleep(0)  # 让 create_task(_ack) 跑完
    msg.ack.assert_awaited_once()
    assert q._unacked == []


async def test_task_done_empty_is_noop() -> None:
    """unacked 为空时 task_done 无副作用。"""
    q, _, _, _, _ = _make_q()
    q.task_done()  # 不抛即可


async def test_ack_error_is_silent() -> None:
    """ack 失败静默（由 broker 超时/重投兜底）。"""
    q, _, _, _, _ = _make_q()
    msg: Any = AsyncMock()
    msg.ack.side_effect = RuntimeError('connection lost')
    await q._ack(msg)  # 不抛即可


async def test_join_restart_cancels_consumer_only() -> None:
    """restart join：取消消费、保留绑定（_bound 仍 True）。"""
    q, _, queue, _, _ = _make_q('restart')
    await q.join()
    queue.cancel.assert_awaited_once_with('tag-1')
    assert q._consuming is False
    assert q._bound is True
    assert q.qsize() == 0


async def test_join_offline_unbinds_and_cancels() -> None:
    """offline join：先 unbind（停路由）→ 排空（backlog 0）→ 取消消费。"""
    q, _, queue, channel, _ = _make_q('offline')
    await q.join()
    queue.unbind.assert_awaited_once()
    channel.get_queue.assert_awaited()  # drain_all 被动查询积压
    queue.cancel.assert_awaited_once_with('tag-1')
    assert q._bound is False
    assert q._consuming is False


async def test_join_offline_drains_positive_backlog(monkeypatch) -> None:
    """offline join：broker 积压 > 0 时 drain_all 循环直至归零。"""
    q, _, queue, _, _ = _make_q('offline')
    backlog = AsyncMock(side_effect=[1, 0])
    monkeypatch.setattr(q, '_broker_backlog', backlog)
    await q.join()
    assert backlog.await_count >= 2
    queue.unbind.assert_awaited_once()
    queue.cancel.assert_awaited_once()


async def test_resume_restart_reconsumes() -> None:
    """restart resume：重新 consume（保留绑定，直接补投）。"""
    q, _, queue, _, _ = _make_q('restart')
    queue.consume.return_value = 'tag-2'
    await q.join()
    await q.resume()
    queue.consume.assert_awaited_with(q._on_message, no_ack=False)
    assert q._consuming is True
    assert q._consumer_tag == 'tag-2'


async def test_resume_offline_rebinds_then_consumes() -> None:
    """offline resume：先重新 bind（只收新）再 consume。"""
    q, _, queue, _, _ = _make_q('offline')
    await q.join()
    await q.resume()
    queue.bind.assert_awaited_once()
    queue.consume.assert_awaited()
    assert q._bound is True
    assert q._consuming is True


async def test_close_cancels_and_closes_connection() -> None:
    """close：取消消费并关闭连接；重复调用幂等。"""
    q, _, queue, _, conn = _make_q()
    await q.close()
    queue.cancel.assert_awaited_once_with('tag-1')
    conn.close.assert_awaited_once()
    await q.close()  # 幂等：不再 cancel/close


async def test_create_requires_aio_pika(monkeypatch) -> None:
    """aio-pika 缺失时 create 抛 ImportError 提示。"""
    monkeypatch.setattr(rabbit_queue, '_aio_pika', None)
    with pytest.raises(ImportError, match='aio-pika'):
        await RabbitFanoutQueue.create('member')


async def test_on_message_decodes_into_inbound() -> None:
    """订阅回调：解码（codec）为完美 Event 后入 inbound。"""
    q, _, _, _, _ = _make_q()
    ev = _payload_event()
    msg: Any = AsyncMock()
    msg.body = q._codec.encode(ev)
    await q._on_message(msg)
    got_msg, got_event = q._inbound.get_nowait()
    assert got_msg is msg
    assert isinstance(got_event.data, BusTestPayload)
    assert got_event.data.value == 1


async def test_qsize_counts_local_backlog() -> None:
    """qsize = inbound + unacked。"""
    q, _, _, _, _ = _make_q()
    msg: Any = AsyncMock()
    ev = _payload_event()
    await q._inbound.put((msg, ev))
    q._unacked.append(AsyncMock())
    assert q.qsize() == 2


# -- 异常分支 / 正常 create 路径补测 --------------------------------------


async def test_join_offline_unbind_error_is_silent() -> None:
    """offline join：unbind 抛错静默（继续排空与取消消费）。"""
    q, _, queue, _, _ = _make_q('offline')
    queue.unbind.side_effect = RuntimeError('no route')
    await q.join()
    assert q._bound is False
    assert q._consuming is False


async def test_cancel_consumer_error_is_silent() -> None:
    """cancel 抛错静默（_cancel_consumer 幂等容错）。"""
    q, _, queue, _, _ = _make_q()
    queue.cancel.side_effect = RuntimeError('channel closed')
    await q.join()  # restart：cancel → drain
    assert q._consuming is False
    assert q._bound is True


async def test_close_connection_error_is_silent() -> None:
    """close：conn.close 抛错静默。"""
    q, _, _, _, conn = _make_q()
    conn.close.side_effect = RuntimeError('gone')
    await q.close()


async def test_broker_backlog_error_returns_zero() -> None:
    """passive 查询抛错 → backlog 视为 0（排空可收敛）。"""
    q, _, _, channel, _ = _make_q('offline')
    channel.get_queue.side_effect = RuntimeError('channel closed')
    assert await q._broker_backlog() == 0


async def test_drain_local_waits_until_empty(monkeypatch) -> None:
    """_drain_local：本地有积压时轮询直至清空（覆盖 while 轮询路径）。"""
    q, _, _, _, _ = _make_q()
    inbound: Any = MagicMock()
    inbound.qsize.side_effect = [1, 0]
    monkeypatch.setattr(q, '_inbound', inbound)
    await q._drain_local()
    assert inbound.qsize.call_count >= 2


async def test_create_success_with_mock_driver(monkeypatch) -> None:
    """create 正常路径：fake aio-pika 驱动覆盖连接/声明/订阅。"""
    fake_driver: Any = MagicMock()
    conn: Any = AsyncMock()
    channel: Any = AsyncMock()
    exchange: Any = AsyncMock()
    queue: Any = AsyncMock()
    queue.bind = AsyncMock()
    queue.consume = AsyncMock(return_value='tag-9')
    channel.set_qos = AsyncMock()
    channel.declare_exchange = AsyncMock(return_value=exchange)
    channel.declare_queue = AsyncMock(return_value=queue)
    conn.channel = AsyncMock(return_value=channel)
    fake_driver.connect_robust = AsyncMock(return_value=conn)
    fake_driver.ExchangeType = SimpleNamespace(FANOUT='fanout')
    fake_driver.DeliveryMode = SimpleNamespace(PERSISTENT=2)
    fake_driver.Message = type('FakeMessage', (), {})
    monkeypatch.setattr(rabbit_queue, '_aio_pika', fake_driver)
    q = await RabbitFanoutQueue.create('member', url='amqp://x', registry=_registry())
    assert q.member_id == 'member'
    assert q._consumer_tag == 'tag-9'
    assert q._consuming is True
    channel.set_qos.assert_awaited_once()
    queue.bind.assert_awaited_once()
    fake_driver.connect_robust.assert_awaited_once_with('amqp://x')
