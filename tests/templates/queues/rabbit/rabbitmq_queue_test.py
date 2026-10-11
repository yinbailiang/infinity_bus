"""RabbitMQ fanout 跨进程 EventQueue 集成测试（需运行中的 RabbitMQ broker）。

标记 ``slow``：默认被 ``-m 'not slow'`` 排除；需要 broker 时用 ``-m slow`` 运行。
broker 不可达时模块自动 skip（避免 CI 因无 broker 失败）。
"""

import asyncio
from typing import Any, List, Literal, Optional

import pytest
from conftest import BusTestPayload, TestEventDecl

from infinity_bus import (
    Event,
    EventBus,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
)
from infinity_bus.templates.queues import RabbitFanoutQueue

pytestmark = pytest.mark.slow


async def _probe(url: str) -> bool:
    """廉价探测 broker：可连接则 True。"""
    import aio_pika

    try:
        conn = await aio_pika.connect(url, timeout=1.0)
        await conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope='module')
def rabbit_url() -> str:
    """broker URL；不可达则跳过整个模块。"""
    from infinity_bus.templates.queues.rabbit import URL

    if not asyncio.run(_probe(URL)):
        pytest.skip('RabbitMQ broker 不可用（请先启动 rabbitmq-server）')
    return URL


async def _purge(member_id: str, url: str) -> None:
    """删除成员持久队列（含积压），保证测试干净。"""
    import aio_pika

    from infinity_bus.templates.queues.rabbit import EXCHANGE

    conn = await aio_pika.connect(url)
    channel = await conn.channel()
    await channel.queue_delete(f'{EXCHANGE}.{member_id}')
    await conn.close()


def _registry() -> EventRegistry:
    reg = EventRegistry()
    reg.register(TestEventDecl)
    return reg


class PayloadSpy(EventHandler):
    """订阅 test.event，收集收到的负载（期待为 BusTestPayload 实例）。"""

    def __init__(self, tag: str) -> None:
        super().__init__(['test.event'])
        self.tag = tag
        self.received: List[BusTestPayload] = []

    async def handle(self, payload: Optional[Any], bus_proxy: Any, raw_event: Event) -> None:
        if isinstance(payload, BusTestPayload):
            self.received.append(payload)

    async def wait_count(self, n: int, timeout: float = 10.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while len(self.received) < n:
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError(f'timeout: 收到 {len(self.received)}，期望 {n}')
            await asyncio.sleep(0.01)


async def _member(
    member_id: str,
    url: str,
    strategy: Literal['restart', 'offline'] = 'restart',
) -> tuple[Any, Any, PayloadSpy]:
    """建一个成员（RabbitFanoutQueue + EventBus），消费就绪后返回 (q, bus, spy)。"""
    q = await RabbitFanoutQueue.create(member_id, url=url, registry=_registry(), strategy=strategy)
    spy = PayloadSpy(member_id)
    bus = EventBus(_registry(), EventHandlerRegistry(), queue=q)
    bus.proxy(member_id).handlers_registry.register(spy)
    await bus.start()
    await asyncio.sleep(0.3)  # 等 broker 侧订阅生效（无协议级就绪信号）
    return q, bus, spy


@pytest.mark.asyncio
async def test_inproc_fanout_and_rebuild(rabbit_url: str) -> None:
    """双成员互泛洪 + 自收；data 均重建为 BusTestPayload 实例（完美 Event）。"""
    for m in ('itA', 'itB'):
        await _purge(m, rabbit_url)
    qa, ba, sa = await _member('itA', rabbit_url)
    qb, bb, sb = await _member('itB', rabbit_url)
    try:
        await ba.proxy('itA').publish('test.event', {'value': 5, 'msg': 'x'})
        await bb.proxy('itB').publish('test.event', {'value': 6, 'msg': 'y'})
        await sa.wait_count(2)
        await sb.wait_count(2)
        assert all(isinstance(p, BusTestPayload) for p in [*sa.received, *sb.received])
        assert sorted(p.value for p in sa.received) == [5, 6]
        assert sorted(p.value for p in sb.received) == [5, 6]
    finally:
        await ba.stop()
        await bb.stop()
        await qa.close()
        await qb.close()
        for m in ('itA', 'itB'):
            await _purge(m, rabbit_url)


@pytest.mark.asyncio
async def test_restart_join_resume_replays(rabbit_url: str) -> None:
    """restart 策略：join 停消费（积压保留）→ resume 补投。"""
    m = 'rjA'
    await _purge(m, rabbit_url)
    q, bus, spy = await _member(m, rabbit_url)  # 默认 restart
    try:
        await bus.proxy('gen1').publish('test.event', {'value': 1})
        await spy.wait_count(1)
        await q.join()  # restart：停消费，保留路由与积压
        await bus.proxy('gen1').publish('test.event', {'value': 2})
        await asyncio.sleep(0.6)
        assert len(spy.received) == 1, 'join 期间不应收到新事件'
        await q.resume()  # 补投 value=2
        await spy.wait_count(2)
        await asyncio.sleep(0.3)
        assert len(spy.received) == 2
        assert spy.received[-1].value == 2
    finally:
        await bus.stop()
        await q.close()
        await _purge(m, rabbit_url)


@pytest.mark.asyncio
async def test_offline_join_only_new(rabbit_url: str) -> None:
    """offline 策略：join 停路由 + 消费已路由 → 离线期事件不进队列，resume 只收新。"""
    m = 'ojA'
    await _purge(m, rabbit_url)
    q, bus, spy = await _member(m, rabbit_url, strategy='offline')
    try:
        await bus.proxy('gen1').publish('test.event', {'value': 1})
        await spy.wait_count(1)
        await q.join()  # offline：unbind + 消费掉所有已路由
        await bus.proxy('gen1').publish('test.event', {'value': 2})
        await bus.proxy('gen1').publish('test.event', {'value': 3})
        await asyncio.sleep(0.6)
        assert len(spy.received) == 1, 'offline join 后事件不应再进队列'
        await q.resume()  # 重新 bind
        await asyncio.sleep(0.3)
        await bus.proxy('gen1').publish('test.event', {'value': 4})
        await spy.wait_count(2)
        await asyncio.sleep(0.3)
        assert len(spy.received) == 2
        assert spy.received[-1].value == 4
    finally:
        await bus.stop()
        await q.close()
        await _purge(m, rabbit_url)
