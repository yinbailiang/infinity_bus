"""幂等 recorder 测试：InMemory / Sqlite 持久共享 + IdempotentHandler 注入语义。"""

import asyncio
from typing import Any, Optional

import pytest
from conftest import BusTestPayload, TestEventDecl
from pydantic import BaseModel

from infinity_bus import (
    Event,
    EventBus,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
)
from infinity_bus.templates.idempotency import (
    IdempotentHandler,
    InMemoryIdempotencyRecorder,
    SqliteIdempotencyRecorder,
)


class IdemCount(IdempotentHandler):
    """注入 recorder 的 test.event 计数器；fail=True 时处理抛错。"""

    def __init__(self, recorder: Any, consumer: str, *, fail: bool = False) -> None:
        super().__init__(['test.event'], recorder, consumer)
        self.count = 0
        self._fail = fail
        self._tried = asyncio.Event()

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        self._tried.set()
        if self._fail:
            raise RuntimeError('boom')
        self.count += 1


def _registry() -> EventRegistry:
    reg = EventRegistry()
    reg.register(TestEventDecl)
    return reg


async def _wait_until(pred: Any, timeout: float = 3.0) -> None:
    """轮询等待条件成立。"""
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError('wait timeout')
        await asyncio.sleep(0.01)


def test_inmemory_mark_and_query() -> None:
    """InMemory recorder：mark 后可查到，未 mark 查不到。"""
    r = InMemoryIdempotencyRecorder()

    async def _go() -> None:
        assert not await r.is_processed('c', 'e1')
        await r.mark_processed('c', 'e1')
        assert await r.is_processed('c', 'e1')
        assert not await r.is_processed('other', 'e1')  # consumer 维度隔离

    asyncio.run(_go())


@pytest.mark.asyncio
async def test_sqlite_shared_across_instances(tmp_path) -> None:
    """Sqlite recorder：跨实例（模拟重启）共享已处理标记。"""
    db = str(tmp_path / 'idem.db')
    r1 = SqliteIdempotencyRecorder(db)
    await r1.start()
    await r1.mark_processed('consumer-C', 'evt-X')
    await r1.close()

    r2 = SqliteIdempotencyRecorder(db)  # 重启后重建（同一文件）
    await r2.start()
    assert await r2.is_processed('consumer-C', 'evt-X')
    assert not await r2.is_processed('consumer-C', 'evt-Y')
    await r2.close()


@pytest.mark.asyncio
async def test_handler_dedups_same_event_id() -> None:
    """同 id 重复投递：IdempotentHandler 只执行一次。"""
    reg = _registry()
    hreg = EventHandlerRegistry()
    recorder = InMemoryIdempotencyRecorder()
    handler = IdemCount(recorder, 'consumer-A')
    await hreg.register(handler)
    q = InMemoryEventQueue()
    bus = EventBus(reg, hreg, queue=q)
    await bus.start()
    try:
        ev = Event(name='test.event', data=BusTestPayload(value=1))
        await q.put(ev)
        await q.put(ev.model_copy())  # 同 id 副本（模拟 at-least-once 重投）
        await _wait_until(lambda: handler.count >= 1)
        await asyncio.sleep(0.05)  # 静默余量：确认副本确实被丢弃
        assert handler.count == 1
    finally:
        await bus.stop()


@pytest.mark.asyncio
async def test_handler_failure_is_not_marked() -> None:
    """handle 抛错不标记：失败项保留给 at-least-once 重投重试。"""
    reg = _registry()
    hreg = EventHandlerRegistry()
    recorder = InMemoryIdempotencyRecorder()
    handler = IdemCount(recorder, 'consumer-A', fail=True)
    await hreg.register(handler)
    q = InMemoryEventQueue()
    bus = EventBus(reg, hreg, queue=q)
    await bus.start()
    try:
        ev = Event(name='test.event', data=BusTestPayload(value=1))
        await q.put(ev)
        await _wait_until(lambda: handler._tried.is_set())
        await asyncio.sleep(0.05)
        assert not await recorder.is_processed('consumer-A', ev.id)
    finally:
        await bus.stop()
