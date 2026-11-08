"""幂等 recorder 测试：InMemory / Sqlite 持久共享 + IdempotentHandler 注入语义。"""

import asyncio
from pathlib import Path
from typing import Any

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

    async def handle(self, payload: BaseModel | None, bus_proxy: Any, raw_event: Event) -> None:
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


# ============================================================================
# Sqlite 记录器的运维属性：写放大与无限增长
# ============================================================================


def _rows(db_path: Path) -> list[tuple[str, str, str]]:
    """绕过被测代码直接读库（校验落库效果）。"""
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        return [
            (str(a), str(b), str(c)) for a, b, c in conn.execute('SELECT consumer, event_id, ts FROM processed_log')
        ]
    finally:
        conn.close()


def _insert(db_path: Path, consumer: str, event_id: str, ts: str) -> None:
    """按表结构手工写一行（模拟历史数据）。"""
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            'CREATE TABLE IF NOT EXISTS processed_log ('
            ' consumer TEXT NOT NULL, event_id TEXT NOT NULL, ts TEXT NOT NULL,'
            ' PRIMARY KEY (consumer, event_id))'
        )
        conn.execute('INSERT OR IGNORE INTO processed_log VALUES (?, ?, ?)', (consumer, event_id, ts))
        conn.commit()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_sqlite_recorder_enables_wal(tmp_path: Path) -> None:
    """连接开 WAL：默认 rollback journal 每次 mark 都要 fsync，等于在派发路径上插 fsync。"""
    import sqlite3

    db = tmp_path / 'dedupe.sqlite'
    recorder = SqliteIdempotencyRecorder(str(db))
    await recorder.start()
    try:
        conn = sqlite3.connect(str(db))
        try:
            mode = conn.execute('PRAGMA journal_mode').fetchone()
        finally:
            conn.close()
    finally:
        await recorder.close()

    assert mode is not None
    assert str(mode[0]).lower() == 'wal'


@pytest.mark.asyncio
async def test_sqlite_recorder_prunes_expired_rows_on_start(tmp_path: Path) -> None:
    """启动时按保留期清理：这张表不清就是只增不减。"""
    from datetime import datetime, timedelta, timezone

    db = tmp_path / 'dedupe.sqlite'
    now = datetime.now(timezone.utc)
    _insert(db, 'c', 'ancient', (now - timedelta(days=30)).isoformat())
    _insert(db, 'c', 'fresh', (now - timedelta(minutes=5)).isoformat())

    recorder = SqliteIdempotencyRecorder(str(db), retention_seconds=7 * 24 * 3600.0)
    await recorder.start()
    await recorder.close()

    assert [row[1] for row in _rows(db)] == ['fresh']


@pytest.mark.asyncio
async def test_sqlite_recorder_sweeps_while_writing(tmp_path: Path) -> None:
    """长跑进程也要清：按巡检间隔在写路径上顺带清理。"""
    db = tmp_path / 'dedupe.sqlite'
    recorder = SqliteIdempotencyRecorder(str(db), retention_seconds=60.0, sweep_interval_seconds=0.0)
    await recorder.start()
    try:
        _insert(db, 'c', 'ancient', '2000-01-01T00:00:00+00:00')
        await recorder.mark_processed('c', 'fresh')
    finally:
        await recorder.close()

    assert sorted(row[1] for row in _rows(db)) == ['fresh']


@pytest.mark.asyncio
async def test_sqlite_recorder_prune_is_public_and_counts(tmp_path: Path) -> None:
    """``prune()`` 可直接调用并返回删除条数（运维手动清库用）。"""
    from datetime import datetime, timedelta, timezone

    db = tmp_path / 'dedupe.sqlite'
    now = datetime.now(timezone.utc)
    recorder = SqliteIdempotencyRecorder(str(db), retention_seconds=7 * 24 * 3600.0)
    await recorder.start()
    try:
        # start 已经清过一次，这里再造过期数据来验证显式调用
        _insert(db, 'c', 'old-1', (now - timedelta(days=30)).isoformat())
        _insert(db, 'c', 'old-2', (now - timedelta(days=20)).isoformat())
        await recorder.mark_processed('c', 'new')

        assert await recorder.prune() == 2
        assert await recorder.is_processed('c', 'new') is True
    finally:
        await recorder.close()


@pytest.mark.asyncio
async def test_sqlite_recorder_retention_zero_keeps_everything(tmp_path: Path) -> None:
    """``retention_seconds=0`` 关掉清理（需要永久审计的场景）。"""
    from datetime import datetime, timedelta, timezone

    db = tmp_path / 'dedupe.sqlite'
    old = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()
    _insert(db, 'c', 'ancient', old)

    recorder = SqliteIdempotencyRecorder(str(db), retention_seconds=0)
    await recorder.start()
    try:
        assert [row[1] for row in _rows(db)] == ['ancient']
        assert await recorder.prune() == 0
    finally:
        await recorder.close()


# ============================================================================
# 原子认领（claim）
# ============================================================================


@pytest.mark.asyncio
async def test_inmemory_claim_is_atomic() -> None:
    """claim：首次成功，重复返回 False；与 mark_processed 共享同一去重表。"""
    r = InMemoryIdempotencyRecorder()
    assert await r.claim('c', 'e1') is True
    assert await r.claim('c', 'e1') is False
    assert await r.is_processed('c', 'e1')
    await r.mark_processed('c', 'e2')
    assert await r.claim('c', 'e2') is False

    await r.release('c', 'e1')
    assert await r.claim('c', 'e1') is True, '撤回认领后重投应能重新派发'


@pytest.mark.asyncio
async def test_sqlite_claim_is_atomic(tmp_path: Path) -> None:
    """claim：INSERT OR IGNORE + rowcount 一步完成；重复与跨 consumer 语义正确。"""
    db = str(tmp_path / 'idem.db')
    r = SqliteIdempotencyRecorder(db)
    await r.start()
    try:
        assert await r.claim('c', 'e1') is True
        assert await r.claim('c', 'e1') is False
        assert await r.is_processed('c', 'e1')
        await r.mark_processed('c', 'e2')
        assert await r.claim('c', 'e2') is False
        assert await r.claim('other', 'e1') is True

        await r.release('c', 'e1')
        assert await r.is_processed('c', 'e1') is False
        assert await r.claim('c', 'e1') is True, '撤回认领后重投应能重新派发'
    finally:
        await r.close()


# ============================================================================
# InMemory recorder 的有界性
# ============================================================================


@pytest.mark.asyncio
async def test_inmemory_recorder_is_bounded() -> None:
    """容量上限：超出后按插入顺序淘汰最旧记录，不会无界增长。"""
    r = InMemoryIdempotencyRecorder(max_entries=2)
    assert await r.claim('c', 'e1') is True
    assert await r.claim('c', 'e2') is True
    assert await r.claim('c', 'e3') is True

    assert await r.is_processed('c', 'e1') is False, '最旧记录应被淘汰'
    assert await r.is_processed('c', 'e2') is True
    assert await r.is_processed('c', 'e3') is True


@pytest.mark.asyncio
async def test_inmemory_recorder_ttl_expires() -> None:
    """TTL：过期记录在读写路径上被惰性清理。"""
    r = InMemoryIdempotencyRecorder(ttl_seconds=0.05)
    assert await r.claim('c', 'e1') is True
    await asyncio.sleep(0.2)  # Windows 事件循环时钟分辨率约 15.6ms，留足余量避免 flaky

    assert await r.is_processed('c', 'e1') is False
    assert await r.claim('c', 'e1') is True, '过期后重投应能重新认领'


@pytest.mark.asyncio
async def test_inmemory_recorder_unbounded_when_zero() -> None:
    """max_entries=0 保留无界语义（显式选择）。"""
    r = InMemoryIdempotencyRecorder(max_entries=0)
    for i in range(100):
        assert await r.claim('c', f'e{i}') is True

    for i in range(100):
        assert await r.is_processed('c', f'e{i}')


def test_inmemory_recorder_rejects_negative_limits() -> None:
    """负数参数当场报错，而不是留下隐形的错误配置。"""
    with pytest.raises(ValueError, match='max_entries'):
        InMemoryIdempotencyRecorder(max_entries=-1)
    with pytest.raises(ValueError, match='ttl_seconds'):
        InMemoryIdempotencyRecorder(ttl_seconds=-1)


@pytest.mark.asyncio
async def test_sqlite_recorder_indexes_ts(tmp_path: Path) -> None:
    """prune 依赖 ts 索引，避免每次清理全表扫描。"""
    import sqlite3

    db = tmp_path / 'dedupe.sqlite'
    recorder = SqliteIdempotencyRecorder(str(db))
    await recorder.start()
    try:
        conn = sqlite3.connect(str(db))
        try:
            names = {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        finally:
            conn.close()
    finally:
        await recorder.close()

    assert 'idx_processed_log_ts' in names
