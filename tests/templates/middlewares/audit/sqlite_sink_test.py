"""SQLite 审计存储后端测试。"""

import time
from typing import Any

import aiosqlite
import pytest
from pydantic import ValidationError

from infinity_bus.templates.middlewares.audit import AuditRecord, RetentionPolicy, SQLiteSink


def _record(
    *,
    name: str = 'mw.ping',
    ts: float | None = None,
    data: str | None = '{"a":1}',
    extra: dict[str, Any] | None = None,
) -> AuditRecord:
    return AuditRecord(
        ts=time.time() if ts is None else ts,
        emitted_at=1.0,
        service='src',
        name=name,
        event_id='event-1',
        event_ids=['event-1'],
        sources=['src'],
        timestamps=['2026-01-01T00:00:00+00:00'],
        payload_size=7,
        payload_sha256='abc',
        data=data,
        extra={} if extra is None else extra,
    )


async def _fetch_all(sink: SQLiteSink, sql: str) -> list[Any]:
    connection = sink._conn  # pyright: ignore[reportPrivateUsage]
    assert connection is not None
    cursor = await connection.execute(sql)
    return list(await cursor.fetchall())


async def _scalar(sink: SQLiteSink, sql: str) -> Any:
    rows = await _fetch_all(sink, sql)
    return rows[0][0]


@pytest.mark.asyncio
async def test_write_and_query(tmp_path) -> None:
    """批量写入后可按列查询。"""
    sink = SQLiteSink(str(tmp_path / 'audit.db'))
    await sink.open()
    await sink.write([_record(name='a'), _record(name='b')])
    rows = await _fetch_all(sink, 'SELECT name, data, payload_size FROM event_log ORDER BY id')
    assert [row['name'] for row in rows] == ['a', 'b']
    assert rows[0]['data'] == '{"a":1}'
    assert rows[0]['payload_size'] == 7
    assert sink.table_name == 'event_log'
    assert sink.db_path.endswith('audit.db')
    await sink.close()
    assert not sink.is_open


@pytest.mark.asyncio
async def test_old_table_is_migrated(tmp_path) -> None:
    """旧 event_log 表自动补齐审计列且保留历史行。"""
    path = str(tmp_path / 'old.db')
    connection = await aiosqlite.connect(path)
    await connection.execute(
        """\
CREATE TABLE event_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    sources     TEXT    NOT NULL DEFAULT '[]',
    data        TEXT,
    event_id    TEXT    NOT NULL,
    event_ids   TEXT    NOT NULL DEFAULT '[]',
    timestamps  TEXT    NOT NULL DEFAULT '[]'
)"""
    )
    await connection.execute("INSERT INTO event_log (name, data, event_id) VALUES ('old', '{}', 'old-id')")
    await connection.commit()
    await connection.close()

    sink = SQLiteSink(path)
    await sink.open()
    await sink.write([_record(name='new')])
    rows = await _fetch_all(sink, 'SELECT name FROM event_log ORDER BY id')
    assert [row['name'] for row in rows] == ['old', 'new']
    columns = await _fetch_all(sink, 'PRAGMA table_info(event_log)')
    assert 'payload_sha256' in {row['name'] for row in columns}
    await sink.close()


@pytest.mark.asyncio
async def test_retention_by_rows(tmp_path) -> None:
    """max_rows 只保留最新记录。"""
    sink = SQLiteSink(str(tmp_path / 'audit.db'), retention=RetentionPolicy(max_rows=2), prune_interval=0)
    await sink.open()
    await sink.write([_record(name=f'n{index}') for index in range(1, 6)])
    await sink.maintain()
    rows = await _fetch_all(sink, 'SELECT name FROM event_log ORDER BY id')
    assert [row['name'] for row in rows] == ['n4', 'n5']
    await sink.close()


@pytest.mark.asyncio
async def test_retention_by_seconds(tmp_path) -> None:
    """retention_seconds 删除过期记录。"""
    sink = SQLiteSink(
        str(tmp_path / 'audit.db'),
        retention=RetentionPolicy(retention_seconds=10),
        prune_interval=0,
    )
    await sink.open()
    await sink.write([_record(name='old', ts=1.0), _record(name='new', ts=time.time())])
    await sink.maintain()
    rows = await _fetch_all(sink, 'SELECT name FROM event_log ORDER BY id')
    assert [row['name'] for row in rows] == ['new']
    await sink.close()


@pytest.mark.asyncio
async def test_retention_by_bytes(tmp_path) -> None:
    """max_bytes 触发删除与 incremental_vacuum。"""
    sink = SQLiteSink(
        str(tmp_path / 'audit.db'),
        retention=RetentionPolicy(max_bytes=1),
        prune_interval=0,
        prune_batch=1000,
    )
    await sink.open()
    await sink.write([_record(name=f'n{index}', data='x' * 2000) for index in range(20)])
    await sink.maintain()
    assert await _scalar(sink, 'SELECT COUNT(*) FROM event_log') == 0
    await sink.close()


@pytest.mark.asyncio
async def test_extra_columns_are_populated(tmp_path) -> None:
    """extra_columns 的值从 record.extra 按列名写入。"""
    sink = SQLiteSink(str(tmp_path / 'audit.db'), extra_columns=['trace_id TEXT'])
    await sink.open()
    await sink.write([_record(extra={'trace_id': 'abc'}), _record(extra={'trace_id': {'nested': 1}})])
    rows = await _fetch_all(sink, 'SELECT trace_id FROM event_log ORDER BY id')
    assert rows[0]['trace_id'] == 'abc'
    assert rows[1]['trace_id'] == '{"nested": 1}'
    await sink.close()


@pytest.mark.asyncio
async def test_old_database_gets_incremental_vacuum(tmp_path) -> None:
    """旧库启用 max_bytes 时执行一次 VACUUM。"""
    path = str(tmp_path / 'old.db')
    connection = await aiosqlite.connect(path)
    await connection.execute('CREATE TABLE event_log (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)')
    await connection.commit()
    await connection.close()

    sink = SQLiteSink(path, retention=RetentionPolicy(max_bytes=1024 * 1024))
    await sink.open()
    assert int(await _scalar(sink, 'PRAGMA auto_vacuum')) == 2
    await sink.close()


@pytest.mark.asyncio
async def test_write_before_open_raises() -> None:
    """未打开时写入报错。"""
    sink = SQLiteSink(':memory:')
    with pytest.raises(RuntimeError, match='未打开'):
        await sink.write([_record()])


@pytest.mark.asyncio
async def test_empty_batch_and_idempotent_close(tmp_path) -> None:
    """空批次与重复关闭都是安全的。"""
    sink = SQLiteSink(str(tmp_path / 'audit.db'))
    await sink.open()
    await sink.write([])
    await sink.maintain()
    await sink.close()
    await sink.close()
    assert not sink.is_open


def test_invalid_configuration() -> None:
    """非法标识符与数值被拒绝。"""
    with pytest.raises(ValueError, match='table_name'):
        SQLiteSink(':memory:', table_name='bad name')
    with pytest.raises(ValueError, match='extra_columns'):
        SQLiteSink(':memory:', extra_columns=['1bad TEXT'])
    with pytest.raises(ValueError, match='重复'):
        SQLiteSink(':memory:', extra_columns=['trace_id TEXT', 'trace_id TEXT'])
    with pytest.raises(ValueError, match='重名'):
        SQLiteSink(':memory:', extra_columns=['data TEXT'])
    with pytest.raises(ValueError, match='prune_interval'):
        SQLiteSink(':memory:', prune_interval=-1)
    with pytest.raises(ValueError, match='prune_batch'):
        SQLiteSink(':memory:', prune_batch=0)


def test_retention_policy_validation() -> None:
    """保留策略拒绝非正数。"""
    with pytest.raises(ValidationError):
        RetentionPolicy(max_rows=0)
