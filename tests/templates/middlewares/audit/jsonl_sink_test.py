"""JSONL 审计存储后端测试。"""

import asyncio
import json

import pytest

from infinity_bus.templates.middlewares.audit import AuditRecord, JSONLSink


def _record(*, name: str = 'mw.ping', extra: dict[str, object] | None = None) -> AuditRecord:
    return AuditRecord(
        ts=1.0,
        emitted_at=1.0,
        service='src',
        name=name,
        event_id='event-1',
        event_ids=['event-1'],
        sources=['src'],
        timestamps=['2026-01-01T00:00:00+00:00'],
        payload_size=7,
        payload_sha256='abc',
        data='{"a":1}',
        extra={} if extra is None else extra,
    )


def _read_lines(path: str) -> list[str]:
    with open(path, encoding='utf-8') as handle:
        return handle.readlines()


@pytest.mark.asyncio
async def test_write_and_read(tmp_path) -> None:
    """记录按行追加并平铺 extra。"""
    path = str(tmp_path / 'audit.jsonl')
    sink = JSONLSink(path)
    await sink.open()
    await sink.write([_record(), _record(name='other', extra={'trace_id': 'abc'})])
    await sink.close()

    lines = await asyncio.to_thread(_read_lines, path)
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first['name'] == 'mw.ping'
    assert first['service'] == 'src'
    assert first['payload_size'] == 7
    second = json.loads(lines[1])
    assert second['trace_id'] == 'abc'


@pytest.mark.asyncio
async def test_append_across_reopen(tmp_path) -> None:
    """关闭后重新打开保持追加语义。"""
    path = str(tmp_path / 'audit.jsonl')
    sink = JSONLSink(path)
    await sink.open()
    await sink.write([_record()])
    await sink.close()
    await sink.open()
    await sink.write([_record(name='second')])
    await sink.close()

    lines = await asyncio.to_thread(_read_lines, path)
    assert [json.loads(line)['name'] for line in lines] == ['mw.ping', 'second']


@pytest.mark.asyncio
async def test_write_before_open_raises(tmp_path) -> None:
    """未打开时写入直接报错，交给中间件降级。"""
    sink = JSONLSink(str(tmp_path / 'audit.jsonl'))
    with pytest.raises(RuntimeError, match='未打开'):
        await sink.write([_record()])


@pytest.mark.asyncio
async def test_open_and_close_are_idempotent(tmp_path) -> None:
    """重复打开 / 关闭是安全的。"""
    sink = JSONLSink(str(tmp_path / 'audit.jsonl'))
    await sink.open()
    await sink.open()
    assert sink.is_open
    await sink.close()
    await sink.close()
    assert not sink.is_open
    assert sink.file_path.endswith('audit.jsonl')
