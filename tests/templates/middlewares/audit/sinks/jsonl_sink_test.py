"""JSONL 审计存储后端测试。"""

import asyncio
import json

import pytest

from infinity_bus.templates.middlewares.audit import AuditRecord
from infinity_bus.templates.middlewares.audit.sinks import JSONLSink


def _record(
    *,
    name: str = 'mw.ping',
    extra: dict[str, object] | None = None,
    payload_unrenderable: dict[str, str] | None = None,
) -> AuditRecord:
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
        payload_unrenderable={} if payload_unrenderable is None else payload_unrenderable,
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


@pytest.mark.asyncio
async def test_payload_unrenderable_is_written_as_top_level_key(tmp_path) -> None:
    """不可渲染字段作为独立顶层键写入，不混入 data。"""
    path = str(tmp_path / 'audit.jsonl')
    sink = JSONLSink(path)
    await sink.open()
    await sink.write([_record(payload_unrenderable={'<0>': 'builtins.object: repr'})])
    await sink.close()

    lines = await asyncio.to_thread(_read_lines, path)
    row = json.loads(lines[0])
    assert row['payload_unrenderable'] == {'<0>': 'builtins.object: repr'}
    assert row['data'] == '{"a":1}'


@pytest.mark.asyncio
async def test_non_json_extra_is_stringified(tmp_path) -> None:
    """不可 JSON 化的 extra 值由 default=str 兜底，不导致写入失败。"""
    path = str(tmp_path / 'audit.jsonl')
    sink = JSONLSink(path)
    await sink.open()
    await sink.write([_record(extra={'obj': object()})])
    await sink.close()

    lines = await asyncio.to_thread(_read_lines, path)
    assert isinstance(json.loads(lines[0])['obj'], str)


@pytest.mark.asyncio
async def test_rotation_by_size_and_backup_count(tmp_path) -> None:
    """超过 max_bytes 时轮转，并只保留 backup_count 个历史文件。"""
    path = str(tmp_path / 'audit.jsonl')
    sink = JSONLSink(path, max_bytes=1, backup_count=1)
    await sink.open()
    await sink.write([_record(name='first')])
    await sink.write([_record(name='second')])
    await sink.write([_record(name='third')])
    await sink.close()

    active = await asyncio.to_thread(_read_lines, path)
    backup = await asyncio.to_thread(_read_lines, f'{path}.1')
    assert [json.loads(line)['name'] for line in active] == ['third']
    assert [json.loads(line)['name'] for line in backup] == ['second']
    assert not (tmp_path / 'audit.jsonl.2').exists()


@pytest.mark.asyncio
async def test_maintain_rotates_full_file(tmp_path) -> None:
    """maintain 在当前文件达到上限时轮转。"""
    path = str(tmp_path / 'audit.jsonl')
    sink = JSONLSink(path, max_bytes=1, backup_count=1)
    await sink.open()
    await sink.write([_record(name='first')])
    await sink.maintain()
    await sink.close()

    active = await asyncio.to_thread(_read_lines, path)
    backup = await asyncio.to_thread(_read_lines, f'{path}.1')
    assert active == []
    assert [json.loads(line)['name'] for line in backup] == ['first']


@pytest.mark.asyncio
async def test_reclaim_forces_rotation(tmp_path) -> None:
    """reclaim 跳过大小阈值，强制轮转当前文件。"""
    path = str(tmp_path / 'audit.jsonl')
    sink = JSONLSink(path, max_bytes=10_000, backup_count=1)
    await sink.open()
    await sink.write([_record(name='first')])
    await sink.reclaim()
    await sink.close()

    active = await asyncio.to_thread(_read_lines, path)
    backup = await asyncio.to_thread(_read_lines, f'{path}.1')
    assert active == []
    assert [json.loads(line)['name'] for line in backup] == ['first']


def test_invalid_rotation_config() -> None:
    """非法轮转参数被拒绝。"""
    with pytest.raises(ValueError, match='max_bytes'):
        JSONLSink('audit.jsonl', max_bytes=0)
    with pytest.raises(ValueError, match='backup_count'):
        JSONLSink('audit.jsonl', backup_count=-1)
