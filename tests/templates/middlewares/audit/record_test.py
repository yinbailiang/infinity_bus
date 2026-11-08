"""审计记录构建测试。"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import BaseModel

from infinity_bus import Event
from infinity_bus.templates.middlewares.audit import AuditRecord
from infinity_bus.templates.middlewares.audit.record import (
    ERROR_EVENT_ID,
    build_error_record,
    build_record,
    canonical_payload,
    canonicalize_payload,
    validate_extra_fields,
)


class _Payload(BaseModel):
    key: str
    count: int = 0


class _LoosePayload(BaseModel):
    value: Any = None


class _HugeStr:
    def __str__(self) -> str:
        return 'x' * 1000


def _event(
    *,
    name: str = 'mw.ping',
    payload: BaseModel | None = None,
    sources: list[str] | None = None,
    timestamps: list[datetime] | None = None,
    event_ids: list[str] | None = None,
) -> Event:
    return Event(
        name=name,
        data=payload,
        sources=['src'] if sources is None else sources,
        timestamps=[datetime(2026, 1, 1, tzinfo=timezone.utc)] if timestamps is None else timestamps,
        event_ids=['parent'] if event_ids is None else event_ids,
    )


def test_build_record_digest() -> None:
    """digest 模式只保留大小与摘要。"""
    payload = _Payload(key='k', count=1)
    event = _event(payload=payload)
    record = build_record(
        event,
        payload_mode='digest',
        payload_allowlist=frozenset(),
        payload_max_bytes=1024,
        extra_fields={'env': 'test'},
        now=100.0,
    )

    canonical = '{"count":1,"key":"k"}'
    assert record.ts == 100.0
    assert record.emitted_at == datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
    assert record.service == 'src'
    assert record.name == 'mw.ping'
    assert record.event_id == event.id
    assert record.event_ids == ['parent']
    assert record.sources == ['src']
    assert record.payload_size == len(canonical.encode('utf-8'))
    assert record.payload_sha256 == hashlib.sha256(canonical.encode('utf-8')).hexdigest()
    assert record.data is None
    assert record.extra == {'env': 'test'}


def test_build_record_full() -> None:
    """full 模式保留负载正文。"""
    record = build_record(
        _event(payload=_Payload(key='k')),
        payload_mode='full',
        payload_allowlist=frozenset(),
        payload_max_bytes=None,
        extra_fields={},
    )
    assert record.data == '{"count":0,"key":"k"}'


def test_allowlist_keeps_body_in_digest_mode() -> None:
    """白名单事件即使 digest 模式也保留正文。"""
    record = build_record(
        _event(name='audit.keep', payload=_Payload(key='k')),
        payload_mode='digest',
        payload_allowlist=frozenset({'audit.keep'}),
        payload_max_bytes=None,
        extra_fields={},
    )
    assert record.data is not None


def test_payload_over_limit_falls_back_to_digest() -> None:
    """超过上限时只保留大小与摘要。"""
    record = build_record(
        _event(payload=_Payload(key='x' * 100)),
        payload_mode='full',
        payload_allowlist=frozenset(),
        payload_max_bytes=10,
        extra_fields={},
    )
    assert record.data is None
    assert record.payload_size > 10
    assert record.payload_sha256 is not None


def test_build_record_without_payload() -> None:
    """无负载时大小与摘要为空。"""
    record = build_record(
        _event(payload=None),
        payload_mode='digest',
        payload_allowlist=frozenset(),
        payload_max_bytes=None,
        extra_fields={},
    )
    assert record.payload_size == 0
    assert record.payload_sha256 is None
    assert record.data is None
    assert record.payload_unrenderable == {}


def test_build_record_stores_unrenderable_fields_separately() -> None:
    """不可渲染字段替换为占位符，真实值进入独立字段。"""
    record = build_record(
        _event(payload=_LoosePayload(value=object())),
        payload_mode='full',
        payload_allowlist=frozenset(),
        payload_max_bytes=None,
        extra_fields={},
    )
    assert record.data is not None
    assert json.loads(record.data) == {'value': '<0>'}
    assert set(record.payload_unrenderable) == {'<0>'}
    assert 'object' in record.payload_unrenderable['<0>']


def test_digest_mode_does_not_store_unrenderable_fields() -> None:
    """digest 模式不保留正文，也不泄露不可渲染字段。"""
    record = build_record(
        _event(payload=_LoosePayload(value=object())),
        payload_mode='digest',
        payload_allowlist=frozenset(),
        payload_max_bytes=None,
        extra_fields={},
    )
    assert record.data is None
    assert record.payload_unrenderable == {}


def test_unrenderable_payload_over_limit_falls_back_to_digest() -> None:
    """上限同时约束旁路映射，超大不可渲染值不会落正文。"""
    record = build_record(
        _event(payload=_LoosePayload(value=_HugeStr())),
        payload_mode='full',
        payload_allowlist=frozenset(),
        payload_max_bytes=100,
        extra_fields={},
    )
    assert record.data is None
    assert record.payload_unrenderable == {}
    assert record.payload_sha256 is not None


def test_payload_digest_is_stable_for_unrenderable_values() -> None:
    """同一逻辑负载中的不可渲染值不改变摘要。"""
    first = build_record(
        _event(payload=_LoosePayload(value=object())),
        payload_mode='digest',
        payload_allowlist=frozenset(),
        payload_max_bytes=None,
        extra_fields={},
    )
    second = build_record(
        _event(payload=_LoosePayload(value=object())),
        payload_mode='digest',
        payload_allowlist=frozenset(),
        payload_max_bytes=None,
        extra_fields={},
    )
    assert first.payload_sha256 is not None
    assert first.payload_sha256 == second.payload_sha256


def test_build_error_record() -> None:
    """发布失败记录带 error 与 ERROR 事件 ID。"""
    record = build_error_record(
        ValueError('boom'),
        name='test.event',
        source='src',
        data={'key': 'v'},
        payload_mode='digest',
        payload_allowlist=frozenset(),
        payload_max_bytes=None,
        extra_fields={},
        now=1.0,
    )
    assert record.event_id == ERROR_EVENT_ID
    assert record.error == 'ValueError: boom'
    assert record.service == 'src'
    assert record.sources == ['src']
    assert record.payload_size > 0


def test_to_json_dict_flattens_extra() -> None:
    """JSONL 友好的平铺字典会展开 extra。"""
    record = AuditRecord(
        ts=1.0,
        service='src',
        name='e',
        event_id='id',
        extra={'trace_id': 'abc'},
    )
    row = record.to_json_dict()
    assert row['trace_id'] == 'abc'
    assert 'extra' not in row


def test_to_json_dict_rejects_collision() -> None:
    """extra 与审计字段重名时拒绝展开。"""
    record = AuditRecord(
        ts=1.0,
        service='src',
        name='e',
        event_id='id',
        extra={'name': 'shadow'},
    )
    with pytest.raises(ValueError, match='重名'):
        record.to_json_dict()


def test_validate_extra_fields_rejects_collision() -> None:
    """构造期即可拦截 extra 重名。"""
    with pytest.raises(ValueError, match='重名'):
        validate_extra_fields({'data': 'x'})


def test_canonical_payload_handles_base_model() -> None:
    """BaseModel 按 JSON 模式展开，``None`` 保持 ``None``。"""
    assert canonical_payload(_Payload(key='k')) == '{"count":0,"key":"k"}'
    assert canonical_payload(None) is None


def test_canonicalize_payload_is_stable_for_unrenderable_values() -> None:
    """占位符编号只取决于结构，与插入顺序和内存地址无关。"""
    first, _ = canonicalize_payload({'a': object(), 'b': [object()]})
    second, _ = canonicalize_payload({'b': [object()], 'a': object()})
    assert first == second == '{"a":"<0>","b":["<1>"]}'


def test_canonicalize_payload_breaks_cycles() -> None:
    """循环引用降级为占位符，规范化结果仍是合法 JSON。"""
    payload: dict[str, Any] = {'name': 'x'}
    payload['self'] = payload
    text, unrenderable = canonicalize_payload(payload)
    assert text is not None
    assert json.loads(text) == {'name': 'x', 'self': '<0>'}
    assert '<0>' in unrenderable
    assert 'dict' in unrenderable['<0>']
