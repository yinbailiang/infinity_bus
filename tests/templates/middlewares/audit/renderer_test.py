"""AuditRecordRenderer 渲染测试。"""

from datetime import datetime, timezone

from pydantic import BaseModel

from infinity_bus import Event
from infinity_bus.templates.middlewares.audit import AuditPayloadConfig, AuditRecordRenderer
from infinity_bus.templates.middlewares.audit.models import PendingError, PendingEvent


class _Payload(BaseModel):
    key: str = 'k'


def _event() -> Event:
    return Event(
        name='mw.ping',
        data=_Payload(),
        sources=['src'],
        timestamps=[datetime(2026, 1, 1, tzinfo=timezone.utc)],
        event_ids=['parent'],
    )


def test_render_digest_default() -> None:
    """默认 digest 模式只保留摘要。"""
    record = AuditRecordRenderer()(PendingEvent(_event()))
    assert record.data is None
    assert record.payload_sha256 is not None


def test_render_full_with_extra_fields() -> None:
    """full 模式与 extra_fields 由 Payload 配置控制。"""
    renderer = AuditRecordRenderer(AuditPayloadConfig(payload_mode='full', extra_fields={'env': 'test'}))
    record = renderer(PendingEvent(_event()))
    assert record.data is not None
    assert record.extra == {'env': 'test'}


def test_render_publish_error() -> None:
    """发布失败条目渲染为 ERROR 记录。"""
    renderer = AuditRecordRenderer()
    record = renderer(
        PendingError(
            error=ValueError('boom'),
            name='missing.event',
            source='src',
            data={'key': 'k'},
        )
    )
    assert record.error == 'ValueError: boom'
    assert record.event_id == 'ERROR'
