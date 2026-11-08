"""AuditWriterConfig / AuditPayloadConfig 参数校验测试。"""

import pytest
from pydantic import ValidationError

from infinity_bus.templates.middlewares.audit import AuditPayloadConfig, AuditWriterConfig


def test_writer_defaults() -> None:
    """Writer 默认配置。"""
    config = AuditWriterConfig()
    assert config.batch_size == 100
    assert config.flush_interval == 0.5
    assert config.queue_maxsize == 10_000
    assert config.fallback is None
    assert config.reconnect_backoff == 1.0
    assert config.reconnect_backoff_max == 30.0
    assert config.close_timeout == 5.0
    assert config.maintenance_interval == 60.0


def test_payload_defaults() -> None:
    """Payload 默认配置。"""
    config = AuditPayloadConfig()
    assert config.payload_mode == 'digest'
    assert config.payload_allowlist == frozenset()
    assert config.payload_max_bytes == 64 * 1024
    assert config.extra_fields == {}


def test_fallback_accepts_callable() -> None:
    """fallback 字段接受可调用对象。"""
    lines: list[str] = []
    config = AuditWriterConfig(fallback=lines.append)
    assert config.fallback is not None
    config.fallback('line')
    assert lines == ['line']


@pytest.mark.parametrize(
    ('field', 'value', 'match'),
    [
        ('batch_size', 0, 'batch_size'),
        ('flush_interval', -1, 'flush_interval'),
        ('queue_maxsize', 0, 'queue_maxsize'),
        ('reconnect_backoff', 0, 'reconnect_backoff'),
        ('close_timeout', 0, 'close_timeout'),
        ('maintenance_interval', 0, 'maintenance_interval'),
    ],
)
def test_writer_invalid_values(field: str, value: object, match: str) -> None:
    """Writer 配置在构造期拒绝非法值。"""
    with pytest.raises(ValidationError, match=match):
        AuditWriterConfig.model_validate({field: value})


@pytest.mark.parametrize(
    ('field', 'value', 'match'),
    [
        ('payload_max_bytes', 0, 'payload_max_bytes'),
        ('payload_mode', 'raw', 'payload_mode'),
    ],
)
def test_payload_invalid_values(field: str, value: object, match: str) -> None:
    """Payload 配置在构造期拒绝非法值。"""
    with pytest.raises(ValidationError, match=match):
        AuditPayloadConfig.model_validate({field: value})


def test_backoff_max_must_not_be_less_than_backoff() -> None:
    """退避上限不能小于首次退避。"""
    with pytest.raises(ValidationError, match='reconnect_backoff_max'):
        AuditWriterConfig(reconnect_backoff=1, reconnect_backoff_max=0.5)


def test_extra_fields_cannot_shadow_audit_fields() -> None:
    """extra_fields 不得与审计字段重名。"""
    with pytest.raises(ValidationError, match='重名'):
        AuditPayloadConfig(extra_fields={'data': 'x'})


def test_allowlist_is_coerced_to_frozenset() -> None:
    """白名单接受可迭代对象并固化为 frozenset。"""
    config = AuditPayloadConfig.model_validate({'payload_allowlist': ['audit.keep', 'audit.keep']})
    assert config.payload_allowlist == frozenset({'audit.keep'})


def test_optional_limits_accept_none() -> None:
    """payload_max_bytes 与 maintenance_interval 支持 None。"""
    payload = AuditPayloadConfig(payload_max_bytes=None)
    writer = AuditWriterConfig(maintenance_interval=None)
    assert payload.payload_max_bytes is None
    assert writer.maintenance_interval is None


def test_configs_are_frozen() -> None:
    """配置不可变，避免运行期被意外改写。"""
    with pytest.raises(ValidationError):
        setattr(AuditWriterConfig(), 'batch_size', 10)
    with pytest.raises(ValidationError):
        setattr(AuditPayloadConfig(), 'payload_mode', 'full')
