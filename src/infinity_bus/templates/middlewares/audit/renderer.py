"""审计记录渲染：按 Payload 配置把待写入条目转换为 AuditRecord。"""

from .config import AuditPayloadConfig
from .models import Pending, PendingEvent
from .record import AuditRecord, build_error_record, build_record


class AuditRecordRenderer:
    """可调用渲染器：``Pending -> AuditRecord``。"""

    def __init__(self, config: AuditPayloadConfig | None = None) -> None:
        self._config = config or AuditPayloadConfig()

    @property
    def config(self) -> AuditPayloadConfig:
        """当前负载配置（不可变）。"""
        return self._config

    def __call__(self, item: Pending) -> AuditRecord:
        """把一条待写入条目渲染为审计记录。"""
        config = self._config
        if isinstance(item, PendingEvent):
            return build_record(
                item.event,
                payload_mode=config.payload_mode,
                payload_allowlist=config.payload_allowlist,
                payload_max_bytes=config.payload_max_bytes,
                extra_fields=config.extra_fields,
            )
        return build_error_record(
            item.error,
            name=item.name,
            source=item.source,
            data=item.data,
            payload_mode=config.payload_mode,
            payload_allowlist=config.payload_allowlist,
            payload_max_bytes=config.payload_max_bytes,
            extra_fields=config.extra_fields,
        )
