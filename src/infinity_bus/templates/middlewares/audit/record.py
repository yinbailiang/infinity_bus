"""审计记录模型与规范化构建。"""

import hashlib
import json
import time
from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field

from infinity_bus import Event

PayloadMode = Literal['digest', 'full']
"""负载记录模式：``digest`` 只存大小与摘要，``full`` 额外存正文。"""

ERROR_EVENT_ID = 'ERROR'
"""发布失败记录使用的 ``event_id`` 占位值。"""


class AuditRecord(BaseModel):
    """一条与存储后端无关的审计记录。"""

    ts: float = Field(description='记录构建时刻（Unix 秒）')
    emitted_at: float | None = Field(default=None, description='事件最后一个时间戳（Unix 秒）')
    service: str = Field(description='发射者（事件来源链末端）')
    name: str = Field(description='事件名称')
    event_id: str = Field(description='事件 ID；发布失败记录为 ``ERROR``')
    event_ids: list[str] = Field(default_factory=list, description='因果事件 ID 链')
    sources: list[str] = Field(default_factory=list, description='事件来源链')
    timestamps: list[str] = Field(default_factory=list, description='事件逐跳时间戳（ISO 8601）')
    payload_size: int = Field(default=0, description='负载 UTF-8 字节数')
    payload_sha256: str | None = Field(default=None, description='负载规范化 JSON 的 SHA-256')
    data: str | None = Field(default=None, description='负载正文；按模式 / 白名单 / 上限决定是否填充')
    error: str | None = Field(default=None, description='发布失败描述')
    extra: dict[str, Any] = Field(default_factory=dict, description='调用方追加的静态字段')

    def to_json_dict(self) -> dict[str, Any]:
        """展开为平铺字典，用于 JSONL 等按字段存储的后端。

        ``extra`` 与审计字段重名时抛出 :class:`ValueError`。
        """
        row: dict[str, Any] = self.model_dump()
        extra: dict[str, Any] = row.pop('extra')
        collisions = set(extra) & set(row)
        if collisions:
            raise ValueError(f'extra_fields 与审计字段重名: {sorted(collisions)}')
        row.update(extra)
        return row


RESERVED_FIELDS: frozenset[str] = frozenset(AuditRecord.model_fields)
"""审计记录保留字段名，``extra_fields`` 不得与之重名。"""


def validate_extra_fields(extra: Mapping[str, Any]) -> None:
    """校验额外字段是否与审计字段重名。"""
    collisions = RESERVED_FIELDS.intersection(extra)
    if collisions:
        raise ValueError(f'extra_fields 与审计字段重名: {sorted(collisions)}')


def canonical_payload(data: dict[str, Any] | BaseModel | None) -> str | None:
    """将负载序列化为键序稳定的紧凑 JSON；``None`` 保持 ``None``。"""
    if data is None:
        return None
    raw: Any = data.model_dump(mode='json') if isinstance(data, BaseModel) else data
    try:
        return json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)
    except (TypeError, ValueError):
        try:
            return json.dumps(raw, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            return str(raw)


def _payload_fields(
    data: dict[str, Any] | BaseModel | None,
    *,
    name: str,
    payload_mode: PayloadMode,
    payload_allowlist: frozenset[str],
    payload_max_bytes: int | None,
) -> tuple[int, str | None, str | None]:
    """返回 ``(字节数, sha256, 正文)``；正文是否保留由模式 / 白名单 / 上限决定。"""
    text = canonical_payload(data)
    if text is None:
        return 0, None, None
    encoded = text.encode('utf-8')
    size = len(encoded)
    digest = hashlib.sha256(encoded).hexdigest()
    keep = payload_mode == 'full' or name in payload_allowlist
    if payload_max_bytes is not None and size > payload_max_bytes:
        keep = False
    return size, digest, text if keep else None


def build_record(
    event: Event,
    *,
    payload_mode: PayloadMode,
    payload_allowlist: frozenset[str],
    payload_max_bytes: int | None,
    extra_fields: Mapping[str, Any],
    now: float | None = None,
) -> AuditRecord:
    """从已发布事件构建审计记录。"""
    size, digest, data = _payload_fields(
        event.data,
        name=event.name,
        payload_mode=payload_mode,
        payload_allowlist=payload_allowlist,
        payload_max_bytes=payload_max_bytes,
    )
    return AuditRecord(
        ts=time.time() if now is None else now,
        emitted_at=event.timestamps[-1].timestamp() if event.timestamps else None,
        service=event.sources[-1] if event.sources else '',
        name=event.name,
        event_id=event.id,
        event_ids=list(event.event_ids),
        sources=list(event.sources),
        timestamps=[timestamp.isoformat() for timestamp in event.timestamps],
        payload_size=size,
        payload_sha256=digest,
        data=data,
        extra=dict(extra_fields),
    )


def build_error_record(
    error: Exception,
    *,
    name: str,
    source: str,
    data: dict[str, Any] | BaseModel | None,
    payload_mode: PayloadMode,
    payload_allowlist: frozenset[str],
    payload_max_bytes: int | None,
    extra_fields: Mapping[str, Any],
    now: float | None = None,
) -> AuditRecord:
    """构建一条发布失败审计记录。"""
    size, digest, payload = _payload_fields(
        data,
        name=name,
        payload_mode=payload_mode,
        payload_allowlist=payload_allowlist,
        payload_max_bytes=payload_max_bytes,
    )
    return AuditRecord(
        ts=time.time() if now is None else now,
        emitted_at=None,
        service=source,
        name=name,
        event_id=ERROR_EVENT_ID,
        event_ids=[],
        sources=[source],
        timestamps=[],
        payload_size=size,
        payload_sha256=digest,
        data=payload,
        error=f'{type(error).__name__}: {error}',
        extra=dict(extra_fields),
    )
