"""审计记录模型与规范化构建。"""

import hashlib
import json
import time
from collections.abc import Mapping
from typing import Any, cast

from pydantic import BaseModel, Field

from infinity_bus import Event

from .models import PayloadMode

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
    payload_sha256: str | None = Field(
        default=None, description='负载规范化 JSON 的 SHA-256（不可渲染字段以占位符参与）'
    )
    data: str | None = Field(default=None, description='负载正文；按模式 / 白名单 / 上限决定是否填充')
    payload_unrenderable: dict[str, str] = Field(
        default_factory=dict,
        description='不可渲染字段：占位符 -> 类型与字符串表示；仅保留正文时填充',
    )
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


def _describe_unrenderable(value: Any) -> str:
    """为不可渲染值生成 ``类型: 字符串表示``，仅用于旁路存储。"""
    type_name = f'{type(value).__module__}.{type(value).__qualname__}'
    try:
        text = str(value)
    except Exception:
        text = '<str() failed>'
    return f'{type_name}: {text}'


class _PayloadNormalizer:
    """把负载递归规范化为 JSON 原生结构，并登记不可渲染值。

    占位符按确定性遍历顺序编号，因此同一逻辑负载（即使不可渲染对象的
    ``str()`` 包含内存地址）会得到相同的规范化 JSON 与摘要。
    """

    def __init__(self) -> None:
        self._unrenderable: dict[str, str] = {}
        self._active: set[int] = set()

    @property
    def unrenderable(self) -> dict[str, str]:
        """占位符到 ``类型: 字符串表示`` 的映射。"""
        return dict(self._unrenderable)

    def normalize(self, value: Any) -> Any:
        """递归返回仅含 JSON 原生类型的结构。"""
        if value is None or isinstance(value, (str, bool, int, float)):
            return value
        if isinstance(value, BaseModel):
            return self._normalize_model(value)
        if isinstance(value, Mapping):
            return self._normalize_mapping(cast(Mapping[Any, Any], value))
        if isinstance(value, (list, tuple)):
            return self._normalize_sequence(cast(list[Any] | tuple[Any, ...], value))
        return self._register(value)

    def _normalize_model(self, model: BaseModel) -> Any:
        try:
            dumped = model.model_dump(mode='json')
        except Exception:
            try:
                dumped = model.model_dump()
            except Exception:
                return self._register(model)
        return self.normalize(dumped)

    def _normalize_mapping(self, value: Mapping[Any, Any]) -> Any:
        marker = id(value)
        if marker in self._active:
            return self._register(value)
        self._active.add(marker)
        try:
            items = sorted(value.items(), key=lambda item: self._sort_key(item[0]))
            return {self._normalize_key(key): self.normalize(item) for key, item in items}
        finally:
            self._active.discard(marker)

    @staticmethod
    def _sort_key(key: Any) -> str:
        """为映射键生成稳定排序键，保证占位符编号与插入顺序无关。"""
        if isinstance(key, str):
            return key
        try:
            return f'{type(key).__module__}.{type(key).__qualname__}:{key!s}'
        except Exception:
            return f'{type(key).__module__}.{type(key).__qualname__}'

    def _normalize_sequence(self, value: list[Any] | tuple[Any, ...]) -> Any:
        marker = id(value)
        if marker in self._active:
            return self._register(value)
        self._active.add(marker)
        try:
            return [self.normalize(item) for item in value]
        finally:
            self._active.discard(marker)

    def _normalize_key(self, key: Any) -> str:
        if isinstance(key, str):
            return key
        if key is None:
            return 'null'
        if isinstance(key, bool):
            return 'true' if key else 'false'
        if isinstance(key, (int, float)):
            return str(key)
        return self._register(key)

    def _register(self, value: Any) -> str:
        token = f'<{len(self._unrenderable)}>'
        self._unrenderable[token] = _describe_unrenderable(value)
        return token


def canonicalize_payload(data: dict[str, Any] | BaseModel | None) -> tuple[str | None, dict[str, str]]:
    """将负载规范化为键序稳定的紧凑 JSON，并返回不可渲染字段映射。

    不可 JSON 渲染的值（任意对象、``bytes``、``set``、循环引用等）在原位置替换为
    ``<0>``、``<1>``… 占位符，真实值以 ``类型: 字符串表示`` 存入旁路映射。
    摘要只取决于负载结构，不受对象内存地址影响；保留正文时满足
    ``sha256(data.encode('utf-8')) == payload_sha256``。
    """
    if data is None:
        return None, {}
    normalizer = _PayloadNormalizer()
    normalized = normalizer.normalize(data)
    text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return text, normalizer.unrenderable


def canonical_payload(data: dict[str, Any] | BaseModel | None) -> str | None:
    """将负载序列化为键序稳定的紧凑 JSON；``None`` 保持 ``None``。"""
    return canonicalize_payload(data)[0]


def _stored_payload_bytes(size: int, unrenderable: Mapping[str, str]) -> int:
    """正文与不可渲染旁路映射的 UTF-8 字节数之和。"""
    if not unrenderable:
        return size
    return size + len(json.dumps(unrenderable, ensure_ascii=False, sort_keys=True).encode('utf-8'))


def _payload_fields(
    data: dict[str, Any] | BaseModel | None,
    *,
    name: str,
    payload_mode: PayloadMode,
    payload_allowlist: frozenset[str],
    payload_max_bytes: int | None,
) -> tuple[int, str | None, str | None, dict[str, str]]:
    """返回 ``(字节数, sha256, 正文, 不可渲染映射)``；正文与映射是否保留由模式 / 白名单 / 上限决定。"""
    text, unrenderable = canonicalize_payload(data)
    if text is None:
        return 0, None, None, {}
    encoded = text.encode('utf-8')
    size = len(encoded)
    digest = hashlib.sha256(encoded).hexdigest()
    keep = payload_mode == 'full' or name in payload_allowlist
    if payload_max_bytes is not None and _stored_payload_bytes(size, unrenderable) > payload_max_bytes:
        keep = False
    if not keep:
        return size, digest, None, {}
    return size, digest, text, unrenderable


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
    size, digest, data, unrenderable = _payload_fields(
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
        payload_unrenderable=unrenderable,
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
    size, digest, payload, unrenderable = _payload_fields(
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
        payload_unrenderable=unrenderable,
        error=f'{type(error).__name__}: {error}',
        extra=dict(extra_fields),
    )
