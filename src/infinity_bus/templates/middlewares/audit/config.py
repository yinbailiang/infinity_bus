"""审计中间件配置。"""

from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import PayloadMode
from .record import validate_extra_fields
from .sinks.base import LogFallback


class AuditWriterConfig(BaseModel):
    """``AuditWriter`` 的不可变配置：队列、批处理、重试与生命周期。"""

    model_config = ConfigDict(frozen=True)

    batch_size: int = Field(default=100, ge=1, description='单批最大记录数')
    flush_interval: float = Field(default=0.5, ge=0, description='单批最长等待（秒），与 batch_size 先到先写')
    queue_maxsize: int = Field(default=10_000, ge=1, description='有界队列容量；满时丢弃最新记录并计数')
    fallback: LogFallback | None = Field(default=None, description='降级回调，签名 (line: str) -> None')
    reconnect_backoff: float = Field(default=1.0, gt=0, description='首次重连退避（秒），之后指数增长')
    reconnect_backoff_max: float = Field(default=30.0, gt=0, description='退避上限（秒）')
    close_timeout: float = Field(default=5.0, gt=0, description='停机排空队列的超时（秒）')
    maintenance_interval: float | None = Field(
        default=60.0,
        gt=0,
        description='独立周期维护间隔（秒）；None 关闭维护',
    )

    @model_validator(mode='after')
    def _validate(self) -> Self:
        if self.reconnect_backoff_max < self.reconnect_backoff:
            raise ValueError('reconnect_backoff_max 不能小于 reconnect_backoff')
        return self


class AuditPayloadConfig(BaseModel):
    """``AuditRecordRenderer`` 的不可变配置：负载记录方式。"""

    model_config = ConfigDict(frozen=True)

    payload_mode: PayloadMode = Field(
        default='digest', description="负载记录模式：'digest' 只存大小与摘要，'full' 额外存正文"
    )
    payload_allowlist: frozenset[str] = Field(default_factory=frozenset, description='事件名白名单，命中时保留正文')
    payload_max_bytes: int | None = Field(default=64 * 1024, ge=1, description='正文超过此值时只保留摘要；None 不限制')
    extra_fields: dict[str, Any] = Field(default_factory=dict, description='每条记录追加的静态字段')

    @model_validator(mode='after')
    def _validate(self) -> Self:
        validate_extra_fields(self.extra_fields)
        return self
