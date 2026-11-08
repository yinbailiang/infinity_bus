"""AuditSink 接口默认行为测试。"""

from collections.abc import Sequence

import pytest

from infinity_bus.templates.middlewares.audit import AuditRecord
from infinity_bus.templates.middlewares.audit.sinks import AuditSink


class _MinimalSink(AuditSink):
    """仅实现抽象方法的最小 Sink。"""

    def __init__(self) -> None:
        self.opened = False

    @property
    def is_open(self) -> bool:
        return self.opened

    async def open(self) -> None:
        self.opened = True

    async def write(self, records: Sequence[AuditRecord]) -> None:
        pass

    async def close(self) -> None:
        self.opened = False


@pytest.mark.asyncio
async def test_default_maintain_and_reclaim_are_noops() -> None:
    """默认 maintain / reclaim 不做事，也不要求子类实现。"""
    sink = _MinimalSink()
    await sink.open()
    await sink.maintain()
    await sink.reclaim()
    assert sink.is_open
    await sink.close()
    assert not sink.is_open
