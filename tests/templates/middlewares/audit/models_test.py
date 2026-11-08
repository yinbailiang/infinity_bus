"""审计状态与队列类型模型测试。"""

from infinity_bus.templates.middlewares.audit.models import (
    STOP,
    AuditState,
    ClosedState,
    ClosingState,
    DegradedState,
    RunningState,
    StopSentinel,
)


def test_inherit_preserves_shared_counters() -> None:
    """状态切换时共享计数器完整保留。"""
    source = DegradedState(
        written_count=1,
        dropped_count=2,
        write_error_count=3,
        fallback_count=4,
        next_retry_at=5.0,
        backoff=6.0,
    )
    target = source.inherit(RunningState())
    assert isinstance(target, RunningState)
    assert (
        target.written_count,
        target.dropped_count,
        target.write_error_count,
        target.fallback_count,
    ) == (1, 2, 3, 4)


def test_degraded_state_keeps_backoff_fields() -> None:
    """降级状态保存重试时刻与退避间隔。"""
    state = DegradedState(next_retry_at=12.5, backoff=2.0)
    assert state.next_retry_at == 12.5
    assert state.backoff == 2.0


def test_all_states_share_base_counters() -> None:
    """所有状态都继承基类的共享计数。"""
    assert isinstance(RunningState(), AuditState)
    assert isinstance(ClosingState(), AuditState)
    assert isinstance(ClosedState(), AuditState)


def test_stop_sentinel_type() -> None:
    """停止信号是可识别的哨兵对象。"""
    assert isinstance(STOP, StopSentinel)
