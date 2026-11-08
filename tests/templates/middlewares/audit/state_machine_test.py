"""AuditStateMachine 状态迁移与决策测试。"""

from infinity_bus.templates.middlewares.audit import AuditWriterConfig
from infinity_bus.templates.middlewares.audit.models import (
    ClosedState,
    ClosingState,
    DegradedState,
    RunningState,
)
from infinity_bus.templates.middlewares.audit.state_machine import AuditStateMachine


class _Clock:
    """可手动推进的单调时钟。"""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _machine(clock: _Clock | None = None) -> AuditStateMachine:
    return AuditStateMachine(
        AuditWriterConfig(reconnect_backoff=1.0, reconnect_backoff_max=4.0),
        clock=clock or _Clock(),
    )


def test_initial_state_is_running() -> None:
    """初始状态可接受记录且不处于降级。"""
    machine = _machine()
    assert isinstance(machine.state, RunningState)
    assert not isinstance(machine.state, DegradedState)
    assert machine.accepts_records()
    assert not machine.writer_should_stop(queue_empty=True)
    assert machine.retry_due()


def test_setup_success_keeps_running_and_counters() -> None:
    """重新 setup 成功后回到运行状态，共享计数保留。"""
    machine = _machine()
    machine.record_written(1)
    machine.begin_setup()
    machine.setup_opened()
    assert isinstance(machine.state, RunningState)
    assert machine.state.written_count == 1


def test_setup_failure_degrades_and_backs_off() -> None:
    """打开失败进入降级，重试失败后指数退避，成功恢复运行。"""
    clock = _Clock()
    machine = _machine(clock)
    machine.setup_failed()
    assert isinstance(machine.state, DegradedState)
    assert machine.state.next_retry_at == 1.0
    assert machine.state.backoff == 2.0
    assert not machine.retry_due()

    clock.now = 1.0
    assert machine.retry_due()
    machine.retry_failed()
    assert machine.state.next_retry_at == 3.0
    assert machine.state.backoff == 4.0

    clock.now = 3.0
    machine.retry_succeeded()
    assert isinstance(machine.state, RunningState)
    assert not isinstance(machine.state, DegradedState)


def test_write_failure_records_error_and_degrades() -> None:
    """写失败累计错误并进入降级。"""
    machine = _machine()
    machine.write_failed()
    assert machine.state.write_error_count == 1
    assert isinstance(machine.state, DegradedState)


def test_teardown_states_reject_records() -> None:
    """停机状态拒绝新记录，写入任务仅在队列空时退出。"""
    machine = _machine()
    machine.begin_teardown()
    assert isinstance(machine.state, ClosingState)
    assert not machine.accepts_records()
    assert not machine.writer_should_stop(queue_empty=False)
    assert machine.writer_should_stop(queue_empty=True)

    machine.teardown_finished()
    assert isinstance(machine.state, ClosedState)
    assert not machine.accepts_records()


def test_counters_survive_state_transitions() -> None:
    """共享计数跨状态保留。"""
    machine = _machine()
    machine.record_written(3)
    machine.record_dropped()
    machine.record_fallback(2)
    machine.begin_teardown()
    machine.teardown_finished()
    assert machine.state.written_count == 3
    assert machine.state.dropped_count == 1
    assert machine.state.fallback_count == 2
