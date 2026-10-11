"""指标中间件测试。"""

from typing import Generator

import pytest
from conftest import SimplePingHandler

from infinity_bus import (
    EventBus,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    MiddlewareChain,
)
from infinity_bus.templates.middlewares import MetricsMiddleware, MetricsSnapshot


@pytest.fixture
def metrics() -> MetricsMiddleware:
    return MetricsMiddleware()


@pytest.fixture
async def chain_with_metrics(metrics: MetricsMiddleware) -> MiddlewareChain:
    chain = MiddlewareChain()
    await chain.add(metrics)
    return chain


# ============================================================================
# 基础指标收集
# ============================================================================


class TestMetricsBasic:
    """指标收集的基本场景。"""

    @pytest.mark.asyncio
    async def test_publish_counter_increments(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        metrics: MetricsMiddleware,
        chain_with_metrics: MiddlewareChain,
    ) -> None:
        """发布事件后 publish_total 计数递增。"""
        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_with_metrics,
        )
        async with bus:
            await bus.proxy('test').publish('mw.ping', {'key': 'a', 'count': 1})
            await handler.wait_received(timeout=2.0)

        snap = metrics.snapshot()
        assert snap.publish_total.get('mw.ping', 0) >= 1

    @pytest.mark.asyncio
    async def test_publish_duration_recorded(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        metrics: MetricsMiddleware,
        chain_with_metrics: MiddlewareChain,
    ) -> None:
        """发布事件后延迟直方图有记录。"""
        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_with_metrics,
        )
        async with bus:
            await bus.proxy('test').publish('mw.ping', {'key': 'b', 'count': 2})
            await handler.wait_received(timeout=2.0)

        snap = metrics.snapshot()
        hist = snap.publish_duration_sec.get('mw.ping')
        assert hist is not None
        assert hist.count >= 1
        assert hist.sum > 0

    @pytest.mark.asyncio
    async def test_multiple_events_separate_counters(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        metrics: MetricsMiddleware,
        chain_with_metrics: MiddlewareChain,
    ) -> None:
        """不同事件名独立计数。"""
        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_with_metrics,
        )
        async with bus:
            await bus.proxy('a').publish('mw.ping', {'key': 'x', 'count': 1})
            await bus.proxy('a').publish('user.login', None)
            await handler.wait_received(timeout=2.0)

        snap = metrics.snapshot()
        assert snap.publish_total.get('mw.ping', 0) >= 1
        assert snap.publish_total.get('user.login', 0) >= 1

    @pytest.mark.asyncio
    async def test_queue_and_active_tasks_gauge(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        metrics: MetricsMiddleware,
        chain_with_metrics: MiddlewareChain,
    ) -> None:
        """快照包含队列深度和活跃任务数。"""
        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_with_metrics,
        )
        async with bus:
            pass  # 启动后立即停止

        snap = metrics.snapshot()
        assert snap.queue_size >= 0
        assert snap.active_tasks >= 0


# ============================================================================
# 错误指标
# ============================================================================


class TestMetricsErrors:
    """发布失败时错误计数的场景。"""

    @pytest.mark.asyncio
    async def test_publish_error_increments(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        metrics: MetricsMiddleware,
        chain_with_metrics: MiddlewareChain,
    ) -> None:
        """发布未注册事件时 errors_total 递增。"""
        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_with_metrics,
        )
        async with bus:
            with pytest.raises(ValueError):
                await bus.proxy('test').publish('unknown.event')

        snap = metrics.snapshot()
        assert snap.publish_errors_total >= 1

    @pytest.mark.asyncio
    async def test_publish_total_includes_errors(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        metrics: MetricsMiddleware,
        chain_with_metrics: MiddlewareChain,
    ) -> None:
        """publish_total 统计所有尝试（含失败），errors_total 单独计数。"""
        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_with_metrics,
        )
        async with bus:
            with pytest.raises(ValueError):
                await bus.proxy('test').publish('unknown.event')

        snap = metrics.snapshot()
        # publish_total 包含失败尝试（Prometheus _total 惯例）
        assert snap.publish_total.get('unknown.event', 0) >= 1
        assert snap.publish_errors_total >= 1


# ============================================================================
# 快照
# ============================================================================


class TestMetricsSnapshot:
    """快照模型的正确性。"""

    @pytest.mark.asyncio
    async def test_empty_snapshot(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        metrics: MetricsMiddleware,
        chain_with_metrics: MiddlewareChain,
    ) -> None:
        """无业务事件时快照仅有系统关机事件。"""
        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_with_metrics,
        )
        async with bus:
            pass

        snap = metrics.snapshot()
        assert isinstance(snap, MetricsSnapshot)
        # 系统关闭事件是自动发布的，排除后业务指标为空
        business = {k: v for k, v in snap.publish_total.items() if not k.startswith('infinity_bus.__')}
        assert business == {}
        assert snap.publish_errors_total == 0

    @pytest.mark.asyncio
    async def test_snapshot_does_not_mutate_internal_state(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        metrics: MetricsMiddleware,
        chain_with_metrics: MiddlewareChain,
    ) -> None:
        """快照是只读副本，修改不影响内部状态。"""
        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain_with_metrics,
        )
        async with bus:
            await bus.proxy('test').publish('mw.ping', {'key': 's', 'count': 3})
            await handler.wait_received(timeout=2.0)

        snap1 = metrics.snapshot()
        snap1.publish_total['mw.ping'] = 999  # 修改副本

        snap2 = metrics.snapshot()
        assert snap2.publish_total.get('mw.ping', 0) == 1  # 内部未受影响


# ============================================================================
# 自定义桶边界
# ============================================================================


class TestMetricsCustomBuckets:
    """自定义直方图桶边界。"""

    @pytest.mark.asyncio
    async def test_custom_bounds(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自定义桶边界在快照中体现。"""
        custom = MetricsMiddleware(histogram_bounds=(0.01, 0.1, 1.0))
        chain = MiddlewareChain()
        await chain.add(custom)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('test').publish('mw.ping', {'key': 'c', 'count': 4})
            await handler.wait_received(timeout=2.0)

        snap = custom.snapshot()
        hist = snap.publish_duration_sec.get('mw.ping')
        assert hist is not None
        assert set(hist.buckets.keys()) == {'0.01', '0.1', '1.0'}


# ============================================================================
# Prometheus SDK 集成
# ============================================================================


class TestPrometheusIntegration:
    """桥接 prometheus_client 官方 SDK。"""

    @pytest.fixture(autouse=True)
    def _clean_global_registry(self) -> Generator[None, None, None]:
        """每个测试后清理全局 REGISTRY，避免指标名冲突。"""
        yield
        import prometheus_client

        collectors = list(prometheus_client.REGISTRY._collector_to_names)
        for c in collectors:
            try:
                prometheus_client.REGISTRY.unregister(c)
            except KeyError:
                pass

    @pytest.mark.asyncio
    async def test_integrate_creates_instruments(
        self,
    ) -> None:
        """integrate_prometheus() 在 registry 中创建 Counter / Histogram / Gauge。"""
        import prometheus_client

        registry = prometheus_client.CollectorRegistry()
        metrics = MetricsMiddleware()
        metrics.integrate_prometheus(registry)

        assert metrics.prometheus_integrated
        # prometheus_client 对 Counter 名自动去掉 _total 后缀（内部名）
        names = {m.name for m in registry.collect()}
        assert 'infinity_bus_publish' in names  # Counter 内部名无 _total
        assert 'infinity_bus_publish_duration_seconds' in names
        assert 'infinity_bus_publish_errors' in names  # Counter 内部名无 _total
        assert 'infinity_bus_queue_size' in names
        assert 'infinity_bus_active_tasks' in names

    @pytest.mark.asyncio
    async def test_integrate_idempotent(
        self,
    ) -> None:
        """重复调用 integrate_prometheus 不会重复注册。"""
        import prometheus_client

        registry = prometheus_client.CollectorRegistry()
        metrics = MetricsMiddleware()
        metrics.integrate_prometheus(registry)
        metrics.integrate_prometheus(registry)

        assert metrics.prometheus_integrated

    @pytest.mark.asyncio
    async def test_publish_increments_prometheus_counter(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """发布事件后 prometheus counter 递增。"""
        import prometheus_client

        registry = prometheus_client.CollectorRegistry()
        metrics = MetricsMiddleware()
        metrics.integrate_prometheus(registry)

        chain = MiddlewareChain()
        await chain.add(metrics)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('test').publish('mw.ping', {'key': 'p', 'count': 1})
            await handler.wait_received(timeout=2.0)

        # 从 registry 收集指标
        samples_by_name: dict[str, float] = {s.name: s.value for m in registry.collect() for s in m.samples}
        assert samples_by_name.get('infinity_bus_publish_total', 0) >= 1
        assert samples_by_name.get('infinity_bus_publish_duration_seconds_count', 0) >= 1

    @pytest.mark.asyncio
    async def test_error_increments_prometheus_errors(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """发布失败时 prometheus errors_total 递增。"""
        import prometheus_client

        registry = prometheus_client.CollectorRegistry()
        metrics = MetricsMiddleware()
        metrics.integrate_prometheus(registry)

        chain = MiddlewareChain()
        await chain.add(metrics)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            with pytest.raises(ValueError):
                await bus.proxy('test').publish('unknown.event')

        samples_by_name = {s.name: s.value for m in registry.collect() for s in m.samples}
        assert samples_by_name.get('infinity_bus_publish_errors_total', 0) >= 1

    @pytest.mark.asyncio
    async def test_default_registry(
        self,
    ) -> None:
        """省略 registry 参数时使用全局 REGISTRY。"""
        metrics = MetricsMiddleware()
        metrics.integrate_prometheus()  # 不传 registry

        assert metrics.prometheus_integrated
        assert metrics._prom_registry is not None  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.asyncio
    async def test_gauge_reflects_bus_state(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """Gauge 回调函数能反映总线真实状态。"""
        import prometheus_client

        registry = prometheus_client.CollectorRegistry()
        metrics = MetricsMiddleware()
        metrics.integrate_prometheus(registry)

        chain = MiddlewareChain()
        await chain.add(metrics)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            pass  # 总线已停止

        # 停止后 queue_size 应为 0
        samples_by_name = {s.name: s.value for m in registry.collect() for s in m.samples}
        assert samples_by_name.get('infinity_bus_queue_size', -1) >= 0


# ============================================================================
# OpenTelemetry SDK 集成
# ============================================================================


class TestOpenTelemetryIntegration:
    """桥接 opentelemetry-api 官方 SDK。"""

    @pytest.mark.asyncio
    async def test_integrate_creates_instruments(
        self,
    ) -> None:
        """integrate_opentelemetry() 创建 Counter / Histogram / ObservableGauge。"""
        from opentelemetry import metrics as otel_metrics

        meter = otel_metrics.get_meter('test_event_bus')
        metrics = MetricsMiddleware()
        metrics.integrate_opentelemetry(meter)

        assert metrics.opentelemetry_integrated
        assert metrics._otel_counter is not None  # pyright: ignore[reportPrivateUsage]
        assert metrics._otel_histogram is not None  # pyright: ignore[reportPrivateUsage]
        assert metrics._otel_errors is not None  # pyright: ignore[reportPrivateUsage]
        assert metrics._otel_queue_gauge is not None  # pyright: ignore[reportPrivateUsage]
        assert metrics._otel_tasks_gauge is not None  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.asyncio
    async def test_integrate_idempotent(
        self,
    ) -> None:
        """重复调用 integrate_opentelemetry 不会重复创建。"""
        from opentelemetry import metrics as otel_metrics

        meter = otel_metrics.get_meter('test_event_bus')
        metrics = MetricsMiddleware()
        metrics.integrate_opentelemetry(meter)
        metrics.integrate_opentelemetry(meter)

        assert metrics.opentelemetry_integrated

    @pytest.mark.asyncio
    async def test_publish_writes_to_otel_instruments(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """发布事件后 OTel 指标有值（通过 mock 验证）。"""
        from unittest.mock import MagicMock

        mock_counter = MagicMock()
        mock_histogram = MagicMock()
        mock_errors = MagicMock()

        metrics = MetricsMiddleware()
        # 手动注入 mock 仪器，跳过 OTel SDK 初始化
        metrics._otel_counter = mock_counter
        metrics._otel_histogram = mock_histogram
        metrics._otel_errors = mock_errors
        metrics._otel_queue_gauge = MagicMock()
        metrics._otel_tasks_gauge = MagicMock()
        metrics._otel_integrated = True

        chain = MiddlewareChain()
        await chain.add(metrics)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('test').publish('mw.ping', {'key': 'o', 'count': 1})
            await handler.wait_received(timeout=2.0)

        # Counter.add(1, {'event_name': 'mw.ping'}) 至少被调用
        mock_counter.add.assert_any_call(1, {'event_name': 'mw.ping'})
        # Histogram.record 被调用
        assert mock_histogram.record.call_count >= 1

    @pytest.mark.asyncio
    async def test_error_increments_otel_errors(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """发布失败时 OTel errors counter 递增。"""
        from unittest.mock import MagicMock

        mock_errors = MagicMock()
        mock_counter = MagicMock()
        mock_histogram = MagicMock()

        metrics = MetricsMiddleware()
        metrics._otel_counter = mock_counter
        metrics._otel_histogram = mock_histogram
        metrics._otel_errors = mock_errors
        metrics._otel_queue_gauge = MagicMock()
        metrics._otel_tasks_gauge = MagicMock()
        metrics._otel_integrated = True

        chain = MiddlewareChain()
        await chain.add(metrics)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            with pytest.raises(ValueError):
                await bus.proxy('test').publish('unknown.event')

        # errors counter 被调用
        mock_errors.add.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_default_meter(
        self,
    ) -> None:
        """省略 meter 参数时自动创建 'infinity_bus' Meter。"""
        metrics = MetricsMiddleware()
        metrics.integrate_opentelemetry()  # 不传 meter

        assert metrics.opentelemetry_integrated
        assert metrics._otel_meter is not None  # pyright: ignore[reportPrivateUsage]
