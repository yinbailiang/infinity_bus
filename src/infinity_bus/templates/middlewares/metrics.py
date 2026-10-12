"""轻量级 Prometheus / OpenTelemetry 风格指标中间件。

零依赖 —— 纯内存计数器与直方图，无需安装 ``prometheus_client`` 或 ``opentelemetry-api``。
可选集成：调用 ``integrate_prometheus()`` / ``integrate_opentelemetry()`` 即可桥接到官方 SDK。

收集指标
--------
- ``infinity_bus_publish_total``        Counter   按事件名统计发布次数
- ``infinity_bus_publish_duration``     Histogram 发布耗时（before_publish → on_publish 结束）
- ``infinity_bus_publish_errors_total`` Counter   发布失败次数
- ``infinity_bus_queue_size``           Gauge     当前队列长度
- ``infinity_bus_active_tasks``         Gauge     当前活跃处理器任务数
- ``infinity_bus_task_total``           Counter   按处理器名 + 结果统计任务完成次数
- ``infinity_bus_task_duration``        Histogram 任务执行耗时（信号量获取后 → 结束）
- ``infinity_bus_task_wait``            Histogram 任务排队耗时（派发 → 信号量获取）

纯内存使用
----------
.. code-block:: python

    from infinity_bus.templates.middlewares import MetricsMiddleware

    metrics = MetricsMiddleware()
    chain = MiddlewareChain()
    chain.add(metrics)

    bus = EventBus(events, handlers, middleware_chain=chain)
    async with bus:
        await bus.proxy("svc").publish("order.created", {...})

    print(metrics.snapshot())

对接 Prometheus SDK
--------------------
.. code-block:: python

    import prometheus_client

    registry = prometheus_client.CollectorRegistry()
    metrics = MetricsMiddleware()
    metrics.integrate_prometheus(registry)  # 若省略则使用 REGISTRY 全局

    # 启动 HTTP 暴露
    prometheus_client.start_http_server(8000, registry=registry)

对接 OpenTelemetry SDK
-----------------------
.. code-block:: python

    from opentelemetry import metrics as otel_metrics

    meter = otel_metrics.get_meter("infinity_bus")
    metrics = MetricsMiddleware()
    metrics.integrate_opentelemetry(meter)
"""

from __future__ import annotations

import importlib
import logging
import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from infinity_bus import (
    BeforePublishNext,
    Event,
    EventBus,
    EventRegistry,
    Middleware,
    OnPublishNext,
    TaskRecord,
)

logger = logging.getLogger(__name__)

# ============================================================================
# 轻量直方图（无外部依赖）
# ============================================================================

# 默认延迟分桶（秒）
_DEFAULT_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)


class Buckets(BaseModel):
    """直方图分桶快照。"""

    count: int = Field(default=0, description='观测总数')
    sum: float = Field(default=0.0, description='观测值总和（秒）')
    buckets: Dict[str, int] = Field(default_factory=dict, description='{上界字符串: 计数}')
    inf: int = Field(default=0, description='超出最大桶上界的样本数')


class _Histogram:
    """纯内存累积分桶直方图。"""

    def __init__(self, bounds: tuple[float, ...] = _DEFAULT_BUCKETS) -> None:
        self._bounds = bounds
        self._buckets: List[int] = [0] * len(bounds)
        self._count = 0
        self._sum = 0.0
        self._inf = 0

    def observe(self, value: float) -> None:
        self._count += 1
        self._sum += value
        placed = False
        for i, bound in enumerate(self._bounds):
            if value <= bound:
                self._buckets[i] += 1
                placed = True
                break
        if not placed:
            self._inf += 1

    def snapshot(self) -> Buckets:
        buckets: Dict[str, int] = {}
        for i, bound in enumerate(self._bounds):
            buckets[str(bound)] = self._buckets[i]
        return Buckets(
            count=self._count,
            sum=self._sum,
            buckets=buckets,
            inf=self._inf,
        )


# ============================================================================
# 快照模型
# ============================================================================


class MetricsSnapshot(BaseModel):
    """一次指标快照（只读）。"""

    publish_total: Dict[str, int] = Field(default_factory=dict, description='{事件名: 发布次数}')
    publish_duration_sec: Dict[str, Buckets] = Field(default_factory=dict, description='{事件名: 延迟直方图}')
    publish_errors_total: int = Field(default=0, description='发布失败总数')
    queue_size: int = Field(default=0, description='当前队列深度')
    active_tasks: int = Field(default=0, description='当前活跃处理器任务数')
    task_total: Dict[str, Dict[str, int]] = Field(default_factory=dict, description='{处理器名: {任务结果: 次数}}')
    task_duration_sec: Dict[str, Buckets] = Field(default_factory=dict, description='{处理器名: 任务执行时长直方图}')
    task_wait_sec: Dict[str, Buckets] = Field(default_factory=dict, description='{处理器名: 排队时长直方图}')


# ============================================================================
# MetricsMiddleware
# ============================================================================


class MetricsMiddleware(Middleware):
    """收集事件总线轻量级 Prometheus / OpenTelemetry 风格指标。

    纯内存实现，零外部依赖。通过 ``snapshot()`` 获取只读快照，
    可与 ``prometheus_client`` 等库配合导出到 scrape 端点。

    参数
    ----
    histogram_bounds:
        延迟直方图分桶上界（秒）。默认覆盖 1ms ~ 30s。
    """

    def __init__(self, histogram_bounds: tuple[float, ...] = _DEFAULT_BUCKETS) -> None:
        self._histogram_bounds = histogram_bounds

        # Counter: 按事件名计数
        self._publish_total: Dict[str, int] = {}
        # Histogram: 按事件名
        self._publish_duration: Dict[str, _Histogram] = {}
        # Counter: 失败
        self._publish_errors_total = 0
        # Counter: 按处理器与结果（success / error / cancelled / skipped）
        self._task_total: Dict[str, Dict[str, int]] = {}
        # Histogram: 按处理器 —— 执行时长 / 排队时长
        self._task_duration: Dict[str, _Histogram] = {}
        self._task_wait: Dict[str, _Histogram] = {}

        # 总线引用（on_setup 注入），用于读取队列/活跃任务
        self._bus: Optional[EventBus] = None

        # —— Prometheus SDK 集成（惰性创建） ——
        self._prom_registry: Any = None
        self._prom_counter: Any = None
        self._prom_histogram: Any = None
        self._prom_errors: Any = None
        self._prom_queue_gauge: Any = None
        self._prom_tasks_gauge: Any = None
        self._prom_task_counter: Any = None
        self._prom_task_duration: Any = None
        self._prom_task_wait: Any = None
        self._prom_integrated = False

        # —— OpenTelemetry SDK 集成（惰性创建） ——
        self._otel_meter: Any = None
        self._otel_counter: Any = None
        self._otel_histogram: Any = None
        self._otel_errors: Any = None
        self._otel_queue_gauge: Any = None
        self._otel_tasks_gauge: Any = None
        self._otel_task_counter: Any = None
        self._otel_task_duration: Any = None
        self._otel_task_wait: Any = None
        self._otel_integrated = False

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def on_setup(self, bus: EventBus) -> None:
        self._bus = bus

    async def on_teardown(self, bus: EventBus) -> None:
        self._bus = None

    # ------------------------------------------------------------------
    # 内部：直方图观测
    # ------------------------------------------------------------------

    def _observe(self, histograms: Dict[str, _Histogram], key: str, value: float) -> _Histogram:
        """按键观测一次直方图（惰性创建）。"""
        hist = histograms.get(key)
        if hist is None:
            hist = _Histogram(self._histogram_bounds)
            histograms[key] = hist
        hist.observe(value)
        return hist

    # ------------------------------------------------------------------
    # 发布钩子
    # ------------------------------------------------------------------

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: Dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        t0 = time.perf_counter()
        try:
            await next(event_registry, name, source, data, old_event)
        except Exception:
            self._publish_errors_total += 1
            if self._prom_integrated:
                self._prom_errors.inc()
            if self._otel_integrated:
                self._otel_errors.add(1)
            raise
        finally:
            elapsed = time.perf_counter() - t0
            # —— 纯内存 ——
            self._publish_total[name] = self._publish_total.get(name, 0) + 1
            self._observe(self._publish_duration, name, elapsed)
            # —— Prometheus SDK ——
            if self._prom_integrated:
                self._prom_counter.labels(event_name=name).inc()
                self._prom_histogram.labels(event_name=name).observe(elapsed)
            # —— OpenTelemetry SDK ——
            if self._otel_integrated:
                self._otel_counter.add(1, {'event_name': name})
                self._otel_histogram.record(elapsed, {'event_name': name})

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        await next(event)

    async def on_task_done(self, record: TaskRecord) -> None:
        """按任务结果更新指标（通知式，不影响任务执行）。

        - 计数：按处理器名 + 结果（``success`` / ``error`` / ``cancelled`` / ``skipped``）；
        - 直方图：任务执行时长（信号量获取后 → 结束）与排队时长（派发 → 信号量获取）。
        """
        outcome = record.outcome or 'unknown'
        handler_name = record.handler_name
        duration_seconds = record.duration_seconds if record.duration_seconds is not None else 0.0
        wait_seconds = max(0.0, (record.started_at - record.dispatched_at).total_seconds())

        counts = self._task_total.get(handler_name)
        if counts is None:
            counts = {}
            self._task_total[handler_name] = counts
        counts[outcome] = counts.get(outcome, 0) + 1
        self._observe(self._task_duration, handler_name, duration_seconds)
        self._observe(self._task_wait, handler_name, wait_seconds)

        if self._prom_integrated:
            self._prom_task_counter.labels(handler=handler_name, outcome=outcome).inc()
            self._prom_task_duration.labels(handler=handler_name).observe(duration_seconds)
            self._prom_task_wait.labels(handler=handler_name).observe(wait_seconds)
        if self._otel_integrated:
            self._otel_task_counter.add(1, {'handler': handler_name, 'outcome': outcome})
            self._otel_task_duration.record(duration_seconds, {'handler': handler_name})
            self._otel_task_wait.record(wait_seconds, {'handler': handler_name})

    # ------------------------------------------------------------------
    # SDK 集成
    # ------------------------------------------------------------------

    def integrate_prometheus(self, registry: Any = None) -> None:
        """桥接到 ``prometheus_client`` 官方 SDK。

        调用后，每次事件发布将同步写入官方 Counter / Histogram / Gauge，
        可直接被 Prometheus scrape 端点采集。

        参数
        ----
        registry:
            ``prometheus_client.CollectorRegistry`` 实例。
            若为 ``None``，使用全局 ``REGISTRY``。

        异常
        ----
        ImportError:
            未安装 ``prometheus_client`` 时抛出。
        """
        if self._prom_integrated:
            return

        try:
            prometheus_client = importlib.import_module('prometheus_client')
        except ImportError as exc:
            raise ImportError('prometheus_client 未安装，请执行: pip install prometheus_client') from exc

        if registry is None:
            registry = prometheus_client.REGISTRY

        self._prom_registry = registry

        self._prom_counter = prometheus_client.Counter(
            'infinity_bus_publish_total',
            '事件发布总次数',
            labelnames=['event_name'],
            registry=registry,
        )
        self._prom_histogram = prometheus_client.Histogram(
            'infinity_bus_publish_duration_seconds',
            '事件发布耗时（秒）',
            labelnames=['event_name'],
            buckets=list(self._histogram_bounds),
            registry=registry,
        )
        self._prom_errors = prometheus_client.Counter(
            'infinity_bus_publish_errors_total',
            '事件发布失败次数',
            registry=registry,
        )

        # Gauge 通过回调函数采集，每次 scrape 时读取最新值
        self._prom_queue_gauge = prometheus_client.Gauge(
            'infinity_bus_queue_size',
            '事件队列当前深度',
            registry=registry,
        )
        self._prom_queue_gauge.set_function(lambda: self._bus.queue_size if self._bus else 0)

        self._prom_tasks_gauge = prometheus_client.Gauge(
            'infinity_bus_active_tasks',
            '当前活跃处理器任务数',
            registry=registry,
        )
        self._prom_tasks_gauge.set_function(lambda: self._bus.active_task_count if self._bus else 0)

        self._prom_task_counter = prometheus_client.Counter(
            'infinity_bus_task_total',
            '处理器任务完成次数（按结果）',
            labelnames=['handler', 'outcome'],
            registry=registry,
        )
        self._prom_task_duration = prometheus_client.Histogram(
            'infinity_bus_task_duration_seconds',
            '处理器任务执行耗时（秒）',
            labelnames=['handler'],
            buckets=list(self._histogram_bounds),
            registry=registry,
        )
        self._prom_task_wait = prometheus_client.Histogram(
            'infinity_bus_task_wait_seconds',
            '处理器任务排队耗时（秒）',
            labelnames=['handler'],
            buckets=list(self._histogram_bounds),
            registry=registry,
        )

        self._prom_integrated = True
        logger.info('MetricsMiddleware 已桥接到 prometheus_client (registry=%s)', registry)

    def integrate_opentelemetry(self, meter: Any = None) -> None:
        """桥接到 ``opentelemetry-api`` 官方 SDK。

        调用后，每次事件发布将同步写入 OTel Counter / Histogram，
        可通过 OTLP exporter 导出到 Collector / Jaeger / Prometheus。

        参数
        ----
        meter:
            ``opentelemetry.metrics.Meter`` 实例。
            若为 ``None``，自动创建名为 ``"infinity_bus"`` 的 Meter。

        异常
        ----
        ImportError:
            未安装 ``opentelemetry-api`` 时抛出。
        """
        if self._otel_integrated:
            return

        try:
            otel_metrics = importlib.import_module('opentelemetry.metrics')
        except ImportError as exc:
            raise ImportError('opentelemetry-api 未安装，请执行: pip install opentelemetry-api') from exc

        if meter is None:
            meter = otel_metrics.get_meter('infinity_bus')

        self._otel_meter = meter

        self._otel_counter = meter.create_counter(
            'infinity_bus.publish_total',
            description='事件发布总次数',
        )
        self._otel_histogram = meter.create_histogram(
            'infinity_bus.publish_duration',
            description='事件发布耗时（秒）',
            unit='s',
        )
        self._otel_errors = meter.create_counter(
            'infinity_bus.publish_errors_total',
            description='事件发布失败次数',
        )

        # Gauge 通过回调函数采集
        def _queue_callback(options: Any) -> Any:
            return otel_metrics.Observation(self._bus.queue_size if self._bus else 0)

        self._otel_queue_gauge = meter.create_observable_gauge(
            'infinity_bus.queue_size',
            description='事件队列当前深度',
            callbacks=[_queue_callback],
        )

        def _tasks_callback(options: Any) -> Any:
            return otel_metrics.Observation(self._bus.active_task_count if self._bus else 0)

        self._otel_tasks_gauge = meter.create_observable_gauge(
            'infinity_bus.active_tasks',
            description='当前活跃处理器任务数',
            callbacks=[_tasks_callback],
        )

        self._otel_task_counter = meter.create_counter(
            'infinity_bus.task_total',
            description='处理器任务完成次数（按结果）',
        )
        self._otel_task_duration = meter.create_histogram(
            'infinity_bus.task_duration',
            description='处理器任务执行耗时（秒）',
            unit='s',
        )
        self._otel_task_wait = meter.create_histogram(
            'infinity_bus.task_wait',
            description='处理器任务排队耗时（秒）',
            unit='s',
        )

        self._otel_integrated = True
        logger.info('MetricsMiddleware 已桥接到 opentelemetry-api (meter=%s)', meter)

    @property
    def prometheus_integrated(self) -> bool:
        """是否已桥接 prometheus_client。"""
        return self._prom_integrated

    @property
    def opentelemetry_integrated(self) -> bool:
        """是否已桥接 opentelemetry-api。"""
        return self._otel_integrated

    # ------------------------------------------------------------------
    # 快照
    # ------------------------------------------------------------------

    def snapshot(self) -> MetricsSnapshot:
        """返回当前所有指标的只读快照。"""
        duration_snap: Dict[str, Buckets] = {name: hist.snapshot() for name, hist in self._publish_duration.items()}
        task_duration_snap: Dict[str, Buckets] = {name: hist.snapshot() for name, hist in self._task_duration.items()}
        task_wait_snap: Dict[str, Buckets] = {name: hist.snapshot() for name, hist in self._task_wait.items()}
        return MetricsSnapshot(
            publish_total=dict(self._publish_total),
            publish_duration_sec=duration_snap,
            publish_errors_total=self._publish_errors_total,
            queue_size=self._bus.queue_size if self._bus else 0,
            active_tasks=self._bus.active_task_count if self._bus else 0,
            task_total={name: dict(counts) for name, counts in self._task_total.items()},
            task_duration_sec=task_duration_snap,
            task_wait_sec=task_wait_snap,
        )
