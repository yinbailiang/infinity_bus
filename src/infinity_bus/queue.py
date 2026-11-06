"""可替换的事件队列抽象 —— 总线内部派发队列的注入接口。

``EventBus`` 只依赖本模块定义的 :class:`EventQueue` 抽象进行事件派发，
不感知任何具体队列实现及其配置；有界/无界、持久化、优先级、跨进程等
语义全部由调用方注入的具体实现自行决定。
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from .event import Event


class InMemoryEventQueueConfig(BaseModel):
    """进程内事件队列的自身配置。"""

    maxsize: int = Field(
        default=1024,
        description='队列最大容量，0 表示无限制（沿用 asyncio.Queue 语义）。',
    )


class EventQueue(ABC):
    """事件队列抽象基类。

    定义总线派发所依赖的最小队列协议：

    - 发布端 ``put``：入队，队列已满时阻塞（背压）；
    - 分发端 ``get`` + ``task_done``：取事件并回报**已派发完成**（携带事件身份）；
    - 停机时 ``join``：等待事件全部派发，保证不丢事件；
    - ``qsize``：暴露积压深度供观测与停机超时估算。

    **「完美 Event 队列」契约**：``put`` 的 Event 与 ``get`` 返回的 Event 均满足
    ``Event.data`` 为**已校验 ``BaseModel`` 实例**（或 None）的不变量。内存实现天然
    满足；跨进程实现须在边界完成编解码与按注册表重建（参考
    :class:`~infinity_bus.templates.queues.codec.EventCodec`），使总线与处理器只见
    完美 Event —— 跨进程语义等同单进程语义。

    具体实现（进程内 / 持久化 / 优先级 / 跨进程）由调用方构造后注入
    :class:`~infinity_bus.bus.EventBus`。
    """

    @abstractmethod
    async def put(self, event: Event) -> None:
        """将一个事件入队；队列已满时阻塞直至有空位（背压）。"""

    @abstractmethod
    async def get(self) -> Event:
        """取出队首事件；队列为空时阻塞直至有事件到达。"""

    @abstractmethod
    def task_done(self, event: Event) -> None:
        """标记一个已取出的事件**已派发完成**，供 ``join`` 判定「事件已完整派发」使用。

        调用时机由总线保证：该事件的所有处理器任务**都获得执行权之后**调用一次；
        任务成败不参与判定（总线只承诺派发，不承诺业务处理成功）。

        ``event`` 让跨进程实现在「并发启动顺序与取出顺序不同」时精确定位对应的
        消息。这是唯一语义：不提供按取出顺序结清的兼容路径。
        """

    def task_abandoned(self, event: Event) -> None:
        """事件未能完整派发（仍有处理器任务未启动）时由总线调用：停止等待该事件。

        默认实现按「已结清」处理（等价于 ``task_done(event)``）：进程内队列没有
        重投能力，保留未结清计数只会让后续 ``join`` 永久等待。具备重投能力的实现
        应覆写此方法：从本地追踪移除，但**不 ack**，让消息在通道关闭时由 broker 重投。
        """
        self.task_done(event)

    @abstractmethod
    def qsize(self) -> int:
        """返回当前待处理事件数。"""

    @abstractmethod
    async def join(self) -> None:
        """阻塞直至所有已入队事件均被取出并完成 ``task_done(event)``（事件已完整派发）。"""


class InMemoryEventQueue(EventQueue):
    """基于 ``asyncio.Queue`` 的进程内默认实现，支持有界背压。

    队列自身的容量等配置由 :class:`InMemoryEventQueueConfig` 决定，
    与总线无关；缺省为有界队列（容量 1024）。
    """

    def __init__(self, config: InMemoryEventQueueConfig | None = None) -> None:
        """构造进程内事件队列。"""
        self._config: InMemoryEventQueueConfig = config or InMemoryEventQueueConfig()
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=self._config.maxsize)

    async def put(self, event: Event) -> None:
        """将一个事件入队；队列已满时阻塞直至有空位（背压）。"""
        await self._queue.put(event)

    async def get(self) -> Event:
        """取出队首事件；队列为空时阻塞直至有事件到达。"""
        return await self._queue.get()

    def task_done(self, event: Event) -> None:
        """标记一个已取出的事件已派发完成（进程内无需按身份定位）。"""
        self._queue.task_done()

    def qsize(self) -> int:
        """返回当前待处理事件数。"""
        return self._queue.qsize()

    async def join(self) -> None:
        """阻塞直至所有已入队事件均被取出并完成 ``task_done(event)``（事件已完整派发）。"""
        await self._queue.join()
