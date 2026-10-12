"""邮箱模式处理器基类 — 事件入队，子类自定义任务循环"""

import asyncio
import logging
import random
from abc import ABC, abstractmethod
from typing import Optional

from pydantic import BaseModel, Field

from infinity_bus import Event, EventBus, EventHandler, Regex

logger = logging.getLogger(__name__)


class MailboxConfig(BaseModel):
    """邮箱处理器配置"""

    queue_put_timeout: Optional[float] = Field(
        default=None,
        description='入队超时（秒），None 表示无限等待。',
    )
    restart_delay: float = Field(
        default=0.5,
        description='restart 策略下重启前的等待间隔（秒）。',
    )
    restart_jitter: float = Field(
        default=0.2,
        description='restart 策略下重启等待的随机偏移上限（秒），避免惊群。',
    )
    max_queue_size: int = Field(
        default=0,
        description='邮箱队列最大容量，0 表示无限制。',
    )

    def restart_sleep(self) -> float:
        """重启等待时间（含随机偏移）"""
        return self.restart_delay + random.uniform(0, self.restart_jitter)


class StopMailbox(Exception):
    """邮箱处理器停止异常，抛出后 process() 循环退出"""

    pass


class MailboxHandler(EventHandler, ABC):
    """邮箱模式处理器

    ``process()`` 后台任务的生命周期由**总线生命周期钩子**驱动：

    - :meth:`on_activate`——注册表绑定总线时启动 ``process()`` 任务；
    - :meth:`on_deactivate`——总线解绑 / 注销时取消任务并清空积压。

    ``get()`` 返回 ``(event, proxy)``——proxy 携带正确的事件链追踪。

    用法::

        class MyHandler(MailboxHandler):
            def __init__(self):
                super().__init__(subscriptions=['sim.timer.tick'])

            async def process(self) -> None:
                while True:
                    event, proxy = await self.get()
                    await proxy.publish(...)
    """

    def __init__(self, subscriptions: list[str | Regex], config: MailboxConfig | None = None) -> None:
        self._config: MailboxConfig = config or MailboxConfig()
        super().__init__(subscriptions=subscriptions.copy(), handle_timeout=None)
        self._queue: asyncio.Queue[tuple[Event, EventBus.Proxy]] = asyncio.Queue(maxsize=self._config.max_queue_size)
        self._task: asyncio.Task[None] | None = None
        self._bus: EventBus | None = None
        self._deactivated: bool = False

    # ------------------------------------------------------------------
    # 生命周期钩子 — 任务随总线激活/解绑启停
    # ------------------------------------------------------------------

    async def on_activate(self, bus: EventBus) -> None:
        """总线激活：订阅登记由基类完成；本类捕获 bus 并启动 process() 后台任务。"""
        await super().on_activate(bus)
        self._bus = bus
        self._deactivated = False
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._process_loop(), name=f'{self.__class__.__name__}._process_loop')

    async def on_deactivate(self, bus: EventBus) -> None:
        """总线下线：订阅撤销由基类完成；本类取消任务并清空积压。"""
        await super().on_deactivate(bus)
        self._deactivated = True
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
        while True:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break
        self._bus = None

    # ------------------------------------------------------------------
    # 事件入口 — 入队
    # ------------------------------------------------------------------

    async def handle(
        self,
        payload: BaseModel | None,
        bus_proxy: EventBus.Proxy,
        raw_event: Event,
    ) -> None:
        """事件到达 → 入队（process() 任务由 :meth:`on_activate` 启动）"""
        await self.put(raw_event, bus_proxy)

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def bus(self) -> EventBus | None:
        """当前绑定的 EventBus，首次 ``handle()`` 调用后可用"""
        return self._bus

    async def put(self, event: Event, proxy: EventBus.Proxy) -> None:
        """将事件入队，供 ``process()`` 处理。

        若入队期间处理器**已被下线**（deactivate / 运行中 unregister 的竞态），
        撤销本次入队并丢弃事件——处理任务已停，遗留积压将无人消费，不如显式丢弃。
        尚未激活（从未上线）时入队不受影响，事件待激活后处理。
        """
        try:
            await asyncio.wait_for(self._queue.put((event, proxy)), timeout=self._config.queue_put_timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(f'{self.__class__.__name__} 事件入队超时')
        if self._deactivated:
            # 入队与下线竞态：邮箱已整体废弃，撤销刚入队的项（撤出的可能是其它
            # 并发入队项——均属丢弃范围，可接受）
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                pass
            logger.debug('%s 丢弃下线后的迟到事件: %s', self.__class__.__name__, event.name)

    async def _process_loop(self) -> None:
        """process() 异常处理循环"""
        while True:
            try:
                await self.process()
            except asyncio.CancelledError:
                logger.info(f'{self.__class__.__name__} process() 被取消')
                break
            except StopMailbox:
                logger.info(f'{self.__class__.__name__} process() 被停止')
                break
            except Exception:
                logger.exception(f'{self.__class__.__name__} process() 异常')
                sleep_time = self._config.restart_sleep()
                logger.warning(f'{self.__class__.__name__} process() 异常，等待 {sleep_time:.3f}s 后重启')
                try:
                    await asyncio.sleep(sleep_time)
                except asyncio.CancelledError:
                    logger.info(f'{self.__class__.__name__} process() 重启等待被取消')
                    break

    # ------------------------------------------------------------------
    # 子类接口
    # ------------------------------------------------------------------

    async def get(self) -> tuple[Event, EventBus.Proxy]:
        """从邮箱取下一个 ``(事件, 总线代理)``（阻塞至有事件到达）

        proxy 携带正确的事件链，publish 时会自动追溯因果。
        """
        return await self._queue.get()

    @abstractmethod
    async def process(self) -> None:
        """子类实现：自定义任务循环，通过 ``event, proxy = await self.get()`` 逐事件处理"""
        ...
