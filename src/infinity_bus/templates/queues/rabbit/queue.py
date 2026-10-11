"""RabbitMQ fanout 跨进程 EventQueue ——「完美 Event 队列」的 RabbitMQ 实现。

多个 ``EventBus`` 注入同一 RabbitMQ fanout 后表现为**一个逻辑总线**：任一发布 →
所有在线成员收到（含自收）；成员离线语义由 ``strategy`` 决定：

- ``restart``（重启，默认）：join 停**消费**保留路由与积压 → resume 重新 consume
  后**补投**离线期积压；
- ``offline``（下线）：join 停**路由**（unbind）并把所有已路由消息消费掉 → resume
  重新 bind 后**只收新**（等价尽力而为广播）。

满足 ``EventQueue``「完美 Event 队列」契约：put 前经 :class:`EventCodec` 编码、
get 前经其解码重建，队列两侧的 ``Event.data`` 始终是已校验 ``BaseModel`` 实例。
RabbitMQ/AMQP 是 push 模型、无 Kafka 式 pause/resume 一等 API ——「暂停接收」的原语
即 ``basic.cancel``（停消费）/ ``queue.unbind``（停路由）。

用法（需运行中的 RabbitMQ broker）::

    # broker 就绪后
    import aio_pika
    from infinity_bus import EventBus, EventDeclaration, EventHandlerRegistry, EventRegistry
    from infinity_bus.templates.queues import EventCodec, RabbitFanoutQueue

    class PingEvent(EventDeclaration):
        name = 'demo.ping'
        payload_type = None

    async def member(member_id: str) -> None:
        reg = EventRegistry()
        reg.register(PingEvent)
        q = await RabbitFanoutQueue.create(member_id, registry=reg)  # restart 默认
        bus = EventBus(reg, EventHandlerRegistry(), queue=q)
        async with bus:
            await bus.proxy(member_id).publish('demo.ping', None)

已知取舍
--------
- 补投靠 durable 命名队列：不用时须显式删队列，否则残留影响下次运行。
- 消费端在途崩溃（已投递未 ack）：RabbitMQ 会重投未 ack 消息（at-least-once）；
  重复投递由 ``infinity_bus.templates.idempotency`` 的 recorder 去重（可选）。
- 负载重建依赖注入的 registry/``EventCodec``；未注册事件保持原始 JSON 值透传。

依赖：需 ``aio-pika``（``pip install infinity_bus[templates]``）。模块惰性导入，
缺失时在 :meth:`RabbitFanoutQueue.create` 抛 ``ImportError`` 提示。
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Literal, Optional

from infinity_bus import Event, EventQueue, EventRegistry

from ..codec import EventCodec

# ---------------------------------------------------------------------------
# aio-pika 惰性导入 —— 仅 RabbitFanoutQueue.create 需要
# ---------------------------------------------------------------------------
_aio_pika: Any = None
_aio_pika_import_error: Optional[ImportError] = None

try:
    import aio_pika as _aio_pika
except ImportError as _e:  # pragma: no cover - 依赖缺失路径
    _aio_pika_import_error = _e


URL = os.environ.get('RABBITMQ_URL', 'amqp://guest:guest@127.0.0.1:5672/')
"""默认 broker URL（可用环境变量 RABBITMQ_URL 覆盖）。"""

EXCHANGE = 'infinity_bus.fanout'
"""默认 fanout exchange 名。"""


def _queue_name(member_id: str) -> str:
    """成员持久队列名：durable + 非 auto-delete，才能承载 restart 策略的补投积压。"""
    return f'{EXCHANGE}.{member_id}'


class RabbitFanoutQueue(EventQueue):
    """RabbitMQ fanout 泛洪队列：每成员一条连接 + 持久命名队列。

    put = exchange.publish（broker 泛洪给所有绑定队列，含自己）；
    get = 从本地 inbound 取（订阅回调灌入，消息未 ack）；
    task_done = basic_ack（逐条确认）；
    join = 按 strategy 分离输入并排空「自己的队列」；
    resume = restart 重新 consume（补投积压）/ offline 重新 bind（只收新）。
    """

    def __init__(
        self,
        connection: Any,
        channel: Any,
        queue: Any,
        exchange: Any,
        member_id: str,
        codec: EventCodec,
        strategy: Literal['restart', 'offline'] = 'restart',
    ) -> None:
        """构造实例（通常经 :meth:`create`，不直接调用）。"""
        self._conn: Any = connection
        self._channel: Any = channel
        self._queue: Any = queue
        self._exchange: Any = exchange
        self.member_id = member_id
        self._codec: EventCodec = codec
        self._strategy: Literal['restart', 'offline'] = strategy
        self._bound: bool = True  # 队列是否绑定 exchange（offline join 后置 False）
        self._consumer_tag: Any = None
        self._consuming: bool = False  # 是否正在消费（join/close 后置 False，resume 恢复）
        self._inbound: asyncio.Queue[tuple[Any, Event]] = asyncio.Queue()
        self._unacked: list[Any] = []  # 已取出未 ack 的 message（dispatch 在途）

    @classmethod
    async def create(
        cls,
        member_id: str,
        *,
        url: str = URL,
        codec: Optional[EventCodec] = None,
        registry: Optional[EventRegistry] = None,
        strategy: Literal['restart', 'offline'] = 'restart',
    ) -> 'RabbitFanoutQueue':
        """连接 broker、声明持久队列并绑定 fanout exchange，开始消费。

        注意：队列必须 durable + 非 auto-delete，才能承载 restart 策略的补投积压。
        codec：线格式编解码器（完美 Event 契约）。缺省时用 ``EventCodec(registry)``。
        registry：事件注册表（可选）。未提供 codec 时用于构造编解码器；也兼容直接
        注入 registry 的用法。
        strategy：'restart'（重启，join 保留路由与积压 → resume 补投）或 'offline'
        （下线，join 停路由并消费掉所有已路由 → resume 只收新）。
        """
        if _aio_pika is None:
            raise ImportError(
                'RabbitFanoutQueue 需要 aio-pika 包，请执行: pip install infinity_bus[templates]'
            ) from _aio_pika_import_error
        actual_codec = codec or EventCodec(registry)
        connection = await _aio_pika.connect_robust(url)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=256)  # 预取到本地 inbound，get 不必逐条等网络
        exchange = await channel.declare_exchange(EXCHANGE, _aio_pika.ExchangeType.FANOUT, durable=True)
        queue = await channel.declare_queue(_queue_name(member_id), durable=True)
        await queue.bind(exchange)
        q = cls(connection, channel, queue, exchange, member_id, actual_codec, strategy=strategy)
        q._consumer_tag = await queue.consume(q._on_message, no_ack=False)
        q._consuming = True
        return q

    async def _on_message(self, message: Any) -> None:
        """订阅回调：解码重建（完美 Event）→ 入本地 inbound（暂不 ack）。"""
        event = self._codec.decode(message.body)
        await self._inbound.put((message, event))

    # -- EventQueue 接口 ---------------------------------------------------

    async def put(self, event: Event) -> None:
        """发布事件：broker 泛洪给所有绑定队列（含自己）。"""
        await self._exchange.publish(
            _aio_pika.Message(body=self._codec.encode(event), delivery_mode=_aio_pika.DeliveryMode.PERSISTENT),
            routing_key='',
        )

    async def get(self) -> Event:
        """取出队首事件；message 移入 unacked 等待 task_done ack。"""
        message, event = await self._inbound.get()
        self._unacked.append(message)
        return event

    def task_done(self) -> None:
        """回报一条已处理：basic_ack 确认（RabbitMQ 据此补 prefetch 配额）。"""
        if not self._unacked:
            return
        message = self._unacked.pop(0)
        # ack 是异步的而 task_done 是同步接口 → 交给事件循环异步发送
        asyncio.get_running_loop().create_task(self._ack(message))

    async def _ack(self, message: Any) -> None:
        """异步发送 basic_ack（失败静默，交由 broker 超时/重投兜底）。"""
        try:
            await message.ack()
        except Exception:
            pass

    def qsize(self) -> int:
        """本地待处理（inbound + 未 ack）：供停机超时估算。"""
        return self._inbound.qsize() + len(self._unacked)

    async def join(self) -> None:
        """按策略分离输入并排空「自己的队列」。

        - **restart（重启，默认）**：只停消费 ``basic.cancel``，保留路由与队列积压；
          排空「本地已投递」（inbound/unacked）。离线期新事件继续进队列（补投载体），
          恢复 :meth:`resume` 后**补投**。
        - **offline（下线）**：先停路由 ``queue.unbind``（队列不再增长，已路由消息仍可
          消费），保持消费把**所有已路由**消息拉完（broker 积压 → 本地 → 处理 ack，
          直到 broker 与本地全空），再停消费。离线期事件不再进队列 → 恢复 :meth:`resume`
          重新 bind 后**只收新**（等价尽力而为广播/不补投）。

        RabbitMQ/AMQP 是 push 模型，无 Kafka 式 pause/resume 一等 API：
        「暂停接收」的原语 = ``basic.cancel``（停消费）/ ``queue.unbind``（停路由）。
        """
        if self._strategy == 'offline':
            if self._bound:  # 1) 先停路由：冻结入口（unbind 前已路由的仍可消费）
                self._bound = False
                try:
                    await self._queue.unbind(self._exchange)
                except Exception:
                    pass
            await self._drain_all()  # 2) 消费掉所有已路由（broker 积压 + 本地）
            await self._cancel_consumer()
        else:
            await self._cancel_consumer()  # 保留路由与积压，仅停消费
            await self._drain_local()

    async def resume(self) -> None:
        """恢复：offline 先重新 bind（只收新）再 consume；restart 直接 consume（补投积压）。"""
        if not self._bound:
            self._bound = True
            await self._queue.bind(self._exchange)
        if not self._consuming:
            self._consumer_tag = await self._queue.consume(self._on_message, no_ack=False)
            self._consuming = True

    async def close(self) -> None:
        """停止消费并关闭连接；**队列保留**（删队列需显式 delete/purge）。"""
        await self._cancel_consumer()
        try:
            await self._conn.close()
        except Exception:
            pass

    # -- 内部：消费/排空 helpers --------------------------------------------

    async def _cancel_consumer(self) -> None:
        """取消 broker 订阅（幂等）。"""
        if self._consuming:
            self._consuming = False
            try:
                if self._consumer_tag is not None:
                    await self._queue.cancel(self._consumer_tag)
            except Exception:
                pass

    async def _drain_local(self) -> None:
        """排空本地已投递：inbound 全被 get、unacked 全 ack（restart 的排空范围）。"""
        deadline = asyncio.get_running_loop().time() + 15.0
        while self._inbound.qsize() or self._unacked:
            if asyncio.get_running_loop().time() >= deadline:
                return
            await asyncio.sleep(0.01)

    async def _drain_all(self) -> None:
        """消费掉所有已路由：broker 积压归零 且 本地排空（offline 的排空范围）。"""
        deadline = asyncio.get_running_loop().time() + 15.0
        while True:
            if self._inbound.qsize() == 0 and not self._unacked and await self._broker_backlog() == 0:
                return
            if asyncio.get_running_loop().time() >= deadline:
                return
            await asyncio.sleep(0.02)

    async def _broker_backlog(self) -> int:
        """passive declare 查询队列剩余消息数（offline 排空判定用）。"""
        try:
            qobj = await self._channel.get_queue(self._queue.name)  # ensure=True = passive
            return qobj.declaration_result.message_count
        except Exception:
            return 0
