"""infinity_bus.templates.queues — 跨进程 EventQueue 与配套策略。

通用能力（无第三方依赖）：线格式编解码器 :class:`EventCodec`（「完美 Event」队列契约）、
传输实现按后端子包组织：``rabbit``（RabbitMQ fanout，惰性依赖 aio-pika）。
"""

from .codec import EventCodec, PayloadType
from .rabbit import EXCHANGE, URL, RabbitFanoutQueue

__all__ = [
    # 编解码器（无第三方依赖）
    'EventCodec',
    'PayloadType',
    # RabbitMQ fanout 队列（惰性依赖 aio-pika）
    'RabbitFanoutQueue',
    'URL',
    'EXCHANGE',
]
