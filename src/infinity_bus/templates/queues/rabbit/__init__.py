"""RabbitMQ fanout 传输策略：跨进程 EventQueue 实现（需 aio-pika，惰性导入）。"""

from .queue import EXCHANGE, URL, RabbitFanoutQueue

__all__ = [
    'RabbitFanoutQueue',
    'URL',
    'EXCHANGE',
]
