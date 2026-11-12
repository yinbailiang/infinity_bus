"""管道抽象：异常类型与 ``Pipe`` 抽象基类。"""

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from types import TracebackType
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger(__name__)


async def cancel_tasks(tasks: Iterable[asyncio.Task[Any]]) -> None:
    """取消并等待一组任务，保留调用方自身的取消信号。"""
    pending = list(tasks)
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.wait(pending)


class PipeHandshakeError(Exception):
    """管道握手失败时抛出。"""

    pass


class PipeTeardownError(Exception):
    """管道关闭过程中发生异常时抛出。"""

    pass


class PipeClosedError(Exception):
    """管道已关闭且数据已消费完时读或者关闭后写时抛出。"""

    pass


class Pipe[PayloadType: BaseModel](ABC):
    """双向异步管道抽象基类
    子类必须实现 ``open``、``close``、``send``、``receive`` 四个方法。
    """

    async def __aenter__(self) -> Pipe[PayloadType]:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        try:
            await self.close()
        except Exception:
            logger.exception('Pipe.close() 发生异常')
        return None

    @abstractmethod
    async def open(self) -> None:
        """打开管道，准备发送/接收数据。"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """关闭管道，释放资源。"""
        pass

    @abstractmethod
    async def send(self, data: PayloadType) -> None:
        """向管道写入一个 Pydantic 模型实例。"""
        pass

    @abstractmethod
    async def receive(self) -> PayloadType:
        """从管道读取下一个 Pydantic 模型实例。"""
        pass
