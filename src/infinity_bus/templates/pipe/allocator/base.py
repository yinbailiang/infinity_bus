"""管道分配器抽象：分配/释放资源，以及端点领取/归还。"""

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

from ..base import Pipe


class PipeAllocator[PayloadType: BaseModel](ABC):
    """管道分配器抽象基类。

    两对方法，职责分离：

    - ``allocate`` / ``release``：**资源**生命周期，由分配方（客户端）负责；
    - ``checkout`` / ``checkin``：**端点**的领取与归还，由借用方（服务端）负责。

    作用域语义（同进程 / 跨进程）由实现决定：同进程实现为两端各持一个端点对象；
    跨进程实现各持本机端点，以 ``pipe_id`` 会合。模板不提供默认实现，
    ``open_pipe`` / ``expect_pipe`` 必须显式传入。

    ``start`` / ``stop`` 用于管理监听器、会话等分配器级资源；默认无操作。
    """

    async def start(self) -> None:
        """启动分配器级资源；默认无操作。"""

    async def stop(self) -> None:
        """释放分配器级资源；默认无操作。"""

    @abstractmethod
    async def allocate(self, **kwargs: Any) -> tuple[str, Pipe[PayloadType]]:
        """分配一条新管道，返回 ``(pipe_id, 分配方本端端点)``。

        ``kwargs`` 由实现解释：进程内实现透传给 ``create_pair()``（如 ``maxsize``），
        跨进程实现可忽略。
        """

    @abstractmethod
    async def release(self, pipe_id: str) -> None:
        """释放本端持有的管道资源并注销。跨进程实现关闭本端连接，对端通过
        EOF / ``checkin()`` 收尾。必须幂等。
        """

    @abstractmethod
    async def checkout(self, pipe_id: str) -> Pipe[PayloadType] | None:
        """借用方（服务端）领取自己的端点，不存在时返回 ``None``。"""

    @abstractmethod
    async def checkin(self, pipe_id: str) -> None:
        """借用方归还自己的端点并注销，不得销毁分配方资源。必须幂等。"""


class SingleRoleAllocator[PayloadType: BaseModel](PipeAllocator[PayloadType]):
    """只实现单一角色（客户端或服务端）的分配器基类。

    未实现的另一侧方法抛出 ``NotImplementedError``，避免把客户端分配器当服务端用。
    """

    async def allocate(self, **kwargs: Any) -> tuple[str, Pipe[PayloadType]]:
        """客户端专有：单角色分配器未实现时抛错。"""
        raise NotImplementedError(f'{type(self).__name__} 不支持 allocate()')

    async def release(self, pipe_id: str) -> None:
        """客户端专有：单角色分配器未实现时抛错。"""
        raise NotImplementedError(f'{type(self).__name__} 不支持 release()')

    async def checkout(self, pipe_id: str) -> Pipe[PayloadType] | None:
        """服务端专有：单角色分配器未实现时抛错。"""
        raise NotImplementedError(f'{type(self).__name__} 不支持 checkout()')

    async def checkin(self, pipe_id: str) -> None:
        """服务端专有：单角色分配器未实现时抛错。"""
        raise NotImplementedError(f'{type(self).__name__} 不支持 checkin()')
