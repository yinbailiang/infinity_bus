"""双向异步管道（Pipe）模板：握手走总线，数据走管道。

设计要点
--------
- **控制面与数据面分离**：``open_pipe`` / ``expect_pipe`` 通过事件总线完成握手
  （路由、超时与应答复用 ``request`` / ``expect`` 模板），实际数据传输由 ``Pipe``
  实现自行负责，不再经过总线。
- **同进程**：内置 ``InProcessPipeAllocator`` 为两端各创建一个端点，共享同一
  双向通道、两个方向独立缓冲（不会读到本端发出的数据）。
- **跨进程**：两端各持一个 ``Pipe`` 对象（各自的本地缓冲 + 各自的传输句柄），以
  ``pipe_id`` 为会合键；``Pipe`` 对象本身不会被传递。``pipe_id`` 是不透明字符串，
  其编码方式（地址、令牌、参数）与数据面连接的建立方式，由两端的 ``PipeAllocator``
  自行约定，本模板不做任何假设。
- **无默认分配器**：``open_pipe`` / ``expect_pipe`` 必须显式传入 ``allocator``，
  不存在模块级默认实例；用哪个作用域（进程内 / 跨进程）由调用方决定。
- **拨号时机**：跨进程实现必须在 ``allocate()`` 阶段发起数据面连接。``expect_pipe``
  在发布握手响应**之前**调用 ``allocator.checkout(pipe_id)``，若服务端在该调用中等待一个
  「响应之后才会发生」的拨号，双方会互相等待直至握手超时。
- **严格 P2P**：一个 ``pipe_id`` 只对应一对端点，管道不承担多路复用；``expect_pipe``
  为一次性接管，不应并发调用去争抢同一个握手请求。
- **关闭是终态**：``PipeClosedError`` 表示管道已关闭且缓冲已耗尽；重连需要重新握手
  并分配新的 ``pipe_id``。

模块布局
--------
- ``base``：异常类型与 ``Pipe`` 抽象基类
- ``allocator``：分配器子包（``PipeAllocator`` 抽象 + 进程内 / ``multiprocessing`` / WebSocket 实现）
- ``protocol``：``PipeOpenRequest`` / ``PipeLinkedResponse``
- ``handshake``：``open_pipe`` / ``expect_pipe``
"""

from . import allocator
from .base import Pipe, PipeClosedError, PipeHandshakeError, PipeTeardownError
from .handshake import expect_pipe, open_pipe
from .protocol import PipeLinkedResponse, PipeOpenRequest

__all__ = [
    # 分配器子包（抽象 + 内置实现）
    'allocator',
    # 异常
    'PipeHandshakeError',
    'PipeTeardownError',
    'PipeClosedError',
    # 抽象
    'Pipe',
    # 握手负载
    'PipeOpenRequest',
    'PipeLinkedResponse',
    # 握手模板
    'open_pipe',
    'expect_pipe',
]
