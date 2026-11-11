"""管道分配器子包：抽象基类与内置实现。

- ``InProcessPipeAllocator``：同进程，创建共享双向通道的端点对；
- ``ProcessPipeServerAllocator`` / ``ProcessPipeClientAllocator``：进程间，
  基于 ``multiprocessing.connection`` 的 Listener / Client（标准库）；
- ``WsPipeServerAllocator`` / ``WsPipeClientAllocator``：WebSocket，
  基于 aiohttp（``pip install infinity_bus[ws]``）。

用法::

    from infinity_bus.templates.pipe import allocator

    pipe_allocator = allocator.InProcessPipeAllocator()
"""

from .base import PipeAllocator, SingleRoleAllocator
from .inprocess import InProcessPipe, InProcessPipeAllocator
from .process import ProcessPipe, ProcessPipeClientAllocator, ProcessPipeServerAllocator
from .ws import WsPipe, WsPipeClientAllocator, WsPipeServerAllocator

__all__ = [
    'PipeAllocator',
    'SingleRoleAllocator',
    'InProcessPipe',
    'InProcessPipeAllocator',
    'ProcessPipe',
    'ProcessPipeClientAllocator',
    'ProcessPipeServerAllocator',
    'WsPipe',
    'WsPipeClientAllocator',
    'WsPipeServerAllocator',
]
