"""``Pipe`` 抽象基类与异步上下文协议测试。"""

import pytest
from pydantic import BaseModel

from infinity_bus.templates.pipe import (
    Pipe,
    PipeClosedError,
    PipeHandshakeError,
    PipeTeardownError,
)


class _FakePipe(Pipe[BaseModel]):
    """记录 open/close 调用的最小实现。"""

    def __init__(self) -> None:
        self.open_calls = 0
        self.close_calls = 0
        self.close_error: Exception | None = None

    async def open(self) -> None:
        self.open_calls += 1

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error

    async def send(self, data: BaseModel) -> None:
        pass

    async def receive(self) -> BaseModel:
        raise NotImplementedError


def test_pipe_is_abstract() -> None:
    """Pipe 不能直接实例化，子类必须实现四个抽象方法。"""
    with pytest.raises(TypeError):
        Pipe[BaseModel]()  # type: ignore[abstract]


def test_pipe_error_hierarchy() -> None:
    """三种管道异常都继承自 Exception。"""
    for error_type in (PipeHandshakeError, PipeTeardownError, PipeClosedError):
        assert issubclass(error_type, Exception)


@pytest.mark.asyncio
async def test_async_context_manager_opens_and_closes() -> None:
    """进入上下文调用 open()，退出上下文调用 close()。"""
    pipe = _FakePipe()

    async with pipe as entered:
        assert entered is pipe
        assert pipe.open_calls == 1
        assert pipe.close_calls == 0

    assert pipe.close_calls == 1


@pytest.mark.asyncio
async def test_aexit_swallows_close_errors() -> None:
    """close() 抛出的异常不应传播到上下文之外。"""
    pipe = _FakePipe()
    pipe.close_error = RuntimeError('boom')

    await pipe.__aexit__(None, None, None)

    assert pipe.close_calls == 1
