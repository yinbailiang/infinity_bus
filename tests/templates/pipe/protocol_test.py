"""握手负载模型（``PipeOpenRequest`` / ``PipeLinkedResponse``）测试。"""

import pytest
from pydantic import ValidationError

from infinity_bus.templates.pipe import PipeLinkedResponse, PipeOpenRequest


def test_pipe_open_request_requires_all_ids() -> None:
    """缺少 session_id / request_id / pipe_id 时校验失败。"""
    with pytest.raises(ValidationError):
        PipeOpenRequest.model_validate({'pipe_id': 'p1'})


def test_pipe_open_request_fields() -> None:
    """完整字段可正常构造。"""
    req = PipeOpenRequest(pipe_id='p1', session_id='s1', request_id='r1')
    assert req.pipe_id == 'p1'
    assert req.session_id == 's1'
    assert req.request_id == 'r1'


def test_pipe_linked_response_defaults() -> None:
    """响应默认成功且无错误信息。"""
    resp = PipeLinkedResponse(session_id='s1', request_id='r1')
    assert resp.success is True
    assert resp.error_msg is None


def test_pipe_linked_response_raise_if_failed() -> None:
    """success=False 且有错误信息时抛出 RuntimeError。"""
    resp = PipeLinkedResponse(session_id='s1', request_id='r1', success=False, error_msg='boom')
    with pytest.raises(RuntimeError, match='boom'):
        resp.raise_if_failed()


def test_pipe_linked_response_failed_without_message() -> None:
    """success=False 但没有错误信息时不抛异常。"""
    resp = PipeLinkedResponse(session_id='s1', request_id='r1', success=False)
    resp.raise_if_failed()
