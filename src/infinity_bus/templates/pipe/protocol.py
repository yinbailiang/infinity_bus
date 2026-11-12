"""管道握手负载模型。"""

from pydantic import Field

from ..request import RequestProtocol, ResponseProtocol


class PipeOpenRequest(RequestProtocol):
    """管道握手请求负载，携带管道唯一标识符。"""

    pipe_id: str = Field(description='管道ID')


class PipeLinkedResponse(ResponseProtocol):
    """管道握手响应负载，标准成功/失败字段由 ResponseProtocol 提供。"""

    pass
