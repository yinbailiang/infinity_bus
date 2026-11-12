"""跨进程管道帧的载荷编解码：按 ``payload_type`` 往返 JSON。"""

from pydantic import BaseModel


def encode_payload(data: BaseModel) -> bytes:
    """把负载编码为 UTF-8 JSON 字节（保留具体类型的字段）。"""
    return data.model_dump_json().encode('utf-8')


def decode_payload[PayloadType: BaseModel](raw: bytes, payload_type: type[PayloadType]) -> PayloadType:
    """按指定类型校验并重建负载。"""
    return payload_type.model_validate_json(raw)
