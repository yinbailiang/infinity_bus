"""管道帧载荷编解码测试。"""

import pytest
from pydantic import BaseModel, ValidationError

from infinity_bus.templates.pipe.codec import decode_payload, encode_payload


class Payload(BaseModel):
    """测试用负载模型。"""

    value: int
    msg: str = 'default'


class OtherPayload(BaseModel):
    """字段不兼容的另一个模型。"""

    name: str


def test_encode_decode_roundtrip() -> None:
    """编码后可还原为原类型与字段。"""
    original = Payload(value=42, msg='hello')

    decoded = decode_payload(encode_payload(original), Payload)

    assert isinstance(decoded, Payload)
    assert decoded == original


def test_decode_validates_payload_type() -> None:
    """按不兼容的类型解码时校验失败。"""
    raw = encode_payload(Payload(value=1))

    with pytest.raises(ValidationError):
        decode_payload(raw, OtherPayload)
