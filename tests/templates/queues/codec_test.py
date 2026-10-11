"""EventCodec 线格式编解码器测试：往返保真 + 「完美 Event」契约。"""

from datetime import datetime, timezone

from conftest import BusTestPayload, TestEventDecl

from infinity_bus import Event, EventRegistry
from infinity_bus.templates.queues import EventCodec


def _registry() -> EventRegistry:
    reg = EventRegistry()
    reg.register(TestEventDecl)
    return reg


def _payload_event() -> Event:
    """带负载的测试事件（data 为已校验 BusTestPayload 实例）。"""
    return Event(name='test.event', data=BusTestPayload(value=7, msg='hi'), sources=['s1'])


def test_payload_roundtrip_rebuilds_instance() -> None:
    """有负载事件往返后 data 被重建为具体 payload 实例（完美 Event），元数据保留。"""
    codec = EventCodec(_registry())
    decoded = codec.decode(codec.encode(_payload_event()))
    assert isinstance(decoded.data, BusTestPayload)
    assert decoded.data.value == 7 and decoded.data.msg == 'hi'
    assert decoded.name == 'test.event'
    assert decoded.sources == ['s1']


def test_none_payload_roundtrip() -> None:
    """无负载事件往返：data 保持 None。"""
    codec = EventCodec()
    ev = Event(name='test.slow', data=None, sources=['a'])
    decoded = codec.decode(codec.encode(ev))
    assert decoded.data is None
    assert decoded.name == 'test.slow'
    assert decoded.sources == ['a']


def test_no_registry_keeps_raw_json() -> None:
    """未注入 registry 时 data 以原始 JSON 值透传（dict），不抛错。"""
    codec = EventCodec()
    ev = Event(name='test.event', data=BusTestPayload(value=1))
    decoded = codec.decode(codec.encode(ev))
    assert isinstance(decoded.data, dict)
    assert decoded.data == {'value': 1, 'msg': 'test'}


def test_unregistered_event_keeps_raw_json() -> None:
    """registry 缺少该事件声明时 data 原样透传（不抛错）。"""
    codec = EventCodec(_registry())
    ev = Event(name='other.unknown', data=BusTestPayload(value=1))
    decoded = codec.decode(codec.encode(ev))
    assert isinstance(decoded.data, dict)


def test_timestamps_roundtrip() -> None:
    """元数据 timestamps（datetime）经线格式往返保持类型与值。"""
    codec = EventCodec()
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    ev = Event(name='test.slow', data=None)
    ev.timestamps.append(ts)
    decoded = codec.decode(codec.encode(ev))
    assert decoded.timestamps == [ts]
