import pytest

from infinity_bus import EventDeclaration, EventRegistry


@pytest.mark.asyncio
async def test_empty_event_name():
    """验证定义空事件名时抛出 TypeError"""
    with pytest.raises(TypeError):

        class EmptyNameEvent(EventDeclaration):  # pyright: ignore[reportUnusedClass]
            name = ''


@pytest.mark.asyncio
async def test_event_register_crud(empty_event_registry: EventRegistry):
    """测试事件注册表的增删查功能"""

    class SampleEvent(EventDeclaration):
        name = 'sample.event'

    empty_event_registry.register(SampleEvent)
    assert empty_event_registry.get(SampleEvent.name) == SampleEvent

    empty_event_registry.unregister(SampleEvent.name)
    assert empty_event_registry.get(SampleEvent.name) is None


@pytest.mark.asyncio
async def test_event_duplicate_register(empty_event_registry: EventRegistry):
    """重复注册同名事件应抛出 ValueError"""

    class DupEvent(EventDeclaration):
        name = 'dup.event'

    empty_event_registry.register(DupEvent)
    with pytest.raises(ValueError, match='重复的事件声明'):
        empty_event_registry.register(DupEvent)


@pytest.mark.asyncio
async def test_event_list_names(empty_event_registry: EventRegistry):
    """测试 list_names 返回所有已注册事件名"""

    class A(EventDeclaration):
        name = 'a.event'

    class B(EventDeclaration):
        name = 'b.event'

    empty_event_registry.register(A)
    empty_event_registry.register(B)
    assert set(empty_event_registry.list_names()) == {'a.event', 'b.event'}


@pytest.mark.asyncio
async def test_event_with_payload_type():
    """测试带 payload_type 的事件声明"""

    from pydantic import BaseModel

    class MyPayload(BaseModel):
        x: int

    class PayloadEvent(EventDeclaration):
        name = 'payload.event'
        payload_type = MyPayload

    reg = EventRegistry()
    reg.register(PayloadEvent)
    decl = reg.get('payload.event')
    assert decl is not None
    assert decl.payload_type is MyPayload
