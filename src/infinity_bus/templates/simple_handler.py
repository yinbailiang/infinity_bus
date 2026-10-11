"""函数到 :class:`EventHandler` 的装饰器转换：将同步/异步函数自动包装为事件处理器。"""

from inspect import isawaitable, signature
from typing import Any, Awaitable, Callable, Optional, Type, Union, cast

from pydantic import BaseModel

from .. import Event, EventBus, EventDeclaration, EventHandler

NoPayloadHandlerFunc = Callable[[], Union[Awaitable[None], None]]
PayloadHandlerFunc = Callable[[Any], Union[Awaitable[None], None]]
HandlerFunc = Union[NoPayloadHandlerFunc, PayloadHandlerFunc]


class GenericEventHandler(EventHandler):
    def __init__(self) -> None:
        raise NotImplementedError('请使用 @handler 装饰器生成处理器子类。')


def handler(
    event_decl: Type[EventDeclaration],
    *,
    handle_timeout: Optional[float] = 32.0,
) -> Callable[[HandlerFunc], Type[GenericEventHandler]]:
    """将异步函数转换为 :class:`EventHandler` 子类的装饰器。"""

    event_name: str = event_decl.name

    def decorator(func: HandlerFunc) -> Type[GenericEventHandler]:
        """校验函数签名并生成对应的 :class:`EventHandler` 子类。"""
        sig = signature(func)
        params = list(sig.parameters.values())

        if event_decl.payload_type is not None:
            if not params:
                raise TypeError(
                    f'事件 {event_name} 要求负载参数，'
                    f'但处理器 {func.__name__}() 未定义参数。\n'
                    f'请修改为: {func.__name__}(payload: {event_decl.payload_type.__name__}) -> None: ...'
                )
            else:
                first_annotation = params[0].annotation
                if first_annotation is not sig.empty and first_annotation is not event_decl.payload_type:
                    raise TypeError(
                        f'处理器 {func.__name__} 参数类型应为 {event_decl.payload_type.__name__}，'
                        f'而不是 {getattr(first_annotation, "__name__", first_annotation)!r}。\n'
                        f'请修改为: {func.__name__}(payload: {event_decl.payload_type.__name__}) -> None: ...'
                    )
        else:
            if params:
                raise TypeError(
                    f'事件 {event_name} 无负载，'
                    f'但处理器 {func.__name__} 定义了参数。\n'
                    f'请修改为: {func.__name__}() -> None: ...'
                )

        _func_has_params = bool(params)

        class _Handler(GenericEventHandler):
            """由 :func:`handler` 装饰器生成的处理器子类。"""

            def __init__(self) -> None:
                EventHandler.__init__(self, subscriptions=[event_decl.name], handle_timeout=handle_timeout)

            async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
                if _func_has_params:
                    assert event_decl.payload_type is not None
                    assert payload is not None
                    assert isinstance(payload, event_decl.payload_type)
                    result = cast(PayloadHandlerFunc, func)(payload)
                else:
                    assert event_decl.payload_type is None
                    assert payload is None
                    result = cast(NoPayloadHandlerFunc, func)()

                if isawaitable(result):
                    await result

        _Handler.__name__ = func.__name__
        _Handler.__qualname__ = func.__qualname__
        _Handler.__module__ = func.__module__
        _Handler.__doc__ = func.__doc__

        return _Handler

    return decorator
