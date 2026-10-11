"""Service handler 工具箱：协议、异常、响应构造、装饰器。"""

import inspect
import logging
from collections.abc import Awaitable, Callable
from types import MappingProxyType
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict

from infinity_bus import Event, EventBus, EventDeclaration, EventHandler
from infinity_bus.templates.request import RequestProtocol, ResponseProtocol, build_response

logger = logging.getLogger(__name__)


class ServiceError(Exception):
    """业务失败基类。

    - ``str(e)`` → response.error_msg
    - ``e.body`` → 合并进 response 的其余字段
    """

    def __init__(self, message: str, **body: Any) -> None:
        super().__init__(message)
        self.body = body


class NotFoundError(ServiceError):
    """目标资源不存在。"""


class ConflictError(ServiceError):
    """状态冲突（重复注册、并发修改等）。"""


class InvalidRequestError(ServiceError):
    """请求参数不合法。"""


class AccessDeniedError(ServiceError):
    """权限不足。"""


class ProcessorMeta(BaseModel):
    """单个 processor 的登记信息：处理方法名与请求/响应事件声明。"""

    model_config = ConfigDict(frozen=True)

    func_name: str
    request: type[EventDeclaration]
    response: type[EventDeclaration] | None


type ProcessorFunc[Handler: ServiceHandler, Payload: RequestProtocol] = Callable[
    [Handler, Payload, EventBus.Proxy, Event],
    Awaitable[dict[str, Any] | None] | dict[str, Any] | None,
]


def process[Handler: ServiceHandler, Payload: RequestProtocol](
    request: type[EventDeclaration], response: type[EventDeclaration] | None = None
) -> Callable[[ProcessorFunc[Handler, Payload]], ProcessorFunc[Handler, Payload]]:
    """登记一个处理方法。

    被装饰方法返回 ``dict[str, Any]`` 或 ``None``：

      - ``dict``  →  ``build_response(payload, response_cls, **ret)``
      - ``None``  →  不回应（纯副作用）
      - 抛 ``ServiceError``  →  自动转 ``success=False``
      - 抛其他异常  →  照常向上抛
    """

    if request.payload_type is None:
        raise TypeError(f'请求事件 {request.name} 载荷不可为空')
    if not issubclass(request.payload_type, RequestProtocol):
        raise TypeError(
            f'请求事件 {request.name} 载荷 {request.payload_type.__name__} 必须是 {RequestProtocol.__name__}'
        )

    if response is not None:
        if response.payload_type is None:
            raise TypeError(f'响应事件 {response.name} 载荷不可为空')
        if not issubclass(response.payload_type, ResponseProtocol):
            raise TypeError(
                f'响应事件 {response.name} 载荷 {response.payload_type.__name__} 必须是 {ResponseProtocol.__name__}'
            )

    def decorator(
        func: ProcessorFunc[Handler, Payload],
    ) -> ProcessorFunc[Handler, Payload]:
        setattr(
            func,
            '_process_meta',
            ProcessorMeta(func_name=func.__name__, request=request, response=response),
        )
        return func

    return decorator


# ======================================================================
# 基类
# ======================================================================


class ServiceHandler(EventHandler):
    """继承即得：订阅自动生成、分发自动查表、响应自动构造、异常自动转换。

    子类只需写 ``@process(...)`` 方法。
    """

    # 每个子类在 __init_subclass__ 里独立填充
    _processors: ClassVar[MappingProxyType[str, ProcessorMeta]] = MappingProxyType({})

    @property
    def processors(self) -> MappingProxyType[str, ProcessorMeta]:
        """已登记的 processor 表（键为请求事件名，只读视图）。"""
        return self._processors

    # ------------------------------------------------------------------
    # 类创建时自动收集
    # ------------------------------------------------------------------

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        table: dict[str, ProcessorMeta] = {}
        for klass in reversed(cls.__mro__):  # 基类先、子类后：子类覆写同名方法会覆盖父类登记
            for name, attr in vars(klass).items():
                meta: ProcessorMeta | None = getattr(attr, '_process_meta', None)
                if meta is None:
                    continue
                table[meta.request.name] = ProcessorMeta(
                    func_name=name,  # 用当前类的名字，兼容覆写
                    request=meta.request,
                    response=meta.response,
                )

        cls._processors = MappingProxyType(table)
        logger.debug(
            '%s 收集到 %d 个 processor: %s',
            cls.__name__,
            len(table),
            sorted(table.keys()),
        )

    # ------------------------------------------------------------------
    # 实例化：把订阅列表交给 EventHandler
    # ------------------------------------------------------------------

    def __init__(self) -> None:
        super().__init__(subscriptions=[*self._processors.keys()])

    # ------------------------------------------------------------------
    # 唯一入口：查表 → 调用 → 构造响应 → 发布
    # ------------------------------------------------------------------

    async def handle(
        self,
        payload: BaseModel | None,
        bus_proxy: EventBus.Proxy,
        raw_event: Event,
    ) -> None:
        """按事件名查表分发到对应 processor，并自动构造/发布响应。

        未登记事件、负载类型不匹配或空负载会被静默忽略；processor 抛出的
        ``ServiceError`` 转为 ``success=False`` 的响应（无响应事件时向上抛出）。
        """
        if payload is None:
            return

        meta: ProcessorMeta | None = self._processors.get(raw_event.name)
        if meta is None:
            logger.debug('忽略未登记的事件 %s', raw_event.name)
            return
        assert meta.request.payload_type is not None
        assert issubclass(meta.request.payload_type, RequestProtocol)
        if not isinstance(payload, meta.request.payload_type):
            logger.debug('忽略事件 %s 的不匹配 payload %s', raw_event.name, type(payload).__name__)
            return

        func: ProcessorFunc[Any, Any] = getattr(type(self), meta.func_name)

        # --- 调用（兼容 sync / async）---
        try:
            body = func(self, payload, bus_proxy, raw_event)
            if inspect.isawaitable(body):
                body = await body
        except ServiceError as e:
            if meta.response is None:
                raise
            assert meta.response.payload_type is not None
            assert issubclass(meta.response.payload_type, ResponseProtocol)
            await bus_proxy.publish(
                meta.response.name,
                build_response(
                    payload,
                    meta.response.payload_type,
                    success=False,
                    error_msg=str(e),
                    **e.body,
                ),
            )
            return

        # --- 单向事件：不回应 ---
        if meta.response is None:
            return

        if body is None:
            return

        # --- 正常响应 ---
        assert meta.response.payload_type is not None
        assert issubclass(meta.response.payload_type, ResponseProtocol)
        await bus_proxy.publish(
            meta.response.name,
            build_response(payload, meta.response.payload_type, **body or {}),
        )
