import logging
import uuid
from abc import ABC
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Optional, Type

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class Event(BaseModel):
    """事件数据类"""

    name: str = Field(description='事件类型')
    data: Optional[BaseModel] = Field(default=None, description='事件附加数据')

    # metadata
    id: str = Field(default_factory=lambda: uuid.uuid4().hex, description='事件UUID')
    sources: List[str] = Field(default_factory=list, description='事件处理链')
    timestamps: List[datetime] = Field(default_factory=lambda: [], description='事件时间戳')
    event_ids: List[str] = Field(default_factory=list, description='因果事件ID链，支持精准重建事件流转路径')


class EventDeclaration(ABC):
    """事件声明抽象基类"""

    name: ClassVar[str]
    payload_type: ClassVar[Optional[Type[BaseModel]]] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.name.strip():
            raise TypeError(f'事件声明类 {cls.__name__} 必须定义非空的 `name` 属性')


class EventRegistry:
    """事件注册表"""

    def __init__(self) -> None:
        self._events: Dict[str, Type[EventDeclaration]] = {}
        self._version: int = 0

    @property
    def version(self) -> int:
        """注册表版本号，每次变更递增。"""
        return self._version

    def register(self, event_decl: Type[EventDeclaration]) -> None:
        """手动注册事件声明"""
        if event_decl.name in self._events:
            raise ValueError(f'重复的事件声明 {event_decl.name}')
        self._events[event_decl.name] = event_decl
        self._version += 1

    def unregister(self, event_name: str) -> None:
        """注销事件声明"""
        if event_name in self._events:
            del self._events[event_name]
            self._version += 1

    def get(self, name: str) -> Optional[Type[EventDeclaration]]:
        return self._events.get(name)

    def list_names(self) -> List[str]:
        return list(self._events.keys())
