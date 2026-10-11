"""Event ↔ bytes 线格式编解码器（跨进程 EventQueue 边界用）。

「完美 Event 队列」契约要求跨进程队列在边界把字节解码回「完美 Event」（``Event.data``
为已校验 ``BaseModel`` 实例）。本模块提供该线格式的官方实现 :class:`EventCodec`。
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, List, Optional, Type

from pydantic import BaseModel

from infinity_bus import Event, EventRegistry

PayloadType = Type[BaseModel]
"""负载模型类别名：``EventDeclaration.payload_type`` 的类型。"""


class EventCodec:
    """「完美 Event」的线格式编解码器：``encode`` / ``decode`` 往返不丢类型语义。

    背景：``Event.data`` 注解为抽象 ``Optional[BaseModel]``，Pydantic 2 会给抽象
    BaseModel 字段装 mock serializer/validator —— ``model_dump_json`` 会把真实负载
    dump 成空对象 ``{}``；``model_validate_json`` 则解出不可用 mock 实例。因此本
    编解码器采用自定义信封：

    - ``encode``：元数据与 data 分开 dump（data 为 ``BaseModel`` 时按具体类型单独
      dump，规避 mock 序列化）；
    - ``decode``：用 ``model_construct`` 保留 data 为原始 JSON 值，再按注册表
      （``name → payload_type``）重建为校验实例。

    跨进程 ``EventQueue`` 应在边界使用本编解码器：``put`` 前 ``encode``、``get``
    前 ``decode`` —— 队列两侧始终是完美 Event（见 ``EventQueue`` 抽象契约）。
    """

    def __init__(self, registry: Optional[EventRegistry] = None) -> None:
        """构造编解码器。

        registry:
            事件注册表（可选）。提供时按 ``payload_type`` 重建负载；为 ``None`` 时
            解码负载保持原始 JSON 值（适用于无负载事件或纯透传场景）。
        """
        self._registry: Optional[EventRegistry] = registry

    def encode(self, event: Event) -> bytes:
        """把 Event 编码为线格式 bytes（自定义信封，UTF-8 JSON）。

        data 若为 ``BaseModel`` 实例按具体类型 dump（mode='json' 处理 datetime/uuid
        等）；否则（None / 原始 JSON 值）原样输出。元数据经 ``model_dump(exclude=
        {'data'})`` 输出，避免抽象字段 mock 序列化。
        """
        body: dict[str, Any] = event.model_dump(mode='json', exclude={'data'})
        body['data'] = event.data.model_dump(mode='json') if isinstance(event.data, BaseModel) else event.data
        return json.dumps(body).encode('utf-8')

    def decode(self, data: bytes) -> Event:
        """把线格式 bytes 解码为 Event，并按注册表重建负载为「完美 Event」。

        不能走 ``Event.model_validate_json``（抽象 ``BaseModel`` 字段的 mock 问题）；
        用 ``model_construct`` 保留 data 为原始 JSON 值，再经 :meth:`_rebuild_data`
        重建。registry 缺失、事件未注册或 ``payload_type`` 为 None（无负载事件）时
        data 原样透传，不抛错。
        """
        raw: Any = json.loads(data)
        event = Event.model_construct(
            name=raw['name'],
            data=raw.get('data'),
            id=raw.get('id'),
            sources=raw.get('sources', []),
            timestamps=self._decode_timestamps(raw.get('timestamps') or []),
            event_ids=raw.get('event_ids', []),
        )
        return self._rebuild_data(event)

    @staticmethod
    def _decode_timestamps(values: List[Any]) -> List[datetime]:
        """把线格式中的 ISO 时间字符串还原为 datetime（无法解析的条目跳过）。"""
        decoded: List[datetime] = []
        for value in values or []:
            if isinstance(value, datetime):
                decoded.append(value)
                continue
            try:
                decoded.append(datetime.fromisoformat(str(value)))
            except (TypeError, ValueError):
                continue  # 非标准时间戳：跳过该条，其余保真
        return decoded

    def _rebuild_data(self, event: Event) -> Event:
        """把 JSON 化的 data 按注册表重建为具体 payload 实例（其余情况原样透传）。"""
        if event.data is None:
            return event
        payload_type = self._payload_type(event.name)
        if payload_type is None:
            return event
        if isinstance(event.data, payload_type):  # 单进程直通 / 已重建 → 无需再转
            return event
        return event.model_copy(update={'data': payload_type.model_validate(event.data)})

    def _payload_type(self, name: str) -> Optional[PayloadType]:
        """注册表中事件声明的负载模型类；无 registry 或未注册时返回 None。"""
        if self._registry is None:
            return None
        decl = self._registry.get(name)
        return decl.payload_type if decl is not None else None
