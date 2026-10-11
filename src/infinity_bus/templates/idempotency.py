"""幂等机制：可注入的 IdempotencyRecorder（uuid 去重）与 IdempotentHandler。

at-least-once 语义下（未 ack 断连重投 / restart 补投）事件可能被重复投递。事件自带
唯一 ``Event.id``（uuid4 hex）作去重键，由 :class:`IdempotencyRecorder` 记录「已处理
标记」。策略可注入替换：进程内内存版、SQLite 持久版（处理完成日志 = 幂等表）等。
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from typing import Any, List, Optional, Protocol, Union

from infinity_bus import Event, EventBus, EventHandler, Regex


class IdempotencyRecorder(Protocol):
    """幂等记录器协议：已处理标记的存取（uuid 去重策略的存储抽象，可注入替换）。

    语义：消费方在处理**前**查 ``is_processed``（已处理则跳过），处理**成功后**再
    ``mark_processed`` —— 失败不标记，at-least-once 重投才会重试。``consumer`` 区分
    泛洪下各成员（同一事件每个成员各自记录自己的处理，互不串扰）。
    """

    async def is_processed(self, consumer: str, event_id: str) -> bool:
        """返回 (consumer, event_id) 是否已处理。"""
        ...

    async def mark_processed(self, consumer: str, event_id: str) -> None:
        """标记 (consumer, event_id) 已处理（重复标记应幂等安全）。"""
        ...


class InMemoryIdempotencyRecorder:
    """进程内内存幂等记录器：最简单的注入策略（进程内去重）。"""

    def __init__(self) -> None:
        """构造空去重表。"""
        self._seen: set[tuple[str, str]] = set()

    async def is_processed(self, consumer: str, event_id: str) -> bool:
        """(consumer, event_id) 是否已处理。"""
        return (consumer, event_id) in self._seen

    async def mark_processed(self, consumer: str, event_id: str) -> None:
        """记录已处理（幂等：重复添加无副作用）。"""
        self._seen.add((consumer, event_id))


class SqliteIdempotencyRecorder:
    """SQLite 持久幂等记录器 ——「处理完成日志 = 幂等表」，跨进程/跨重启去重。

    零第三方依赖：stdlib ``sqlite3`` + ``asyncio.to_thread``（不阻塞事件循环）。
    表 ``processed_log`` 主键 (consumer, event_id)，``INSERT OR IGNORE`` 使并发重复的
    mark 幂等安全（主键约束兜底）—— 同一份持久存储可兼作「处理完成」审计与去重表。
    """

    def __init__(self, db_path: str = ':memory:') -> None:
        """指定 SQLite 数据库文件路径（默认内存库）。"""
        self._db_path = db_path
        self._conn: Any = None
        self._lock = asyncio.Lock()  # 串行化 sqlite 访问（同一连接不跨线程）

    async def start(self) -> None:
        """连接 SQLite 并确保 ``processed_log`` 表存在。"""

        def _init() -> Any:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.execute(
                'CREATE TABLE IF NOT EXISTS processed_log ('
                ' consumer TEXT NOT NULL,'
                ' event_id TEXT NOT NULL,'
                ' ts TEXT NOT NULL,'
                ' PRIMARY KEY (consumer, event_id))'
            )
            conn.commit()
            return conn

        async with self._lock:
            self._conn = await asyncio.to_thread(_init)

    async def is_processed(self, consumer: str, event_id: str) -> bool:
        """(consumer, event_id) 是否已处理。"""
        async with self._lock:
            return await asyncio.to_thread(self._query, consumer, event_id)

    def _query(self, consumer: str, event_id: str) -> bool:
        cur = self._conn.execute('SELECT 1 FROM processed_log WHERE consumer=? AND event_id=?', (consumer, event_id))
        return cur.fetchone() is not None

    async def mark_processed(self, consumer: str, event_id: str) -> None:
        """记录已处理（INSERT OR IGNORE：重复标记安全）。"""
        async with self._lock:
            await asyncio.to_thread(self._insert, consumer, event_id)

    def _insert(self, consumer: str, event_id: str) -> None:
        self._conn.execute(
            'INSERT OR IGNORE INTO processed_log (consumer, event_id, ts) VALUES (?, ?, ?)',
            (consumer, event_id, datetime.now(timezone.utc).isoformat()),
        )
        self._conn.commit()

    async def close(self) -> None:
        """关闭 SQLite 连接（幂等）。"""
        async with self._lock:
            if self._conn is not None:
                await asyncio.to_thread(self._conn.close)
                self._conn = None


class IdempotentHandler(EventHandler):
    """幂等处理器基类：注入 recorder/consumer，把「查重 + 成功记」从业务里抽出。

    覆写 ``__call__``（``EventBus`` 调用 handler 的入口）：已处理 → 直接返回（丢弃
    重复投递）；未处理 → 执行 ``handle``，成功后 ``mark_processed`` —— ``handle``
    抛错则不标记，留给 at-least-once 重投重试。策略由注入的 recorder 决定（内存 /
    SQLite / 自定义）。
    """

    def __init__(
        self,
        subscriptions: Optional[List[Union[Regex, str]]],
        recorder: IdempotencyRecorder,
        consumer: str,
        handle_timeout: Optional[float] = 32.0,
    ) -> None:
        """构造幂等处理器。

        subscriptions: 订阅的事件名/正则（同 ``EventHandler``）。
        recorder: 幂等记录器（注入策略）。
        consumer: 消费方标识（泛洪下各成员区分，避免互相串扰去重）。
        """
        super().__init__(subscriptions, handle_timeout)
        self._recorder: IdempotencyRecorder = recorder
        self._consumer: str = consumer

    async def __call__(self, bus: EventBus, event: Event) -> None:
        """总线入口：查重 → 执行 handle → 成功后记录；已处理则跳过。"""
        if await self._recorder.is_processed(self._consumer, event.id):
            return  # 已处理：uuid 去重，丢弃重复投递
        await super().__call__(bus, event)  # 执行 handle（抛错则不进入下一行）
        await self._recorder.mark_processed(self._consumer, event.id)
