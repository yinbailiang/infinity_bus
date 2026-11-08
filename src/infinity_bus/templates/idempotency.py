"""幂等机制：可注入的 IdempotencyRecorder（uuid 去重）与 IdempotentHandler。

at-least-once 语义下（未 ack 断连重投 / restart 补投）事件可能被重复投递。事件自带
唯一 ``Event.id``（uuid4 hex）作去重键，由 :class:`IdempotencyRecorder` 记录「已处理
标记」。策略可注入替换：进程内内存版、SQLite 持久版（处理完成日志 = 幂等表）等。
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Protocol

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

    async def claim(self, consumer: str, event_id: str) -> bool:
        """原子认领：未处理则标记并返回 ``True``，已处理返回 ``False``。

        把「查重 + 记账」合成一次原子操作，适合派发边界（``before_dispatch``）的
        去重认领。它与 :class:`IdempotentHandler` 的「成功后标记」语义不同：``claim``
        是处理前记账，失败不会自动撤回（停机放弃派发时应由中间件
        ``on_dispatch_abandoned`` 调用 :meth:`release` 撤回）。

        **认领是事件级的**：不区分 handler。若停机时事件只有部分处理器已启动，整个
        认领会被撤回，重投后已启动的处理器也可能再次执行 —— 该模式保证「未完整派发
        的事件可以重试」，不保证「已启动的处理器不重复执行」；副作用需幂等，或按
        处理器维度去重（例如在 ``consumer`` 中带上 handler id）。
        """
        ...

    async def release(self, consumer: str, event_id: str) -> None:
        """撤回认领：删除已处理标记，让重投的事件可以重新派发（幂等空操作安全）。

        事件级的撤回：若事件只有部分处理器启动就被放弃，重投后已启动的处理器也会
        再次执行（见 :meth:`claim` 的说明）。
        """
        ...


class InMemoryIdempotencyRecorder:
    """进程内内存幂等记录器：最简单的注入策略（进程内去重）。

    **有界**：记录数超过 ``max_entries`` 时按插入顺序淘汰最旧条目（FIFO）；可选的
    ``ttl_seconds`` 在读写路径上惰性过期（0 = 不过期）。被淘汰 / 过期的记录不再提供
    去重保护 —— 对进程内尽力去重可接受；需要跨重启强幂等请用 SQLite 版。
    """

    def __init__(self, *, max_entries: int = 100_000, ttl_seconds: float = 0.0) -> None:
        """构造有界去重表。

        max_entries：最多保留的记录数（0 = 无界）；超出时按插入顺序淘汰最旧条目。
        ttl_seconds：记录有效期（秒，0 = 不过期）；在读写路径上惰性清理。
        """
        if max_entries < 0:
            raise ValueError('max_entries 不能为负数')
        if ttl_seconds < 0:
            raise ValueError('ttl_seconds 不能为负数')
        self._max_entries: int = max_entries
        self._ttl_seconds: float = ttl_seconds
        self._seen: OrderedDict[tuple[str, str], float] = OrderedDict()

    def _now(self) -> float:
        return asyncio.get_running_loop().time()

    def _prune_expired(self, now: float) -> None:
        if self._ttl_seconds <= 0:
            return
        while self._seen:
            _, ts = next(iter(self._seen.items()))
            if now - ts < self._ttl_seconds:
                break
            self._seen.popitem(last=False)

    def _trim_size(self) -> None:
        if self._max_entries <= 0:
            return
        while len(self._seen) > self._max_entries:
            self._seen.popitem(last=False)

    def _record(self, consumer: str, event_id: str) -> None:
        now = self._now()
        self._prune_expired(now)
        key = (consumer, event_id)
        self._seen.pop(key, None)  # 重复标记刷新为最新（延长 TTL）
        self._seen[key] = now
        self._trim_size()

    async def is_processed(self, consumer: str, event_id: str) -> bool:
        """(consumer, event_id) 是否已处理。"""
        self._prune_expired(self._now())
        return (consumer, event_id) in self._seen

    async def mark_processed(self, consumer: str, event_id: str) -> None:
        """记录已处理（幂等：重复标记无副作用，并刷新其时间戳）。"""
        self._record(consumer, event_id)

    async def claim(self, consumer: str, event_id: str) -> bool:
        """原子认领：未处理则记录并返回 ``True``；已处理返回 ``False``。"""
        now = self._now()
        self._prune_expired(now)
        key = (consumer, event_id)
        if key in self._seen:
            return False
        self._seen[key] = now
        self._trim_size()
        return True

    async def release(self, consumer: str, event_id: str) -> None:
        """撤回认领（删除标记；不存在时无副作用）。"""
        self._seen.pop((consumer, event_id), None)


class SqliteIdempotencyRecorder:
    """SQLite 持久幂等记录器 ——「处理完成日志 = 幂等表」，跨进程/跨重启去重。

    零第三方依赖：stdlib ``sqlite3`` + ``asyncio.to_thread``（不阻塞事件循环）。
    表 ``processed_log`` 主键 (consumer, event_id)，``INSERT OR IGNORE`` 使并发重复的
    mark 幂等安全（主键约束兜底）—— 同一份持久存储可兼作「处理完成」审计与去重表。

    **写入放大**：连接开 ``journal_mode=WAL`` + ``synchronous=NORMAL``。默认参数
    （rollback journal + ``synchronous=FULL``）下每次 mark 都要 fsync，实测一条
    「查重 + 标记」约 13ms；去重是「每事件 × 每处理器」各一次，等于在派发路径上插了
    个 fsync。代价是掉电可能丢最后几个事务——对去重表可接受：少一条记录最坏等于
    「该事件被重复执行一次」，也就是没有去重时的行为。

    **无限增长**：``retention_seconds`` 内的记录才保留（默认 7 天，0 表示永久保留），
    清理在 ``start`` 时做一次，之后每 ``sweep_interval_seconds`` 在写路径上顺带做一次
    —— 不清的话这张表只增不减（实测单个服务一天 5 万行）。
    """

    def __init__(
        self,
        db_path: str = ':memory:',
        *,
        retention_seconds: float = 7 * 24 * 3600.0,
        sweep_interval_seconds: float = 600.0,
    ) -> None:
        """指定 SQLite 数据库文件路径（默认内存库）与保留期/清理间隔。

        db_path：数据库文件；``':memory:'`` 表示进程内内存库（进程退出即丢）。
        retention_seconds：去重记录保留期（秒），只要覆盖任何重投窗口即可；0 = 永不清理。
        sweep_interval_seconds：清理巡检间隔（秒），避免把扫表摊到每条事件上。
        """
        self._db_path = db_path
        self._retention_seconds: float = retention_seconds
        self._sweep_interval_seconds: float = sweep_interval_seconds
        self._last_sweep: float = 0.0
        self._conn: Any = None
        self._lock = asyncio.Lock()  # 串行化 sqlite 访问（同一连接不跨线程）

    async def start(self) -> None:
        """连接 SQLite、确保表存在、打开 WAL，并按保留期清理一次。"""

        def _init() -> Any:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute(
                'CREATE TABLE IF NOT EXISTS processed_log ('
                ' consumer TEXT NOT NULL,'
                ' event_id TEXT NOT NULL,'
                ' ts TEXT NOT NULL,'
                ' PRIMARY KEY (consumer, event_id))'
            )
            # prune 按 ts 删除；没有索引时每次清理都是全表扫描
            conn.execute('CREATE INDEX IF NOT EXISTS idx_processed_log_ts ON processed_log(ts)')
            conn.commit()
            return conn

        async with self._lock:
            self._conn = await asyncio.to_thread(_init)
            self._last_sweep = asyncio.get_running_loop().time()
            await asyncio.to_thread(self._prune)

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
            await self._maybe_sweep()

    def _insert(self, consumer: str, event_id: str) -> None:
        self._conn.execute(
            'INSERT OR IGNORE INTO processed_log (consumer, event_id, ts) VALUES (?, ?, ?)',
            (consumer, event_id, datetime.now(timezone.utc).isoformat()),
        )
        self._conn.commit()

    async def claim(self, consumer: str, event_id: str) -> bool:
        """原子认领：未处理则写入并返回 ``True``；已处理返回 ``False``。"""
        async with self._lock:
            claimed = await asyncio.to_thread(self._claim, consumer, event_id)
            await self._maybe_sweep()
            return claimed

    def _claim(self, consumer: str, event_id: str) -> bool:
        cur = self._conn.execute(
            'INSERT OR IGNORE INTO processed_log (consumer, event_id, ts) VALUES (?, ?, ?)',
            (consumer, event_id, datetime.now(timezone.utc).isoformat()),
        )
        self._conn.commit()
        return cur.rowcount == 1

    async def release(self, consumer: str, event_id: str) -> None:
        """撤回认领：删除已处理标记（不存在时无副作用）。"""
        async with self._lock:
            await asyncio.to_thread(self._release, consumer, event_id)

    def _release(self, consumer: str, event_id: str) -> None:
        self._conn.execute('DELETE FROM processed_log WHERE consumer=? AND event_id=?', (consumer, event_id))
        self._conn.commit()

    async def prune(self) -> int:
        """按保留期删除过期记录，返回删除条数（0 = 未开启清理 / 无可删）。"""
        async with self._lock:
            return await asyncio.to_thread(self._prune)

    def _prune(self) -> int:
        if self._retention_seconds <= 0:
            return 0
        cutoff = datetime.now(timezone.utc).timestamp() - self._retention_seconds
        cutoff_iso = datetime.fromtimestamp(cutoff, timezone.utc).isoformat()
        cur = self._conn.execute('DELETE FROM processed_log WHERE ts < ?', (cutoff_iso,))
        self._conn.commit()
        return int(cur.rowcount if cur.rowcount is not None and cur.rowcount > 0 else 0)

    async def _maybe_sweep(self) -> None:
        """按巡检间隔触发清理：多数写入只是顺手看一下时间戳，不扫表。"""
        if self._retention_seconds <= 0:
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_sweep < self._sweep_interval_seconds:
            return
        self._last_sweep = now
        await asyncio.to_thread(self._prune)

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
        subscriptions: list[Regex | str] | None,
        recorder: IdempotencyRecorder,
        consumer: str,
        handle_timeout: float | None = 32.0,
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
