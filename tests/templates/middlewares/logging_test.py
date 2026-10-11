"""日志中间件测试：SQLiteLogging / JSONLLogging。"""

import asyncio
import json
import os
import tempfile
from typing import Any, List, Optional

import pytest
from conftest import (
    SimplePingHandler,
)
from pydantic import BaseModel

from infinity_bus import (
    Event,
    EventBus,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    MiddlewareChain,
)
from infinity_bus.templates.middlewares import (
    JSONLLoggingMiddleware,
    SQLiteLoggingMiddleware,
)


def _read_lines(path: str) -> List[str]:
    """同步读取文件全部行（供 asyncio.to_thread 使用）。"""
    with open(path, encoding='utf-8') as f:
        return f.readlines()


# ============================================================================
# 共用 fixtures
# ============================================================================


@pytest.fixture
def chain() -> MiddlewareChain:
    return MiddlewareChain()


class _StartupAnnouncerHandler(EventHandler):
    """在激活阶段创建后台任务、总线开放发布后立即广播消息（模拟处理器启动发布）。"""

    def __init__(self) -> None:
        super().__init__(subscriptions=[])
        self._announce_task: Optional[asyncio.Task[None]] = None

    def on_activate(self, bus: EventBus) -> None:
        super().on_activate(bus)
        self._announce_task = asyncio.create_task(self._announce(bus))

    async def _announce(self, bus: EventBus) -> None:
        # 任务首次被调度时发布开关已同步置位（activate 与 enable 之间无挂起点）
        await bus.proxy('announcer').publish('mw.ping', {'key': 'startup', 'count': 1})

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        pass


class _SlowSetupJSONL(JSONLLoggingMiddleware):
    """放大启动窗口：on_setup 前延迟，用于回归验证启动期发布不丢日志。"""

    async def on_setup(self, bus: EventBus) -> None:
        await asyncio.sleep(0.05)
        await super().on_setup(bus)


class _SlowSetupSQLite(SQLiteLoggingMiddleware):
    """同 :class:`_SlowSetupJSONL`，SQLite 版本。"""

    async def on_setup(self, bus: EventBus) -> None:
        await asyncio.sleep(0.05)
        await super().on_setup(bus)


# ============================================================================
# SQLiteLoggingMiddleware
# ============================================================================


class TestSQLiteLoggingMiddleware:
    """aiosqlite 日志中间件（含降级）"""

    @pytest.mark.asyncio
    async def test_logs_event_to_memory_db(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """事件发布后被记录到内存 SQLite"""

        mw = SQLiteLoggingMiddleware(':memory:')
        chain = MiddlewareChain()
        await chain.add(mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy('test_src').publish('mw.ping', {'key': 'hello', 'count': 1})
            await handler.wait_received(timeout=2.0)

            # 查询 SQLite 确认写入（在连接关闭前查询）
            assert mw.is_connect
            cursor = await mw._conn.execute(  # pyright: ignore[reportPrivateUsage]
                f'SELECT name, sources, data FROM {mw._table}'  # pyright: ignore[reportPrivateUsage]
            )
            rows = await cursor.fetchall()
            assert len(rows) >= 1
            assert rows[0]['name'] == 'mw.ping'
            sources = json.loads(rows[0]['sources'])
            assert 'test_src' in sources

    @pytest.mark.asyncio
    async def test_logs_to_file_db(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """日志写入文件数据库"""

        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        try:
            mw = SQLiteLoggingMiddleware(db_path)
            chain = MiddlewareChain()
            await chain.add(mw)

            handler = SimplePingHandler()
            handler_registry.register(handler)

            bus = EventBus(
                base_event_registry,
                handler_registry,
                queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
                middleware_chain=chain,
            )
            async with bus:
                await bus.proxy('src').publish('mw.ping', {'key': 'file', 'count': 42})
                await handler.wait_received(timeout=2.0)

            # 文件应该存在且非空
            assert await asyncio.to_thread(os.path.exists, db_path)
            assert await asyncio.to_thread(os.path.getsize, db_path) > 0
            assert mw.is_connect
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass

    @pytest.mark.asyncio
    async def test_on_publish_error_logs_fallback(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """发布异常时 on_publish_error 写入 fallback"""
        fallback_lines: List[str] = []

        def fake_fallback(line: str) -> None:
            fallback_lines.append(line)

        mw = SQLiteLoggingMiddleware(':memory:', fallback=fake_fallback)
        chain = MiddlewareChain()
        await chain.add(mw)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:

            async def _noop(error: Exception, name: str, source: str, data: dict[str, Any] | BaseModel | None) -> None:
                pass

            await chain.build_on_publish_error(_noop)(ValueError('boom'), 'test.event', 'src', {'key': 'v'})

        assert len(fallback_lines) >= 1
        record = json.loads(fallback_lines[0])
        assert 'ValueError' in record['error']

    @pytest.mark.asyncio
    async def test_startup_publish_from_handler_is_logged(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """回归：处理器在激活阶段发布的消息必须写入 SQLite——
        中间件先于处理器初始化，启动窗口内不存在「目标未就绪」竞态。"""
        fallback_lines: List[str] = []
        mw = _SlowSetupSQLite(':memory:', fallback=fallback_lines.append)
        chain = MiddlewareChain()
        await chain.add(mw)

        ping = SimplePingHandler()
        handler_registry.register(ping)
        handler_registry.register(_StartupAnnouncerHandler())

        bus = EventBus(
            base_event_registry,
            handler_registry,
            queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
            middleware_chain=chain,
        )
        async with bus:
            await ping.wait_received(timeout=2.0)

            cursor = await mw._conn.execute('SELECT name FROM event_log')  # pyright: ignore[reportPrivateUsage]
            rows = await cursor.fetchall()
            assert any(row['name'] == 'mw.ping' for row in rows)

        assert fallback_lines == []


# ============================================================================
# JSONLLoggingMiddleware
# ============================================================================


class TestJSONLLoggingMiddleware:
    """JSONL 文件日志中间件（含降级）"""

    @pytest.mark.asyncio
    async def test_logs_event_to_file(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """事件发布后被记录到 JSONL 文件"""
        with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as f:
            file_path = f.name

        try:
            mw = JSONLLoggingMiddleware(file_path)
            chain = MiddlewareChain()
            await chain.add(mw)

            handler = SimplePingHandler()
            handler_registry.register(handler)

            bus = EventBus(
                base_event_registry,
                handler_registry,
                queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
                middleware_chain=chain,
            )
            async with bus:
                await bus.proxy('test_src').publish(
                    'mw.ping',
                    {'key': 'hello', 'count': 1},
                )
                await handler.wait_received(timeout=2.0)

            # 文件应该存在且非空
            assert await asyncio.to_thread(os.path.exists, file_path)
            assert await asyncio.to_thread(os.path.getsize, file_path) > 0

            # 验证 JSONL 内容
            lines = await asyncio.to_thread(_read_lines, file_path)
            assert len(lines) >= 1
            record = json.loads(lines[0])
            assert record['name'] == 'mw.ping'
            assert 'test_src' in record['sources']
        finally:
            try:
                os.unlink(file_path)
            except OSError:
                pass

    @pytest.mark.asyncio
    async def test_fallback_on_unwritable_path(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """当目标路径的父目录是一个文件（非目录）时，降级到 fallback"""
        fallback_lines: List[str] = []

        def fake_fallback(line: str) -> None:
            fallback_lines.append(line)

        # 创建临时文件，然后将其作为"目录"使用，触发 os.makedirs 失败
        with tempfile.NamedTemporaryFile(delete=False) as parent_file:
            parent_path = parent_file.name

        try:
            # 把 JSONL 路径指向 parent_file/sub.jsonl，但 parent_file 是文件不是目录
            jsonl_path = os.path.join(parent_path, 'sub.jsonl')
            mw = JSONLLoggingMiddleware(
                jsonl_path,
                fallback=fake_fallback,
            )
            chain = MiddlewareChain()
            await chain.add(mw)

            handler = SimplePingHandler()
            handler_registry.register(handler)

            bus = EventBus(
                base_event_registry,
                handler_registry,
                queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
                middleware_chain=chain,
            )
            async with bus:
                await bus.proxy('src').publish(
                    'mw.ping',
                    {'key': 'fallback', 'count': 99},
                )
                await handler.wait_received(timeout=2.0)

            assert len(fallback_lines) >= 1
            record = json.loads(fallback_lines[0])
            assert record['name'] == 'mw.ping'
        finally:
            try:
                os.unlink(parent_path)
            except OSError:
                pass

    @pytest.mark.asyncio
    async def test_on_publish_error_logs_fallback(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """发布异常时 on_publish_error 写入 fallback"""
        fallback_lines: List[str] = []

        def fake_fallback(line: str) -> None:
            fallback_lines.append(line)

        with tempfile.TemporaryDirectory() as tmpdir:
            mw = JSONLLoggingMiddleware(os.path.join(tmpdir, 'events.jsonl'), fallback=fake_fallback)
            chain = MiddlewareChain()
            await chain.add(mw)

            bus = EventBus(
                base_event_registry,
                handler_registry,
                queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
                middleware_chain=chain,
            )
            async with bus:

                async def _noop(
                    error: Exception, name: str, source: str, data: dict[str, Any] | BaseModel | None
                ) -> None:
                    pass

                await chain.build_on_publish_error(_noop)(
                    ValueError('boom'),
                    'test.event',
                    'src',
                    {'key': 'v'},
                )

            assert len(fallback_lines) >= 1
            record = json.loads(fallback_lines[0])
            assert 'ValueError' in record['error']

    @pytest.mark.asyncio
    async def test_startup_publish_from_handler_is_logged(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """回归：处理器在激活阶段发布的消息必须写入 JSONL——
        中间件先于处理器初始化，启动窗口内不存在「目标未就绪」竞态。"""
        with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as f:
            file_path = f.name

        try:
            fallback_lines: List[str] = []
            mw = _SlowSetupJSONL(file_path, fallback=fallback_lines.append)
            chain = MiddlewareChain()
            await chain.add(mw)

            ping = SimplePingHandler()
            handler_registry.register(ping)
            handler_registry.register(_StartupAnnouncerHandler())

            bus = EventBus(
                base_event_registry,
                handler_registry,
                queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=10)),
                middleware_chain=chain,
            )
            async with bus:
                await ping.wait_received(timeout=2.0)

            lines = await asyncio.to_thread(_read_lines, file_path)
            assert any(json.loads(line)['name'] == 'mw.ping' for line in lines)
            assert fallback_lines == []
        finally:
            try:
                os.unlink(file_path)
            except OSError:
                pass
