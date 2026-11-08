"""SQLite 审计存储后端。"""

import json
import logging
import re
import time
from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, Field

from .record import AuditRecord
from .sink import AuditSink

_aiosqlite: Any = None
_aiosqlite_import_error: ImportError | None = None

try:
    import aiosqlite as _aiosqlite
except ImportError as _error:
    _aiosqlite_import_error = _error

logger = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r'[A-Za-z_][A-Za-z0-9_]*\Z')

_CORE_COLUMNS: tuple[str, ...] = (
    'ts',
    'emitted_at',
    'service',
    'name',
    'event_id',
    'event_ids',
    'sources',
    'timestamps',
    'payload_size',
    'payload_sha256',
    'data',
    'error',
)

_CORE_COLUMN_NAMES = frozenset(_CORE_COLUMNS)

_MIGRATION_COLUMNS: dict[str, str] = {
    'ts': 'REAL NOT NULL DEFAULT 0',
    'emitted_at': 'REAL',
    'service': "TEXT NOT NULL DEFAULT ''",
    'name': "TEXT NOT NULL DEFAULT ''",
    'event_id': "TEXT NOT NULL DEFAULT ''",
    'event_ids': "TEXT NOT NULL DEFAULT '[]'",
    'sources': "TEXT NOT NULL DEFAULT '[]'",
    'timestamps': "TEXT NOT NULL DEFAULT '[]'",
    'payload_size': 'INTEGER NOT NULL DEFAULT 0',
    'payload_sha256': 'TEXT',
    'data': 'TEXT',
    'error': 'TEXT',
}


def _require_identifier(value: str, kind: str) -> None:
    """拒绝可被拼进 SQL 的非法标识符。"""
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f'{kind} 必须是合法 SQL 标识符: {value!r}')


class RetentionPolicy(BaseModel):
    """审计表保留策略。

    三个上限相互正交，任一超出都从最老的记录开始删除；
    全部为 ``None`` 时不执行清理。
    """

    retention_seconds: float | None = Field(default=None, gt=0, description='按写入时刻保留的秒数')
    max_rows: int | None = Field(default=None, gt=0, description='保留的最大行数')
    max_bytes: int | None = Field(default=None, gt=0, description='主数据库文件的目标逻辑字节数')


class SQLiteSink(AuditSink):
    """把审计记录批量写入 SQLite，并按需执行保留策略与磁盘回收。

    生产特性：WAL、批量事务写入、失败重连（由中间件驱动）、
    三档保留策略、``incremental_vacuum`` 回收、旧表增量迁移。
    """

    def __init__(
        self,
        db_path: str = ':memory:',
        *,
        table_name: str = 'event_log',
        retention: RetentionPolicy | None = None,
        prune_interval: float = 60.0,
        prune_batch: int = 1000,
        extra_columns: Sequence[str] | None = None,
        busy_timeout: float = 5.0,
        vacuum: bool = True,
    ) -> None:
        _require_identifier(table_name, 'table_name')
        if prune_interval < 0:
            raise ValueError('prune_interval 不能为负')
        if prune_batch < 1:
            raise ValueError('prune_batch 必须为正整数')
        self._db_path = db_path
        self._table = table_name
        self._retention = retention
        self._prune_interval = prune_interval
        self._prune_batch = prune_batch
        self._busy_timeout = busy_timeout
        self._vacuum = vacuum
        self._extra_columns = list(extra_columns or [])
        self._extra_names: list[str] = []
        for fragment in self._extra_columns:
            name = fragment.strip().split(maxsplit=1)[0] if fragment.strip() else ''
            _require_identifier(name, 'extra_columns')
            self._extra_names.append(name)
        if len(set(self._extra_names)) != len(self._extra_names):
            raise ValueError('extra_columns 存在重复列名')
        collisions = _CORE_COLUMN_NAMES.intersection(self._extra_names)
        if collisions:
            raise ValueError(f'extra_columns 与审计字段重名: {sorted(collisions)}')
        self._conn: Any = None
        self._last_prune_at = 0.0

    @property
    def is_open(self) -> bool:
        """连接是否已打开。"""
        return self._conn is not None

    @property
    def db_path(self) -> str:
        """数据库路径。"""
        return self._db_path

    @property
    def table_name(self) -> str:
        """审计表名。"""
        return self._table

    async def open(self) -> None:
        """打开连接、初始化表结构并补齐旧表缺失的列。"""
        if self._conn is not None:
            return
        if _aiosqlite is None:
            raise ImportError('SQLiteSink 需要 aiosqlite 包，请执行: pip install infinity_bus[templates]') from (
                _aiosqlite_import_error
            )
        connection = await _aiosqlite.connect(self._db_path)
        try:
            await connection.execute(f'PRAGMA busy_timeout = {int(self._busy_timeout * 1000)}')
            await connection.execute('PRAGMA journal_mode = WAL')
            await connection.execute('PRAGMA synchronous = NORMAL')
            await connection.execute('PRAGMA auto_vacuum = INCREMENTAL')
            connection.row_factory = _aiosqlite.Row
            await connection.execute(self._create_table_sql())
            await self._migrate(connection)
            await connection.execute(f'CREATE INDEX IF NOT EXISTS idx_{self._table}_ts ON {self._table}(ts)')
            await connection.commit()
            await self._enable_incremental_vacuum(connection)
        except Exception:
            await connection.close()
            raise
        self._conn = connection
        self._last_prune_at = 0.0
        logger.info('SQLiteSink 已打开: %s (table=%s)', self._db_path, self._table)

    async def write(self, records: Sequence[AuditRecord]) -> None:
        """在单个事务中批量写入记录。"""
        connection = self._conn
        if connection is None:
            raise RuntimeError('SQLiteSink 未打开')
        if not records:
            return
        columns = ', '.join((*_CORE_COLUMNS, *self._extra_names))
        placeholders = ', '.join('?' for _ in range(len(_CORE_COLUMNS) + len(self._extra_names)))
        sql = f'INSERT INTO {self._table} ({columns}) VALUES ({placeholders})'
        try:
            await connection.executemany(sql, [self._row(record) for record in records])
            await connection.commit()
        except Exception:
            await connection.rollback()
            raise

    async def maintain(self) -> None:
        """按 ``prune_interval`` 限频执行保留策略。"""
        if self._retention is None or self._conn is None:
            return
        now = time.monotonic()
        if now - self._last_prune_at < self._prune_interval:
            return
        self._last_prune_at = now
        await self._prune()

    async def close(self) -> None:
        """提交并关闭连接。"""
        connection = self._conn
        self._conn = None
        if connection is None:
            return
        try:
            await connection.commit()
        except Exception:
            logger.exception('SQLiteSink 关闭前提交失败')
        await connection.close()

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    def _create_table_sql(self) -> str:
        extra = ''.join(f',\n    {fragment}' for fragment in self._extra_columns)
        return f"""\
CREATE TABLE IF NOT EXISTS {self._table} (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ts             REAL    NOT NULL DEFAULT 0,
    emitted_at     REAL,
    service        TEXT    NOT NULL DEFAULT '',
    name           TEXT    NOT NULL,
    event_id       TEXT    NOT NULL,
    event_ids      TEXT    NOT NULL DEFAULT '[]',
    sources        TEXT    NOT NULL DEFAULT '[]',
    timestamps     TEXT    NOT NULL DEFAULT '[]',
    payload_size   INTEGER NOT NULL DEFAULT 0,
    payload_sha256 TEXT,
    data           TEXT,
    error          TEXT{extra}
)"""

    async def _migrate(self, connection: Any) -> None:
        cursor = await connection.execute(f'PRAGMA table_info({self._table})')
        rows = await cursor.fetchall()
        await cursor.close()
        existing = {str(row[1]) for row in rows}
        for name, definition in _MIGRATION_COLUMNS.items():
            if name not in existing:
                await connection.execute(f'ALTER TABLE {self._table} ADD COLUMN {name} {definition}')
        for fragment, name in zip(self._extra_columns, self._extra_names):
            if name not in existing:
                await connection.execute(f'ALTER TABLE {self._table} ADD COLUMN {fragment}')

    async def _enable_incremental_vacuum(self, connection: Any) -> None:
        if not self._vacuum or self._retention is None or self._retention.max_bytes is None:
            return
        cursor = await connection.execute('PRAGMA auto_vacuum')
        row = await cursor.fetchone()
        await cursor.close()
        if row is not None and int(row[0]) == 2:
            return
        await connection.execute('PRAGMA auto_vacuum = INCREMENTAL')
        await connection.execute('VACUUM')
        await connection.commit()
        logger.info('SQLiteSink 已对旧库执行一次 VACUUM 以启用 incremental_vacuum')

    def _row(self, record: AuditRecord) -> tuple[Any, ...]:
        return (
            record.ts,
            record.emitted_at,
            record.service,
            record.name,
            record.event_id,
            json.dumps(record.event_ids, ensure_ascii=False),
            json.dumps(record.sources, ensure_ascii=False),
            json.dumps(record.timestamps, ensure_ascii=False),
            record.payload_size,
            record.payload_sha256,
            record.data,
            record.error,
            *(self._bind_extra(record.extra.get(name)) for name in self._extra_names),
        )

    @staticmethod
    def _bind_extra(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bytes)):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

    async def _prune(self) -> None:
        connection = self._conn
        policy = self._retention
        if connection is None or policy is None:
            return
        deleted = 0
        if policy.retention_seconds is not None:
            cutoff = time.time() - policy.retention_seconds
            deleted += await self._delete(
                f'id IN (SELECT id FROM {self._table} WHERE ts < ? ORDER BY id ASC LIMIT ?)',
                (cutoff, self._prune_batch),
            )
        if policy.max_rows is not None:
            deleted += await self._delete(
                f'id IN (SELECT id FROM {self._table} WHERE id <= '
                f'(SELECT id FROM {self._table} ORDER BY id DESC LIMIT 1 OFFSET ?) '
                f'ORDER BY id ASC LIMIT ?)',
                (policy.max_rows, self._prune_batch),
            )
        if policy.max_bytes is not None:
            deleted += await self._prune_bytes(policy.max_bytes)
        if deleted:
            await connection.execute(f'PRAGMA incremental_vacuum({self._prune_batch})')
            await connection.commit()
            logger.debug('SQLiteSink 保留策略删除 %d 行', deleted)

    async def _delete(self, where: str, params: Sequence[Any]) -> int:
        connection = self._conn
        if connection is None:
            return 0
        cursor = await connection.execute(f'DELETE FROM {self._table} WHERE {where}', tuple(params))
        deleted = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount > 0 else 0
        await cursor.close()
        await connection.commit()
        return int(deleted)

    async def _prune_bytes(self, max_bytes: int) -> int:
        connection = self._conn
        if connection is None:
            return 0
        deleted_total = 0
        for _ in range(4):
            size = await self._db_size(connection)
            if size <= max_bytes:
                break
            rows = await self._row_count(connection)
            if rows == 0:
                break
            average = max(size // rows, 1)
            excess = size - max_bytes
            delete_count = min(self._prune_batch, max(1, (excess + average - 1) // average))
            deleted = await self._delete(
                f'id IN (SELECT id FROM {self._table} ORDER BY id ASC LIMIT ?)',
                (delete_count,),
            )
            if deleted == 0:
                break
            deleted_total += deleted
            await connection.execute(f'PRAGMA incremental_vacuum({self._prune_batch})')
            await connection.commit()
        return deleted_total

    async def _db_size(self, connection: Any) -> int:
        page_count_cursor = await connection.execute('PRAGMA page_count')
        page_count_row = await page_count_cursor.fetchone()
        await page_count_cursor.close()
        page_size_cursor = await connection.execute('PRAGMA page_size')
        page_size_row = await page_size_cursor.fetchone()
        await page_size_cursor.close()
        if page_count_row is None or page_size_row is None:
            return 0
        return int(page_count_row[0]) * int(page_size_row[0])

    async def _row_count(self, connection: Any) -> int:
        cursor = await connection.execute(f'SELECT COUNT(*) FROM {self._table}')
        row = await cursor.fetchone()
        await cursor.close()
        return int(row[0]) if row is not None else 0
