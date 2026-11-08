"""JSONL 文件审计存储后端。"""

import asyncio
import json
import logging
import os
from collections.abc import Sequence
from typing import Any

from ..record import AuditRecord
from .base import AuditSink

_aiofiles: Any = None
_aiofiles_import_error: ImportError | None = None

try:
    import aiofiles as _aiofiles
except ImportError as _error:
    _aiofiles_import_error = _error

logger = logging.getLogger(__name__)


class JSONLSink(AuditSink):
    """把审计记录按行追加到 JSONL 文件。

    每条记录写成一行紧凑 JSON，``extra_fields`` 平铺到顶层，便于
    ``tail -f`` / ``jq`` 直接消费。父目录不存在时在 :meth:`open` 中自动创建。
    """

    def __init__(
        self,
        file_path: str = 'events.jsonl',
        *,
        encoding: str = 'utf-8',
        max_bytes: int | None = None,
        backup_count: int = 3,
    ) -> None:
        if max_bytes is not None and max_bytes < 1:
            raise ValueError('max_bytes 必须为正整数或 None')
        if backup_count < 0:
            raise ValueError('backup_count 不能为负')
        self._file_path = file_path
        self._encoding = encoding
        self._max_bytes = max_bytes
        self._backup_count = backup_count
        self._handle: Any = None
        self._write_lock = asyncio.Lock()

    @property
    def is_open(self) -> bool:
        """文件句柄是否已打开。"""
        return self._handle is not None

    @property
    def file_path(self) -> str:
        """JSONL 文件路径。"""
        return self._file_path

    @property
    def max_bytes(self) -> int | None:
        """触发轮转的当前文件大小上限。"""
        return self._max_bytes

    @property
    def backup_count(self) -> int:
        """保留的历史文件数量。"""
        return self._backup_count

    async def open(self) -> None:
        """创建父目录并以追加模式打开文件句柄。"""
        if self._handle is not None:
            return
        if _aiofiles is None:
            raise ImportError('JSONLSink 需要 aiofiles 包，请执行: pip install infinity_bus[templates]') from (
                _aiofiles_import_error
            )
        parent = os.path.dirname(self._file_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._handle = await _aiofiles.open(self._file_path, mode='a', encoding=self._encoding)
        logger.info('JSONLSink 已打开: %s', self._file_path)

    async def write(self, records: Sequence[AuditRecord]) -> None:
        """追加写入一批记录并刷新到磁盘。"""
        handle = self._handle
        if handle is None:
            raise RuntimeError('JSONLSink 未打开')
        if not records:
            return
        payload = ''.join(
            json.dumps(record.to_json_dict(), ensure_ascii=False, separators=(',', ':'), default=str) + '\n'
            for record in records
        )
        payload_size = len(payload.encode(self._encoding))
        async with self._write_lock:
            handle = self._handle
            if handle is None:
                raise RuntimeError('JSONLSink 未打开')
            if self._max_bytes is not None:
                try:
                    current_size = await asyncio.to_thread(os.path.getsize, self._file_path)
                except OSError:
                    current_size = 0
                if current_size > 0 and current_size + payload_size > self._max_bytes:
                    await self._rotate_locked()
                    handle = self._handle
                    if handle is None:
                        raise RuntimeError('JSONLSink 未打开')
            await handle.write(payload)
            await handle.flush()

    async def maintain(self) -> None:
        """当前文件达到 ``max_bytes`` 时轮转；未配置上限时无操作。"""
        if self._max_bytes is None:
            return
        async with self._write_lock:
            try:
                size = await asyncio.to_thread(os.path.getsize, self._file_path)
            except OSError:
                return
            if size >= self._max_bytes:
                await self._rotate_locked()

    async def reclaim(self) -> None:
        """写入失败后的强制轮转；未配置 ``max_bytes`` 时无操作。"""
        if self._max_bytes is None:
            return
        async with self._write_lock:
            if self._handle is not None:
                await self._rotate_locked()

    async def _rotate_locked(self) -> None:
        """在写入锁内轮转当前文件，并保留 ``backup_count`` 个历史文件。"""
        handle = self._handle
        if handle is None:
            return
        await handle.close()
        self._handle = None
        try:
            await asyncio.to_thread(self._rotate_files)
        finally:
            self._handle = await _aiofiles.open(self._file_path, mode='a', encoding=self._encoding)
        logger.info('JSONLSink 已轮转: %s', self._file_path)

    def _rotate_files(self) -> None:
        """同步执行文件重命名与旧备份清理。"""
        path = self._file_path
        if self._backup_count == 0:
            if os.path.exists(path):
                os.remove(path)
            return
        oldest = f'{path}.{self._backup_count}'
        if os.path.exists(oldest):
            os.remove(oldest)
        for index in range(self._backup_count - 1, 0, -1):
            source = f'{path}.{index}'
            if os.path.exists(source):
                os.replace(source, f'{path}.{index + 1}')
        if os.path.exists(path):
            os.replace(path, f'{path}.1')

    async def close(self) -> None:
        """关闭文件句柄。"""
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        await handle.close()
