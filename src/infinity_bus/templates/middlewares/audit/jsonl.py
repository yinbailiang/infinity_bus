"""JSONL 文件审计存储后端。"""

import asyncio
import json
import logging
import os
from collections.abc import Sequence
from typing import Any

from .record import AuditRecord
from .sink import AuditSink

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

    def __init__(self, file_path: str = 'events.jsonl', *, encoding: str = 'utf-8') -> None:
        self._file_path = file_path
        self._encoding = encoding
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
        payload = ''.join(json.dumps(record.to_json_dict(), ensure_ascii=False) + '\n' for record in records)
        async with self._write_lock:
            await handle.write(payload)
            await handle.flush()

    async def close(self) -> None:
        """关闭文件句柄。"""
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        await handle.close()
