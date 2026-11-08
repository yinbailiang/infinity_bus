"""审计日志存储后端（接口 + 内置实现）。"""

from .base import AuditSink, LogFallback
from .jsonl import JSONLSink
from .sqlite import RetentionPolicy, SQLiteSink

__all__ = [
    'AuditSink',
    'JSONLSink',
    'LogFallback',
    'RetentionPolicy',
    'SQLiteSink',
]
