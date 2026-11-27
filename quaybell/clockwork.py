"""内部时间基准：Unix 整数毫秒。

存储与领域计算一律使用整数毫秒；对外合同（HTTP JSON、quaybell-trace/1 证据）
仍按 Unix 秒浮点解释，由本模块在边界层显式换算，避免内部毫秒值泄漏到旧客户端。
"""
from time import time_ns

MILLIS_PER_SECOND = 1000

CHANNEL_FIELDS = ('created',)
POLICY_FIELDS = ('created',)
SUBSCRIPTION_FIELDS = ('created',)
MESSAGE_FIELDS = ('published', 'available')
DELIVERY_FIELDS = ('ready_at', 'terminal_at')
ATTEMPT_FIELDS = ('started', 'expires', 'finished')
RETENTION_FIELDS = ('published', 'cutoff')


def now_ms():
    """当前时刻的 Unix 整数毫秒（截断亚毫秒部分）。"""
    return time_ns() // 1_000_000


def to_seconds(value):
    """整数毫秒 -> Unix 秒浮点；NULL 透传。"""
    if value is None:
        return None
    return value / MILLIS_PER_SECOND


def stamp(record, fields):
    """复制记录并把指定毫秒字段换算为对外的 Unix 秒。"""
    result = dict(record)
    for field in fields:
        if field in result:
            result[field] = to_seconds(result[field])
    return result


def channel(record):
    return stamp(record, CHANNEL_FIELDS)


def policy(record):
    return stamp(record, POLICY_FIELDS)


def subscription(record):
    return stamp(record, SUBSCRIPTION_FIELDS)


def message(record):
    return stamp(record, MESSAGE_FIELDS)


def delivery(record):
    return stamp(record, DELIVERY_FIELDS)


def attempt(record):
    return stamp(record, ATTEMPT_FIELDS)


def lease(record):
    return stamp(record, ('expires',))


def outcome(record):
    return stamp(record, ('effective_at',))


def retention_row(record):
    return stamp(record, RETENTION_FIELDS)


def event(record):
    """日志事件：顶层 at 与 details 内嵌的 expires/effective_at 都按秒呈现。"""
    result = stamp(record, ('at',))
    details = result.get('details')
    if isinstance(details, dict):
        for field in ('expires', 'effective_at'):
            if field in details:
                details = details | {field: to_seconds(details[field])}
        result['details'] = details
    return result
