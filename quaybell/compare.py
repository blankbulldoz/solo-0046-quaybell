"""兼容入口：两组对照能力的短模块名。"""
from .comparison import (group_side, pair_from_body, pair_from_query,  # noqa: F401
                         require_pair, snapshot)


def compare(db, message_id, first, second, now):
    return snapshot(db, message_id, first, second, now)
