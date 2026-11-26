"""分片入口的兼容命名。

实现集中在 :mod:`quaybell.assignments`；这里用领域词汇 ``partitions`` 再导出常用
函数，方便以 ``from quaybell import partitions`` 方式复用。
"""
from .assignments import (  # noqa: F401
    MAX_PARTITIONS,
    authorize,
    channel_view,
    claim_context,
    claim_guard,
    configure,
    configure_channel,
    current,
    fence_now,
    fence_open_leases,
    join,
    key_rows,
    member_view,
    partition_for,
    summary,
    touch_member,
    view,
)

# 常见别名称呼
partition_for_key = partition_for
key_partition = partition_for
resolve_partition = partition_for
assign = configure
create_assignment = configure
ownership = view
get_assignment = current


def rebalance(db, channel, group_name, raw, now):
    """以再平衡语义提交一次分配（允许使用已登记成员）。"""
    return configure(db, channel, group_name, raw or {}, now, rebalance=True)
