"""消费组发布额度：在线调额、未结计数与发布准入。

额度按消费组限制该组处于未结状态（waiting/leased）的分发数量：
- 0 表示不限额度，兼容既有频道；
- 正整数表示最多同时保留多少条未结分发，ACK/死信终态立即归还，NACK/超时回到
  waiting 仍占用；
- 一次发布匹配多个组时，只要有一个组没有空位就整笔拒绝，拒绝回执列出所有卡住的组；
- 下调额度不会驱逐既有积压，也不会阻止正常消费归还额度，只在空位恢复前持续拦截新发布。
"""
from . import inputs, journal, subscriptions
from .errors import conflict

# 0 代表不限额度；上限只用于防止误操作把额度设成天文数字。
LIMIT = (0, 1_000_000)
OUTSTANDING = "state IN ('waiting','leased')"


def quota_value(raw):
    return inputs.integer(raw.get('quota', 0), *LIMIT, '发布额度')


def outstanding(db, group_id):
    row = db.execute(
        f'SELECT COUNT(*) FROM deliveries WHERE subscription_id=? AND {OUTSTANDING}', (group_id,)).fetchone()
    return row[0]


def _has_room(db, group_id, quota):
    # 借助部分索引只探测第 quota 条未结分发是否存在，扫描量受额度而非历史积压约束。
    row = db.execute(
        f'SELECT 1 FROM deliveries WHERE subscription_id=? AND {OUTSTANDING} LIMIT 1 OFFSET ?',
        (group_id, quota - 1)).fetchone()
    return row is None


def status(db, group):
    quota = group['publish_quota']
    open_count = outstanding(db, group['id'])
    return {'quota': quota, 'outstanding': open_count,
            'available': None if quota == 0 else max(0, quota - open_count)}


def admit(db, groups):
    """发布前的整笔准入；任一匹配组满额即抛出带卡住明细的冲突错误。"""
    blocked = []
    for group in groups:
        quota = group['publish_quota']
        if quota == 0 or _has_room(db, group['id'], quota):
            continue
        open_count = outstanding(db, group['id'])
        blocked.append({'group': group['name'], 'quota': quota,
                        'outstanding': open_count, 'requested': 1,
                        'available': max(0, quota - open_count)})
    if blocked:
        names = '、'.join(item['group'] for item in blocked)
        raise conflict(f'发布被额度拒绝：消费组 {names} 已无空位',
                       {'code': 'quota_exhausted', 'blocked': blocked})


def set_quota(db, channel, group_name, raw, now):
    inputs.require(raw, ('quota',))
    desired = quota_value(raw)
    record = subscriptions.get(db, channel, group_name)
    previous = record['publish_quota']
    if previous != desired:
        db.execute('UPDATE subscriptions SET publish_quota=? WHERE id=?', (desired, record['id']))
        journal.append(db, channel, now, 'quota_adjusted',
                       {'group': group_name, 'previous': previous, 'quota': desired})
        record = subscriptions.get(db, channel, group_name)
    return record | status(db, record)
