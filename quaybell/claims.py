from . import attempts, delivery, eligibility, expiry, inputs, journal, policy, publication, subscriptions


def claim(db, channel, group, raw, now):
    """领取写路径：暂停闸门 -> 组内有界回收 -> 有限就绪键 -> 发租约。

    全部步骤在同一个写事务内；``eligibility.ready`` 只给键，暂停判断、
    回收编排和租约写入都留在本路径。暂停时直接返回且**不**回收，已有
    租约仍可经 ack/nack 处置，过期租约可由全局 ``/sweep`` 回收。
    """
    inputs.require(raw, ('worker',), ('limit',))
    worker = inputs.name(raw['worker'], '工作进程')
    limit = inputs.integer(raw.get('limit', 1), 1, 30, '领取上限')
    sub = subscriptions.get(db, channel, group)
    if sub['paused']:
        return {'paused': True, 'items': []}
    expiry.reap(db, now, 100, sub['id'])
    results = []
    for item in eligibility.ready(db, sub['id'], now, limit):
        rule = policy.read(db, channel, item['policy'])
        lease = attempts.open_attempt(db, item, worker, now, rule['lease_seconds'])
        delivery.lease(db, item['id'], lease['number'])
        message = publication.get(db, item['message_id'])
        journal.append(db, channel, now, 'leased', {'delivery_id': item['id'], 'attempt': lease['number'],
                                                   'worker': worker, 'expires': lease['expires']}, message['id'])
        results.append(dict(delivery_id=item['id'], message_id=message['id'], lane=item['lane'],
                            policy=item['policy'], lease=lease, envelope=message['envelope']))
    return {'paused': False, 'items': results}
