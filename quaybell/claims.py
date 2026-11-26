from . import attempts, delivery, eligibility, expiry, inputs, journal, policy, publication, subscriptions


def claim(db, channel, group, raw, now):
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
        claimed = dict(delivery_id=item['id'], message_id=message['id'], lane=item['lane'],
                       policy=item['policy'], lease=lease, envelope=message['envelope'])
        env = message['envelope'] if isinstance(message['envelope'], dict) else {}
        if env.get('batch'):
            payload = env.get('payload') if isinstance(env.get('payload'), dict) else {}
            claimed.update(batch_id=payload.get('batch_id'), batch=True,
                           parts=payload.get('parts'), shards=payload.get('shards'),
                           part_count=payload.get('part_count'))
        results.append(claimed)
    return {'paused': False, 'items': results}
