from . import attempts, confluence_service, delivery, eligibility, expiry, inputs, journal, policy, publication, subscriptions


def claim(db, channel, group, raw, now):
    inputs.require(raw, ('worker',), ('limit',))
    worker = inputs.name(raw['worker'], '工作进程')
    limit = inputs.integer(raw.get('limit', 1), 1, 30, '领取上限')
    sub = subscriptions.get(db, channel, group)
    if sub['paused']:
        return {'paused': True, 'items': []}
    expiry.reap(db, now, 100, sub['id'])
    if confluence_service.is_enabled(db, sub['id']):
        # Quaybell has no background scheduler; claims lazily cross an agreed
        # confluence budget in the same write transaction.
        confluence_service.advance(db, channel, sub['name'], {}, now)
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
