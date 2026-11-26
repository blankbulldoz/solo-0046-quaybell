from . import coalescing, policy, publication, subscriptions
from . import attempts, delivery, eligibility, expiry, inputs, journal


def claim(db, channel, group, raw, now):
    optional = ('limit', *coalescing.MODE_FIELDS, *coalescing.BOOL_FIELDS,
                *coalescing.TIMING_FIELDS)
    inputs.require(raw, ('worker',), optional)
    worker = inputs.name(raw['worker'], '工作进程')
    limit = inputs.integer(raw.get('limit', 1), 1, 30, '领取上限')
    sub = subscriptions.get(db, channel, group)
    if sub['paused']:
        return {'paused': True, 'items': []}
    expiry.reap(db, now, 100, sub['id'])

    # Delayed latest readings mature at ready_at. Claim is the ordinary lazy
    # maintenance point, just as expired leases are reaped before selection.
    coalescing.apply_due(db, sub['id'], now)
    override_fields = (*coalescing.MODE_FIELDS, *coalescing.BOOL_FIELDS)
    if any(field in raw for field in override_fields):
        override_latest, _ = coalescing.settings_from_input(raw)
        if override_latest and not sub['coalesce']:
            coalescing.apply_due(db, sub['id'], now, only_enabled=False)

    results = []
    for item in eligibility.ready(db, sub['id'], now, limit):
        rule = policy.read(db, channel, item['policy'])
        lease = attempts.open_attempt(db, item, worker, now, rule['lease_seconds'])
        delivery.lease(db, item['id'], lease['number'])
        message = publication.get(db, item['message_id'])
        journal.append(db, channel, now, 'leased', {'delivery_id': item['id'], 'attempt': lease['number'],
                                                   'worker': worker, 'expires': lease['expires']}, message['id'])
        results.append(dict(delivery_id=item['id'], message_id=message['id'], lane=item['lane'],
                            policy=item['policy'], attempt_count=lease['number'],
                            lease=lease, envelope=message['envelope']))
    return {'paused': False, 'mode': sub['mode'], 'items': results}
