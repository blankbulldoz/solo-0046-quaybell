from . import attempts, delivery, eligibility, expiry, inputs, journal, policy, publication, renewal, subscriptions


def claim(db, channel, group, raw, now):
    inputs.require(raw, ('worker',),
                  ('limit', 'lease_seconds', *renewal.BUDGET_FIELDS,
                   *renewal.BUDGET_EXPIRES_FIELDS, *renewal.BUDGET_COUNT_FIELDS))
    worker = inputs.name(raw['worker'], '工作进程')
    limit = inputs.integer(raw.get('limit', 1), 1, 30, '领取上限')
    requested_lease = raw.get('lease_seconds')
    lease_override = None if requested_lease is None else inputs.integer(
        requested_lease, 1, 300, '租约秒数')
    sub = subscriptions.get(db, channel, group)
    if sub['paused']:
        return {'paused': True, 'items': []}
    expiry.reap(db, now, 100, sub['id'])
    results = []
    for item in eligibility.ready(db, sub['id'], now, limit):
        rule = policy.read(db, channel, item['policy'])
        step = rule['lease_seconds'] if lease_override is None else lease_override
        budget_seconds, budget_expires = renewal.claim_budget(raw, {'lease_seconds': step}, now)
        lease = attempts.open_attempt(db, item, worker, now, step, budget_expires, step)
        lease['budget_seconds'] = budget_seconds
        lease['renewals'] = 0
        lease['max_expires'] = budget_expires
        delivery.lease(db, item['id'], lease['number'])
        message = publication.get(db, item['message_id'])
        journal.append(db, channel, now, 'leased', {'delivery_id': item['id'], 'attempt': lease['number'],
                                                   'worker': worker, 'expires': lease['expires'],
                                                   'budget_expires': budget_expires}, message['id'])
        results.append(dict(delivery_id=item['id'], message_id=message['id'], lane=item['lane'],
                            policy=item['policy'], lease=lease, envelope=message['envelope']))
    return {'paused': False, 'items': results}
