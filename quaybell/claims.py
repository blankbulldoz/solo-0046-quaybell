from . import attempts, delivery, eligibility, expiry, inputs, journal, policy, publication, subscriptions

BUDGET_LIMIT = 4 * 1024 * 1024


def claim(db, channel, group, raw, now):
    inputs.require(raw, ('worker',), ('limit', 'max_bytes'))
    worker = inputs.name(raw['worker'], '工作进程')
    limit = inputs.integer(raw.get('limit', 1), 1, 30, '领取上限')
    budget = inputs.integer(raw['max_bytes'], 0, BUDGET_LIMIT, '载荷字节预算') if 'max_bytes' in raw else None
    sub = subscriptions.get(db, channel, group)
    if sub['paused']:
        return {'paused': True, 'outcome': 'paused', 'items': [], 'budget': summary(budget, 0),
                'unfit': {'total': 0, 'items': []}, 'deferred': {'total': 0, 'items': []}}
    expiry.reap(db, now, 100, sub['id'])
    selection = eligibility.partition(db, sub['id'], now, limit, budget)
    results = []
    for item in selection['picked']:
        rule = policy.read(db, channel, item['policy'])
        lease = attempts.open_attempt(db, item, worker, now, rule['lease_seconds'])
        delivery.lease(db, item['id'], lease['number'])
        message = publication.get(db, item['message_id'])
        journal.append(db, channel, now, 'leased', {'delivery_id': item['id'], 'attempt': lease['number'],
                                                   'worker': worker, 'expires': lease['expires']}, message['id'])
        results.append(dict(delivery_id=item['id'], message_id=message['id'], lane=item['lane'],
                            policy=item['policy'], payload_bytes=item['payload_bytes'],
                            lease=lease, envelope=message['envelope']))
    return {'paused': False, 'outcome': verdict(results, selection),
            'items': results, 'budget': summary(budget, selection['used']),
            'unfit': {'total': selection['unfit_total'], 'items': selection['unfit']},
            'deferred': {'total': selection['deferred_total'], 'items': selection['deferred']}}


def summary(budget, used):
    return {'max_bytes': budget, 'used_bytes': used,
            'remaining_bytes': None if budget is None else budget - used}


def verdict(results, selection):
    if results:
        return 'claimed'
    if selection['unfit_total']:
        return 'oversized'
    return 'empty'
