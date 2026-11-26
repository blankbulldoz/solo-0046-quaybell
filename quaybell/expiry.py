from . import attempts, coalescing, delivery, inputs, outcomes


def reap(db, now, limit, group_id=None):
    result = []
    for attempt in attempts.expired(db, now, limit, group_id):
        item = delivery.get(db, attempt['delivery_id'])
        result.append(outcomes.apply(db, item, attempt, 'expired', '租约超时', now))
    return result


def sweep(db, raw, now):
    inputs.require(raw, (), ('limit',))
    limit = inputs.integer(raw.get('limit', 50), 1, 100, '回收上限')
    recovered = reap(db, now, limit)
    coalesced = coalescing.apply_all_due(db, now, limit=limit)
    return {'recovered': recovered, 'coalesced': coalesced}
