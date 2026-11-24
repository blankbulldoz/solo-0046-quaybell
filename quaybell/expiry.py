from . import attempts, delivery, inputs, outcomes


def reap(db, now, limit, group_id=None):
    result = []
    for attempt in attempts.expired(db, now, limit, group_id):
        item = delivery.get(db, attempt['delivery_id'])
        result.append(outcomes.apply(db, item, attempt, 'expired', '租约超时', now))
    return result


def sweep(db, raw, now):
    inputs.require(raw, (), ('limit',))
    return {'recovered': reap(db, now, inputs.integer(raw.get('limit', 50), 1, 100, '回收上限'))}
