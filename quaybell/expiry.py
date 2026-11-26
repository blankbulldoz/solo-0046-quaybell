from . import attempts, delivery, inputs, journal, outcomes


def terminate(db, item, now):
    """Move a delivery whose message deadline has passed into the dead letters."""
    attempt = attempts.open_for(db, item['id'])
    if attempt is not None:
        return outcomes.apply(db, item, attempt, 'expired', outcomes.DEADLINE, now,
                              terminal=True, kind='stale')
    delivery.close(db, item['id'], 'dead', now, outcomes.DEADLINE)
    result = dict(delivery_id=item['id'], attempt=None, outcome='expired', state='dead',
                  effective_at=now, reason=outcomes.DEADLINE, terminal_reason=outcomes.DEADLINE)
    journal.append(db, item['channel'], now, 'stale', result, item['message_id'])
    return result


def stale(db, now, limit, group_id=None):
    query = '''SELECT d.id FROM deliveries d JOIN messages m ON m.id=d.message_id
               WHERE d.state IN ('waiting','leased') AND m.deadline IS NOT NULL AND m.deadline<=?'''
    values = [now]
    if group_id is not None:
        query += ' AND d.subscription_id=?'
        values.append(group_id)
    rows = db.execute(query + ' ORDER BY d.id LIMIT ?', (*values, limit))
    return [terminate(db, delivery.get(db, row['id']), now) for row in rows]


def reap(db, now, limit, group_id=None):
    result = []
    for attempt in attempts.expired(db, now, limit, group_id):
        item = delivery.get(db, attempt['delivery_id'])
        if delivery.stale(item, now):
            result.append(outcomes.apply(db, item, attempt, 'expired', outcomes.DEADLINE, now,
                                         terminal=True, kind='stale'))
        else:
            result.append(outcomes.apply(db, item, attempt, 'expired', outcomes.LEASE_TIMEOUT, now))
    return result


def sweep(db, raw, now):
    inputs.require(raw, (), ('limit',))
    limit = inputs.integer(raw.get('limit', 50), 1, 100, '回收上限')
    expired = stale(db, now, limit)
    return {'recovered': reap(db, now, limit), 'stale': expired}
