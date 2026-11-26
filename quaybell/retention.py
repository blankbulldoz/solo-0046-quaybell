from . import batches, catalog, inputs, journal


def eligible(db, channel, now, limit):
    catalog.get(db, channel)
    return [dict(row) for row in db.execute('''
        SELECT m.id,m.published,m.policy,m.published+p.retention_seconds AS cutoff
        FROM messages m JOIN policies p ON p.channel=m.channel AND p.version=m.policy
        WHERE m.channel=? AND m.published+p.retention_seconds<=?
          AND NOT EXISTS(SELECT 1 FROM deliveries d WHERE d.message_id=m.id
                         AND d.state IN ('waiting','leased'))
        ORDER BY m.id LIMIT ?''', (channel, now, limit))]


def plan(db, channel, now, limit=100):
    catalog.get(db, channel)
    return {'observed_at': now, 'eligible': eligible(db, channel, now, limit),
            'retirable_batches': batches.retirable(db, channel, now, limit)}


def prune(db, channel, raw, now):
    inputs.require(raw, (), ('limit',))
    limit = inputs.integer(raw.get('limit', 100), 1, 100, '清理上限')
    keys = [item['id'] for item in eligible(db, channel, now, limit)]
    db.executemany('DELETE FROM messages WHERE id=?', [(key,) for key in keys])
    retired = batches.retire(db, channel, now, limit)
    if keys:
        journal.append(db, channel, now, 'pruned', {'message_ids': keys})
    return {'removed': keys, 'retired_batches': retired, 'observed_at': now}
