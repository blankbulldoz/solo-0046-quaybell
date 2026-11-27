from . import catalog, clockwork, inputs, journal


def eligible(db, channel, now, limit):
    catalog.get(db, channel)
    rows = db.execute('''
        SELECT m.id,m.published,m.policy,m.published+p.retention_seconds*1000 AS cutoff
        FROM messages m JOIN policies p ON p.channel=m.channel AND p.version=m.policy
        WHERE m.channel=? AND m.published+p.retention_seconds*1000<=?
          AND NOT EXISTS(SELECT 1 FROM deliveries d WHERE d.message_id=m.id
                         AND d.state IN ('waiting','leased'))
        ORDER BY m.id LIMIT ?''', (channel, now, limit))
    return [clockwork.retention_row(dict(row)) for row in rows]


def plan(db, channel, now, limit=100):
    return {'observed_at': clockwork.to_seconds(now), 'eligible': eligible(db, channel, now, limit)}


def prune(db, channel, raw, now):
    inputs.require(raw, (), ('limit',))
    keys = [item['id'] for item in eligible(db, channel, now,
                                          inputs.integer(raw.get('limit', 100), 1, 100, '清理上限'))]
    db.executemany('DELETE FROM messages WHERE id=?', [(key,) for key in keys])
    if keys:
        journal.append(db, channel, now, 'pruned', {'message_ids': keys})
    return {'removed': keys, 'observed_at': clockwork.to_seconds(now)}
