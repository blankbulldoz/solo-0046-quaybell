from . import catalog, inputs, journal
from .codec import unpack


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
    return {'observed_at': now, 'eligible': eligible(db, channel, now, limit)}


def prune(db, channel, raw, now):
    inputs.require(raw, (), ('limit',))
    keys = [item['id'] for item in eligible(db, channel, now,
                                          inputs.integer(raw.get('limit', 100), 1, 100, '清理上限'))]
    # 删除前固化顺序键、标签和信封摘要：清理后原信封不可得，但清理记录仍能
    # 让补订按标签过滤并把缺口落到正确顺序键；证据包再凭摘要对上原信封。
    records = []
    for key in keys:
        row = db.execute('SELECT id,lane,envelope,fingerprint,published FROM messages WHERE id=?',
                         (key,)).fetchone()
        if row is not None:
            records.append(dict(message_id=row['id'], lane=row['lane'],
                                tags=unpack(row['envelope']).get('tags', {}),
                                fingerprint=row['fingerprint'], published=row['published']))
    db.executemany('DELETE FROM messages WHERE id=?', [(key,) for key in keys])
    if keys:
        journal.append(db, channel, now, 'pruned',
                       {'message_ids': keys, 'messages': records})
    return {'removed': keys, 'observed_at': now}
