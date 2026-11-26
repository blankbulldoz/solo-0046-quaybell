from . import catalog, inputs, journal
from .codec import pack, unpack
from .envelope import labels
from .errors import conflict, missing


def decoded(row):
    return dict(row) | {'tags': unpack(row['tags']), 'paused': bool(row['paused'])}


def for_channel(db, channel):
    return [decoded(row) for row in db.execute(
        'SELECT * FROM subscriptions WHERE channel=? ORDER BY id', (channel,))]


def get(db, channel, group):
    row = db.execute('SELECT * FROM subscriptions WHERE channel=? AND name=?', (channel, group)).fetchone()
    if row is None:
        raise missing('找不到消费组')
    return decoded(row)


def create(db, channel, raw, now):
    catalog.get(db, channel)
    inputs.require(raw, ('name',), ('tags',))
    group = inputs.name(raw['name'], '消费组')
    tags = labels(raw.get('tags', {}))
    if any(item['name'] == group for item in for_channel(db, channel)):
        raise conflict('消费组已存在')
    db.execute('INSERT INTO subscriptions(channel,name,tags,created) VALUES (?,?,?,?)',
               (channel, group, pack(tags), now))
    journal.append(db, channel, now, 'subscribed', {'group': group, 'tags': tags})
    return get(db, channel, group)


def replace_tags(db, channel, group, raw, now):
    record = get(db, channel, group)
    inputs.require(raw, ('tags',))
    tags = labels(raw['tags'])
    if tags == record['tags']:
        raise conflict('匹配标签未变化')
    last = db.execute('SELECT COALESCE(MAX(id), 0) FROM messages WHERE channel=?',
                      (channel,)).fetchone()[0]
    db.execute('UPDATE subscriptions SET tags=? WHERE id=?', (pack(tags), record['id']))
    carried = [dict(row) for row in db.execute(
        '''SELECT d.id AS delivery_id, d.message_id, d.lane, d.state FROM deliveries d
           WHERE d.subscription_id=? AND d.state IN ('waiting','leased')
           ORDER BY d.id LIMIT 50''', (record['id'],))]
    total = db.execute('''SELECT COUNT(*) FROM deliveries
                          WHERE subscription_id=? AND state IN ('waiting','leased')''',
                       (record['id'],)).fetchone()[0]
    boundary = {'switched_at': now, 'last_message_id': last, 'carried_total': total,
                'carried': carried}
    journal.append(db, channel, now, 'retagged',
                   {'group': group, 'previous_tags': record['tags'], 'tags': tags,
                    'last_message_id': last, 'carried_total': total})
    return {'channel': channel, 'group': group, 'previous_tags': record['tags'],
            'tags': tags, 'boundary': boundary}


def pause(db, channel, group, raw, now):
    inputs.require(raw, ('paused',))
    desired = inputs.boolean(raw['paused'], '暂停状态')
    record = get(db, channel, group)
    if desired != record['paused']:
        db.execute('UPDATE subscriptions SET paused=? WHERE id=?', (desired, record['id']))
        journal.append(db, channel, now, 'group_paused' if desired else 'group_resumed', {'group': group})
    return get(db, channel, group)
