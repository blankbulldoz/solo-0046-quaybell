from . import catalog, inputs, journal, quota
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
    inputs.require(raw, ('name',), ('tags', 'quota'))
    group = inputs.name(raw['name'], '消费组')
    tags = labels(raw.get('tags', {}))
    limit = quota.quota_value(raw)
    if any(item['name'] == group for item in for_channel(db, channel)):
        raise conflict('消费组已存在')
    db.execute('INSERT INTO subscriptions(channel,name,tags,created,publish_quota) VALUES (?,?,?,?,?)',
               (channel, group, pack(tags), now, limit))
    journal.append(db, channel, now, 'subscribed', {'group': group, 'tags': tags, 'quota': limit})
    return get(db, channel, group)


def pause(db, channel, group, raw, now):
    inputs.require(raw, ('paused',))
    desired = inputs.boolean(raw['paused'], '暂停状态')
    record = get(db, channel, group)
    if desired != record['paused']:
        db.execute('UPDATE subscriptions SET paused=? WHERE id=?', (desired, record['id']))
        journal.append(db, channel, now, 'group_paused' if desired else 'group_resumed', {'group': group})
    return get(db, channel, group)
