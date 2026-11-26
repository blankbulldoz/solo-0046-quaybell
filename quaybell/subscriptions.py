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


def rule(db, subscription_id, version):
    row = db.execute('SELECT * FROM subscription_rules WHERE subscription_id=? AND version=?',
                     (subscription_id, version)).fetchone()
    if row is None:
        raise missing('找不到订阅规则版本')
    return dict(row) | {'tags': unpack(row['tags'])}


def rules_for(db, channel):
    '''Every registered baseline rule with its owning group, used for matching.'''
    return [dict(row) | {'tags': unpack(row['tags'])} for row in db.execute(
        '''SELECT s.id AS subscription_id,s.channel,s.name AS group_name,s.paused,
                  r.version,r.tags
           FROM subscriptions s JOIN subscription_rules r
             ON r.subscription_id=s.id AND r.version=s.version
           WHERE s.channel=? ORDER BY s.id''', (channel,))]


def revise(db, channel, group, raw, now):
    inputs.require(raw, ('tags',))
    record = get(db, channel, group)
    version = record['version'] + 1
    tags = labels(raw['tags'])
    db.execute('INSERT INTO subscription_rules(subscription_id,version,tags,created) VALUES (?,?,?,?)',
               (record['id'], version, pack(tags), now))
    db.execute('UPDATE subscriptions SET tags=?,version=? WHERE id=?', (pack(tags), version, record['id']))
    journal.append(db, channel, now, 'rules_revised', {'group': group, 'version': version, 'tags': tags})
    return get(db, channel, group)


def create(db, channel, raw, now):
    catalog.get(db, channel)
    inputs.require(raw, ('name',), ('tags',))
    group = inputs.name(raw['name'], '消费组')
    tags = labels(raw.get('tags', {}))
    if any(item['name'] == group for item in for_channel(db, channel)):
        raise conflict('消费组已存在')
    key = db.execute('INSERT INTO subscriptions(channel,name,tags,version,created) VALUES (?,?,?,1,?)',
                     (channel, group, pack(tags), now)).lastrowid
    db.execute('INSERT INTO subscription_rules(subscription_id,version,tags,created) VALUES (?,1,?,?)',
               (key, pack(tags), now))
    journal.append(db, channel, now, 'subscribed', {'group': group, 'tags': tags, 'version': 1})
    return get(db, channel, group)


def pause(db, channel, group, raw, now):
    inputs.require(raw, ('paused',))
    desired = inputs.boolean(raw['paused'], '暂停状态')
    record = get(db, channel, group)
    if desired != record['paused']:
        db.execute('UPDATE subscriptions SET paused=? WHERE id=?', (desired, record['id']))
        journal.append(db, channel, now, 'group_paused' if desired else 'group_resumed', {'group': group})
    return get(db, channel, group)
