from . import catalog, inputs, journal
from .codec import pack, unpack
from .envelope import labels
from .errors import conflict, missing

# Accepted request spellings are centralized in coalescing, but imported lazily
# where needed to avoid a module-loading cycle.
DEFAULT_MODE = 'all'
DEFAULT_TIMING = 'ready'


def decoded(row):
    item = dict(row) | {'tags': unpack(row['tags']), 'paused': bool(row['paused'])}
    from . import coalescing
    return coalescing.decorate(item)


def for_channel(db, channel):
    return [decoded(row) for row in db.execute(
        'SELECT * FROM subscriptions WHERE channel=? ORDER BY id', (channel,))]


def get(db, channel, group):
    row = db.execute('SELECT * FROM subscriptions WHERE channel=? AND name=?', (channel, group)).fetchone()
    if row is None:
        raise missing('找不到消费组')
    return decoded(row)


def create(db, channel, raw, now):
    from .coalescing import CREATE_OPTIONAL, requested_alias, settings_from_input

    payload = dict(raw)
    for nested_key in ('config', 'options', 'settings'):
        nested = payload.pop(nested_key, None)
        if nested is not None:
            if not isinstance(nested, dict):
                raise inputs.invalid(f'{nested_key} 须为对象')
            for key, value in nested.items():
                payload.setdefault(key, value)

    catalog.get(db, channel)
    inputs.require(payload, ('name',), (*CREATE_OPTIONAL, 'config', 'options', 'settings'))
    group = inputs.name(payload['name'], '消费组')
    tags = labels(payload.get('tags', {}))
    mode, timing = settings_from_input(payload)
    mode_value = 'latest' if mode else DEFAULT_MODE
    mode_alias = requested_alias(payload, mode)
    if any(item['name'] == group for item in for_channel(db, channel)):
        raise conflict('消费组已存在')
    db.execute('''INSERT INTO subscriptions(channel,name,tags,created,mode,mode_alias,coalesce_timing)
                  VALUES (?,?,?,?,?,?,?)''',
               (channel, group, pack(tags), now, mode_value, mode_alias, timing))
    journal.append(db, channel, now, 'subscribed',
                   {'group': group, 'tags': tags, 'mode': mode_value,
                    'mode_alias': mode_alias,
                    'coalesce': mode, 'coalesce_timing': timing})
    return get(db, channel, group)


def pause(db, channel, group, raw, now):
    inputs.require(raw, ('paused',))
    desired = inputs.boolean(raw['paused'], '暂停状态')
    record = get(db, channel, group)
    if desired != record['paused']:
        db.execute('UPDATE subscriptions SET paused=? WHERE id=?', (desired, record['id']))
        journal.append(db, channel, now, 'group_paused' if desired else 'group_resumed', {'group': group})
    return get(db, channel, group)
