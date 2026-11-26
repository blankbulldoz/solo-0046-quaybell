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
    inputs.require(raw, ('name',), ('tags', 'mode', 'merge_mode', 'confluence', 'merge',
                                    'enabled', 'sources', 'source_order',
                                    'config', 'settings', 'options',
                                    'time_budget_seconds', 'budget_seconds',
                                    'gap_budget_seconds', 'wait_budget_seconds',
                                    'time_budget', 'budget', 'timeout_seconds',
                                    'start', 'initial_sequence', 'anchor_on_first',
                                    'block_other_sources'))
    group = inputs.name(raw['name'], '消费组')
    tags = labels(raw.get('tags', {}))
    if any(item['name'] == group for item in for_channel(db, channel)):
        raise conflict('消费组已存在')
    db.execute('INSERT INTO subscriptions(channel,name,tags,created) VALUES (?,?,?,?)',
               (channel, group, pack(tags), now))
    journal.append(db, channel, now, 'subscribed', {'group': group, 'tags': tags})
    record = get(db, channel, group)
    mode = raw.get('mode') or raw.get('merge_mode')
    merge_requested = mode in ('confluence', 'merge', 'sequence', 'sequence_merge',
                               'source_sequence') or raw.get('confluence') is not None or raw.get('merge') is not None or any(
        key in raw for key in ('time_budget_seconds', 'budget_seconds', 'gap_budget_seconds',
                               'wait_budget_seconds', 'time_budget', 'budget', 'gap_timeout',
                               'gap_timeout_seconds', 'sources', 'source_order'))
    if merge_requested:
        from . import confluence_service
        config_raw = dict(raw)
        for nested_key in ('confluence', 'merge', 'config', 'settings', 'options'):
            nested = raw.get(nested_key)
            if isinstance(nested, dict):
                for key, value in nested.items():
                    config_raw.setdefault(key, value)
        record = record | {'confluence': confluence_service.configure(
            db, channel, group, config_raw, now)}
    return record


def pause(db, channel, group, raw, now):
    inputs.require(raw, ('paused',))
    desired = inputs.boolean(raw['paused'], '暂停状态')
    record = get(db, channel, group)
    if desired != record['paused']:
        db.execute('UPDATE subscriptions SET paused=? WHERE id=?', (desired, record['id']))
        journal.append(db, channel, now, 'group_paused' if desired else 'group_resumed', {'group': group})
    return get(db, channel, group)
