"""Latest-reading coalescing for opt-in subscriptions.

A delivery is replaceable only while it has never left the queue: state
``waiting`` and ``attempt_count == 0``.  Leased deliveries and deliveries
which have already entered a retry retain their normal lifecycle.
"""
from . import inputs, journal, subscriptions
from .errors import invalid

LATEST = 'latest'
ALL = 'all'
READY = 'ready'
PUBLISH = 'publish'

MODE_FIELDS = ('mode', 'merge_mode', 'merge_strategy', 'coalesce_mode',
               'conflation_mode', 'latest_mode', 'strategy')
BOOL_FIELDS = ('coalesce', 'coalesced', 'coalescing', 'conflation', 'conflated',
               'latest_only', 'latest', 'latest_readings', 'only_latest',
               'keep_latest', 'merge_latest', 'merged', 'compact',
               'drop_stale_readings')
TIMING_FIELDS = ('coalesce_timing', 'supersede_timing', 'supersede_when',
                 'replace_when', 'coalesce_at', 'timing')
CREATE_OPTIONAL = ('tags', *MODE_FIELDS, *BOOL_FIELDS, *TIMING_FIELDS)

_LATEST_TOKENS = {
    'latest', 'latest_only', 'latest_reading', 'latest_readings',
    'only_latest', 'only_newest', 'keep_latest', 'latest_readings',
    'newest', 'coalesce', 'coalesced',
    'coalescing', 'conflate', 'conflated', 'conflation', 'merge', 'merged',
    'merging', 'supersede', 'superseded', 'drop_old', 'drop_old_readings',
    'drop_stale', 'drop_stale_readings',
}
_ALL_TOKENS = {
    'all', 'every', 'each', 'everything', 'all_readings', 'every_reading',
    'every_readings', 'normal', 'standard', 'default', 'off', 'none',
    'disabled', 'false', 'no',
}
_READY_TOKENS = {'ready', 'due', 'eligible', 'on_ready', 'when_ready',
                 'delay_elapsed', 'after_delay'}
_PUBLISH_TOKENS = {'publish', 'published', 'publication', 'immediate',
                   'immediately', 'enqueue', 'enqueued', 'now', 'on_publish'}


def _token(value, label):
    if isinstance(value, bool):
        return LATEST if value else ALL
    if not isinstance(value, str):
        raise invalid(f'{label}须为字符串或布尔值')
    return value.strip().lower().replace('-', '_').replace(' ', '_')


def _kind(value, label):
    if isinstance(value, dict):
        value = value.get('strategy', value.get('mode', value.get('type', value.get('value'))))
    token = _token(value, label)
    if token in _LATEST_TOKENS:
        return LATEST
    if token in _ALL_TOKENS:
        return ALL
    raise invalid(f'{label}只支持 all/every（每条）或 latest/coalesce（只留最新）')


def settings_from_input(raw):
    """Accept the small set of plausible external spellings.

    The persisted contract is intentionally canonical (``mode='latest'``);
    aliases keep request bodies readable for callers using coalesce/conflate
    terminology.
    """
    enabled = None
    for field in MODE_FIELDS:
        if field in raw:
            kind = _kind(raw[field], field)
            next_enabled = kind == LATEST
            if enabled is not None and enabled != next_enabled:
                raise invalid('合并模式选项互相冲突')
            enabled = next_enabled
    for field in BOOL_FIELDS:
        if field in raw:
            value = raw[field]
            if type(value) is bool:
                candidate = value
            elif isinstance(value, str):
                candidate = _kind(value, field) == LATEST
            elif isinstance(value, dict):
                if 'enabled' in value:
                    candidate = value['enabled']
                    if type(candidate) is not bool:
                        raise invalid(f'{field}.enabled 须为布尔值')
                else:
                    candidate = _kind(value, field) == LATEST
            else:
                raise invalid(f'{field}须为布尔值、字符串或配置对象')
            if enabled is not None and enabled != candidate:
                raise invalid('合并模式选项互相冲突')
            enabled = candidate

    timing = READY
    seen_timing = None
    for field in TIMING_FIELDS:
        if field in raw:
            value = raw[field]
            token = _token(value, field)
            if token in _READY_TOKENS:
                candidate = READY
            elif token in _PUBLISH_TOKENS:
                candidate = PUBLISH
            else:
                raise invalid(f'{field}只支持 ready（到期后）或 publish（发布时）')
            if seen_timing is not None and seen_timing != candidate:
                raise invalid('合并时机选项互相冲突')
            seen_timing = timing = candidate
    return bool(enabled), timing


def requested_alias(raw, enabled):
    """Echo the caller's enum spelling for response ergonomics."""
    for field in MODE_FIELDS:
        if field in raw:
            value = raw[field]
            if isinstance(value, str):
                return value.strip()
            return LATEST if _kind(value, field) == LATEST else ALL
    return LATEST if enabled else ALL


def decorate(group):
    canonical = group.get('mode') or ALL
    latest = canonical == LATEST or bool(group.get('coalesce'))
    timing = group.get('coalesce_timing') or READY
    alias = group.get('mode_alias') or (LATEST if latest else ALL)
    return group | {
        'mode': alias,
        'canonical_mode': LATEST if latest else ALL,
        'merge_mode': alias,
        'merge_strategy': alias,
        'coalesce_mode': alias,
        'conflation_mode': alias,
        'coalesce': latest,
        'coalescing': latest,
        'coalesce_enabled': latest,
        'conflation': latest,
        'conflation_enabled': latest,
        'latest_only': latest,
        'coalesce_timing': timing,
        'supersede_when': timing,
    }


def publication_can_supersede(group, ready_at, now):
    """Only the explicit publish-time policy collapses immediately.

    The default ready policy is applied lazily by claim/coalesce passes. That
    lets operators observe the full stacked backlog and makes the delay
    boundary explicit instead of rewriting history while a later item is not
    yet effective.
    """
    if group.get('mode') != LATEST and not group.get('coalesce'):
        return False
    timing = group.get('coalesce_timing') or READY
    return timing == PUBLISH


def supersede(db, new_delivery_id, now, trigger):
    new = db.execute('SELECT * FROM deliveries WHERE id=?', (new_delivery_id,)).fetchone()
    if new is None:
        from .errors import missing
        raise missing('找不到投递记录')
    new = dict(new)
    old_rows = db.execute('''SELECT id,message_id FROM deliveries
                            WHERE subscription_id=? AND lane=? AND id<?
                              AND state='waiting' AND attempt_count=0
                            ORDER BY id''',
                         (new['subscription_id'], new['lane'], new_delivery_id)).fetchall()
    old_ids = [row['id'] for row in old_rows]
    if not old_ids:
        existing = [row['old_delivery_id'] for row in db.execute(
            'SELECT old_delivery_id FROM delivery_supersessions WHERE new_delivery_id=? ORDER BY old_delivery_id',
            (new_delivery_id,))]
        return {'delivery_id': new_delivery_id, 'new_delivery_id': new_delivery_id,
                'supersedes': existing, 'replaced': existing, 'count': len(existing),
                'trigger': trigger, 'repeated': bool(existing)}

    old_message_ids = []
    for row in old_rows:
        old_id, old_message_id = row['id'], row['message_id']
        old_message_ids.append(old_message_id)
        db.execute("UPDATE deliveries SET state='superseded',terminal_at=?,superseded_by=? WHERE id=?",
                   (now, new_delivery_id, old_id))
        db.execute('''INSERT OR REPLACE INTO delivery_supersessions
                      (old_delivery_id,new_delivery_id,subscription_id,lane,at,trigger,
                       old_message_id,new_message_id)
                      VALUES (?,?,?,?,?,?,?,?)''',
                   (old_id, new_delivery_id, new['subscription_id'], new['lane'], now, trigger,
                    old_message_id, new['message_id']))
        journal.append(db, _channel(db, new['subscription_id']), now, 'superseded', {
            'delivery_id': old_id,
            'state': 'superseded',
            'superseded_by': new_delivery_id,
            'new_delivery_id': new_delivery_id,
            'superseding_message_id': new['message_id'],
            'new_message_id': new['message_id'],
            'trigger': trigger,
            'mode': LATEST,
        }, old_message_id)

    result = {'delivery_id': new_delivery_id, 'new_delivery_id': new_delivery_id,
              'supersedes': old_ids, 'replaced': old_ids, 'old_delivery_ids': old_ids,
              'old_message_ids': old_message_ids, 'count': len(old_ids),
              'trigger': trigger, 'repeated': False}
    journal.append(db, _channel(db, new['subscription_id']), now, 'coalesced',
                   result | {'new_message_id': new['message_id']}, new['message_id'])
    return result


def _channel(db, subscription_id):
    return db.execute('SELECT channel FROM subscriptions WHERE id=?',
                      (subscription_id,)).fetchone()[0]


def apply_due(db, subscription_id, now, trigger=READY, limit=200, only_enabled=True):
    row = db.execute('SELECT channel,name,mode FROM subscriptions WHERE id=?',
                     (subscription_id,)).fetchone()
    if row is None:
        from .errors import missing
        raise missing('找不到消费组')
    if only_enabled and row['mode'] != LATEST:
        return []
    rows = db.execute('''SELECT MAX(id) AS id FROM deliveries
                         WHERE subscription_id=? AND state='waiting' AND attempt_count=0
                           AND ready_at<=?
                         GROUP BY lane ORDER BY MAX(id) LIMIT ?''',
                      (subscription_id, now, limit)).fetchall()
    results = []
    for row in rows:
        result = supersede(db, row['id'], now, trigger if trigger in (READY, 'manual') else READY)
        if result['count']:
            results.append(result)
    return results


def apply_all_due(db, now, channel=None, limit=500):
    params = [LATEST]
    where = "mode=?"
    if channel is not None:
        where += ' AND channel=?'
        params.append(channel)
    params.append(limit)
    ids = [row['id'] for row in db.execute(
        f'SELECT id FROM subscriptions WHERE {where} ORDER BY id LIMIT ?', params)]
    results = []
    for subscription_id in ids:
        results.extend(apply_due(db, subscription_id, now))
    return results


def configure(db, channel, group_name, raw, now):
    group = subscriptions.get(db, channel, group_name)
    inputs.require(raw, (), (*MODE_FIELDS, *BOOL_FIELDS, *TIMING_FIELDS, 'enabled'))
    config_raw = dict(raw)
    if 'enabled' in config_raw:
        enabled_value = config_raw.pop('enabled')
        if type(enabled_value) is not bool:
            raise invalid('enabled 须为布尔值')
        config_raw.setdefault('coalesce', enabled_value)
    enabled, timing = settings_from_input(config_raw)
    alias = requested_alias(config_raw, enabled) if config_raw else (
        LATEST if group['coalesce'] else ALL)
    if not raw:
        enabled = group['coalesce']
        timing = group.get('coalesce_timing', READY)
    mode = LATEST if enabled else ALL
    if mode != group['canonical_mode'] or timing != group.get('coalesce_timing', READY) \
            or alias != group.get('mode_alias'):
        db.execute('UPDATE subscriptions SET mode=?,mode_alias=?,coalesce_timing=? WHERE id=?',
                   (mode, alias, timing, group['id']))
        journal.append(db, channel, now, 'group_mode_changed',
                       {'group': group_name, 'mode': mode, 'mode_alias': alias,
                        'coalesce': enabled, 'coalesce_timing': timing})
    return subscriptions.get(db, channel, group_name)


def coalesce_endpoint(db, channel, group_name, raw, now):
    config_keys = (*MODE_FIELDS, *BOOL_FIELDS, *TIMING_FIELDS, 'enabled')
    inputs.require(raw, (), ('limit', 'force', 'run', 'flush', *config_keys))
    config_raw = {key: raw[key] for key in config_keys if key in raw}
    if config_raw:
        group = configure(db, channel, group_name, config_raw, now)
    else:
        group = subscriptions.get(db, channel, group_name)

    limit = inputs.integer(raw.get('limit', 200), 1, 500, '合并上限')
    if 'force' in raw and type(raw['force']) is not bool:
        raise invalid('force 须为布尔值')
    should_run = raw.get('run', raw.get('flush', True))
    if type(should_run) is not bool:
        raise invalid('run/flush 须为布尔值')
    if not group['coalesce'] or not should_run:
        return {'group': group, 'mode': group['mode'], 'coalesce': group['coalesce'],
                'coalesced': [], 'results': [], 'observed_at': now}
    if raw.get('force') is True:
        rows = db.execute('''SELECT MAX(id) AS id FROM deliveries
                             WHERE subscription_id=? AND state='waiting' AND attempt_count=0
                             GROUP BY lane ORDER BY MAX(id) LIMIT ?''',
                          (group['id'], limit)).fetchall()
        results = [supersede(db, row['id'], now, 'manual') for row in rows]
        results = [item for item in results if item['count']]
    else:
        results = apply_due(db, group['id'], now, trigger='manual', limit=limit)
    return {'group': group, 'mode': group['mode'], 'coalesce': True,
            'coalesced': results, 'results': results, 'observed_at': now}


def all_endpoint(db, raw, now):
    inputs.require(raw, (), ('limit', 'channel'))
    channel = raw.get('channel')
    if channel is not None:
        channel = inputs.name(channel, '频道名')
        from . import catalog
        catalog.get(db, channel)
    limit = inputs.integer(raw.get('limit', 500), 1, 1000, '合并上限')
    results = apply_all_due(db, now, channel=channel, limit=limit)
    return {'coalesced': results, 'results': results, 'observed_at': now}


def relation_for_delivery(db, delivery_id):
    row = db.execute('''SELECT * FROM delivery_supersessions WHERE old_delivery_id=?''',
                     (delivery_id,)).fetchone()
    return dict(row) if row else None


def supersessions_for_delivery(db, delivery_id):
    return [dict(row) for row in db.execute(
        'SELECT * FROM delivery_supersessions WHERE new_delivery_id=? ORDER BY old_delivery_id',
        (delivery_id,))]


def transitive_old_ids(db, new_delivery_id):
    result, seen, frontier = [], set(), [new_delivery_id]
    while frontier:
        current = frontier.pop()
        rows = db.execute('SELECT old_delivery_id FROM delivery_supersessions WHERE new_delivery_id=?',
                          (current,)).fetchall()
        for row in rows:
            old_id = row['old_delivery_id']
            if old_id not in seen:
                seen.add(old_id)
                result.append(old_id)
                frontier.append(old_id)
    return sorted(result)


def enrich_delivery(db, item):
    relation = relation_for_delivery(db, item['id'])
    relations = supersessions_for_delivery(db, item['id'])
    ids = [row['old_delivery_id'] for row in relations]
    result = dict(item)
    if relation:
        new_id = relation['new_delivery_id']
        result['state'] = 'superseded'
        result['superseded_by'] = new_id
        result['superseded_by_delivery_id'] = new_id
        result['superseded_by_delivery'] = new_id
        result['replaced_by'] = new_id
        result['merged_into'] = new_id
        result['coalesced_into'] = new_id
        result['superseded_by_message_id'] = relation['new_message_id']
        result['supersession'] = dict(relation)
    result['supersedes'] = ids
    result['superseded_ids'] = ids
    result['superseded_all'] = transitive_old_ids(db, item['id'])
    result['replaces'] = ids
    result['replaced'] = ids
    result['merged_ids'] = ids
    result['supersession_links'] = [dict(row) for row in relations]
    result['coalesced'] = bool(relation or ids)
    result['superseded'] = bool(relation)
    result['status'] = result['state']
    if relation:
        result['coalescence_state'] = 'superseded'
    elif ids:
        result['coalescence_state'] = 'latest'
    return result


def recent(db, channel, limit=20):
    return [dict(row) for row in db.execute('''SELECT rel.*, s.name AS group_name
                                               FROM delivery_supersessions rel
                                               JOIN subscriptions s ON s.id=rel.subscription_id
                                               WHERE s.channel=?
                                               ORDER BY rel.at DESC, rel.old_delivery_id DESC
                                               LIMIT ?''', (channel, limit))]


# Concise callable aliases for callers/tests using the operation name rather
# than the subsystem name.
coalesce = apply_due
coalesce_due = apply_due
merge_due = apply_due
coalesce_delivery = supersede
supersede_delivery = supersede
merge_delivery = supersede
