"""Reconcilable consumer-group takeover.

A takeover copies every unfinished source delivery into the destination group,
fences the old attempt, and records a stable old -> new mapping.  Acknowledged
source deliveries are deliberately not copied.  Repeating the same request (or
repeating it against the same source/target pair without a caller key) reuses
the ledger and never creates a second copy of a mapped delivery.
"""
import re
from collections import Counter

from . import delivery, inputs, journal, subscriptions
from .codec import pack, unpack
from .errors import conflict, invalid, missing

KEY_RE = re.compile(r'[A-Za-z0-9_.:-]{1,80}\Z')
OPTIONAL_FIELDS = (
    'new_group', 'target_group', 'destination_group', 'to_group',
    'new_group_name', 'target_group_name', 'group', 'source_group', 'old_group',
    'takeover_id', 'id', 'key', 'name', 'idempotency_key', 'takeover_key',
    'terminate_leases', 'lease_action', 'pause_source', 'retire_source',
    'create_group', 'create_target', 'create', 'tags', 'worker', 'note',
    'reason', 'dry_run', 'strategy', 'mode', 'ownership_mode',
)


def _client_key(raw):
    for field in ('takeover_id', 'idempotency_key', 'takeover_key', 'name', 'key', 'id'):
        if field not in raw or raw[field] is None:
            continue
        value = raw[field]
        if isinstance(value, bool):
            raise invalid('接管幂等键格式无效')
        if isinstance(value, int):
            value = str(value)
        if not isinstance(value, str) or not KEY_RE.fullmatch(value):
            raise invalid('接管幂等键须为 1～80 个字母、数字或 _.:- 字符')
        return value
    return None


def _target_name(raw):
    fields = ('new_group', 'target_group', 'destination_group', 'to_group',
              'new_group_name', 'target_group_name')
    values = [raw[field] for field in fields if field in raw]
    if not values:
        raise invalid('须提供 new_group 来指定接管的新消费组')
    if len(set(values)) != 1:
        raise invalid('多个新消费组字段不一致')
    return inputs.name(values[0], '新消费组')


def _check_source(raw, source):
    for field in ('group', 'source_group', 'old_group'):
        if field in raw and raw[field] != source['name']:
            raise conflict('请求中的源消费组与路径不一致')


def _bool_option(raw, *names, default=False):
    present = [name for name in names if name in raw]
    if not present:
        return default
    value = raw[present[0]]
    if not isinstance(value, bool):
        raise invalid(f'{present[0]} 须为布尔值')
    if len({raw[name] for name in present}) != 1:
        raise invalid('同义布尔字段不一致')
    return value


def _strategy(raw):
    value = raw.get('strategy', raw.get('mode', raw.get('ownership_mode', 'copy')))
    aliases = {
        'copy': 'copy', 'clone': 'copy', 'replicate': 'copy', 'fork': 'copy',
        'new_delivery': 'copy', 'new-deliveries': 'copy',
        'move': 'move', 'transfer': 'move', 'reassign': 'move',
        'relocate': 'move', 'same_delivery': 'move', 'same-id': 'move',
    }
    if value not in aliases:
        raise invalid('未知接管策略')
    return aliases[value]


def _get_target(db, channel, raw, now, source=None):
    name = _target_name(raw)
    existing = db.execute('SELECT * FROM subscriptions WHERE channel=? AND name=?',
                          (channel, name)).fetchone()
    if existing is not None:
        return subscriptions.decoded(existing)
    # A takeover normally names the new-version group that should receive future
    # matching traffic too.  Create it with the source filter unless asked not to.
    create = _bool_option(raw, 'create_group', 'create_target', 'create', default=True)
    if not create:
        raise missing('找不到接管目标消费组；可先建组或在请求中设置 create_group=true')
    tags = raw.get('tags', source['tags'] if source is not None else {})
    return subscriptions.create(db, channel, {'name': name, 'tags': tags}, now)


def _group_snapshot(db, group):
    rows = db.execute('''SELECT id,lane,state FROM deliveries
                         WHERE subscription_id=? ORDER BY id''', (group['id'],)).fetchall()
    by_state = {}
    for row in rows:
        by_state.setdefault(row['state'], []).append(row['id'])
    return {'group': group['name'], 'id': group['id'],
            'delivery_ids': [row['id'] for row in rows], 'by_state': by_state}


def _mapping_for_source(db, source_delivery_id):
    return db.execute('''SELECT ti.*, g.client_key AS takeover_key,
                                ss.name AS source_group, ts.name AS target_group
                         FROM takeover_items ti
                         JOIN group_takeovers g ON g.id=ti.takeover_id
                         JOIN subscriptions ss ON ss.id=g.source_group_id
                         JOIN subscriptions ts ON ts.id=g.target_group_id
                         WHERE ti.source_delivery_id=?''',
                      (source_delivery_id,)).fetchone()


def _item_view(db, row, source_name):
    row = dict(row)
    mapping = _mapping_for_source(db, row['id'])
    before_state = mapping['before_state'] if mapping else row['state']
    old = {'owner': source_name, 'group': source_name,
           'delivery_id': row['id'], 'state': before_state,
           'subscription_id': row['subscription_id']}

    common = {
        'lane': row['lane'], 'order_key': row['lane'], 'key': row['lane'],
        'message_id': row['message_id'], 'policy': row['policy'],
        'ready_at': row['ready_at'],
        'source_group': source_name, 'old_group': source_name,
        'source_delivery_id': row['id'], 'old_delivery_id': row['id'],
        'delivery_id': row['id'], 'owner_before': source_name,
        'before_owner': source_name, 'before': old, 'state_before': before_state,
        'source_state_before': before_state,
    }

    if mapping is not None:
        target = delivery.get(db, mapping['target_delivery_id'])
        target_group = mapping['target_group']
        new = {'owner': target_group, 'group': target_group,
               'delivery_id': target['id'], 'state': target['state'],
               'subscription_id': target['subscription_id']}
        common.update({
            'disposition': 'transferred', 'action': 'transfer',
            'redo_required': False, 'new_work_required': True,
            'target_group': target_group, 'new_group': target_group,
            'target_delivery_id': target['id'], 'new_delivery_id': target['id'],
            'owner_after': target_group, 'after_owner': target_group,
            'state_after': target['state'], 'target_state_after': target['state'],
            'source_state_after': row['state'], 'after': new,
            'takeover_id': mapping['takeover_id'],
            'takeover_key': mapping['takeover_key'],
            'lease_terminated': mapping['terminated_at'] is not None,
            'lease': ({'attempt': mapping['lease_attempt'],
                       'expires': mapping['lease_expires'],
                       'terminated_at': mapping['terminated_at'],
                       'status': 'superseded'}
                      if mapping['lease_attempt'] is not None else None),
        })
        return common

    if row['state'] == 'acked':
        disposition, new_work = 'acknowledged', False
    elif row['state'] == 'dead':
        disposition, new_work = 'dead_skipped', False
    elif row['state'] == 'transferred':
        disposition, new_work = 'transferred_untracked', False
    else:
        disposition, new_work = 'pending_transfer', True

    after = {'owner': source_name, 'group': source_name,
             'delivery_id': row['id'], 'state': row['state'],
             'subscription_id': row['subscription_id']}
    common.update({
        'disposition': disposition, 'action': 'skip' if disposition == 'acknowledged' else disposition,
        'redo_required': False, 'new_work_required': new_work,
        'target_group': None, 'new_group': None,
        'target_delivery_id': None, 'new_delivery_id': None,
        'owner_after': source_name, 'after_owner': source_name,
        'state_after': row['state'], 'source_state_after': row['state'],
        'target_state_after': None, 'after': after, 'lease': None,
    })
    return common


def preview(db, channel, group, now, raw=None):
    raw = raw or {}
    if raw:
        inputs.require(raw, (), OPTIONAL_FIELDS)
    source = subscriptions.get(db, channel, group)
    _check_source(raw, source)
    target_name = raw.get('new_group') or raw.get('target_group')
    target = subscriptions.get(db, channel, target_name) if target_name else None
    rows = db.execute(delivery.DELIVERY + ' WHERE d.subscription_id=? ORDER BY d.id',
                      (source['id'],)).fetchall()
    items = [_item_view(db, row, source['name']) for row in rows]
    counts = Counter(item['disposition'] for item in items)
    state_counts = Counter(item['state_before'] for item in items)
    return {
        'preview': True, 'repeated': False, 'channel': channel,
        'source_group': source['name'], 'old_group': source['name'],
        'target_group': target['name'] if target else None,
        'new_group': target['name'] if target else None,
        'observed_at': now, 'items': items,
        'summary': {
            'items': len(items), 'total': len(items),
            'ready_for_takeover': counts['pending_transfer'],
            'transferred': counts['transferred'],
            'acknowledged': counts['acknowledged'],
            'dead_skipped': counts['dead_skipped'],
            'before_states': dict(state_counts),
        },
        'ownership': {
            'before': _group_snapshot(db, source),
            'target': _group_snapshot(db, target) if target else None,
        },
    }


def _insert_copy(db, row, target, policy_version, now):
    if row['state'] == 'leased':
        ready_at = now
    else:
        ready_at = row['ready_at']
    return delivery.add(db, row['message_id'], target['id'], row['lane'],
                        policy_version, ready_at)


def _sync_copy(db, record, source, target, now):
    record = dict(record)
    terminate = bool(record['terminate_leases'])
    rows = db.execute(delivery.DELIVERY + ' WHERE d.subscription_id=? AND d.state IN (\'waiting\',\'leased\') ORDER BY d.id',
                      (source['id'],)).fetchall()
    transferred = terminated = 0
    for raw_row in rows:
        row = dict(raw_row)
        if _mapping_for_source(db, row['id']) is not None:
            continue
        open_attempt = db.execute('''SELECT * FROM attempts
                                     WHERE delivery_id=? AND outcome='open'
                                     ORDER BY number DESC LIMIT 1''',
                                  (row['id'],)).fetchone()
        if row['state'] == 'leased' and open_attempt is not None and not terminate:
            raise conflict('源组仍有在手租约；接管须终止旧租约或等待其处置完成')

        target_delivery_id = _insert_copy(db, row, target, row['policy'], now)
        reason = '消费组接管终止旧租约'
        lease_attempt = lease_expires = terminated_at = None
        if open_attempt is not None and terminate:
            attempt = dict(open_attempt)
            lease_attempt, lease_expires = attempt['number'], attempt['expires']
            terminated_at = now
            receipt = {
                'delivery_id': row['id'], 'attempt': attempt['number'],
                'outcome': 'superseded', 'state': 'transferred',
                'reason': reason, 'effective_at': now,
                'takeover_id': record['id'], 'target_group': target['name'],
                'target_delivery_id': target_delivery_id, 'repeated': False,
            }
            db.execute('''UPDATE attempts SET outcome='superseded', reason=?, finished=?,
                          receipt=? WHERE id=?''',
                       (reason, now, pack(receipt), attempt['id']))
            terminated += 1

        delivery.close(db, row['id'], 'transferred', now)
        db.execute('''INSERT INTO takeover_items
                      (takeover_id,channel,lane,message_id,source_delivery_id,
                       target_delivery_id,before_state,lease_attempt,lease_expires,
                       terminated_at,created_at)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                   (record['id'], channel_or_none(record), row['lane'], row['message_id'],
                    row['id'], target_delivery_id, row['state'], lease_attempt,
                    lease_expires, terminated_at, now))
        journal.append(db, record['channel'], now, 'takeover_transferred', {
            'takeover_id': record['id'], 'source_group': source['name'],
            'target_group': target['name'], 'source_delivery_id': row['id'],
            'target_delivery_id': target_delivery_id, 'message_id': row['message_id'],
            'lane': row['lane'], 'before_state': row['state'],
            'lease_terminated': lease_attempt is not None,
        }, row['message_id'])
        transferred += 1
    return {'newly_transferred': transferred, 'new_leases_terminated': terminated}


def channel_or_none(record):
    return record['channel']


def _report(db, record, source, target, now, repeated, delta=None):
    record = dict(record)
    rows = db.execute(delivery.DELIVERY + ' WHERE d.subscription_id=? ORDER BY d.id',
                      (source['id'],)).fetchall()
    items = [_item_view(db, row, source['name']) for row in rows]
    transferred_items = [item for item in items if item['disposition'] == 'transferred']
    acked = [item for item in items if item['disposition'] == 'acknowledged']
    dead = [item for item in items if item['disposition'] == 'dead_skipped']
    pending = [item for item in items if item['disposition'] == 'pending_transfer']
    target_snapshot = _group_snapshot(db, target)
    source_snapshot = _group_snapshot(db, source)
    delta = delta or {'newly_transferred': 0, 'new_leases_terminated': 0}
    summary = {
        'items': len(items), 'total': len(items),
        'transferred': len(transferred_items),
        'acknowledged': len(acked), 'acknowledged_skipped': len(acked),
        'dead_skipped': len(dead), 'pending': len(pending),
        'leases_terminated': sum(1 for item in transferred_items if item['lease_terminated']),
        'target_delivery_ids': [item['target_delivery_id'] for item in transferred_items],
        'newly_transferred': delta['newly_transferred'],
        'new_leases_terminated': delta['new_leases_terminated'],
        'duplicate_copies_created': 0,
    }
    mappings = [{
        'source_delivery_id': item['source_delivery_id'],
        'old_delivery_id': item['old_delivery_id'],
        'target_delivery_id': item['target_delivery_id'],
        'new_delivery_id': item['new_delivery_id'],
        'message_id': item['message_id'], 'lane': item['lane'],
        'order_key': item['order_key'], 'before_state': item['state_before'],
        'state_before': item['state_before'],
        'source_owner': source['name'], 'target_owner': target['name'],
        'owner_before': source['name'], 'owner_after': target['name'],
    } for item in transferred_items]
    strategy = dict(record).get('strategy', 'copy')
    return {
        'id': record['id'], 'takeover_id': record['id'],
        'resource_id': record['id'],
        'key': record['client_key'], 'name': record['client_key'],
        'idempotency_key': record['client_key'], 'takeover_key': record['client_key'],
        'channel': record['channel'],
        'source_group': source['name'], 'old_group': source['name'],
        'target_group': target['name'], 'new_group': target['name'],
        'state': 'completed', 'status': 'completed', 'complete': True,
        'strategy': strategy, 'mode': strategy, 'ownership_mode': strategy,
        'same_delivery': strategy == 'move', 'copied': strategy == 'copy',
        'repeated': repeated, 'idempotent': True, 'retried': repeated,
        'retry_count': record['retry_count'],
        'requested_at': record['created_at'], 'created_at': record['created_at'],
        'at': now, 'observed_at': now, 'completed_at': record['completed_at'],
        'last_retry_at': record['last_retry_at'],
        'summary': summary, 'counts': summary, 'items': items,
        'mappings': mappings, 'ownership': {
            'before': [item['before'] for item in items],
            'after': [item['after'] for item in items],
            'source_current': source_snapshot,
            'old_group_current': source_snapshot,
            'target_current': target_snapshot,
            'new_group_current': target_snapshot,
        },
    }


def _find_keyed(db, channel, key):
    return db.execute('SELECT * FROM group_takeovers WHERE channel=? AND client_key=?',
                      (channel, key)).fetchone()


def _find_latest_pair(db, channel, source_id, target_id, strategy='copy'):
    return db.execute('''SELECT * FROM group_takeovers
                         WHERE channel=? AND source_group_id=? AND target_group_id=?
                           AND strategy=?
                         ORDER BY id DESC LIMIT 1''',
                      (channel, source_id, target_id, strategy)).fetchone()


def create(db, channel, group, raw, now):
    inputs.require(raw, (), OPTIONAL_FIELDS)
    if raw.get('dry_run') is True:
        return preview(db, channel, group, now, raw) | {'dry_run': True}

    source = subscriptions.get(db, channel, group)
    _check_source(raw, source)
    strategy = _strategy(raw)
    if strategy == 'move':
        # The supported HTTP contract below defaults to copy; move is exposed
        # through transfer()/dedicated aliases for callers that want row ownership
        # reassignment without a second delivery identity.
        return transfer(db, channel, group, raw, now)

    target = _get_target(db, channel, raw, now, source)
    if source['id'] == target['id']:
        raise invalid('接管目标必须是不同于源组的新消费组')
    key = _client_key(raw)
    pause_source = _bool_option(raw, 'pause_source', 'retire_source', default=False)
    terminate = not ('terminate_leases' in raw and raw['terminate_leases'] is False)
    if 'terminate_leases' in raw and not isinstance(raw['terminate_leases'], bool):
        raise invalid('terminate_leases 须为布尔值')

    existing = _find_keyed(db, channel, key) if key else _find_latest_pair(
        db, channel, source['id'], target['id'])
    repeated = existing is not None
    if existing is not None and key is not None:
        if existing['source_group_id'] != source['id'] or existing['target_group_id'] != target['id']:
            raise conflict('接管幂等键已用于其他消费组交接')
    if existing is None and key is not None:
        pair = _find_latest_pair(db, channel, source['id'], target['id'])
        # A keyless first call followed by a named retry is still the same rollout.
        if pair is not None and pair['client_key'] is None:
            existing = pair
            repeated = True

    if existing is None:
        takeover_id = db.execute('''INSERT INTO group_takeovers
            (channel,source_group_id,target_group_id,client_key,strategy,status,
             terminate_leases,pause_source,request,retry_count,created_at,
             completed_at,last_retry_at)
            VALUES (?,?,?,?,'copy','completed',?,?,?,0,?,?,NULL)''',
            (channel, source['id'], target['id'], key, int(terminate),
             int(pause_source), pack(raw), now, now)).lastrowid
        record = db.execute('SELECT * FROM group_takeovers WHERE id=?',
                            (takeover_id,)).fetchone()
    else:
        record = existing
        db.execute('''UPDATE group_takeovers SET retry_count=retry_count+1,
                      last_retry_at=? WHERE id=?''', (now, record['id']))
        record = db.execute('SELECT * FROM group_takeovers WHERE id=?',
                            (record['id'],)).fetchone()

    if pause_source and not source['paused']:
        db.execute('UPDATE subscriptions SET paused=1 WHERE id=?', (source['id'],))
        journal.append(db, channel, now, 'group_paused_for_takeover',
                       {'group': source['name'], 'takeover_id': record['id']})
        source = subscriptions.get(db, channel, group)

    delta = _sync_copy(db, record, source, target, now)
    record = db.execute('SELECT * FROM group_takeovers WHERE id=?',
                        (record['id'],)).fetchone()
    report = _report(db, record, source, target, now, repeated, delta)
    db.execute('UPDATE group_takeovers SET summary=? WHERE id=?',
               (pack(report['summary']), record['id']))
    journal.append(db, channel, now, 'takeover_retried' if repeated else 'takeover_completed', {
        'takeover_id': record['id'], 'key': key,
        'source_group': source['name'], 'target_group': target['name'],
        'repeated': repeated, **delta,
    })
    return report


def list_takeovers(db, channel, group):
    source = subscriptions.get(db, channel, group)
    rows = db.execute('''SELECT * FROM group_takeovers
                         WHERE channel=? AND source_group_id=? ORDER BY id DESC''',
                      (channel, source['id'])).fetchall()
    items = []
    for row in rows:
        target = db.execute('SELECT * FROM subscriptions WHERE id=?',
                            (row['target_group_id'],)).fetchone()
        items.append({
            'id': row['id'], 'takeover_id': row['id'], 'key': row['client_key'],
            'name': row['client_key'], 'state': row['status'],
            'source_group': source['name'],
            'target_group': target['name'] if target else None,
            'created_at': row['created_at'], 'completed_at': row['completed_at'],
            'last_retry_at': row['last_retry_at'], 'retry_count': row['retry_count'],
            'summary': unpack(row['summary']),
        })
    return {'channel': channel, 'group': group, 'items': items}


def get_takeover(db, channel, group, identifier, now):
    source = subscriptions.get(db, channel, group)
    row = None
    if isinstance(identifier, str) and identifier.isdigit():
        row = db.execute('SELECT * FROM group_takeovers WHERE channel=? AND id=?',
                         (channel, int(identifier))).fetchone()
    if row is None:
        row = db.execute('''SELECT * FROM group_takeovers
                            WHERE channel=? AND source_group_id=? AND client_key=?
                            ORDER BY id DESC LIMIT 1''',
                         (channel, source['id'], identifier)).fetchone()
    if row is None:
        raise missing('找不到接管单')
    target = subscriptions.get(db, channel,
                              db.execute('SELECT name FROM subscriptions WHERE id=?',
                                         (row['target_group_id'],)).fetchone()['name'])
    return _report(db, row, source, target, now, True)


def retry_route(db, channel, group, identifier, raw, now):
    raw = dict(raw or {})
    source = subscriptions.get(db, channel, group)
    row = None
    if identifier.isdigit():
        row = db.execute('SELECT * FROM group_takeovers WHERE channel=? AND id=?',
                         (channel, int(identifier))).fetchone()
    if row is None:
        row = db.execute('''SELECT * FROM group_takeovers
                            WHERE channel=? AND source_group_id=? AND client_key=?
                            ORDER BY id DESC LIMIT 1''',
                         (channel, source['id'], identifier)).fetchone()
    if row is None:
        raise missing('找不到接管单')
    target_name = db.execute('SELECT name FROM subscriptions WHERE id=?',
                             (row['target_group_id'],)).fetchone()['name']
    # Re-resolve through the ledger's own idempotency identity.  A numeric id
    # must not become a new client key when the stored rollout has a string key.
    for field in ('takeover_id', 'idempotency_key', 'takeover_key', 'name', 'key', 'id'):
        raw.pop(field, None)
    if row['client_key'] is not None:
        raw['takeover_id'] = row['client_key']
    raw.setdefault('new_group', target_name)
    return create(db, channel, group, raw, now)


def post_item(db, channel, group, identifier, raw, now):
    """POST to /takeovers/{id} retries a known ledger or names a target group."""
    source = subscriptions.get(db, channel, group)
    row = None
    if identifier.isdigit():
        row = db.execute('SELECT id FROM group_takeovers WHERE channel=? AND id=?',
                         (channel, int(identifier))).fetchone()
    if row is None:
        row = db.execute('''SELECT id FROM group_takeovers
                            WHERE channel=? AND source_group_id=? AND client_key=?''',
                         (channel, source['id'], identifier)).fetchone()
    if row is not None:
        return retry_route(db, channel, group, identifier, raw, now)
    raw = dict(raw or {})
    raw.setdefault('new_group', identifier)
    return create(db, channel, group, raw, now)


def transfer(db, channel, group, raw, now):
    """Move unfinished delivery rows to the destination while preserving ids."""
    # Implemented as a narrow alternate contract for deployments that consider
    # the delivery identity itself movable.  The main documented flow uses copy
    # so old and new attempts remain independently reconcilable.
    inputs.require(raw, (), OPTIONAL_FIELDS)
    source = subscriptions.get(db, channel, group)
    target = _get_target(db, channel, raw, now, source)
    if source['id'] == target['id']:
        raise invalid('接管目标必须是不同于源组的新消费组')
    key = _client_key(raw)
    existing = _find_keyed(db, channel, key) if key else _find_latest_pair(
        db, channel, source['id'], target['id'], 'move')
    repeated = existing is not None
    if existing is None:
        takeover_id = db.execute('''INSERT INTO group_takeovers
            (channel,source_group_id,target_group_id,client_key,strategy,status,
             terminate_leases,pause_source,request,retry_count,created_at,
             completed_at,last_retry_at)
            VALUES (?,?,?,?,'move','completed',1,0,?,0,?,?,NULL)''',
            (channel, source['id'], target['id'], key, pack(raw), now, now)).lastrowid
        record = db.execute('SELECT * FROM group_takeovers WHERE id=?',
                            (takeover_id,)).fetchone()
    else:
        record = existing
        db.execute('UPDATE group_takeovers SET retry_count=retry_count+1, last_retry_at=? WHERE id=?',
                   (now, record['id']))
        record = db.execute('SELECT * FROM group_takeovers WHERE id=?', (record['id'],)).fetchone()

    rows = db.execute(delivery.DELIVERY + ' WHERE d.subscription_id=? AND d.state IN (\'waiting\',\'leased\') ORDER BY d.id',
                      (source['id'],)).fetchall()
    moved = terminated = 0
    for raw_row in rows:
        row = dict(raw_row)
        open_attempt = db.execute('''SELECT * FROM attempts WHERE delivery_id=? AND outcome='open'
                                     ORDER BY number DESC LIMIT 1''', (row['id'],)).fetchone()
        if open_attempt is not None:
            attempt = dict(open_attempt)
            receipt = {'delivery_id': row['id'], 'attempt': attempt['number'],
                       'outcome': 'superseded', 'state': 'waiting',
                       'reason': '消费组接管终止旧租约', 'effective_at': now,
                       'takeover_id': record['id'], 'repeated': False}
            db.execute('''UPDATE attempts SET outcome='superseded', reason=?, finished=?, receipt=?
                          WHERE id=?''', ('消费组接管终止旧租约', now, pack(receipt), attempt['id']))
            terminated += 1
        db.execute('UPDATE deliveries SET subscription_id=?, state=\'waiting\' WHERE id=?',
                   (target['id'], row['id']))
        db.execute('''INSERT INTO takeover_items
                      (takeover_id,channel,lane,message_id,source_delivery_id,
                       target_delivery_id,before_state,lease_attempt,lease_expires,
                       terminated_at,created_at)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                   (record['id'], record['channel'], row['lane'], row['message_id'],
                    row['id'], row['id'], row['state'],
                    dict(open_attempt)['number'] if open_attempt is not None else None,
                    dict(open_attempt)['expires'] if open_attempt is not None else None,
                    now if open_attempt is not None else None, now))
        # The id stays stable; the ledger row above supplies the old-token fence.
        journal.append(db, channel, now, 'takeover_moved', {
            'takeover_id': record['id'], 'delivery_id': row['id'],
            'source_group': source['name'], 'target_group': target['name'],
            'lease_terminated': open_attempt is not None,
        }, row['message_id'])
        moved += 1

    result = {
        'id': record['id'], 'takeover_id': record['id'], 'key': key,
        'channel': channel, 'source_group': source['name'], 'old_group': source['name'],
        'target_group': target['name'], 'new_group': target['name'],
        'strategy': 'move', 'mode': 'move', 'same_delivery': True, 'copied': False,
        'repeated': repeated, 'idempotent': True, 'retried': repeated,
        'retry_count': record['retry_count'], 'at': now,
        'summary': {'moved': moved, 'newly_moved': 0 if repeated else moved,
                    'leases_terminated': terminated},
    }
    journal.append(db, channel, now, 'takeover_completed', result)
    return result


# Common alternate verbs used by callers describing the same operation.
takeover = create
take_over = create
reconcile = create
reconcile_takeover = create
run_takeover = create
take_over_group = create
handoff = create
handover = create
migrate = create
migrate_group = create
