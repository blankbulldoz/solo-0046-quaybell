from collections import Counter

from . import attempts, catalog, deadletters, delivery, inputs, journal, policy
from .errors import conflict, invalid, missing

BATCH_LIMIT = 100
PREVIEW_STATES = ('eligible', 'already_redriven')
CONFIRM_STATES = ('redriven', 'already_redriven', 'stale_policy')


def redrive(db, key, now):
    prior = deadletters.get(db, key)
    existing = db.execute('SELECT id,policy FROM deliveries WHERE parent_id=?', (key,)).fetchone()
    if existing:
        return {'source_id': key, 'delivery_id': existing['id'], 'policy': existing['policy'], 'repeated': True}
    return _create_successor(db, prior, now)


def _create_successor(db, prior, now):
    active = policy.read(db, prior['channel'])
    new_id = delivery.add(db, prior['message_id'], prior['subscription_id'], prior['lane'],
                          active['version'], now, prior['id'])
    result = {'source_id': prior['id'], 'delivery_id': new_id, 'policy': active['version'], 'repeated': False}
    journal.append(db, prior['channel'], now, 'redriven', result, prior['message_id'])
    return result


def _successor(db, key):
    return db.execute('SELECT id,policy FROM deliveries WHERE parent_id=?', (key,)).fetchone()


def _batch_ids(raw, *, with_expected=False):
    optional = ('expected',) if with_expected else ()
    inputs.require(raw, ('delivery_ids',), optional)
    values = raw['delivery_ids']
    if not isinstance(values, list) or not values or len(values) > BATCH_LIMIT:
        raise invalid(f'重投编号须为 1～{BATCH_LIMIT} 个投递编号组成的列表')
    ids = [inputs.integer(value, 1, 2 ** 63 - 2, '投递编号') for value in values]
    if len(set(ids)) != len(ids):
        raise invalid('重投编号不得重复')
    return ids


def _load_dead(db, channel, ids):
    rows = {}
    for key in ids:
        item = delivery.get(db, key)
        if item['channel'] != channel:
            raise missing('找不到投递记录')
        if item['state'] != 'dead':
            raise conflict('投递尚未进入死信状态')
        rows[key] = item
    return rows


def _preview_item(db, item, active_version):
    child = _successor(db, item['id'])
    return {
        'delivery_id': item['id'], 'message_id': item['message_id'], 'group_name': item['group_name'],
        'lane': item['lane'], 'source_policy': item['policy'], 'current_policy': active_version,
        'state': 'already_redriven' if child else 'eligible',
        'successor_id': child['id'] if child else None,
        'last_reason': _last_reason(db, item['id']),
    }


def _last_reason(db, key):
    history = attempts.for_delivery(db, key)
    return history[-1]['reason'] if history else None


def preview(db, channel, raw, now):
    catalog.get(db, channel)
    ids = _batch_ids(raw)
    rows = _load_dead(db, channel, ids)
    active = policy.read(db, channel)
    items = [_preview_item(db, rows[key], active['version']) for key in ids]
    totals, eligible = Counter(), Counter()
    for item in items:
        totals[item['group_name']] += 1
        if item['state'] == 'eligible':
            eligible[item['group_name']] += 1
    counts = Counter(item['state'] for item in items)
    return {
        'channel': channel, 'observed_at': now, 'policy_version': active['version'],
        'affected_groups': [{'group': name, 'total': totals[name], 'eligible': eligible.get(name, 0)}
                            for name in sorted(totals)],
        'counts': {state: counts.get(state, 0) for state in PREVIEW_STATES},
        'items': items,
    }


def confirm(db, channel, raw, now):
    catalog.get(db, channel)
    ids = _batch_ids(raw, with_expected=True)
    expected = inputs.integer(raw.get('expected'), 1, 2 ** 63 - 2, '预期策略版号')
    rows = _load_dead(db, channel, ids)
    active = policy.read(db, channel)
    items, counts, totals, created = [], Counter(), Counter(), Counter()
    for key in ids:
        child = _successor(db, key)
        if child:
            state, successor_id, version = 'already_redriven', child['id'], child['policy']
        elif active['version'] != expected:
            state, successor_id, version = 'stale_policy', None, None
        else:
            made = _create_successor(db, rows[key], now)
            state, successor_id, version = 'redriven', made['delivery_id'], made['policy']
        counts[state] += 1
        group_name = rows[key]['group_name']
        totals[group_name] += 1
        if state == 'redriven':
            created[group_name] += 1
        items.append({'delivery_id': key, 'message_id': rows[key]['message_id'], 'group_name': group_name,
                      'lane': rows[key]['lane'], 'source_policy': rows[key]['policy'],
                      'current_policy': active['version'], 'last_reason': _last_reason(db, key),
                      'state': state, 'successor_id': successor_id, 'policy': version,
                      'repeated': state == 'already_redriven'})
    return {
        'channel': channel, 'observed_at': now, 'expected_version': expected,
        'policy_version': active['version'],
        'affected_groups': [{'group': name, 'total': totals[name], 'redriven': created.get(name, 0)}
                            for name in sorted(totals)],
        'counts': {state: counts.get(state, 0) for state in CONFIRM_STATES},
        'items': items,
    }
