"""SQLite-backed endpoints for gap-aware source confluence."""

from __future__ import annotations

from . import codec, inputs, journal, subscriptions
from .confluence import SequenceConfluence
from .errors import invalid, missing


def _budget(raw):
    for key in ('time_budget_seconds', 'budget_seconds', 'gap_budget_seconds',
                'wait_budget_seconds', 'time_budget', 'budget', 'timeout_seconds',
                'max_wait_seconds', 'gap_timeout', 'gap_timeout_seconds',
                'wait_timeout_seconds', 'max_gap_wait_seconds'):
        if key in raw and raw[key] is not None:
            value = raw[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise invalid('合流时间预算须为数值秒数')
            if not 0 <= value <= 86400:
                raise invalid('合流时间预算须为 0～86400 秒')
            return float(value)
    return None


def _source_name(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise invalid('来源须为 1～64 字符的字符串或整数标识')
    if isinstance(value, str) and not 1 <= len(value) <= 64:
        raise invalid('来源须为 1～64 字符的字符串')
    return value


def _sequence(raw):
    value = None
    for key in ('sequence', 'seq', 'sequence_number', 'source_sequence', 'number'):
        if key in raw and raw[key] is not None:
            value = raw[key]
            break
    return inputs.integer(value, 0, 2**63 - 2, '来源序号')


def _options(raw):
    options = {}
    budget = _budget(raw)
    if budget is not None:
        options['budget'] = budget
    if 'start' in raw:
        options['start'] = inputs.integer(raw['start'], 0, 2**31 - 1, '起始序号')
    if 'initial_sequence' in raw:
        options['start'] = inputs.integer(raw['initial_sequence'], 0, 2**31 - 1, '起始序号')
    if 'anchor_on_first' in raw:
        options['anchor_on_first'] = inputs.boolean(raw['anchor_on_first'], '首项锚定')
    if 'block_other_sources' in raw:
        options['block_other_sources'] = inputs.boolean(raw['block_other_sources'], '阻挡其他来源')
    sources = raw.get('sources') or raw.get('source_order')
    if sources is not None:
        if not isinstance(sources, list) or not all(isinstance(s, str) and s for s in sources):
            raise invalid('sources 须为非空字符串数组')
        options['sources'] = sources
    return options


def _row(db, group):
    return db.execute('SELECT * FROM confluence_states WHERE subscription_id=?',
                      (group['id'],)).fetchone()


def is_enabled(db, group_id):
    return db.execute('SELECT 1 FROM confluence_states WHERE subscription_id=?',
                      (group_id,)).fetchone() is not None


def load(db, channel, group_name):
    group = subscriptions.get(db, channel, group_name)
    row = _row(db, group)
    if row is None:
        raise missing('该消费组尚未开启合流模式')
    coordinator = SequenceConfluence.from_state(codec.unpack(row['body']))
    return group, row, coordinator


def _store(db, channel, group, coordinator, now, created=False):
    body = codec.pack(coordinator.to_state())
    if created:
        db.execute('''INSERT INTO confluence_states(subscription_id,channel,group_name,body,created,updated)
                      VALUES (?,?,?,?,?,?)''',
                   (group['id'], channel, group['name'], body, now, now))
    else:
        db.execute('''UPDATE confluence_states SET channel=?, group_name=?, body=?, updated=?
                      WHERE subscription_id=?''',
                   (channel, group['name'], body, now, group['id']))

    ids = {}
    for record in coordinator.records.values():
        source, sequence = record['source'], record['sequence']
        packed = codec.pack(record)
        receipt = db.execute('''SELECT id FROM confluence_receipts
                                WHERE subscription_id=? AND source=? AND sequence=?''',
                            (group['id'], source, sequence)).fetchone()
        if receipt is None:
            cursor = db.execute('''INSERT INTO confluence_receipts
                          (subscription_id,local_id,source,sequence,status,traceable,body,updated)
                          VALUES (?,?,?,?,?,?,?,?)''',
                       (group['id'], record['id'], source, sequence, record['status'],
                        1 if record.get('traceable') else 0, packed, now))
            global_id = cursor.lastrowid
        else:
            global_id = receipt['id']
            db.execute('''UPDATE confluence_receipts
                          SET local_id=?, status=?, traceable=?, body=?, updated=?
                          WHERE id=?''',
                       (record['id'], record['status'],
                        1 if record.get('traceable') else 0, packed, now, global_id))
        ids[(source, sequence)] = global_id
        gate_state = 'released' if record['status'] in ('released', 'reconciled') else (
            'late' if record['status'] == 'late' else 'buffered')
        db.execute('''UPDATE confluence_delivery_links
                      SET gate_state=?, local_id=?, updated=?
                      WHERE subscription_id=? AND source=? AND sequence=?
                        AND gate_state != 'duplicate' ''',
                   (gate_state, record['id'], now, group['id'], source, sequence))
    return ids


def register_delivery(db, channel, group, source, sequence, delivery_id, now, payload=None):
    """Attach an ordinary broker delivery to its per-source sequence gate."""
    row = _row(db, group)
    if row is None:
        return None
    coordinator = SequenceConfluence.from_state(codec.unpack(row['body']))
    result = coordinator.submit(source, sequence, payload, at=now)
    if not result.get('duplicate'):
        coordinator.records[result['id']]['delivery_id'] = delivery_id
        result['delivery_id'] = delivery_id
    _store(db, channel, group, coordinator, now)

    duplicate = bool(result.get('duplicate'))
    if duplicate:
        gate_state = 'duplicate'
        link = db.execute('''SELECT delivery_id FROM confluence_delivery_links
                             WHERE subscription_id=? AND source=? AND sequence=?
                               AND gate_state != 'duplicate'
                             ORDER BY delivery_id LIMIT 1''',
                          (group['id'], source, sequence)).fetchone()
        duplicate_of_delivery = link['delivery_id'] if link else None
    else:
        gate_state = 'released' if result['status'] in ('released', 'reconciled') else (
            'late' if result['status'] == 'late' else 'buffered')
        duplicate_of_delivery = None
    db.execute('''INSERT INTO confluence_delivery_links
                  (delivery_id,subscription_id,source,sequence,local_id,gate_state,
                   duplicate_of_delivery_id,created,updated)
                  VALUES (?,?,?,?,?,?,?,?,?)''',
               (delivery_id, group['id'], source, sequence, result['id'], gate_state,
                duplicate_of_delivery, now, now))
    if duplicate:
        result['submission_delivery_id'] = delivery_id
    else:
        result['delivery_id'] = delivery_id
    result['gate_state'] = gate_state
    if duplicate_of_delivery:
        result['duplicate_delivery_id'] = duplicate_of_delivery
    result.pop('snapshot', None)
    return result


def _with_trace_ids(value, ids, channel, group):
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            out[key] = _with_trace_ids(item, ids, channel, group)
        source = out.get('source')
        sequence = out.get('sequence')
        if source is not None and isinstance(sequence, int) and (source, sequence) in ids:
            global_id = ids[(source, sequence)]
            out['trace_record_id'] = global_id
            out['trace_url'] = f'/v1/confluence-records/{global_id}'
            local_id = out.get('record_id')
            if not isinstance(local_id, int) and 'identity' in out and isinstance(out.get('id'), int):
                local_id = out['id']
            if isinstance(local_id, int):
                out['group_trace_url'] = (
                    f'/v1/channels/{channel}/groups/{group}/confluence/records/{local_id}'
                )
        return out
    if isinstance(value, list):
        return [_with_trace_ids(item, ids, channel, group) for item in value]
    return value


def _configuration(raw):
    data = dict(raw or {})
    for nested_key in ('config', 'settings', 'options', 'confluence', 'merge', 'mode_config'):
        nested = data.get(nested_key)
        if isinstance(nested, dict):
            for key, value in nested.items():
                data.setdefault(key, value)
    return data


def configure(db, channel, group_name, raw, now):
    raw = _configuration(raw)
    group = subscriptions.get(db, channel, group_name)
    existing = _row(db, group)
    options = _options(raw)
    if existing is None:
        coordinator = SequenceConfluence(**options)
        created = True
    else:
        coordinator = SequenceConfluence.from_state(codec.unpack(existing['body']))
        coordinator.configure(options)
        created = False
    ids = _store(db, channel, group, coordinator, now, created=created)
    enabled = raw.get('enabled', raw.get('merge', raw.get('confluence', True)))
    mode = raw.get('mode') or raw.get('merge_mode') or 'sequence'
    if enabled is False or mode in ('off', 'disabled', 'all', 'ordinary'):
        # Sequence confluence is opt-in.  "all" restores ordinary delivery;
        # retaining the row would make that ambiguous, so remove config/receipts.
        db.execute('DELETE FROM confluence_states WHERE subscription_id=?', (group['id'],))
        db.execute('DELETE FROM confluence_receipts WHERE subscription_id=?', (group['id'],))
        journal.append(db, channel, now, 'confluence_disabled', {'group': group_name})
        return {'enabled': False, 'mode': 'all', 'group': group_name}

    journal.append(db, channel, now, 'confluence_configured', {
        'group': group_name, 'mode': mode,
        'time_budget_seconds': coordinator.time_budget_seconds,
        'sources': coordinator.source_order,
    })
    snapshot = coordinator.snapshot()
    data = {
        'enabled': True,
        'mode': 'confluence',
        'requested_mode': mode,
        'group': group_name,
        'channel': channel,
        'created': created,
        'config_url': f'/v1/channels/{channel}/groups/{group_name}/confluence',
        'submit_url': f'/v1/channels/{channel}/groups/{group_name}/confluence/records',
        'advance_url': f'/v1/channels/{channel}/groups/{group_name}/confluence/advance',
        'time_budget_seconds': coordinator.time_budget_seconds,
        'budget_seconds': coordinator.time_budget_seconds,
        'sources': coordinator.source_order,
        'gaps': snapshot['gaps'],
        'missing_sequences': snapshot['missing_sequences'],
        'counts': snapshot['counts'],
        'snapshot': snapshot,
    }
    return _with_trace_ids(data, ids, channel, group_name)


def submit(db, channel, group_name, raw, now):
    raw = dict(raw or {})
    for nested_key in ('record', 'item', 'event'):
        nested = raw.get(nested_key)
        if isinstance(nested, dict):
            for key, value in nested.items():
                raw.setdefault(key, value)
    group = subscriptions.get(db, channel, group_name)
    row = _row(db, group)
    if row is None:
        # Convenience for compact clients: a submit carrying a budget can turn
        # the group into confluence mode in the same call.
        coordinator = SequenceConfluence(**_options(raw))
        created = True
    else:
        coordinator = SequenceConfluence.from_state(codec.unpack(row['body']))
        created = False

    source = _source_name(raw.get('source', raw.get('source_id',
                          raw.get('chain', raw.get('origin')))))
    sequence = _sequence(raw)
    payload = raw.get('payload', raw.get('data', raw.get('body')))
    at = now
    if any(k in raw for k in ('at', 'now', 'observation_time')):
        candidate = raw.get('at', raw.get('now', raw.get('observation_time')))
        if not isinstance(candidate, (int, float)) or isinstance(candidate, bool):
            raise invalid('观察时间须为 Unix 秒数值')
        at = float(candidate)

    result = coordinator.submit(source, sequence, payload, at=at)
    ids = _store(db, channel, group, coordinator, now, created=created)
    global_id = ids[(source, sequence)]
    result['trace_record_id'] = global_id
    result['trace_url'] = f'/v1/confluence-records/{global_id}'
    result['group_trace_url'] = (
        f'/v1/channels/{channel}/groups/{group_name}/confluence/records/{result["id"]}'
    )
    journal.append(db, channel, now, 'confluence_submitted', {
        'group': group_name, 'source': source, 'sequence': sequence,
        'status': result['status'], 'duplicate': bool(result.get('duplicate')),
        'trace_record_id': global_id,
    })
    result['snapshot'] = _with_trace_ids(coordinator.snapshot(), ids, channel, group_name)
    return _with_trace_ids(result, ids, channel, group_name)


def snapshot(db, channel, group_name, now=None):
    group, row, coordinator = load(db, channel, group_name)
    ids = {}
    for receipt in db.execute('SELECT id,source,sequence FROM confluence_receipts WHERE subscription_id=?',
                              (group['id'],)):
        ids[(receipt['source'], receipt['sequence'])] = receipt['id']
    data = {
        'enabled': True,
        'mode': 'confluence',
        'group': group_name,
        'channel': channel,
        'observed_at': now,
        **coordinator.snapshot(),
    }
    return _with_trace_ids(data, ids, channel, group_name)


def gaps(db, channel, group_name, now=None):
    data = snapshot(db, channel, group_name, now)
    return {'channel': channel, 'group': group_name,
            'gaps': data['gaps'], 'missing_sequences': data['missing_sequences'],
            'gap_report': data['gap_report']}


def advance(db, channel, group_name, raw, now):
    group, row, coordinator = load(db, channel, group_name)
    at = now
    if raw and any(k in raw for k in ('now', 'at')):
        candidate = raw.get('now', raw.get('at'))
        if not isinstance(candidate, (int, float)) or isinstance(candidate, bool):
            raise invalid('推进时间须为 Unix 秒数值')
        at = float(candidate)
    result = coordinator.advance(at)
    ids = _store(db, channel, group, coordinator, now)
    journal.append(db, channel, now, 'confluence_advanced', {
        'group': group_name,
        'expired': len(result['skipped_gaps']),
        'released': len(result['released']),
    })
    result['channel'] = channel
    result['group'] = group_name
    return _with_trace_ids(result, ids, channel, group_name)


def global_trace(db, record_global_id):
    receipt = db.execute('SELECT * FROM confluence_receipts WHERE id=?',
                         (record_global_id,)).fetchone()
    if receipt is None:
        raise missing('找不到合流追查记录')
    group = db.execute('SELECT * FROM subscriptions WHERE id=?',
                       (receipt['subscription_id'],)).fetchone()
    if group is None:
        raise missing('找不到消费组')
    state_row = _row(db, group)
    coordinator = SequenceConfluence.from_state(codec.unpack(state_row['body']))
    record = coordinator.get(receipt['local_id'])
    return {
        'id': receipt['id'],
        'trace_record_id': receipt['id'],
        'global_id': receipt['id'],
        'local_id': receipt['local_id'],
        'channel': state_row['channel'],
        'group': group['name'],
        'source': record.get('source', receipt['source']),
        'sequence': receipt['sequence'],
        'status': receipt['status'],
        'traceable': bool(receipt['traceable']),
        'trace_status': record.get('trace_status'),
        'trace_url': f'/v1/confluence-records/{receipt["id"]}',
        'group_trace_url': (f'/v1/channels/{state_row["channel"]}/groups/{group["name"]}'
                            f'/confluence/records/{receipt["local_id"]}'),
        'record': record,
        'incidents': [i for i in coordinator.incidents if i.get('record_id') == receipt['local_id']],
    }


def reconcile(db, channel, group_name, local_id, raw, now):
    group, row, coordinator = load(db, channel, group_name)
    note = None
    if isinstance(raw, dict):
        note = raw.get('note', raw.get('reason'))
        at = raw.get('now', raw.get('at', now))
        if not isinstance(at, (int, float)) or isinstance(at, bool):
            raise invalid('追平时间须为 Unix 秒数值')
    else:
        at = now
    record = coordinator.reconcile_late(int(local_id), at, note)
    _store(db, channel, group, coordinator, now)
    journal.append(db, channel, now, 'confluence_late_reconciled', {
        'group': group_name, 'local_id': int(local_id), 'record_id': record['id'],
    })
    return group_trace(db, channel, group_name, int(local_id))


def group_trace(db, channel, group_name, local_id):
    group, row, coordinator = load(db, channel, group_name)
    record = coordinator.get(int(local_id))
    receipt = db.execute('''SELECT id FROM confluence_receipts
                            WHERE subscription_id=? AND local_id=?''',
                         (group['id'], int(local_id))).fetchone()
    global_id = receipt['id'] if receipt else None
    return {
        'channel': channel,
        'group': group_name,
        'local_id': int(local_id),
        'trace_record_id': global_id,
        'trace_url': f'/v1/confluence-records/{global_id}' if global_id else None,
        'record': record,
        'incidents': [i for i in coordinator.incidents if i.get('record_id') == int(local_id)],
    }
