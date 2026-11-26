"""保留候选、冻结的清理计划以及按确认名单执行清理。

审批流程分两阶段：``prepare`` 把当时的候选名单和观察时刻冻结成持久计划；
``apply`` 只处理计划内的消息编号，并在写事务内逐条复查安全条件。预览之后
才到期的消息不在冻结名单内，不会被连带删除；名单中的死信若已被重投出
waiting/leased 后继分发，则标记为变化并跳过，避免级联删掉活跃分发。

计划执行结果持久保存：管理员重试或两人同时批准时，写事务串行提交，只有
第一个调用真正删除，其余调用读取同一份已记录结果并标记 ``repeated``。
"""

from . import catalog, inputs, journal
from .codec import pack, unpack
from .errors import missing

PLAN_LIMIT = 100
ID_LIMIT = 100
APPROVER_KEYS = ('approved_by', 'approver', 'by')
PLAN_REF_KEYS = ('plan_id', 'planId', 'retention_plan_id')
ID_LIST_KEYS = ('ids', 'message_ids', 'confirm_ids', 'confirmed_ids', 'candidate_ids')
CANDIDATE_KEYS = ('eligible', 'items', 'candidates')


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


def _approver(raw):
    value = next((raw[key] for key in APPROVER_KEYS if key in raw), None)
    if value is None:
        return None
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= 60:
        raise inputs.invalid('批准人须为 1～60 个字符的字符串')
    return value.strip()


def _positive_id(value, label='编号'):
    return inputs.integer(value, 1, 2**63 - 1, label)


def _normalize_ids(value, label):
    if not isinstance(value, list) or len(value) > ID_LIMIT:
        raise inputs.invalid(f'{label}须为至多 {ID_LIMIT} 个编号的数组')
    ids = []
    for entry in value:
        raw_id = entry.get('id') if isinstance(entry, dict) else entry
        ids.append(_positive_id(raw_id, label))
    ordered = []
    seen = set()
    for message_id in ids:
        if message_id not in seen:
            seen.add(message_id)
            ordered.append(message_id)
    return ordered


def _plan_ref_from(raw):
    for key in PLAN_REF_KEYS:
        if key in raw and raw[key] is not None:
            return _positive_id(raw[key], '清理计划编号')
    if 'plan' in raw and raw['plan'] is not None:
        value = raw['plan']
        if isinstance(value, int) and type(value) is int:
            return _positive_id(value, '清理计划编号')
        if isinstance(value, dict):
            for key in ('plan_id', 'planId', 'id'):
                if key in value:
                    return _positive_id(value[key], '清理计划编号')
    return None


def _embedded_ids(raw):
    """从各种合理的“确认名单”载荷中提取编号，找不到返回 None。"""
    for key in ID_LIST_KEYS:
        if key in raw:
            return _normalize_ids(raw[key], key)
    for key in CANDIDATE_KEYS:
        if key in raw:
            return _normalize_ids(raw[key], key)
    plan = raw.get('plan')
    if isinstance(plan, dict):
        for key in ID_LIST_KEYS:
            if key in plan:
                return _normalize_ids(plan[key], f'plan.{key}')
        for key in CANDIDATE_KEYS:
            if key in plan:
                return _normalize_ids(plan[key], f'plan.{key}')
    return None


def _plan_exists(db, channel, plan_id):
    return db.execute('SELECT 1 FROM retention_plans WHERE id=? AND channel=?',
                      (plan_id, channel)).fetchone() is not None


def prepare(db, channel, raw, now):
    inputs.require(raw, (), ('limit', *APPROVER_KEYS, 'name'))
    limit = inputs.integer(raw.get('limit', PLAN_LIMIT), 1, PLAN_LIMIT, '清理上限')
    rows = eligible(db, channel, now, limit)
    ids = [row['id'] for row in rows]
    cursor = db.execute('''
        INSERT INTO retention_plans
            (channel,status,observed_at,created_at,candidate,message_ids,approvals)
        VALUES (?, 'prepared', ?, ?, ?, ?, '[]')''',
        (channel, now, now, pack(rows), pack(ids)))
    plan_id = cursor.lastrowid
    journal.append(db, channel, now, 'retention_prepared',
                   {'plan_id': plan_id, 'message_ids': ids, 'count': len(ids)})
    return fetch(db, channel, plan_id)


def _serialize(row):
    candidate = unpack(row['candidate'])
    ids = unpack(row['message_ids'])
    approvals = unpack(row['approvals'] or '[]')
    result = unpack(row['result']) if row['result'] else None
    record = {
        'plan_id': row['id'],
        'id': row['id'],
        'channel': row['channel'],
        'status': row['status'],
        'state': row['status'],
        'outcome': 'pending' if row['status'] == 'prepared' else None,
        'completed': row['status'] == 'applied',
        'observed_at': row['observed_at'],
        'created_at': row['created_at'],
        'applied_at': row['applied_at'],
        'eligible': candidate,
        'items': candidate,
        'candidates': candidate,
        'message_ids': ids,
        'ids': ids,
        'count': len(ids),
        'approved_by': row['approved_by'],
        'approvals': approvals,
    }
    if result:
        record['result'] = result
        record.update(result)
    return record


def fetch(db, channel, plan_id):
    catalog.get(db, channel)
    row = db.execute('SELECT * FROM retention_plans WHERE id=? AND channel=?',
                     (plan_id, channel)).fetchone()
    if row is None:
        raise missing('找不到此频道下的清理计划')
    return _serialize(row)


def fetch_any(db, plan_id):
    row = db.execute('SELECT * FROM retention_plans WHERE id=?', (plan_id,)).fetchone()
    if row is None:
        raise missing('找不到清理计划')
    return _serialize(row)


def list_plans(db, channel):
    catalog.get(db, channel)
    rows = db.execute('SELECT * FROM retention_plans WHERE channel=? ORDER BY id DESC LIMIT 20',
                      (channel,)).fetchall()
    plans = [_serialize(row) for row in rows]
    return {'plans': plans, 'items': plans, 'count': len(plans)}


def _skip(message_id, reason, text, **extra):
    entry = {'id': message_id, 'message_id': message_id, 'reason': reason,
             'code': reason, 'status': 'skipped', 'message': text}
    entry.update(extra)
    entry.setdefault('changed', True)
    codes = {'changed'}
    if reason != 'changed':
        codes.add(reason)
    if reason in ('redriven', 'active_delivery', 'active_attempt'):
        codes.add('blocked')
        codes.add('active')
    entry['reasons'] = sorted(codes)
    return entry


def _evaluate_and_delete(db, channel, ids, now):
    """按冻结编号逐条复查并删除；返回 (已删编号, 变化跳过明细)。"""
    removed = []
    skipped = []
    for message_id in ids:
        message = db.execute('''
            SELECT m.id, m.published, m.policy,
                   m.published + p.retention_seconds AS cutoff
            FROM messages m
            JOIN policies p ON p.channel = m.channel AND p.version = m.policy
            WHERE m.id=? AND m.channel=?''', (message_id, channel)).fetchone()
        if message is None:
            skipped.append(_skip(message_id, 'not_found',
                                 '名单中的消息已不存在，可能已被其他清理删除'))
            continue
        active = db.execute('''
            SELECT d.id, d.parent_id FROM deliveries d
            WHERE d.message_id=? AND d.state IN ('waiting','leased')
            ORDER BY d.id LIMIT 1''', (message_id,)).fetchone()
        if active is not None:
            redriven = active['parent_id'] is not None
            if redriven:
                skipped.append(_skip(
                    message_id, 'redriven',
                    f'名单中的死信已重投为活跃分发 #{active["id"]}，跳过删除以保住该分发',
                    delivery_id=active['id'], active_delivery_id=active['id'],
                    source_delivery_id=active['parent_id'], redriven=True,
                    cause='active_delivery'))
            else:
                skipped.append(_skip(
                    message_id, 'active_delivery',
                    f'消息仍有活跃分发 #{active["id"]}（等待或租约中），整组保留',
                    delivery_id=active['id'], active_delivery_id=active['id'],
                    source_delivery_id=active['parent_id'], redriven=False))
            continue
        open_attempt = db.execute('''
            SELECT a.id FROM attempts a JOIN deliveries d ON d.id = a.delivery_id
            WHERE d.message_id=? AND a.outcome='open' LIMIT 1''', (message_id,)).fetchone()
        if open_attempt is not None:
            skipped.append(_skip(message_id, 'active_attempt',
                                 '消息仍有未结束的租约尝试，整组保留',
                                 attempt_id=open_attempt['id']))
            continue
        if message['cutoff'] > now:
            skipped.append(_skip(message_id, 'not_due',
                                '复查时仍未达到发布时绑定策略的保留截止时刻',
                                cutoff=message['cutoff']))
            continue
        db.execute('DELETE FROM messages WHERE id=?', (message_id,))
        removed.append(message_id)
    return removed, skipped


def _execute(db, channel, plan_id, who, now):
    row = db.execute('SELECT * FROM retention_plans WHERE id=? AND channel=?',
                     (plan_id, channel)).fetchone()
    if row is None:
        raise missing('找不到此频道下的清理计划')
    approvals = unpack(row['approvals'] or '[]')
    if row['status'] == 'applied':
        approvals.append({'by': who, 'at': now, 'repeated': True})
        db.execute('UPDATE retention_plans SET approvals=? WHERE id=?',
                   (pack(approvals), plan_id))
        fresh = db.execute('SELECT * FROM retention_plans WHERE id=?', (plan_id,)).fetchone()
        return _serialize(fresh) | {'repeated': True, 'already_applied': True}

    ids = unpack(row['message_ids'])
    removed, skipped = _evaluate_and_delete(db, channel, ids, now)
    if not ids:
        outcome = 'empty'
    elif not skipped:
        outcome = 'applied'
    elif removed:
        outcome = 'partial'
    else:
        outcome = 'skipped'
    result = {
        'plan_id': plan_id,
        'status': 'applied',
        'state': 'applied',
        'outcome': outcome,
        'completed': True,
        'observed_at': row['observed_at'],
        'applied_at': now,
        'approved_by': who,
        'message_ids': ids,
        'removed': removed,
        'deleted': removed,
        'removed_ids': removed,
        'deleted_ids': removed,
        'removed_message_ids': removed,
        'deleted_message_ids': removed,
        'removed_count': len(removed),
        'deleted_count': len(removed),
        'skipped': skipped,
        'changed': skipped,
        'skipped_ids': [item['id'] for item in skipped],
        'changed_ids': [item['id'] for item in skipped],
        'skipped_count': len(skipped),
    }
    approvals.append({'by': who, 'at': now, 'repeated': False})
    db.execute('''
        UPDATE retention_plans
        SET status='applied', applied_at=?, approved_by=?, approvals=?, result=?
        WHERE id=?''', (now, who, pack(approvals), pack(result), plan_id))
    journal.append(db, channel, now, 'pruned',
                   {'plan_id': plan_id, 'message_ids': removed,
                    'removed': removed, 'skipped': skipped})
    fresh = db.execute('SELECT * FROM retention_plans WHERE id=?', (plan_id,)).fetchone()
    return _serialize(fresh) | {'repeated': False, 'already_applied': False}


def apply(db, channel, plan_id, raw, now):
    who = _approver(raw if isinstance(raw, dict) else {})
    return _execute(db, channel, _positive_id(plan_id, '清理计划编号'), who, now)


def apply_any(db, plan_id, raw, now):
    plan_id = _positive_id(plan_id, '清理计划编号')
    row = db.execute('SELECT channel FROM retention_plans WHERE id=?', (plan_id,)).fetchone()
    if row is None:
        raise missing('找不到清理计划')
    who = _approver(raw if isinstance(raw, dict) else {})
    return _execute(db, row['channel'], plan_id, who, now)


def apply_confirmed(db, channel, ids, raw, now):
    """不经持久计划编号、直接确认一批具体编号（仍落一份计划留痕并立即执行）。"""
    who = _approver(raw if isinstance(raw, dict) else {})
    ids = _dedupe(list(ids))
    cursor = db.execute('''
        INSERT INTO retention_plans
            (channel,status,observed_at,created_at,candidate,message_ids,approvals)
        VALUES (?, 'prepared', ?, ?, ?, ?, '[]')''',
        (channel, now, now, pack([{'id': item} for item in ids]), pack(ids)))
    return _execute(db, channel, cursor.lastrowid, who, now)


def _dedupe(ids):
    ordered = []
    seen = set()
    for message_id in ids:
        message_id = _positive_id(message_id, 'ids')
        if message_id not in seen:
            seen.add(message_id)
            ordered.append(message_id)
    return ordered


def prune(db, channel, raw, now):
    """POST .../retention 的兼容入口。

    - ``{}`` / ``{"limit": n}``：保留基线行为，按当前时刻重算并清理；
    - ``{"plan_id": ...}``：批准并执行冻结计划，重试返回同一结果；
    - ``{"ids": [...]}`` / ``{"eligible": [...]}``：直接确认具体名单。
    """
    raw = raw if isinstance(raw, dict) else {}

    plan_id = _plan_ref_from(raw)
    if plan_id is None and 'id' in raw and raw['id'] is not None:
        candidate_id = _positive_id(raw['id'], '编号')
        if _plan_exists(db, channel, candidate_id):
            plan_id = candidate_id
        else:
            return apply_confirmed(db, channel, [candidate_id], raw, now)
    if plan_id is not None:
        return apply(db, channel, plan_id, raw, now)

    ids = _embedded_ids(raw)
    if ids is not None:
        return apply_confirmed(db, channel, ids, raw, now)

    mode = raw.get('action', raw.get('mode'))
    if mode in ('plan', 'prepare', 'create', 'create_plan', 'preview'):
        return prepare(db, channel, raw, now)

    inputs.require(raw, (), ('limit', 'action', 'mode', 'id', 'plan', 'confirm', 'confirmed',
                             'approve', 'approved', *PLAN_REF_KEYS, *ID_LIST_KEYS,
                             *CANDIDATE_KEYS, *APPROVER_KEYS))
    keys = [item['id'] for item in eligible(db, channel, now,
                                          inputs.integer(raw.get('limit', 100), 1, 100, '清理上限'))]
    db.executemany('DELETE FROM messages WHERE id=?', [(key,) for key in keys])
    if keys:
        journal.append(db, channel, now, 'pruned', {'message_ids': keys})
    return {'removed': keys, 'deleted': keys, 'observed_at': now,
            'repeated': False, 'plan_id': None}
