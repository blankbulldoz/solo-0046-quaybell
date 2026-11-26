from .delivery import DELIVERY
from .errors import missing


def ready(db, group_id, now, limit):
    return [dict(row) for row in db.execute(DELIVERY + '''
        WHERE d.subscription_id=? AND d.state='waiting' AND d.ready_at<=?
          AND NOT EXISTS (SELECT 1 FROM deliveries p
             WHERE p.subscription_id=d.subscription_id AND p.lane=d.lane AND p.id<d.id
               AND p.state IN ('waiting','leased'))
        ORDER BY d.id LIMIT ?''', (group_id, now, limit))]


def unsettled(db, group_id, lane):
    '''本组同一顺序键上仍在挡路的投递，按投递号排列。其他顺序键不进入。'''
    return [dict(row) for row in db.execute('''
        SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,d.attempt_count,d.parent_id,
               a.expires,a.worker
        FROM deliveries d LEFT JOIN attempts a
          ON a.delivery_id=d.id AND a.outcome='open'
        WHERE d.subscription_id=? AND d.lane=? AND d.state IN ('waiting','leased')
        ORDER BY d.id''', (group_id, lane))]


def cause(row, head_id, now, paused):
    if row['state'] == 'leased':
        return 'expired' if row['expires'] <= now else 'leased'
    if row['id'] != head_id:
        return 'blocked'
    if paused:
        return 'paused'
    return 'delayed' if row['ready_at'] > now else 'ready'


def describe(row, reason):
    return {'delivery_id': row['id'], 'message_id': row['message_id'], 'state': row['state'],
            'cause': reason, 'ready_at': row['ready_at'], 'expires': row['expires'],
            'worker': row['worker']}


def snapshot(db, group, lane, target_id, now):
    '''重投提交那一刻冻结的判断：身前还有多少先行债务、当时真正挡住整条键的是谁。'''
    rows = unsettled(db, group['id'], lane)
    head_id = rows[0]['id'] if rows else None
    preceding = [row for row in rows if row['id'] < target_id]
    blocking = preceding[0] if preceding else None
    return {
        'recorded': True,
        'at': now,
        'group': group['name'],
        'lane': lane,
        'prior_debts': len(preceding),
        'preceding_ids': [row['id'] for row in preceding],
        'position': len(preceding) + 1,
        'paused': bool(group['paused']),
        'blocking': describe(blocking, cause(blocking, head_id, now, group['paused'])) if blocking else None,
    }


def progress(db, group, lane, target_id, now, preceding_ids=()):
    '''读取时该后继在本组本键上的当前位置；与提交时冻结的判断分开计算。'''
    rows = unsettled(db, group['id'], lane)
    head_id = rows[0]['id'] if rows else None
    target = next((row for row in rows if row['id'] == target_id), None)
    ahead = [row for row in rows if row['id'] < target_id]
    record = db.execute('SELECT id,state,ready_at,terminal_at FROM deliveries WHERE id=?',
                        (target_id,)).fetchone()
    if record is None:
        raise missing('找不到投递记录')
    record = dict(record)
    current = {row['id'] for row in rows}
    return {
        'observed_at': now,
        'state': record['state'],
        'in_queue': target is not None,
        'position': len(ahead) + 1 if target else None,
        'ahead': len(ahead),
        'ahead_ids': [row['id'] for row in ahead],
        'head_id': head_id,
        'head': describe(rows[0], cause(rows[0], head_id, now, group['paused'])) if rows else None,
        'blocking': describe(ahead[0], cause(ahead[0], head_id, now, group['paused'])) if ahead else None,
        'cause': cause(target, head_id, now, group['paused']) if target else None,
        'ready_at': target['ready_at'] if target else record['ready_at'],
        'expires': target['expires'] if target else None,
        'worker': target['worker'] if target else None,
        'departed_preceding': [key for key in preceding_ids if key not in current],
        'reached': target is not None and not ahead,
        'claimable': (target is not None and target['state'] == 'waiting' and not ahead
                      and target['ready_at'] <= now and not group['paused']),
    }


def explain(db, group, now):
    rows = db.execute('''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,a.expires
                         FROM deliveries d LEFT JOIN attempts a
                           ON a.delivery_id=d.id AND a.outcome='open'
                         WHERE d.subscription_id=? AND d.state IN ('waiting','leased') ORDER BY d.id''',
                      (group['id'],))
    heads, positions = {}, {}
    for row in rows:
        d = dict(row)
        head = heads.setdefault(d['lane'], d['id'])
        position = positions.get(d['lane'], 0) + 1
        positions[d['lane']] = position
        yield d | {'cause': cause(d, head, now, group['paused']), 'head_id': head,
                   'position': position, 'ahead': position - 1}
