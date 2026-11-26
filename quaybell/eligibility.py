from .delivery import DELIVERY

OPEN_ATTEMPT = '''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,d.attempt_count,
                         d.policy,d.parent_id,d.terminal_at,a.id AS attempt_id,a.number AS attempt_number,
                         a.worker,a.started,a.expires
                  FROM deliveries d LEFT JOIN attempts a
                    ON a.delivery_id=d.id AND a.outcome='open'
                  WHERE d.subscription_id=? AND d.state IN ('waiting','leased')'''


def classify(row, head_id, paused, now):
    """与看板一致的单一原因分类：租约状态优先，其次顺序阻挡、暂停、延迟与可领。"""
    if row['state'] == 'leased':
        return 'expired' if row['expires'] <= now else 'leased'
    if row['id'] != head_id:
        return 'blocked'
    if paused:
        return 'paused'
    return 'delayed' if row['ready_at'] > now else 'ready'


def actionable(row, head_id, paused, now):
    """现场可采取的动作：有效租约可处置；未暂停的就绪队头可领取；其余无动作。"""
    if row['state'] == 'leased':
        return 'settle' if row['expires'] > now else 'none'
    if (row['state'] == 'waiting' and row['id'] == head_id and not paused
            and row['ready_at'] <= now):
        return 'claim'
    return 'none'


def ready(db, group_id, now, limit):
    return [dict(row) for row in db.execute(DELIVERY + '''
        WHERE d.subscription_id=? AND d.state='waiting' AND d.ready_at<=?
          AND NOT EXISTS (SELECT 1 FROM deliveries p
             WHERE p.subscription_id=d.subscription_id AND p.lane=d.lane AND p.id<d.id
               AND p.state IN ('waiting','leased'))
        ORDER BY d.id LIMIT ?''', (group_id, now, limit))]


def explain(db, group, now):
    rows = db.execute('''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,a.expires
                         FROM deliveries d LEFT JOIN attempts a
                           ON a.delivery_id=d.id AND a.outcome='open'
                         WHERE d.subscription_id=? AND d.state IN ('waiting','leased') ORDER BY d.id''',
                      (group['id'],))
    heads = {}
    for row in rows:
        d = dict(row)
        head = heads.setdefault(d['lane'], d['id'])
        yield d | {'cause': classify(d, head, group['paused'], now), 'head_id': head}


def lane_queue(db, group_id, lane):
    """同一消费组、同一顺序键上的全部未处置投递，不受看板样本上限约束。"""
    rows = db.execute(OPEN_ATTEMPT + ' AND d.lane=? ORDER BY d.id', (group_id, lane))
    return [dict(row) for row in rows]


def open_attempt_view(row):
    if row['attempt_id'] is None:
        return None
    return {'id': row['attempt_id'], 'delivery_id': row['id'], 'number': row['attempt_number'],
            'worker': row['worker'], 'started': row['started'], 'expires': row['expires'],
            'outcome': 'open'}


def annotate(row, head_id, paused, now):
    view = {key: row[key] for key in ('id', 'message_id', 'lane', 'state', 'ready_at',
                                      'attempt_count', 'policy', 'parent_id', 'terminal_at', 'expires')}
    return view | {'head_id': head_id, 'cause': classify(row, head_id, paused, now),
                   'actionable': actionable(row, head_id, paused, now),
                   'open_attempt': open_attempt_view(row)}
