from . import catalog, codec, deadletters, delivery, inputs, journal
from .errors import conflict, invalid

KINDS = ('dismissed', 'handed_off')
RECENT_LIMIT = 50


def _standing(item, successor):
    original = {
        'delivery_id': item['id'],
        'can_redrive': False,
        'can_lease': False,
        'can_settle': False,
        'traceable': True,
        'note': '原投递保持死信终态并附结案记录，只能追溯；重投和处置均已关闭',
    }
    if successor is None:
        handoff = None
    else:
        handoff = {
            'delivery_id': successor['id'],
            'state': successor['state'],
            'can_lease': successor['state'] == 'waiting',
            'can_settle': successor['state'] == 'leased',
            'traceable': True,
            'note': '后继是独立投递，沿用本组顺序键并绑定重投时策略，按自身状态继续生命周期',
        }
    return {'original': original, 'successor': handoff}


def _receipt(db, row, item, successor):
    basis = {
        'delivery_id': item['id'],
        'message_id': item['message_id'],
        'channel': item['channel'],
        'group': item['group_name'],
        'lane': item['lane'],
        'policy': item['policy'],
        'state': item['state'],
        'terminal_at': item['terminal_at'],
        'kind': row['kind'],
        'worker': row['worker'],
        'reason': row['reason'],
        'closed_at': row['at'],
        'successor_id': row['successor_id'],
    }
    facts = dict(format='quaybell-closure/1', basis=basis,
                 standing=_standing(item, successor))
    return facts | {'sha256': codec.fingerprint(codec.pack(facts))}


def _successor(db, key):
    return db.execute('SELECT * FROM deliveries WHERE parent_id=?', (key,)).fetchone()


def for_message(db, message_id):
    rows = db.execute('SELECT * FROM closures WHERE message_id=? ORDER BY id', (message_id,))
    result = []
    for row in rows:
        item = delivery.get(db, row['delivery_id'])
        successor = db.execute('SELECT * FROM deliveries WHERE id=?',
                               (row['successor_id'],)).fetchone() if row['successor_id'] else None
        result.append(dict(row) | {'receipt': codec.unpack(row['receipt']),
                                   'successor_state': successor['state'] if successor else None})
    return result


def for_delivery(db, key):
    row = db.execute('SELECT * FROM closures WHERE delivery_id=?', (key,)).fetchone()
    if row is None:
        return None
    return dict(row) | {'receipt': codec.unpack(row['receipt'])}


def list_recent(db, channel, limit=RECENT_LIMIT):
    catalog.get(db, channel)
    rows = db.execute('SELECT * FROM closures WHERE channel=? ORDER BY id DESC LIMIT ?',
                      (channel, limit))
    result = []
    for row in rows:
        successor = db.execute('SELECT * FROM deliveries WHERE id=?',
                               (row['successor_id'],)).fetchone() if row['successor_id'] else None
        result.append(dict(row) | {'receipt': codec.unpack(row['receipt']),
                                   'successor_state': successor['state'] if successor else None})
    return result


def close(db, key, raw, now):
    inputs.require(raw, ('kind', 'reason'), ('worker',))
    kind = raw['kind']
    if kind not in KINDS:
        raise invalid('结案类型须为 dismissed（无需重投）或 handed_off（已交后继）')
    reason = raw['reason']
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 240:
        raise invalid('结案依据须为 1～240 个字符')
    worker = inputs.name(raw.get('worker', 'duty'), '值守员')
    item = deadletters.get(db, key)
    prior = for_delivery(db, key)
    if prior is not None:
        if prior['kind'] == kind and prior['reason'] == reason and prior['worker'] == worker:
            return prior['receipt'] | {'repeated': True}
        raise conflict('该死信已结案，不能改换结案类型或依据；原结案凭证仍可追溯')
    successor = _successor(db, key)
    if kind == 'dismissed' and successor is not None:
        raise conflict('已追加后继投递，须按 handed_off 结案，不能判为无需重投')
    if kind == 'handed_off' and successor is None:
        raise conflict('尚无后继投递，不能按已交后继结案；请先重投或改判无需重投')
    record = db.execute('''INSERT INTO closures(channel,delivery_id,message_id,successor_id,kind,worker,reason,at,receipt)
                           VALUES (?,?,?,?,?,?,?,?,?)''',
                        (item['channel'], key, item['message_id'],
                         successor['id'] if successor else None, kind, worker, reason, now, ''))
    closure_id = record.lastrowid
    receipt = _receipt(db, db.execute('SELECT * FROM closures WHERE id=?', (closure_id,)).fetchone(),
                       item, successor)
    db.execute('UPDATE closures SET receipt=? WHERE id=?', (codec.pack(receipt), closure_id))
    journal.append(db, item['channel'], now, 'closed',
                   {'closure_id': closure_id, 'delivery_id': key, 'kind': kind,
                    'reason': reason, 'worker': worker, 'successor_id': receipt['basis']['successor_id']},
                   item['message_id'])
    return receipt | {'repeated': False}
