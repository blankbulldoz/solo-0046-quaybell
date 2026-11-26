"""临时重演会话：按原失败事实选入窗口，生成与生产组隔离、可单独处置的回放队列。"""
import secrets
from collections import Counter

from . import catalog, codec, inputs, policy, subscriptions
from .errors import conflict, missing

BASIS = 'attempt_failed'
BASIS_TEXT = ('选入事实：所选消费组中，原尝试的失败时刻落在窗口内的 nacked（取 finished）'
              '与 expired（取 expires）尝试；同一原投递只取窗口内最后一个失败尝试，'
              '按失败时刻与原投递 ID 升序。窗口与消息发布时间无关。')
MAX_ITEMS = 500
MAX_WINDOW = 31 * 86400
STATES = ('waiting', 'leased', 'acked', 'dead')
OUTCOMES = ('open', 'acked', 'nacked', 'expired')
FAILURE = "CASE WHEN a.outcome='expired' THEN a.expires ELSE a.finished END"


def _session(db, key):
    row = db.execute('SELECT * FROM replays WHERE id=?', (key,)).fetchone()
    if row is None:
        raise missing('找不到重演会话')
    return dict(row)


def _item(db, key):
    row = db.execute('''SELECT i.*,r.state AS replay_state,r.group_name,r.policy,
                               r.lease_seconds,r.retry_seconds,r.max_attempts
                        FROM replay_items i JOIN replays r ON r.id=i.replay_id
                        WHERE i.id=?''', (key,)).fetchone()
    if row is None:
        raise missing('找不到回放项')
    return dict(row)


def _event(db, replay_id, now, kind, item_id, details):
    db.execute('INSERT INTO replay_events(replay_id,at,kind,item_id,details) VALUES (?,?,?,?,?)',
               (replay_id, now, kind, item_id, codec.pack(details)))


def _window(session):
    return {'from': session['window_from'], 'to': session['window_to'],
            'basis': BASIS, 'basis_text': BASIS_TEXT}


def _policy(session):
    return {'version': session['policy'], 'lease_seconds': session['lease_seconds'],
            'retry_seconds': session['retry_seconds'], 'max_attempts': session['max_attempts']}


def _counts(items):
    counter = Counter(item['state'] if isinstance(item, dict) else item for item in items)
    return {state: counter.get(state, 0) for state in STATES}


def _backoff(session, number):
    return min(3600, session['retry_seconds'] * 2 ** (number - 1))


def production_snapshot(db, channel, subscription_id, group=None):
    deliveries = dict.fromkeys(STATES, 0)
    deliveries.update({row['state']: row['count'] for row in db.execute(
        'SELECT state,COUNT(*) AS count FROM deliveries WHERE subscription_id=? GROUP BY state',
        (subscription_id,))})
    attempts = dict.fromkeys(OUTCOMES, 0)
    attempts.update({row['outcome']: row['count'] for row in db.execute(
        '''SELECT a.outcome,COUNT(*) AS count FROM attempts a
           JOIN deliveries d ON d.id=a.delivery_id WHERE d.subscription_id=? GROUP BY a.outcome''',
        (subscription_id,))})
    messages = db.execute('SELECT COUNT(*) FROM messages WHERE channel=?', (channel,)).fetchone()[0]
    return {'group': group, 'messages': messages, 'deliveries': deliveries, 'attempts': attempts}


def _delta(before, after):
    return {'messages': after['messages'] - before['messages'],
            'deliveries': {state: after['deliveries'][state] - before['deliveries'][state]
                           for state in STATES},
            'attempts': {outcome: after['attempts'][outcome] - before['attempts'][outcome]
                         for outcome in OUTCOMES}}


def _select(db, subscription_id, start, end, limit):
    rows = db.execute(f'''
        SELECT d.id AS delivery_id,d.message_id,d.lane AS delivery_lane,d.policy AS delivery_policy,
               d.state AS delivery_state,d.ready_at AS delivery_ready,
               m.channel,m.producer,m.dedup_key,m.lane AS message_lane,m.envelope,m.published,
               a.id AS attempt_id,a.number AS attempt_number,a.worker,a.outcome AS attempt_outcome,
               a.started,a.expires,a.finished,a.reason,{FAILURE} AS failed_at
          FROM attempts a
          JOIN deliveries d ON d.id=a.delivery_id
          JOIN messages m ON m.id=d.message_id
         WHERE d.subscription_id=? AND a.outcome IN ('nacked','expired') AND {FAILURE}>=?
           AND {FAILURE}<=?
           AND a.number=(SELECT MAX(a2.number) FROM attempts a2
                          WHERE a2.delivery_id=d.id AND a2.outcome IN ('nacked','expired')
                            AND (CASE WHEN a2.outcome='expired' THEN a2.expires
                                      ELSE a2.finished END)>=?
                            AND (CASE WHEN a2.outcome='expired' THEN a2.expires
                                      ELSE a2.finished END)<=?)
         ORDER BY {FAILURE},d.id LIMIT ?''',
                     (subscription_id, start, end, start, end, limit))
    return [dict(row) for row in rows]


def _matched(db, subscription_id, start, end):
    return db.execute(f'''SELECT COUNT(DISTINCT d.id) FROM attempts a
                          JOIN deliveries d ON d.id=a.delivery_id
                          WHERE d.subscription_id=? AND a.outcome IN ('nacked','expired')
                            AND {FAILURE}>=? AND {FAILURE}<=?''',
                      (subscription_id, start, end)).fetchone()[0]


def _item_facts(row):
    return {'message': {'id': row['message_id'], 'channel': row['channel'],
                        'producer': row['producer'], 'dedup_key': row['dedup_key'],
                        'lane': row['message_lane'], 'published': row['published']},
            'delivery': {'id': row['delivery_id'], 'state': row['delivery_state'],
                         'policy': row['delivery_policy'], 'ready_at': row['delivery_ready']},
            'attempt': {'id': row['attempt_id'], 'number': row['attempt_number'],
                        'worker': row['worker'], 'outcome': row['attempt_outcome'],
                        'started': row['started'], 'expires': row['expires'],
                        'finished': row['finished'], 'reason': row['reason'],
                        'failed_at': row['failed_at']}}


def create(db, channel, group, raw, now):
    inputs.require(raw, ('window_from', 'window_to'), ('limit',))
    catalog.get(db, channel)
    sub = subscriptions.get(db, channel, group)
    start = inputs.integer(raw['window_from'], 0, 2 ** 63 - 2, '窗口起点')
    end = inputs.integer(raw['window_to'], 0, 2 ** 63 - 2, '窗口终点')
    if end < start:
        raise inputs.invalid('窗口终点须不早于窗口起点')
    if end - start > MAX_WINDOW:
        raise inputs.invalid('窗口最多 31 天')
    limit = inputs.integer(raw.get('limit', 200), 1, MAX_ITEMS, '选入上限')
    if db.execute("SELECT 1 FROM replays WHERE channel=? AND subscription_id=? "
                  "AND window_from=? AND window_to=? AND state='open'",
                  (channel, sub['id'], start, end)).fetchone():
        raise conflict('同一消费组的该窗口已有打开的重演会话')
    rule = policy.read(db, channel)
    matched = _matched(db, sub['id'], start, end)
    chosen = _select(db, sub['id'], start, end, limit + 1)
    truncated = len(chosen) > limit
    chosen = chosen[:limit]
    baseline = production_snapshot(db, channel, sub['id'], sub['name'])
    key = db.execute('''INSERT INTO replays(channel,subscription_id,group_name,window_from,window_to,
                         basis,policy,lease_seconds,retry_seconds,max_attempts,state,created,baseline)
                        VALUES (?,?,?,?,?,?,?,?,?,?,'open',?,?)''',
                     (channel, sub['id'], sub['name'], start, end, BASIS, rule['version'],
                      rule['lease_seconds'], rule['retry_seconds'], rule['max_attempts'],
                      now, codec.pack(baseline))).lastrowid
    for row in chosen:
        db.execute('''INSERT INTO replay_items(replay_id,source_delivery_id,source_message_id,
                        source_attempt_id,source_attempt_number,source_attempt_outcome,lane,envelope,
                        source_facts,state,ready_at)
                      VALUES (?,?,?,?,?,?,?,?,?,'waiting',?)''',
                   (key, row['delivery_id'], row['message_id'], row['attempt_id'],
                    row['attempt_number'], row['attempt_outcome'], row['delivery_lane'],
                    row['envelope'], codec.pack(_item_facts(row)), now))
    _event(db, key, now, 'replay_created', None,
           {'window': {'from': start, 'to': end, 'basis': BASIS}, 'matched': matched,
            'selected': len(chosen), 'truncated': truncated, 'limit': limit})
    return detail(db, key, now) | {'matched': matched, 'selected': len(chosen),
                                   'truncated': truncated}


def _live(db, rows):
    if not rows:
        return {}
    marks = ','.join('?' * len(rows))
    deliveries = {row['id']: row['state'] for row in db.execute(
        f'''SELECT d.id,d.state FROM deliveries d JOIN messages m ON m.id=d.message_id
            WHERE d.id IN ({marks})''', [row['source_delivery_id'] for row in rows])}
    attempts = {row['id']: row['outcome'] for row in db.execute(
        f'SELECT id,outcome FROM attempts WHERE id IN ({marks})',
        [row['source_attempt_id'] for row in rows])}
    return {row['source_delivery_id']: {
        'message_present': row['source_delivery_id'] in deliveries,
        'delivery_state': deliveries.get(row['source_delivery_id']),
        'attempt_outcome': attempts.get(row['source_attempt_id'])}
        for row in rows}


def _attempts(db, item_id):
    result = []
    for row in db.execute('''SELECT id,item_id,number,worker,started,expires,outcome,
                                    finished,reason,receipt
                             FROM replay_attempts WHERE item_id=? ORDER BY number''', (item_id,)):
        record = dict(row)
        record['receipt'] = codec.unpack(record['receipt']) if record['receipt'] else None
        result.append(record)
    return result


def _item_view(db, row, live):
    facts = codec.unpack(row['source_facts'])
    source = {'message_id': row['source_message_id'], 'delivery_id': row['source_delivery_id'],
              'attempt_id': row['source_attempt_id'],
              'attempt_number': row['source_attempt_number'],
              'attempt_outcome': row['source_attempt_outcome'],
              'message': facts['message'], 'delivery': facts['delivery'],
              'attempt': facts['attempt']}
    return {'id': row['id'], 'state': row['state'], 'lane': row['lane'],
            'ready_at': row['ready_at'], 'attempt_count': row['attempt_count'],
            'terminal_at': row['terminal_at'], 'envelope': codec.unpack(row['envelope']),
            'source': source, 'live': live.get(row['source_delivery_id'], {
                'message_present': False, 'delivery_state': None, 'attempt_outcome': None}),
            'attempts': _attempts(db, row['id'])}


def _items(db, replay_id):
    rows = [dict(row) for row in db.execute(
        'SELECT * FROM replay_items WHERE replay_id=? ORDER BY id', (replay_id,))]
    live = _live(db, rows)
    return [_item_view(db, row, live) for row in rows]


def _events(db, replay_id):
    return [dict(row) | {'details': codec.unpack(row['details'])}
            for row in db.execute('SELECT id,at,kind,item_id,details FROM replay_events '
                                  'WHERE replay_id=? ORDER BY id', (replay_id,))]


def detail(db, replay_id, now):
    session = _session(db, replay_id)
    items = _items(db, replay_id)
    current = production_snapshot(db, session['channel'], session['subscription_id'],
                                  session['group_name'])
    result = {'id': session['id'], 'channel': session['channel'], 'group': session['group_name'],
              'state': session['state'], 'created': session['created'],
              'closed': session['closed'], 'window': _window(session),
              'policy': _policy(session), 'counts': _counts(items), 'items': items,
              'observed_at': now,
              'production': {'baseline': codec.unpack(session['baseline']), 'current': current,
                             'delta': _delta(codec.unpack(session['baseline']), current)}}
    if session['state'] == 'closed':
        result['summary'] = codec.unpack(session['summary'])
    return result


def list_for_channel(db, channel, limit=20):
    catalog.get(db, channel)
    result = []
    for row in db.execute('SELECT * FROM replays WHERE channel=? ORDER BY id DESC LIMIT ?',
                          (channel, limit)):
        session = dict(row)
        states = [item['state'] for item in db.execute(
            'SELECT state FROM replay_items WHERE replay_id=?', (session['id'],))]
        result.append({'id': session['id'], 'channel': session['channel'],
                       'group': session['group_name'], 'state': session['state'],
                       'created': session['created'], 'closed': session['closed'],
                       'window': _window(session), 'counts': _counts(states)})
    return result


def claim(db, replay_id, raw, now):
    inputs.require(raw, ('worker',), ('limit',))
    worker = inputs.name(raw['worker'], '工作进程')
    limit = inputs.integer(raw.get('limit', 1), 1, 30, '领取上限')
    session = _session(db, replay_id)
    if session['state'] != 'open':
        raise conflict('重演会话已关闭，不能再领取')
    _reap(db, session, now, 100)
    rows = [dict(row) for row in db.execute('''
        SELECT * FROM replay_items i
         WHERE i.replay_id=? AND i.state='waiting' AND i.ready_at<=?
           AND NOT EXISTS (SELECT 1 FROM replay_attempts a
                            WHERE a.item_id=i.id AND a.outcome='open')
         ORDER BY i.id LIMIT ?''', (replay_id, now, limit))]
    live = _live(db, rows)
    items = []
    for row in rows:
        token = secrets.token_urlsafe(24)
        number = row['attempt_count'] + 1
        expires = now + session['lease_seconds']
        db.execute('''INSERT INTO replay_attempts(item_id,number,token_digest,worker,started,expires,
                         outcome) VALUES (?,?,?,?,?,?,'open')''',
                   (row['id'], number, codec.fingerprint(token), worker, now, expires))
        db.execute("UPDATE replay_items SET state='leased',attempt_count=? WHERE id=?",
                   (number, row['id']))
        _event(db, replay_id, now, 'replay_leased', row['id'],
               {'attempt': number, 'worker': worker, 'expires': expires})
        view = _item_view(db, row | {'state': 'leased', 'attempt_count': number}, live)
        items.append({'item_id': row['id'], 'replay_id': replay_id, 'lane': row['lane'],
                      'source': view['source'], 'envelope': view['envelope'],
                      'lease': {'number': number, 'token': token, 'expires': expires}})
    return {'replay_id': replay_id, 'state': session['state'], 'observed_at': now,
            'items': items}


def _by_token(db, item_id, token):
    row = db.execute('SELECT * FROM replay_attempts WHERE item_id=? AND token_digest=?',
                     (item_id, codec.fingerprint(token))).fetchone()
    if row is None:
        raise conflict('该凭据不属于此回放项')
    return dict(row)


def settle(db, item_id, raw, now, disposition):
    inputs.require(raw, ('token',) if disposition == 'acked' else ('token', 'reason'))
    token = inputs.token(raw['token'], '租约凭据')
    reason = raw.get('reason')
    if disposition == 'nacked' and (not isinstance(reason, str) or not reason.strip()
                                    or len(reason) > 240):
        raise inputs.invalid('失败原因须为 1～240 个字符')
    item = _item(db, item_id)
    if item['replay_state'] != 'open':
        raise conflict('重演会话已关闭，不能再处置')
    attempt = _by_token(db, item_id, token)
    if attempt['outcome'] == disposition and attempt['reason'] == reason:
        return codec.unpack(attempt['receipt']) | {'repeated': True}
    if attempt['outcome'] != 'open' or item['state'] != 'leased' or attempt['expires'] <= now:
        raise conflict('租约已处置或超时')
    return _apply(db, item, attempt, disposition, reason, now) | {'repeated': False}


def _apply(db, item, attempt, disposition, reason, now):
    if disposition == 'acked':
        state, when = 'acked', now
    elif attempt['number'] >= item['max_attempts']:
        state, when = 'dead', now
    else:
        origin = attempt['expires'] if disposition == 'expired' else now
        state, when = 'waiting', origin + _backoff(item, attempt['number'])
    result = {'item_id': item['id'], 'replay_id': item['replay_id'], 'attempt': attempt['number'],
              'outcome': disposition, 'state': state, 'effective_at': when, 'reason': reason}
    if state == 'waiting':
        db.execute('UPDATE replay_items SET state=?,ready_at=?,terminal_at=NULL WHERE id=?',
                   (state, when, item['id']))
    else:
        db.execute('UPDATE replay_items SET state=?,terminal_at=? WHERE id=?',
                   (state, when, item['id']))
    db.execute('''UPDATE replay_attempts SET outcome=?,reason=?,finished=?,receipt=? WHERE id=?''',
               (disposition, reason, now, codec.pack(result), attempt['id']))
    _event(db, item['replay_id'], now, 'replay_' + disposition, item['id'], result)
    return result


def _reap(db, session, now, limit):
    rows = db.execute('''SELECT a.*,i.state AS item_state FROM replay_attempts a
                         JOIN replay_items i ON i.id=a.item_id
                         WHERE i.replay_id=? AND a.outcome='open' AND a.expires<=?
                         ORDER BY a.expires,a.id LIMIT ?''',
                      (session['id'], now, limit))
    for row in rows:
        attempt = dict(row)
        if attempt['item_state'] == 'leased':
            _apply(db, _item(db, attempt['item_id']), attempt, 'expired', '租约超时', now)


def close(db, replay_id, raw, now):
    inputs.require(raw, ())
    session = _session(db, replay_id)
    if session['state'] == 'closed':
        return codec.unpack(session['summary']) | {'repeated': True}
    if db.execute('''SELECT 1 FROM replay_attempts a JOIN replay_items i ON i.id=a.item_id
                     WHERE i.replay_id=? AND a.outcome='open' LIMIT 1''',
                  (replay_id,)).fetchone():
        raise conflict('会话仍有未应答的租约，请先确认、拒绝或等待超时')
    states = [item['state'] for item in db.execute(
        'SELECT state FROM replay_items WHERE replay_id=?', (replay_id,))]
    at_close = production_snapshot(db, session['channel'], session['subscription_id'],
                                   session['group_name'])
    summary = {'id': replay_id, 'channel': session['channel'], 'group': session['group_name'],
               'state': 'closed', 'closed': now, 'counts': _counts(states),
               'production': {'baseline': codec.unpack(session['baseline']),
                              'at_close': at_close,
                              'delta': _delta(codec.unpack(session['baseline']), at_close)}}
    db.execute("UPDATE replays SET state='closed',closed=?,summary=? WHERE id=?",
               (now, codec.pack(summary), replay_id))
    _event(db, replay_id, now, 'replay_closed', None, {'counts': summary['counts']})
    return summary | {'repeated': False}


def export(db, replay_id, now):
    session = _session(db, replay_id)
    items = _items(db, replay_id)
    baseline = codec.unpack(session['baseline'])
    current = production_snapshot(db, session['channel'], session['subscription_id'],
                                  session['group_name'])
    at_close = (codec.unpack(session['summary'])['production']['at_close']
                if session['state'] == 'closed' else None)
    data = {'format': 'quaybell-replay/1', 'observed_at': now,
            'replay': {'id': session['id'], 'channel': session['channel'],
                       'group': session['group_name'], 'state': session['state'],
                       'created': session['created'], 'closed': session['closed'],
                       'window': _window(session), 'policy': _policy(session),
                       'counts': _counts(items), 'items': items,
                       'events': _events(db, replay_id)},
            'production': {'baseline': baseline, 'at_close': at_close, 'current': current,
                           'delta': _delta(baseline, current)}}
    data['sha256'] = codec.fingerprint(codec.pack(data))
    return (codec.pack(data) + '\n').encode('ascii')
