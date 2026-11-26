"""补订交接：为归档组按标签和时间窗补订历史消息，并对窗口内删除做对账。

对账只依据三类现存材料下结论：
- 仍在库的原消息（直接补订一条本组投递）；
- 清理日志里删除前固化的顺序键、标签和信封摘要；
- 值班员在清理前导出、经摘要核验的证据包。

只用得到清理编号、拿不到原信封的删除不能归类，先立一个屏障投递挡住该
顺序键；未人工裁决前热键的新发布不能越过这个仍欠的前序。冷键（没有缺口
的顺序键）不受影响，不必等热键把全部历史补完。
"""
from . import catalog, inputs, journal, policy, subscriptions
from .codec import fingerprint, pack, unpack
from .envelope import labels
from .errors import conflict, invalid, missing

# 仍欠前序、需要继续补材料或人工裁决的条目状态。
ENTRY_OPEN = ('unclassified', 'evidence_pending')


def _row(db, table, key, label):
    row = db.execute(f'SELECT * FROM {table} WHERE id=?', (key,)).fetchone()
    if row is None:
        raise missing(f'找不到{label}')
    return dict(row)


def get_handoff(db, key):
    handoff = _row(db, 'backfills', key, '补订交接单')
    handoff['tags'] = unpack(handoff['tags'])
    handoff['entries'] = [dict(row) for row in db.execute(
        'SELECT * FROM backfill_entries WHERE backfill_id=? ORDER BY lane,position,id', (key,))]
    return handoff


def list_handoffs(db, channel):
    catalog.get(db, channel)
    placeholders = ','.join('?' for _ in ENTRY_OPEN)
    result = []
    for row in db.execute('SELECT * FROM backfills WHERE channel=? ORDER BY id', (channel,)):
        item = dict(row) | {'tags': unpack(row['tags'])}
        item['open'] = db.execute(
            f'SELECT COUNT(*) FROM backfill_entries WHERE backfill_id=? AND status IN ({placeholders})',
            (row['id'], *ENTRY_OPEN)).fetchone()[0]
        result.append(item)
    return {'channel': channel, 'items': result}


def _pruned_records(db, channel, window_start, window_end):
    """清理日志在删除前固化了每条消息的顺序键、标签与信封摘要。

    按清理发生时间挑出窗口内的清理事件，返回 {message_id: 明细}；
    同一消息被多次记录时保留最早一条，并兼容只记录编号的旧日志。
    """
    records = {}
    rows = db.execute("SELECT details FROM journal WHERE channel=? AND kind='pruned' AND at>=? AND at<?",
                      (channel, window_start, window_end))
    for row in rows:
        details = unpack(row['details'])
        for item in details.get('messages', ()):
            records.setdefault(item['message_id'], item)
        for message_id in details.get('message_ids', ()):
            records.setdefault(message_id, {'message_id': message_id})
    return records


def _matched(envelope, tags):
    return all(envelope.get('tags', {}).get(key) == value for key, value in tags.items())


def _lane_tail(db, subscription_id, lane):
    row = db.execute('SELECT COALESCE(MAX(position),0) FROM deliveries WHERE subscription_id=? AND lane=?',
                     (subscription_id, lane)).fetchone()
    return row[0]


def _insert_delivery(db, *, message_id, sub_id, lane, version, now, position):
    return db.execute('''INSERT INTO deliveries
        (message_id,subscription_id,lane,policy,state,ready_at,position)
        VALUES (?,?,?,?,'waiting',?,?)''',
                      (message_id, sub_id, lane, version, now, position)).lastrowid


def _insert_barrier(db, *, sub_id, lane, version, now, position):
    """无信封的屏障投递：不可领取，只在该顺序键上挡住后继。"""
    return db.execute('''INSERT INTO deliveries
        (message_id,subscription_id,lane,policy,state,ready_at,position)
        VALUES (NULL,?,?,?,'blocked',?,?)''',
                      (sub_id, lane, version, now, position)).lastrowid


def _drop_barrier(db, entry):
    """先解除条目的投递引用再删屏障，避免外键阻挡。"""
    if entry['delivery_id'] is None:
        return
    db.execute('UPDATE backfill_entries SET delivery_id=NULL WHERE id=?', (entry['id'],))
    db.execute('DELETE FROM deliveries WHERE id=?', (entry['delivery_id'],))


def create(db, channel, group, raw, now):
    inputs.require(raw, ('name', 'window_start', 'window_end'), ('tags', 'limit'))
    sub = subscriptions.get(db, channel, group)
    name = inputs.token(raw['name'], '交接单名称')
    window_start = inputs.epoch(raw['window_start'], 0, 2 ** 63 - 2, '窗口起点')
    window_end = inputs.epoch(raw['window_end'], 0, 2 ** 63 - 2, '窗口终点')
    if window_end <= window_start:
        raise invalid('窗口终点必须晚于起点')
    tags = labels(raw.get('tags', {}))
    limit = inputs.integer(raw.get('limit', 500), 1, 2000, '对账上限')

    existing = db.execute('SELECT id FROM backfills WHERE channel=? AND subscription_id=? AND name=?',
                          (channel, sub['id'], name)).fetchone()
    if existing:
        # 可重试：同身份请求重放返回同一份交接单和当前对账，不重复建投递。
        return report(db, existing['id']) | {'repeated': True}

    handoff_id = db.execute('''INSERT INTO backfills
        (channel,subscription_id,name,tags,window_start,window_end,created_at)
        VALUES (?,?,?,?,?,?,?)''',
        (channel, sub['id'], name, pack(tags), window_start, window_end, now)).lastrowid

    active = policy.read(db, channel)
    counts = dict(delivered=0, recovered_evicted=0, unclassified=0, evidence_pending=0)
    seen = set()
    # 每个顺序键收集窗口内、标签匹配的全部补订项，按原始发布时间排序，
    # 之后整块前插，保证补订前序位于交接期间的新发布之前。
    by_lane = {}

    def remember(lane, published, payload):
        by_lane.setdefault(lane, []).append((published, payload))

    # 1) 仍在库、标签匹配的历史消息：直接补订一条本组投递。
    survivors = db.execute('''SELECT id,lane,envelope,published FROM messages
        WHERE channel=? AND published>=? AND published<? ORDER BY published,id LIMIT ?''',
                           (channel, window_start, window_end, limit)).fetchall()
    for row in survivors:
        envelope = unpack(row['envelope'])
        if not _matched(envelope, tags):
            continue
        seen.add(row['id'])
        remember(row['lane'], row['published'],
                 {'kind': 'delivered', 'message_id': row['id']})

    # 2) 窗口内已删除的消息。有证据包恢复原信封的记为有据删除；
    #    只有清理编号、信封已不可得的，凭清理记录固化的顺序键立未归类屏障。
    pruned = _pruned_records(db, channel, window_start, window_end)
    for message_id in sorted(pruned):
        if message_id in seen:
            continue
        item = pruned[message_id]
        if item.get('published') is not None and not (window_start <= item['published'] < window_end):
            continue
        evidence = db.execute('SELECT * FROM evidence_packs WHERE channel=? AND message_id=?',
                              (channel, message_id)).fetchone()
        if evidence is None:
            # 清理记录固化了标签：不匹配本交接标签的删除与本组无关。
            if not _matched({'tags': item.get('tags', {})}, tags):
                continue
            lane = item.get('lane') or f'#deleted-{message_id}'
            remember(lane, item.get('published', 0),
                     {'kind': 'unclassified', 'message_id': message_id, 'lane': lane})
            continue
        envelope = unpack(evidence['envelope'])
        if not _matched(envelope, tags):
            continue
        remember(evidence['lane'], item.get('published', evidence['observed_at']),
                 {'kind': 'recovered_evicted', 'message_id': message_id, 'evidence': dict(evidence)})

    # 3) 已上交证据、但清理日志里没有对应删除记录的条目，先挂待证，等更多材料。
    for evidence in db.execute('SELECT * FROM evidence_packs WHERE channel=? AND backfill_id IS NULL',
                               (channel,)):
        envelope = unpack(evidence['envelope'])
        if not (window_start <= evidence['observed_at'] < window_end) or not _matched(envelope, tags):
            continue
        if evidence['message_id'] in seen:
            continue
        remember(evidence['lane'], evidence['observed_at'],
                 {'kind': 'evidence_pending', 'message_id': evidence['message_id'],
                  'evidence': dict(evidence)})

    # 统一前插：同键内按原始发布时间排序，负序号让整块补订位于既有投递之前。
    for lane, items in by_lane.items():
        items.sort(key=lambda pair: (pair[0], pair[1]['message_id']))
        base = _lane_tail(db, sub['id'], lane)
        for offset, (_, payload) in enumerate(items, start=1):
            slot = offset - base - 1
            kind = payload['kind']
            message_id = payload['message_id']
            if kind == 'delivered':
                delivery_id = _insert_delivery(db, message_id=message_id, sub_id=sub['id'], lane=lane,
                                               version=active['version'], now=now, position=slot)
                db.execute('''INSERT INTO backfill_entries
                    (backfill_id,message_id,delivery_id,lane,position,status)
                    VALUES (?,?,?,?,?,'delivered')''',
                           (handoff_id, message_id, delivery_id, lane, slot))
                counts['delivered'] += 1
            elif kind == 'unclassified':
                barrier = _insert_barrier(db, sub_id=sub['id'], lane=lane,
                                          version=active['version'], now=now, position=slot)
                db.execute('''INSERT INTO backfill_entries
                    (backfill_id,message_id,delivery_id,lane,position,status)
                    VALUES (?,?,?,?,?,'unclassified')''',
                           (handoff_id, message_id, barrier, lane, slot))
                counts['unclassified'] += 1
            elif kind == 'recovered_evicted':
                evidence = payload['evidence']
                db.execute('''INSERT INTO backfill_entries
                    (backfill_id,message_id,lane,position,status,observed_at,evidence_sha256)
                    VALUES (?,?,?,?,?,'recovered_evicted',?,?)''',
                           (handoff_id, message_id, lane, slot,
                            evidence['observed_at'], evidence['sha256']))
                db.execute('UPDATE evidence_packs SET backfill_id=? WHERE id=?',
                           (handoff_id, evidence['id']))
                counts['recovered_evicted'] += 1
            else:
                evidence = payload['evidence']
                db.execute('''INSERT INTO backfill_entries
                    (backfill_id,message_id,lane,position,status,observed_at,evidence_sha256)
                    VALUES (?,?,?,?,NULL,'evidence_pending',?,?)''',
                           (handoff_id, message_id, lane, evidence['observed_at'], evidence['sha256']))
                counts['evidence_pending'] += 1

    journal.append(db, channel, now, 'backfill_opened',
                   {'backfill_id': handoff_id, 'group': group, 'tags': tags,
                    'window': [window_start, window_end], **counts})
    return report(db, handoff_id) | {'repeated': False}


def verify_pack(raw):
    if not isinstance(raw, dict) or raw.get('format') != 'quaybell-trace/1':
        raise invalid('证据包须为 quaybell-trace/1 导出对象')
    digest = raw.get('sha256')
    if not isinstance(digest, str):
        raise invalid('证据包缺少摘要')
    body = {key: value for key, value in raw.items() if key != 'sha256'}
    if fingerprint(pack(body)) != digest:
        raise conflict('证据摘要不符，材料可能被改动')
    trace = raw.get('trace')
    if not isinstance(trace, dict) or not isinstance(trace.get('message'), dict):
        raise invalid('证据包缺少消息轨迹')
    return raw, digest, trace


def intake(db, channel, raw, now):
    catalog.get(db, channel)
    inputs.require(raw, ('pack',), ('backfill',))
    pack_obj, digest, trace = verify_pack(raw['pack'])
    message = trace['message']
    message_id = message.get('id')
    if type(message_id) is not int or message_id <= 0:
        raise invalid('证据包缺少有效消息编号')
    envelope = message.get('envelope')
    if not isinstance(envelope, dict):
        raise invalid('证据包缺少原信封')
    observed_at = pack_obj.get('observed_at')
    if type(observed_at) not in (int, float):
        raise invalid('证据包缺少观察时间')

    existing = db.execute('SELECT * FROM evidence_packs WHERE channel=? AND message_id=?',
                          (channel, message_id)).fetchone()
    if existing:
        if existing['sha256'] != digest:
            raise conflict('同一消息编号已存在内容不同的证据包')
        return dict(existing) | {'repeated': True}

    handoff_id = None
    if raw.get('backfill') is not None:
        handoff = get_handoff(db, inputs.integer(raw['backfill'], 1, 2 ** 63 - 2, '交接单编号'))
        if handoff['channel'] != channel:
            raise conflict('证据包与交接单不属于同一频道')
        handoff_id = handoff['id']

    record_id = db.execute('''INSERT INTO evidence_packs
        (channel,message_id,fingerprint,sha256,observed_at,producer,dedup_key,lane,envelope,
         received_at,backfill_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
        (channel, message_id, message.get('fingerprint', ''), digest, observed_at,
         message.get('producer', ''), message.get('dedup_key', ''), message.get('lane', ''),
         pack(envelope), now, handoff_id)).lastrowid

    if handoff_id is not None:
        _attach_evidence(db, handoff_id, dict(db.execute(
            'SELECT * FROM evidence_packs WHERE id=?', (record_id,)).fetchone()))
    journal.append(db, channel, now, 'evidence_received',
                   {'message_id': message_id, 'sha256': digest, 'backfill_id': handoff_id})
    return dict(db.execute('SELECT * FROM evidence_packs WHERE id=?', (record_id,)).fetchone()) | {
        'repeated': False}


def _next_position(db, handoff_id, lane):
    row = db.execute('SELECT COALESCE(MAX(position),0) FROM backfill_entries WHERE backfill_id=? AND lane=?',
                     (handoff_id, lane)).fetchone()
    return row[0] + 1


def _attach_evidence(db, handoff_id, evidence):
    """证据到位后把同编号的未归类/待证条目升级为有据删除并撤掉屏障。"""
    envelope = unpack(evidence['envelope'])
    handoff = get_handoff(db, handoff_id)
    if not _matched(envelope, handoff['tags']):
        return
    entry = db.execute('SELECT * FROM backfill_entries WHERE backfill_id=? AND message_id=?',
                       (handoff_id, evidence['message_id'])).fetchone()
    if entry is None or entry['status'] not in ENTRY_OPEN:
        return
    _drop_barrier(db, entry)
    slot = entry['position'] if entry['position'] is not None else _next_position(db, handoff_id, evidence['lane'])
    db.execute('''UPDATE backfill_entries SET status='recovered_evicted', lane=?, position=?,
                  observed_at=?, evidence_sha256=?, resolution=NULL,
                  resolved_at=NULL, note=NULL WHERE id=?''',
               (evidence['lane'], slot, evidence['observed_at'], evidence['sha256'], entry['id']))


def resolve(db, key, raw, now):
    """人工裁决只有清理编号、拿不到原信封的未归类删除。"""
    inputs.require(raw, ('entry', 'resolution'), ('note',))
    entry_id = inputs.integer(raw['entry'], 1, 2 ** 63 - 2, '条目编号')
    if raw['resolution'] not in ('accepted', 'skipped'):
        raise invalid('裁决取值须为 accepted 或 skipped')
    note = raw.get('note')
    if note is not None and (not isinstance(note, str) or not note.strip() or len(note) > 240):
        raise invalid('裁决备注须为 1～240 个字符')
    entry = _row(db, 'backfill_entries', entry_id, '补订条目')
    if entry['backfill_id'] != key:
        raise missing('该条目不属于此交接单')
    if entry['status'] != 'unclassified':
        raise conflict('只有未归类条目需要人工裁决')
    handoff = get_handoff(db, key)
    _drop_barrier(db, entry)
    db.execute('''UPDATE backfill_entries SET status=?, resolution=?, resolved_at=?, note=?
                  WHERE id=?''',
               (raw['resolution'], raw['resolution'], now, note, entry_id))
    journal.append(db, handoff['channel'], now, 'backfill_resolved',
                   {'backfill_id': key, 'entry': entry_id, 'resolution': raw['resolution']})
    return report(db, key)


def report(db, key):
    handoff = get_handoff(db, key)
    counts, open_lanes = {}, set()
    for entry in handoff['entries']:
        counts[entry['status']] = counts.get(entry['status'], 0) + 1
        if entry['status'] in ENTRY_OPEN:
            open_lanes.add(entry['lane'])
    handoff['summary'] = counts
    handoff['open_lanes'] = sorted(open_lanes)
    handoff['sealed'] = not open_lanes
    handoff['observed_at'] = max(
        (e['observed_at'] for e in handoff['entries'] if e['observed_at']), default=None)
    return handoff
