from collections import Counter, defaultdict

from . import codec, delivery, inputs, outcomes
from .errors import conflict, invalid, missing

MAX_ENTRIES = 100
STATUSES = ('applied', 'replayed', 'stale', 'conflict')


def _entries_input(raw):
    inputs.require(raw, ('batch', 'worker', 'entries'), ())
    batch_key = inputs.name(raw['batch'], '批次标识')
    worker = inputs.name(raw['worker'], '工作进程')
    entries_raw = raw['entries']
    if not isinstance(entries_raw, list) or not 1 <= len(entries_raw) <= MAX_ENTRIES:
        raise invalid(f'回执条目须为 1～{MAX_ENTRIES} 项的数组')
    entries = []
    for index, item in enumerate(entries_raw):
        if not isinstance(item, dict):
            raise invalid('回执条目须为对象')
        allowed = ('delivery_id', 'disposition', 'token', 'reason')
        if not {'delivery_id', 'disposition', 'token'} <= item.keys() or item.keys() - set(allowed):
            raise invalid('回执条目字段缺失或包含未识别字段')
        delivery_id = inputs.integer(item['delivery_id'], 1, 2**63 - 2, '投递编号')
        disposition = item['disposition']
        if disposition not in ('ack', 'nack'):
            raise invalid('回执结果须为 ack 或 nack')
        token = inputs.token(item['token'], '租约凭据')
        reason = item.get('reason')
        if disposition == 'nack':
            if not isinstance(reason, str) or not reason.strip() or len(reason) > 240:
                raise invalid('失败原因须为 1～240 个字符')
        elif reason is not None:
            raise invalid('ack 回执不能携带失败原因')
        entries.append(dict(position=index, delivery_id=delivery_id,
                            disposition='acked' if disposition == 'ack' else 'nacked',
                            token=token, reason=reason,
                            token_digest=codec.fingerprint(token)))
    return batch_key, worker, entries


def _canonical(entries):
    return codec.pack([dict(delivery_id=item['delivery_id'], disposition=item['disposition'],
                            token_digest=item['token_digest'], reason=item['reason'])
                       for item in entries])


def _conflict_positions(entries):
    """同一租约（投递+凭据）在批内出现相互矛盾的结果时，所有相关条目都标冲突。"""
    groups = defaultdict(list)
    for item in entries:
        groups[(item['delivery_id'], item['token_digest'])].append(item)
    conflicts = set()
    for items in groups.values():
        outcomes_seen = {(item['disposition'], item['reason']) for item in items}
        if len(outcomes_seen) > 1:
            conflicts.update(item['position'] for item in items)
    return conflicts


def _classify(db, item, now):
    row = db.execute('SELECT * FROM attempts WHERE delivery_id=? AND token_digest=?',
                     (item['delivery_id'], item['token_digest'])).fetchone()
    if row is None:
        return 'stale', '该凭据不属于此投递', None
    attempt = dict(row)
    if attempt['outcome'] == item['disposition'] and attempt['reason'] == item['reason']:
        result = codec.unpack(attempt['receipt']) if attempt['receipt'] is not None else None
        return 'replayed', '先前已确认，复用原收据', result
    try:
        target = delivery.get(db, item['delivery_id'])
    except missing as exc:
        return 'stale', str(exc), None
    if attempt['outcome'] != 'open' or target['state'] != 'leased' or attempt['expires'] <= now:
        return 'stale', '租约已处置或超时，凭据已失效', None
    record = outcomes.apply(db, target, attempt, item['disposition'], item['reason'], now)
    return 'applied', '首次生效', record


def _store(db, batch_key, worker, entries, fingerprint, now):
    batch_id = db.execute('''INSERT INTO receipt_batches(batch_key,worker,fingerprint,created)
                             VALUES (?,?,?,?)''',
                          (batch_key, worker, fingerprint, now)).lastrowid
    db.executemany('''INSERT INTO receipt_entries
                      (batch_id,position,delivery_id,disposition,token_digest,reason,status,detail,result)
                      VALUES (?,?,?,?,?,?,?,?,?)''',
                   [(batch_id, item['position'], item['delivery_id'], item['disposition'],
                     item['token_digest'], item['reason'], item['status'], item['detail'],
                     codec.pack(item['result']) if item['result'] is not None else None)
                    for item in entries])
    return batch_id


def lodge(db, raw, now):
    batch_key, worker, entries = _entries_input(raw)
    digest = codec.fingerprint(_canonical(entries))
    existing = db.execute('SELECT id FROM receipt_batches WHERE batch_key=?', (batch_key,)).fetchone()
    if existing is not None:
        stored = db.execute('SELECT fingerprint FROM receipt_batches WHERE id=?',
                            (existing['id'],)).fetchone()
        if stored['fingerprint'] != digest:
            raise conflict('该批次标识已登记另一批回执')
        return _view(db, existing['id']) | {'repeated': True}
    conflicts = _conflict_positions(entries)
    for item in entries:
        if item['position'] in conflicts:
            item['status'] = 'conflict'
            item['detail'] = '同租约在批内存在相互矛盾的结果，两项均不生效'
            item['result'] = None
            continue
        status, detail, result = _classify(db, item, now)
        item['status'], item['detail'], item['result'] = status, detail, result
    batch_id = _store(db, batch_key, worker, entries, digest, now)
    return _view(db, batch_id) | {'repeated': False}


def _view(db, batch_id):
    batch = db.execute('SELECT * FROM receipt_batches WHERE id=?', (batch_id,)).fetchone()
    if batch is None:
        raise missing('找不到上传收据')
    rows = db.execute('SELECT * FROM receipt_entries WHERE batch_id=? ORDER BY position', (batch_id,))
    items, counts = [], Counter()
    for row in rows:
        counts[row['status']] += 1
        items.append(dict(position=row['position'], delivery_id=row['delivery_id'],
                          disposition='ack' if row['disposition'] == 'acked' else 'nack',
                          reason=row['reason'], status=row['status'], detail=row['detail'],
                          result=codec.unpack(row['result']) if row['result'] is not None else None))
    return dict(receipt_id=batch['id'], batch=batch['batch_key'], worker=batch['worker'],
                created_at=batch['created'],
                counts={status: counts.get(status, 0) for status in STATUSES}, entries=items)


def receipt(db, key):
    return _view(db, key)
