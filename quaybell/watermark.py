from . import catalog, codec, journal, subscriptions
from .errors import missing

FORMAT = 'quaybell-watermark/1'


def pending(db, channel, group_name, message_id, lane, version, digest, published):
    """Record that a matched message joined this group's watermark history."""
    db.execute('''INSERT OR IGNORE INTO watermark_entries
                  (channel,group_name,message_id,lane,policy,message_sha256,published,state)
                  VALUES (?,?,?,?,?,?,?,'pending')''',
               (channel, group_name, message_id, lane, version, digest, published))


def confirmed(db, item, attempt, now):
    """Pin an acknowledgement into the ledger so cleanup cannot erase its basis."""
    snapshot = db.execute('SELECT lane,policy,fingerprint,published FROM messages WHERE id=?',
                          (item['message_id'],)).fetchone()
    existing = db.execute('SELECT state FROM watermark_entries WHERE channel=? AND group_name=? AND message_id=?',
                          (item['channel'], item['group_name'], item['message_id'])).fetchone()
    if existing is not None and existing['state'] == 'confirmed':
        return
    if existing is not None:
        db.execute('''UPDATE watermark_entries
                      SET state='confirmed',delivery_id=?,parent_id=?,attempt=?,confirmed_at=?
                      WHERE channel=? AND group_name=? AND message_id=?''',
                   (item['id'], item['parent_id'], attempt['number'], now,
                    item['channel'], item['group_name'], item['message_id']))
    elif snapshot is not None:
        db.execute('''INSERT INTO watermark_entries
                      (channel,group_name,message_id,lane,policy,message_sha256,published,state,
                       delivery_id,parent_id,attempt,confirmed_at)
                      VALUES (?,?,?,?,?,?,?,'confirmed',?,?,?,?)''',
                   (item['channel'], item['group_name'], item['message_id'], snapshot['lane'],
                    snapshot['policy'], snapshot['fingerprint'], snapshot['published'],
                    item['id'], item['parent_id'], attempt['number'], now))


def _entries(db, channel, group_name, subscription_id=None):
    rows = [dict(row) for row in db.execute(
        'SELECT * FROM watermark_entries WHERE channel=? AND group_name=? ORDER BY message_id',
        (channel, group_name))]
    if subscription_id is None:
        return rows
    known = {row['message_id'] for row in rows}
    legacy = [dict(row) for row in db.execute(
        '''SELECT m.id,m.lane,m.policy,m.fingerprint,m.published FROM messages m
           WHERE m.channel=? AND EXISTS(SELECT 1 FROM deliveries d
             WHERE d.message_id=m.id AND d.subscription_id=?)
           ORDER BY m.id''', (channel, subscription_id))]
    for row in legacy:
        if row['id'] in known:
            continue
        hit = db.execute('''SELECT d.id,d.parent_id,a.number,a.finished FROM deliveries d
                            JOIN attempts a ON a.delivery_id=d.id AND a.outcome='acked'
                            WHERE d.message_id=? AND d.subscription_id=?
                            ORDER BY d.id, a.number DESC LIMIT 1''',
                         (row['id'], subscription_id)).fetchone()
        rows.append({'channel': channel, 'group_name': group_name, 'message_id': row['id'],
                     'lane': row['lane'], 'policy': row['policy'],
                     'message_sha256': row['fingerprint'], 'published': row['published'],
                     'state': 'confirmed' if hit else 'pending',
                     'delivery_id': hit['id'] if hit else None,
                     'parent_id': hit['parent_id'] if hit else None,
                     'attempt': hit['number'] if hit else None,
                     'confirmed_at': hit['finished'] if hit else None})
    return sorted(rows, key=lambda row: row['message_id'])


def _chain(entries):
    basis, ahead, gap = [], [], None
    for row in entries:
        if gap is None:
            if row['state'] == 'confirmed':
                basis.append(row)
            else:
                gap = row
        elif row['state'] == 'confirmed':
            ahead.append(row)
    return basis, ahead, gap


def _basis(row):
    return evidence_of(row)


def evidence_of(entry):
    """Canonical evidence captured for a ledger row, independent of row layout."""
    return {'message_id': entry['message_id'], 'lane': entry['lane'], 'policy': entry['policy'],
            'published_at': entry['published'], 'message_sha256': entry['message_sha256'],
            'delivery_id': entry['delivery_id'], 'parent_id': entry['parent_id'],
            'attempt': entry['attempt'], 'confirmed_at': entry['confirmed_at']}


def _last_attempt(db, message_id, subscription_id):
    row = db.execute('''SELECT a.number,a.outcome,a.reason,a.finished FROM attempts a
                        JOIN deliveries d ON d.id=a.delivery_id
                        WHERE d.message_id=? AND d.subscription_id=?
                        ORDER BY a.id DESC LIMIT 1''', (message_id, subscription_id)).fetchone()
    return dict(row) if row else None


def _gap_info(db, entry, subscription_id):
    deliveries = [dict(row) for row in db.execute(
        '''SELECT id,state,parent_id FROM deliveries
           WHERE message_id=? AND subscription_id=? ORDER BY id''',
        (entry['message_id'], subscription_id))]
    active = [row for row in deliveries if row['state'] in ('waiting', 'leased')]
    unsettled = [row for row in deliveries if row['state'] != 'acked']
    if active:
        blocking, state = active[0], active[0]['state']
    elif unsettled:
        blocking, state = unsettled[0], 'dead'
    elif deliveries:
        blocking, state = deliveries[0], 'unconfirmed'
    else:
        blocking, state = None, 'unconfirmed'
    return {'message_id': entry['message_id'], 'lane': entry['lane'],
            'message_sha256': entry['message_sha256'], 'state': state,
            'delivery_id': blocking['id'] if blocking else None,
            'parent_id': blocking['parent_id'] if blocking else None,
            'last_attempt': _last_attempt(db, entry['message_id'], subscription_id)}


def _skipped(db, channel, group_name, watermark, entries):
    if watermark is None:
        return []
    known = {row['message_id'] for row in entries if row['message_id'] <= watermark}
    ids = [row['id'] for row in db.execute(
        'SELECT id FROM messages WHERE channel=? AND id<=? ORDER BY id', (channel, watermark))]
    return [key for key in ids if key not in known]


def _ledger(entries):
    confirmed_count = sum(1 for row in entries if row['state'] == 'confirmed')
    return {'entries': len(entries), 'confirmed': confirmed_count,
            'outstanding': len(entries) - confirmed_count}


def _present(db, entries):
    keys = [row['message_id'] for row in entries]
    total = 0
    for start in range(0, len(keys), 400):
        chunk = keys[start:start + 400]
        total += db.execute(f'''SELECT COUNT(*) FROM messages
                                WHERE id IN ({','.join('?' * len(chunk))})''', chunk).fetchone()[0]
    return total


def _beyond(entries, gap):
    if gap is None:
        return 0
    return sum(1 for row in entries
               if row['message_id'] > gap['message_id'] and row['state'] != 'confirmed')


def _lanes(entries, global_gap):
    """Per-lane progress: a gap in one lane never advances while blocked, and lanes advance independently."""
    order, groups = [], {}
    for row in entries:
        if row['lane'] not in groups:
            order.append(row['lane'])
            groups[row['lane']] = []
        groups[row['lane']].append(row)
    lanes = []
    for lane in order:
        basis, ahead, gap = _chain(groups[lane])
        lanes.append({'lane': lane,
                      'watermark_message_id': basis[-1]['message_id'] if basis else None,
                      'gap_message_id': gap['message_id'] if gap else None,
                      'confirmed': len(basis), 'ahead': len(ahead),
                      'holds_global': bool(gap and global_gap and gap['message_id'] == global_gap['message_id'])})
    return lanes


def ensure(db, channel, group_name, subscription_id):
    """Backfill and reconcile entries for deliveries created before the ledger existed."""
    db.execute('''INSERT OR IGNORE INTO watermark_entries
                  (channel,group_name,message_id,lane,policy,message_sha256,published,state)
                  SELECT m.channel,?,m.id,m.lane,m.policy,m.fingerprint,m.published,'pending'
                  FROM messages m JOIN deliveries d ON d.message_id=m.id
                  WHERE m.channel=? AND d.subscription_id=?''',
               (group_name, channel, subscription_id))
    for row in db.execute('''SELECT message_id FROM watermark_entries
                             WHERE channel=? AND group_name=? AND state='pending'
                             ORDER BY message_id''', (channel, group_name)):
        hit = db.execute('''SELECT d.id,d.parent_id,a.number,a.finished FROM deliveries d
                            JOIN attempts a ON a.delivery_id=d.id AND a.outcome='acked'
                            WHERE d.message_id=? AND d.subscription_id=? AND d.state='acked'
                            ORDER BY d.id, a.number DESC LIMIT 1''',
                         (row['message_id'], subscription_id)).fetchone()
        if hit is not None:
            db.execute('''UPDATE watermark_entries
                          SET state='confirmed',delivery_id=?,parent_id=?,attempt=?,confirmed_at=?
                          WHERE channel=? AND group_name=? AND message_id=?''',
                       (hit['id'], hit['parent_id'], hit['number'], hit['finished'],
                        channel, group_name, row['message_id']))


def _summary(db, channel, group_name, subscription_id, entries, now_key, now):
    basis, ahead, gap = _chain(entries)
    watermark = basis[-1]['message_id'] if basis else None
    payload = {'format': FORMAT, 'channel': channel, 'group': group_name,
               'watermark_message_id': watermark,
               'archive_ready': gap is None and watermark is not None,
               'confirmed': [_basis(row) for row in basis],
               'ahead': [_basis(row) for row in ahead],
               'gap': _gap_info(db, gap, subscription_id) if gap else None,
               'skipped_message_ids': _skipped(db, channel, group_name, watermark, entries),
               'unconfirmed_after_gap': _beyond(entries, gap),
               'lanes': _lanes(entries, gap),
               'ledger': _ledger(entries),
               'messages_present': _present(db, entries),
               now_key: now}
    return payload


def view(db, channel, group, now):
    catalog.get(db, channel)
    group = subscriptions.get(db, channel, group)
    entries = _entries(db, channel, group['name'], group['id'])
    result = _summary(db, channel, group['name'], group['id'], entries, 'observed_at', now)
    result['certificates'] = [dict(row) for row in db.execute(
        '''SELECT id,issued_at,watermark_message_id FROM watermark_certificates
           WHERE channel=? AND group_name=? ORDER BY id DESC LIMIT 5''',
        (channel, group['name']))]
    return result


def _verify(db, payload, stored):
    digest = codec.fingerprint(codec.pack(payload))
    missing_ids, mismatched, retained = [], [], 0
    for item in payload.get('confirmed', []):
        row = db.execute('''SELECT * FROM watermark_entries
                            WHERE channel=? AND group_name=? AND message_id=?''',
                         (payload['channel'], payload['group'], item['message_id'])).fetchone()
        if row is None or row['state'] != 'confirmed':
            missing_ids.append(item['message_id'])
            continue
        if evidence_of(dict(row)) != item:
            mismatched.append(item['message_id'])
        if db.execute('SELECT 1 FROM messages WHERE id=?', (item['message_id'],)).fetchone():
            retained += 1
    checked = len(payload.get('confirmed', []))
    return {'digest_matches': digest == stored, 'recomputed_sha256': digest,
            'basis_matches': not missing_ids and not mismatched, 'basis_checked': checked,
            'basis_missing': missing_ids, 'basis_mismatched': mismatched,
            'messages_retained': retained, 'messages_cleaned': checked - retained}


def issue(db, channel, group, now):
    catalog.get(db, channel)
    group = subscriptions.get(db, channel, group)
    ensure(db, channel, group['name'], group['id'])
    entries = _entries(db, channel, group['name'], group['id'])
    key = db.execute('''INSERT INTO watermark_certificates
                        (channel,group_name,issued_at,watermark_message_id,payload,sha256)
                        VALUES (?,?,?,?,?,?)''',
                     (channel, group['name'], now, None, '{}', '')).lastrowid
    payload = _summary(db, channel, group['name'], group['id'], entries, 'issued_at', now)
    payload['certificate_id'] = key
    digest = codec.fingerprint(codec.pack(payload))
    db.execute('UPDATE watermark_certificates SET watermark_message_id=?,payload=?,sha256=? WHERE id=?',
               (payload['watermark_message_id'], codec.pack(payload), digest, key))
    journal.append(db, channel, now, 'watermark_issued',
                   {'certificate_id': key, 'group': group['name'],
                    'watermark_message_id': payload['watermark_message_id']})
    return payload | {'sha256': digest, 'verification': _verify(db, payload, digest)}


def reproduce(db, key):
    row = db.execute('SELECT * FROM watermark_certificates WHERE id=?', (key,)).fetchone()
    if row is None:
        raise missing('找不到水位凭证')
    payload = codec.unpack(row['payload'])
    return payload | {'sha256': row['sha256'],
                      'verification': _verify(db, payload, row['sha256'])}
