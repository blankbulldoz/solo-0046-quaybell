"""Offline batch backfill: one commitment per client batch key, replayed by position."""
import sqlite3

from . import catalog, codec, inputs, journal, policy, publication
from .envelope import Envelope
from .errors import RelayError, conflict, invalid, missing

MAX_ENTRIES = 100
REPLAY_DEFAULT = 86400


def _content(prepared):
    canonical = [dict(producer=item['envelope'].producer, key=item['envelope'].dedup_key,
                      envelope=item['encoded']) for item in prepared]
    return codec.fingerprint(codec.pack(canonical))


def _receipt_digest(channel, batch_key, content_digest, positions):
    return codec.fingerprint(codec.pack(dict(batch=batch_key, channel=channel,
                                             content_digest=content_digest,
                                             positions=positions)))


def _row(db, channel, batch_key):
    found = db.execute('SELECT * FROM batches WHERE channel=? AND batch_key=?',
                       (channel, batch_key)).fetchone()
    return None if found is None else dict(found)


def _prepare(raw):
    inputs.require(raw, ('batch', 'entries'), ('replay_seconds',))
    batch_key = inputs.batch_key(raw['batch'], '批标识')
    entries = raw['entries']
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_ENTRIES:
        raise invalid(f'批内信封须为 1～{MAX_ENTRIES} 个对象的数组')
    prepared = []
    for position, item in enumerate(entries):
        if not isinstance(item, dict):
            raise invalid(f'第 {position + 1} 个位置须为信封对象')
        try:
            envelope = Envelope.from_input(item)
        except RelayError as exc:
            raise RelayError(exc.status, exc.code, f'第 {position + 1} 个位置：{exc}') from None
        prepared.append(dict(envelope=envelope, encoded=envelope.encoded(),
                             fingerprint=codec.fingerprint(envelope.encoded())))
    return batch_key, prepared


def _check_batch_internal(prepared):
    seen = {}
    for position, item in enumerate(prepared):
        envelope = item['envelope']
        pair = (envelope.producer, envelope.dedup_key)
        if pair in seen:
            first, encoded = seen[pair]
            if encoded != item['encoded']:
                raise conflict(f'同一批内第 {first + 1} 与第 {position + 1} 个位置'
                               f'对生产键 {envelope.producer}/{envelope.dedup_key} 给出不同内容，'
                               '整批未写入')
        else:
            seen[pair] = (position, item['encoded'])


def _check_existing(db, channel, prepared):
    for position, item in enumerate(prepared):
        envelope = item['envelope']
        existing = db.execute('SELECT * FROM messages WHERE channel=? AND producer=? AND dedup_key=?',
                              (channel, envelope.producer, envelope.dedup_key)).fetchone()
        if existing and (existing['fingerprint'] != item['fingerprint']
                         or existing['envelope'] != item['encoded']):
            raise conflict(f'第 {position + 1} 个位置的生产键 {envelope.producer}/{envelope.dedup_key} '
                           '已用于另一条消息，整批未写入')


def _replay_doc(db, row, channel, positions, repeated):
    pruned = [item['position'] for item in positions
              if db.execute('SELECT 1 FROM messages WHERE id=?', (item['message_id'],)).fetchone() is None]
    return dict(batch=row['batch_key'], channel=channel, entries=row['entries'],
                state='replayable', submitted=row['submitted'], replay_until=row['replay_until'],
                content_digest=row['fingerprint'], receipt_digest=row['digest'],
                retire_after=row['retire_after'], positions=positions,
                pruned_positions=pruned, repeated=repeated)


def _closed(row, channel):
    return dict(batch=row['batch_key'], channel=channel, entries=row['entries'],
                state='closed', submitted=row['submitted'], replay_until=row['replay_until'],
                content_digest=row['fingerprint'], receipt_digest=row['digest'],
                retire_after=row['retire_after'], positions=None,
                note='补传期已结束：仍能确认该批标识曾以同一顺序和内容一次性提交；'
                     '逐位置消息号和分发集合不可再取，也不能重发。记录到期后随清理退役。')


def _closed_error(row):
    return RelayError(410, 'replay_window_closed',
                      f'批标识 {row["batch_key"]} 的补传期已于 {int(row["replay_until"])} 结束：'
                      f'只能确认当时的 {row["entries"]} 个位置与内容摘要 '
                      f'{row["fingerprint"][:16]}…，逐位置回执不可再取，也不能重发')


def submit(db, channel, raw, now):
    catalog.get(db, channel)
    batch_key, prepared = _prepare(raw)
    content_digest = _content(prepared)
    found = _row(db, channel, batch_key)
    if found is not None:
        if found['fingerprint'] != content_digest:
            raise conflict(f'批标识 {batch_key} 已用于另一组顺序或内容，整批未写入')
        if now > found['replay_until']:
            raise _closed_error(found)
        return _replay_doc(db, found, channel, codec.unpack(found['receipt']), True)

    replay_seconds = inputs.integer(raw.get('replay_seconds', REPLAY_DEFAULT), 1, 604800, '补传期秒数')
    _check_batch_internal(prepared)
    _check_existing(db, channel, prepared)
    active = policy.read(db, channel)
    materialized = {}
    positions = []
    for position, item in enumerate(prepared):
        envelope = item['envelope']
        pair = (envelope.producer, envelope.dedup_key)
        if pair in materialized:
            receipt = materialized[pair] | {'reused': True}
        else:
            stored = db.execute('SELECT * FROM messages WHERE channel=? AND producer=? AND dedup_key=?',
                                (channel, envelope.producer, envelope.dedup_key)).fetchone()
            if stored is not None:
                receipt = publication.receipt(db, dict(stored), False) | {'reused': True}
            else:
                receipt = publication.materialize(db, channel, envelope, item['encoded'],
                                                  item['fingerprint'], active, now) | {'reused': False}
            materialized[pair] = receipt
        positions.append(dict(position=position, producer=envelope.producer,
                              key=envelope.dedup_key, message_id=receipt['message_id'],
                              policy=receipt['policy'], reused=receipt['reused'],
                              fanout=receipt['fanout']))

    stored_receipt = codec.pack(positions)
    digest = _receipt_digest(channel, batch_key, content_digest, positions)
    replay_until = now + replay_seconds
    retire_after = replay_until + active['retention_seconds']
    try:
        db.execute('INSERT INTO batches(channel,batch_key,fingerprint,entries,receipt,digest,'
                   'submitted,replay_until,retire_after) VALUES (?,?,?,?,?,?,?,?,?)',
                   (channel, batch_key, content_digest, len(positions), stored_receipt, digest,
                    now, replay_until, retire_after))
    except sqlite3.IntegrityError:
        raise conflict(f'批标识 {batch_key} 已被并发提交，请按原顺序和内容重试') from None
    journal.append(db, channel, now, 'batch_submitted',
                   dict(batch=batch_key, entries=len(positions), content_digest=content_digest,
                        message_ids=[item['message_id'] for item in positions],
                        reused_positions=[item['position'] for item in positions if item['reused']],
                        replay_until=replay_until, retire_after=retire_after))
    return dict(batch=batch_key, channel=channel, entries=len(positions), state='replayable',
                submitted=now, replay_until=replay_until, content_digest=content_digest,
                receipt_digest=digest, retire_after=retire_after,
                positions=positions, repeated=False)


def view(db, channel, batch_key, now):
    catalog.get(db, channel)
    found = _row(db, channel, batch_key)
    if found is None:
        raise missing('找不到该批次的补交记录')
    if now <= found['replay_until']:
        return _replay_doc(db, found, channel, codec.unpack(found['receipt']), False)
    return _closed(found, channel)


def recent(db, channel, limit=10):
    rows = db.execute('SELECT batch_key,entries,submitted,replay_until,retire_after '
                      'FROM batches WHERE channel=? ORDER BY submitted DESC,batch_key DESC LIMIT ?',
                      (channel, limit))
    return [dict(row) for row in rows]


def retirable(db, channel, now, limit):
    rows = db.execute('SELECT batch_key,retire_after FROM batches '
                      'WHERE channel=? AND retire_after<=? ORDER BY retire_after,batch_key LIMIT ?',
                      (channel, now, limit))
    return [dict(row) for row in rows]


def retire(db, channel, now, limit):
    keys = [item['batch_key'] for item in retirable(db, channel, now, limit)]
    if keys:
        db.executemany('DELETE FROM batches WHERE channel=? AND batch_key=?',
                       [(channel, key) for key in keys])
        journal.append(db, channel, now, 'batches_retired', {'batch_keys': keys})
    return keys
