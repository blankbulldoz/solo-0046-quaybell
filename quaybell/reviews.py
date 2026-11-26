"""Post-409 review and explicit republication candidates.

A producer can accidentally reuse a key for a different envelope.  The plain
publish endpoint keeps its baseline contract (same envelope -> stable receipt,
different envelope -> 409); this module supplies the *review* around that 409
and a two-phase *candidate* flow for the genuinely new alert:

1. ``conflict_review`` compares the stored alert with the incoming one, exposes
   the original stable receipt and suggests a free candidate key.
2. ``create_candidate`` persists a pending candidate only.  It never inserts a
   message or delivery, so an unconfirmed candidate can never be published.
3. ``confirm`` re-validates, in one write transaction, that the final key is
   still free and re-resolves the actual destinations against the current
   topology.  The resulting receipt (message id + fanout) is stored on the
   candidate and every repeated confirm returns that same stable receipt.

Two pending candidates may deliberately carry the same ``candidate_key``; the
``messages`` unique constraint plus the transactional re-check ensures only one
of them can confirm under it.  Retention pruning the original alert does not
cascade into candidate records: the review keeps its own snapshot.
"""

import sqlite3

from . import codec, inputs, publication, routing
from .envelope import Envelope
from .errors import conflict, missing

CANDIDATE_KINDS = '(?:candidates|publication-candidates|reviews|publication-reviews)'
_EXTRA_FIELDS = ('candidate_key', 'new_key', 'replacement_key', 'final_key',
                 'original_key', 'based_on_key', 'conflict_key', 'old_key',
                 'original_message_id')
_ENVELOPE_FIELDS = ('producer', 'key', 'lane', 'payload', 'tags', 'delay_seconds')


def error_aliases(review):
    """A few convenient top-level fields for clients that do not read ``review``."""
    original = review.get('original') or {}
    return {
        'suggested_key': review.get('suggested_key'),
        'candidate_key': review.get('candidate_key'),
        'original_message_id': original.get('message_id'),
        'existing_message_id': original.get('message_id'),
    }


def suggest_key(db, channel, producer, key):
    """Smallest ``key-N`` not yet used by a committed message.

    Pending candidates are intentionally not scanned: two simultaneous reviews
    must be able to receive the same suggestion, and confirmation is where the
    collision is settled.
    """
    n = 2
    while True:
        suffix = f'-{n}'
        base = key[:96 - len(suffix)]
        candidate = f'{base}{suffix}'
        if publication.find(db, channel, producer, candidate) is None:
            return candidate
        n += 1


def _envelope_request(row):
    content = codec.unpack(row['envelope'])
    return {'producer': row['producer'], 'key': row['dedup_key'], **content}


def message_brief(db, row):
    receipt = publication.receipt(db, row, False)
    request = _envelope_request(row)
    return {
        'message_id': row['id'], 'id': row['id'],
        'channel': row['channel'], 'producer': row['producer'],
        'key': row['dedup_key'], 'dedup_key': row['dedup_key'],
        'lane': row['lane'], 'policy': row['policy'], 'published': row['published'],
        'fingerprint': row['fingerprint'], 'digest': row['fingerprint'],
        'envelope': codec.unpack(row['envelope']),
        'envelope_request': request, 'request_body': request,
        'receipt': receipt, 'stable_receipt': receipt, 'fanout': receipt['fanout'],
    }


def conflict_review(db, channel, envelope, now, existing=None):
    key = envelope.dedup_key
    if existing is None:
        existing = publication.find(db, channel, envelope.producer, key)
    original = message_brief(db, existing) if existing is not None else None
    incoming_content = dict(lane=envelope.lane, tags=envelope.tags,
                           payload=envelope.payload, delay_seconds=envelope.delay_seconds)
    incoming_request = {'producer': envelope.producer, 'key': key, **incoming_content}
    incoming_fp = codec.fingerprint(envelope.encoded())
    destinations = [group['name'] for group in routing.matching(db, channel, envelope.tags)]
    suggested = suggest_key(db, channel, envelope.producer, key)
    candidate_body = {k: incoming_request[k] for k in _ENVELOPE_FIELDS if k in incoming_request}
    candidate_body.update({'candidate_key': suggested, 'original_key': key})
    if original is not None:
        candidate_body['original_message_id'] = original['message_id']
    doc = {
        'kind': 'key_conflict', 'reason': 'key_in_use',
        'channel': channel, 'producer': envelope.producer, 'key': key,
        'observed_at': now,
        'original_state': 'present' if original is not None else 'absent',
        'original': original, 'existing': original, 'stored': original,
        'current': original, 'conflicting': original, 'conflicting_message': original,
        'incoming': {
            'envelope': incoming_content, 'envelope_request': incoming_request,
            'request_body': incoming_request,
            'fingerprint': incoming_fp, 'digest': incoming_fp,
            'destinations': destinations, 'groups': destinations,
        },
        'attempted': {
            'envelope': incoming_content, 'envelope_request': incoming_request,
            'fingerprint': incoming_fp, 'destinations': destinations,
        },
        'attempted_envelope': incoming_content,
        'fingerprint': incoming_fp,
        'destinations': destinations, 'groups': destinations,
        'suggested_key': suggested, 'candidate_key': suggested,
        'resend_request': incoming_request if original is None else original['envelope_request'],
        'candidate_request': candidate_body,
        'paths': {
            'messages': f'/v1/channels/{channel}/messages',
            'candidates': f'/v1/channels/{channel}/candidates',
        },
    }
    if original is not None:
        doc['resend'] = {'method': 'POST', 'path': doc['paths']['messages'],
                         'body': original['envelope_request']}
    return doc


def _parse_creation(raw):
    # New endpoints are liberal about unknown fields so alias clients carrying
    # extra confirmation metadata do not trip strict envelope validation.
    extra = {key: raw[key] for key in _EXTRA_FIELDS if key in raw}
    filtered = {key: raw[key] for key in _ENVELOPE_FIELDS if key in raw}
    return Envelope.from_input(filtered), extra


def _load_original(db, channel, producer, original_key, original_message_id):
    if original_message_id is not None:
        try:
            message_id = int(original_message_id)
        except (TypeError, ValueError):
            raise inputs.invalid('original_message_id 须为整数')
        row = db.execute('SELECT * FROM messages WHERE id=?', (message_id,)).fetchone()
        if row is not None and row['channel'] == channel:
            return row
    if original_key is not None:
        return publication.find(db, channel, producer, original_key)
    return None


def create_candidate(db, channel, raw, now):
    envelope, extra = _parse_creation(raw)
    provided_key = extra.get('candidate_key') or extra.get('new_key') \
        or extra.get('replacement_key') or extra.get('final_key')
    original_key = extra.get('original_key') or extra.get('based_on_key') \
        or extra.get('conflict_key') or extra.get('old_key') or envelope.dedup_key
    original = _load_original(db, channel, envelope.producer, original_key,
                              extra.get('original_message_id'))
    if provided_key is not None:
        wanted_key = inputs.token(provided_key, '候选新键')
    elif original is not None and original['fingerprint'] != codec.fingerprint(envelope.encoded()):
        wanted_key = suggest_key(db, channel, envelope.producer, original_key)
    else:
        # No occupied conflicting original (e.g. it was pruned): the operator
        # may deliberately reuse the released key for a brand new receipt.
        wanted_key = envelope.dedup_key
    snapshot = message_brief(db, original) if original is not None else None
    encoded = envelope.encoded()
    cursor = db.execute('''
        INSERT INTO publication_candidates
          (channel,producer,original_key,original_message_id,original_snapshot,
           candidate_key,lane,envelope,fingerprint,status,created)
        VALUES (?,?,?,?,?,?,?,?,?,'pending',?)''',
        (channel, envelope.producer, original_key,
         original['id'] if original is not None else None,
         codec.pack(snapshot) if snapshot is not None else None,
         wanted_key, envelope.lane, encoded, codec.fingerprint(encoded), now))
    return get_candidate(db, cursor.lastrowid, channel=channel)


def _original_state(db, row):
    snapshot = codec.unpack(row['original_snapshot']) if row['original_snapshot'] else None
    current = None
    if row['original_message_id'] is not None:
        current = db.execute('SELECT * FROM messages WHERE id=?',
                            (row['original_message_id'],)).fetchone()
        if current is not None and current['channel'] != row['channel']:
            current = None
    if current is None and row['original_key']:
        current = publication.find(db, row['channel'], row['producer'], row['original_key'])
    if current is not None:
        return 'present', message_brief(db, current)
    if snapshot is not None:
        return 'pruned', dict(snapshot, gone=True)
    return 'absent', None


def candidate_view(db, row):
    data = dict(row)
    content = codec.unpack(row['envelope'])
    receipt = codec.unpack(row['receipt']) if row['receipt'] else None
    original_state, original = _original_state(db, row)
    if receipt is not None:
        destinations = list(receipt['fanout'])
        fanout = receipt['fanout']
    else:
        destinations = [group['name']
                        for group in routing.matching(db, row['channel'], content['tags'])]
        fanout = None
    final_key = row['final_key'] or row['candidate_key']
    request_body = {'producer': row['producer'], 'key': final_key, **content}
    suggested = suggest_key(db, row['channel'], row['producer'],
                            row['original_key'] or row['candidate_key'])
    published = row['status'] == 'confirmed'
    delivery_rows = [{'group_name': group_name, 'delivery_id': delivery_id}
                     for group_name, delivery_id in sorted((receipt or {}).get('fanout', {}).items())]
    binding = None
    if receipt is not None:
        binding = {
            'candidate_id': row['id'],
            'final_key': row['final_key'],
            'envelope_fingerprint': row['fingerprint'],
            'fingerprint': row['fingerprint'],
            'destinations': receipt['fanout'],
            'fanout': receipt['fanout'],
            'deliveries': delivery_rows,
            'message_id': receipt['message_id'],
            'receipt': receipt,
            'stable_receipt': receipt,
            'confirmed_at': row['confirmed_at'],
        }
    doc = {
        'id': row['id'], 'candidate_id': row['id'],
        'channel': row['channel'], 'producer': row['producer'],
        'status': row['status'], 'state': row['status'],
        'pending': row['status'] == 'pending', 'confirmed': published,
        'discarded': row['status'] == 'discarded',
        'original_key': row['original_key'],
        'candidate_key': row['candidate_key'], 'requested_key': row['candidate_key'],
        'final_key': row['final_key'], 'key': final_key, 'dedup_key': final_key,
        'lane': row['lane'],
        'envelope': content, 'final_envelope': content,
        'envelope_request': request_body, 'request_body': request_body,
        'fingerprint': row['fingerprint'], 'digest': row['fingerprint'],
        'destinations': destinations, 'groups': destinations,
        'destination_groups': destinations,
        'destinations_current': destinations if not published else None,
        'actual_destinations': fanout, 'fanout': fanout,
        'deliveries': delivery_rows,
        'binding': binding,
        'receipt': receipt, 'stable_receipt': receipt,
        'message_id': row['message_id'] or (receipt or {}).get('message_id'),
        'original_state': original_state,
        'original': original, 'existing': original,
        'original_snapshot': codec.unpack(row['original_snapshot']) if row['original_snapshot'] else None,
        'reuses_released_key': bool(row['original_key'] and final_key == row['original_key']
                                    and original_state != 'present'),
        'suggested_key': suggested,
        'created_at': row['created'], 'confirmed_at': row['confirmed_at'],
        'confirm_body': {'fingerprint': row['fingerprint']},
        'paths': {
            'self': f'/v1/channels/{row["channel"]}/candidates/{row["id"]}',
            'confirm': f'/v1/channels/{row["channel"]}/candidates/{row["id"]}/confirm',
            'discard': f'/v1/channels/{row["channel"]}/candidates/{row["id"]}/discard',
        },
    }
    return doc


def get_candidate(db, candidate_id, channel=None):
    row = db.execute('SELECT * FROM publication_candidates WHERE id=?',
                     (candidate_id,)).fetchone()
    if row is None or (channel is not None and row['channel'] != channel):
        raise missing('找不到发布候选单')
    return candidate_view(db, row)


def list_candidates(db, channel):
    rows = db.execute('SELECT * FROM publication_candidates WHERE channel=? ORDER BY id DESC LIMIT 100',
                      (channel,)).fetchall()
    items = [candidate_view(db, row) for row in rows]
    return {'channel': channel, 'candidates': items, 'items': items,
            'pending': sum(item['status'] == 'pending' for item in items)}


def _final_envelope(row, final_key):
    content = codec.unpack(row['envelope'])
    return Envelope(producer=row['producer'], dedup_key=final_key, lane=content['lane'],
                    tags=content['tags'], payload=content['payload'],
                    delay_seconds=content['delay_seconds'])


def _key_conflict(db, row, envelope, now, existing):
    review = conflict_review(db, row['channel'], envelope, now, existing=existing)
    # For the standard race (both confirm the recorded candidate key), suggest
    # the next free key in the *original* sequence (dup-key-2 -> dup-key-3)
    # rather than nesting suffixes (dup-key-2-2).  A custom key override gets
    # its own next-suffix suggestion.
    if envelope.dedup_key == row['candidate_key'] and row['original_key']:
        suggested = suggest_key(db, row['channel'], row['producer'], row['original_key'])
        review['suggested_key'] = suggested
        review['candidate_key'] = suggested
    review['candidate_id'] = row['id']
    raise conflict('最终键已被另一条告警占用；两个不同信封不能共用同一生产键，可改用建议的新键后再次确认',
                   {'review': review, 'conflict': review, **error_aliases(review)})


def confirm(db, candidate_id, raw, now):
    raw = raw if isinstance(raw, dict) else {}
    row = db.execute('SELECT * FROM publication_candidates WHERE id=?',
                     (candidate_id,)).fetchone()
    if row is None:
        raise missing('找不到发布候选单')
    nested = raw.get('envelope') if isinstance(raw.get('envelope'), dict) else {}
    expected = raw.get('fingerprint', raw.get('digest', nested.get('fingerprint')))
    if expected is not None and expected != row['fingerprint']:
        raise conflict('确认指纹与候选单绑定的最终信封不一致，已拒绝发布',
                       {'reason': 'envelope_mismatch', 'candidate_id': candidate_id})
    # A client may resubmit the whole final envelope at confirmation; it must
    # canonicalize to exactly the fingerprint bound when the candidate opened.
    envelope_body = nested if nested else raw
    if isinstance(envelope_body, dict) and (
            isinstance(nested, dict) and nested or {'lane', 'payload'} <= envelope_body.keys()):
        candidate_fields = {key: envelope_body[key] for key in _ENVELOPE_FIELDS
                            if key in envelope_body}
        candidate_fields.setdefault('producer', row['producer'])
        candidate_fields.setdefault('key', row['candidate_key'])
        submitted = Envelope.from_input(candidate_fields)
        if codec.fingerprint(submitted.encoded()) != row['fingerprint']:
            raise conflict('提交的最终信封与候选单绑定的信封不一致，已拒绝发布',
                           {'reason': 'envelope_mismatch', 'candidate_id': candidate_id})
    if row['status'] == 'discarded':
        raise conflict('候选单已放弃，不能确认发布；请重新建立候选',
                       {'reason': 'candidate_discarded', 'candidate_id': candidate_id})
    supplied_key = raw.get('key') or raw.get('candidate_key') or raw.get('final_key') \
        or nested.get('key')

    if row['status'] == 'confirmed':
        # Stable receipt replay: a confirmed candidate answers every repeat with
        # exactly the receipt recorded on its first successful confirmation.
        if supplied_key is not None and supplied_key != row['final_key']:
            raise conflict('候选单已确认并绑定另一最终键的稳定回执，不能改键重发',
                           {'reason': 'receipt_bound', 'candidate_id': candidate_id,
                            'receipt': codec.unpack(row['receipt'])})
        doc = get_candidate(db, candidate_id)
        receipt = doc['receipt']
        return dict(doc, repeated=True, policy=receipt['policy'],
                    fanout=receipt['fanout'], message_id=receipt['message_id'])

    key_choice = inputs.token(supplied_key or row['candidate_key'], '最终生产键')
    envelope = _final_envelope(row, key_choice)

    existing = publication.find(db, row['channel'], row['producer'], key_choice)
    if existing is not None:
        if existing['fingerprint'] == row['fingerprint'] \
                and existing['envelope'] == row['envelope']:
            # The exact final envelope was committed out-of-band (or by a racing
            # identical confirmation): attach to that stable receipt instead of
            # creating a second message.
            receipt = publication.receipt(db, existing, True)
            _mark_confirmed(db, row['id'], key_choice, receipt, now,
                            message_id=existing['id'])
            doc = get_candidate(db, candidate_id)
            return dict(doc, repeated=True, policy=receipt['policy'],
                        fanout=receipt['fanout'], message_id=receipt['message_id'])
        _key_conflict(db, row, envelope, now, existing)

    try:
        receipt = publication.commit_new(
            db, row['channel'], envelope, now,
            journal_details={'candidate_id': row['id'], 'via': 'publication_candidate'})
    except sqlite3.IntegrityError:
        # Backstop for the unique (channel, producer, dedup_key) constraint if
        # another writer won the race between the read above and the insert.
        winner = publication.find(db, row['channel'], row['producer'], key_choice)
        _key_conflict(db, row, envelope, now, winner)
    _mark_confirmed(db, row['id'], key_choice, receipt, now,
                    message_id=receipt['message_id'])
    doc = get_candidate(db, candidate_id)
    return dict(doc, repeated=False, policy=receipt['policy'],
                fanout=receipt['fanout'], message_id=receipt['message_id'])


def _mark_confirmed(db, candidate_id, final_key, receipt, now, *, message_id):
    db.execute('''UPDATE publication_candidates
                  SET status='confirmed', final_key=?, destinations=?, receipt=?,
                      message_id=?, confirmed_at=?
                  WHERE id=?''',
               (final_key, codec.pack(receipt), codec.pack(receipt),
                message_id, now, candidate_id))


def discard(db, candidate_id, raw, now):
    row = db.execute('SELECT * FROM publication_candidates WHERE id=?',
                     (candidate_id,)).fetchone()
    if row is None:
        raise missing('找不到发布候选单')
    if row['status'] == 'confirmed':
        raise conflict('候选单已确认发布，不能放弃', {'reason': 'already_confirmed'})
    if row['status'] == 'pending':
        db.execute("UPDATE publication_candidates SET status='discarded' WHERE id=?",
                   (candidate_id,))
    return get_candidate(db, candidate_id)


def resend_message(db, message_id, now):
    """Re-submit a stored envelope through the normal idempotent publish path."""
    row = db.execute('SELECT * FROM messages WHERE id=?', (message_id,)).fetchone()
    if row is None:
        raise missing('原消息已不存在（可能已被保留清理），无旧回执可复用；请作为新告警复核发布')
    request = _envelope_request(row)
    return publication.publish(db, row['channel'], request, now)
