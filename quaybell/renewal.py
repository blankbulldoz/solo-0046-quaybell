"""Lease renewal with a fixed total processing budget per attempt.

A renewal only touches the token holder's own open attempt.  The deadline is
extended from its current value (so consecutive renewals stay observable even
with a frozen test clock), while ``attempts.budget_expires`` is the absolute
ceiling granted at claim time.  Every write runs inside the caller's
``BEGIN IMMEDIATE`` transaction, so a renewal and a concurrent reaper have one
definitive winner.
"""

from . import attempts, codec, delivery, inputs, journal, policy
from .errors import conflict

# Absolute total budget a claim may request for one attempt.
BUDGET_MIN = 1
BUDGET_MAX = 604800
# A claim without an explicit budget grants the initial lease plus two
# full-length renewals (three policy lease periods in total).
DEFAULT_BUDGET_PERIODS = 3
EXTEND_MIN = 1
EXTEND_MAX = 604800

BUDGET_FIELDS = ('budget_seconds', 'budgetSeconds', 'budget', 'max_lease_seconds',
                 'maxLeaseSeconds', 'lease_budget_seconds', 'lease_budget',
                 'total_budget_seconds', 'total_lease_seconds', 'total_seconds',
                 'processing_budget_seconds', 'processing_budget', 'processing_seconds',
                 'max_processing_seconds', 'max_processing_time_seconds',
                 'max_duration_seconds', 'lease_lifetime_seconds',
                 'max_lease_duration_seconds', 'duration_budget_seconds', 'max_seconds')
BUDGET_EXPIRES_FIELDS = ('budget_expires', 'budgetExpires', 'budget_deadline',
                         'max_expires', 'deadline')
BUDGET_COUNT_FIELDS = ('max_renewals', 'renewal_limit', 'max_extensions',
                       'renewal_budget', 'max_renew_count')
SECONDS_FIELDS = ('seconds', 'extend_seconds', 'extension_seconds', 'lease_seconds',
                  'increment_seconds', 'ttl_seconds', 'ttl')
TARGET_FIELDS = ('new_expires', 'expires', 'expires_at', 'target_expires', 'deadline')
KEY_FIELDS = ('idempotency_key', 'idempotencyKey', 'request_id', 'requestId',
              'renew_id', 'renewId', 'idempotency', 'nonce', 'client_token', 'clientToken', 'key')
SEQUENCE_FIELDS = ('renewal_number', 'renew_sequence', 'sequence', 'seq')


def default_budget_seconds(rule):
    return rule['lease_seconds'] * DEFAULT_BUDGET_PERIODS


def _budget_amount(value):
    if isinstance(value, dict):
        value = value.get('seconds', value.get('budget_seconds', value.get('total_seconds')))
    return value


def claim_budget(raw, rule, now):
    """Resolve the absolute cumulative ceiling for a freshly opened attempt."""
    given = next((raw[key] for key in BUDGET_FIELDS if key in raw), None)
    count = next((raw[key] for key in BUDGET_COUNT_FIELDS if key in raw), None)
    absolute = next((raw[key] for key in BUDGET_EXPIRES_FIELDS if key in raw), None)
    step = rule['lease_seconds']
    if given is not None:
        seconds = inputs.integer(_budget_amount(given), BUDGET_MIN, BUDGET_MAX, '续租总预算')
        return seconds, now + seconds
    if count is not None:
        renewals = inputs.integer(count, 0, 100, '续租次数预算')
        seconds = step * (renewals + 1)  # initial lease plus the allowed renewals
        return seconds, now + seconds
    if absolute is not None:
        if isinstance(absolute, bool) or not isinstance(absolute, (int, float)):
            raise inputs.invalid('预算截止时刻须为 Unix 秒数值')
        seconds = absolute - now
        if not BUDGET_MIN <= seconds <= BUDGET_MAX:
            raise inputs.invalid('续租总预算须为领取后 1～604800 秒')
        return seconds, float(absolute)
    seconds = default_budget_seconds(rule)
    return seconds, now + seconds


def _pick(raw, fields):
    for key in fields:
        if key in raw and raw[key] is not None:
            return raw[key]
    return None


def _stored(db, attempt_id, number=None, key=None):
    if key is not None:
        row = db.execute('SELECT receipt FROM lease_renewals WHERE attempt_id=? AND request_key=?',
                         (attempt_id, key)).fetchone()
    else:
        row = db.execute('SELECT receipt FROM lease_renewals WHERE attempt_id=? AND number=?',
                         (attempt_id, number)).fetchone()
    return None if row is None else codec.unpack(row['receipt'])


def _present(stored, token, repeated):
    lease = dict(stored.get('lease') or {})
    lease['token'] = token
    return dict(stored) | {'token': token, 'lease': lease, 'repeated': repeated}


def extend(db, key, raw, now, header_key=None):
    token_value = _pick(raw, ('token', 'lease_token', 'credential'))
    token = inputs.token(token_value, '租约凭据')
    request_key = _pick(raw, KEY_FIELDS) or header_key
    if request_key is not None and (not isinstance(request_key, str)
                                    or not 1 <= len(request_key) <= 96):
        raise inputs.invalid('续租请求标识须为 1～96 个字符')
    sequence = _pick(raw, SEQUENCE_FIELDS)
    if sequence is not None:
        sequence = inputs.integer(sequence, 1, 10_000, '续租序号')

    item = delivery.get(db, key)
    attempt = attempts.by_token(db, key, token)

    # Explicit retries are answered from the recorded renewal receipt before
    # any liveness check, matching the ack/nack replay contract.
    if request_key is not None:
        stored = _stored(db, attempt['id'], key=request_key)
        if stored is not None:
            return _present(stored, token, True)
    if sequence is not None:
        stored = _stored(db, attempt['id'], number=sequence)
        if stored is not None:
            return _present(stored, token, True)

    if attempt['outcome'] != 'open' or item['state'] != 'leased' or attempt['expires'] <= now:
        raise conflict('租约已处置或超时，不能续租')

    rule = policy.read(db, item['channel'], item['policy'])
    cap = attempt['budget_expires']
    if cap is None:  # upgraded v1 database with an attempt that predates budgets
        cap = attempt['started'] + default_budget_seconds(rule)
        db.execute('UPDATE attempts SET budget_expires=? WHERE id=?', (cap, attempt['id']))

    target = _pick(raw, TARGET_FIELDS)
    if target is not None:
        if isinstance(target, bool) or not isinstance(target, (int, float)):
            raise inputs.invalid('续租截止时刻须为 Unix 秒数值')
        candidate = float(target)
        extension = candidate - attempt['expires']
        if extension <= 0:
            raise conflict('续租只能延长尚未到期的租约')
    else:
        given_seconds = _pick(raw, SECONDS_FIELDS)
        default_step = attempt['lease_step'] or rule['lease_seconds']
        extension = (default_step if given_seconds is None
                     else inputs.integer(given_seconds, EXTEND_MIN, EXTEND_MAX, '续租秒数'))
        candidate = attempt['expires'] + extension

    count = attempt['renew_count'] + 1
    if candidate > cap:
        # A keyless resend of the immediately previous renewal asks for the
        # same extension that is already recorded; answer with that receipt
        # instead of extending twice or reporting an opaque budget failure.
        if request_key is None and sequence is None and target is None and attempt['renew_count']:
            last = db.execute('''SELECT receipt FROM lease_renewals
                                 WHERE attempt_id=? ORDER BY number DESC LIMIT 1''',
                              (attempt['id'],)).fetchone()
            if last is not None:
                stored = codec.unpack(last['receipt'])
                if abs(stored.get('extended_by', 0) - extension) < 1e-9:
                    return _present(stored, token, True)
        raise _budget_conflict(cap, attempt['expires'])

    updated = db.execute('''UPDATE attempts SET expires=?, renew_count=?
                            WHERE id=? AND outcome='open' AND expires=?''',
                         (candidate, count, attempt['id'], attempt['expires']))
    if updated.rowcount != 1:
        raise conflict('租约状态已被回收或处置，请重新领取')

    budget_seconds = cap - attempt['started']
    result = dict(delivery_id=key, attempt=attempt['number'], state='leased', outcome='renewed',
                  previous_expires=attempt['expires'], expires=candidate,
                  extended_by=extension, renewals=count,
                  budget_expires=cap, budget_seconds=budget_seconds, budget=budget_seconds,
                  max_expires=cap, remaining_seconds=cap - candidate,
                  lease=dict(id=attempt['id'], number=attempt['number'], expires=candidate,
                             budget_expires=cap, max_expires=cap, renewals=count))
    db.execute('''INSERT INTO lease_renewals(attempt_id,number,seconds,from_expires,to_expires,at,
                                             request_key,receipt)
                  VALUES (?,?,?,?,?,?,?,?)''',
               (attempt['id'], count, extension, attempt['expires'], candidate, now,
                request_key, codec.pack(result)))
    journal.append(db, item['channel'], now, 'renewed',
                   {k: v for k, v in result.items() if k != 'lease'} | {'renewal': count},
                   item['message_id'])
    return _present(codec.unpack(codec.pack(result)), token, False)


def _budget_conflict(cap, current_expires):
    err = conflict('续租累计时长不能超过首次领取确定的总预算')
    err.details = {'reason_code': 'lease_budget_exceeded', 'budget_expires': cap,
                   'remaining_seconds': max(0, cap - current_expires)}
    return err


def history(db, attempt_ids):
    if not attempt_ids:
        return {}
    marks = ','.join('?' for _ in attempt_ids)
    rows = db.execute(f'''SELECT attempt_id,number,seconds,from_expires,to_expires,at
                          FROM lease_renewals WHERE attempt_id IN ({marks}) ORDER BY id''',
                      tuple(attempt_ids)).fetchall()
    grouped = {}
    for row in rows:
        entry = dict(row)
        attempt_id = entry.pop('attempt_id')
        grouped.setdefault(attempt_id, []).append(entry)
    return grouped
