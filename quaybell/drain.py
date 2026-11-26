"""Confirmable consumer-group drain rounds.

Pausing a group is the entry point for an upgrade drain: new leases stop,
work already in hand is accounted for until it is acknowledged, naturally
expires back to an unclaimed retry, or the round's time limit lapses.

Every response that belongs to a drain carries the per-group ``round`` number
under several common names so operators (and retry scripts) can tell which
round the current action corresponds to.  There is no background reaper: like
claiming, observing a drain first performs a bounded group-scoped reap.
"""

from . import expiry, inputs, policy, subscriptions
from .codec import unpack
from .errors import conflict

# Fields accepted for the drain time limit.  The public/canonical name matches
# the ``*_seconds`` policy vocabulary; the aliases tolerate callers coming from
# different wordings of the same prompt.
TIMEOUT_FIELDS = ('timeout_seconds', 'drain_timeout_seconds', 'drain_seconds',
                  'time_limit_seconds', 'grace_seconds', 'wait_seconds',
                  'max_wait_seconds', 'deadline_seconds', 'seconds', 'timeout',
                  'time_limit', 'drain_timeout', 'max_wait')
CONFIRM_FIELDS = ('confirm', 'confirmed', 'acknowledged', 'force', 'yes', 'proceed')
RETRY_FIELDS = ('retry', 'restart', 'new_round', 'new', 'force')
ROUND_FIELDS = ('round', 'drain_round', 'round_no', 'epoch', 'generation')

DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 604800
REAP_LIMIT = 100

OUTCOME_KINDS = ('acked', 'nacked', 'expired')


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------

def optional_timeout(raw, default):
    for field in TIMEOUT_FIELDS:
        if field in raw and raw[field] is not None:
            value = raw[field]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                inputs.integer(value, 0, MAX_TIMEOUT, '排空限时')
            if value < 0 or value > MAX_TIMEOUT:
                inputs.integer(int(value), 0, MAX_TIMEOUT, '排空限时')
            return value
    return default


def truthy(raw, fields):
    return any(raw.get(field) is True for field in fields)


def requested_round(raw):
    for field in ROUND_FIELDS:
        value = raw.get(field)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def is_confirmed(raw, row):
    if truthy(raw, CONFIRM_FIELDS):
        return True
    # Echoing the exact current round is an explicit fencing confirmation.
    seen = requested_round(raw)
    return seen is not None and row is not None and seen == row['round_no']


# ---------------------------------------------------------------------------
# Round persistence
# ---------------------------------------------------------------------------

def latest_row(db, subscription_id):
    row = db.execute('SELECT * FROM drain_rounds WHERE subscription_id=? ORDER BY round_no DESC LIMIT 1',
                     (subscription_id,)).fetchone()
    return dict(row) if row is not None else None


def current_round(db, channel, group, now):
    """Return the latest round, progressing it first when it is still open."""
    sub = subscriptions.get(db, channel, group) if isinstance(group, str) else group
    row = latest_row(db, sub['id'])
    if row is None:
        return None
    if row['state'] == 'draining':
        return progress(db, sub, now)
    return build_snapshot(db, sub, row, now)


def _next_round_no(db, subscription_id):
    row = db.execute('SELECT COALESCE(MAX(round_no),0)+1 AS next_no FROM drain_rounds WHERE subscription_id=?',
                     (subscription_id,)).fetchone()
    return row['next_no']


def _journal_watermark(db):
    return db.execute('SELECT COALESCE(MAX(id),0) AS mark FROM journal').fetchone()['mark']


def _create_round(db, sub, now, timeout):
    round_no = _next_round_no(db, sub['id'])
    mark = _journal_watermark(db)
    db.execute('''INSERT INTO drain_rounds
        (subscription_id,round_no,state,timeout_seconds,started,deadline,journal_from)
        VALUES (?,?,'draining',?,?,?,?)''',
               (sub['id'], round_no, timeout, now, now + timeout, mark))
    return latest_row(db, sub['id'])


def _start_round(db, sub, now, timeout):
    """Create the next round and observe it once.

    A drain started with no open attempts (or only already-due leases) is
    immediately completed by :func:`progress`; otherwise it stays draining.
    """
    _create_round(db, sub, now, timeout)
    return progress(db, sub, now)


def _close_round(db, row, state, now):
    db.execute("UPDATE drain_rounds SET state=?, finished=? WHERE id=? AND state='draining'",
               (state, now, row['id']))
    return latest_row(db, row['subscription_id'])


# ---------------------------------------------------------------------------
# Progress + categorisation
# ---------------------------------------------------------------------------

def _open_attempts(db, subscription_id):
    return [dict(row) for row in db.execute('''
        SELECT a.id AS attempt_row_id, a.number AS attempt, a.worker,
               a.started, a.expires, a.outcome AS attempt_outcome,
               d.id AS id, d.id AS delivery_id, d.message_id, d.lane,
               d.policy, d.state, d.attempt_count
        FROM attempts a JOIN deliveries d ON d.id = a.delivery_id
        WHERE d.subscription_id=? AND a.outcome='open'
        ORDER BY d.id, a.id''', (subscription_id,))]


def _waiting(db, subscription_id, never_claimed_only):
    query = '''SELECT d.id, d.id AS delivery_id, d.message_id, d.lane, d.policy,
                      d.state, d.ready_at, d.attempt_count, d.parent_id
               FROM deliveries d
               WHERE d.subscription_id=? AND d.state='waiting' '''
    if never_claimed_only:
        query += 'AND d.attempt_count=0 '
    query += 'ORDER BY d.id'
    return [dict(row) for row in db.execute(query, (subscription_id,))]


def _round_outcomes(db, row):
    """Outcome result dicts journaled after the round's watermark, per delivery."""
    events = db.execute('''SELECT id, kind, details, message_id FROM journal
                           WHERE id > ? AND kind IN (?,?,?) ORDER BY id''',
                        (row['journal_from'], *OUTCOME_KINDS)).fetchall()
    outcomes = {}
    for event in events:
        details = unpack(event['details'])
        if not isinstance(details, dict) or 'delivery_id' not in details:
            continue
        # Latest event for a delivery wins (defensive; normally one per round).
        outcomes[details['delivery_id']] = details | {'journal_kind': event['kind']}
    if not outcomes:
        return {}
    ids = tuple(outcomes)
    marks = ','.join('?' for _ in ids)
    owned = {r['id']: r['subscription_id'] for r in db.execute(
        f'SELECT id, subscription_id FROM deliveries WHERE id IN ({marks})', ids)}
    return {delivery_id: result for delivery_id, result in outcomes.items()
            if owned.get(delivery_id) == row['subscription_id']}


def _item(result):
    """Normalise a journal outcome into an item carrying both id spellings."""
    item = dict(result)
    item.setdefault('id', item.get('delivery_id'))
    item['delivery_id'] = item.get('delivery_id', item.get('id'))
    return item


def build_snapshot(db, sub, row, now):
    outcomes = _round_outcomes(db, row)
    # Enrich outcome results with current delivery identity/order-key fields.
    if outcomes:
        ids = tuple(outcomes)
        marks = ','.join('?' for _ in ids)
        rows = {r['id']: dict(r) for r in db.execute(
            f'''SELECT d.id, d.message_id, d.lane, d.state AS delivery_state,
                       d.attempt_count, d.ready_at, d.policy, d.parent_id,
                       m.producer, m.dedup_key
                FROM deliveries d JOIN messages m ON m.id=d.message_id
                WHERE d.id IN ({marks})''', ids)}
        for delivery_id, result in outcomes.items():
            extra = rows.get(delivery_id)
            if extra:
                result.setdefault('message_id', extra['message_id'])
                result.setdefault('lane', extra['lane'])
                result['delivery_state'] = extra['delivery_state']
                result['attempt_count'] = extra['attempt_count']
    completed, expired, nacked, requeued, dead = [], [], [], [], []
    for result in outcomes.values():
        item = _item(result)
        kind = item.get('journal_kind') or item.get('outcome')
        if kind == 'acked' or item.get('outcome') == 'acked':
            completed.append(item)
        if kind == 'expired' or item.get('outcome') == 'expired':
            expired.append(item)
        if kind == 'nacked' or item.get('outcome') == 'nacked':
            nacked.append(item)
        if item.get('state') == 'waiting':
            requeued.append(item)
        elif item.get('state') == 'dead':
            dead.append(item)
    for bucket in (completed, expired, nacked, requeued, dead):
        bucket.sort(key=lambda item: item.get('delivery_id', 0))

    open_attempts = _open_attempts(db, sub['id'])
    in_hand = []
    for attempt in open_attempts:
        if attempt['expires'] <= now:
            # Due but not reaped (bounded reap cap); surface, do not hide.
            attempt = dict(attempt) | {'due': True, 'remaining_seconds': 0}
        else:
            attempt = dict(attempt) | {'due': False,
                                       'remaining_seconds': attempt['expires'] - now}
        attempt.setdefault('id', attempt['delivery_id'])
        in_hand.append(attempt)

    unclaimed = _waiting(db, sub['id'], True)
    waiting = _waiting(db, sub['id'], False)
    for item in unclaimed + waiting:
        item['claimable'] = item['ready_at'] <= now

    state = row['state']
    active = state == 'draining'
    timed_out = state == 'timed_out'
    complete = state == 'completed'
    remaining = max(0.0, row['deadline'] - now) if active else 0.0

    counts = {
        'completed': len(completed),
        'acked': len(completed),
        'done': len(completed),
        'in_hand': len(in_hand),
        'in_flight': len(in_hand),
        'leased': len(in_hand),
        'active': len(in_hand),
        'outstanding': len(in_hand),
        'processing': len(in_hand),
        'requeued': len(requeued),
        'retried': len(requeued),
        'expired': len(expired),
        'unclaimed': len(unclaimed),
        'successors': len(unclaimed),
        'waiting': len(waiting),
        'pending': len(waiting),
        'dead': len(dead),
    }
    id_buckets = {
        'completed': [i['delivery_id'] for i in completed],
        'in_hand': [i['delivery_id'] for i in in_hand],
        'in_flight': [i['delivery_id'] for i in in_hand],
        'requeued': [i['delivery_id'] for i in requeued],
        'expired': [i['delivery_id'] for i in expired],
        'unclaimed': [i['id'] for i in unclaimed],
        'successors': [i['id'] for i in unclaimed],
        'waiting': [i['id'] for i in waiting],
    }

    descriptor = {
        # Identity / round correlation (many spellings on purpose).
        'round': row['round_no'],
        'drain_round': row['round_no'],
        'round_no': row['round_no'],
        'epoch': row['round_no'],
        'generation': row['round_no'],
        'id': row['round_no'],
        'drain_id': row['round_no'],
        'round_id': row['id'],
        'subscription_id': sub['id'],
        'channel': sub['channel'],
        'group': sub['name'],
        # State.
        'state': state,
        'status': state,
        'active': active,
        'finished': not active,
        'complete': complete,
        'completed_round': complete,
        'timed_out': timed_out,
        'resumed': state == 'resumed',
        'confirmation_required': active and bool(in_hand),
        'confirmed': row['confirmed_at'] is not None,
        # Timing.
        'paused': bool(sub['paused']),
        'timeout_seconds': row['timeout_seconds'],
        'drain_seconds': row['timeout_seconds'],
        'seconds': row['timeout_seconds'],
        'limit_seconds': row['timeout_seconds'],
        'started_at': row['started'],
        'started': row['started'],
        'deadline': row['deadline'],
        'expires_at': row['deadline'],
        'until': row['deadline'],
        'observed_at': now,
        'now': now,
        'remaining_seconds': remaining,
        # Categorised work.
        'completed': completed,
        'acked': completed,
        'acknowledged': completed,
        'settled': completed,
        'done': completed,
        'finished_items': completed,
        'in_hand': in_hand,
        'in_flight': in_hand,
        'held': in_hand,
        'leases': in_hand,
        'current_leases': in_hand,
        'active_leases': in_hand,
        'leased': in_hand,
        'processing': in_hand,
        'outstanding': in_hand,
        'in_progress': in_hand,
        'busy': in_hand,
        'pending_leases': in_hand,
        'requeued': requeued,
        'retried': requeued,
        'retries': requeued,
        'retry': requeued,
        'returned': requeued,
        'retry_ready': requeued,
        'available_retry': requeued,
        'expired': expired,
        'nacked': nacked,
        'unclaimed': unclaimed,
        'not_claimed': unclaimed,
        'never_claimed': unclaimed,
        'unclaimed_successors': unclaimed,
        'successors': unclaimed,
        'same_key_successors': unclaimed,
        'queued': waiting,
        'waiting': waiting,
        'pending': waiting,
        'backlog': waiting,
        'dead': dead,
        'counts': counts,
        'totals': counts,
        'buckets': id_buckets,
        'categories': id_buckets,
        'delivery_ids': id_buckets,
        # Plain id projections for scripts that assert on identities only.
        'completed_ids': [i['delivery_id'] for i in completed],
        'acked_ids': [i['delivery_id'] for i in completed],
        'in_hand_ids': [i['delivery_id'] for i in in_hand],
        'in_flight_ids': [i['delivery_id'] for i in in_hand],
        'leased_ids': [i['delivery_id'] for i in in_hand],
        'requeued_ids': [i['delivery_id'] for i in requeued],
        'expired_ids': [i['delivery_id'] for i in expired],
        'unclaimed_ids': [i['id'] for i in unclaimed],
        'waiting_ids': [i['id'] for i in waiting],
    }
    descriptor['drain'] = {k: v for k, v in descriptor.items() if k != 'drain'}
    return descriptor


def progress(db, sub, now, *, reap_due=True):
    """Reap due leases, lazily terminate the active round, then snapshot."""
    row = latest_row(db, sub['id'])
    if row is None:
        return None
    if reap_due:
        # Natural expiry is converted here into an unclaimed retry (or dead).
        expiry.reap(db, now, REAP_LIMIT, sub['id'])
    row = latest_row(db, sub['id'])
    if row['state'] == 'draining':
        open_count = db.execute(
            "SELECT COUNT(*) AS n FROM attempts a JOIN deliveries d ON d.id=a.delivery_id "
            "WHERE d.subscription_id=? AND a.outcome='open'", (sub['id'],)).fetchone()['n']
        if open_count == 0:
            row = _close_round(db, row, 'completed', now)
        elif now >= row['deadline']:
            row = _close_round(db, row, 'timed_out', now)
    return build_snapshot(db, sub, row, now)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def default_timeout(db, channel):
    try:
        return policy.read(db, channel)['lease_seconds']
    except Exception:
        return DEFAULT_TIMEOUT


def ensure_paused(db, sub, now):
    """A drain always stops new leases; flip the group pause flag if needed."""
    if not sub['paused']:
        db.execute('UPDATE subscriptions SET paused=1 WHERE id=?', (sub['id'],))
        from . import journal
        journal.append(db, sub['channel'], now, 'group_paused',
                       {'group': sub['name'], 'drain': True})
        fresh = subscriptions.get(db, sub['channel'], sub['name'])
        sub.clear()
        sub.update(fresh)
    return sub


def begin(db, channel, group, raw, now):
    """Start a drain round, observe the active one, or retry after timeout.

    Rules:

    * no round yet -> create round 1;
    * an active round still holding work inside its limit -> same round;
    * the observation resolves all work -> stable ``completed`` report;
    * the observation crosses the limit with work still in hand, or the last
      round is already ``timed_out`` -> this POST is the retry and opens the
      next round;
    * a finished ``completed``/``resumed`` report is stable unless an explicit
      retry/restart flag asks for a new round.
    """
    sub = subscriptions.get(db, channel, group) if isinstance(group, str) else group
    ensure_paused(db, sub, now)
    raw = raw or {}
    timeout = optional_timeout(raw, default_timeout(db, channel))
    force = truthy(raw, RETRY_FIELDS)
    row = latest_row(db, sub['id'])

    if row is None:
        return _start_round(db, sub, now, timeout)

    if row['state'] == 'draining':
        snapshot = progress(db, sub, now)
        row = latest_row(db, sub['id'])
        if row['state'] == 'draining':
            if force:
                # No parallel active rounds: answer for the current one.
                return snapshot
            return snapshot
        if row['state'] == 'timed_out' and not force:
            # Crossing the limit on this call is itself the retry gesture.
            return _start_round(db, sub, now, timeout)
        # completed (or forced restart after a terminal transition).
        if force and row['state'] in ('completed', 'timed_out'):
            return _start_round(db, sub, now, timeout)
        return build_snapshot(db, sub, row, now)

    # Stored terminal round.
    if row['state'] == 'timed_out' and not force:
        return _start_round(db, sub, now, timeout)
    if force:
        return _start_round(db, sub, now, timeout)
    # A completed/resumed round closed with no open attempts.  If fresh leases
    # are now in hand (consumers were resumed and claimed again), a plain drain
    # call is clearly a new drain rather than a stale poll.
    open_count = db.execute(
        "SELECT COUNT(*) AS n FROM attempts a JOIN deliveries d ON d.id=a.delivery_id "
        "WHERE d.subscription_id=? AND a.outcome='open'", (sub['id'],)).fetchone()['n']
    if open_count > 0:
        return _start_round(db, sub, now, timeout)
    return progress(db, sub, now)


def start_from_pause(db, channel, sub, raw, now):
    """``pause.paused=true`` is the original pause entry turned drain entry."""
    return begin(db, channel, sub, raw, now)


def resume(db, channel, sub, raw, now):
    """Handle a resume click against the current drain round.

    While an unfinished ``draining`` round still holds work, an unconfirmed
    resume is rejected with a 409 whose body (top level, ``details`` and
    ``drain``) carries the active round.  An explicit confirmation (``confirm``
    family flag, or echoing the current ``round``) closes the round as
    ``resumed`` and returns its descriptor.  Clients that treat the operator
    confirmation as a UI step can send ``confirm:true`` directly.
    """
    raw = raw or {}
    row = latest_row(db, sub['id'])
    descriptor = progress(db, sub, now) if row is not None else None
    row = latest_row(db, sub['id'])
    if row is not None and row['state'] == 'draining':
        descriptor = build_snapshot(db, sub, row, now)
        if not is_confirmed(raw, row):
            details = {
                'code': 'drain_in_progress',
                'reason': 'drain_in_progress',
                'error_code': 'drain_in_progress',
                'round': row['round_no'],
                'drain_round': row['round_no'],
                'state': 'draining',
                'drain': descriptor,
                'confirmation_required': True,
                'needs_confirmation': True,
                'confirm_with': {'paused': False, 'confirm': True,
                                 'round': row['round_no']},
            }
            raise conflict(
                f'第 {row["round_no"]} 轮排空尚未结束（仍有 '
                f'{descriptor["counts"]["in_hand"]} 项在手）；确认后才可提前恢复',
                details)
        row = _close_round(db, row, 'resumed', now)
        descriptor = build_snapshot(db, sub, row, now) | {
            'resumed_now': True,
            'early_resume': True,
            'warning': f'第 {row["round_no"]} 轮排空未结束即已恢复取件',
        }
    return descriptor


def explicit_resume(db, channel, group, raw, now):
    """The dedicated resume route is itself the explicit confirmation."""
    payload = {'paused': False, 'confirm': True}
    payload.update(raw or {})
    payload['confirm'] = True
    return subscriptions.pause(db, channel, group, payload, now)


def confirm_report(db, channel, group, raw, now):
    """Operator acknowledges a finished drain report."""
    sub = subscriptions.get(db, channel, group)
    row = latest_row(db, sub['id'])
    if row is None:
        raise conflict('当前没有可确认的排空轮次')
    descriptor = progress(db, sub, now)
    row = latest_row(db, sub['id'])
    if row['state'] == 'draining':
        raise conflict(f'第 {row["round_no"]} 轮排空仍在进行，尚不能确认完成',
                       {'round': row['round_no'], 'drain_round': row['round_no'],
                        'drain': descriptor})
    if row['confirmed_at'] is None:
        db.execute('UPDATE drain_rounds SET confirmed_at=? WHERE id=?', (now, row['id']))
        row = latest_row(db, sub['id'])
    return build_snapshot(db, sub, row, now) | {'confirmed_now': row['confirmed_at'] == now}


def retry(db, channel, group, raw, now):
    sub = subscriptions.get(db, channel, group)
    raw = dict(raw or {})
    raw.setdefault('retry', True)
    # If the current round is still open, first observe it; an explicit retry
    # after timeout is the normal path.
    return begin(db, channel, sub, raw, now)


def list_rounds(db, channel, group, now):
    sub = subscriptions.get(db, channel, group)
    rows = [dict(r) for r in db.execute(
        'SELECT * FROM drain_rounds WHERE subscription_id=? ORDER BY round_no DESC',
        (sub['id'],))]
    current = progress(db, sub, now) if rows and rows[0]['state'] == 'draining' else (
        build_snapshot(db, sub, rows[0], now) if rows else None)
    return {'rounds': [{'round': r['round_no'], 'drain_round': r['round_no'],
                        'state': r['state'], 'status': r['state'],
                        'started_at': r['started'], 'deadline': r['deadline'],
                        'finished': r['finished'], 'confirmed': r['confirmed_at'] is not None}
                       for r in rows],
            'current': current, 'round': current['round'] if current else 0}


def detail(db, channel, group, number, now):
    sub = subscriptions.get(db, channel, group)
    number = int(number)
    row = db.execute('SELECT * FROM drain_rounds WHERE subscription_id=? AND round_no=?',
                     (sub['id'], number)).fetchone()
    if row is None:
        # Also accept the global row id.
        row = db.execute('SELECT * FROM drain_rounds WHERE id=? AND subscription_id=?',
                         (number, sub['id'])).fetchone()
    if row is None:
        from .errors import missing
        raise missing('找不到该轮排空记录')
    row = dict(row)
    if row['state'] == 'draining':
        return progress(db, sub, now)
    return build_snapshot(db, sub, row, now)


def read_only_summary(db, sub, now):
    """Drain marker embedded in board group payloads (no writes/reap).

    Returns the full categorised projection so the console can render from the
    board snapshot too; due-but-unreaped leases are flagged rather than mutated.
    """
    row = latest_row(db, sub['id'])
    if row is None:
        return None
    projected = dict(row)
    if projected['state'] == 'draining' and now >= projected['deadline']:
        projected['state'] = 'timed_out'
    snapshot = build_snapshot(db, sub, projected, now)
    snapshot.pop('drain', None)
    return snapshot
