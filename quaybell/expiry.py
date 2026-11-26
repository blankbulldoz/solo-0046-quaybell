from . import attempts, codec, delivery, inputs, outcomes
from .errors import conflict

# 请求里“观测上界”和“幂等标识”的常见命名都接受，避免值守脚本因别名被 400 挡住。
BOUND_FIELDS = ('upper_bound', 'through', 'until', 'as_of', 'horizon', 'observed_until')
KEY_FIELDS = ('request_key', 'idempotency_key', 'request_id', 'client_token', 'key')
# 分段游标类字段当前不是必需：开放尝试筛选本身就是游标，列出它们仅为兼容各类调用方。
CURSOR_FIELDS = ('cursor', 'after', 'segment', 'segment_id', 'sweep_id')
OPTIONAL_FIELDS = ('limit', *BOUND_FIELDS, *KEY_FIELDS, *CURSOR_FIELDS)


def reap_segment(db, now, limit, group_id=None, upper_bound=None):
    """回收一段到期尝试，返回 (尝试行, 处置结果) 对，顺序即截止时间顺序。"""
    pairs = []
    for attempt in attempts.expired(db, now, limit, group_id, upper_bound):
        item = delivery.get(db, attempt['delivery_id'])
        result = outcomes.apply(db, item, attempt, 'expired', '租约超时', now)
        pairs.append((attempt, result))
    return pairs


def reap(db, now, limit, group_id=None, upper_bound=None):
    return [result for _, result in reap_segment(db, now, limit, group_id, upper_bound)]


def current_bound(db, now):
    """当下所有已到期开放尝试中的最大原截止时间，即新的观测上界。"""
    return db.execute("SELECT MAX(expires) AS bound FROM attempts WHERE outcome='open' AND expires<=?",
                      (now,)).fetchone()['bound']


def _range(lo, hi):
    if lo is None:
        return None
    # 同时给出完整键名和短键名，方便不同阅读习惯的调用方直接取值。
    return {'from_expires': lo, 'to_expires': hi, 'from': lo, 'to': hi}


def _advanced(db, bound):
    """已经走过的截止范围，包含领取时组内回收等非 /sweep 路径留下的 expired 尝试。"""
    if bound is None:
        row = db.execute("""SELECT COUNT(*) AS count, MIN(expires) AS lo, MAX(expires) AS hi
                            FROM attempts WHERE outcome='expired'""").fetchone()
    else:
        row = db.execute("""SELECT COUNT(*) AS count, MIN(expires) AS lo, MAX(expires) AS hi
                            FROM attempts WHERE outcome='expired' AND expires<=?""",
                         (bound,)).fetchone()
    count = row['count'] or 0
    span = _range(row['lo'], row['hi'])
    return {'count': count, 'from_expires': row['lo'], 'to_expires': row['hi'],
            'range': span}


def describe(db, now, bound=None):
    """单个观测时间上的回收进度：上界内剩余与上界外新到期严格分开。"""
    expired = db.execute("""SELECT
            SUM(CASE WHEN outcome='open' AND expires<=? THEN 1 ELSE 0 END) AS expired,
            SUM(CASE WHEN outcome='open' AND expires>? THEN 1 ELSE 0 END) AS pending
        FROM attempts""", (now, now)).fetchone()
    expired_count = expired['expired'] or 0
    pending = expired['pending'] or 0
    if bound is None:
        upper_bound = current_bound(db, now)
        remaining, newly_expired = expired_count, 0
    else:
        upper_bound = bound
        cap = min(bound, now)
        remaining = db.execute(
            "SELECT COUNT(*) AS n FROM attempts WHERE outcome='open' AND expires<=?",
            (cap,)).fetchone()['n']
        newly_expired = db.execute(
            "SELECT COUNT(*) AS n FROM attempts WHERE outcome='open' AND expires>? AND expires<=?",
            (bound, now)).fetchone()['n']
    advanced = _advanced(db, upper_bound)
    progress = {
        'upper_bound': upper_bound, 'remaining': remaining, 'newly_expired': newly_expired,
        'expired': expired_count, 'pending': pending, 'caught_up': remaining == 0,
        'advanced_to': advanced['to_expires'],
    }
    return {
        'observed_at': now,
        'upper_bound': upper_bound,
        'bound': upper_bound,
        'horizon': upper_bound,
        'current_upper_bound': current_bound(db, now),
        'expired': expired_count,
        'remaining': remaining,
        'remaining_count': remaining,
        'newly_expired': newly_expired,
        'new_expired': newly_expired,
        'pending': pending,
        'caught_up': remaining == 0,
        'complete': remaining == 0,
        'progress': progress,
        'advanced': advanced,
        'advanced_range': advanced['range'],
        'advanced_from': advanced['from_expires'],
        'advanced_to': advanced['to_expires'],
    }


def observe(db, now, raw=None):
    raw = raw or {}
    bound = _bound(raw) if any(field in raw for field in BOUND_FIELDS) else None
    return describe(db, now, bound)


def _bound(raw):
    for field in BOUND_FIELDS:
        if field in raw and raw[field] is not None:
            value = raw[field]
            if isinstance(value, str):  # GET 查询串里的上界天然是字符串
                try:
                    value = float(value)
                except ValueError:
                    raise inputs.invalid('观测上界须为 Unix 秒') from None
            return inputs.moment(value, '观测上界')
    return None


def _key(raw):
    for field in KEY_FIELDS:
        if field in raw and raw[field] is not None:
            return inputs.token(raw[field], '回收请求标识')
    return None


def _segment_range(pairs):
    if not pairs:
        return None
    deadlines = [attempt['expires'] for attempt, _ in pairs]
    return _range(min(deadlines), max(deadlines))


def sweep(db, raw, now):
    inputs.require(raw, (), OPTIONAL_FIELDS)
    limit = inputs.integer(raw.get('limit', 50), 1, 100, '回收上限')
    request_key = _key(raw)
    identity = codec.fingerprint(codec.pack(
        {'limit': limit, 'upper_bound': _bound(raw), 'cursor': raw.get('cursor') or raw.get('after')}))

    if request_key is not None:
        saved = db.execute('SELECT request_digest,response FROM sweep_receipts WHERE request_key=?',
                           (request_key,)).fetchone()
        if saved is not None:
            if saved['request_digest'] != identity:
                raise conflict('回收请求标识已用于不同的回收参数')
            return codec.unpack(saved['response']) | {'repeated': True}

    explicit_bound = _bound(raw)
    # 未显式给上界时，以回收前看到的最大到期截止作为本轮观测上界。
    bound = explicit_bound if explicit_bound is not None else current_bound(db, now)
    pairs = reap_segment(db, now, limit, upper_bound=bound)
    segment_range = _segment_range(pairs)
    next_cursor = ({'expires': pairs[-1][0]['expires'], 'id': pairs[-1][0]['id']}
                   if pairs else None)
    report = describe(db, now, bound) | {
        'limit': limit,
        'recovered': [result for _, result in pairs],
        'segment_count': len(pairs),
        'segment_range': segment_range,
        'range': segment_range,
        'next_cursor': next_cursor,
        'cursor': next_cursor,
    }
    if request_key is not None:
        db.execute('''INSERT INTO sweep_receipts(request_key,request_digest,response,created)
                      VALUES (?,?,?,?)''',
                   (request_key, identity, codec.pack(report), now))
    return report | {'repeated': False}
