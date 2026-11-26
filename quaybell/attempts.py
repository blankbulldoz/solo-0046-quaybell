import secrets

from . import codec
from .errors import conflict

PUBLIC = 'id,delivery_id,number,worker,started,expires,budget_expires,renew_count,outcome,finished,reason'


def open_attempt(db, delivery, worker, now, seconds, budget_expires=None, step_seconds=None):
    token = secrets.token_urlsafe(24)
    number = delivery['attempt_count'] + 1
    if budget_expires is None:
        budget_expires = now + seconds * 3  # two default renewals when caller sets no budget
    step_seconds = seconds if step_seconds is None else step_seconds
    key = db.execute('''INSERT INTO attempts(delivery_id,number,token_digest,worker,started,expires,
                                             budget_expires,lease_step,renew_count,outcome)
                        VALUES (?,?,?,?,?,?,?,?,0,'open')''',
                     (delivery['id'], number, codec.fingerprint(token), worker, now, now + seconds,
                      budget_expires, step_seconds)).lastrowid
    return dict(id=key, token=token, number=number, expires=now + seconds,
                budget_expires=budget_expires, renew_count=0)


def by_token(db, delivery_id, token):
    row = db.execute('SELECT * FROM attempts WHERE delivery_id=? AND token_digest=?',
                     (delivery_id, codec.fingerprint(token))).fetchone()
    if row is None:
        raise conflict('该凭据不属于此投递')
    return dict(row)


def expired(db, now, limit, subscription_id=None):
    query = """SELECT a.* FROM attempts a JOIN deliveries d ON d.id=a.delivery_id
               WHERE a.outcome='open' AND a.expires<=?"""
    values = [now]
    if subscription_id is not None:
        query += ' AND d.subscription_id=?'
        values.append(subscription_id)
    return [dict(row) for row in db.execute(query + ' ORDER BY a.expires,a.id LIMIT ?', (*values, limit))]


def finish(db, attempt, outcome, reason, now, receipt):
    db.execute('UPDATE attempts SET outcome=?,reason=?,finished=?,receipt=? WHERE id=?',
               (outcome, reason, now, codec.pack(receipt), attempt['id']))


def for_delivery(db, key):
    rows = db.execute(f'SELECT {PUBLIC} FROM attempts WHERE delivery_id=? ORDER BY number', (key,))
    result = []
    for row in rows:
        item = dict(row)
        item['renewals'] = item.get('renew_count', 0)
        result.append(item)
    return result
