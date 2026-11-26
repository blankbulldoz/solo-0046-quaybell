import secrets

from . import codec
from .errors import conflict

# result_digest 是供应商原始报文的摘要，属内部完整性核对字段，不进入任何对外视图。
PUBLIC = ('id,delivery_id,number,worker,started,expires,outcome,finished,reason,'
          'result_rule,result_unit,canonical_amount,result_reason')
RESULT_COLUMNS = ('result_rule', 'result_unit', 'canonical_amount', 'result_digest', 'result_reason')


def open_attempt(db, delivery, worker, now, seconds):
    token = secrets.token_urlsafe(24)
    number = delivery['attempt_count'] + 1
    key = db.execute('''INSERT INTO attempts(delivery_id,number,token_digest,worker,started,expires,outcome)
                        VALUES (?,?,?,?,?,?,'open')''',
                     (delivery['id'], number, codec.fingerprint(token), worker, now, now + seconds)).lastrowid
    return dict(id=key, token=token, number=number, expires=now + seconds)


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


def finish(db, attempt, outcome, reason, now, receipt, result=None):
    if result is None:
        db.execute('UPDATE attempts SET outcome=?,reason=?,finished=?,receipt=? WHERE id=?',
                   (outcome, reason, now, codec.pack(receipt), attempt['id']))
        return
    values = [result[key] for key in RESULT_COLUMNS]
    db.execute('''UPDATE attempts SET outcome=?,reason=?,finished=?,receipt=?,
                  result_rule=?,result_unit=?,canonical_amount=?,result_digest=?,result_reason=? WHERE id=?''',
               (outcome, reason, now, codec.pack(receipt), *values, attempt['id']))


def for_delivery(db, key):
    rows = [dict(row) for row in db.execute(
        f'SELECT {PUBLIC} FROM attempts WHERE delivery_id=? ORDER BY number', (key,))]
    for row in rows:
        if row['result_rule'] is None:
            for column in ('result_rule', 'result_unit', 'canonical_amount', 'result_reason'):
                row.pop(column, None)
    return rows
