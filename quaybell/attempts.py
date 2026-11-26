import secrets

from . import codec, failures
from .errors import conflict

PUBLIC = ('id,delivery_id,number,worker,started,expires,outcome,finished,reason,'
          'classification')


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


def finish(db, attempt, outcome, reason, now, receipt, classification=None):
    db.execute('''UPDATE attempts SET outcome=?,reason=?,finished=?,receipt=?,classification=?
                  WHERE id=?''',
               (outcome, reason, now, codec.pack(receipt), classification, attempt['id']))


def for_delivery(db, key):
    rows = [dict(row) for row in db.execute(
        f'SELECT {PUBLIC} FROM attempts WHERE delivery_id=? ORDER BY number', (key,))]
    for row in rows:
        # 旧记录没有分类列值时，按结果回退，保证历史仍可解释。
        classification = row.get('classification') or (
            failures.TIMEOUT if row['outcome'] == 'expired'
            else failures.FAILED if row['outcome'] == 'nacked' else None)
        row['classification'] = classification
        if classification is not None:
            row.update(failures.class_fields(classification))
    return rows
