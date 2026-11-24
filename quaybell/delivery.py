from .errors import missing

DELIVERY = '''SELECT d.*,m.channel,s.name AS group_name FROM deliveries d
              JOIN messages m ON m.id=d.message_id JOIN subscriptions s ON s.id=d.subscription_id'''


def add(db, message_id, group_id, lane, version, ready_at, parent_id=None):
    return db.execute('''INSERT INTO deliveries(message_id,subscription_id,lane,policy,state,ready_at,parent_id)
                         VALUES (?,?,?,?,'waiting',?,?)''',
                      (message_id, group_id, lane, version, ready_at, parent_id)).lastrowid


def get(db, key):
    row = db.execute(DELIVERY + ' WHERE d.id=?', (key,)).fetchone()
    if row is None:
        raise missing('找不到投递记录')
    return dict(row)


def for_message(db, message_id):
    return [dict(row) for row in db.execute(DELIVERY + ' WHERE d.message_id=? ORDER BY d.id',
                                            (message_id,))]


def lease(db, key, number):
    db.execute("UPDATE deliveries SET state='leased',attempt_count=? WHERE id=?", (number, key))


def close(db, key, state, when):
    if state == 'waiting':
        db.execute('UPDATE deliveries SET state=?,ready_at=? WHERE id=?', (state, when, key))
    else:
        db.execute('UPDATE deliveries SET state=?,terminal_at=? WHERE id=?', (state, when, key))
