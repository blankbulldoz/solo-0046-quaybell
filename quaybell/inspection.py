from . import attempts, comparison as _comparison, delivery, journal, publication


def trace(db, message_id):
    message = publication.get(db, message_id)
    deliveries = [item | {'attempts': attempts.for_delivery(db, item['id'])}
                  for item in delivery.for_message(db, message_id)]
    return {'message': message, 'deliveries': deliveries, 'events': journal.events_for(db, message_id)}


def compare(db, message_id, first, second, now=None):
    """两组对照读模型；保留在 inspection 命名空间下便于既有调查入口调用。"""
    if now is None:
        from time import time
        now = time()
    return _comparison.snapshot(db, message_id, first, second, now)


def recent(db, channel, limit=30):
    rows = db.execute('''SELECT id,producer,dedup_key,lane,policy,published,available
                         FROM messages WHERE channel=? ORDER BY id DESC LIMIT ?''', (channel, limit))
    return [dict(row) for row in rows]
