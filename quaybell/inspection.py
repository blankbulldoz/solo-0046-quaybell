from . import attempts, delivery, journal, publication, redrive


def trace(db, message_id, now=None):
    message = publication.get(db, message_id)
    items = delivery.for_message(db, message_id)
    sources = {item['id']: item for item in items}
    deliveries = []
    for item in items:
        view = item | {'attempts': attempts.for_delivery(db, item['id'])}
        source = sources.get(item['parent_id'])
        if source is not None and now is not None:
            view['redrive'] = redrive.detail(db, source, source['id'], item['id'], now)
        deliveries.append(view)
    return {'message': message, 'deliveries': deliveries, 'events': journal.events_for(db, message_id)}


def recent(db, channel, limit=30):
    rows = db.execute('''SELECT id,producer,dedup_key,lane,policy,published,available
                         FROM messages WHERE channel=? ORDER BY id DESC LIMIT ?''', (channel, limit))
    return [dict(row) for row in rows]
