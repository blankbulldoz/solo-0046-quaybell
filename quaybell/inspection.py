from . import attempts, clockwork, delivery, journal, publication


def trace(db, message_id):
    message = clockwork.message(publication.get(db, message_id))
    deliveries = delivery.for_message(db, message_id)
    for item in deliveries:
        item['attempts'] = attempts.for_delivery(db, item['id'])
    return {'message': message, 'deliveries': deliveries, 'events': journal.events_for(db, message_id)}


def recent(db, channel, limit=30):
    rows = db.execute('''SELECT id,producer,dedup_key,lane,policy,published,available
                         FROM messages WHERE channel=? ORDER BY id DESC LIMIT ?''', (channel, limit))
    return [clockwork.message(dict(row)) for row in rows]
