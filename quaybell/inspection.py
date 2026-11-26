from . import attempts, delivery, journal, publication, renewal


def trace(db, message_id):
    message = publication.get(db, message_id)
    deliveries = []
    for item in delivery.for_message(db, message_id):
        history = attempts.for_delivery(db, item['id'])
        renewals = renewal.history(db, [row['id'] for row in history])
        for row in history:
            row['renewal_history'] = renewals.get(row['id'], [])
        deliveries.append(item | {'attempts': history})
    return {'message': message, 'deliveries': deliveries, 'events': journal.events_for(db, message_id)}


def recent(db, channel, limit=30):
    rows = db.execute('''SELECT id,producer,dedup_key,lane,policy,published,available
                         FROM messages WHERE channel=? ORDER BY id DESC LIMIT ?''', (channel, limit))
    return [dict(row) for row in rows]
