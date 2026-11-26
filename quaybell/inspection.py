from . import attempts, correction, delivery, journal, publication


def trace(db, message_id):
    message = publication.get(db, message_id)
    deliveries = [item | {'attempts': attempts.for_delivery(db, item['id'])}
                  for item in delivery.for_message(db, message_id)]
    facts = {'message': message, 'deliveries': deliveries, 'events': journal.events_for(db, message_id),
             'correction': correction.for_message(db, message_id),
             'corrections': correction.as_source(db, message_id)}
    return facts


def recent(db, channel, limit=30):
    rows = db.execute('''SELECT m.id,m.producer,m.dedup_key,m.lane,m.policy,m.published,m.available,
                                m.origin_delivery_id,c.id AS correction_id
                         FROM messages m LEFT JOIN corrections c ON c.message_id=m.id
                         WHERE m.channel=? ORDER BY m.id DESC LIMIT ?''', (channel, limit))
    return [dict(row) for row in rows]
