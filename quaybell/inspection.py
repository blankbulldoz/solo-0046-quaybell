from . import coalescing, publication
from . import attempts, delivery


def trace(db, message_id):
    message = publication.get(db, message_id)
    deliveries = [coalescing.enrich_delivery(db, item | {'attempts': attempts.for_delivery(db, item['id'])})
                  for item in delivery.for_message(db, message_id)]
    return {'message': message, 'deliveries': deliveries, 'events': journal_events(db, message_id)}


def journal_events(db, message_id):
    from . import journal
    return journal.events_for(db, message_id)


def recent(db, channel, limit=30):
    rows = db.execute('''SELECT id,producer,dedup_key,lane,policy,published,available
                         FROM messages WHERE channel=? ORDER BY id DESC LIMIT ?''', (channel, limit))
    return [dict(row) for row in rows]
