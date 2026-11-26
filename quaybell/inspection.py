from . import attempts, delivery, journal, publication, review
from .errors import gone


def trace(db, message_id, now):
    if db.execute('SELECT 1 FROM messages WHERE id=?', (message_id,)).fetchone() is None:
        source = review.pruned_source(db, message_id)
        if source is not None:
            raise gone('消息已按保留期清理，详情不再可用',
                       {'message_id': message_id, 'channel': source['channel'],
                        'pruned_at': source['at']})
    message = publication.get(db, message_id)
    deliveries = [item | {'attempts': attempts.for_delivery(db, item['id'])}
                  for item in delivery.for_message(db, message_id)]
    return {'observed_at': now, 'message': message, 'deliveries': deliveries,
            'events': journal.events_for(db, message_id)}
