from . import attempts, bridging, delivery, journal, publication


def trace(db, message_id):
    message = publication.get(db, message_id)
    deliveries = [item | {'attempts': attempts.for_delivery(db, item['id'])}
                  for item in delivery.for_message(db, message_id)]
    result = {'message': message, 'deliveries': deliveries,
             'events': journal.events_for(db, message_id)}
    if message['origin'] == 'direct':
        result['bridges'] = bridging.receipt(db, message_id)
    else:
        result['incoming'] = [{'bridge': row['bridge_name'], 'version': row['bridge_version'],
                               'source_channel': row['source_channel'],
                               'source_message_id': row['source_message_id']}
                              for row in bridging.incoming_for(db, message_id)]
    return result


def recent(db, channel, limit=30):
    rows = db.execute('''SELECT id,producer,dedup_key,lane,policy,origin,published,available
                         FROM messages WHERE channel=? ORDER BY id DESC LIMIT ?''', (channel, limit))
    return [dict(row) for row in rows]
