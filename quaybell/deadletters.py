from . import attempts, catalog, delivery, relay
from .errors import conflict


def get(db, key):
    item = delivery.get(db, key)
    if item['state'] != 'dead':
        raise conflict('投递尚未进入死信状态')
    return item


def list_recent(db, channel, limit=50):
    catalog.get(db, channel)
    rows = db.execute(delivery.DELIVERY + " WHERE m.channel=? AND d.state='dead' ORDER BY d.id DESC LIMIT ?",
                      (channel, limit))
    result = []
    for row in rows:
        child = db.execute('SELECT id FROM deliveries WHERE parent_id=?', (row['id'],)).fetchone()
        history = attempts.for_delivery(db, row['id'])
        relayed = relay.for_delivery(db, row['id'])
        result.append(dict(row) | {'last_attempt': history[-1],
                                   'redriven_as': child['id'] if child else None,
                                   'relayed_to': None if relayed is None
                                   else {'channel': relayed['target_channel'],
                                         'message_id': relayed['target_message_id']}})
    return result
