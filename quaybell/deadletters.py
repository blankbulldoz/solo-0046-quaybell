from . import attempts, catalog, closures, delivery
from .errors import conflict


def get(db, key):
    item = delivery.get(db, key)
    if item['state'] != 'dead':
        raise conflict('投递尚未进入死信状态')
    return item


def ensure_open(db, key):
    item = get(db, key)
    if closures.for_delivery(db, key) is not None:
        raise conflict('该死信已结案退出值守，不能再重投；结案凭证可在消息轨迹中追溯')
    return item


def list_recent(db, channel, limit=50):
    catalog.get(db, channel)
    rows = db.execute(delivery.DELIVERY + '''
        WHERE m.channel=? AND d.state='dead'
          AND NOT EXISTS (SELECT 1 FROM closures c WHERE c.delivery_id=d.id)
        ORDER BY d.id DESC LIMIT ?''', (channel, limit))
    result = []
    for row in rows:
        child = db.execute('SELECT id FROM deliveries WHERE parent_id=?', (row['id'],)).fetchone()
        history = attempts.for_delivery(db, row['id'])
        result.append(dict(row) | {'last_attempt': history[-1], 'redriven_as': child['id'] if child else None})
    return result
