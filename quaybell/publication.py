from . import catalog, coalescing, codec, delivery, journal, policy, routing
from .envelope import Envelope
from .errors import conflict, missing


def get(db, message_id):
    row = db.execute('SELECT * FROM messages WHERE id=?', (message_id,)).fetchone()
    if row is None:
        raise missing('找不到消息')
    return dict(row) | {'envelope': codec.unpack(row['envelope'])}


def _supersession_receipt(db, fanout):
    result = []
    for group_name, delivery_id in fanout.items():
        old = coalescing.supersessions_for_delivery(db, delivery_id)
        if old:
            ids = [row['old_delivery_id'] for row in old]
            result.append({'group': group_name, 'delivery_id': delivery_id,
                           'new_delivery_id': delivery_id, 'supersedes': ids,
                           'replaced': ids})
    return result


def receipt(db, message, repeated):
    original = [item for item in delivery.for_message(db, message['id']) if item['parent_id'] is None]
    fanout = {item['group_name']: item['id'] for item in original}
    supersessions = _supersession_receipt(db, fanout)
    return dict(message_id=message['id'], policy=message['policy'], repeated=repeated,
                fanout=fanout, supersessions=supersessions,
                coalesced=[old for item in supersessions for old in item['supersedes']])


def publish(db, channel, raw, now):
    catalog.get(db, channel)
    envelope = Envelope.from_input(raw)
    encoded = envelope.encoded()
    digest = codec.fingerprint(encoded)
    existing = db.execute('SELECT * FROM messages WHERE channel=? AND producer=? AND dedup_key=?',
                          (channel, envelope.producer, envelope.dedup_key)).fetchone()
    if existing:
        if existing['fingerprint'] != digest or existing['envelope'] != encoded:
            raise conflict('同一生产键已用于另一条消息')
        message = get(db, existing['id'])
        return receipt(db, message, True)
    active = policy.read(db, channel)
    ready_at = now + envelope.delay_seconds
    key = db.execute('''INSERT INTO messages(channel,producer,dedup_key,lane,envelope,fingerprint,policy,published,available)
                        VALUES (?,?,?,?,?,?,?,?,?)''',
                     (channel, envelope.producer, envelope.dedup_key, envelope.lane, encoded, digest,
                      active['version'], now, ready_at)).lastrowid
    fanout = {}
    supersessions = []
    for group in routing.matching(db, channel, envelope.tags):
        delivery_id = delivery.add(db, key, group['id'], envelope.lane, active['version'], ready_at)
        fanout[group['name']] = delivery_id
        if coalescing.publication_can_supersede(group, ready_at, now):
            result = coalescing.supersede(db, delivery_id, now, 'published')
            if result['count']:
                ids = result['supersedes']
                supersessions.append({'group': group['name'], 'delivery_id': delivery_id,
                                      'new_delivery_id': delivery_id, 'supersedes': ids,
                                      'replaced': ids})
    journal.append(db, channel, now, 'published',
                   {'fanout': fanout, 'policy': active['version'],
                    'supersessions': supersessions}, key)
    return dict(message_id=key, policy=active['version'], repeated=False, fanout=fanout,
                supersessions=supersessions,
                coalesced=[old for item in supersessions for old in item['supersedes']])
