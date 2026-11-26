from . import bridging, catalog, codec, delivery, journal, policy, routing
from .envelope import Envelope
from .errors import conflict, missing


def get(db, message_id):
    row = db.execute('SELECT * FROM messages WHERE id=?', (message_id,)).fetchone()
    if row is None:
        raise missing('找不到消息')
    return dict(row) | {'envelope': codec.unpack(row['envelope'])}


def receipt(db, message, repeated):
    original = [item for item in delivery.for_message(db, message['id']) if item['parent_id'] is None]
    return dict(message_id=message['id'], policy=message['policy'], origin=message['origin'],
                repeated=repeated,
                fanout={item['group_name']: item['id'] for item in original},
                bridges=bridging.receipt(db, message['id']))


def publish(db, channel, raw, now):
    catalog.get(db, channel)
    envelope = Envelope.from_input(raw)
    encoded = envelope.encoded()
    digest = codec.fingerprint(encoded)
    existing = db.execute('''SELECT * FROM messages WHERE channel=? AND producer=? AND dedup_key=?
                             AND origin='direct' ''',
                          (channel, envelope.producer, envelope.dedup_key)).fetchone()
    if existing:
        if existing['fingerprint'] != digest or existing['envelope'] != encoded:
            raise conflict('同一生产键已用于另一条消息')
        return receipt(db, existing, True)
    active = policy.read(db, channel)
    ready_at = now + envelope.delay_seconds
    key = db.execute('''INSERT INTO messages(channel,producer,dedup_key,lane,envelope,fingerprint,
                          policy,origin,published,available)
                        VALUES (?,?,?,?,?,?,?,'direct',?,?)''',
                     (channel, envelope.producer, envelope.dedup_key, envelope.lane, encoded, digest,
                      active['version'], now, ready_at)).lastrowid
    message = get(db, key)
    fanout = {}
    for group in routing.matching(db, channel, envelope.tags):
        fanout[group['name']] = delivery.add(db, key, group['id'], envelope.lane, active['version'], ready_at)
    journal.append(db, channel, now, 'published', {'fanout': fanout, 'policy': active['version']}, key)
    # 同一写事务内转发：只有目标消息和分发都落库后，源回执才确认。
    bridges = bridging.forward(db, channel, message, envelope, now)
    return dict(message_id=key, policy=active['version'], origin='direct', repeated=False,
                fanout=fanout, bridges=bridges)
