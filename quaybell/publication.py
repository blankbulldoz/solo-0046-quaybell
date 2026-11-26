from . import catalog, codec, delivery, journal, policy, routing
from .envelope import Envelope
from .errors import conflict, missing


def get(db, message_id):
    row = db.execute('SELECT * FROM messages WHERE id=?', (message_id,)).fetchone()
    if row is None:
        raise missing('找不到消息')
    return dict(row) | {'envelope': codec.unpack(row['envelope'])}


def receipt(db, message, repeated):
    original = [item for item in delivery.for_message(db, message['id']) if item['parent_id'] is None]
    return dict(message_id=message['id'], policy=message['policy'], repeated=repeated,
                fanout={item['group_name']: item['id'] for item in original})


def record_message(db, channel, envelope, now, groups, origin_delivery_id=None):
    """Store one envelope and append one delivery per group, sharing the caller's transaction.

    ``origin_delivery_id`` marks a correction: it keeps the producer key of the
    source message while the message itself takes a fresh identity, so the
    partial unique index on ordinary messages still guards normal publication.
    """
    encoded = envelope.encoded()
    digest = codec.fingerprint(encoded)
    active = policy.read(db, channel)
    ready_at = now + envelope.delay_seconds
    key = db.execute('''INSERT INTO messages(channel,producer,dedup_key,lane,envelope,fingerprint,
                                               policy,published,available,origin_delivery_id)
                        VALUES (?,?,?,?,?,?,?,?,?,?)''',
                     (channel, envelope.producer, envelope.dedup_key, envelope.lane, encoded, digest,
                      active['version'], now, ready_at, origin_delivery_id)).lastrowid
    fanout = {}
    for group_id, group_name in groups:
        fanout[group_name] = delivery.add(db, key, group_id, envelope.lane, active['version'], ready_at)
    return dict(message_id=key, policy=active['version'], digest=digest, fanout=fanout)


def publish(db, channel, raw, now):
    catalog.get(db, channel)
    envelope = Envelope.from_input(raw)
    encoded = envelope.encoded()
    digest = codec.fingerprint(encoded)
    existing = db.execute('''SELECT * FROM messages
                             WHERE channel=? AND producer=? AND dedup_key=? AND origin_delivery_id IS NULL''',
                          (channel, envelope.producer, envelope.dedup_key)).fetchone()
    if existing:
        if existing['fingerprint'] != digest or existing['envelope'] != encoded:
            raise conflict('同一生产键已用于另一条消息')
        return receipt(db, existing, True)
    groups = [(group['id'], group['name']) for group in routing.matching(db, channel, envelope.tags)]
    stored = record_message(db, channel, envelope, now, groups)
    journal.append(db, channel, now, 'published', {'fanout': stored['fanout'], 'policy': stored['policy']},
                   stored['message_id'])
    return dict(message_id=stored['message_id'], policy=stored['policy'], repeated=False, fanout=stored['fanout'])
