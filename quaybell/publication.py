from . import catalog, codec, delivery, journal, policy, routing, watermark
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
        return receipt(db, existing, True)
    active = policy.read(db, channel)
    ready_at = now + envelope.delay_seconds
    key = db.execute('''INSERT INTO messages(channel,producer,dedup_key,lane,envelope,fingerprint,policy,published,available)
                        VALUES (?,?,?,?,?,?,?,?,?)''',
                     (channel, envelope.producer, envelope.dedup_key, envelope.lane, encoded, digest,
                      active['version'], now, ready_at)).lastrowid
    fanout = {}
    for group in routing.matching(db, channel, envelope.tags):
        fanout[group['name']] = delivery.add(db, key, group['id'], envelope.lane, active['version'], ready_at)
        watermark.pending(db, channel, group['name'], key, envelope.lane, active['version'], digest, now)
    journal.append(db, channel, now, 'published', {'fanout': fanout, 'policy': active['version']}, key)
    return dict(message_id=key, policy=active['version'], repeated=False, fanout=fanout)
