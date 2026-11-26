from . import catalog, codec, delivery, expiry, journal, policy, routing
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
                deadline=message['deadline'],
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
    deadline = None if envelope.deadline_seconds is None else now + envelope.deadline_seconds
    key = db.execute('''INSERT INTO messages(channel,producer,dedup_key,lane,envelope,fingerprint,policy,
                                             published,available,deadline)
                        VALUES (?,?,?,?,?,?,?,?,?,?)''',
                     (channel, envelope.producer, envelope.dedup_key, envelope.lane, encoded, digest,
                      active['version'], now, ready_at, deadline)).lastrowid
    # A delivery that cannot become ready before the deadline is never claimable;
    # settle it in the dead letters now so it does not hold up its order key.
    undeliverable = deadline is not None and deadline <= ready_at
    fanout = {}
    for group in routing.matching(db, channel, envelope.tags):
        fanout[group['name']] = delivery.add(db, key, group['id'], envelope.lane,
                                             active['version'], ready_at)
    journal.append(db, channel, now, 'published',
                   {'fanout': fanout, 'policy': active['version'], 'deadline': deadline,
                    'ready_at': ready_at}, key)
    if undeliverable:
        for delivery_id in fanout.values():
            expiry.terminate(db, delivery.get(db, delivery_id), now)
    return dict(message_id=key, policy=active['version'], repeated=False,
                deadline=deadline, fanout=fanout)
