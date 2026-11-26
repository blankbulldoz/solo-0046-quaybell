from . import catalog, codec, confluence_service, delivery, journal, policy, routing
from .envelope import Envelope
from .errors import conflict, invalid, missing


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
    groups = routing.matching(db, channel, envelope.tags)
    if envelope.source is None:
        enabled_group = next((group for group in groups
                              if confluence_service.is_enabled(db, group['id'])), None)
        if enabled_group is not None:
            raise invalid(f"消费组 {enabled_group['name']} 处于合流模式，消息须同时提供 source 和 sequence")
    ready_at = now + envelope.delay_seconds
    key = db.execute('''INSERT INTO messages(channel,producer,dedup_key,lane,envelope,fingerprint,policy,published,available)
                        VALUES (?,?,?,?,?,?,?,?,?)''',
                     (channel, envelope.producer, envelope.dedup_key, envelope.lane, encoded, digest,
                      active['version'], now, ready_at)).lastrowid
    fanout = {}
    confluence = {}
    for group in groups:
        delivery_id = delivery.add(db, key, group['id'], envelope.lane, active['version'], ready_at)
        fanout[group['name']] = delivery_id
        if envelope.source is not None and confluence_service.is_enabled(db, group['id']):
            confluence[group['name']] = confluence_service.register_delivery(
                db, channel, group, envelope.source, envelope.sequence,
                delivery_id, now, envelope.payload)
    journal.append(db, channel, now, 'published', {'fanout': fanout, 'policy': active['version']}, key)
    result = dict(message_id=key, policy=active['version'], repeated=False, fanout=fanout)
    if confluence:
        result['confluence'] = confluence
    return result
