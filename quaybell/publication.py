from . import catalog, codec, delivery, inputs, journal, policy, routing, shadow
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
    observed = inputs.boolean(raw.get('observed', False), '观测标志')
    catalog.get(db, channel)
    envelope = Envelope.from_input(raw, extra=('observed',))
    encoded = envelope.encoded()
    digest = codec.fingerprint(encoded)
    existing = db.execute('SELECT * FROM messages WHERE channel=? AND producer=? AND dedup_key=?',
                          (channel, envelope.producer, envelope.dedup_key)).fetchone()
    if existing:
        if existing['fingerprint'] != digest or existing['envelope'] != encoded:
            raise conflict('同一生产键已用于另一条消息')
        if observed:
            # A replay under observation is a real publication attempt; it keeps
            # the committed receipt and creates no new deliveries, but shadow
            # still records what it saw.
            shadow.record(db, channel, envelope, existing['id'], now, repeated=True, deliveries_created=0)
            journal.append(db, channel, now, 'publish_observed',
                           {'message_id': existing['id'], 'repeated': True}, existing['id'])
        return receipt(db, existing, True)
    active = policy.read(db, channel)
    ready_at = now + envelope.delay_seconds
    key = db.execute('''INSERT INTO messages(channel,producer,dedup_key,lane,envelope,fingerprint,policy,published,available)
                        VALUES (?,?,?,?,?,?,?,?,?)''',
                     (channel, envelope.producer, envelope.dedup_key, envelope.lane, encoded, digest,
                      active['version'], now, ready_at)).lastrowid
    fanout, rules = {}, {}
    for group in routing.matching(db, channel, envelope.tags):
        fanout[group['name']] = delivery.add(db, key, group['id'], envelope.lane, active['version'], ready_at)
        rules[group['name']] = group['version']
    journal.append(db, channel, now, 'published', {'fanout': fanout, 'policy': active['version'],
                                                   'subscription_rules': rules}, key)
    shadow.record(db, channel, envelope, key, now, repeated=False, deliveries_created=len(fanout))
    return dict(message_id=key, policy=active['version'], repeated=False, fanout=fanout)
