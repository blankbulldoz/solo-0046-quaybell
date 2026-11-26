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


def find(db, channel, producer, key):
    return db.execute('SELECT * FROM messages WHERE channel=? AND producer=? AND dedup_key=?',
                      (channel, producer, key)).fetchone()


def commit_new(db, channel, envelope, now, *, journal_details=None):
    """Insert the envelope, its fanout and the published event atomically.

    Shared by direct publishes and confirmed candidates so both paths produce
    byte-identical receipts for the same committed envelope.
    """
    encoded = envelope.encoded()
    digest = codec.fingerprint(encoded)
    active = policy.read(db, channel)
    ready_at = now + envelope.delay_seconds
    key = db.execute('''INSERT INTO messages(channel,producer,dedup_key,lane,envelope,fingerprint,policy,published,available)
                        VALUES (?,?,?,?,?,?,?,?,?)''',
                     (channel, envelope.producer, envelope.dedup_key, envelope.lane, encoded, digest,
                      active['version'], now, ready_at)).lastrowid
    fanout = {}
    for group in routing.matching(db, channel, envelope.tags):
        fanout[group['name']] = delivery.add(db, key, group['id'], envelope.lane, active['version'], ready_at)
    details = {'fanout': fanout, 'policy': active['version']}
    if journal_details:
        details.update(journal_details)
    journal.append(db, channel, now, 'published', details, key)
    row = db.execute('SELECT * FROM messages WHERE id=?', (key,)).fetchone()
    return receipt(db, row, False)


def publish(db, channel, raw, now):
    catalog.get(db, channel)
    envelope = Envelope.from_input(raw)
    encoded = envelope.encoded()
    digest = codec.fingerprint(encoded)
    existing = find(db, channel, envelope.producer, envelope.dedup_key)
    if existing:
        if existing['fingerprint'] != digest or existing['envelope'] != encoded:
            # Lazy import: reviews depends on this module's commit/receipt helpers.
            from . import reviews
            review = reviews.conflict_review(db, channel, envelope, now, existing=existing)
            raise conflict('同一生产键已用于另一条消息；请复核后重发旧告警或改用新键确认发布',
                           {'review': review, 'conflict': review, **reviews.error_aliases(review)})
        return receipt(db, existing, True)
    return commit_new(db, channel, envelope, now)
