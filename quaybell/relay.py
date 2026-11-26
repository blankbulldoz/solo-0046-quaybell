import sqlite3

from . import attempts, catalog, codec, delivery, inputs, journal, publication
from .envelope import Envelope
from .errors import conflict, invalid

ORIGIN_CHANNEL = 'x-quaybell-origin-channel'
ORIGIN_MESSAGE = 'x-quaybell-origin-message'
ORIGIN_DELIVERY = 'x-quaybell-origin-delivery'
ORIGIN_TAGS = (ORIGIN_CHANNEL, ORIGIN_MESSAGE, ORIGIN_DELIVERY)


def _row(db, source_id):
    return db.execute('SELECT * FROM relays WHERE source_delivery_id=?', (source_id,)).fetchone()


def origin_tags(source_channel, source_message_id, source_delivery_id):
    return {ORIGIN_CHANNEL: source_channel, ORIGIN_MESSAGE: str(source_message_id),
            ORIGIN_DELIVERY: str(source_delivery_id)}


def _target_envelope(source_message, source_channel, source_delivery_id):
    envelope = source_message['envelope']
    tags = dict(envelope['tags'])
    collided = [key for key in ORIGIN_TAGS if key in tags]
    if collided:
        raise invalid('原信封标签与转运溯源标签冲突：' + '、'.join(collided))
    tags.update(origin_tags(source_channel, source_message['id'], source_delivery_id))
    return Envelope(source_message['producer'], source_message['dedup_key'], envelope['lane'],
                    tags, envelope['payload'], 0)


def _summary(row, repeated):
    return {'source_id': row['source_delivery_id'], 'source_channel': row['source_channel'],
            'target_channel': row['target_channel'], 'target_message_id': row['target_message_id'],
            'reason': row['reason'], 'repeated': repeated}


def _commitment(db, row, repeated):
    message = publication.get(db, row['target_message_id'])
    return _summary(row, repeated) | {'fanout': publication.receipt(db, message, True)['fanout']}


def relay(db, source_id, raw, now):
    inputs.require(raw, ('channel',))
    target_channel = inputs.name(raw['channel'], '修复频道名')
    source = delivery.get(db, source_id)
    if source['state'] != 'dead':
        raise conflict('投递尚未进入死信状态，不能转运')
    catalog.get(db, target_channel)
    existing = _row(db, source_id)
    if existing is not None:
        if existing['target_channel'] != target_channel:
            raise conflict('该死信已转运到其他修复频道，不能改派')
        return _commitment(db, existing, True)
    source_message = publication.get(db, source['message_id'])
    history = attempts.for_delivery(db, source_id)
    reason = history[-1]['reason']
    envelope = _target_envelope(source_message, source['channel'], source_id)
    try:
        receipt = publication.publish(db, target_channel, {
            'producer': envelope.producer, 'key': envelope.dedup_key, 'lane': envelope.lane,
            'tags': envelope.tags, 'payload': envelope.payload,
        }, now)
        provenance = codec.pack({'source_channel': source['channel'],
                                 'source_message_id': source['message_id'],
                                 'source_delivery_id': source_id,
                                 'source_attempt': history[-1]['number'], 'reason': reason,
                                 'relayed_at': now})
        db.execute('''INSERT INTO relays(source_delivery_id,source_message_id,source_channel,
                                        target_channel,target_message_id,reason,provenance,created)
                      VALUES (?,?,?,?,?,?,?,?)''',
                   (source_id, source['message_id'], source['channel'], target_channel,
                    receipt['message_id'], reason, provenance, now))
    except sqlite3.IntegrityError:
        raise conflict('该修复频道已存在同一生产者与生产键的消息，不能重复转运') from None
    row = _row(db, source_id)
    journal.append(db, source['channel'], now, 'relayed', _summary(row, False), source['message_id'])
    journal.append(db, target_channel, now, 'received',
                   {'source_channel': source['channel'], 'source_delivery_id': source_id,
                    'target_message_id': receipt['message_id'], 'reason': reason},
                   receipt['message_id'])
    return _commitment(db, row, False)


def for_delivery(db, source_id):
    row = _row(db, source_id)
    return None if row is None else _summary(row, False)


def for_message(db, message_id):
    rows = db.execute('SELECT * FROM relays WHERE source_message_id=? ORDER BY id', (message_id,))
    return [_summary(row, False) | {'provenance': codec.unpack(row['provenance'])} for row in rows]


def origin_for(db, message_id):
    row = db.execute('SELECT * FROM relays WHERE target_message_id=?', (message_id,)).fetchone()
    if row is None:
        return None
    provenance = codec.unpack(row['provenance'])
    return {'source_id': row['source_delivery_id'] if row['source_delivery_id'] is not None
            else provenance['source_delivery_id'],
            'source_channel': row['source_channel'],
            'target_channel': row['target_channel'], 'target_message_id': row['target_message_id'],
            'reason': row['reason'], 'repeated': False, 'provenance': provenance}
