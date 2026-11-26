from . import attempts, codec, delivery, inputs, journal, policy, publication, subscriptions
from .envelope import Envelope
from .errors import conflict, invalid


def _envelope(raw, source, group):
    """Overlay a correction request on the source message's published identity."""
    if not isinstance(raw, dict):
        raise invalid('修正信封须为对象')
    inputs.require(raw, ('payload',), ('producer', 'key', 'lane', 'tags', 'delay_seconds'))
    inherited = dict(producer=source['producer'], key=source['dedup_key'], lane=source['lane'],
                     tags=source['envelope']['tags'], payload=raw['payload'])
    if 'tags' in raw:
        inherited['tags'] = raw['tags']
    if 'delay_seconds' in raw:
        inherited['delay_seconds'] = raw['delay_seconds']
    envelope = Envelope.from_input(inherited)
    given = {'producer': raw.get('producer'), 'key': raw.get('key'), 'lane': raw.get('lane')}
    wanted = {'producer': source['producer'], 'key': source['dedup_key'], 'lane': source['lane']}
    if any(value is not None and value != wanted[field] for field, value in given.items()):
        raise invalid('修正须沿用原消息的生产者、生产键和顺序键')
    if not all(envelope.tags.get(key) == value for key, value in group['tags'].items()):
        raise invalid('修正后的标签仍须匹配原消费组')
    if envelope.encoded() == codec.pack(source['envelope']):
        raise invalid('修正信封须与原信封不同；原样重发请使用重投')
    return envelope


def _validate(raw):
    inputs.require(raw, ('operator', 'reason', 'envelope'))
    operator = inputs.operator(raw['operator'], '值班员')
    reason = raw['reason']
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 240:
        raise invalid('修订原因须为 1～240 个字符')
    return operator, reason.strip()


def _source_snapshot(db, source_delivery):
    history = attempts.for_delivery(db, source_delivery['id'])
    terminal = history[-1] if history else None
    return {'state': source_delivery['state'],
            'attempt': codec.pack(terminal) if terminal else None}


def source_view(db, row):
    """The terminal failure a correction answers, readable even after the source is pruned."""
    item = db.execute(delivery.DELIVERY + ' WHERE d.id=?', (row['source_delivery_id'],)).fetchone()
    snapshot = codec.unpack(row['source_attempt']) if row['source_attempt'] else None
    if item is None:
        return dict(delivery_id=row['source_delivery_id'], message_id=row['source_message_id'],
                    group_name=row['source_group'], lane=row['source_lane'], state=row['source_state'],
                    last_attempt=snapshot, pruned=True)
    history = attempts.for_delivery(db, item['id'])
    return dict(delivery_id=item['id'], message_id=item['message_id'], group_name=item['group_name'],
                lane=item['lane'], state=item['state'], last_attempt=history[-1] if history else None,
                pruned=False)


def view(db, row, repeated):
    return dict(correction_id=row['id'], source=source_view(db, row), message_id=row['message_id'],
                delivery_id=row['delivery_id'], policy=row['policy'], lane=row['source_lane'],
                appended_after=row['appended_after'], operator=row['operator'], reason=row['reason'],
                created=row['created'], repeated=repeated)


def for_message(db, message_id):
    row = db.execute('SELECT * FROM corrections WHERE message_id=?', (message_id,)).fetchone()
    return view(db, row, False) if row else None


def as_source(db, message_id):
    """Corrections that answer a dead delivery of this message."""
    rows = db.execute('SELECT * FROM corrections WHERE source_message_id=? ORDER BY id', (message_id,))
    return [dict(correction_id=row['id'], source_delivery_id=row['source_delivery_id'],
                 message_id=row['message_id'], delivery_id=row['delivery_id'], operator=row['operator'],
                 reason=row['reason'], created=row['created']) for row in rows]


def as_dead_target(db, delivery_id):
    rows = db.execute('SELECT * FROM corrections WHERE source_delivery_id=? ORDER BY id', (delivery_id,))
    return [dict(correction_id=row['id'], message_id=row['message_id'], delivery_id=row['delivery_id'],
                 operator=row['operator'], reason=row['reason'], created=row['created']) for row in rows]


def correct(db, key, raw, now):
    operator, reason = _validate(raw)
    source_delivery = delivery.get(db, key)
    if source_delivery['state'] != 'dead':
        raise conflict('只有死信投递可以提交修正')
    source_message = publication.get(db, source_delivery['message_id'])
    group = db.execute('SELECT * FROM subscriptions WHERE id=?',
                       (source_delivery['subscription_id'],)).fetchone()
    envelope = _envelope(raw['envelope'], source_message, subscriptions.decoded(group))
    encoded = envelope.encoded()
    existing = db.execute('SELECT * FROM corrections WHERE source_delivery_id=? AND operator=?',
                          (key, operator)).fetchone()
    if existing is not None:
        stored = db.execute('SELECT envelope FROM messages WHERE id=?', (existing['message_id'],)).fetchone()
        if stored is not None and stored['envelope'] == encoded and existing['reason'] == reason:
            return view(db, existing, True)
        raise conflict('该值班员已对此死信提交过另一份修正')
    active = policy.read(db, source_message['channel'])
    snapshot = _source_snapshot(db, source_delivery)
    tail = db.execute('SELECT MAX(id) FROM deliveries WHERE subscription_id=? AND lane=?',
                      (source_delivery['subscription_id'], source_delivery['lane'])).fetchone()[0]
    # The corrected envelope gets a fresh message identity under the source producer
    # key, and one delivery appended to the tail of the source group's lane.
    stored = publication.record_message(db, source_message['channel'], envelope, now,
                                        [(source_delivery['subscription_id'], source_delivery['group_name'])],
                                        origin_delivery_id=key)
    delivery_id = stored['fanout'][source_delivery['group_name']]
    correction = db.execute('''INSERT INTO corrections(channel,source_delivery_id,source_message_id,source_group,
                                                        source_lane,source_state,source_attempt,appended_after,
                                                        message_id,delivery_id,operator,reason,policy,created)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                            (source_message['channel'], key, source_message['id'], source_delivery['group_name'],
                             source_delivery['lane'], snapshot['state'], snapshot['attempt'], tail,
                             stored['message_id'], delivery_id, operator, reason, active['version'], now)).lastrowid
    journal.append(db, source_message['channel'], now, 'corrected',
                   {'correction_id': correction, 'message_id': stored['message_id'],
                    'delivery_id': delivery_id, 'operator': operator, 'reason': reason,
                    'source_delivery_id': key, 'appended_after': tail}, source_message['id'])
    journal.append(db, source_message['channel'], now, 'published',
                   {'fanout': stored['fanout'], 'policy': stored['policy'], 'corrects_delivery_id': key},
                   stored['message_id'])
    row = db.execute('SELECT * FROM corrections WHERE id=?', (correction,)).fetchone()
    return view(db, row, False)
