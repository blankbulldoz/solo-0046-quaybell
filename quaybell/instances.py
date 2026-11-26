from . import attempts, codec, delivery, inputs, journal, outcomes, subscriptions
from .errors import conflict, missing

RELEASE_REASON = '实例注销交回'


def get(db, key):
    row = db.execute('''SELECT i.*,s.channel,s.name AS group_name FROM instances i
                        JOIN subscriptions s ON s.id=i.subscription_id WHERE i.id=?''',
                     (key,)).fetchone()
    if row is None:
        raise missing('找不到工作实例')
    return dict(row)


def register(db, channel, group, raw, now):
    sub = subscriptions.get(db, channel, group)
    inputs.require(raw, ('worker',))
    worker = inputs.name(raw['worker'], '工作进程')
    key = db.execute('INSERT INTO instances(subscription_id,worker,registered) VALUES (?,?,?)',
                     (sub['id'], worker, now)).lastrowid
    journal.append(db, channel, now, 'instance_registered',
                   {'instance': key, 'group': group, 'worker': worker})
    return dict(id=key, worker=worker, channel=channel, group=group, registered=now, released=None)


def attach(db, sub, key, worker):
    if type(key) is not int or key < 1:
        raise inputs.invalid('实例编号须为正整数')
    row = db.execute('SELECT * FROM instances WHERE id=?', (key,)).fetchone()
    if row is None:
        raise conflict('找不到登记的工作实例')
    if row['subscription_id'] != sub['id']:
        raise conflict('该实例不属于此消费组')
    if row['worker'] != worker:
        raise conflict('该实例属于另一个工作进程')
    if row['released'] is not None:
        raise conflict('该实例已注销')
    return row['id']


def deregister(db, key, raw, now):
    inputs.require(raw, ())
    record = get(db, key)
    if record['released'] is not None:
        return codec.unpack(record['receipt']) | {'repeated': True}
    returned = []
    for attempt in attempts.held_by(db, key):
        item = delivery.get(db, attempt['delivery_id'])
        returned.append(outcomes.apply(db, item, attempt, 'released', RELEASE_REASON, now))
    receipt = {'instance': key, 'worker': record['worker'], 'returned': returned}
    db.execute('UPDATE instances SET released=?,receipt=? WHERE id=?',
               (now, codec.pack(receipt), key))
    journal.append(db, record['channel'], now, 'instance_deregistered',
                   {'instance': key, 'group': record['group_name'], 'worker': record['worker'],
                    'returned': [item['delivery_id'] for item in returned]})
    return receipt | {'repeated': False}
