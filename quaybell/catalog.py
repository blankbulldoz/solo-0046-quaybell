from . import inputs, journal, policy, reconcile
from .errors import conflict, missing


def get(db, channel):
    row = db.execute('SELECT * FROM channels WHERE name=?', (channel,)).fetchone()
    if row is None:
        raise missing('找不到频道')
    return dict(row)


def create(db, raw, now):
    inputs.require(raw, ('name',), ('policy',))
    channel = inputs.name(raw['name'], '频道名')
    config = raw.get('policy', policy.DEFAULT)
    if not isinstance(config, dict):
        raise inputs.invalid('投递策略须为对象')
    if db.execute('SELECT 1 FROM channels WHERE name=?', (channel,)).fetchone():
        raise conflict('频道已存在')
    db.execute('INSERT INTO channels VALUES (?,?)', (channel, now))
    first = policy.create(db, channel, 1, config, now)
    result_rule = reconcile.seed_default(db, channel, now)
    journal.append(db, channel, now, 'channel_created',
                   {'version': 1, 'result_rule': result_rule['version']})
    return get(db, channel) | {'policy': first, 'result_rule': result_rule}


def update_policy(db, channel, raw, now):
    inputs.require(raw, (*policy.LIMITS, 'expected'))
    expected = inputs.integer(raw['expected'], 1, 2**63-2, '当前版号')
    record = policy.revise(db, channel, expected, {key: raw[key] for key in policy.LIMITS}, now)
    journal.append(db, channel, now, 'policy_revised', {'version': record['version']})
    return record


def update_result_rule(db, channel, raw, now):
    inputs.require(raw, ('base_unit', 'tick_scale', 'units', 'expected'))
    expected = inputs.integer(raw['expected'], 1, 2**63-2, '当前规则版号')
    return reconcile.revise(db, channel, expected, raw, now)

def list_all(db):
    return [dict(row) for row in db.execute('SELECT * FROM channels ORDER BY name')]
