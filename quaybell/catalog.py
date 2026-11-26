from . import impact, inputs, journal, policy
from .errors import conflict, missing, stale


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
    journal.append(db, channel, now, 'channel_created', {'version': 1})
    return get(db, channel) | {'policy': first}


def update_policy(db, channel, raw, now):
    inputs.require(raw, (*policy.LIMITS, 'expected', 'impact_token'))
    expected = inputs.integer(raw['expected'], 1, 2**63-2, '当前版号')
    token = inputs.token(raw['impact_token'], '对照标记')
    current = policy.read(db, channel)
    if current['version'] != expected:
        raise conflict('投递策略已更新，请重新载入')
    candidate = policy.checked({key: raw[key] for key in policy.LIMITS}) | {'version': expected + 1}
    if token != impact.state_token(db, channel, candidate):
        detail = impact.comparison(db, channel, now, current, candidate)
        raise stale('对照之后状态或拟修订值已有变化，旧对照不再代表当前状态；'
                    '请按最新对照重新确认', detail)
    record = policy.revise(db, channel, expected, {key: raw[key] for key in policy.LIMITS}, now)
    journal.append(db, channel, now, 'policy_revised',
                   {'version': record['version'], 'previous': expected,
                    'policy': impact.policy_view(record)})
    return record | {'previous': expected}


def list_all(db):
    return [dict(row) for row in db.execute('SELECT * FROM channels ORDER BY name')]
