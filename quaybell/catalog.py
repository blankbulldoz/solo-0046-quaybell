from . import inputs, journal, policy
from .errors import conflict, missing


def get(db, channel):
    row = db.execute('SELECT * FROM channels WHERE name=?', (channel,)).fetchone()
    if row is None:
        raise missing('找不到频道')
    return dict(row)


def create(db, raw, now):
    if 'name' not in raw:
        raise inputs.invalid('请求缺少 name')
    channel = inputs.name(raw['name'], '频道名')
    config = raw.get('policy', policy.DEFAULT)
    if not isinstance(config, dict):
        raise inputs.invalid('投递策略须为对象')
    partition_count = raw.get('partitions', raw.get(
        'partition_count', raw.get('shards', raw.get('num_partitions', raw.get('partition_num')))))
    if partition_count is not None:
        partition_count = inputs.integer(partition_count, 1, 64, '分片数量')
    if db.execute('SELECT 1 FROM channels WHERE name=?', (channel,)).fetchone():
        raise conflict('频道已存在')
    db.execute('INSERT INTO channels(name,created,partition_count) VALUES (?,?,?)',
               (channel, now, partition_count))
    first = policy.create(db, channel, 1, config, now)
    journal.append(db, channel, now, 'channel_created', {'version': 1, 'partitions': partition_count})
    return get(db, channel) | {'policy': first, 'partition_count': partition_count}


def update_policy(db, channel, raw, now):
    inputs.require(raw, (*policy.LIMITS, 'expected'))
    expected = inputs.integer(raw['expected'], 1, 2**63-2, '当前版号')
    record = policy.revise(db, channel, expected, {key: raw[key] for key in policy.LIMITS}, now)
    journal.append(db, channel, now, 'policy_revised', {'version': record['version']})
    return record


def list_all(db):
    return [dict(row) for row in db.execute('SELECT * FROM channels ORDER BY name')]
