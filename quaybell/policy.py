from . import inputs
from .errors import conflict, missing

LIMITS = {'lease_seconds': (1, 300, '租约秒数'), 'retry_seconds': (0, 300, '初始退避秒数'),
          'max_attempts': (1, 12, '最大尝试次数'), 'retention_seconds': (1, 604800, '保留秒数')}
DEFAULT = dict(lease_seconds=24, retry_seconds=3, max_attempts=3, retention_seconds=3600)


def checked(raw):
    inputs.require(raw, LIMITS)
    return {key: inputs.integer(raw[key], *limits) for key, limits in LIMITS.items()}


def validated(raw):
    return list(checked(raw).values())


def read(db, channel, version=None):
    base = 'SELECT * FROM policies WHERE channel=?'
    if version is None:
        row = db.execute(base + ' ORDER BY version DESC LIMIT 1', (channel,)).fetchone()
    else:
        row = db.execute(base + ' AND version=?', (channel, version)).fetchone()
    if row is None:
        raise missing('找不到投递策略')
    return dict(row)


def create(db, channel, version, raw, now):
    db.execute('INSERT INTO policies VALUES (?,?,?,?,?,?,?)',
               (channel, version, *validated(raw), now))
    return read(db, channel, version)


def revise(db, channel, expected, raw, now):
    current = read(db, channel)
    if current['version'] != expected:
        raise conflict('投递策略已更新，请重新载入')
    return create(db, channel, expected + 1, raw, now)


def delay(record, number):
    return min(3600, record['retry_seconds'] * 2 ** (number - 1))
