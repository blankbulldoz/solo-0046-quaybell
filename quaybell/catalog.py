from . import forensics, inputs, journal, policy
from .errors import conflict, missing


def get(db, channel):
    row = db.execute('SELECT * FROM channels WHERE name=?', (channel,)).fetchone()
    if row is None:
        raise missing('找不到频道')
    return dict(row)


def create(db, raw, now):
    inputs.require(raw, ('name',), ('policy', 'audit', *forensics.ENABLE_FIELDS,
                                    *forensics.TTL_FIELDS))
    channel = inputs.name(raw['name'], '频道名')
    config = raw.get('policy', policy.DEFAULT)
    if not isinstance(config, dict):
        raise inputs.invalid('投递策略须为对象')
    policy_config = {key: value for key, value in config.items()
                     if key not in forensics.TTL_FIELDS
                     and key not in (*forensics.ENABLE_FIELDS, 'audit')}
    audit_options = {}
    audit_value = config.get('audit')
    if isinstance(audit_value, bool):
        audit_options['enabled'] = audit_value
    elif isinstance(audit_value, dict):
        audit_options.update(audit_value)
    for field in (*forensics.ENABLE_FIELDS, *forensics.TTL_FIELDS):
        if field in config:
            audit_options[field] = config[field]
    audit_value = raw.get('audit')
    if isinstance(audit_value, bool):
        audit_options['enabled'] = audit_value
    elif isinstance(audit_value, dict):
        audit_options.update(audit_value)
    for field in (*forensics.ENABLE_FIELDS, *forensics.TTL_FIELDS):
        if field in raw:
            audit_options[field] = raw[field]
    audit_requested = bool(audit_options) or 'audit' in raw or 'audit' in config
    if db.execute('SELECT 1 FROM channels WHERE name=?', (channel,)).fetchone():
        raise conflict('频道已存在')
    db.execute('INSERT INTO channels VALUES (?,?)', (channel, now))
    first = policy.create(db, channel, 1, policy_config, now)
    if audit_requested:
        enabled, ttl, enabled_at = forensics.settings_from_creation(audit_options, now)
        db.execute('''INSERT INTO audit_settings(channel,enabled,retention_seconds,enabled_at,updated_at)
                      VALUES (?,?,?,?,?)''', (channel, int(enabled), ttl, enabled_at, now))
    journal.append(db, channel, now, 'channel_created', {'version': 1})
    return get(db, channel) | {'policy': first}


def update_policy(db, channel, raw, now):
    inputs.require(raw, (*policy.LIMITS, 'expected'),
                   ('audit', *forensics.ENABLE_FIELDS, *forensics.TTL_FIELDS))
    expected = inputs.integer(raw['expected'], 1, 2**63-2, '当前版号')
    record = policy.revise(db, channel, expected, {key: raw[key] for key in policy.LIMITS}, now)
    audit_fields = ('audit', *forensics.ENABLE_FIELDS, *forensics.TTL_FIELDS)
    if any(field in raw for field in audit_fields):
        audit_body = dict(raw.get('audit') if isinstance(raw.get('audit'), dict) else {})
        if isinstance(raw.get('audit'), bool):
            audit_body['enabled'] = raw['audit']
        for field in forensics.ENABLE_FIELDS:
            if field in raw:
                audit_body.setdefault('enabled', raw[field])
        for field in forensics.TTL_FIELDS:
            if field in raw:
                audit_body.setdefault('retention_seconds', raw[field])
        record['audit'] = forensics.configure(db, channel, audit_body, now)
    journal.append(db, channel, now, 'policy_revised', {'version': record['version']})
    return record


def list_all(db):
    return [dict(row) for row in db.execute('SELECT * FROM channels ORDER BY name')]
