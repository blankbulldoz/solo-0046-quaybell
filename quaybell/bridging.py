"""频道桥接：把源频道匹配条件的直接发布转成目标频道的独立消息。"""
from . import catalog, codec, delivery, inputs, journal, policy, routing
from .codec import pack, unpack
from .envelope import labels
from .errors import conflict, invalid, missing


def decoded(row):
    return dict(row) | {'tags': unpack(row['tags'])}


def for_channel(db, source):
    catalog.get(db, source)
    rows = db.execute('''SELECT b.* FROM bridges b
                         JOIN (SELECT source_channel AS sc, name AS nm, MAX(version) AS version
                               FROM bridges GROUP BY source_channel,name) latest
                           ON latest.sc=b.source_channel AND latest.nm=b.name AND latest.version=b.version
                         WHERE b.source_channel=? ORDER BY b.id''', (source,))
    return [decoded(row) for row in rows]


def get(db, source, name, version=None):
    if version is None:
        row = db.execute('''SELECT * FROM bridges WHERE source_channel=? AND name=?
                            ORDER BY version DESC LIMIT 1''', (source, name)).fetchone()
    else:
        row = db.execute('SELECT * FROM bridges WHERE source_channel=? AND name=? AND version=?',
                         (source, name, version)).fetchone()
    if row is None:
        raise missing('找不到桥接配置')
    return decoded(row)


def name_exists(db, source, name):
    return db.execute('SELECT 1 FROM bridges WHERE source_channel=? AND name=? LIMIT 1',
                      (source, name)).fetchone() is not None


def create(db, source, raw, now):
    catalog.get(db, source)
    inputs.require(raw, ('name', 'target', 'tags'), ())
    name = inputs.name(raw['name'], '桥接名')
    target = inputs.name(raw['target'], '目标频道名')
    if target == source:
        raise invalid('桥接目标必须是另一个频道')
    tags = labels(raw['tags'])
    catalog.get(db, target)
    if name_exists(db, source, name):
        raise conflict('桥接名已存在')
    version = 1
    db.execute('INSERT INTO bridges(source_channel,name,version,target_channel,tags,created) VALUES (?,?,?,?,?,?)',
               (source, name, version, target, pack(tags), now))
    journal.append(db, source, now, 'bridge_configured',
                   {'bridge': name, 'version': version, 'target': target, 'tags': tags})
    return get(db, source, name, version)


def revise(db, source, name, raw, now):
    inputs.require(raw, ('target', 'tags', 'expected'), ())
    expected = inputs.integer(raw['expected'], 1, 2**63 - 2, '当前桥接版号')
    target = inputs.name(raw['target'], '目标频道名')
    if target == source:
        raise invalid('桥接目标必须是另一个频道')
    catalog.get(db, target)
    tags = labels(raw['tags'])
    current = get(db, source, name)
    if current['version'] != expected:
        raise conflict('桥接配置已更新，请重新载入')
    version = expected + 1
    db.execute('INSERT INTO bridges(source_channel,name,version,target_channel,tags,created) VALUES (?,?,?,?,?,?)',
               (source, name, version, target, pack(tags), now))
    journal.append(db, source, now, 'bridge_revised',
                   {'bridge': name, 'version': version, 'target': target, 'tags': tags})
    return get(db, source, name, version)


def matches(rule, tags):
    required = rule['tags']
    if isinstance(required, str):
        required = unpack(required)
    return bool(all(tags.get(key) == value for key, value in required.items()))


def preview(db, source, envelope):
    return [{'bridge': rule['name'], 'version': rule['version'], 'target': rule['target_channel'],
             'match': matches(rule, envelope.tags)} for rule in for_channel(db, source)]


def _insert_target(db, source, rule, message, envelope, active, ready_at, now):
    encoded = envelope.encoded()
    key = db.execute('''INSERT INTO messages(channel,producer,dedup_key,lane,envelope,fingerprint,
                          policy,origin,published,available)
                        VALUES (?,?,?,?,?,?,?,'bridged',?,?)''',
                     (rule['target_channel'], envelope.producer, envelope.dedup_key, envelope.lane,
                      encoded, codec.fingerprint(encoded), active['version'], now, ready_at)).lastrowid
    fanout = {}
    for group in routing.matching(db, rule['target_channel'], envelope.tags):
        fanout[group['name']] = delivery.add(db, key, group['id'], envelope.lane,
                                             active['version'], ready_at)
    link = db.execute('''INSERT INTO bridge_links(bridge_id,source_channel,target_channel,
                          source_message_id,target_message_id,bridge_version,at)
                         VALUES (?,?,?,?,?,?,?)''',
                      (rule['id'], source, rule['target_channel'], message['id'], key,
                       rule['version'], now)).lastrowid
    journal.append(db, rule['target_channel'], now, 'bridge_received',
                   {'bridge': rule['name'], 'bridge_version': rule['version'],
                    'source_channel': source, 'source_message_id': message['id'], 'fanout': fanout}, key)
    journal.append(db, source, now, 'bridged',
                   {'bridge': rule['name'], 'bridge_version': rule['version'],
                    'target': rule['target_channel'], 'target_message_id': key,
                    'target_fanout': fanout, 'link_id': link}, message['id'])
    return key, fanout


def forward(db, source, message, envelope, now):
    """按源频道当前各桥接的最新版本，把一条直接发布转发到匹配目标。"""
    ready_at = now + envelope.delay_seconds
    results = []
    for rule in for_channel(db, source):
        if not matches(rule, envelope.tags):
            continue
        active = policy.read(db, rule['target_channel'])
        target_id, fanout = _insert_target(db, source, rule, message, envelope, active, ready_at, now)
        results.append({'bridge': rule['name'], 'version': rule['version'],
                        'target': rule['target_channel'], 'target_message_id': target_id,
                        'target_fanout': fanout})
    return results


def links_for(db, message_id):
    rows = db.execute('''SELECT l.*, b.name AS bridge_name FROM bridge_links l
                         JOIN bridges b ON b.id=l.bridge_id WHERE l.source_message_id=? ORDER BY l.id''',
                      (message_id,))
    return [dict(row) for row in rows]


def incoming_for(db, message_id):
    rows = db.execute('''SELECT l.*, b.name AS bridge_name FROM bridge_links l
                         JOIN bridges b ON b.id=l.bridge_id WHERE l.target_message_id=?''',
                      (message_id,))
    return [dict(row) for row in rows]


def receipt(db, message_id):
    """从持久化的桥接链接重算回执，重发与配置换版都沿用当时结果。"""
    result = []
    for row in links_for(db, message_id):
        target_fanout = None
        if row['target_message_id'] is not None:
            target_fanout = {item['group_name']: item['id']
                             for item in delivery.for_message(db, row['target_message_id'])
                             if item['parent_id'] is None}
        result.append({'bridge': row['bridge_name'], 'version': row['bridge_version'],
                       'target': row['target_channel'], 'target_message_id': row['target_message_id'],
                       'target_fanout': target_fanout})
    return result
