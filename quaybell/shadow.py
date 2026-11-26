"""Shadow subscription observation.

An observation compares the rule a consumer group actually applies to
publications with an unregistered candidate rule set. Every publication
received while the observation runs is matched against both, but only the
registered group creates real deliveries, so a candidate can never hand a
consumer an extra copy.

Each recorded decision carries the baseline rule version and the candidate
version that were used for it, and candidate revisions live in their own
versioned table owned by the observation. A later export can therefore still
separate what each version actually saw even if the candidate was switched or
the real subscription was revised mid-run.
"""
from . import catalog, codec, errors, inputs, journal, subscriptions
from .codec import pack, unpack
from .envelope import labels

MAX_VERSION = 2**31 - 1


def _decoded(row):
    return dict(row) | {'tags': unpack(row['tags'])}


def _subscription(db, subscription_id):
    row = db.execute('SELECT * FROM subscriptions WHERE id=?', (subscription_id,)).fetchone()
    if row is None:
        raise errors.missing('找不到消费组')
    return dict(row)


def _observing(db, subscription_id):
    row = db.execute("SELECT * FROM shadow_observations WHERE subscription_id=? AND state='observing'",
                     (subscription_id,)).fetchone()
    return None if row is None else dict(row)


def _observation(db, observation_id):
    row = db.execute('SELECT * FROM shadow_observations WHERE id=?', (observation_id,)).fetchone()
    if row is None:
        raise errors.missing('找不到影子观测')
    return dict(row)


def _candidates(db, observation_id):
    return [_decoded(row) for row in db.execute(
        'SELECT * FROM shadow_candidates WHERE observation_id=? ORDER BY version', (observation_id,))]


def _candidate(db, observation, version):
    row = db.execute('SELECT * FROM shadow_candidates WHERE observation_id=? AND version=?',
                     (observation['id'], version)).fetchone()
    if row is None:
        raise errors.conflict(f'候选规则版本 {version} 不属于该观测')
    return _decoded(row)


def _scope(db, channel, group):
    """Resolve the group and reject a shadow call aimed at another channel."""
    record = subscriptions.get(db, channel, group)
    if record['channel'] != channel:
        raise errors.missing('找不到消费组')
    return record


def _free_name(db, channel, name, record):
    if name in [item['name'] for item in subscriptions.for_channel(db, channel) if item['id'] != record['id']]:
        raise errors.conflict('候选规则名与该频道的真实消费组重名，影子规则不能借用它的分发')


def _describe(db, observation):
    baseline_rule = subscriptions.rule(db, observation['subscription_id'], observation['baseline_version'])
    candidates = _candidates(db, observation['id'])
    current = candidates[-1] if candidates else None
    return {
        'id': observation['id'], 'channel': observation['channel'],
        'group': _subscription(db, observation['subscription_id'])['name'],
        'state': observation['state'], 'started': observation['started'], 'stopped': observation['stopped'],
        'baseline': {'version': baseline_rule['version'], 'tags': baseline_rule['tags'], 'registered': True},
        'candidate': None if current is None else {
            'version': current['version'], 'name': current['name'],
            'tags': current['tags'], 'registered': False},
        'candidate_history': candidates,
        'deliveries_created': 0,
        'summary': summary(db, observation),
    }


def start(db, channel, group, raw, now):
    inputs.require(raw, ('candidate', 'tags'), ('baseline',))
    catalog.get(db, channel)
    record = _scope(db, channel, group)
    if _observing(db, record['id']) is not None:
        raise errors.conflict('该消费组已有进行中的影子观测')
    name = inputs.name(raw['candidate'], '候选规则名')
    _free_name(db, channel, name, record)
    baseline = inputs.integer(raw['baseline'], 1, MAX_VERSION, '基线规则版本') \
        if raw.get('baseline') is not None else record['version']
    if db.execute('SELECT 1 FROM subscription_rules WHERE subscription_id=? AND version=?',
                  (record['id'], baseline)).fetchone() is None:
        raise errors.invalid('请求的基线规则版本不存在')
    tags = labels(raw['tags'])
    observation = db.execute('''INSERT INTO shadow_observations
                                (channel,subscription_id,baseline_version,candidate_version,state,started)
                                VALUES (?,?,?,?,'observing',?)''',
                             (channel, record['id'], baseline, 1, now)).lastrowid
    db.execute('INSERT INTO shadow_candidates(observation_id,version,name,tags,created) VALUES (?,1,?,?,?)',
               (observation, name, pack(tags), now))
    journal.append(db, channel, now, 'shadow_started',
                   {'observation': observation, 'group': group, 'candidate': name,
                    'baseline_version': baseline, 'candidate_version': 1})
    return _describe(db, _observation(db, observation))


def describe(db, channel, group):
    record = subscriptions.get(db, channel, group)
    observation = _observing(db, record['id'])
    if observation is None:
        return {'group': group, 'observation': None}
    return {'group': group, 'observation': _describe(db, observation)}


def switch(db, channel, group, raw, now):
    inputs.require(raw, ('candidate', 'tags'))
    record = _scope(db, channel, group)
    observation = _observing(db, record['id'])
    if observation is None:
        raise errors.conflict('该消费组没有进行中的影子观测')
    name = inputs.name(raw['candidate'], '候选规则名')
    _free_name(db, channel, name, record)
    tags = labels(raw['tags'])
    version = _candidates(db, observation['id'])[-1]['version'] + 1
    if version > MAX_VERSION:
        raise errors.conflict('候选规则版本已用尽')
    db.execute('INSERT INTO shadow_candidates(observation_id,version,name,tags,created) VALUES (?,?,?,?,?)',
               (observation['id'], version, name, pack(tags), now))
    db.execute('UPDATE shadow_observations SET candidate_version=? WHERE id=?', (version, observation['id']))
    journal.append(db, channel, now, 'shadow_candidate_revised',
                   {'observation': observation['id'], 'candidate': name, 'candidate_version': version})
    return _describe(db, _observation(db, observation['id']))


def stop(db, channel, group, raw, now):
    inputs.require(raw, ())
    record = _scope(db, channel, group)
    observation = _observing(db, record['id'])
    if observation is None:
        raise errors.conflict('该消费组没有进行中的影子观测')
    db.execute("UPDATE shadow_observations SET state='stopped',stopped=? WHERE id=?",
               (now, observation['id']))
    journal.append(db, channel, now, 'shadow_stopped',
                   {'observation': observation['id'], 'group': group, 'stopped': now})
    stopped = _observation(db, observation['id'])
    return {'observation': _describe(db, stopped), 'comparison': comparison(db, stopped)}


def _attempts(db, observation_id):
    return [dict(row) | {'tags': unpack(row['tags'])}
            for row in db.execute(
                '''SELECT e.id AS event_id,e.at AS published,e.repeated,e.deliveries_created,
                          d.* FROM shadow_events e
                   JOIN shadow_decisions d ON d.event_id=e.id
                   WHERE e.observation_id=? ORDER BY e.id''', (observation_id,))]


def _windows(db, observation):
    """One window per real publication attempt, preserving both rules' verdicts."""
    windows = []
    for item in _attempts(db, observation['id']):
        baseline_rule = subscriptions.rule(db, observation['subscription_id'], item['baseline_version'])
        candidate = _candidate(db, observation, item['candidate_version'])
        added = item['baseline'] == 0 and item['candidate'] == 1
        lost = item['baseline'] == 1 and item['candidate'] == 0
        windows.append(dict(
            message_id=item['message_id'], published=item['published'],
            repeated=bool(item['repeated']),
            deliveries_created=item['deliveries_created'],
            producer=item['producer'], dedup_key=item['dedup_key'], lane=item['lane'],
            tags=item['tags'],
            baseline={'version': baseline_rule['version'], 'tags': baseline_rule['tags'],
                      'matched': bool(item['baseline'])},
            candidate={'version': candidate['version'], 'name': candidate['name'],
                       'tags': candidate['tags'], 'matched': bool(item['candidate'])},
            delivered=bool(item['delivered']),
            added=bool(added), lost=bool(lost), differing=bool(added or lost),
            # "delivered" says the group holds a copy of this message; the
            # per-attempt count says whether this attempt added one.
            # A lost match is only a real loss if this attempt could have
            # produced a copy: replays keep an earlier receipt and add none.
            realized_lost=bool(lost and item['deliveries_created'] > 0),
            replay_lost=bool(lost and item['deliveries_created'] == 0),
        ))
    return windows


def _counts(windows):
    return {
        'attempts': len(windows),
        'distinct_messages': len({item['message_id'] for item in windows}),
        'baseline_matched': sum(item['baseline']['matched'] for item in windows),
        'candidate_matched': sum(item['candidate']['matched'] for item in windows),
        'added': sum(item['added'] for item in windows),
        'lost': sum(item['lost'] for item in windows),
        'differing': sum(item['differing'] for item in windows),
        'delivered_to_real_group': sum(item['delivered'] for item in windows),
        'replayed_attempts': sum(item['repeated'] for item in windows),
    }


def _segments(db, observation):
    buckets = {}
    for window in _windows(db, observation):
        buckets.setdefault((window['baseline']['version'], window['candidate']['version']), []).append(window)
    return [{
        'baseline_rule': baseline_version,
        'candidate_rule': candidate_version,
        'candidate_name': windows[0]['candidate']['name'],
        'candidate_tags': windows[0]['candidate']['tags'],
        'first_published': windows[0]['published'],
        'last_published': windows[-1]['published'],
        'counts': _counts(windows),
        'windows': windows,
    } for (baseline_version, candidate_version), windows in buckets.items()]


def summary(db, observation):
    return _counts(_windows(db, observation))


def comparison(db, observation):
    """Added matches, lost matches, and their denominator, split by rule versions."""
    windows = _windows(db, observation)
    counts = _counts(windows)
    return {
        'observation_id': observation['id'],
        'state': observation['state'],
        'added_matches': [item for item in windows if item['added']],
        'lost_matches': [item for item in windows if item['lost']],
        'realized_lost_matches': [item for item in windows if item['realized_lost']],
        'replay_lost_matches': [item for item in windows if item['replay_lost']],
        'denominator': {key: counts[key] for key in
                        ('attempts', 'distinct_messages', 'replayed_attempts',
                         'baseline_matched', 'candidate_matched', 'differing',
                         'delivered_to_real_group')},
        'segments': _segments(db, observation),
    }


def record(db, channel, envelope, message_id, now, repeated=False, deliveries_created=0):
    """Evaluate running observations for one publication attempt.

    This only reads and appends observation facts; it never creates deliveries.
    """
    results = []
    observations = [dict(row) for row in db.execute(
        "SELECT * FROM shadow_observations WHERE channel=? AND state='observing' ORDER BY id", (channel,))]
    for observation in observations:
        subscription = _subscription(db, observation['subscription_id'])
        version = subscription['version']
        baseline = subscriptions.rule(db, observation['subscription_id'], version)['tags']
        candidate = _candidates(db, observation['id'])[-1]
        delivered = db.execute('SELECT 1 FROM deliveries WHERE subscription_id=? AND message_id=?',
                               (observation['subscription_id'], message_id)).fetchone() is not None
        baseline_hit = int(all(envelope.tags.get(key) == value for key, value in baseline.items()))
        candidate_hit = int(all(envelope.tags.get(key) == value for key, value in candidate['tags'].items()))
        event = db.execute('''INSERT INTO shadow_events(observation_id,message_id,at,repeated,deliveries_created)
                             VALUES (?,?,?,?,?)''',
                           (observation['id'], message_id, now, int(repeated), deliveries_created)).lastrowid
        db.execute('''INSERT INTO shadow_decisions(event_id,observation_id,message_id,published,
                     producer,dedup_key,lane,tags,baseline_version,candidate_version,baseline,candidate,delivered)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                   (event, observation['id'], message_id, now, envelope.producer, envelope.dedup_key,
                    envelope.lane, pack(envelope.tags), version, candidate['version'],
                    baseline_hit, candidate_hit, int(delivered)))
        journal.append(db, channel, now, 'shadow_seen',
                       {'observation': observation['id'], 'message_id': message_id,
                        'repeated': bool(repeated), 'deliveries_created': deliveries_created,
                        'baseline_version': version, 'candidate_version': candidate['version'],
                        'baseline': bool(baseline_hit), 'candidate': bool(candidate_hit),
                        'delivered': bool(delivered)}, message_id)
        results.append({'observation_id': observation['id'], 'message_id': message_id,
                        'baseline_version': version, 'candidate_version': candidate['version']})
    return results


def export(db, channel, group, now):
    record = subscriptions.get(db, channel, group)
    observations = [dict(row) for row in db.execute(
        'SELECT * FROM shadow_observations WHERE subscription_id=? ORDER BY id', (record['id'],))]
    items = [{'observation': _describe(db, observation), 'comparison': comparison(db, observation)}
             for observation in observations]
    data = dict(format='quaybell-shadow/1', channel=channel, group=group, observed_at=now,
                observations=items)
    data['sha256'] = codec.fingerprint(codec.pack(data))
    return (codec.pack(data) + '\n').encode('ascii')
