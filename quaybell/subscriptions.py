from . import catalog, inputs, journal
from .codec import pack, unpack
from .envelope import labels
from .errors import conflict, missing


def decoded(row):
    return dict(row) | {'tags': unpack(row['tags']), 'paused': bool(row['paused'])}


def for_channel(db, channel):
    return [decoded(row) for row in db.execute(
        'SELECT * FROM subscriptions WHERE channel=? ORDER BY id', (channel,))]


def get(db, channel, group):
    row = db.execute('SELECT * FROM subscriptions WHERE channel=? AND name=?', (channel, group)).fetchone()
    if row is None:
        raise missing('找不到消费组')
    return decoded(row)


def create(db, channel, raw, now):
    catalog.get(db, channel)
    inputs.require(raw, ('name',), ('tags',))
    group = inputs.name(raw['name'], '消费组')
    tags = labels(raw.get('tags', {}))
    if any(item['name'] == group for item in for_channel(db, channel)):
        raise conflict('消费组已存在')
    db.execute('INSERT INTO subscriptions(channel,name,tags,created) VALUES (?,?,?,?)',
               (channel, group, pack(tags), now))
    journal.append(db, channel, now, 'subscribed', {'group': group, 'tags': tags})
    return get(db, channel, group)


def pause(db, channel, group, raw, now):
    # The pause entry is also the drain entry, so it accepts optional drain
    # fields (timeout / confirmation / round correlation) in addition to
    # ``paused``.  Validate the one required field strictly but tolerate the
    # drain vocabulary.
    if not isinstance(raw, dict) or 'paused' not in raw:
        inputs.require(raw or {}, ('paused',))
    desired = inputs.boolean(raw['paused'], '暂停状态')
    record = get(db, channel, group)

    # Imported lazily: drain depends on this module, so a module-level import
    # would create an import cycle.
    from . import drain

    descriptor = None
    if desired:
        was_paused = bool(record['paused'])
        if not was_paused:
            db.execute('UPDATE subscriptions SET paused=1 WHERE id=?', (record['id'],))
            journal.append(db, channel, now, 'group_paused', {'group': group, 'drain': True})
            record = get(db, channel, group)
        # Pausing starts (or correlates with) the current drain round.  A
        # genuine pause gesture from an active group opens a fresh round even
        # if an older terminal round exists; a redundant paused=true call is
        # idempotent and just observes the current round.
        drain_raw = dict(raw)
        if not was_paused:
            drain_raw.setdefault('retry', True)
        descriptor = drain.start_from_pause(db, channel, record, drain_raw, now)
        record = get(db, channel, group)
    else:
        # Raises 409 carrying the active round when a drain is unfinished and
        # the caller has not explicitly confirmed the early resume.
        descriptor = drain.resume(db, channel, record, raw, now)
        if record['paused']:
            db.execute('UPDATE subscriptions SET paused=0 WHERE id=?', (record['id'],))
            journal.append(db, channel, now, 'group_resumed',
                           {'group': group,
                            'drain_round': descriptor.get('round') if descriptor else None})
            record = get(db, channel, group)
        if descriptor is not None:
            descriptor['paused'] = False
            descriptor['active'] = False if descriptor.get('state') == 'resumed' else descriptor['active']

    reply = dict(record)
    if descriptor:
        group_id = reply['id']
        reply.update(descriptor)
        # Preserve the subscription identity from the original pause contract;
        # the round identity lives in round/drain_round/round_id.
        reply['id'] = group_id
        reply['subscription_id'] = group_id
        # Keep the nested copy consistent with the merged top-level view.
        reply['drain'] = {k: v for k, v in descriptor.items() if k != 'drain'} | {
            'paused': reply['paused'], 'subscription_id': group_id}
    return reply
