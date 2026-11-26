"""可重新分配的固定分片归属。

一个消费组维护单调递增的分配版本（generation）。顺序键（消息里的 ``lane``）
经稳定哈希落到固定数量的分片，每个分片在当前版本只有一个归属工作进程。
取件在事务内按当前归属过滤，并把发放时的分配版本盖到尝试上，供操作者核对
“旧版租约”和跨版本交接。重新分配立即改写归属表；未闭合的租约在其截止前仍是
该次尝试的唯一处理凭据，配合同键队首阻挡，保证同一顺序键任一时刻只有一个进程
能够实际处理。
"""
from . import codec, inputs, journal, subscriptions
from .errors import conflict, invalid

MAX_PARTITIONS = 64
COUNT_KEYS = ('partitions', 'partition_count', 'partition_total', 'num_partitions',
              'n_partitions', 'partition_num', 'shards', 'shard_count', 'shard_total',
              'count', 'parts')
WORKER_KEYS = ('workers', 'worker_names', 'members', 'processes', 'assignees', 'consumers')
MAP_KEYS = ('assignment', 'assignments', 'mapping', 'partition_map', 'owner_map',
            'shard_map', 'owners')
WORKER_NAME_KEYS = ('worker', 'name', 'worker_id', 'worker_name', 'member', 'consumer',
                    'consumer_id', 'assignee', 'owner')


def partition_for(lane, count):
    """顺序键 -> 分片。复用本项目规范摘要（sha-256）后取模，稳定可复算。"""
    return int(codec.fingerprint(str(lane)), 16) % count


def _opt_int(raw, keys, label):
    for key in keys:
        if key in raw and raw[key] is not None:
            return inputs.integer(raw[key], 1, MAX_PARTITIONS, label), key
    return None, None


def _worker(value):
    return inputs.worker_name(value)


def resolve_worker(raw):
    for key in WORKER_NAME_KEYS:
        if isinstance(raw.get(key), str) and raw[key].strip():
            return _worker(raw[key])
    raise invalid('须提供工作进程名 worker')


def _worker_entries(value):
    if value is None:
        return None
    if not isinstance(value, list) or not value or len(value) > MAX_PARTITIONS:
        raise invalid('工作进程名单须为 1～64 项的数组')
    workers = []
    for entry in value:
        if isinstance(entry, str):
            name = entry
        elif isinstance(entry, dict):
            name = None
            for key in WORKER_NAME_KEYS:
                candidate = entry.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    name = candidate
                    break
            if name is None:
                raise invalid('工作进程条目须包含 name 或 worker')
        else:
            raise invalid('工作进程名单条目须为名称或对象')
        name = _worker(name)
        if name not in workers:
            workers.append(name)
    return workers


def _normalize_map(value):
    if value is None:
        return None
    result = {}
    if isinstance(value, dict):
        for raw_partition, raw_owner in value.items():
            try:
                partition = int(raw_partition)
            except (TypeError, ValueError):
                raise invalid('分片编号须为 0 起的整数') from None
            owner = raw_owner
            if isinstance(owner, dict):
                owner = next((owner.get(key) for key in WORKER_NAME_KEYS
                              if isinstance(owner.get(key), str)), None)
            if not isinstance(owner, str):
                raise invalid(f'分片 {partition} 须指定归属进程')
            result[partition] = _worker(owner)
    elif isinstance(value, list):
        for entry in value:
            if not isinstance(entry, dict):
                raise invalid('分片映射条目须为对象')
            partition = next((entry.get(key) for key in ('partition', 'id', 'index', 'shard')
                              if isinstance(entry.get(key), int) and not isinstance(entry.get(key), bool)),
                             None)
            owner = next((entry.get(key) for key in WORKER_NAME_KEYS
                          if isinstance(entry.get(key), str)), None)
            if partition is None or owner is None:
                raise invalid('分片映射条目须包含 partition 和 worker/owner')
            result[partition] = _worker(owner)
    else:
        raise invalid('分片映射须为对象或数组')
    if not result:
        raise invalid('分片映射不能为空')
    for partition in result:
        if not 0 <= partition < MAX_PARTITIONS:
            raise invalid('分片编号须为 0～63')
    return result


def _distribute(count, workers, strategy):
    if strategy in ('range', 'sticky', 'sticky-range'):
        width = len(workers)
        return {partition: workers[min(width - 1, partition * width // count)]
                for partition in range(count)}, 'range'
    return {partition: workers[partition % len(workers)] for partition in range(count)}, 'round_robin'


def current_row(db, channel, group_name):
    return db.execute('''SELECT * FROM assignment_versions
                         WHERE channel=? AND group_name=?
                         ORDER BY version DESC LIMIT 1''', (channel, group_name)).fetchone()


def current(db, channel, group_name):
    row = current_row(db, channel, group_name)
    if row is None:
        return None
    record = dict(row)
    record['plan'] = codec.unpack(record['plan'])
    record['owners'] = [dict(item) for item in db.execute(
        'SELECT partition,owner FROM partition_owners WHERE channel=? AND group_name=? AND version=? ORDER BY partition',
        (channel, group_name, record['version']))]
    record['owner_map'] = {item['partition']: item['owner'] for item in record['owners']}
    return record


def summary(db, channel, group_name):
    record = current_row(db, channel, group_name)
    channel_row = db.execute('SELECT partition_count FROM channels WHERE name=?', (channel,)).fetchone()
    if record is None:
        return {'configured': False, 'version': 0, 'assignment_version': 0,
                'partition_count': channel_row['partition_count'] if channel_row else None,
                'partitions': [], 'workers': []}
    owners = [dict(item) for item in db.execute(
        'SELECT partition,owner FROM partition_owners WHERE channel=? AND group_name=? AND version=? ORDER BY partition',
        (channel, group_name, record['version']))]
    worker_names = []
    for item in owners:
        if item['owner'] not in worker_names:
            worker_names.append(item['owner'])
    return {'configured': True, 'version': record['version'],
            'assignment_version': record['version'], 'generation': record['version'],
            'partition_count': record['partition_count'],
            'strategy': record['strategy'], 'partitions': owners, 'workers': worker_names}


def _registered(db, channel, group_name):
    rows = db.execute('''SELECT worker FROM group_members
                         WHERE channel=? AND group_name=? ORDER BY seen ASC, worker ASC''',
                      (channel, group_name)).fetchall()
    return [row['worker'] for row in rows]


def _prior_roster(prior):
    if prior is None:
        return []
    return [prior['owner_map'][partition] for partition in sorted(prior['owner_map'])]


def _lease_mode(raw):
    policy = raw.get('lease_policy') or raw.get('lease_strategy') or raw.get('on_lease')
    if policy in ('honor', 'grace', 'drain', 'none', 'wait'):
        return 'honor'
    if policy in ('all', 'strict', 'immediate', 'fence_all'):
        return 'all'
    if policy in ('move', 'moved', 'on_move', 'fence_moved'):
        return 'move'
    if raw.get('fence') is True or raw.get('preempt') is True or raw.get('takeover') is True:
        return 'move'
    return 'move'


def fence_open_leases(db, sub, owner_map, count, now, mode='move', current_version=None):
    """使失去归属的进程持有的旧租约失效，并把旧债务立即放回可领取状态。

    mode:
    - 'move'（默认）：仅当顺序键在新版本改属其他进程时围栏旧租约；
      归属未变的进程可继续凭原凭据处置，直到租约到期。
    - 'all'：围栏本组所有开放尝试（无论归属是否变化）。
    - 'honor'：不主动围栏，沿用租约到期/回收流程。
    """
    if mode == 'honor':
        return []
    rows = db.execute('''SELECT a.id AS attempt_id,a.delivery_id,a.worker,a.assignment_version,
                                d.lane,d.message_id
                         FROM attempts a JOIN deliveries d ON d.id=a.delivery_id
                         WHERE d.subscription_id=? AND a.outcome='open' ''', (sub['id'],)).fetchall()
    fenced = []
    for row in rows:
        item = dict(row)
        partition = partition_for(item['lane'], count) if count else None
        new_owner = owner_map.get(partition) if owner_map else None
        moved = new_owner is not None and new_owner != item['worker']
        stale_generation = (mode == 'all' and current_version is not None
                            and item['assignment_version'] is not None
                            and item['assignment_version'] < current_version)
        if mode != 'all' and not moved:
            continue
        if mode == 'all' and not (moved or stale_generation):
            continue
        reason = '分片重新分配，旧版租约已围栏' if moved else '分配版本更新，旧代际租约已围栏'
        db.execute("UPDATE attempts SET outcome='expired',finished=?,reason=? WHERE id=?",
                   (now, reason, item['attempt_id']))
        db.execute("UPDATE deliveries SET state='waiting',ready_at=? WHERE id=?",
                   (now, item['delivery_id']))
        event = {'delivery_id': item['delivery_id'], 'attempt_id': item['attempt_id'],
                 'lane': item['lane'], 'partition': partition, 'old_worker': item['worker'],
                 'owner': new_owner, 'assignment_version': item['assignment_version'],
                 'reason': 'ownership_moved' if moved else 'stale_generation'}
        journal.append(db, sub['channel'], now, 'lease_fenced', event, item['message_id'])
        fenced.append(event)
    return fenced


def configure(db, channel, group_name, raw, now, *, rebalance=False):
    sub = subscriptions.get(db, channel, group_name)
    channel_row = db.execute('SELECT * FROM channels WHERE name=?', (channel,)).fetchone()
    prior = current(db, channel, group_name)

    expected = raw.get('expected', raw.get('expected_version'))
    if expected is not None:
        expected = inputs.integer(expected, 0, 2 ** 63 - 2, '预期分配版本')
        if expected == 0:
            if prior is not None:
                raise conflict('分片分配已存在更高版本，请重新载入')
        elif prior is None or prior['version'] != expected:
            raise conflict('分片分配版本已更新，请重新载入')

    count, count_key_present = _opt_int(raw, COUNT_KEYS, '分片数量')
    explicit = _normalize_map(next((raw[key] for key in MAP_KEYS if key in raw), None))
    workers = _worker_entries(next((raw[key] for key in WORKER_KEYS if key in raw), None))

    strategy = raw.get('strategy') or raw.get('assignor') or ('explicit' if explicit else None)
    if strategy not in (None, 'round_robin', 'range', 'sticky', 'sticky-range', 'explicit'):
        raise invalid('分配策略须为 round_robin、range 或 explicit')

    if explicit is not None:
        if count is None:
            count = prior['partition_count'] if prior else (
                channel_row['partition_count'] if channel_row and channel_row['partition_count']
                else max(explicit) + 1)
        owner_map = dict(prior['owner_map']) if prior else {}
        owner_map.update(explicit)
        missing = [partition for partition in range(count) if partition not in owner_map]
        if missing:
            raise invalid(f"分片 {missing} 尚未指定归属进程")
        owner_map = {partition: owner_map[partition] for partition in range(count)}
        strategy_name = 'explicit'
    else:
        if workers is None:
            workers = _registered(db, channel, group_name)
        if not workers and prior is not None:
            workers = _prior_roster(prior)
        if not workers:
            if rebalance:
                raise conflict('还没有可用于重新分配的工作进程，请先登记 workers 或成员')
            raise invalid('须提供 workers 工作进程名单或 assignment 分片映射')
        if count is None:
            count = channel_row['partition_count'] if channel_row and channel_row['partition_count'] else len(workers)
        owner_map, strategy_name = _distribute(count, workers, strategy or 'round_robin')

    if len(set(owner_map.values())) > MAX_PARTITIONS:
        raise invalid('工作进程数量超出上限')

    if prior is not None and prior['partition_count'] == count and prior['owner_map'] == owner_map \
            and not raw.get('force'):
        result = view(db, channel, group_name, now)
        return result | {'repeated': True, 'changed': False, 'version': prior['version']}

    version = prior['version'] + 1 if prior else 1
    plan = codec.pack({'partition_count': count, 'strategy': strategy_name,
                      'owner_map': {str(p): w for p, w in owner_map.items()}})
    db.execute('''INSERT INTO assignment_versions(channel,group_name,version,partition_count,strategy,plan,created)
                  VALUES (?,?,?,?,?,?,?)''',
               (channel, group_name, version, count, strategy_name, plan, now))
    db.executemany('''INSERT INTO partition_owners(channel,group_name,version,partition,owner)
                      VALUES (?,?,?,?,?)''',
                   [(channel, group_name, version, partition, owner)
                    for partition, owner in sorted(owner_map.items())])
    db.executemany('''INSERT INTO group_members(channel,group_name,worker,seen) VALUES (?,?,?,?)
                      ON CONFLICT(channel,group_name,worker) DO UPDATE SET seen=excluded.seen''',
                   [(channel, group_name, owner, now) for owner in sorted(set(owner_map.values()))])
    if count_key_present or channel_row['partition_count'] is None:
        db.execute('UPDATE channels SET partition_count=? WHERE name=?', (count, channel))
    lease_mode = _lease_mode(raw)
    fenced = fence_open_leases(db, sub, owner_map, count, now, lease_mode, version)
    kind = 'assignment_rebalanced' if prior else 'assignment_configured'
    journal.append(db, channel, now, kind,
                   {'group': group_name, 'version': version, 'partition_count': count,
                    'strategy': strategy_name, 'lease_policy': lease_mode,
                    'fenced_leases': len(fenced),
                    'owners': {str(p): w for p, w in sorted(owner_map.items())}})
    return view(db, channel, group_name, now) | {'repeated': False, 'changed': True,
                                                 'lease_policy': lease_mode,
                                                 'fenced_leases': fenced}


def configure_channel(db, channel, raw, now):
    from . import catalog
    catalog.get(db, channel)
    group_name = raw.get('group') or raw.get('group_name')
    if group_name is None:
        groups = subscriptions.for_channel(db, channel)
        if len(groups) == 1:
            group_name = groups[0]['name']
        elif groups:
            raise invalid('频道有多个消费组，请在请求中指定 group')
        else:
            count, _ = _opt_int(raw, COUNT_KEYS, '分片数量')
            if count is None:
                raise invalid('频道还没有消费组；请先建组，或提供 group 与分片配置')
            db.execute('UPDATE channels SET partition_count=? WHERE name=?', (count, channel))
            journal.append(db, channel, now, 'partitions_configured', {'partition_count': count})
            return channel_view(db, channel, now)
    return configure(db, channel, group_name, raw, now)


def _lane_states(db, sub, now, current_version):
    lane_names = [row['lane'] for row in db.execute(
        'SELECT DISTINCT lane FROM deliveries WHERE subscription_id=? ORDER BY lane', (sub['id'],))]
    rows = db.execute('''SELECT d.id,d.lane,d.state,d.ready_at,d.attempt_count,
                                a.expires AS lease_expires,a.worker AS lease_worker,
                                a.number AS attempt_number,a.assignment_version AS attempt_version,
                                a.partition AS attempt_partition
                         FROM deliveries d
                         LEFT JOIN attempts a ON a.delivery_id=d.id AND a.outcome='open'
                         WHERE d.subscription_id=? AND d.state IN ('waiting','leased')
                         ORDER BY d.lane,d.id''', (sub['id'],)).fetchall()
    grouped = {}
    for row in rows:
        grouped.setdefault(row['lane'], []).append(dict(row))
    states = {}
    for lane in lane_names:
        unresolved = grouped.get(lane, [])
        if not unresolved:
            last = db.execute(
                'SELECT state FROM deliveries WHERE subscription_id=? AND lane=? ORDER BY id DESC LIMIT 1',
                (sub['id'], lane)).fetchone()
            states[lane] = {'state': 'idle', 'cause': 'idle', 'pending': 0, 'debt': False,
                            'last_terminal_state': last['state'] if last else None,
                            'head_delivery_id': None, 'ready_at': None, 'expires': None,
                            'open_attempt': None}
            continue
        head = unresolved[0]
        tail = len(unresolved) - 1
        open_attempt = None
        if head['state'] == 'leased':
            state = 'expired' if head['lease_expires'] is not None and head['lease_expires'] <= now else 'leased'
            open_attempt = {'number': head['attempt_number'], 'worker': head['lease_worker'],
                            'expires': head['lease_expires'],
                            'assignment_version': head['attempt_version'],
                            'partition': head['attempt_partition'],
                            'stale': head['attempt_version'] is not None
                            and current_version != head['attempt_version']}
        elif head['ready_at'] > now:
            state = 'retry' if head['attempt_count'] > 0 else 'delayed'
        else:
            state = 'ready'
        debt = head['attempt_count'] > 0
        states[lane] = {'state': state, 'cause': state, 'pending': len(unresolved),
                        'queued_behind': tail, 'debt': debt,
                        'paused': bool(sub['paused']),
                        'head_delivery_id': head['id'], 'ready_at': head['ready_at'],
                        'expires': head['lease_expires'], 'open_attempt': open_attempt}
    return lane_names, states


def key_rows(db, channel, group_name, now, extra_lanes=()):
    sub = subscriptions.get(db, channel, group_name)
    record = current(db, channel, group_name)
    current_version = record['version'] if record else 0
    lane_names, states = _lane_states(db, sub, now, current_version)
    for lane in extra_lanes:
        if lane not in states:
            lane_names.append(lane)
            states[lane] = {'state': 'empty', 'cause': 'empty', 'pending': 0, 'debt': False,
                            'head_delivery_id': None, 'ready_at': None, 'expires': None,
                            'open_attempt': None}
    rows = []
    owner_map = record['owner_map'] if record else {}
    count = record['partition_count'] if record else None
    version = record['version'] if record else 0
    for lane in lane_names:
        partition = partition_for(lane, count) if count else None
        owner = owner_map.get(partition) if record else None
        state = states[lane]
        open_attempt = state.get('open_attempt')
        lease_worker = open_attempt['worker'] if open_attempt else None
        if open_attempt and owner is not None and lease_worker != owner:
            handoff = 'fenceable'
        elif open_attempt:
            handoff = 'lease_bound'
        elif state['state'] in ('ready', 'retry', 'delayed', 'blocked', 'paused', 'expired'):
            handoff = 'ready_for_owner'
        else:
            handoff = 'idle'
        rows.append({
            'lane': lane, 'ordering_key': lane, 'key': lane,
            'partition': partition, 'shard': partition,
            'owner': owner, 'worker': owner, 'assignee': owner,
            'current_owner': lease_worker or owner,
            'serving_worker': lease_worker or owner,
            'lease_worker': lease_worker, 'active_worker': lease_worker,
            'handoff': handoff,
            'assignment_version': version if record else None, 'version': version if record else 0,
            'authorized_worker': owner, **state,
        })
    return rows, record, sub


def view(db, channel, group_name, now, query=None):
    query = query or {}
    extra = [item for item in str(query.get('keys', '')).split(',') if item]
    rows, record, sub = key_rows(db, channel, group_name, now, extra)
    worker_filter = query.get('worker')
    if worker_filter:
        worker_filter = inputs.worker_name(worker_filter)
        for row in rows:
            row['authorized'] = row['owner'] == worker_filter
            row['mine'] = row['owner'] == worker_filter
        if query.get('all') not in ('1', 'true', 'yes') and query.get('include_others') not in ('1', 'true', 'yes'):
            rows = [row for row in rows if row['owner'] == worker_filter]
    members = {row['worker']: row['seen'] for row in db.execute(
        'SELECT worker,seen FROM group_members WHERE channel=? AND group_name=?', (channel, group_name))}
    partitions_view = []
    worker_partitions = {}
    if record:
        for item in record['owners']:
            partition, owner = item['partition'], item['owner']
            partitions_view.append({'partition': partition, 'shard': partition, 'owner': owner,
                                    'worker': owner, 'assignee': owner,
                                    'assignment_version': record['version']})
            worker_partitions.setdefault(owner, []).append(partition)
    worker_names = list(members)
    for owner in worker_partitions:
        if owner not in worker_names:
            worker_names.append(owner)
    workers_view = [{'name': worker, 'worker': worker,
                     'partitions': sorted(worker_partitions.get(worker, [])),
                     'last_seen': members.get(worker),
                     'registered': worker in members,
                     'owner': worker in worker_partitions}
                    for worker in sorted(worker_names)]
    base = {'channel': channel, 'group': group_name, 'group_name': group_name,
            'observed_at': now, 'hashing': 'sha256-hex-mod'}
    if record is None:
        channel_row = db.execute('SELECT partition_count FROM channels WHERE name=?', (channel,)).fetchone()
        return base | {'configured': False, 'version': 0, 'assignment_version': 0, 'generation': 0,
                       'partition_count': channel_row['partition_count'] if channel_row else None,
                       'partitions': [], 'workers': workers_view, 'members': workers_view,
                       'keys': rows, 'ownership': rows}
    return base | {'configured': True, 'version': record['version'],
                   'assignment_version': record['version'], 'generation': record['version'],
                   'partition_count': record['partition_count'], 'strategy': record['strategy'],
                   'partitions': partitions_view,
                   'worker_assignments': [{'worker': w, 'partitions': ps}
                                          for w, ps in sorted(worker_partitions.items())],
                   'workers': workers_view, 'members': workers_view,
                   'keys': rows, 'ownership': rows}


def channel_view(db, channel, now, query=None):
    from . import catalog
    catalog.get(db, channel)
    channel_row = db.execute('SELECT * FROM channels WHERE name=?', (channel,)).fetchone()
    groups = []
    for group in subscriptions.for_channel(db, channel):
        item = summary(db, channel, group['name'])
        item['group'] = group['name']
        item['name'] = group['name']
        item['paused'] = group['paused']
        groups.append(item)
    return {'channel': channel, 'observed_at': now,
            'partition_count': channel_row['partition_count'],
            'partitions': channel_row['partition_count'],
            'groups': groups, 'assignments': groups}


def authorize(db, channel, group_name, raw, now):
    worker = resolve_worker(raw)
    lanes = raw.get('lanes', raw.get('keys', raw.get('ordering_keys', ())))
    if isinstance(lanes, str):
        lanes = [item for item in lanes.split(',') if item]
    if not isinstance(lanes, (list, tuple)):
        raise invalid('lanes 须为顺序键数组')
    lanes = [inputs.token(item, '顺序键') if isinstance(item, str) else invalid('顺序键须为字符串')
             for item in lanes]
    partitions = raw.get('partitions')
    requested_partitions = None
    if partitions is not None:
        if not isinstance(partitions, list) or not partitions:
            raise invalid('partitions 须为非空整数数组')
        requested_partitions = {inputs.integer(item, 0, MAX_PARTITIONS - 1, '分片编号')
                                for item in partitions}
    rows, record, _ = key_rows(db, channel, group_name, now, lanes)
    if record is None:
        decisions = [{'lane': lane, 'authorized': True, 'reason': 'assignment_unconfigured'}
                     for lane in lanes]
        return {'channel': channel, 'group': group_name, 'worker': worker,
                'configured': False, 'version': 0, 'assignment_version': 0,
                'partitions': None, 'authorized': True, 'decisions': decisions}
    owned = {partition for partition, owner in record['owner_map'].items() if owner == worker}
    if requested_partitions is not None and not requested_partitions <= owned:
        raise forbidden_owner(requested_partitions - owned)
    decisions = []
    for row in rows:
        authorized = row['owner'] == worker
        decisions.append({'lane': row['lane'], 'ordering_key': row['lane'],
                          'partition': row['partition'], 'owner': row['owner'],
                          'worker': row['owner'], 'authorized': authorized,
                          'reason': 'owner' if authorized else 'owned_by_other'})
    return {'channel': channel, 'group': group_name, 'worker': worker,
            'configured': True, 'version': record['version'],
            'assignment_version': record['version'], 'generation': record['version'],
            'partition_count': record['partition_count'], 'partitions': sorted(owned),
            'authorized': bool(owned) if requested_partitions is None else True,
            'worker_authorized': bool(owned), 'decisions': decisions}


def forbidden_owner(missing):
    from .errors import forbidden
    return forbidden(f'该工作进程不拥有分片 {sorted(missing)}，不能领取或操作其消息')


def claim_guard(db, channel, group_name, worker, raw):
    """返回 (assignment_record, allowed_partitions)；不合法的显式分片请求直接 403。"""
    record = current(db, channel, group_name)
    if record is None:
        return None, None
    owned = {partition for partition, owner in record['owner_map'].items() if owner == worker}
    requested = raw.get('partitions')
    if 'partition' in raw and raw['partition'] is not None:
        single = inputs.integer(raw['partition'], 0, MAX_PARTITIONS - 1, '分片编号')
        requested = [single] if requested is None else list(requested) + [single]
    if requested is not None:
        if not isinstance(requested, list) or not requested:
            raise invalid('partitions 须为分片编号数组')
        requested_set = {inputs.integer(item, 0, MAX_PARTITIONS - 1, '分片编号') for item in requested}
        if not requested_set <= owned:
            raise forbidden_owner(requested_set - owned)
        return record, requested_set
    return record, owned


def claim_context(record, worker):
    if record is None:
        return {'configured': False, 'version': 0, 'assignment_version': 0, 'generation': 0,
                'partition_count': None, 'worker': worker, 'partitions': None,
                'authorized': True, 'worker_authorized': True, 'assignment': None}
    owned = sorted(partition for partition, owner in record['owner_map'].items() if owner == worker)
    small = {'configured': True, 'version': record['version'],
             'assignment_version': record['version'], 'generation': record['version'],
             'partition_count': record['partition_count'], 'worker': worker,
             'partitions': owned, 'authorized': bool(owned), 'worker_authorized': bool(owned)}
    return small | {'assignment': small}


def fence_now(db, channel, group_name, raw, now):
    """操作者显式围栏：使非当前归属进程（或所有旧代际）的开放租约立即失效。"""
    sub = subscriptions.get(db, channel, group_name)
    record = current(db, channel, group_name)
    if record is None:
        raise conflict('该消费组尚未配置分片分配')
    mode = 'all' if (raw or {}).get('all') is True else 'move'
    fenced = fence_open_leases(db, sub, record['owner_map'], record['partition_count'], now,
                               mode, record['version'])
    if fenced:
        journal.append(db, channel, now, 'leases_fenced',
                       {'group': group_name, 'version': record['version'],
                        'fenced_leases': len(fenced)})
    return {'channel': channel, 'group': group_name, 'version': record['version'],
            'assignment_version': record['version'], 'mode': mode, 'fenced_leases': fenced}


def touch_member(db, channel, group_name, worker, now):
    subscriptions.get(db, channel, group_name)
    worker = _worker(worker)
    db.execute('''INSERT INTO group_members(channel,group_name,worker,seen) VALUES (?,?,?,?)
                  ON CONFLICT(channel,group_name,worker) DO UPDATE SET seen=excluded.seen''',
               (channel, group_name, worker, now))
    return member_view(db, channel, group_name)


def touch_from_body(db, channel, group_name, raw, now):
    return touch_member(db, channel, group_name, resolve_worker(raw), now)


_CONFIG_TRIGGERS = frozenset(
    COUNT_KEYS + WORKER_KEYS + MAP_KEYS + (
        'strategy', 'assignor', 'lease_policy', 'lease_strategy', 'fence',
        'preempt', 'takeover', 'use_members', 'force'))


def join(db, channel, group_name, raw, now):
    """工作进程入组/心跳；若同时携带分片配置字段，则在同一次调用里完成再分配。"""
    worker = resolve_worker(raw)
    if any(key in raw for key in _CONFIG_TRIGGERS):
        result = configure(db, channel, group_name, raw, now, rebalance=bool(raw.get('rebalance')))
        result['worker'] = worker
        result['worker_partitions'] = sorted(
            p for p, w in result['owner_map'].items() if w == worker) if result.get('configured') else []
        return result
    view_data = touch_member(db, channel, group_name, worker, now)
    me = next((m for m in view_data['members'] if m['worker'] == worker), None)
    return view_data | {'worker': worker, 'member': me,
                        'worker_partitions': me['partitions'] if me else []}


def leave(db, channel, group_name, worker):
    subscriptions.get(db, channel, group_name)
    worker = _worker(worker)
    db.execute('DELETE FROM group_members WHERE channel=? AND group_name=? AND worker=?',
               (channel, group_name, worker))
    return {'channel': channel, 'group': group_name, 'worker': worker, 'left': True}


def leave_from_body(db, channel, group_name, raw):
    return leave(db, channel, group_name, resolve_worker(raw))


def member_view(db, channel, group_name):
    record = current(db, channel, group_name)
    worker_partitions = {}
    version = record['version'] if record else 0
    if record:
        for partition, owner in record['owner_map'].items():
            worker_partitions.setdefault(owner, []).append(partition)
    members = [dict(row) for row in db.execute(
        'SELECT worker,seen AS last_seen FROM group_members WHERE channel=? AND group_name=? ORDER BY worker',
        (channel, group_name))]
    for member in members:
        member['name'] = member['worker']
        member['partitions'] = sorted(worker_partitions.get(member['worker'], []))
    known = {member['worker'] for member in members}
    for owner, partitions in sorted(worker_partitions.items()):
        if owner not in known:
            members.append({'worker': owner, 'name': owner, 'last_seen': None,
                            'partitions': sorted(partitions)})
    return {'channel': channel, 'group': group_name,
            'version': version, 'assignment_version': version,
            'members': members, 'workers': members}
