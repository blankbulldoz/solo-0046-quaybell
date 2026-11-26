from . import assignments, attempts, delivery, eligibility, expiry, inputs, journal, policy, publication, subscriptions

# 开启分片归属后，一次扫描的同键队首上限；本机 SQLite 上足够覆盖演练与值守规模。
FILTER_SCAN = 2000


def _worker(raw):
    value = raw.get('worker', raw.get('worker_id', raw.get('worker_name', raw.get('consumer'))))
    return inputs.worker_name(value or '', '工作进程')


def _declared_filter(db, channel, raw):
    """未建立属主表时，允许进程用 partition(s) 声明自己接的固定分片（仅过滤）。"""
    requested = raw.get('partitions')
    if 'partition' in raw and raw['partition'] is not None:
        single = inputs.integer(raw['partition'], 0, assignments.MAX_PARTITIONS - 1, '分片编号')
        requested = [single] if requested is None else list(requested) + [single]
    if requested is None:
        return None, None
    if not isinstance(requested, list) or not requested:
        raise inputs.invalid('partitions 须为分片编号数组')
    selected = {inputs.integer(item, 0, assignments.MAX_PARTITIONS - 1, '分片编号')
                for item in requested}
    count = raw.get('partition_count', raw.get('partitions_total'))
    if count is None:
        row = db.execute('SELECT partition_count FROM channels WHERE name=?', (channel,)).fetchone()
        count = row['partition_count'] if row is not None else None
    if count is None:
        count = max(selected) + 1
    count = inputs.integer(count, 1, assignments.MAX_PARTITIONS, '分片数量')
    selected = {p for p in selected if p < count}
    return count, selected


def claim(db, channel, group, raw, now):
    worker = _worker(raw)
    limit = inputs.integer(raw.get('limit', 1), 1, 30, '领取上限')
    sub = subscriptions.get(db, channel, group)
    assignments.touch_member(db, channel, group, worker, now)
    record, allowed = assignments.claim_guard(db, channel, group, worker, raw)
    declared_count, declared = (None, None) if record is not None else _declared_filter(db, channel, raw)
    context = assignments.claim_context(record, worker)
    if sub['paused']:
        return {'paused': True, 'items': [], **context}
    expiry.reap(db, now, 100, sub['id'])
    filtering = record is not None or declared is not None
    scan_limit = limit if not filtering else FILTER_SCAN
    results = []
    for item in eligibility.ready(db, sub['id'], now, scan_limit):
        partition = None
        if record is not None:
            partition = assignments.partition_for(item['lane'], record['partition_count'])
            if partition not in allowed:
                continue
        elif declared is not None:
            partition = assignments.partition_for(item['lane'], declared_count)
            if partition not in declared:
                continue
        rule = policy.read(db, channel, item['policy'])
        version = record['version'] if record is not None else None
        lease = attempts.open_attempt(db, item, worker, now, rule['lease_seconds'], version, partition)
        delivery.lease(db, item['id'], lease['number'])
        message = publication.get(db, item['message_id'])
        journal.append(db, channel, now, 'leased', {'delivery_id': item['id'], 'attempt': lease['number'],
                                                   'worker': worker, 'expires': lease['expires'],
                                                   'assignment_version': version,
                                                   'partition': partition}, message['id'])
        results.append(dict(delivery_id=item['id'], message_id=message['id'], lane=item['lane'],
                            ordering_key=item['lane'], key=item['lane'], partition=partition,
                            shard=partition, owner=worker, worker=worker,
                            assignment_version=version, policy=item['policy'], lease=lease,
                            envelope=message['envelope']))
        if len(results) >= limit:
            break
    return {'paused': False, 'items': results, **context}
