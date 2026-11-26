# One head per lane: the lowest open delivery of each sequence key. The
# recursive step seeks the next lane above the current one, so a lane with a
# large backlog costs a single index seek - successors are never walked.
HEADS = '''WITH RECURSIVE lanes(lane) AS (
    SELECT MIN(lane) FROM deliveries
      WHERE subscription_id=? AND state IN ('waiting','leased')
    UNION ALL
    SELECT (SELECT MIN(p.lane) FROM deliveries p
              WHERE p.subscription_id=? AND p.state IN ('waiting','leased') AND p.lane>lanes.lane)
      FROM lanes WHERE lanes.lane IS NOT NULL
  )
  SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,d.policy,d.attempt_count,m.payload_bytes
    FROM lanes
    JOIN deliveries d ON d.subscription_id=? AND d.lane=lanes.lane AND d.id=(
         SELECT MIN(q.id) FROM deliveries q
          WHERE q.subscription_id=? AND q.lane=lanes.lane AND q.state IN ('waiting','leased'))
    JOIN messages m ON m.id=d.message_id
   ORDER BY d.id'''

REPORT_LIMIT = 30


def heads(db, group_id):
    """Every lane head of the group, open (waiting or leased) or not."""
    return [dict(row) for row in db.execute(HEADS, (group_id, group_id, group_id, group_id))]


def partition(db, group_id, now, limit, budget):
    """Split ready lane heads into claimed, oversized and carried-over entries.

    Heads are visited in delivery-id order. A head is judged against the whole
    budget first: one that cannot fit even an empty batch is unfit and keeps
    blocking its lane, so a smaller successor never bypasses it and a later
    claim with a larger budget still reaches it. A head that fits alone but not
    beside the batch already picked is deferred, and the scan continues so
    colder keys keep draining. `budget` of None means no byte ceiling.
    """
    picked, used, unfit, deferred = [], 0, [], []
    counts = {'unfit': 0, 'deferred': 0}
    for item in heads(db, group_id):
        if item['state'] != 'waiting' or item['ready_at'] > now:
            continue
        size = item['payload_bytes']
        if budget is not None and size > budget:
            counts['unfit'] += 1
            if len(unfit) < REPORT_LIMIT:
                unfit.append(detail(item, 'oversized'))
            continue
        if budget is not None and used + size > budget:
            counts['deferred'] += 1
            if len(deferred) < REPORT_LIMIT:
                deferred.append(detail(item, 'budget'))
            continue
        picked.append(item)
        used += size
        if len(picked) >= limit:
            # Batch is full; stop before touching further lanes so a hot
            # backlog never forces a full scan of its successors.
            break
    return {'picked': picked, 'used': used, 'unfit': unfit, 'unfit_total': counts['unfit'],
            'deferred': deferred, 'deferred_total': counts['deferred']}


def detail(item, reason):
    return {'delivery_id': item['id'], 'message_id': item['message_id'], 'lane': item['lane'],
            'payload_bytes': item['payload_bytes'], 'reason': reason}


def explain(db, group, now):
    rows = db.execute('''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,a.expires
                         FROM deliveries d LEFT JOIN attempts a
                           ON a.delivery_id=d.id AND a.outcome='open'
                         WHERE d.subscription_id=? AND d.state IN ('waiting','leased') ORDER BY d.id''',
                      (group['id'],))
    lane_heads = {}
    for row in rows:
        d = dict(row)
        head = lane_heads.setdefault(d['lane'], d['id'])
        if d['state'] == 'leased':
            cause = 'expired' if d['expires'] <= now else 'leased'
        elif head != d['id']:
            cause = 'blocked'
        elif group['paused']:
            cause = 'paused'
        else:
            cause = 'delayed' if d['ready_at'] > now else 'ready'
        yield d | {'cause': cause, 'head_id': head}
