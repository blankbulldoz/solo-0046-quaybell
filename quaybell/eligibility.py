from . import dependencies
from .delivery import DELIVERY


def ready(db, group_id, now, limit):
    gates = dependencies.gate_cache(db, group_id)
    candidates = [dict(row) for row in db.execute(DELIVERY + '''
        WHERE d.subscription_id=? AND d.state='waiting' AND d.ready_at<=?
          AND NOT EXISTS (SELECT 1 FROM deliveries p
             WHERE p.subscription_id=d.subscription_id AND p.lane=d.lane AND p.id<d.id
               AND p.state IN ('waiting','leased'))
        ORDER BY d.id''', (group_id, now))]
    results = []
    for item in candidates:
        cause, _ = dependencies.gate_of(gates.get(item['id'], []))
        if cause is None:
            results.append(item)
            if len(results) >= limit:
                break
    return results


def explain(db, group, now):
    gates = dependencies.gate_cache(db, group['id'])
    rows = db.execute('''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,a.expires
                         FROM deliveries d LEFT JOIN attempts a
                           ON a.delivery_id=d.id AND a.outcome='open'
                         WHERE d.subscription_id=? AND d.state IN ('waiting','leased') ORDER BY d.id''',
                      (group['id'],))
    heads = {}
    for row in rows:
        d = dict(row)
        head = heads.setdefault(d['lane'], d['id'])
        gate_cause, blocking = dependencies.gate_of(gates.get(d['id'], []))
        if d['state'] == 'leased':
            cause = 'expired' if d['expires'] <= now else 'leased'
        elif gate_cause is not None:
            cause = gate_cause
        elif head != d['id']:
            cause = 'blocked'
        elif group['paused']:
            cause = 'paused'
        else:
            cause = 'delayed' if d['ready_at'] > now else 'ready'
        yield d | {'cause': cause, 'head_id': head,
                   'prerequisites': sorted(detail['delivery_id'] for detail in gates.get(d['id'], [])),
                   'blocking_prerequisites': blocking}
