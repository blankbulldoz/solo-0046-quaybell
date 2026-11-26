from .delivery import DELIVERY


def ready(db, group_id, now, limit):
    return [dict(row) for row in db.execute(DELIVERY + '''
        WHERE d.subscription_id=? AND d.state='waiting' AND d.ready_at<=?
          AND NOT EXISTS (
            SELECT 1 FROM confluence_delivery_links self_gate
            WHERE self_gate.delivery_id=d.id AND self_gate.gate_state <> 'released'
          )
          AND NOT EXISTS (SELECT 1 FROM deliveries p
             LEFT JOIN confluence_delivery_links prior_gate ON prior_gate.delivery_id=p.id
             WHERE p.subscription_id=d.subscription_id AND p.lane=d.lane AND p.id<d.id
               AND p.state IN ('waiting','leased')
               AND (prior_gate.delivery_id IS NULL OR prior_gate.gate_state='released'))
        ORDER BY d.id LIMIT ?''', (group_id, now, limit))]


def explain(db, group, now):
    rows = db.execute('''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,a.expires,
                              cg.gate_state,cg.source AS gate_source,cg.sequence AS gate_sequence
                         FROM deliveries d
                         LEFT JOIN attempts a
                           ON a.delivery_id=d.id AND a.outcome='open'
                         LEFT JOIN confluence_delivery_links cg ON cg.delivery_id=d.id
                         WHERE d.subscription_id=? AND d.state IN ('waiting','leased') ORDER BY d.id''',
                      (group['id'],))
    heads = {}
    for row in rows:
        d = dict(row)
        gate_state = d.get('gate_state')
        if gate_state in ('buffered', 'late', 'duplicate'):
            cause = {'buffered': 'sequence_gap', 'late': 'late_traceable',
                     'duplicate': 'duplicate_sequence'}[gate_state]
            yield d | {'cause': cause, 'head_id': None, 'blocking': False}
            continue
        head = heads.setdefault(d['lane'], d['id'])
        if d['state'] == 'leased':
            cause = 'expired' if d['expires'] <= now else 'leased'
        elif head != d['id']:
            cause = 'blocked'
        elif group['paused']:
            cause = 'paused'
        else:
            cause = 'delayed' if d['ready_at'] > now else 'ready'
        yield d | {'cause': cause, 'head_id': head, 'blocking': True}
