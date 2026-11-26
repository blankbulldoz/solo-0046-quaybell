from .delivery import DELIVERY


def ready(db, group_id, now, limit):
    return [dict(row) for row in db.execute(DELIVERY + '''
        WHERE d.subscription_id=? AND d.state='waiting' AND d.ready_at<=?
          AND NOT EXISTS (SELECT 1 FROM deliveries p
             WHERE p.subscription_id=d.subscription_id AND p.lane=d.lane AND p.id<d.id
               AND p.state IN ('waiting','leased'))
        ORDER BY d.id LIMIT ?''', (group_id, now, limit))]


def explain(db, group, now):
    rows = db.execute('''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,a.expires,
                                a.budget_expires,a.renew_count
                         FROM deliveries d LEFT JOIN attempts a
                           ON a.delivery_id=d.id AND a.outcome='open'
                         WHERE d.subscription_id=? AND d.state IN ('waiting','leased') ORDER BY d.id''',
                      (group['id'],))
    heads = {}
    for row in rows:
        d = dict(row)
        head = heads.setdefault(d['lane'], d['id'])
        if d['state'] == 'leased':
            cause = 'expired' if d['expires'] <= now else 'leased'
        elif head != d['id']:
            cause = 'blocked'
        elif group['paused']:
            cause = 'paused'
        else:
            cause = 'delayed' if d['ready_at'] > now else 'ready'
        yield d | {'cause': cause, 'head_id': head}
