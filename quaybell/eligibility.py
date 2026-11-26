from .delivery import DELIVERY

# 未处置状态：屏障（blocked）与等待、租约中一样会挡住同键后继。
ACTIVE = ('waiting', 'leased', 'blocked')


def ready(db, group_id, now, limit):
    placeholders = ','.join('?' for _ in ACTIVE)
    return [dict(row) for row in db.execute(DELIVERY + f'''
        WHERE d.subscription_id=? AND d.state='waiting' AND d.ready_at<=?
          AND NOT EXISTS (SELECT 1 FROM deliveries p
             WHERE p.subscription_id=d.subscription_id AND p.lane=d.lane
               AND p.position<d.position AND p.state IN ({placeholders}))
        ORDER BY d.lane,d.position LIMIT ?''', (group_id, now, *ACTIVE, limit))]


def explain(db, group, now):
    rows = db.execute('''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,d.position,a.expires
                         FROM deliveries d LEFT JOIN attempts a
                           ON a.delivery_id=d.id AND a.outcome='open'
                         WHERE d.subscription_id=? AND d.state IN ('waiting','leased','blocked')
                         ORDER BY d.lane,d.position,d.id''',
                      (group['id'],))
    heads = {}
    for row in rows:
        d = dict(row)
        head = heads.setdefault(d['lane'], d['id'])
        if d['state'] == 'blocked':
            cause = 'blocked_gap'
        elif d['state'] == 'leased':
            cause = 'expired' if d['expires'] <= now else 'leased'
        elif head != d['id']:
            cause = 'blocked'
        elif group['paused']:
            cause = 'paused'
        else:
            cause = 'delayed' if d['ready_at'] > now else 'ready'
        yield d | {'cause': cause, 'head_id': head}
