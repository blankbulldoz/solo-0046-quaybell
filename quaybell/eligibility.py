from .delivery import DELIVERY

# 顺序键最近一次获得服务的位置：该键所有投递上最大的尝试号。尝试号单调递增，
# 未服务过的键取 0，因此可领键按“最久未服务”轮转，而不是总让 ID 小的热键排在前面。
SERVICE = '''COALESCE((SELECT MAX(a.id) FROM attempts a
                          JOIN deliveries served ON served.id=a.delivery_id
                         WHERE served.subscription_id=d.subscription_id
                           AND served.lane=d.lane), 0)'''
SERVED_AT = '''(SELECT MAX(a.started) FROM attempts a
                 JOIN deliveries served ON served.id=a.delivery_id
                WHERE served.subscription_id=d.subscription_id
                  AND served.lane=d.lane)'''


def ready(db, group_id, now, limit):
    return [dict(row) for row in db.execute(DELIVERY + f'''
        WHERE d.subscription_id=? AND d.state='waiting' AND d.ready_at<=?
          AND NOT EXISTS (SELECT 1 FROM deliveries p
             WHERE p.subscription_id=d.subscription_id AND p.lane=d.lane AND p.id<d.id
               AND p.state IN ('waiting','leased'))
        ORDER BY {SERVICE}, d.id LIMIT ?''', (group_id, now, limit))]


def explain(db, group, now):
    rows = db.execute(f'''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,a.expires,
                                 {SERVICE} AS service_order, {SERVED_AT} AS last_served
                          FROM deliveries d LEFT JOIN attempts a
                            ON a.delivery_id=d.id AND a.outcome='open'
                          WHERE d.subscription_id=? AND d.state IN ('waiting','leased')
                          ORDER BY service_order,d.id''',
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
