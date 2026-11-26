from . import delivery, eligibility, subscriptions
from .errors import missing

TERMINAL_NOTE = '该投递已终态，不再占用顺序键；后继不受其阻挡'


def _group_record(db, item):
    row = db.execute('SELECT * FROM subscriptions WHERE id=?', (item['subscription_id'],)).fetchone()
    if row is None:
        raise missing('找不到消费组')
    return subscriptions.decoded(row)


def investigate(db, delivery_id, now):
    """只读积压调查：不领取、不回收，回答该投递在等哪一条以及现场能做什么。"""
    target = delivery.get(db, delivery_id)
    group = _group_record(db, target)
    lane_rows = eligibility.lane_queue(db, group['id'], target['lane'])
    active_ids = [row['id'] for row in lane_rows]
    head_id = active_ids[0] if active_ids else None
    entries = [eligibility.annotate(row, head_id, group['paused'], now) for row in lane_rows]
    target_view = next((entry for entry in entries if entry['id'] == delivery_id), None)
    if target_view is None:
        target_view = {'id': target['id'], 'message_id': target['message_id'], 'lane': target['lane'],
                       'state': target['state'], 'ready_at': target['ready_at'],
                       'attempt_count': target['attempt_count'], 'policy': target['policy'],
                       'parent_id': target['parent_id'], 'terminal_at': target['terminal_at'],
                       'expires': None, 'head_id': head_id, 'cause': 'terminal',
                       'actionable': 'none', 'open_attempt': None}
    head = next((entry for entry in entries if entry['id'] == head_id), None)
    return {
        'observed_at': now,
        'channel': target['channel'],
        'group': {'id': group['id'], 'name': group['name'], 'paused': group['paused'],
                  'tags': group['tags']},
        'target': target_view,
        'head': head,
        'blocked_by': head['id'] if head and head['id'] != delivery_id else None,
        'lane': target['lane'],
        'lane_size': len(entries),
        'lane_queue': entries,
        'note': TERMINAL_NOTE if head is None else None,
    }


def handoff(db, delivery_id, now):
    """冻结交接证据：固化观察时刻的现场读数，供接班人复核当时的结论。"""
    facts = investigate(db, delivery_id, now)
    head, target = facts['head'], facts['target']
    if head is None:
        reason = '顺序键上已无未处置投递'
    elif head['id'] == target['id']:
        reason = f"队头 #{head['id']} 当前原因为 {head['cause']}，可采取动作 {head['actionable']}"
    else:
        reason = (f"被同键队头 #{head['id']}（{head['cause']}）阻挡，"
                  f"现场可采取动作 {head['actionable']}")
    return {'format': 'quaybell-handoff/1', 'observed_at': now,
            'delivery_id': delivery_id, 'channel': facts['channel'],
            'group': facts['group'], 'lane': facts['lane'],
            'conclusion': {'target_id': target['id'], 'target_state': target['state'],
                           'target_cause': target['cause'], 'blocked_by': facts['blocked_by'],
                           'head_id': head['id'] if head else None,
                           'head_state': head['state'] if head else None,
                           'head_cause': head['cause'] if head else None,
                           'head_actionable': head['actionable'] if head else 'none',
                           'head_ready_at': head['ready_at'] if head else None,
                           'head_expires': head['expires'] if head else None,
                           'lane_size': facts['lane_size'], 'reason': reason},
            'live': facts}
