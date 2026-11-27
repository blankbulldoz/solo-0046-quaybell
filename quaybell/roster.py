"""值班重投排程：用一次队列观察排出两轮重投清单。

清单只读队列、死信和尝试，不改动状态，也不领取任何投递。依据的规则分三类，
输出时分别标注：

* 约束（团队给定）：每轮最多追加两个重投，每轮只保证确认一个热键件。
* 引擎事实：重投把后继追加到同键队尾；同键只有队首可领；不同键互不阻挡；
  冷键在同一轮可反复领取确认，直到清空。
* 计划假设：两轮交界时热键上发布一条正常告警（`assumed`）。这是清单里唯一的
  外部假设，值班员要按实际交界发布替换它。

热键取死信最多的顺序键（至少两条）。第一轮补冷键空缺，并确认热键上已在等待的
后继；热键死信押到交界告警之后。第二轮先确认交界热键告警，再把热键后继追加到
它后面，因此不挡任何正常件。清单给出两轮动作、可并行项、会被挡或只能顺延的
正常件，以及第一轮漏确认任一计划项时第二轮要换掉的项与两轮期限。
"""
from . import catalog, subscriptions

FORMAT = 'quaybell-roster/1'
CONSTRAINTS = {'redrives_per_round': 2, 'hot_confirmations_per_round': 1}
HOT_MINIMUM_DEAD = 2

QUEUE = '''SELECT d.id,d.message_id,d.lane,d.state,d.ready_at,d.parent_id,a.expires
           FROM deliveries d LEFT JOIN attempts a
             ON a.delivery_id=d.id AND a.outcome='open'
           WHERE d.subscription_id=? AND d.state IN ('waiting','leased') ORDER BY d.id'''
DEAD = '''SELECT d.id,d.message_id,d.lane,
                 (SELECT c.id FROM deliveries c WHERE c.parent_id=d.id) AS successor
          FROM deliveries d WHERE d.subscription_id=? AND d.state='dead' ORDER BY d.lane,d.id'''


# ---------------------------------------------------------------- 观察

def _hot_lane(dead):
    """热键是死信最多的顺序键，至少两条；并列取名字靠前的。"""
    counts = {}
    for row in dead:
        counts[row['lane']] = counts.get(row['lane'], 0) + 1
    ranked = sorted(counts, key=lambda lane: (-counts[lane], lane))
    return ranked[0] if ranked and counts[ranked[0]] >= HOT_MINIMUM_DEAD else None


def _node(row):
    return {'delivery': row['id'], 'message': row['message_id'], 'lane': row['lane'],
            'kind': 'successor' if row['parent_id'] else 'message',
            'source': row['parent_id'], 'state': row['state'],
            'ready_at': row['ready_at'], 'expires': row['expires'],
            'label': f'#{row["id"]}', 'assumed': False, 'projected': False}


def _world(db, group, now):
    lanes = {}
    for row in db.execute(QUEUE, (group['id'],)):
        node = _node(dict(row))
        lanes.setdefault(node['lane'], []).append(node)
    dead = [dict(row) for row in db.execute(DEAD, (group['id'],))]
    return {'lanes': lanes, 'dead': dead, 'hot': _hot_lane(dead),
            'paused': group['paused'], 'now': now}


def _clone(world):
    return {'lanes': {lane: [dict(node) for node in nodes] for lane, nodes in world['lanes'].items()},
            'dead': [dict(row) for row in world['dead']], 'hot': world['hot'],
            'paused': world['paused'], 'now': world['now']}


def _annotate(world):
    for nodes in world['lanes'].values():
        head = nodes[0] if nodes else None
        for index, node in enumerate(nodes):
            node['head'] = head['label'] if head else None
            if index > 0:
                node['cause'] = 'blocked'
            elif node['state'] == 'leased':
                node['cause'] = 'expired' if (node['expires'] or 0) <= world['now'] else 'leased'
            elif world['paused']:
                node['cause'] = 'paused'
            else:
                node['cause'] = 'delayed' if node['ready_at'] > world['now'] else 'ready'


def _token(node):
    if node['delivery'] is not None:
        return f'delivery:{node["delivery"]}'
    if node['source'] is None:
        return f'assumed:{node["lane"]}:{node["label"]}'
    return f'redrive:{node["source"]}'


def _pending(world):
    return [row for row in world['dead'] if row['successor'] is None]


def _describe(node, **extra):
    return {'delivery': node['delivery'], 'label': node['label'], 'message': node['message'],
            'lane': node['lane'], 'kind': node['kind'], 'source': node['source'],
            'cause': node['cause'], 'head': node['head'], 'token': _token(node),
            'assumed': node['assumed'],
            'projected': node['delivery'] is None or node.get('projected', False)} | extra


def _queue_view(world):
    _annotate(world)
    return [{'lane': lane,
             'head': nodes[0]['label'] if nodes else None,
             'queue': [{'label': node['label'], 'delivery': node['delivery'],
                        'kind': node['kind'], 'source': node['source'],
                        'cause': node['cause'], 'assumed': node['assumed'],
                        'projected': node['delivery'] is None
                                   or node.get('projected', False)} for node in nodes]}
            for lane, nodes in sorted(world['lanes'].items())]


# ---------------------------------------------------------------- 单轮

def _issue(world, source, reason):
    """在本世界内追加后继，并只改本世界那份死信记录。"""
    row = next(item for item in world['dead'] if item['id'] == source)
    label = f'后继({row["id"]})'
    world['lanes'].setdefault(row['lane'], []).append(
        {'delivery': None, 'message': row['message_id'], 'lane': row['lane'],
         'kind': 'successor', 'source': row['id'], 'state': 'waiting',
         'ready_at': world['now'], 'expires': None, 'label': label,
         'assumed': False, 'projected': True})
    row['successor'] = label
    return {'source': row['id'], 'message': row['message_id'], 'lane': row['lane'],
            'successor': label, 'reason': reason}


def _eligibility(world, lane, number, slots):
    """本轮能否向该键追加后继：新后继须本轮可确认，或之后不再有交界发布。"""
    nodes = world['lanes'].get(lane, [])
    if world['paused']:
        return False, '消费组暂停，本轮无法确认，追加的后继会留在队首'
    if lane == world['hot'] and number == 1:
        if nodes and nodes[0]['kind'] == 'successor':
            return False, (f'热键本轮唯一确认额度要留给已在等待的后继 {nodes[0]["label"]}；'
                           '现在追加的新后继无法本轮确认，交界热告警会被它挡住')
        return False, ('热键本轮唯一确认额度不能挪给新后继；先等交界热告警发布，'
                       '再把后继追到它之后')
    if number == 1 and nodes:
        return False, '本键仍有未完成投递，追加的后继无法本轮确认，交界告警会被它挡住'
    if nodes and nodes[0]['kind'] != 'message':
        return False, f'本键队首 {nodes[0]["label"]} 是未确认的后继，先补确认，不再追加'
    if slots <= 0:
        return False, f'本轮重投上限 {CONSTRAINTS["redrives_per_round"]} 已用满'
    return True, None


def _run(world, number, skip=(), hot_quota=CONSTRAINTS['hot_confirmations_per_round'],
         slots=CONSTRAINTS['redrives_per_round']):
    """规划并模拟一轮：至多 slots 个重投，热键至多 hot_quota 个确认。

    冷键不受确认额度限制：同一轮内可以反复领取并确认本键队首，直到清空。
    """
    _annotate(world)
    by_lane = {}
    for row in _pending(world):
        by_lane.setdefault(row['lane'], []).append(row)

    redrives, holds, deferred = [], [], []
    for lane in sorted(by_lane, key=lambda name: (name == world['hot'], name)):
        batch = sorted(by_lane[lane], key=lambda row: row['id'])
        ok, reason = _eligibility(world, lane, number, slots - len(redrives))
        if not ok:
            holds += [{'source': row['id'], 'message': row['message_id'], 'lane': lane,
                       'reason': reason} for row in batch]
            continue
        redrives.append(_issue(world, batch[0]['id'], '本键空缺，后继排到队尾且本轮可确认'))
        deferred += [{'source': row['id'], 'message': row['message_id'], 'lane': lane,
                      'reason': '同键本轮已追加一个后继'} for row in batch[1:]]
    _annotate(world)

    confirmed, hot_left = [], hot_quota
    for lane in sorted(world['lanes']):
        while True:
            nodes = world['lanes'][lane]
            if not nodes or nodes[0]['cause'] != 'ready':
                break
            node = nodes[0]
            if _token(node) in skip:
                deferred.append(_describe(node, reason='本轮有意跳过'))
                break
            if lane == world['hot'] and hot_left <= 0:
                deferred.append(_describe(node, reason='热键本轮唯一确认额度已用完'))
                break
            confirmed.append(_describe(node, hot=lane == world['hot']))
            world['lanes'][lane] = nodes = nodes[1:]
            if lane == world['hot']:
                hot_left -= 1
            _annotate(world)

    blocked = [_describe(node, unblock=node['head'])
               for lane in sorted(world['lanes']) for node in world['lanes'][lane]
               if node['cause'] == 'blocked']
    unavailable = [_describe(node, reason=node['cause'])
                   for lane in sorted(world['lanes']) for node in world['lanes'][lane]
                   if node is world['lanes'][lane][0]
                   and node['cause'] in ('delayed', 'leased', 'expired', 'paused')]
    return {
        'round': number,
        'basis': 'observed' if number == 1 else 'projected',
        'redrives': redrives,
        'holds': holds,
        'confirm': confirmed,
        'deferred': deferred,
        'blocked': blocked,
        'unavailable': unavailable,
        'linked': [{'source': row['id'], 'message': row['message_id'], 'lane': row['lane'],
                    'successor': row['successor']}
                   for row in world['dead'] if row['successor'] is not None],
        'parallel': _parallel(redrives, confirmed),
        'after': _queue_view(world),
    }, world


def _parallel(redrives, confirmed):
    groups = []
    if len(redrives) > 1:
        groups.append({'kind': 'redrives', 'lanes': [row['lane'] for row in redrives],
                       'items': [{'lane': row['lane'], 'source': row['source']}
                                 for row in redrives],
                       'reason': '不同顺序键互不阻挡，可在同一轮同时追加'})
    if len({item['lane'] for item in confirmed}) > 1:
        groups.append({'kind': 'confirm',
                       'lanes': sorted({item['lane'] for item in confirmed}),
                       'items': [{'lane': item['lane'], 'label': item['label'],
                                  'token': item['token'], 'assumed': item['assumed']}
                                 for item in confirmed],
                       'reason': '不同顺序键可同批领取；热键只出一个，不占冷键额度'})
    return groups


# ---------------------------------------------------------------- 交界与偏差

def _boundary_lanes(world):
    """计划假设：两轮交界时，热键上发布一条正常告警（封面场景的唯一假设）。"""
    return [world['hot']] if world['hot'] else []


def _publish_boundary(world, lanes):
    for lane in lanes:
        world['lanes'].setdefault(lane, []).append(
            {'delivery': None, 'message': None, 'lane': lane, 'kind': 'message',
             'source': None, 'state': 'waiting', 'ready_at': world['now'],
             'expires': None, 'label': f'交界告警({lane})', 'assumed': True,
             'projected': True})
    _annotate(world)


def _diff(baseline, recovered):
    base_redrives = {row['source']: row for row in baseline['redrives']}
    other_redrives = {row['source']: row for row in recovered['redrives']}
    base_confirm = {item['token']: item for item in baseline['confirm']}
    other_confirm = {item['token']: item for item in recovered['confirm']}
    dropped = [dict(row, kind='redrive') for key, row in base_redrives.items()
               if key not in other_redrives]
    dropped += [dict(item, kind='confirm') for key, item in base_confirm.items()
                if key not in other_confirm]
    added = [dict(row, kind='redrive') for key, row in other_redrives.items()
             if key not in base_redrives]
    added += [dict(item, kind='confirm') for key, item in other_confirm.items()
              if key not in base_confirm]
    return dropped, added


def _contingency(world, planned, boundary, baseline):
    """逐个模拟第一轮计划确认被漏掉时，第二轮的换项与两轮期限。

    第一轮的计划确认由两轮共有的事实决定：本轮追加的冷键后继、以及热键上
    已在等待的后继。漏掉热键项时第二轮唯一的热键额度只能补它，交界热告警
    落到第三轮；漏掉冷键项时冷键不受额度限制，可在第二轮连做两次确认，不必
    换掉任何重投。
    """
    outcomes = []
    for item in planned:
        missed = {'token': item['token'], 'delivery': item['delivery'],
                  'label': item['label'], 'lane': item['lane'], 'kind': item['kind'],
                  'hot': item['lane'] == world['hot']}
        lane, hot = missed['lane'], missed['hot']
        _, after_first = _run(_clone(world), 1, skip={missed['token']})
        _publish_boundary(after_first, boundary)
        as_planned, _ = _run(_clone(after_first), 2)
        beyond = None
        if hot:
            swap, _ = _run(_clone(after_first), 2)
            extra, _ = _run(_clone(after_first), 2, hot_quota=2)
            met = {row['token'] for row in extra['confirm'] if row['assumed']}
            beyond = {'hot_confirmations_in_round2': 2,
                      'confirm': [{'lane': row['lane'], 'label': row['label'],
                                   'token': row['token'], 'assumed': row['assumed']}
                                  for row in extra['confirm']],
                      'meets_deadline': {row['token'] for row in baseline['confirm']
                                         if row['assumed']} <= met,
                      'breaks': '每轮只保证确认一个热键件'}
            promised = {row['token'] for row in swap['confirm'] if row['assumed']}
            keeping = {row['token'] for row in baseline['confirm'] if row['assumed']} <= promised
            if keeping:
                deadline = 'met'
            elif beyond['meets_deadline']:
                deadline = 'missed_unless_extra_hot_confirmation'
            else:
                deadline = 'missed'
            if swap['redrives']:
                recovery = ('热键每轮只保证一个确认：第二轮只能把它用于补确认漏掉的后继，'
                            '热键死信不再追加新后继；交界热告警落到第三轮。'
                            '要在两轮内清掉告警，只能当轮多确认一个热键件，'
                            '即超出「每轮一个热键」的承诺。')
            else:
                recovery = ('热键每轮只保证一个确认：第二轮只能把它用于补确认漏掉的后继，'
                            '交界热告警落到第三轮。要在两轮内清掉告警，'
                            '只能当轮多确认一个热键件，即超出「每轮一个热键」的承诺。')
        else:
            swap, _ = _run(_clone(after_first), 2)
            deadline = 'met'
            recovery = (f'冷键确认不受热键额度限制，无需换掉第二轮的重投。'
                        f'第二轮先在 {lane} 补确认 {missed["label"]}，其余照投影执行。')
        dropped, added = _diff(baseline, swap)
        as_planned_met = {row['token'] for row in as_planned['confirm'] if row['assumed']}
        swap_met = {row['token'] for row in swap['confirm'] if row['assumed']}
        at_risk, still_at_risk = [], []
        for alert in (row for row in baseline['confirm'] if row['assumed']):
            if alert['token'] in as_planned_met:
                continue
            if any(row['token'] == alert['token'] for row in as_planned['blocked']):
                state = 'blocked'
            elif any(row['token'] == alert['token'] for row in as_planned['deferred']):
                state = 'deferred'
            else:
                state = 'pending_third_round'
            at_risk.append(dict(alert, cause=state))
            if alert['token'] not in swap_met:
                still_at_risk.append(dict(alert, cause=state))
        if still_at_risk and deadline in ('met',):
            deadline = 'missed'
        outcomes.append({
            'missed': missed,
            'as_planned_round2': {'redrives': as_planned['redrives'],
                                  'confirm': as_planned['confirm'],
                                  'blocked': [row for row in as_planned['blocked']
                                              if row['assumed']],
                                  'deferred': as_planned['deferred']},
            'swap_round2': {'redrives': swap['redrives'], 'confirm': swap['confirm'],
                            'blocked': [row for row in swap['blocked'] if row['assumed']],
                            'deferred': swap['deferred']},
            'drop': dropped,
            'add': added,
            'alarms_at_risk': at_risk,
            'alarms_still_at_risk': still_at_risk,
            'beyond_promise': beyond,
            'deadline': deadline,
            'recovery': recovery,
        })
    return outcomes


def _hazards(world, boundary):
    """反例：交界前就把热键死信重投出去，后继会占住队首，挡住交界热告警。"""
    hot_pending = [row for row in _pending(world) if row['lane'] == world['hot']]
    if not hot_pending:
        return []
    trial = _clone(world)
    row = sorted(hot_pending, key=lambda item: item['id'])[0]
    _issue(trial, row['id'], '过早重投')
    _publish_boundary(trial, boundary)
    order = [{'label': node['label'], 'kind': node['kind'], 'cause': node['cause']}
             for node in trial['lanes'].get(world['hot'], [])]
    blocked = [node for node in order if node['kind'] == 'message' and node['cause'] == 'blocked']
    return [{
        'if': f'在交界前重投热键死信 #{row["id"]}',
        'lane': world['hot'],
        'order': order,
        'blocked_normal': blocked,
        'effect': ('热键队首仍是已在等待的后继，新后继排在它之后，交界热告警被压到'
                   '两条后继之后；热键每轮只确认一个件时，告警要等到第三轮。'),
    }]


# ---------------------------------------------------------------- 汇总

def _deadline(world, first, second):
    """两轮收尾的硬指标：交界告警是否收齐、热键死信各落在第几轮。"""
    alerts = [row for row in second['confirm'] if row['assumed']]
    remaining = [row for row in second['deferred'] if row['assumed']]
    remaining += [row for lane in second['after'] for row in lane['queue']
                  if row['assumed']]
    pending = [row for row in _pending(world)]
    return {
        'boundary_alerts_in_round2': len(alerts),
        'boundary_alerts_left_after_round2': len(remaining),
        'meets_two_round_deadline': not remaining,
        'hot_dead_letters': [{'source': row['id'], 'lane': row['lane'],
                              'redriven_by_round': next(
                                  (record['round'] for record in (first, second)
                                   if any(item['source'] == row['id']
                                          for item in record['redrives'])),
                                  'before_observation' if row['successor'] else None)}
                             for row in world['dead'] if row['lane'] == world['hot']],
        'unredriven_dead_letters': [row['id'] for row in pending
                                    if not any(item['source'] == row['id']
                                               for record in (first, second)
                                               for item in record['redrives'])],
    }


def _checks(world, boundary, first, second):
    _annotate(world)
    return {
        'observed_at': world['now'],
        'hot_lane': world['hot'],
        'constraints': dict(CONSTRAINTS),
        'assumptions': {'boundary_lanes': boundary,
                        'text': ('交界时上述每个键各发布一条正常告警；冷键同一轮可反复领取确认，'
                                 '热键每轮只保证一次。assumed=True 来自交界假设，'
                                 'projected=True 来自重投预测')},
        'queue': [{'delivery': node['delivery'], 'message': node['message'], 'lane': lane,
                   'position': index, 'kind': node['kind'], 'source': node['source'],
                   'cause': node['cause'], 'head': node['head']}
                  for lane, nodes in sorted(world['lanes'].items())
                  for index, node in enumerate(nodes)],
        'dead': [{'delivery': row['id'], 'message': row['message_id'], 'lane': row['lane'],
                  'successor': row['successor']} for row in world['dead']],
        'round1_predictions': _predictions(first),
        'round2_predictions': _predictions(second),
    }


def _predictions(record):
    return {'redrives': [{'source': row['source'], 'lane': row['lane']}
                         for row in record['redrives']],
            'confirm': [{'token': row['token'], 'delivery': row['delivery'],
                         'label': row['label'], 'lane': row['lane'],
                         'assumed': row['assumed']} for row in record['confirm']],
            'after': record['after']}


def plan(db, channel, now):
    catalog.get(db, channel)
    groups = []
    for group in subscriptions.for_channel(db, channel):
        world = _world(db, group, now)
        boundary = _boundary_lanes(world)
        first, after_first = _run(_clone(world), 1)
        junction = _clone(after_first)
        _publish_boundary(junction, boundary)
        second, _ = _run(_clone(junction), 2)
        lanes = []
        for lane in sorted(set(world['lanes']) | {row['lane'] for row in world['dead']}):
            lanes.append({'lane': lane, 'role': 'hot' if lane == world['hot'] else 'cold',
                          'queue': [node['label'] for node in world['lanes'].get(lane, [])],
                          'dead': [{'delivery': row['id'], 'message': row['message_id'],
                                    'successor': row['successor']}
                                   for row in world['dead'] if row['lane'] == lane]})
        groups.append({
            'group': group['name'],
            'paused': group['paused'],
            'hot_lane': world['hot'],
            'constraints': dict(CONSTRAINTS),
            'lanes': lanes,
            'rounds': [first, second],
            'deadline': _deadline(world, first, second),
            'hazards': _hazards(world, boundary),
            'contingency': _contingency(world, first['confirm'], boundary, second),
            'checks': _checks(world, boundary, first, second),
        })
    return {'format': FORMAT, 'observed_at': now, 'channel': channel, 'groups': groups}


# ---------------------------------------------------------------- 值班清单文本

DEADLINE = {'met': '两轮内可补齐', 'met_with_swap': '换项后两轮内可补齐',
            'missed': '两轮内无法补齐',
            'missed_unless_extra_hot_confirmation': '守住每轮一个热键就补不齐；多确认一个热键件才能补齐'}
CAUSE = {'blocked': '被队首挡住', 'deferred': '额度用尽顺延',
         'pending_third_round': '本轮未轮到，需第三轮',
         'delayed': '未到就绪时间', 'leased': '租约中，等确认或超时',
         'expired': '租约已超时，等回收', 'paused': '消费组暂停'}


def render(group):
    """把一组的排程渲染成值班员可直接照做的条目。"""
    hot = group['hot_lane'] or '无（没有任何顺序键积到两条死信）'
    lines = [f'消费组 {group["group"]} · 热键 {hot}'
             + ('（暂停中）' if group['paused'] else '')]
    deadline = group['deadline']
    lines.append('两轮期限：' + ('满足' if deadline['meets_two_round_deadline'] else '不满足')
                 + f'，第二轮后仍欠交界告警 {deadline["boundary_alerts_left_after_round2"]} 条')
    normal_blocked = [row for record in group['rounds'] for row in record['blocked']]
    if normal_blocked:
        lines.append('正常件：' + '、'.join(f'{row["label"]}（{row["lane"]}）'
                                          for row in normal_blocked) + ' 会被挡')
    else:
        lines.append('正常件：照清单执行时，交界告警不会被任何重投挡住')
    for record in group['rounds']:
        tag = '实况' if record['basis'] == 'observed' else '投影'
        lines.append(f'第 {record["round"]} 轮（{tag}）')
        for row in record['redrives']:
            lines.append(f'  重投 源投递 #{row["source"]}（{row["lane"]}）→ {row["successor"]}')
        for row in record['holds']:
            lines.append(f'  押后 源投递 #{row["source"]}（{row["lane"]}）：{row["reason"]}')
        for row in record['confirm']:
            mark = '假设' if row['assumed'] else ('投影' if row['projected'] else '实件')
            lines.append(f'  确认 {row["label"]}（{row["lane"]}，{mark}）')
        for row in record['deferred']:
            lines.append(f'  顺延 {row["label"]}（{row["lane"]}）：{row["reason"]}')
        for row in record['unavailable']:
            lines.append(f'  未就绪 {row["label"]}（{row["lane"]}）：'
                         f'{CAUSE.get(row["reason"], row["reason"])}')
        for row in record['blocked']:
            lines.append(f'  被挡 {row["label"]}（{row["lane"]}）：队首 {row["head"]}')
        for entry in record['parallel']:
            lines.append(f'  可并行：{"、".join(entry["lanes"])} 的{entry["kind"]}'
                         f'（{entry["reason"]}）')
    for hazard in group['hazards']:
        lines.append(f'禁区：{hazard["if"]}（{hazard["lane"]}）')
        lines.append('  队列会变成：' + ' → '.join(
            f'{row["label"]}[{row["cause"]}]' for row in hazard['order']))
        for row in hazard['blocked_normal']:
            lines.append(f'  被挡的正常件：{row["label"]}（{hazard["lane"]}）')
        lines.append(f'  后果：{hazard["effect"]}')
    for outcome in group['contingency']:
        missed = outcome['missed']
        lines.append(f'若第 1 轮漏确认 {missed["label"]}（{missed["lane"]}）：'
                     f'{DEADLINE.get(outcome["deadline"], outcome["deadline"])}')
        if not outcome['drop']:
            lines.append('  第二轮换掉：无（该漏项不占重投额度，只需补一次确认）')
        for row in outcome['drop']:
            kind = '重投' if row['kind'] == 'redrive' else '确认'
            name = row.get('label') or f'源投递 #{row.get("source")}'
            lines.append(f'  第二轮换掉：{kind} {name}（{row["lane"]}）')
        for row in outcome['add']:
            kind = '重投' if row['kind'] == 'redrive' else '确认'
            lines.append(f'  第二轮补上：{kind} {row.get("label", "#" + str(row.get("source")))}'
                         f'（{row["lane"]}）')
        for row in outcome['alarms_at_risk']:
            lines.append(f'  受影响的正常件：{row["label"]}（{row["lane"]}，'
                         f'{CAUSE.get(row["cause"], row["cause"])}）')
        lines.append(f'  处置：{outcome["recovery"]}')
    return '\n'.join(lines)
