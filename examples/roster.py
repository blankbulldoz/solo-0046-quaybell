"""值班重投排程：用真实 HTTP 实例复现两轮重投清单并核对承诺。

样本：热键 coil-hot 上有两条死信，其中一条已有等待中的后继；coil-a、coil-b
各有一条老死信。两轮交界时热键再发布一条正常告警。处置人每轮只保证确认一个
热键件，每轮最多接两个重投。

`nominal` 按清单走完两轮，用队列观察和领取、确认、重投的原始回执逐条核对；
`cold_drift` 与 `hot_drift` 分别演示第一轮漏确认冷键后继和热键后继时，清单
给出的第二轮换项、被挡的正常件和两轮期限。
"""
import json

from quaybell import roster as planner
from quaybell.codec import fingerprint, pack

from .session import session

POLICY = dict(lease_seconds=300, retry_seconds=60, max_attempts=2, retention_seconds=86400)
GROUP = 'duty-desk'
MAIN = 'roster-main'
COLD_DRIFT = 'roster-cold-drift'
HOT_DRIFT = 'roster-hot-drift'
HAZARD = 'roster-hazard'
ALERTS = (('alert-hot', 'coil-hot'),)


# ---------------------------------------------------------------- 公共动作

def _claim(client, channel, limit=6):
    return client.request(f'/channels/{channel}/groups/{GROUP}/claim',
                          {'worker': 'duty-desk', 'limit': limit})['items']


def _publish(client, channel, key, lane, producer='pager-fleet'):
    return client.request(f'/channels/{channel}/messages', {
        'producer': producer, 'key': key, 'lane': lane,
        'payload': {'key': key}, 'tags': {}}, 201)


def _ack(client, channel, item):
    reply = client.request(f"/deliveries/{item['delivery_id']}/ack",
                           {'token': item['lease']['token']})
    assert reply['delivery_id'] == item['delivery_id']
    assert reply['attempt'] == item['lease']['number']
    assert reply['state'] == 'acked' and reply['repeated'] is False
    return reply


def _nack(client, channel, item):
    return client.request(f"/deliveries/{item['delivery_id']}/nack",
                          {'token': item['lease']['token'], 'reason': '下游解码失败'})


def _redrive(client, channel, source):
    return client.request(f'/deliveries/{source}/redrive', {}, 201)


def _board(client, channel):
    """一次队列观察：未完成项按投递 ID 排序，带队首与原因；死信带后继。"""
    record = client.request(f'/channels/{channel}')
    group = record['groups'][0]
    return {'observed_at': record['observed_at'], 'counts': group['counts'],
            'queue': [{'id': item['id'], 'lane': item['lane'], 'state': item['state'],
                       'cause': item['cause'], 'head': item['head_id']}
                      for item in group['sample']],
            'dead': [{'id': item['id'], 'lane': item['lane'], 'message': item['message_id'],
                      'successor': item['redriven_as']} for item in record['dead']]}


def _lane(board, lane):
    rows = [item for item in board['queue'] if item['lane'] == lane]
    assert [item['id'] for item in rows] == sorted(item['id'] for item in rows)
    return rows


def _roster(client, channel):
    return client.request(f'/channels/{channel}/roster')['groups'][0]


def _tag(batch, observed_successor, issued):
    """给领取项标上清单令牌，便于和预测集合比较。"""
    redriven = {record['delivery_id']: source for source, record in issued.items()}
    for item in batch:
        delivery = item['delivery_id']
        item['_token'] = (f'delivery:{delivery}' if delivery == observed_successor
                          else f'redrive:{redriven[delivery]}')
    return batch


# ---------------------------------------------------------------- 造样本

def _build(client, channel):
    """复现四条死信，并给第二条热键死信追加一个等待中的后继。"""
    client.request('/channels', {'name': channel, 'policy': POLICY}, 201)
    client.request(f'/channels/{channel}/groups', {'name': GROUP}, 201)
    keys = (('hot-alpha', 'coil-hot'), ('hot-beta', 'coil-hot'),
            ('cold-a', 'coil-a'), ('cold-b', 'coil-b'))
    messages, dead = {}, {}
    for key, lane in keys:
        reply = _publish(client, channel, key, lane)
        messages[key] = reply['message_id']
        dead[key] = reply['fanout'][GROUP]
    # max_attempts=2；coil-hot 上 hot-beta 一直压在 hot-alpha 后面，直到它终态。
    for expected, state in ((('hot-alpha', 'cold-a', 'cold-b'), 'waiting'),
                            (('hot-alpha', 'cold-a', 'cold-b'), 'dead'),
                            (('hot-beta',), 'waiting'), (('hot-beta',), 'dead')):
        batch = _claim(client, channel)
        got = tuple(next(key for key, delivery in dead.items()
                         if delivery == item['delivery_id']) for item in batch)
        assert got == expected, (expected, got)
        for item in batch:
            assert _nack(client, channel, item)['state'] == state
        client.tick(POLICY['retry_seconds'] + 1)
    successor = _redrive(client, channel, dead['hot-beta'])
    assert successor['source_id'] == dead['hot-beta'] and successor['repeated'] is False
    return {'messages': messages, 'dead': dead, 'successor': successor['delivery_id']}


def _alarms(client, channel):
    """交界发布：热键上的一条正常告警（封面场景的唯一交界告警）。"""
    reply = {key: _publish(client, channel, key, lane, 'alert-publisher')
             for key, lane in ALERTS}
    return {key: {'message': reply[key]['message_id'],
                  'delivery': reply[key]['fanout'][GROUP]} for key in reply}


# ---------------------------------------------------------------- 名义排程

def _nominal(client):
    facts = _build(client, MAIN)
    dead, successor = facts['dead'], facts['successor']
    before = _board(client, MAIN)
    assert [item['id'] for item in before['queue']] == [successor]
    assert sorted(item['id'] for item in before['dead']) == sorted(dead.values())
    assert _lane(before, 'coil-hot') == [{'id': successor, 'lane': 'coil-hot',
                                          'state': 'waiting', 'cause': 'ready',
                                          'head': successor}]
    group = _roster(client, MAIN)
    assert group['hot_lane'] == 'coil-hot'
    first, second = group['rounds']
    assert first['basis'] == 'observed' and second['basis'] == 'projected'
    assert [row['source'] for row in first['redrives']] == [dead['cold-a'], dead['cold-b']]
    assert {item['token'] for item in first['confirm']} == {
        f'delivery:{successor}', f"redrive:{dead['cold-a']}", f"redrive:{dead['cold-b']}"}
    assert [row['source'] for row in first['holds']] == [dead['hot-alpha']]
    assert first['blocked'] == [] and second['blocked'] == []
    assert {entry['kind'] for entry in first['parallel']} == {'redrives', 'confirm'}

    # 第 1 轮：先并行追加两个冷键后继，再同批确认三个键的队首。
    issued = {row['source']: _redrive(client, MAIN, row['source']) for row in first['redrives']}
    assert len({record['delivery_id'] for record in issued.values()}) == 2
    batch = _tag(_claim(client, MAIN, 6), successor, issued)
    assert {item['_token'] for item in batch} == {item['token'] for item in first['confirm']}
    receipts = {item['_token']: _ack(client, MAIN, item) for item in batch}
    stale = next(item for item in batch if item['_token'] == f'delivery:{successor}')
    assert client.request(f'/deliveries/{successor}/ack',
                          {'token': 'not-a-' + stale['lease']['token']}, 409)['error'] == 'conflict'
    replayed = client.request(f'/deliveries/{successor}/ack', {'token': stale['lease']['token']})
    assert replayed['repeated'] is True and replayed['state'] == 'acked'
    after_first = _board(client, MAIN)
    assert after_first['queue'] == []
    assert all(not row['queue'] for row in first['after'])

    # 交界：热键发布一条正常告警，已可领。
    alarms = _alarms(client, MAIN)
    boundary = _board(client, MAIN)
    assert [item['id'] for item in boundary['queue']] == [alarms['alert-hot']['delivery']]
    assert boundary['queue'][0]['cause'] == 'ready'
    assert boundary['queue'][0]['head'] == boundary['queue'][0]['id']
    assert [row['lane'] for row in second['redrives']] == ['coil-hot']
    assert [row['source'] for row in second['redrives']] == [dead['hot-alpha']]

    # 第 2 轮：先追加热键后继（排在交界告警之后），再确认交界告警。
    hot_redrive = _redrive(client, MAIN, dead['hot-alpha'])
    spread = _board(client, MAIN)
    hot_lane = _lane(spread, 'coil-hot')
    assert [item['id'] for item in hot_lane] == [alarms['alert-hot']['delivery'],
                                                 hot_redrive['delivery_id']]
    assert hot_lane[0]['cause'] == 'ready' and hot_lane[1]['cause'] == 'blocked'
    assert hot_lane[1]['head'] == alarms['alert-hot']['delivery']
    assert _lane(spread, 'coil-a') == [] and _lane(spread, 'coil-b') == []

    batch = _claim(client, MAIN, 6)
    assert [item['delivery_id'] for item in batch] == [alarms['alert-hot']['delivery']]
    for item in batch:
        _ack(client, MAIN, item)
    assert sorted(row['source'] for row in second['linked']) == sorted(dead.values())
    tail = _board(client, MAIN)
    assert [item['id'] for item in tail['queue']] == [hot_redrive['delivery_id']]
    assert tail['counts'] == {'ready': 1}

    # 第 3 轮只收热键新后继；收完后证据里的后继链闭合、摘要可复算。
    final = _claim(client, MAIN)
    assert [item['delivery_id'] for item in final] == [hot_redrive['delivery_id']]
    _ack(client, MAIN, final[0])
    assert _board(client, MAIN)['queue'] == []
    trace = client.request(f"/messages/{facts['messages']['hot-alpha']}")
    lineage = [item for item in trace['deliveries'] if item['id'] == hot_redrive['delivery_id']]
    assert lineage[0]['parent_id'] == dead['hot-alpha'] and lineage[0]['state'] == 'acked'
    evidence = client.request(f"/messages/{facts['messages']['hot-alpha']}/evidence")
    digest = evidence.pop('sha256')
    assert fingerprint(pack(evidence)) == digest
    assert evidence['observed_at'] == _board(client, MAIN)['observed_at']

    return {
        'sample': {'channel': MAIN, 'policy': POLICY, 'messages': facts['messages'],
                   'dead': dead, 'hot_successor': successor},
        'roster': {'hot_lane': group['hot_lane'], 'constraints': group['constraints'],
                   'round1': _summary(first, issued, receipts),
                   'round2': _summary(second, {}, {}),
                   'deadline': group['deadline']},
        'brief': planner.render(group),
        'checks': {'queue_before': before['queue'], 'queue_after': tail['queue'],
                   'dead_sources': sorted(row['source'] for row in second['linked']),
                   'evidence_sha256': digest},
    }


def _summary(round_record, issued, receipts):
    return {'redrives': [{'lane': row['lane'], 'source': row['source'],
                          'delivery': issued[row['source']]['delivery_id']
                                      if row['source'] in issued else None}
                         for row in round_record['redrives']],
            'confirm': [{'lane': item['lane'], 'label': item['label'], 'token': item['token'],
                         'receipt': receipts.get(item['token'])}
                        for item in round_record['confirm']],
            'holds': round_record['holds'],
            'blocked': round_record['blocked'],
            'parallel': [entry['kind'] for entry in round_record['parallel']]}


# ---------------------------------------------------------------- 偏差

def _prepare_drift(client, channel):
    """第一轮按清单重投两个冷键并同批领取，返回未确认项交给调用者。"""
    facts = _build(client, channel)
    group = _roster(client, channel)
    first, second = group['rounds']
    issued = {row['source']: _redrive(client, channel, row['source'])
              for row in first['redrives']}
    batch = _tag(_claim(client, channel, 6), facts['successor'], issued)
    assert {item['_token'] for item in batch} == {item['token'] for item in first['confirm']}
    return facts, group, first, second, issued, batch


def _cold_drift(client):
    """漏确认一个冷键后继：冷键不受热键额度限制，第二轮连做两次确认即可。"""
    facts, group, first, second, issued, batch = _prepare_drift(client, COLD_DRIFT)
    cold_b = facts['dead']['cold-b']
    held = next(item for item in batch if item['_token'] == f'redrive:{cold_b}')
    for item in batch:
        if item is not held:
            _ack(client, COLD_DRIFT, item)
    contingency = next(row for row in group['contingency']
                       if row['missed']['token'] == f'redrive:{cold_b}')
    assert contingency['missed']['hot'] is False
    assert contingency['deadline'] == 'met'
    # 冷键漏项不占热键额度：无需换掉任何重投。
    assert contingency['drop'] == []
    assert [row['token'] for row in contingency['add']] == [f'redrive:{cold_b}']
    assert contingency['alarms_at_risk'] == []
    assert contingency['alarms_still_at_risk'] == []
    assert [row['source'] for row in contingency['swap_round2']['redrives']] == [
        facts['dead']['hot-alpha']]
    # 交界只在热键发布一条告警：冷键的第二次确认后没有别的正常件。
    swapped = [(row['lane'], row['label']) for row in contingency['swap_round2']['confirm']]
    assert swapped == [
        ('coil-b', f'后继({cold_b})'),
        ('coil-hot', '交界告警(coil-hot)')], swapped

    alarms = _alarms(client, COLD_DRIFT)
    boundary = _board(client, COLD_DRIFT)
    lane_b = _lane(boundary, 'coil-b')
    assert [item['id'] for item in lane_b] == [issued[cold_b]['delivery_id']]
    # 漏确认的后继仍是租约中的队首，热键交界告警不受影响。
    assert lane_b[0]['cause'] == 'leased'
    assert [item['id'] for item in _lane(boundary, 'coil-hot')] == [
        alarms['alert-hot']['delivery']]

    # 第二轮：热键重投照常；补确认冷键后继后，热键交界告警仍可确认。
    hot_redrive = _redrive(client, COLD_DRIFT, facts['dead']['hot-alpha'])
    assert _ack(client, COLD_DRIFT, held)['state'] == 'acked'
    catch_up = _claim(client, COLD_DRIFT, 6)
    assert [item['delivery_id'] for item in catch_up] == [alarms['alert-hot']['delivery']]
    for item in catch_up:
        _ack(client, COLD_DRIFT, item)
    tail = _board(client, COLD_DRIFT)
    assert [item['id'] for item in tail['queue']] == [hot_redrive['delivery_id']]
    last = _claim(client, COLD_DRIFT)
    assert [item['delivery_id'] for item in last] == [hot_redrive['delivery_id']]
    _ack(client, COLD_DRIFT, last[0])
    assert _board(client, COLD_DRIFT)['queue'] == []
    return {'channel': COLD_DRIFT, 'missed': contingency['missed'],
            'round2': {'drop': contingency['drop'], 'add': contingency['add'],
                       'alarms_at_risk': contingency['alarms_at_risk'],
                       'recovery': contingency['recovery']},
            'checks': {'cold_lane_after_drift': lane_b,
                       'hot_redrive': hot_redrive['delivery_id'],
                       'queue_after': _board(client, COLD_DRIFT)['queue']}}


def _hot_drift(client):
    """漏确认热键后继：第二轮唯一的热键确认只能补它，交界热告警落到第三轮。"""
    facts, group, first, second, issued, batch = _prepare_drift(client, HOT_DRIFT)
    successor, hot_alpha = facts['successor'], facts['dead']['hot-alpha']
    held = next(item for item in batch if item['_token'] == f'delivery:{successor}')
    for item in batch:
        if item is not held:
            _ack(client, HOT_DRIFT, item)
    contingency = next(row for row in group['contingency']
                       if row['missed']['token'] == f'delivery:{successor}')
    assert contingency['missed']['hot'] is True
    assert contingency['deadline'] == 'missed_unless_extra_hot_confirmation'
    assert [row['source'] for row in contingency['drop'] if row.get('kind') == 'redrive'] == [
        hot_alpha]
    assert [row['token'] for row in contingency['drop'] if row.get('kind') == 'confirm'] == [
        'assumed:coil-hot:交界告警(coil-hot)']
    assert [row['token'] for row in contingency['add']] == [f'delivery:{successor}']
    assert [row['label'] for row in contingency['alarms_at_risk']] == ['交界告警(coil-hot)']
    beyond = contingency['beyond_promise']
    assert beyond['meets_deadline'] is True
    assert [row['label'] for row in beyond['confirm']] == [
        f'#{successor}', '交界告警(coil-hot)'], [row['label'] for row in beyond['confirm']]

    alarms = _alarms(client, HOT_DRIFT)
    boundary = _board(client, HOT_DRIFT)
    hot_lane = _lane(boundary, 'coil-hot')
    assert [item['id'] for item in hot_lane] == [successor, alarms['alert-hot']['delivery']]
    assert hot_lane[1]['cause'] == 'blocked' and hot_lane[1]['head'] == successor

    # 第二轮不重投 hot-alpha。热键确认额度先补确认后继，交界热告警只能等第三轮：
    # 它可以被同一批领到，但本轮不确认，凭据留到第三轮再用。
    assert _ack(client, HOT_DRIFT, held)['state'] == 'acked'
    catch_up = _claim(client, HOT_DRIFT, 6)
    assert [item['delivery_id'] for item in catch_up] == [alarms['alert-hot']['delivery']]
    deferred = catch_up[0]
    pending = _board(client, HOT_DRIFT)
    assert [item['id'] for item in pending['queue']] == [alarms['alert-hot']['delivery']]
    assert pending['queue'][0]['cause'] == 'leased'
    assert _ack(client, HOT_DRIFT, deferred)['state'] == 'acked'
    assert _board(client, HOT_DRIFT)['queue'] == []
    trace = client.request(f"/messages/{facts['messages']['hot-alpha']}")
    assert [item for item in trace['deliveries'] if item['parent_id'] == hot_alpha] == []
    return {'channel': HOT_DRIFT, 'missed': contingency['missed'],
            'round2': {'drop': contingency['drop'], 'add': contingency['add'],
                       'alarms_at_risk': contingency['alarms_at_risk'],
                       'beyond_promise': beyond, 'recovery': contingency['recovery']},
            'checks': {'hot_lane_after_drift': hot_lane,
                       'deferred_hot_alarm': alarms['alert-hot']['delivery'],
                       'queue_after': _board(client, HOT_DRIFT)['queue'],
                       'hot_alpha_successors': []}}


def _hazard(client):
    """反例核对：交界前重投热键死信会把交界热告警压到两条后继之后。"""
    facts = _build(client, HAZARD)
    group = _roster(client, HAZARD)
    hazard = group['hazards'][0]
    assert hazard['lane'] == 'coil-hot'
    assert [row['label'] for row in hazard['blocked_normal']] == ['交界告警(coil-hot)']
    premature = _redrive(client, HAZARD, facts['dead']['hot-alpha'])
    alarms = _alarms(client, HAZARD)
    hot_lane = _lane(_board(client, HAZARD), 'coil-hot')
    assert [item['id'] for item in hot_lane] == [facts['successor'],
                                                 premature['delivery_id'],
                                                 alarms['alert-hot']['delivery']]
    assert [item['cause'] for item in hot_lane] == ['ready', 'blocked', 'blocked']
    # 第二轮热键只能确认已在等待的后继；交界热告警仍是第三条，两轮内确认不完。
    first = _claim(client, HAZARD, 1)
    assert [item['delivery_id'] for item in first] == [facts['successor']]
    _ack(client, HAZARD, first[0])
    second = _claim(client, HAZARD, 1)
    assert [item['delivery_id'] for item in second] == [premature['delivery_id']]
    _ack(client, HAZARD, second[0])
    pending = _board(client, HAZARD)
    hot_rest = _lane(pending, 'coil-hot')
    assert len(hot_rest) == 1 and hot_rest[0]['id'] == alarms['alert-hot']['delivery'], (
        [(item['id'], item['lane'], item['cause']) for item in pending['queue']])
    # 第三轮：热告警在同批可领，冷键两条告警也同时可领。
    third = _claim(client, HAZARD, 6)
    assert [item['delivery_id'] for item in third] == [alarms['alert-hot']['delivery']]
    for item in third:
        _ack(client, HAZARD, item)
    assert _board(client, HAZARD)['queue'] == []
    trace = client.request(f"/messages/{facts['messages']['hot-alpha']}")
    assert [item for item in trace['deliveries']
            if item['parent_id'] == facts['dead']['hot-alpha']]
    return {'channel': HAZARD, 'hazard': hazard,
            'observed_order': [item['id'] for item in hot_lane],
            'hot_alarm_left_for_third_round': alarms['alert-hot']['delivery']}


def roster(client):
    return {'nominal': _nominal(client), 'cold_drift': _cold_drift(client),
            'hot_drift': _hot_drift(client), 'hazard': _hazard(client)}


def main():
    with session() as client:
        result = roster(client)
    print(json.dumps(result, ensure_ascii=False, indent=1))


if __name__ == '__main__':
    main()
