"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import logging

from quaybell.assignments import partition_for
from quaybell.codec import fingerprint, pack

from .session import CHANNEL, GROUP, POLICY, envelope, session


def separated_pair():
    """找两个在 2 分片与 3 分片下都落在不同分片的顺序键，让演练归属确定。"""
    names = [f'device-key-{i:02d}' for i in range(300)]
    for hot in names:
        for idle in names:
            if hot == idle:
                continue
            if (partition_for(hot, 2) != partition_for(idle, 2)
                    and partition_for(hot, 3) != partition_for(idle, 3)):
                return hot, idle
    raise AssertionError('找不到可分离的顺序键')



def channel(client):
    record = client.channel()
    client.request('/channels', {'name': 'wire-gate'}, 409)
    assert record['policy']['version'] == 1
    return record


def revision(client):
    client.channel()
    client.subscribe()
    old = client.publish()
    data = POLICY | {'expected': 1, 'lease_seconds': 12}
    record = client.request(CHANNEL + '/policy', data, 201)
    client.request(CHANNEL + '/policy', data, 409)
    new = client.publish('new-signal', 'coil-south')
    first, second = client.claim(2)
    assert (old['policy'], new['policy']) == (1, 2)
    assert second['lease']['expires'] - first['lease']['expires'] == 8
    return {'active': record['version'], 'old_lease': first['lease']['expires'], 'new_lease': second['lease']['expires']}


def group(client):
    client.channel()
    client.publish('early')
    client.subscribe(tags={'feed': 'samples'})
    hit = client.publish('matched')
    miss = client.publish('wrong', tags={'feed': 'alerts'})
    assert list(hit['fanout']) == ['oscillator'] and miss['fanout'] == {}
    assert client.claim()[0]['message_id'] == hit['message_id']
    return {'matched': hit['fanout'], 'unmatched': miss['fanout']}


def preview(client):
    client.channel()
    client.subscribe(tags={'feed': 'samples'})
    request = envelope()
    candidate = client.request(CHANNEL + '/preview', request)
    client.request(CHANNEL + '/preview', request | {'tags': {'feed': []}}, 400)
    assert candidate['groups'] == ['oscillator']
    assert client.request(CHANNEL)['messages'] == []
    return candidate


def publish(client):
    client.channel()
    client.subscribe()
    first = client.publish(payload={'second': 2, 'first': 1})
    client.subscribe('after-image')
    same = client.publish(expected=200, payload={'first': 1, 'second': 2})
    client.publish(expected=409, payload={'first': 2, 'second': 2})
    assert same == first | {'repeated': True}
    return same


def inspect(client):
    client.channel()
    client.subscribe()
    sent = client.publish()
    taken = client.claim()[0]
    client.settle(taken)
    trace = client.request(f"/messages/{sent['message_id']}")
    client.request('/messages/99999', expected=404)
    assert [event['kind'] for event in trace['events']] == ['published', 'leased', 'acked']
    assert taken['lease']['token'] not in pack(trace)
    return trace


def pause(client):
    client.channel()
    client.subscribe()
    client.publish()
    client.publish('second', 'coil-south')
    taken = client.claim()[0]
    client.request(GROUP + '/pause', {'paused': True})
    assert client.claim() == [] and client.settle(taken)['state'] == 'acked'
    client.request(GROUP + '/pause', {'paused': False})
    return {'resumed_delivery': client.claim()[0]['delivery_id']}


def claim(client):
    client.channel()
    client.subscribe()
    head = client.publish(delay_seconds=4)
    tail = client.publish('tail')
    independent = client.publish('aside', 'coil-south')
    first = client.claim(3)
    assert [item['message_id'] for item in first] == [independent['message_id']]
    client.tick(4)
    next_item = client.claim()[0]
    assert next_item['message_id'] == head['message_id']
    client.settle(next_item)
    assert client.claim()[0]['message_id'] == tail['message_id']
    return {'initial': independent['message_id'], 'head': head['message_id'], 'after_ack': tail['message_id']}


def ack(client):
    client.channel()
    client.subscribe()
    client.publish()
    taken = client.claim()[0]
    a = client.settle(taken)
    b = client.settle(taken)
    client.settle(taken | {'lease': {'token': 'stale-lease'}}, expected=409)
    assert b == a | {'repeated': True}
    return b


def retry(client):
    client.channel()
    client.subscribe()
    client.publish()
    first = client.claim()[0]
    next_time = client.settle(first, 'nack')['effective_at']
    assert client.claim() == []
    client.tick(3)
    second = client.claim()[0]
    assert second['lease']['number'] == 2 and second['lease']['token'] != first['lease']['token']
    client.settle(first, expected=409)
    return {'next_time': next_time, 'final': client.settle(second, 'nack')}


def sweep(client):
    client.channel()
    client.subscribe()
    client.publish()
    item = client.claim()[0]
    client.tick(4)
    client.settle(item, expected=409)
    client.tick(3)
    result = client.request('/sweep', {'limit': 1})
    assert result['recovered'][0]['effective_at'] == item['lease']['expires'] + 3
    assert client.claim()[0]['lease']['number'] == 2
    return result


def dead(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    client.subscribe('witness')
    item = client.publish()
    client.request(f"/deliveries/{item['fanout']['witness']}/redrive", {}, 409)
    client.settle(client.claim()[0], 'nack')
    rows = client.request(CHANNEL + '/dead')['items']
    assert len(rows) == 1 and rows[0]['group_name'] == 'oscillator'
    assert rows[0]['last_attempt']['outcome'] == 'nacked'
    return rows[0]


def redrive(client):
    first = dead(client)
    client.request(CHANNEL + '/policy', POLICY | {'expected': 1}, 201)
    path = f"/deliveries/{first['id']}/redrive"
    new = client.request(path, {}, 201)
    assert client.request(path, {}) == new | {'repeated': True}
    assert client.claim()[0]['policy'] == 2
    return new


def board(client):
    client.channel()
    client.subscribe()
    client.publish(delay_seconds=20)
    client.publish('tail')
    client.publish('ready', 'coil-south')
    old = client.request(CHANNEL)['groups'][0]
    client.request(GROUP + '/pause', {'paused': True})
    now = client.request(CHANNEL)['groups'][0]
    assert old['counts'] == dict(blocked=1, delayed=1, ready=1)
    assert now['counts'] == dict(blocked=1, paused=2)
    return {'first': old['counts'], 'after_pause': now['counts']}


def retention(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    first = client.publish()
    later = client.publish('stay', 'coil-south')
    client.settle(client.claim()[0], 'nack')
    root = first['fanout']['oscillator']
    child = client.request(f'/deliveries/{root}/redrive', {}, 201)
    picked = client.claim(2)
    assert {row['delivery_id'] for row in picked} == {child['delivery_id'], later['fanout']['oscillator']}
    client.settle(next(row for row in picked if row['delivery_id'] == child['delivery_id']))
    client.tick(11)
    plan = client.request(CHANNEL + '/retention')
    assert [row['id'] for row in plan['eligible']] == [first['message_id']]
    done = client.request(CHANNEL + '/retention', {})
    client.request(f"/messages/{first['message_id']}", expected=404)
    assert done['removed'] == [first['message_id']] and client.publish()['message_id'] > later['message_id']
    return done


def export(client):
    facts = inspect(client)
    record = client.request(f"/messages/{facts['message']['id']}/evidence")
    digest = record.pop('sha256')
    assert record['trace'] == facts and fingerprint(pack(record)) == digest
    return {'format': record['format'], 'sha256': digest}


def partitions(client):
    """2 分片调到 3：空闲键立即迁走，热键旧债务仍排在后继之前，版本/归属可核对。"""
    client.channel(dict(lease_seconds=30, retry_seconds=1, max_attempts=8, retention_seconds=3600))
    client.subscribe()
    hot_lane, idle_lane = separated_pair()
    # 初始：两个固定分片，两个进程各接一个（round_robin：p0->process-a, p1->process-b）
    first = client.request(GROUP + '/partitions',
                           {'partitions': 2, 'workers': ['process-a', 'process-b']})
    assert first['version'] == 1 and len(first['partitions']) == 2
    hot_p2, idle_p2 = partition_for(hot_lane, 2), partition_for(idle_lane, 2)
    assert hot_p2 != idle_p2
    hot_owner_v1 = first['partitions'][hot_p2]['owner']
    idle_owner_v1 = first['partitions'][idle_p2]['owner']
    # 热键两条（旧债务 + 后继），空闲键一条
    client.publish('hot-debt', hot_lane)
    client.publish('hot-successor', hot_lane)
    client.publish('idle-one', idle_lane)
    # 热键：领取 -> 拒绝退避 -> 再次领取，形成“旧版租约 + 重试”
    debt = client.claim(1, worker=hot_owner_v1)[0]
    assert debt['assignment_version'] == 1 and debt['partition'] == hot_p2
    client.settle(debt, 'nack')
    assert client.claim(1, worker=hot_owner_v1) == []
    client.tick(1)
    old_lease = client.claim(1, worker=hot_owner_v1)[0]
    assert old_lease['lease']['number'] == 2 and old_lease['lease']['assignment_version'] == 1
    # 空闲键由其属主处理并确认，进入 idle
    idle_item = client.claim(1, worker=idle_owner_v1)[0]
    assert idle_item['lane'] == idle_lane
    client.settle(idle_item)
    # 调到 3 分片：热键改属 process-b（旧进程失权 -> 围栏），空闲键改属新进程 process-c
    hot_p3, idle_p3 = partition_for(hot_lane, 3), partition_for(idle_lane, 3)
    remaining = next(p for p in range(3) if p not in (hot_p3, idle_p3))
    plan = {str(hot_p3): 'process-b', str(idle_p3): 'process-c', str(remaining): 'process-a'}
    second = client.request(GROUP + '/partitions',
                            {'partitions': 3, 'assignment': plan, 'expected': 1})
    assert second['version'] == 2 and len(second['fenced_leases']) == 1
    assert second['fenced_leases'][0]['delivery_id'] == debt['delivery_id']
    # 旧版凭据不能再处置（代际围栏）
    client.settle(old_lease, 'ack', expected=409)
    # 空闲键无需等待，新属主立刻可领
    client.publish('idle-two', idle_lane)
    moved_idle = client.claim(1, worker='process-c')
    assert len(moved_idle) == 1 and moved_idle[0]['lane'] == idle_lane
    # 热键新属主一次最多只拿到旧债务队首，绝不能越过它拿到后继
    hot_batch = client.claim(3, worker='process-b')
    assert [item['delivery_id'] for item in hot_batch] == [debt['delivery_id']]
    takeover = hot_batch[0]
    assert takeover['lease']['number'] == 3 and takeover['lease']['assignment_version'] == 2
    client.request(GROUP + '/claim', {'worker': 'process-a', 'partition': hot_p3}, 403)
    client.settle(takeover)
    successor = client.claim(1, worker='process-b')[0]
    assert successor['message_id'] != debt['message_id'] and successor['lane'] == hot_lane
    client.settle(successor)
    # 操作者核对版本与每键当前归属
    board = client.request(GROUP + '/assignments')
    assert board['version'] == 2 and board['partition_count'] == 3
    owners = {row['lane']: row for row in board['keys']}
    assert owners[hot_lane]['owner'] == 'process-b'
    assert owners[idle_lane]['owner'] == 'process-c'
    assert owners[hot_lane]['state'] == 'idle'
    scoped = client.request(GROUP + '/keys?worker=process-b')
    assert all(row['owner'] == 'process-b' for row in scoped['keys']) and \
        any(row['lane'] == hot_lane for row in scoped['keys'])
    auth = client.request(GROUP + '/authorize',
                          {'worker': 'process-b', 'lanes': [hot_lane, idle_lane]})
    assert {row['lane']: row['authorized'] for row in auth['decisions']} == \
        {hot_lane: True, idle_lane: False}
    trace = client.request(f"/messages/{debt['message_id']}")
    leased = [event for event in trace['events'] if event['kind'] == 'leased']
    assert [event['details'].get('assignment_version') for event in leased] == [1, 1, 2]
    assert any(event['kind'] == 'lease_fenced' for event in trace['events'])
    return {'version': board['version'], 'hot_key': hot_lane, 'idle_key': idle_lane,
            'hot_owner': owners[hot_lane]['owner'], 'idle_owner': owners[idle_lane]['owner'],
            'debt_delivery': debt['delivery_id'], 'successor': successor['delivery_id']}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, partitions=partitions)


def main():
    parser = argparse.ArgumentParser(description='运行一个有界的真实中继工作流')
    parser.add_argument('workflow', choices=FLOWS.keys())
    args = parser.parse_args()
    logging.getLogger().setLevel(logging.WARNING)
    with session() as client:
        result = FLOWS[args.workflow](client)
    print(f'{args.workflow} 通过 {pack(result)}')


if __name__ == '__main__':
    main()
