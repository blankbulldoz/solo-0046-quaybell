"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import logging

from quaybell.codec import fingerprint, pack

from .session import CHANNEL, GROUP, POLICY, envelope, session


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


def _sample(board, delivery_id):
    return next(item for group in board['groups'] for item in group['sample']
                if item['id'] == delivery_id)


def requirement(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    cal_a = client.publish('cal-a', 'coil-cal')
    cal_b = client.publish('cal-b', 'coil-cal')
    sample = client.publish('sample', 'coil-sample')
    other = client.publish('other-sample', 'coil-other')
    a_id, b_id, sample_id, other_id = (cal_a['fanout']['oscillator'], cal_b['fanout']['oscillator'],
                                       sample['fanout']['oscillator'], other['fanout']['oscillator'])

    # 采样依赖两台设备校准；重复声明幂等，不会重复落库。
    declared = client.require(sample_id, [a_id, b_id])
    assert declared == {'delivery_id': sample_id, 'requires': [a_id, b_id],
                        'added': [a_id, b_id], 'repeated': False}
    repeated = client.require(sample_id, [a_id, b_id], expected=200)
    assert repeated['repeated'] and repeated['requires'] == [a_id, b_id]

    # 积压解释与领取一致：采样被门控，其他顺序键照常消费；同键校准仍按队首顺序领取。
    board = client.board()
    gated = _sample(board, sample_id)
    assert gated['cause'] == 'gated' and gated['prerequisites'] == [a_id, b_id]
    assert [row['delivery_id'] for row in gated['blocking_prerequisites']] == [a_id, b_id]
    picked = client.claim(5)
    assert [item['delivery_id'] for item in picked] == [a_id, other_id]

    # 确认第一台校准后，采样仍被另一台门控；全部确认后采样才就绪。
    client.settle(next(item for item in picked if item['delivery_id'] == a_id))
    board = client.board()
    waiting = _sample(board, sample_id)
    assert waiting['cause'] == 'gated'
    assert [row['delivery_id'] for row in waiting['blocking_prerequisites']] == [b_id]
    next_round = client.claim(5)
    assert [item['delivery_id'] for item in next_round] == [b_id]
    client.settle(next_round[0])
    unblocked = client.claim()
    assert [item['delivery_id'] for item in unblocked] == [sample_id]
    client.settle(unblocked[0])

    # 前置终败：采样永远不会就绪，解释里能看到是哪条死信及原因。
    doomed = client.publish('doomed-sample', 'coil-sample')
    doomed_id = doomed['fanout']['oscillator']
    bad_cal = client.publish('bad-cal', 'coil-cal')
    bad_id = bad_cal['fanout']['oscillator']
    client.require(doomed_id, [bad_id])
    bad_lease = next(item for item in client.claim(5) if item['delivery_id'] == bad_id)
    client.settle(bad_lease, 'nack')
    assert client.claim() == []
    board = client.board()
    stalled = _sample(board, doomed_id)
    assert stalled['cause'] == 'gated_dead'
    detail = stalled['blocking_prerequisites'][0]
    assert detail == {'delivery_id': bad_id, 'state': 'dead', 'reason': '下游拒绝校验帧'}
    trace = client.trace(doomed['message_id'])
    assert trace['deliveries'][0]['requires'] == [bad_id]

    # 同键先入队的采样反过来依赖后入队校准：同键死锁，拒绝并说明无法推进。
    earlier = client.publish('earlier', 'coil-north')
    later = client.publish('later', 'coil-north')
    earlier_id, later_id = (earlier['fanout']['oscillator'], later['fanout']['oscillator'])
    conflict = client.require(earlier_id, [later_id], expected=409)
    assert '永远无法确认' in conflict['message'] and f'#{later_id}' in conflict['message']

    # 跨顺序键的循环依赖同样被拒绝；自身依赖属于请求格式错误，未知投递按 404 处理。
    loop_b = client.publish('loop-b', 'coil-loop-b')['fanout']['oscillator']
    client.require(loop_b, [earlier_id])
    cycle = client.require(earlier_id, [loop_b], expected=409)
    assert '循环依赖' in cycle['message']
    self_ref = client.require(earlier_id, [earlier_id], expected=400)
    assert '自身' in self_ref['message']
    client.require(99999, [earlier_id], expected=404)
    return {'gated_sample': sample_id, 'terminal_failure': stalled['cause'],
            'rejected_lane_dependency': later_id, 'other_lane_delivered': other_id}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, requirement=requirement)


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
