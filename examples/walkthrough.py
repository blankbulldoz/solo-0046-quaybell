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


def release(client):
    client.channel()
    client.subscribe()

    # 场景一：只剩一次失败预算的分发被反复让回，让回不消耗失败预算。
    budget_msg = client.publish('budget-key', 'coil-north')
    first = client.claim()[0]
    failed = client.settle(first, 'nack')
    assert failed['state'] == 'waiting'
    client.tick(3)
    held = client.claim()[0]
    assert held['lease']['number'] == 2  # 再失败一次就会进死信

    sent_at = client.clock[0]
    ticket = client.release(held)
    assert ticket['outcome'] == 'released' and ticket['state'] == 'waiting'
    assert ticket['effective_at'] == ticket['ready_at'] == sent_at + 3
    assert ticket['retry_after'] == 3
    assert (ticket['failures'], ticket['max_attempts'], ticket['remaining_failures']) == (1, 2, 1)
    # 相同让回请求重试读取同一记录
    assert client.release(held) == ticket | {'repeated': True}
    # 冷却期内不能立刻再领，避免连续空转
    assert client.claim() == []

    # 反复“让回 -> 冷却 -> 再领”，剩余失败预算始终是一次
    generations = [held['lease']['number']]
    current = held
    for worker in ('standby-worker', 'bench-worker'):
        client.tick(3)
        current = client.claim(worker=worker)[0]
        generations.append(current['lease']['number'])
        now = client.clock[0]
        again = client.release(current)
        assert again['effective_at'] == now + 3
        assert (again['failures'], again['remaining_failures']) == (1, 1)
        assert client.claim() == []
    assert generations == [2, 3, 4]

    # 预算是真实的：保留的最后一次失败仍会把投递送入死信
    client.tick(3)
    last = client.claim()[0]
    assert last['lease']['number'] == 5
    assert client.settle(last, 'nack')['state'] == 'dead'

    trace = client.request(f"/messages/{budget_msg['message_id']}")
    delivery = next(row for row in trace['deliveries'] if row['id'] == held['delivery_id'])
    outcomes_seq = [attempt['outcome'] for attempt in delivery['attempts']]
    assert outcomes_seq == ['nacked', 'released', 'released', 'released', 'nacked'], outcomes_seq
    released_rows = [attempt for attempt in delivery['attempts'] if attempt['outcome'] == 'released']
    assert {attempt['worker'] for attempt in released_rows} == {'bench-worker', 'standby-worker'}
    assert all(attempt['finished'] is not None for attempt in released_rows)
    assert [event['kind'] for event in trace['events']].count('released') == 3
    assert held['lease']['token'] not in pack(trace)

    # 场景二：让回的同键队首在冷却期仍挡住后继，重新领取并确认后后继才推进。
    head_msg = client.publish('head-key', 'coil-east')
    tail_msg = client.publish('tail-key', 'coil-east')
    picked = client.claim(10)
    assert [item['message_id'] for item in picked] == [head_msg['message_id']]
    head = picked[0]
    client.release(head)
    assert client.claim(10) == []  # 队首冷却中，同键后继不得超越
    client.tick(3)
    reclaimed = client.claim(10)[0]
    assert reclaimed['delivery_id'] == head['delivery_id']
    client.settle(reclaimed)
    successor = client.claim(10)[0]
    assert successor['message_id'] == tail_msg['message_id']
    client.settle(successor)

    # 场景三：过期后才到达的让回请求被拒绝，处置仍走原截止时间的超时规则。
    late_msg = client.publish('late-key', 'coil-west')
    leased = client.claim()[0]
    expires = leased['lease']['expires']
    client.tick(5)  # 租约 4 秒，此刻已过期
    denied = client.release(leased, expected=409)
    assert denied['error'] == 'conflict'
    client.tick(2)  # 到达 expires + retry_seconds
    swept = client.request('/sweep', {'limit': 10})
    record = next(row for row in swept['recovered'] if row['delivery_id'] == leased['delivery_id'])
    assert record['outcome'] == 'expired' and record['effective_at'] == expires + 3
    replacement = client.claim()[0]
    assert replacement['delivery_id'] == leased['delivery_id']
    assert replacement['lease']['number'] == 2
    client.settle(replacement)
    late_trace = client.request(f"/messages/{late_msg['message_id']}")
    late_delivery = next(row for row in late_trace['deliveries'] if row['id'] == leased['delivery_id'])
    assert [attempt['outcome'] for attempt in late_delivery['attempts']] == ['expired', 'acked']
    return {'released_attempts': len(released_rows), 'reclaim_generations': generations,
            'reclaim_delay_seconds': 3, 'late_release_status': 409}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep,
             dead=dead, redrive=redrive, board=board, retention=retention, export=export,
             release=release)


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
