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


def fairness(client):
    policy = dict(lease_seconds=60, retry_seconds=20, max_attempts=3, retention_seconds=600)
    client.channel(policy)
    client.subscribe()
    # 两个热键各两条立即可领，另各一条延迟到冷键重试同一时刻；冷键一条立即就绪。
    for lane, key, delay in (('coil-north', 'hot-a1', 0), ('coil-north', 'hot-a2', 0),
                             ('coil-north', 'hot-a3', 20), ('coil-south', 'hot-b1', 0),
                             ('coil-south', 'hot-b2', 0), ('coil-south', 'hot-b3', 20),
                             ('coil-west', 'cold-1', 0)):
        client.publish(key, lane, delay_seconds=delay)
    sequence = []

    def take(expect=None):
        items = client.claim(1)
        assert len(items) <= 1
        if not items:
            return None
        row = {'lane': items[0]['lane'], 'delivery_id': items[0]['delivery_id'],
               'attempt': items[0]['lease']['number']}
        sequence.append(row)
        if expect is not None:
            assert items[0]['lane'] == expect, (items[0]['lane'], expect)
        return items[0]

    # 三个键同时可领且都未服务过：按投递 ID 各得一次机会，冷键不会垫底。
    first = take('coil-north')
    second = take('coil-south')
    cold = take('coil-west')
    assert [first['delivery_id'], second['delivery_id'], cold['delivery_id']] == [1, 4, 7]
    client.settle(first)
    client.settle(second)
    retry_at = client.settle(cold, 'nack')['effective_at']

    # 冷键退避后，两个热键各再取一条队首，仍然交替且不越队。
    third = take('coil-north')
    fourth = take('coil-south')
    assert [third['delivery_id'], fourth['delivery_id']] == [2, 5]
    client.settle(third)
    client.settle(fourth)

    # 热键队首已结清但延迟消息未到，冷键在退避中：空领取，不提前发租约。
    assert take() is None
    client.tick(int(retry_at - client.clock[0]) - 1)
    assert take() is None

    # 冷键重试到点，与两个热键的延迟消息同时可领。冷键最久未服务，先取；
    # 同键队首递进后，两个热键再按上次服务先后各取一条。
    client.tick(1)
    late = take('coil-west')
    fifth = take('coil-north')
    sixth = take('coil-south')
    assert late['lease']['number'] == 2
    assert [late['delivery_id'], fifth['delivery_id'], sixth['delivery_id']] == [7, 3, 6]
    for item in (late, fifth, sixth):
        client.settle(item)
    assert take() is None

    # 停组再恢复：暂停期间领取为空，恢复后新键照常可领。
    client.publish('hold-1', 'coil-east')
    client.request(GROUP + '/pause', {'paused': True})
    paused = client.request(GROUP + '/claim', {'worker': 'bench-worker', 'limit': 1})
    assert paused == {'paused': True, 'items': []}
    client.request(GROUP + '/pause', {'paused': False})
    resumed = take('coil-east')
    client.settle(resumed)

    return {'sequence': sequence,
            'lanes': [item['lane'] for item in sequence],
            'empty_claims': {'idle_before_retry': True, 'one_second_early': True},
            'retry_ready_at': retry_at,
            'paused_claim': paused,
            'first_service': [row['delivery_id'] for row in sequence[:3]],
            'late_retry_delivery': late['delivery_id']}


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


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, fairness=fairness, ack=ack, retry=retry,
             sweep=sweep, dead=dead, redrive=redrive, board=board, retention=retention, export=export)


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
