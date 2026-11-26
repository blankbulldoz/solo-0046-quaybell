"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import logging
from threading import Barrier, Thread

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


def _kill(client, channel, group, reason):
    item = client.claim_in(channel, group)[0]
    client.request(f"/deliveries/{item['delivery_id']}/nack", {'token': item['lease']['token'],
                                                                'reason': reason})
    return item['delivery_id']


def transfer(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe('oscillator')
    client.subscribe('witness')
    first_msg = client.publish()
    first_fanout = first_msg['fanout']
    first_dead_id = _kill(client, 'wire-gate', 'oscillator', '下游拒绝校验帧')
    witness_item = client.claim_in('wire-gate', 'witness')[0]
    client.settle(witness_item)
    dead_rows = client.request(CHANNEL + '/dead')['items']
    assert [row['id'] for row in dead_rows] == [first_dead_id]
    source_dead = dead_rows[0]
    reason = source_dead['last_attempt']['reason']
    repair_channel = 'repair-bay'
    client.relay(source_dead['id'], 'missing-bay', expected=404)
    client.make_channel(repair_channel)
    client.request(f"/deliveries/{first_fanout['witness']}/relay", {'channel': repair_channel}, 409)
    first = client.relay(source_dead['id'], repair_channel, expected=201)
    target_id = first['target_message_id']
    assert first['fanout'] == {} and first['reason'] == reason
    client.relay(source_dead['id'], 'other-bay', expected=404)
    client.make_channel('other-bay')
    client.relay(source_dead['id'], 'other-bay', expected=409)
    target = client.request(f'/messages/{target_id}')
    origin = target['origin']
    assert origin['source_id'] == first_dead_id
    assert origin['source_channel'] == 'wire-gate' and origin['reason'] == reason
    assert origin['provenance']['source_attempt'] == source_dead['last_attempt']['number']
    origin_tags = target['message']['envelope']['tags']
    assert origin_tags['x-quaybell-origin-delivery'] == str(first_dead_id)
    source_trace = client.request(f"/messages/{first_msg['message_id']}")
    relayed = [event for event in source_trace['events'] if event['kind'] == 'relayed']
    assert relayed and relayed[0]['details']['target_message_id'] == target_id
    replay = client.relay(source_dead['id'], repair_channel)
    assert replay == first | {'repeated': True}
    barrier = Barrier(2)
    replies, statuses = [], []

    def operator():
        barrier.wait()
        status, body = client.post_raw(f"/deliveries/{source_dead['id']}/relay",
                                       {'channel': repair_channel})
        statuses.append(status)
        replies.append(body)

    threads = [Thread(target=operator) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert set(statuses) == {200}
    assert all(item['repeated'] and item['target_message_id'] == target_id for item in replies)
    second_msg = client.publish('signal-2')
    second_fanout = second_msg['fanout']
    second_dead_id = _kill(client, 'wire-gate', 'oscillator', '帧序号回退')
    other_dead_id = _kill(client, 'wire-gate', 'witness', '校验和不符')
    assert {row['id'] for row in client.request(CHANNEL + '/dead')['items']} == {
        first_dead_id, second_dead_id, other_dead_id}
    client.make_group(repair_channel, 'healer-a')
    client.make_group(repair_channel, 'healer-b')
    selected = client.relay(second_dead_id, repair_channel, expected=201)
    second_target = selected['target_message_id']
    assert set(selected['fanout']) == {'healer-a', 'healer-b'}
    client.request(f"/deliveries/{other_dead_id}/relay", {'channel': repair_channel}, 409)
    claimed = client.claim_in(repair_channel, 'healer-a') + client.claim_in(repair_channel, 'healer-b')
    assert {item['delivery_id'] for item in claimed} == set(selected['fanout'].values())
    assert all(item['message_id'] == second_target for item in claimed)
    for item in claimed:
        client.settle(item)
    board = client.request(f'/channels/{repair_channel}')
    assert sorted(item['id'] for item in board['messages']) == sorted((target_id, second_target))
    source_evidence = client.request(f"/messages/{first_msg['message_id']}/evidence")
    assert source_evidence['trace']['relays'][0]['target_message_id'] == target_id
    target_evidence = client.request(f'/messages/{target_id}/evidence')
    assert target_evidence['trace']['origin']['source_id'] == first_dead_id
    second_evidence = client.request(f'/messages/{second_target}/evidence')
    assert second_evidence['trace']['origin']['reason'] == '帧序号回退'
    client.tick(11)
    plan = client.request(CHANNEL + '/retention')
    assert [row['id'] for row in plan['eligible']] == [first_msg['message_id'], second_msg['message_id']]
    client.request(CHANNEL + '/retention', {})
    client.request(f"/messages/{first_msg['message_id']}", expected=404)
    client.request(f"/deliveries/{first_dead_id}/relay", {'channel': repair_channel}, 404)
    surviving = client.request(f'/messages/{target_id}')
    assert surviving['origin']['source_id'] == first_dead_id
    assert surviving['origin']['provenance']['source_message_id'] == first_msg['message_id']
    return {'target_message_id': target_id, 'second_target_message_id': second_target,
            'zero_subscriber_fanout': first['fanout'], 'repair_fanout': selected['fanout'],
            'duplicate_statuses': sorted(set(statuses)),
            'source_sha256': source_evidence['sha256'],
            'target_sha256': target_evidence['sha256']}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, transfer=transfer)


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
