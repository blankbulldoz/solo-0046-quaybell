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


def batch(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    old = client.publish('prod-old-1', 'coil-north')
    client.request(CHANNEL + '/messages', {'producer': 'other-source', 'key': 'gateway-blip',
        'lane': 'coil-south', 'payload': {'value': 18}, 'tags': {'feed': 'samples'}}, 201)
    entries = [envelope('prod-old-1', 'coil-north'),
               envelope('gateway-new-1', 'coil-east'),
               envelope('gateway-new-2', 'coil-east', payload={'value': 19}),
               envelope('gateway-new-2', 'coil-east', payload={'value': 19})]
    first = client.request(CHANNEL + '/batches', {'batch': 'gateway-0007', 'entries': entries,
                                                  'replay_seconds': 600}, 201)
    assert first['repeated'] is False and first['entries'] == 4
    assert first['positions'][0]['message_id'] == old['message_id']
    assert [position['reused'] for position in first['positions']] == [True, False, False, True]
    assert first['positions'][2]['message_id'] == first['positions'][3]['message_id']
    assert first['positions'][2]['fanout'] == first['positions'][3]['fanout']
    assert all('oscillator' in position['fanout'] for position in first['positions'])
    client.request(CHANNEL + '/batches', {'batch': 'gateway-0007', 'entries': entries,
                                          'replay_seconds': 600}, 200)
    replay = client.request(CHANNEL + '/batches', {'batch': 'gateway-0007', 'entries': entries}, 200)
    assert replay['repeated'] is True and replay['positions'] == first['positions']
    reordered = [entries[1], entries[0], entries[2], entries[3]]
    client.request(CHANNEL + '/batches', {'batch': 'gateway-0007', 'entries': reordered}, 409)
    client.request(CHANNEL + '/batches', {'batch': 'gateway-0007',
        'entries': entries[:2] + [entries[2] | {'payload': {'value': 20}}] + entries[3:]}, 409)
    before = client.request(CHANNEL)['messages']
    same_twice = client.request(CHANNEL + '/batches', {'batch': 'gateway-0008', 'entries': [
        envelope('lane-order-1', 'coil-west'), envelope('lane-order-2', 'coil-west')],
        'replay_seconds': 600}, 201)
    clashing = client.request(CHANNEL + '/batches', {'batch': 'gateway-0009', 'entries': [
        envelope('lane-clash', 'coil-west'),
        envelope('lane-clash', 'coil-west', payload={'value': 21})]}, 409)
    after = client.request(CHANNEL)['messages']
    assert len(after) == len(before) + 2
    assert clashing['error'] == 'conflict'
    claimed = [item for item in client.claim(30) if item['message_id'] in {
        position['message_id'] for position in same_twice['positions']}]
    assert len(claimed) == 1
    client.settle(claimed[0])
    claimed.append(client.claim(30)[0])
    for item in claimed:
        client.settle(item)
    client.tick(11)
    done = client.request(CHANNEL + '/retention', {})
    gone = [position['message_id'] for position in same_twice['positions']]
    assert set(gone) <= set(done['removed'])
    for message_id in gone:
        client.request(f'/messages/{message_id}', expected=404)
    recovered = client.request(CHANNEL + '/batches', {'batch': 'gateway-0008',
        'entries': [envelope('lane-order-1', 'coil-west'), envelope('lane-order-2', 'coil-west')]}, 200)
    assert recovered['repeated'] is True and recovered['pruned_positions'] == [0, 1]
    view = client.request(CHANNEL + '/batches/gateway-0008')
    assert view['state'] == 'replayable' and view['positions'] == same_twice['positions']
    client.request(CHANNEL + '/batches', {'batch': 'gateway-0008', 'entries': [
        envelope('lane-order-2', 'coil-west'), envelope('lane-order-1', 'coil-west')]}, 409)
    client.tick(600)
    closed = client.request(CHANNEL + '/batches', {'batch': 'gateway-0008',
        'entries': [envelope('lane-order-1', 'coil-west'), envelope('lane-order-2', 'coil-west')]}, 410)
    assert closed['error'] == 'replay_window_closed'
    kept = client.request(CHANNEL + '/batches/gateway-0008')
    assert kept['state'] == 'closed' and kept['positions'] is None and kept['entries'] == 2
    client.tick(11)
    retired = client.request(CHANNEL + '/retention', {})
    assert 'gateway-0008' in retired['retired_batches']
    assert 'gateway-0007' in retired['retired_batches']
    client.request(CHANNEL + '/batches/gateway-0008', expected=404)
    return {'first': [position['message_id'] for position in first['positions']],
            'reordered': reordered[0]['key'], 'clash_batch': 'gateway-0009',
            'pruned': done['removed'], 'closed': kept['state'], 'retired': retired['retired_batches']}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, batch=batch)


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
