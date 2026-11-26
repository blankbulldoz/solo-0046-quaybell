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


def replay(client):
    client.channel(POLICY)
    client.subscribe()
    base = client.clock[0]

    early = client.publish('replay-early', 'coil-north')
    client.settle(client.claim()[0], 'nack')
    client.tick(3)
    client.settle(client.claim()[0], 'nack')
    middle = client.publish('replay-middle', 'coil-south')
    client.settle(client.claim()[0], 'nack')
    client.publish('replay-later', 'coil-west', delay_seconds=3600)

    window = {'window_from': int(base + 2), 'window_to': int(base + 6)}
    client.request(f'{CHANNEL}/groups/oscillator/replays',
                   {'window_from': int(base + 6), 'window_to': int(base + 2)}, 400)
    opened = client.open_replay(**window)
    assert opened['window']['basis'] == 'attempt_failed' and opened['window']['basis_text']
    assert opened['selected'] == 2 and opened['matched'] == 2 and opened['truncated'] is False
    assert opened['counts'] == dict(waiting=2, leased=0, acked=0, dead=0)
    items = {item['source']['message_id']: item for item in opened['items']}
    assert set(items) == {early['message_id'], middle['message_id']}
    early_item = items[early['message_id']]['source']
    middle_item = items[middle['message_id']]['source']
    assert early_item['message']['published'] < opened['window']['from']
    assert opened['window']['from'] <= early_item['attempt']['failed_at'] <= opened['window']['to']
    assert early_item['attempt']['number'] == 2 and early_item['attempt']['outcome'] == 'nacked'
    assert early_item['delivery']['id'] == early['fanout']['oscillator']
    assert opened['window']['from'] <= middle_item['message']['published'] <= opened['window']['to']
    assert middle_item['attempt']['number'] == 1

    client.tick(8)
    pruned = client.request(CHANNEL + '/retention', {})
    assert pruned['removed'] == [early['message_id']]
    client.request(f"/messages/{early['message_id']}", expected=404)
    after_prune = client.replay(opened['id'])
    gone = next(item for item in after_prune['items'] if item['source']['message_id'] == early['message_id'])
    assert gone['live']['message_present'] is False
    assert gone['source']['message']['id'] == early['message_id']
    assert gone['source']['attempt']['id'] == early_item['attempt']['id']
    assert gone['source']['attempt']['reason'] == early_item['attempt']['reason']

    claimed = client.replay_claim(opened['id'], limit=10)
    assert {row['source']['message_id'] for row in claimed} == {early['message_id'], middle['message_id']}
    assert all(row['lease']['token'] and row['source']['attempt']['id'] for row in claimed)

    production = client.claim()[0]
    assert production['delivery_id'] == middle['fanout']['oscillator']
    confirm = next(row for row in claimed if row['source']['message_id'] == middle['message_id'])
    settled = client.replay_settle(confirm)
    assert settled['state'] == 'acked' and settled['repeated'] is False
    assert client.replay_settle(confirm) == settled | {'repeated': True}
    both = client.replay(opened['id'])
    assert next(i for i in both['items'] if i['id'] == confirm['item_id'])['state'] == 'acked'
    traced = client.request(f"/messages/{middle['message_id']}")
    production_row = next(d for d in traced['deliveries'] if d['id'] == middle['fanout']['oscillator'])
    assert production_row['state'] == 'leased'
    client.replay_settle(next(row for row in claimed if row['source']['message_id'] == early['message_id']))

    closed = client.close_replay(opened['id'])
    assert closed['counts'] == dict(waiting=0, leased=0, acked=2, dead=0)
    assert closed['production']['delta']['messages'] == -1
    assert closed['production']['delta']['attempts']['nacked'] == -2
    assert closed['production']['delta']['deliveries']['dead'] == -1
    assert client.close_replay(opened['id']) == closed | {'repeated': True}
    client.replay_claim(opened['id'], expected=409)
    final = client.replay(opened['id'])
    assert final['production']['current'] == closed['production']['at_close']

    record = client.request(f"/replays/{opened['id']}/evidence")
    digest = record.pop('sha256')
    assert record['format'] == 'quaybell-replay/1'
    assert fingerprint(pack(record)) == digest
    assert record['replay']['counts'] == closed['counts']
    assert record['production']['at_close'] == closed['production']['at_close']
    return {'replay': opened['id'], 'basis': opened['window']['basis'],
            'selected': opened['selected'], 'sha256': digest,
            'production_delta': closed['production']['delta']}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, replay=replay)


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
