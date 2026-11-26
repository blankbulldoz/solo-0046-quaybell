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


def _group_delivery(trace, group):
    return next(item for item in trace['deliveries'] if item['group_name'] == group)


def coalesce(client):
    client.channel(POLICY | {'lease_seconds': 60})
    client.subscribe('latest', mode='latest')
    client.subscribe('plain')
    lane = 'reading-lane'

    first = client.publish('reading-1', lane, payload={'reading': 1})
    d1 = first['fanout']['latest']
    leased = client.claim(group='latest')[0]
    assert leased['delivery_id'] == d1

    second = client.publish('reading-2', lane, payload={'reading': 2})
    third = client.publish('reading-3', lane, payload={'reading': 3})
    delayed = client.publish('reading-4', lane, payload={'reading': 4}, delay_seconds=5)
    d2, d3, d4 = (second['fanout']['latest'], third['fanout']['latest'],
                  delayed['fanout']['latest'])

    # While the newest reading is delayed it is not yet effective: the two
    # unattempted backlog deliveries remain stacked and visible to operators.
    assert delayed['supersessions'] == []
    for message_id, delivery_id in ((second['message_id'], d2), (third['message_id'], d3)):
        item = _group_delivery(client.trace(message_id), 'latest')
        assert item['id'] == delivery_id and item['state'] == 'waiting'
        assert item['attempt_count'] == 0 and item['superseded_by'] is None

    # Once ready_at arrives, the next claim performs the lazy coalescing pass.
    # d1 is still leased, so d4 replaces d2/d3 but cannot overtake d1.
    client.tick(5)
    assert client.claim(5, group='latest') == []
    old2 = _group_delivery(client.trace(second['message_id']), 'latest')
    old3 = _group_delivery(client.trace(third['message_id']), 'latest')
    new4 = _group_delivery(client.trace(delayed['message_id']), 'latest')
    assert (old2['state'], old2['superseded_by']) == ('superseded', d4)
    assert (old3['state'], old3['superseded_by']) == ('superseded', d4)
    assert old2['superseded_by_message_id'] == delayed['message_id']
    assert new4['supersedes'] == [d2, d3]

    board = client.request(CHANNEL)
    links = {(row['old_delivery_id'], row['new_delivery_id']) for row in board['supersessions']}
    assert {(d2, d4), (d3, d4)} <= links

    # The leased reading is settled normally; only then does d4 become claimable.
    assert client.settle(leased)['state'] == 'acked'
    got = client.claim(group='latest')
    assert [item['delivery_id'] for item in got] == [d4]
    assert got[0]['envelope']['payload'] == {'reading': 4}
    client.settle(got[0])

    # An attempted/nacked delivery is not eligible for replacement. A still
    # unattempted delivery behind it can be replaced by the next fresh reading.
    retry_lane = 'retry-lane'
    retry1 = client.publish('retry-1', retry_lane)
    r1 = retry1['fanout']['latest']
    retry_lease = client.claim(group='latest')[0]
    assert retry_lease['delivery_id'] == r1
    assert client.settle(retry_lease, 'nack')['state'] == 'waiting'
    retry2 = client.publish('retry-2', retry_lane)
    r2 = retry2['fanout']['latest']
    retry3 = client.publish('retry-3', retry_lane)
    r3 = retry3['fanout']['latest']
    assert retry3['supersessions'] == []
    assert _group_delivery(client.trace(retry1['message_id']), 'latest')['state'] == 'waiting'
    client.tick(3)
    retried = client.claim(group='latest')
    assert [item['delivery_id'] for item in retried] == [r1]
    assert retried[0]['lease']['number'] == 2
    replaced_middle = _group_delivery(client.trace(retry2['message_id']), 'latest')
    assert (replaced_middle['state'], replaced_middle['superseded_by']) == ('superseded', r3)
    assert _group_delivery(client.trace(retry1['message_id']), 'latest')['state'] == 'leased'
    client.settle(retried[0])
    assert [item['delivery_id'] for item in client.claim(group='latest')] == [r3]

    # The ordinary group is independent and receives all four original readings
    # in order; same-lane head-of-line blocking still requires settling heads.
    ordinary = []
    for _ in range(4):
        item = client.claim(group='plain')[0]
        ordinary.append(item['message_id'])
        client.settle(item)
    assert ordinary == [
        first['message_id'], second['message_id'], third['message_id'], delayed['message_id']]
    return {'processing': d1, 'superseded': [d2, d3], 'latest': d4,
            'retry_protected': r1, 'ordinary_messages': ordinary}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, coalesce=coalesce,
             latest=coalesce, merge=coalesce, conflation=coalesce)


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
