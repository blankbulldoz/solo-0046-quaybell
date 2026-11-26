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


def correct(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe(tags={'feed': 'samples'})
    client.subscribe('witness')
    sent = client.publish()
    client.settle(client.claim()[0], 'nack')
    source = client.request(CHANNEL + '/dead')['items'][0]
    fixed = client.correct(source['id'], 'night-owl', {'payload': {'value': 81, 'unit': 'uV'}})
    assert fixed['message_id'] != sent['message_id']
    assert fixed['source']['delivery_id'] == source['id']
    assert fixed['source']['last_attempt']['outcome'] == 'nacked'
    assert client.correct(source['id'], 'night-owl', {'payload': {'value': 81, 'unit': 'uV'}},
                          expected=200) == fixed | {'repeated': True}
    client.correct(source['id'], 'night-owl', {'payload': {'value': 82}}, expected=409)
    client.correct(source['id'], 'heron-lead', dict(envelope()), expected=400)
    client.correct(source['id'], 'heron-lead', {'payload': {'value': 81}, 'tags': {'feed': 'alerts'}},
                   expected=400)
    client.correct(source['id'], 'heron-lead', {'payload': {'value': 81}, 'key': 'other-key'},
                   expected=400)
    client.correct(sent['fanout']['witness'], 'night-owl', {'payload': {'value': 81}}, expected=409)
    taken = client.claim()[0]
    assert taken['delivery_id'] == fixed['delivery_id'] and taken['message_id'] == fixed['message_id']
    assert taken['envelope']['payload'] == {'value': 81, 'unit': 'uV'}
    assert taken['policy'] == 1
    trace = client.request(f"/messages/{fixed['message_id']}")
    assert trace['correction']['operator'] == 'night-owl'
    assert trace['correction']['reason'] == '修正解码后的载荷单位'
    assert trace['correction']['source']['delivery_id'] == source['id']
    assert trace['correction']['source']['last_attempt']['outcome'] == 'nacked'
    record = client.request(f"/messages/{sent['message_id']}")
    assert [event['kind'] for event in record['events']] == ['published', 'leased', 'nacked', 'corrected']
    assert record['corrections'][0]['message_id'] == fixed['message_id']
    assert [row['id'] for row in record['deliveries']] == [source['id'], sent['fanout']['witness']]
    assert record['deliveries'][0]['state'] == 'dead'
    return {'source': source['id'], 'message': fixed['message_id'], 'delivery': fixed['delivery_id']}


def correction_race(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    client.publish()
    client.settle(client.claim()[0], 'nack')
    source = client.request(CHANNEL + '/dead')['items'][0]
    night = client.correct(source['id'], 'night-owl', {'payload': {'value': 81}})
    day = client.correct(source['id'], 'heron-lead', {'payload': {'value': 82}},
                         reason='白班复核后改按整数读数解码')
    assert len({source['id'], night['message_id'], day['message_id']}) == 3
    assert night['message_id'] != day['message_id']
    assert night['delivery_id'] < day['delivery_id']
    order = [item['id'] for item in client.request(CHANNEL)['groups'][0]['sample']
             if item['lane'] == 'coil-north']
    assert order == sorted(order) == [night['delivery_id'], day['delivery_id']]
    head = client.claim(4)
    assert [item['delivery_id'] for item in head] == [night['delivery_id']]
    client.settle(head[0])
    tail = client.claim(4)
    assert [item['delivery_id'] for item in tail] == [day['delivery_id']]
    assert [item['envelope']['payload']['value'] for item in head + tail] == [81, 82]
    assert client.request(f"/messages/{source['message_id']}")['corrections'] == [
        {'correction_id': night['correction_id'], 'source_delivery_id': source['id'],
         'message_id': night['message_id'], 'delivery_id': night['delivery_id'],
         'operator': 'night-owl', 'reason': '修正解码后的载荷单位', 'created': night['created']},
        {'correction_id': day['correction_id'], 'source_delivery_id': source['id'],
         'message_id': day['message_id'], 'delivery_id': day['delivery_id'],
         'operator': 'heron-lead', 'reason': '白班复核后改按整数读数解码', 'created': day['created']},
    ]
    return {'source': source['id'], 'night': night['message_id'], 'day': day['message_id']}


def correction_lost(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    client.publish()
    client.settle(client.claim()[0], 'nack')
    source = client.request(CHANNEL + '/dead')['items'][0]
    request = {'payload': {'value': 81}}
    first = client.correct(source['id'], 'night-owl', request)
    before = client.request(f"/messages/{source['message_id']}")
    again = client.correct(source['id'], 'night-owl', request, expected=200)
    assert again == first | {'repeated': True}
    assert again['correction_id'] == first['correction_id']
    after = client.request(f"/messages/{source['message_id']}")
    assert after == before and len(after['corrections']) == 1
    assert client.claim()[0]['delivery_id'] == first['delivery_id']
    return {'correction': first['correction_id'], 'message': first['message_id'], 'replayed': True}


def correct_order(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    client.subscribe('witness')
    first = client.publish()
    tail = client.publish('tail')
    client.settle(client.claim()[0], 'nack')
    source = next(row for row in client.request(CHANNEL + '/dead')['items']
                  if row['message_id'] == first['message_id'])
    fixed = client.correct(source['id'], 'night-owl', {'payload': {'value': 7}})
    lane = [item['id'] for item in client.request(CHANNEL)['groups'][0]['sample']
            if item['lane'] == 'coil-north']
    assert lane == [tail['fanout']['oscillator'], fixed['delivery_id']]
    assert fixed['appended_after'] == tail['fanout']['oscillator']
    held = client.claim()[0]
    assert held['message_id'] == tail['message_id']
    assert client.claim() == []
    client.settle(held)
    assert client.claim()[0]['message_id'] == fixed['message_id']
    original = client.request(f"/messages/{first['message_id']}")
    assert [row['id'] for row in original['deliveries']] == [source['id'], first['fanout']['witness']]
    assert original['deliveries'][1]['state'] == 'waiting'
    corrected = client.request(f"/messages/{fixed['message_id']}")
    assert corrected['message']['producer'] == original['message']['producer']
    assert corrected['message']['dedup_key'] == original['message']['dedup_key']
    assert corrected['message']['origin_delivery_id'] == source['id']
    assert corrected['deliveries'][0]['lane'] == source['lane']
    assert corrected['correction']['source']['message_id'] == first['message_id']
    assert corrected['correction']['source']['last_attempt']['reason'] == '下游拒绝校验帧'
    assert client.request('/channels')['channels'][0]['name'] == 'wire-gate'
    client.publish(expected=409, payload={'value': 999})
    assert client.publish(expected=200)['message_id'] == first['message_id']
    dead = client.request(CHANNEL + '/dead')['items']
    assert dead[0]['id'] == source['id'] and dead[0]['corrections'][0]['message_id'] == fixed['message_id']
    return {'tail': tail['message_id'], 'corrected_after': fixed['delivery_id'], 'lane': lane}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, correct=correct,
             correction_race=correction_race, correction_lost=correction_lost,
             correct_order=correct_order)


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
