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


def handoff(client):
    client.channel()
    client.subscribe()
    hot = [client.publish(f'hot-{index:02}', 'coil-north') for index in range(1, 22)]
    alert_id = hot[-1]['fanout']['oscillator']
    board = client.request(CHANNEL)['groups'][0]
    assert len(board['sample']) == 20
    assert alert_id not in {row['id'] for row in board['sample']}

    first = client.claim()[0]
    head_id = first['delivery_id']
    live = client.request(f'/deliveries/{alert_id}/investigation')
    assert live['blocked_by'] == head_id and live['lane_size'] == 21
    assert live['head']['cause'] == 'leased' and live['head']['actionable'] == 'settle'
    assert live['target']['actionable'] == 'none'

    client.request(GROUP + '/pause', {'paused': True})
    paused_view = client.request(f'/deliveries/{alert_id}/investigation')
    assert paused_view['group']['paused'] is True
    assert paused_view['head']['cause'] == 'leased' and paused_view['head']['actionable'] == 'settle'
    client.settle(first)
    waiting_view = client.request(f'/deliveries/{alert_id}/investigation')
    assert waiting_view['blocked_by'] == head_id + 1
    assert waiting_view['head']['cause'] == 'paused' and waiting_view['head']['actionable'] == 'none'
    client.request(GROUP + '/pause', {'paused': False})

    leased = client.claim()[0]
    client.tick(5)
    expired_view = client.request(f'/deliveries/{alert_id}/investigation')
    assert expired_view['head']['id'] == leased['delivery_id']
    assert expired_view['head']['state'] == 'leased' and expired_view['head']['cause'] == 'expired'
    assert expired_view['head']['actionable'] == 'none'
    assert expired_view['head']['open_attempt']['outcome'] == 'open'

    frozen = client.request(f'/deliveries/{alert_id}/handoff')
    assert frozen['format'] == 'quaybell-handoff/1' and frozen['observed_at'] == expired_view['observed_at']
    assert frozen['live'] == expired_view
    digest = frozen.pop('sha256')
    assert fingerprint(pack(frozen)) == digest
    assert frozen['conclusion']['head_cause'] == 'expired'
    assert frozen['conclusion']['blocked_by'] == leased['delivery_id']
    assert first['lease']['token'] not in pack(frozen) and leased['lease']['token'] not in pack(frozen)

    recovered = client.request('/sweep', {'limit': 1})
    assert recovered['recovered'][0]['delivery_id'] == leased['delivery_id']
    backoff_view = client.request(f'/deliveries/{alert_id}/investigation')
    assert backoff_view['head']['id'] == leased['delivery_id']
    assert backoff_view['head']['state'] == 'waiting' and backoff_view['head']['cause'] == 'delayed'
    assert frozen['conclusion']['head_cause'] == 'expired'

    client.tick(3)
    retry_item = client.claim()[0]
    assert retry_item['delivery_id'] == leased['delivery_id'] and retry_item['lease']['number'] == 2
    client.settle(retry_item)
    ready_view = client.request(f'/deliveries/{alert_id}/investigation')
    assert ready_view['head']['cause'] == 'ready' and ready_view['head']['actionable'] == 'claim'
    while True:
        batch = client.claim(30)
        if not batch:
            break
        for item in batch:
            client.settle(item)
    empty = client.request(f'/deliveries/{alert_id}/investigation')
    assert empty['lane_queue'] == [] and empty['head'] is None
    assert empty['target']['cause'] == 'terminal' and empty['note']
    client.request('/deliveries/99999/investigation', expected=404)
    return {'alert_delivery': alert_id, 'lane_depth': 21,
            'frozen_head_cause': frozen['conclusion']['head_cause'],
            'today_head_cause': backoff_view['head']['cause'],
            'final_target_state': empty['target']['state']}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, handoff=handoff, retention=retention, export=export)


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
