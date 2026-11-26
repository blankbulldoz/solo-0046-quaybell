"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor

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


def cancel(client):
    client.channel(POLICY | {'max_attempts': 3})
    client.subscribe('rotor')
    client.subscribe('idle')
    first = client.publish()
    tail = client.publish('signal-2')
    rotor, idle = first['fanout']['rotor'], first['fanout']['idle']
    rotor_tail, idle_tail = tail['fanout']['rotor'], tail['fanout']['idle']
    tried = client.claim_for('rotor')[0]
    client.settle(tried, 'nack')
    client.request(f'/deliveries/{rotor}/cancel', {}, 409)
    done = client.cancel(idle)
    assert done == {'delivery_id': idle, 'state': 'cancelled', 'effective_at': done['effective_at'],
                    'reason': '值守确认该组无需处理', 'repeated': False}
    assert client.cancel(idle) == done | {'repeated': True}
    idle_successor = client.claim_for('idle')[0]
    assert idle_successor['delivery_id'] == idle_tail
    assert client.claim_for('rotor') == []
    trace = client.request(f"/messages/{first['message_id']}")
    states = {row['id']: row['state'] for row in trace['deliveries']}
    assert states == {rotor: 'waiting', idle: 'cancelled'}
    event = next(item for item in trace['events'] if item['kind'] == 'cancelled')
    assert event['details'] == done
    evidence = client.request(f"/messages/{first['message_id']}/evidence")
    digest = evidence.pop('sha256')
    assert fingerprint(pack(evidence)) == digest
    assert any(item['kind'] == 'cancelled' for item in evidence['trace']['events'])
    client.tick(3)
    rotor_retry = client.claim_for('rotor')[0]
    assert rotor_retry['delivery_id'] == rotor and rotor_retry['lease']['number'] == 2
    client.settle(rotor_retry)
    advanced = client.claim_for('rotor')[0]
    assert advanced['delivery_id'] == rotor_tail
    return {'cancelled': idle, 'rejected_with_attempt': rotor,
            'successor_after_cancel': idle_tail, 'successor_after_retry': rotor_tail,
            'event': event['details']}


def cancel_race(client):
    client.channel()
    client.subscribe()
    first = client.publish()
    delivery_id = first['fanout']['oscillator']

    def race(mode):
        if mode == 'cancel':
            return mode, client.request(f'/deliveries/{delivery_id}/cancel', {}, expected=None)
        return mode, client.claim()

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = [future.result() for future in [pool.submit(race, 'claim'),
                                                   pool.submit(race, 'cancel')]]
    claimed = next(body for mode, body in outcomes if mode == 'claim')
    cancelled = next(body for mode, body in outcomes if mode == 'cancel')
    winner = 'claim' if claimed and cancelled.get('error') else 'cancel'
    trace = client.request(f"/messages/{first['message_id']}")
    row = next(item for item in trace['deliveries'] if item['id'] == delivery_id)
    if winner == 'claim':
        assert claimed[0]['delivery_id'] == delivery_id and cancelled['error'] == 'conflict'
        assert row['state'] == 'leased' and len(row['attempts']) == 1
    else:
        assert claimed == [] and cancelled['state'] == 'cancelled'
        assert row['state'] == 'cancelled' and row['attempts'] == []
        assert client.claim() == []
    return {'winner': winner, 'delivery_state': row['state'],
            'claim_items': len(claimed), 'cancelled': cancelled.get('state')}


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
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep,
             cancel=cancel, cancel_race=cancel_race, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export)


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
