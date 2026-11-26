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
    report = client.impact(data)
    client.request(CHANNEL + '/policy', data, 400)
    record = client.request(CHANNEL + '/policy', data | {'impact_token': report['baseline']['token']}, 201)
    client.request(CHANNEL + '/policy', data, 400)
    client.request(CHANNEL + '/policy', data | {'impact_token': report['baseline']['token']}, 409)
    new = client.publish('new-signal', 'coil-south')
    first, second = client.claim(2)
    assert (old['policy'], new['policy']) == (1, 2)
    assert second['lease']['expires'] - first['lease']['expires'] == 8
    return {'active': record['version'], 'old_lease': first['lease']['expires'], 'new_lease': second['lease']['expires']}


def impact(client):
    rule = POLICY | {'lease_seconds': 60, 'max_attempts': 2}
    client.channel(rule)
    client.subscribe()
    client.publish('held')
    client.publish('dies', 'coil-east')
    client.publish('redriven-source', 'coil-west')
    held, gone, source = client.claim(3)
    client.settle(gone, 'nack')
    client.settle(source, 'nack')
    client.tick(3)
    dead_a, dead_b = client.claim(2)
    client.settle(dead_a, 'nack')
    client.settle(dead_b, 'nack')
    client.request(f"/deliveries/{dead_b['delivery_id']}/redrive", {}, 201)
    client.publish('retried', 'coil-south')
    revived, retried = client.claim(2)
    client.settle(revived)
    client.settle(retried, 'nack')
    body = rule | {'expected': 1, 'lease_seconds': 9, 'retry_seconds': 8, 'max_attempts': 4}
    report = client.impact(body)
    effects = {item['key']: item for item in report['effects']}
    assert effects['in_hand_leases']['count'] == 1 and effects['in_hand_leases']['certainty'] == 'settled'
    assert effects['in_hand_leases']['items'][0]['delivery_id'] == held['delivery_id']
    assert effects['waiting_retries']['count'] == 1 and effects['waiting_retries']['certainty'] == 'settled'
    assert effects['waiting_retries']['items'][0]['next_lease_seconds'] == rule['lease_seconds']
    assert effects['waiting_retries']['items'][0]['next_backoff_seconds'] == rule['retry_seconds']
    assert effects['dead_redrive']['count'] == 1 and effects['dead_redrive']['certainty'] == 'pending'
    assert effects['dead_redrive']['already_redriven'] == 1
    assert effects['next_publish']['certainty'] == 'pending'
    assert report['settled_count'] == 2 and report['baseline']['version'] == 1
    assert client.request(CHANNEL + '/policy', body, 400)
    client.request(CHANNEL + '/policy', body | {'impact_token': report['baseline']['token']}, 201)
    assert client.request(CHANNEL + '/policy', body | {'impact_token': report['baseline']['token']}, 409)
    assert client.request(CHANNEL)['policy']['version'] == 2
    assert held['policy'] == retried['policy'] == 1
    return {'settled': report['settled_count'],
            'effects': {key: item['certainty'] for key, item in effects.items()}}


def stale(client):
    client.channel()
    client.subscribe()
    client.publish('held')
    held = client.claim()[0]
    body = POLICY | {'expected': 1, 'lease_seconds': 12}
    report = client.impact(body)
    token = report['baseline']['token']
    client.publish('after-preview', 'coil-south')
    refused = client.request(CHANNEL + '/policy', body | {'impact_token': token}, 409)
    assert refused['error'] == 'stale_preview' and refused['detail']['baseline']['token'] != token
    assert refused['detail']['effects'][0]['count'] == 1
    assert client.request(CHANNEL)['policy']['version'] == 1
    assert client.settle(held)['state'] == 'acked'
    latest = client.impact(body)
    assert latest['baseline']['token'] != token
    client.request(CHANNEL + '/policy', body | {'impact_token': latest['baseline']['token']}, 201)
    assert client.request(CHANNEL)['policy']['version'] == 2
    return {'refused': refused['error'], 'settled': latest['settled_count']}


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
    client.revise(POLICY | {'expected': 1})
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


FLOWS = dict(channel=channel, revision=revision, impact=impact, stale=stale, group=group,
             preview=preview, publish=publish, inspect=inspect, pause=pause, claim=claim, ack=ack,
             retry=retry, sweep=sweep, dead=dead, redrive=redrive, board=board, retention=retention,
             export=export)


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
