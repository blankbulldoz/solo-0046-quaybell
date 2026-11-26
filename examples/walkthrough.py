"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import logging
import threading

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


def close_dismissed(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    client.publish()
    dead_id = client.fail_once()
    client.request(f'/deliveries/{dead_id}/close',
                   {'kind': 'handed_off', 'reason': '尚无后继，不能按已交后继结案'}, 409)
    receipt = client.close(dead_id, 'dismissed')
    assert receipt['repeated'] is False and receipt['format'] == 'quaybell-closure/1'
    assert receipt['basis']['kind'] == 'dismissed' and receipt['basis']['delivery_id'] == dead_id
    assert receipt['standing']['original'] == {
        'delivery_id': dead_id, 'can_redrive': False, 'can_lease': False, 'can_settle': False,
        'traceable': True,
        'note': '原投递保持死信终态并附结案记录，只能追溯；重投和处置均已关闭'}
    assert receipt['standing']['successor'] is None
    assert fingerprint(pack(dict(format=receipt['format'], basis=receipt['basis'],
                                 standing=receipt['standing']))) == receipt['sha256']
    again = client.close(dead_id, 'dismissed', expected=200)
    assert again == receipt | {'repeated': True}
    client.close(dead_id, 'dismissed', reason='改换依据的重试必须冲突', expected=409)
    client.request(f'/deliveries/{dead_id}/redrive', {}, 409)
    watch = client.request(CHANNEL + '/dead')['items']
    assert all(item['id'] != dead_id for item in watch)
    history = client.request(CHANNEL + '/closures')['items']
    assert history[0]['receipt']['sha256'] == receipt['sha256'] and history[0]['kind'] == 'dismissed'
    facts = client.request(f"/messages/{history[0]['message_id']}")
    assert [item['kind'] for item in facts['closures']] == ['dismissed']
    assert [event['kind'] for event in facts['events']][-1] == 'closed'
    exported = client.request(f"/messages/{history[0]['message_id']}/evidence")
    assert exported['trace']['closures'][0]['receipt']['sha256'] == receipt['sha256']
    return {'closed': dead_id, 'sha256': receipt['sha256'], 'retry_repeated': again['repeated']}


def close_handoff(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    client.publish()
    dead_id = client.fail_once()
    child = client.request(f'/deliveries/{dead_id}/redrive', {}, 201)
    client.close(dead_id, 'dismissed', expected=409)
    receipt = client.close(dead_id, 'handed_off')
    assert receipt['basis']['successor_id'] == child['delivery_id']
    standing = receipt['standing']
    assert standing['original']['can_redrive'] is False and standing['original']['traceable'] is True
    assert standing['successor'] == {
        'delivery_id': child['delivery_id'], 'state': 'waiting', 'can_lease': True,
        'can_settle': False, 'traceable': True,
        'note': '后继是独立投递，沿用本组顺序键并绑定重投时策略，按自身状态继续生命周期'}
    picked = client.claim()[0]
    client.settle(picked)
    assert picked['delivery_id'] == child['delivery_id']
    history = client.request(CHANNEL + '/closures')['items']
    assert history[0]['successor_state'] == 'acked'
    return {'source': dead_id, 'successor': child['delivery_id'], 'kind': receipt['basis']['kind']}


def close_race(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    client.publish()
    dead_id = client.fail_once()
    barrier = threading.Barrier(2)
    outcomes = {}

    def closer():
        barrier.wait()
        try:
            outcomes['close'] = client.close(dead_id, 'dismissed', reason='值班员甲判定无需重投')
        except RuntimeError as exc:
            outcomes['close_error'] = exc.args[0]

    def redriver():
        barrier.wait()
        try:
            outcomes['redrive'] = client.request(f'/deliveries/{dead_id}/redrive', {}, 201)
        except RuntimeError as exc:
            outcomes['redrive_error'] = exc.args[0]

    threads = [threading.Thread(target=closer), threading.Thread(target=redriver)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    watch = {item['id']: item for item in client.request(CHANNEL + '/dead')['items']}
    if 'close' in outcomes:
        assert 'redrive_error' in outcomes and outcomes['redrive_error'][1] == 409
        assert outcomes['close']['repeated'] is False
        assert dead_id not in watch
        assert client.close(dead_id, 'dismissed', reason='值班员甲判定无需重投',
                            expected=200)['repeated'] is True
    else:
        assert 'redrive' in outcomes and outcomes['close_error'][1] == 409
        child = outcomes['redrive']['delivery_id']
        assert dead_id in watch and watch[dead_id]['redriven_as'] == child
        receipt = client.close(dead_id, 'handed_off', reason='值班员乙确认后继已接手')
        assert receipt['basis']['successor_id'] == child
        client.close(dead_id, 'dismissed', expected=409)
        assert dead_id not in {item['id'] for item in client.request(CHANNEL + '/dead')['items']}
    assert len(client.request(CHANNEL + '/closures')['items']) == 1
    return {'winner': 'close' if 'close' in outcomes else 'redrive', 'outcomes': sorted(outcomes)}


def close_retention(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    held = client.publish('held', 'coil-south')
    first = client.publish()
    held_dead = client.fail_once()
    held_child = client.request(f'/deliveries/{held_dead}/redrive', {}, 201)
    client.close(held_dead, 'handed_off', reason='后继仍在手里，保留期到点也不能清理')
    client.publish('first-again')
    first_dead = client.fail_once()
    client.close(first_dead, 'dismissed', reason='无需重投，可随保留期退出')
    client.tick(11)
    plan = client.request(CHANNEL + '/retention')
    assert [row['id'] for row in plan['eligible']] == [first['message_id']]
    done = client.request(CHANNEL + '/retention', {})
    assert done['removed'] == [first['message_id']]
    assert done['closed_delivery_ids'] == [first_dead]
    client.request(f"/messages/{first['message_id']}", expected=404)
    held_facts = client.request(f"/messages/{held['message_id']}")
    assert held_facts['closures'][0]['receipt']['basis']['kind'] == 'handed_off'
    successor = client.claim()[0]
    assert successor['delivery_id'] == held_child['delivery_id']
    client.settle(successor)
    client.request(CHANNEL + '/retention', {})
    client.request(f"/messages/{held['message_id']}", expected=404)
    assert client.request(CHANNEL + '/closures')['items'] == []
    return {'first_prune': done['closed_delivery_ids'], 'held_message': held['message_id']}


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
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, close_dismissed=close_dismissed, close_handoff=close_handoff,
             close_race=close_race, close_retention=close_retention,
             board=board, retention=retention, export=export)


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
