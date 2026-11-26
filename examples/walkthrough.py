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


def _kill_lane_pair(client):
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    first = client.publish('lane-first')
    second = client.publish('lane-second')
    client.settle(client.claim()[0], 'nack')
    client.settle(client.claim()[0], 'nack')
    rows = client.request(CHANNEL + '/dead')['items']
    ids = [row['id'] for row in rows]
    assert rows[0]['lane'] == rows[1]['lane'] == 'coil-north'
    assert {first['fanout']['oscillator'], second['fanout']['oscillator']} <= set(ids)
    return rows


def batch_redrive(client):
    rows = _kill_lane_pair(client)
    ids = [row['id'] for row in rows[:2]]
    client.request(CHANNEL + '/redrive-preview', {}, 400)
    client.request(CHANNEL + '/redrive-preview', {'delivery_ids': [ids[0], ids[0]]}, 400)
    client.request(CHANNEL + '/redrive-preview', {'delivery_ids': [99999]}, 404)
    # 预览：两条同顺序键死信都仍可重投，并汇总将影响的消费组
    preview = client.request(CHANNEL + '/redrive-preview', {'delivery_ids': ids})
    assert preview['policy_version'] == 1
    assert preview['counts'] == {'eligible': 2, 'already_redriven': 0}
    assert preview['affected_groups'] == [{'group': 'oscillator', 'total': 2, 'eligible': 2}]
    # 预览之后，值班同事先把其中一条重投掉
    stolen = client.request(f'/deliveries/{ids[0]}/redrive', {}, 201)
    changed = client.request(CHANNEL + '/redrive-preview', {'delivery_ids': ids})
    assert changed['counts'] == {'eligible': 1, 'already_redriven': 1}
    # 确认之前策略修订到 v2：持有旧版号确认时整体不写入，分类逐项说明留下原因
    client.request(CHANNEL + '/policy', POLICY | {'expected': 1, 'lease_seconds': 12}, 201)
    stale = client.request(CHANNEL + '/redrive-confirm', {'delivery_ids': ids, 'expected': 1})
    by_id = {item['delivery_id']: item for item in stale['items']}
    assert stale['counts'] == {'redriven': 0, 'already_redriven': 1, 'stale_policy': 1}
    assert by_id[ids[0]]['state'] == 'already_redriven'
    assert by_id[ids[0]]['successor_id'] == stolen['delivery_id']
    assert by_id[ids[1]]['state'] == 'stale_policy' and by_id[ids[1]]['successor_id'] is None
    # 重新预览后按当前 v2 确认：实际只提交仍可重投的那一条
    reviewed = client.request(CHANNEL + '/redrive-preview', {'delivery_ids': ids})
    assert reviewed['counts'] == {'eligible': 1, 'already_redriven': 1}
    submitted = client.request(CHANNEL + '/redrive-confirm', {'delivery_ids': ids, 'expected': 2})
    final = {item['delivery_id']: item for item in submitted['items']}
    assert submitted['counts'] == {'redriven': 1, 'already_redriven': 1, 'stale_policy': 0}
    assert final[ids[0]]['state'] == 'already_redriven'
    assert final[ids[0]]['successor_id'] == stolen['delivery_id']
    assert final[ids[1]]['state'] == 'redriven' and final[ids[1]]['policy'] == 2
    new_successor = final[ids[1]]['successor_id']
    # 已成功重投的源不能再造新后继：重复确认仍定位同一个后继
    repeated = client.request(CHANNEL + '/redrive-confirm', {'delivery_ids': ids, 'expected': 2})
    assert repeated['counts'] == {'redriven': 0, 'already_redriven': 2, 'stale_policy': 0}
    again = {item['delivery_id']: item for item in repeated['items']}
    assert again[ids[0]]['successor_id'] == stolen['delivery_id']
    assert again[ids[1]]['successor_id'] == new_successor
    # 同顺序键队尾：先产生的后继是队首并保留 v1，确认产生的后继排在其后并绑定 v2
    head = client.claim()[0]
    assert head['delivery_id'] == stolen['delivery_id'] and head['policy'] == 1
    client.settle(head)
    tail = client.claim()[0]
    assert tail['delivery_id'] == new_successor and tail['policy'] == 2
    return {'preview': preview['counts'], 'stale_confirm': stale['counts'],
            'submitted': submitted['counts'], 'repeated': repeated['counts']}


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
             redrive=redrive, batch_redrive=batch_redrive, board=board, retention=retention, export=export)


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
