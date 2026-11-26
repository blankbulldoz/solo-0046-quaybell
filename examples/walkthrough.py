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
    """分片上传 + 封口：重启续传、夹发普通消息的次序、取消释放与回执取回。"""
    client.channel()
    client.subscribe()
    lane = 'coil-batch'

    opened = client.create_batch('device-batch-9', lane)
    bid = opened['batch_id']
    assert opened['state'] == 'open' and opened['part_count'] == 0

    first = client.put_shard(bid, 0, {'device': 'sensor-0', 'reading': 41})
    assert first['repeated'] is False
    # 同一编号同样内容重发：幂等复用。
    resent = client.put_shard(bid, 0, {'device': 'sensor-0', 'reading': 41}, expected=200)
    assert resent['repeated'] is True and resent['id'] == first['id']

    # 两个分片之间插入一条同顺序键的普通发布。
    middle = client.publish('middle-pulse', lane, payload={'middle': True})
    middle_delivery = middle['fanout']['oscillator']

    client.put_shard(bid, 1, {'device': 'sensor-1', 'reading': 42})

    # 模拟进程重启：同一个 SQLite 文件，暂存分片与批次状态都还在。
    client.restart()
    status = client.request(f'{CHANNEL}/batches/{bid}')
    assert status['state'] == 'open' and status['part_count'] == 2
    again = client.put_shard(bid, 0, {'device': 'sensor-0', 'reading': 41}, expected=200)
    assert again['repeated'] is True
    client.put_shard(bid, 2, {'device': 'sensor-2', 'reading': 43})

    sealed = client.seal_batch(bid)
    assert sealed['repeated'] is False and sealed['part_count'] == 3
    batch_delivery = sealed['fanout']['oscillator']
    # 封口发生在夹发普通消息之后：投递号给出明确可复验的先后。
    assert middle_delivery < batch_delivery

    # 封口回执可因网络重发被取回：重封与 GET 回执是同一条记录。
    resealed = client.seal_batch(bid, expected=200)
    assert resealed['repeated'] is True and resealed['message_id'] == sealed['message_id']
    receipt = client.batch_receipt(bid)
    assert receipt['message_id'] == sealed['message_id'] and receipt['fanout'] == sealed['fanout']

    # 普通消费者按顺序键先领到夹发的普通消息，确认后才领到整批。
    first_item = client.claim()[0]
    assert first_item['message_id'] == middle['message_id']
    assert first_item['envelope']['payload'] == {'middle': True}
    client.settle(first_item)
    batch_item = client.claim()[0]
    assert batch_item['message_id'] == sealed['message_id']
    parts = batch_item['envelope']['payload']['parts']
    assert [row['reading'] for row in parts] == [41, 42, 43]
    client.settle(batch_item)

    # 取消一个已传分片但未封口的批次：同键普通发布必须立即可领，不被永久挡住。
    doomed = client.create_batch('device-batch-cancel', 'coil-cancel')
    client.put_shard(doomed, 0, {'device': 'x'})
    cancelled = client.cancel_batch(doomed)
    assert cancelled['cancelled'] is True and cancelled['repeated'] is False
    again_cancel = client.cancel_batch(doomed)
    assert again_cancel['repeated'] is True
    after = client.publish('after-cancel', 'coil-cancel', payload={'alive': True})
    item = client.claim()[0]
    assert item['message_id'] == after['message_id']
    client.settle(item)

    # 次序可在消息轨迹中复验：批次消息以 published 开头并能取到证据。
    trace = client.request(f"/messages/{sealed['message_id']}")
    assert [event['kind'] for event in trace['events'][:1]] == ['published']
    return {'middle_delivery': middle_delivery, 'batch_delivery': batch_delivery,
            'message_id': sealed['message_id'], 'cancelled': doomed['batch_id']}


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
