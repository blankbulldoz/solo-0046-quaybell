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


def backfill(client):
    # 归档组订阅带标签的消息；保留期很短，便于演示一小时窗口内的清理。
    client.channel(POLICY | {'retention_seconds': 6, 'max_attempts': 1})
    client.subscribe(tags={'feed': 'samples'})
    start = client.clock[0]

    # 热键 coil-north：两条历史将被清理，前者清理前导出证据（有据删除），
    # 后者清理后只剩编号（未归类删除）；再加一条延迟、清理时仍未就绪而幸存的消息。
    proven = client.publish('gap-proven', 'coil-north')
    numbered = client.publish('gap-numbered', 'coil-north')
    surviving = client.publish('gap-survivor', 'coil-north', delay_seconds=20)
    # 不匹配标签的消息也会被清理，但补订按标签过滤后应忽略。
    client.publish('ignored', 'coil-north', tags={'feed': 'alerts'})

    # 处置到终态（延迟消息未就绪、领不到，因此清理时仍有活动义务而幸存）。
    proven_pack = client.evidence(proven['message_id'])
    for _ in range(4):
        for item in client.claim(limit=10):
            client.settle(item)
    client.tick(7)
    pruned = client.request(CHANNEL + '/retention', {})
    assert {proven['message_id'], numbered['message_id']} <= set(pruned['removed'])
    assert surviving['message_id'] not in pruned['removed']
    client.request(f"/messages/{proven['message_id']}", expected=404)
    window_end = client.clock[0] + 1

    # 交接期间正常发布继续：热键后继与冷键新消息照常入队。
    hot_after = client.publish('hot-after', 'coil-north')
    cold_after = client.publish('cold-after', 'coil-south')

    # 冷键没有缺口，立即能领取，不必等热键把历史补完。
    first_batch = client.claim(limit=10)
    assert [row['message_id'] for row in first_batch] == [cold_after['message_id']]
    client.settle(first_batch[0])

    # 创建响应丢失：同身份请求重放返回同一份交接单，不重复建投递。
    handoff = client.backfill('archive-hour', (start, window_end))
    replay = client.backfill('archive-hour', (start, window_end), expected=200)
    assert replay['repeated'] is True and replay['id'] == handoff['id']
    entry_count = len(handoff['entries'])
    assert len(client.backfill('archive-hour', (start, window_end), expected=200)['entries']) == entry_count

    statuses = {entry['message_id']: entry['status'] for entry in handoff['entries']}
    # 幸存消息直接补订；两条删除先未归类；标签不匹配的删除被过滤，冷键不出现缺口。
    assert statuses[surviving['message_id']] == 'delivered'
    assert statuses[proven['message_id']] == 'unclassified'
    assert statuses[numbered['message_id']] == 'unclassified'
    assert handoff['sealed'] is False and handoff['open_lanes'] == ['coil-north']

    # 热键被未归类屏障挡住，后继仍领不到；冷键没有缺口，其补订可独立领取。
    second_batch = client.claim(limit=10)
    assert {row['lane'] for row in second_batch} == {'coil-south'}
    assert hot_after['message_id'] not in [row['message_id'] for row in second_batch]
    for item in second_batch:
        client.settle(item)

    # 上交清理前的证据包：摘要核验通过后，未归类缺口升级为有据删除并撤屏障。
    intake = client.intake(proven_pack, handoff['id'])
    assert intake['repeated'] is False
    assert client.intake(proven_pack, handoff['id'], expected=200)['repeated'] is True
    tampered = dict(proven_pack)
    tampered['trace'] = dict(proven_pack['trace'], observed_at=proven_pack['observed_at'] + 1)
    client.intake(tampered, handoff['id'], expected=409)
    after_evidence = client.handoff(handoff['id'])
    proven_entry = next(e for e in after_evidence['entries'] if e['message_id'] == proven['message_id'])
    assert proven_entry['status'] == 'recovered_evicted'

    # 只剩清理编号的删除仍无法归类，值班员人工裁决跳过并撤除最后一个屏障。
    numbered_entry = next(e for e in after_evidence['entries']
                          if e['message_id'] == numbered['message_id'])
    sealed = client.resolve_gap(handoff['id'], numbered_entry['id'], 'skipped')
    assert sealed['sealed'] is True and sealed['open_lanes'] == []

    # 屏障撤除后热键仍按 补订前序(幸存) -> 交接期新发布 领取，后继不越位。
    claimed = []
    while True:
        batch = client.claim(limit=10)
        if not batch:
            break
        for item in batch:
            claimed.append(item['message_id'])
            client.settle(item)
    assert claimed == [surviving['message_id'], hot_after['message_id']]
    return {'backfill': handoff['id'], 'sealed': sealed['sealed'],
            'summary': sealed['summary'], 'hot_order': claimed}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, backfill=backfill)


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
