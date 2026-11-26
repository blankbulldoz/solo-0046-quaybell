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


def bridge(client):
    src = '/channels/signal-weir'
    dst = '/channels/relay-basin'
    # 两个频道各自建立，目标频道有自己的策略与订阅。
    client.request('/channels', {'name': 'signal-weir', 'policy': POLICY}, 201)
    client.request('/channels', {'name': 'relay-basin',
                                 'policy': POLICY | {'lease_seconds': 7}}, 201)
    client.request(src + '/groups', {'name': 'weir-rotor', 'tags': {}}, 201)
    client.request(dst + '/groups', {'name': 'basin-keeper', 'tags': {'feed': 'telemetry'}}, 201)
    # 桥接只在标签精确匹配时转发，空标签组仍按目标订阅过滤。
    rule = client.request(src + '/bridges',
                          {'name': 'span', 'target': 'relay-basin', 'tags': {'feed': 'telemetry'}}, 201)
    assert rule['version'] == 1 and rule['target_channel'] == 'relay-basin'
    client.request(src + '/bridges',
                   {'name': 'span', 'target': 'relay-basin', 'tags': {'feed': 'telemetry'}}, 409)
    client.request(src + '/bridges',
                   {'name': 'bad-span', 'target': 'missing-channel', 'tags': {}}, 404)

    # 预查只读：标签不匹配不会产生任何转发，且不写入消息。
    miss_env = envelope('skip', 'coil-south', tags={'feed': 'alerts'})
    miss_preview = client.request(src + '/preview', miss_env)
    assert miss_preview['bridges'] == [{'bridge': 'span', 'version': 1,
                                        'target': 'relay-basin', 'match': False}]
    miss = client.request(src + '/messages', miss_env, 201)
    assert miss['bridges'] == [] and miss['origin'] == 'direct'
    assert client.request(dst)['messages'] == []

    # 匹配发布：源回执在同一事务内确认目标已持久化，并给出目标分发。
    hit_env = envelope('pulse-9', 'coil-north', tags={'feed': 'telemetry'})
    hit_preview = client.request(src + '/preview', hit_env)
    assert hit_preview['bridges'][0]['match'] is True
    first = client.request(src + '/messages', hit_env, 201)
    target_id = first['bridges'][0]['target_message_id']
    assert {k: first['bridges'][0][k] for k in ('bridge', 'version', 'target', 'target_message_id')} == \
        {'bridge': 'span', 'version': 1, 'target': 'relay-basin', 'target_message_id': target_id}
    target_delivery_id = first['bridges'][0]['target_fanout']['basin-keeper']
    assert isinstance(target_delivery_id, int)
    assert list(first['fanout']) == ['weir-rotor']
    target = client.request(f'/messages/{target_id}')
    assert target['message']['origin'] == 'bridged'
    assert target['incoming'][0] == {'bridge': 'span', 'version': 1,
                                     'source_channel': 'signal-weir',
                                     'source_message_id': first['message_id']}
    # 目标频道按自己的订阅和策略分发。
    target_delivery = client.request(dst + '/groups/basin-keeper/claim', {'worker': 'basin-worker'})
    assert target_delivery['items'][0]['message_id'] == target_id
    assert target_delivery['items'][0]['delivery_id'] == target_delivery_id
    # 目标频道策略租约 7 秒（源频道为 4 秒），由受控时钟确认。
    assert target_delivery['items'][0]['lease']['expires'] == client.clock[0] + 7

    # 目标频道同生产者同键的直接发布与桥接消息并存，谁也不会被误认。
    direct = client.request(dst + '/messages',
                            dict(producer='signal-source', key='pulse-9', lane='coil-north',
                                 tags={'feed': 'telemetry'}, payload={'value': 18}), 201)
    assert direct['origin'] == 'direct' and direct['message_id'] != target_id
    board = client.request(dst)
    origins = {row['id']: row['origin'] for row in board['messages']}
    assert origins[target_id] == 'bridged' and origins[direct['message_id']] == 'direct'

    # 源回执丢失后按原身份重发：复用原回执，不再产生新的桥接转发。
    before = len(client.request(dst)['messages'])
    resent = client.request(src + '/messages', hit_env, 200)
    assert resent == first | {'repeated': True}
    after = client.request(dst)['messages']
    assert len(after) == before
    assert client.request(f"/messages/{first['message_id']}")['bridges'] == first['bridges']

    # 桥接配置换版：旧回执仍定位第一版当时的目标结果，新发布按新版另建链接。
    revised = client.request(src + '/bridges/span',
                             {'target': 'relay-basin', 'tags': {'feed': 'samples', 'kind': 'span'},
                              'expected': 1}, 201)
    assert revised['version'] == 2
    client.request(src + '/bridges/span',
                   {'target': 'relay-basin', 'tags': {'feed': 'samples'}, 'expected': 1}, 409)
    old_trace = client.request(f"/messages/{first['message_id']}")
    assert {k: old_trace['bridges'][0][k] for k in ('bridge', 'version', 'target', 'target_message_id')} == \
        {'bridge': 'span', 'version': 1, 'target': 'relay-basin', 'target_message_id': target_id}
    next_env = envelope('pulse-10', 'coil-north', tags={'feed': 'samples', 'kind': 'span'})
    new_hit = client.request(src + '/messages', next_env, 201)
    assert new_hit['bridges'][0]['version'] == 2
    assert new_hit['bridges'][0]['target_message_id'] != target_id
    new_target = client.request(f"/messages/{new_hit['bridges'][0]['target_message_id']}")
    assert new_target['incoming'][0]['version'] == 2
    # 换版后不满足新条件的发布不会被转发。
    dropped = client.request(src + '/messages', envelope('pulse-11', 'coil-south'), 201)
    assert dropped['bridges'] == []
    # 证据导出与轨迹一致，桥接结果随源消息一起冻结。
    evidence_record = client.request(f"/messages/{first['message_id']}/evidence")
    assert evidence_record['trace']['bridges'] == old_trace['bridges']
    return {'first_target': target_id,
            'resent_targets': [row['target_message_id'] for row in resent['bridges']],
            'old_receipt_version': old_trace['bridges'][0]['version'],
            'new_receipt_version': new_hit['bridges'][0]['version'],
            'new_target': new_hit['bridges'][0]['target_message_id']}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, bridge=bridge)


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
