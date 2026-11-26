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


def quota(client):
    """在线发布额度：积压不丢、整笔拒绝指出卡住组、下调不绕过、对账与重启一致。"""
    client.channel()
    client.subscribe()                                  # oscillator 无限额度（默认）
    client.subscribe('relay-hold', quota=1)            # 热组初始只剩一格
    client.subscribe('spare', quota=3)

    # 运维先给频道加额度：在线上调立即生效，无需重启。
    adjusted = client.set_quota('relay-hold', 2)
    assert adjusted['quota'] == 2 and adjusted['outstanding'] == 0 and adjusted['available'] == 2

    # 热组在两个顺序键上各占一项：一条待领、一条 NACK 后等待退避的重试，两格全满。
    client.publish('hot-1', 'coil-north')
    held = client.claim(group='relay-hold', worker='hot-worker')[0]
    client.publish('hot-2', 'coil-south')
    retry_item = client.claim(group='relay-hold', worker='hot-worker')[0]
    client.settle(retry_item, 'nack')
    hot = next(group for group in client.request(CHANNEL)['groups']
               if group['name'] == 'relay-hold')
    assert hot['quota'] == {'quota': 2, 'outstanding': 2, 'available': 0}

    # 新发布同时匹配多个组，relay-hold 没空位 → 整笔拒绝并指出卡住的组；
    # 同笔在还有空位的 spare 也不得留下任何分发（不丢债务、不留残片）。
    rejected = client.publish('blocked-publish', 'coil-north', expected=409)
    assert rejected['error'] == 'conflict' and rejected['code'] == 'quota_exhausted'
    assert rejected['blocked'] == [
        {'group': 'relay-hold', 'quota': 2, 'outstanding': 2, 'requested': 1, 'available': 0}]
    board = client.request(CHANNEL)
    assert next(group for group in board['groups'] if group['name'] == 'relay-hold')['quota']['outstanding'] == 2
    # 被整笔拒绝的消息在 spare 也没有残片：仍是热组前两笔带来的 2 条。
    assert next(group for group in board['groups'] if group['name'] == 'spare')['quota']['outstanding'] == 2
    assert all('blocked-publish' not in str(item) for item in board['messages'])

    # 事故处置：在线把额度下调到当前积压以下（2 → 1），拒绝仍只点名真正卡住的热组。
    lowered = client.set_quota('relay-hold', 1)
    assert lowered['quota'] == 1 and lowered['outstanding'] == 2 and lowered['available'] == 0
    client.publish('still-blocked', 'coil-north', expected=409)

    # 下调不绕过此组：正常消费（先 ACK 待领项，退避结束后重领重试项再 ACK）逐步归还额度；
    # 恢复前新发布持续被拒，恢复后整笔发布成功并同时落到全部匹配组。
    client.settle(held)
    hot = next(group for group in client.request(CHANNEL)['groups']
               if group['name'] == 'relay-hold')
    assert hot['quota'] == {'quota': 1, 'outstanding': 1, 'available': 0}
    client.publish('no-slot-yet', 'coil-north', expected=409)
    client.tick(3)
    retried = client.claim(group='relay-hold', worker='hot-worker')[0]
    assert retried['lease']['number'] == 2
    ack_receipt = client.settle(retried)
    assert ack_receipt['state'] == 'acked' and ack_receipt['repeated'] is False
    recovered = client.publish('recovered', 'coil-north')
    assert set(recovered['fanout']) == {'oscillator', 'relay-hold', 'spare'}
    hot = next(group for group in client.request(CHANNEL)['groups']
               if group['name'] == 'relay-hold')
    assert hot['quota']['outstanding'] == 1 and hot['quota']['available'] == 0

    # 值班员用实际回执对账：重试项的消息追溯里发布、领取、NACK、重领与 ACK 事件齐备。
    trace = client.request(f"/messages/{retry_item['message_id']}")
    kinds = [event['kind'] for event in trace['events']]
    assert kinds[0] == 'published' and kinds.count('leased') == 2
    assert {'nacked', 'acked'} <= set(kinds)

    # 平稳流量不随已有积压变慢：热组前两笔和 recovered 已让 spare 达到满额（额度 3），
    # 每次发布的准入只做受额度约束的有界索引探测，与历史总量无关。
    client.settle(client.claim(group='relay-hold', worker='hot-worker')[0])
    spare = next(group for group in client.request(CHANNEL)['groups'] if group['name'] == 'spare')
    assert spare['quota'] == {'quota': 3, 'outstanding': 3, 'available': 0}
    blocked_again = client.publish('steady-overflow', 'coil-spare', expected=409)
    assert [item['group'] for item in blocked_again['blocked']] == ['spare']
    client.settle(client.claim(group='spare', worker='spare-worker')[0])
    assert client.publish('steady-next', 'coil-spare')['fanout']['spare'] > 0
    return {'rejected': rejected['blocked'], 'lowered': lowered['quota'],
            'recovered_message': recovered['message_id']}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, quota=quota)


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
