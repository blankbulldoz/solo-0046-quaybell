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


def reconcile(client):
    client.channel()
    client.subscribe()

    # 场景一：新版两个供应商把同一设备产量分别报成整克与千克小数，归一化后等量；
    # 客户端丢响应后换单位重交，必须读回同一承诺而不是误报冲突。
    first = client.publish('yield-a')
    taken = client.claim()[0]
    grams = {'unit': 'g', 'amount': 1200, 'supplier': 'vendor-scale-g'}
    accepted = client.settle(taken, result=grams)
    assert accepted['state'] == 'acked' and accepted['result']['canonical_amount'] == '1200000'
    assert accepted['result']['result_unit'] == 'g'
    assert accepted['result']['result_rule'] == 1 and accepted['repeated'] is False
    lost_reply = client.settle(taken, result={'unit': 'kg', 'amount': '1.200', 'supplier': 'vendor-scale-kg'})
    assert lost_reply['repeated'] is True
    assert lost_reply['result']['canonical_amount'] == accepted['result']['canonical_amount']
    # 键顺序和数字写法不同的等价报文同样命中幂等，不被序列化差异误伤。
    shuffled = client.settle(taken, result={'supplier': 'vendor-scale-kg', 'amount': '1.2', 'unit': 'kg'})
    assert shuffled == lost_reply

    # 场景二：界面按千克取整后都显示 1.2 kg，但实际量不同的重交不能当成重复。
    lookalike = client.settle(
        taken, expected=409,
        result={'unit': 'kg', 'amount': '1.201', 'supplier': 'vendor-scale-kg'})
    assert '不一致' in lookalike['message']
    assert '已记录 1200.000 g' in lookalike['message'] and '1201.000 g' in lookalike['message']
    # 冲突拒收不改写原承诺：再交等量报文仍读回第一次接收的回执。
    assert client.settle(taken, result=grams)['result']['canonical_amount'] == '1200000'

    # 场景三：原租约过期后才送达的结果必须拒收，且解释被拒收的归一化量。
    late_msg = client.publish('yield-b')
    late_item = client.claim()[0]
    client.tick(4)
    late = client.settle(late_item, expected=409,
                         result={'unit': 'kg', 'amount': '2.5', 'supplier': 'vendor-scale-kg'})
    assert '租约已处置或超时' in late['message'] and '2500.000 g' in late['message']
    assert '结果未被接收' in late['message']
    client.request('/sweep', {'limit': 1})
    client.tick(3)
    retaken = client.claim()[0]
    assert retaken['delivery_id'] == late_msg['fanout']['oscillator']
    assert retaken['lease']['number'] == 2
    recovered = client.settle(retaken, result={'unit': 'g', 'amount': 2500})
    assert recovered['result']['canonical_amount'] == '2500000'

    # 规则升级：历史 ACK 仍按接收时钉住的 v1 解释，新投递使用 v2。
    new_rule = client.result_rule({
        'base_unit': 'g', 'tick_scale': 0,
        'units': {'g': {'factor': '1'}, 'kg': {'factor': '1000'}, 'mg': {'factor': '0.001'}}})
    assert new_rule['version'] == 2
    # 旧记录在 v2（毫克级小数会被拒）下仍按 v1 接受原始重交。
    legacy_repeat = client.settle(taken, result={'unit': 'kg', 'amount': '1.200'})
    assert legacy_repeat['result']['result_rule'] == 1 and legacy_repeat['repeated'] is True
    fresh = client.publish('yield-c')
    fresh_item = client.claim()[0]
    coarse = client.settle(fresh_item, result={'unit': 'g', 'amount': 88})
    assert coarse['result']['result_rule'] == 2 and coarse['result']['canonical_amount'] == '88'

    # 对外证据：只含归一化量、规则版本与决定原因，供应商原始明细和内部摘要不出现。
    evidence = client.request(f"/messages/{first['message_id']}/evidence")
    encoded = pack(evidence)
    assert 'vendor-scale' not in encoded and 'result_digest' not in encoded
    assert evidence['result_rules']['1']['base_unit'] == 'g'
    digest = evidence.pop('sha256')
    assert fingerprint(pack(evidence)) == digest
    return {'accepted': accepted['result'], 'rule_after_change': new_rule['version'],
            'evidence_sha256': digest}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, reconcile=reconcile)


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
