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


def classify(client):
    """临时限流走退避、永久拒绝立即终结；旧原因文字保持旧行为。

    消息 A：v1 策略下第 1 次尝试被限流 -> 按处置时间 +3s 重试；策略改版到
    v2 后，旧投递仍绑定 v1，第 2 次尝试租约到期 -> 从原截止时间 +6s（3*2）
    安排第 3 次；第 3 次再到期时预算（max_attempts=3）用尽 -> 死信。
    消息 B：第 1 次尝试即明确永久拒绝 -> 不等待、不消耗预算，立即死信。
    消息 C：旧消费者只传 reason -> 仍是旧的退避行为，且重试同一 NACK 读回执。
    消息 D：用 retryable=false 的布尔写法表达永久拒绝，验证等价别名。
    """
    rule = POLICY | {'max_attempts': 3}
    client.channel(rule)
    client.subscribe()
    t0 = client.clock[0]

    # A：旧策略下先被限流。
    throttled = client.publish('throttled-frame', 'coil-north')
    id_a = throttled['fanout']['oscillator']
    first = client.claim()[0]
    assert first['lease']['number'] == 1 and first['policy'] == 1
    rate = client.settle(first, 'nack', reason='外部网关 429 限流',
                         classification='rate_limited')
    assert (rate['state'], rate['classification'], rate['retryable']) == (
        'waiting', 'rate_limited', True)
    assert rate['effective_at'] == t0 + POLICY['retry_seconds']
    assert client.claim() == []

    # 策略改版：放宽租约/预算，但 A 已绑定 v1，不受影响。
    revised = client.request(CHANNEL + '/policy',
                             rule | {'expected': 1, 'lease_seconds': 12,
                                     'retry_seconds': 5, 'max_attempts': 5}, 201)
    assert revised['version'] == 2

    client.tick(3)
    second = client.claim()[0]
    assert (second['lease']['number'], second['policy']) == (2, 1)
    assert second['lease']['expires'] == t0 + 3 + POLICY['lease_seconds'] == t0 + 7

    # 第 2 次租约恰好到期后全局回收：退避从原租约截止 t0+7 起算，
    # 第 2 次失败对应指数倍数 2，故下一次最早 t0+13。
    client.tick(4)
    recovered = client.request('/sweep', {'limit': 10})['recovered']
    assert len(recovered) == 1
    expiry_one = recovered[0]
    assert (expiry_one['outcome'], expiry_one['classification'],
            expiry_one['state']) == ('expired', 'timeout', 'waiting')
    assert expiry_one['effective_at'] == t0 + 7 + POLICY['retry_seconds'] * 2 == t0 + 13

    client.tick(6)
    third = client.claim()[0]
    assert third['lease']['number'] == 3 and third['policy'] == 1
    assert third['lease']['expires'] == t0 + 17
    client.tick(4)
    final_expiry = client.request('/sweep', {})['recovered'][0]
    assert (final_expiry['state'], final_expiry['terminal_reason']) == (
        'dead', 'max_attempts')
    assert final_expiry['effective_at'] == t0 + 17

    trace_a = client.request(f"/messages/{throttled['message_id']}")
    attempts_a = trace_a['deliveries'][0]['attempts']
    assert [a['number'] for a in attempts_a] == [1, 2, 3]
    assert [a['outcome'] for a in attempts_a] == ['nacked', 'expired', 'expired']
    assert [a['classification'] for a in attempts_a] == [
        'rate_limited', 'timeout', 'timeout']
    assert trace_a['deliveries'][0]['terminal_reason'] == 'max_attempts'

    # B：另一条消息在第 1 次尝试即遭到明确永久拒绝。
    rejected = client.publish('unparseable-frame', 'coil-south')
    permanent = client.settle(client.claim()[0], 'nack',
                              reason='载荷不是合法帧，无法解析',
                              classification='permanent')
    assert permanent['state'] == 'dead' and permanent['terminal'] is True
    assert permanent['retryable'] is False
    assert permanent['terminal_reason'] == 'permanent_rejection'
    trace_b = client.request(f"/messages/{rejected['message_id']}")
    assert len(trace_b['deliveries'][0]['attempts']) == 1
    assert trace_b['deliveries'][0]['terminal_reason'] == 'permanent_rejection'

    # C：旧消费者只传 reason 文字 -> 旧退避行为；同请求重试读同一回执。
    # C 在改版后发布，绑定 v2（retry_seconds=5），所以下一次为 t0+22。
    legacy = client.publish('legacy-text', 'coil-east')
    old_item = client.claim()[0]
    once = client.settle(old_item, 'nack')
    twice = client.settle(old_item, 'nack')
    assert once['state'] == 'waiting' and once['classification'] == 'failed'
    assert once['policy'] == 2 and once['effective_at'] == t0 + 22
    assert twice == once | {'repeated': True}

    # D：布尔别名 retryable=false 与显式 permanent 等价。
    aliased = client.publish('alias-reject', 'coil-west')
    via_bool = client.settle(client.claim()[0], 'nack', retryable=False)
    assert via_bool['state'] == 'dead' and via_bool['classification'] == 'permanent'

    dead = client.request(CHANNEL + '/dead')['items']
    dead_ids = {row['id'] for row in dead}
    assert {id_a, rejected['fanout']['oscillator'],
            aliased['fanout']['oscillator']} <= dead_ids
    assert legacy['fanout']['oscillator'] not in dead_ids

    return {
        'policy_versions': {'bound_to_a': 1, 'current': 2},
        'rate_limited_then_expired': {
            'delivery_id': id_a,
            'attempts': [{'number': a['number'], 'outcome': a['outcome'],
                          'classification': a['classification'],
                          'finished': a['finished'], 'reason': a['reason']}
                         for a in attempts_a],
            'next_after_rate_limit_at': t0 + 3,
            'lease_two_expired_at': t0 + 7,
            'next_after_recovery_at': expiry_one['effective_at'],
            'lease_three_expired_at': t0 + 17,
            'terminated_at': final_expiry['effective_at'],
            'terminal_reason': 'max_attempts（第 3 次仍失败，用尽 v1 预算 3 次）',
        },
        'permanent_rejection': {
            'delivery_id': rejected['fanout']['oscillator'],
            'terminal_on_attempt': 1,
            'terminal_reason': 'permanent_rejection（无法解析，不再退避或计数）',
        },
        'legacy_reason_only': {
            'classification': once['classification'],
            'next_attempt_at': once['effective_at'],
            'same_nack_replay': twice['repeated'],
        },
    }


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
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, classify=classify,
             classification=classify, disposition=classify, failures=classify, reject=classify,
             rejection=classify, throttle=classify, throttled=classify, ratelimit=classify,
             rate_limit=classify, permanent=classify,
             sweep=sweep, dead=dead,
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
