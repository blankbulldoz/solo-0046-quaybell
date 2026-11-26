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
    # 原有合同：恰好到期的旧凭据确认返回 409，回收按原截止时间 + 退避重算。
    client.channel()
    client.subscribe()
    client.publish()
    item = client.claim()[0]
    client.tick(4)
    client.settle(item, expected=409)
    client.tick(3)
    original_request = {'limit': 1}
    result = client.request('/sweep', original_request)
    assert result['recovered'][0]['effective_at'] == item['lease']['expires'] + 3
    second = client.claim()[0]
    assert second['lease']['number'] == 2
    client.settle(second)
    # 原回收请求在处置完成后重试是安全的：不会重复关闭，也没有新的到期尝试。
    settled_retry = client.request('/sweep', original_request)
    assert settled_retry['recovered'] == [] and settled_retry['remaining'] == 0

    # 维护窗口场景：提高后续投递的尝试预算，便于演示第二代退避同样锚定原截止。
    client.request(CHANNEL + '/policy', POLICY | {'expected': 1, 'max_attempts': 5}, 201)

    # 几十条租约：每条使用独立顺序键，两次领取（单次上限 30）在同一受控时钟下发租，
    # 因而共享完全相同的截止时间。
    total = 40
    for index in range(total):
        client.publish(f'batch-{index:02d}', f'bay-{index:02d}')
    batch = client.claim(30) + client.claim(30)
    assert len(batch) == total
    deadlines = {taken['lease']['expires'] for taken in batch}
    assert len(deadlines) == 1
    deadline = deadlines.pop()

    # 维护窗口过去两个小时：所有租约早就到期，但回收一直没有运行。
    maintenance_delay = 7200
    client.tick(maintenance_delay)
    reap_now = client.clock[0]

    # 观测上界 = 当下到期开放尝试中的最大原截止；此刻 40 条全部落在同一上界内。
    observed = client.request('/sweep')
    assert observed['upper_bound'] == deadline
    assert (observed['remaining'], observed['newly_expired']) == (total, 0)

    # 第一段（带幂等标识）：只推进 10 条，响应给出本轮截止范围和上界内剩余。
    first_request = {'limit': 10, 'upper_bound': deadline, 'request_key': 'maintenance-window-segment-a'}
    first = client.request('/sweep', first_request)
    first_ids = [row['delivery_id'] for row in first['recovered']]
    assert len(first_ids) == 10
    assert first['segment_range']['from_expires'] == deadline == first['segment_range']['to_expires']
    assert first['remaining'] == total - 10 and first['caught_up'] is False

    # 重试“原回收请求”：命中持久回执，逐字段返回首次结果并标记 repeated，不会再回收下一批。
    retried = client.request('/sweep', first_request)
    assert retried == first | {'repeated': True}
    assert [row['delivery_id'] for row in retried['recovered']] == first_ids

    # 再推进一段（不带标识，重复同形请求会继续追赶而不是重放）。
    second_segment = client.request('/sweep', {'limit': 10, 'upper_bound': deadline})
    assert len(second_segment['recovered']) == 10
    assert second_segment['remaining'] == total - 20

    # 分段之间一次正常领取会先做组内有界回收：剩余 20 条在这里被领取路径回收，
    # 随后立即重领出第 2 次尝试——下一次可领时间是 deadline+3，远早于当前时钟，
    # 所以两个小时的维护延迟没有整体推后就绪时刻。
    claim_reply = client.request(GROUP + '/claim', {'worker': 'catchup-worker', 'limit': 30})
    claim_recovered = claim_reply['recovered']
    assert len(claim_recovered) == total - 20
    assert {row['effective_at'] for row in claim_recovered} == {deadline + 3}
    assert all(row['effective_at'] < reap_now for row in claim_recovered)
    assert len(claim_reply['items']) == 30
    assert {taken['lease']['number'] for taken in claim_reply['items']} == {2}
    next_deadline = reap_now + 4
    assert {taken['lease']['expires'] for taken in claim_reply['items']} == {next_deadline}

    # 再按旧上界回收一段：回收数为 0，但进度显示上界内已清空、累计推进覆盖全部 40 条。
    caught = client.request('/sweep', {'limit': 10, 'upper_bound': deadline})
    assert caught['recovered'] == [] and caught['remaining'] == 0 and caught['caught_up'] is True
    assert caught['advanced']['count'] == total + 1
    assert caught['advanced']['from_expires'] == item['lease']['expires']
    assert caught['advanced']['to_expires'] == deadline

    # 新领出的 30 个租约 5 秒后到期：它们的截止晚于旧观测上界，属于“新到期”，
    # 而不是旧上界内的“剩余”。
    client.tick(5)
    watch = client.request('/sweep', {'limit': 1, 'upper_bound': deadline})
    assert watch['remaining'] == 0
    assert watch['newly_expired'] == 30
    assert watch['current_upper_bound'] == next_deadline

    # 按新上界回收第二代：退避按“原截止 next_deadline”翻倍为 6 秒，与维护延迟无关。
    wave = client.request('/sweep', {'limit': 100, 'upper_bound': next_deadline})
    assert len(wave['recovered']) == 30
    assert {row['effective_at'] for row in wave['recovered']} == {next_deadline + 6}

    # 恰好走到 next_deadline+6 即可再次领取第 3 次尝试，证明就绪时间没有被回收时刻推后。
    client.tick(5)
    third = client.claim(30)
    assert len(third) == 30 and {taken['lease']['number'] for taken in third} == {3}

    return {
        'lease_deadline': deadline,
        'maintenance_delay_seconds': maintenance_delay,
        'observed_upper_bound': observed['upper_bound'],
        'segment_size': 10,
        'first_segment_ids': first_ids,
        'original_request_repeated': retried['repeated'],
        'claim_triggered_recovered': len(claim_recovered),
        'remaining_before_claim': total - 20,
        'remaining_after_claim': caught['remaining'],
        'newly_expired_beyond_bound': watch['newly_expired'],
        'next_ready_at': next_deadline + 6,
        'third_attempts_claimed': len(third),
        'legacy': result,
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
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep,
             catchup=sweep, dead=dead,
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
