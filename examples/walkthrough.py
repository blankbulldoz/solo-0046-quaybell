"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor

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


def renew(client):
    client.channel()
    client.subscribe()
    origin = client.clock[0]

    # --- 大信号包：连续续租两次，丢失响应后按原请求重发 ---------------------
    big = client.publish('big-signal', 'lane-big')
    item = client.claim()[0]
    path = f"/deliveries/{item['delivery_id']}/renew"
    assert item['lease']['expires'] == origin + 4
    assert item['lease']['budget_expires'] == origin + 12  # 首次领取确定的总预算

    first = client.renew(item, request_id='renew-1')
    assert (first['expires'], first['renewals']) == (origin + 8, 1)
    assert client.renew(item, request_id='renew-1') == first | {'repeated': True}

    second = client.renew(item, request_id='renew-2')
    assert (second['expires'], second['renewals'], second['remaining_seconds']) == (origin + 12, 2, 0)
    assert client.renew(item, request_id='renew-2') == second | {'repeated': True}
    # 不带请求标识的原样重发，在预算边界上也返回已记录的续租回执。
    assert client.renew(item) == second | {'repeated': True}
    # 换一个新的续租请求仍受总预算硬约束。
    over = client.renew(item, request_id='renew-3', expected=409)
    assert over['error'] == 'conflict' and over['reason_code'] == 'lease_budget_exceeded'
    assert over['budget_expires'] == origin + 12
    # 别人的凭据不能续我的租约。
    client.request(path, {'token': 'stale-lease'}, 409)
    # 现有短任务仍按原节奏确认，确认响应丢失后重发读到同一回执。
    acked = client.settle(item)
    assert client.settle(item) == acked | {'repeated': True}

    # --- 续租后修订策略：旧投递沿用旧策略，同键后继保持阻挡并可解释 ----------
    head_msg = client.publish('policy-head', 'lane-order')
    tail_msg = client.publish('policy-tail', 'lane-order')
    client.publish('policy-aside', 'lane-other')
    claimed = {row['message_id']: row for row in client.claim(10)}
    head = claimed[head_msg['message_id']]
    assert tail_msg['message_id'] not in claimed  # 同键后继被队首挡住

    renewed = client.renew(head)
    assert renewed['expires'] == origin + 8
    revised = client.request(CHANNEL + '/policy', POLICY | {'expected': 1, 'lease_seconds': 12}, 201)
    assert revised['version'] == 2
    renewed_again = client.renew(head)  # 旧投递仍按旧版 4 秒步进，而不是新版 12 秒
    assert renewed_again['expires'] == origin + 12

    new_policy_msg = client.publish('new-policy', 'lane-fresh')
    fresh = next(row for row in client.claim(10) if row['message_id'] == new_policy_msg['message_id'])
    assert fresh['lease']['expires'] == origin + 12 and fresh['lease']['budget_expires'] == origin + 36
    client.settle(fresh)

    board = client.request(CHANNEL)
    group_view = next(row for row in board['groups'] if row['name'] == 'oscillator')
    causes = {row['message_id']: row['cause'] for row in group_view['sample']}
    assert causes[head_msg['message_id']] == 'leased'
    assert causes[tail_msg['message_id']] == 'blocked'

    client.tick(12)  # 恰好到两次续租后的截止时刻 origin+12
    board = client.request(CHANNEL)
    group_view = next(row for row in board['groups'] if row['name'] == 'oscillator')
    causes = {row['message_id']: row['cause'] for row in group_view['sample']}
    assert causes[head_msg['message_id']] == 'expired'
    assert causes[tail_msg['message_id']] == 'blocked'  # 到期未回收前队首仍挡住后继

    swept = client.request('/sweep', {'limit': 100})
    head_recovery = next(row for row in swept['recovered'] if row['delivery_id'] == head['delivery_id'])
    assert head_recovery['effective_at'] == renewed_again['expires'] + 3  # 从续租后的截止起算
    board = client.request(CHANNEL)
    group_view = next(row for row in board['groups'] if row['name'] == 'oscillator')
    causes = {row['message_id']: row['cause'] for row in group_view['sample']}
    assert causes[head_msg['message_id']] == 'delayed'
    assert causes[tail_msg['message_id']] == 'blocked'

    client.tick(3)
    retried = {row['message_id']: row for row in client.claim(10)}
    head_attempt = retried[head_msg['message_id']]
    assert head_attempt['lease']['number'] == 2 and head_attempt['policy'] == 1
    client.settle(head_attempt)
    tail = client.claim()[0]
    assert tail['message_id'] == tail_msg['message_id'] and tail['policy'] == 1
    client.settle(tail)
    for row in retried.values():  # 独立键上的并行重试投递一并确认收尾
        if row['delivery_id'] != head_attempt['delivery_id']:
            client.settle(row)

    # --- 租约刚到期：顺序视角下续租 409，回收按原截止入账 -------------------
    edge = client.publish('edge-signal', 'lane-edge')  # 修订后发布，绑定 12 秒新租约
    edge_item = client.claim()[0]
    edge_expires = edge_item['lease']['expires']
    client.tick(12)
    assert client.clock[0] == edge_expires
    assert client.renew(edge_item, expected=409)['error'] == 'conflict'
    edge_sweep = client.request('/sweep', {'limit': 100})
    edge_recovery = next(row for row in edge_sweep['recovered']
                         if row['delivery_id'] == edge_item['delivery_id'])
    assert edge_recovery['effective_at'] == edge_expires + 3
    client.settle(edge_item, expected=409)  # 旧凭据随回收失效
    client.tick(3)
    edge_retry = client.claim()[0]
    assert edge_retry['lease']['number'] == 2
    client.settle(edge_retry)

    trace = client.request(f"/messages/{big['message_id']}")
    attempt = trace['deliveries'][0]['attempts'][0]
    assert attempt['renew_count'] == 2 and attempt['renewals'] == 2
    assert len(attempt['renewal_history']) == 2
    assert [event['kind'] for event in trace['events']].count('renewed') == 2
    assert item['lease']['token'] not in pack(trace)
    return {'renewals': 2, 'budget_expires': origin + 12,
            'head_retry_at': head_recovery['effective_at'], 'edge_retry_at': edge_recovery['effective_at']}


def renew_race(client):
    """Concurrent renew/sweep at the exact deadline must have one winner."""
    client.channel()
    client.subscribe()
    client.publish('race-signal', 'lane-race')
    item = client.claim()[0]
    delivery = item['delivery_id']
    client.tick(4)  # 精确停在 expires 时刻：按既有约定此刻租约已到期
    parties = 6
    barrier = __import__('threading').Barrier(parties)

    def renew_one(index):
        barrier.wait()
        return client.raw(f'/deliveries/{delivery}/renew',
                          {'token': item['lease']['token'], 'request_id': f'race-{index}'})

    def sweep_one(index):
        barrier.wait()
        return client.raw('/sweep', {'limit': 10})

    with ThreadPoolExecutor(parties) as pool:
        futures = [pool.submit(renew_one, i) for i in range(4)]
        futures += [pool.submit(sweep_one, i) for i in range(2)]
        results = [future.result(timeout=20) for future in futures]
    renew_results = results[:4]
    sweep_results = results[4:]
    assert all(code == 409 for code, _ in renew_results), renew_results
    assert all(code == 200 for code, _ in sweep_results), sweep_results
    recovered = sum(len(body.get('recovered', [])) for _, body in sweep_results)
    assert recovered == 1, recovered
    trace = client.request(f"/messages/{item['message_id']}")
    assert trace['deliveries'][0]['attempts'][0]['outcome'] == 'expired'
    return {'renew_conflicts': 4, 'recovered_once': recovered}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export,
             renew=renew, **{'renew-race': renew_race})


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
