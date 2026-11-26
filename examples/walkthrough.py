"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import logging
from threading import Barrier, Thread

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


def approval(client):
    """冻结名单审批：新到期不连带、重投保护活跃分发、并发批准恰好执行一次。"""
    client.channel(POLICY | {'max_attempts': 1})
    client.subscribe()
    clean = client.publish('approved-clean', 'lane-clean')
    guarded = client.publish('redriven-guard', 'lane-guard')
    taken = client.claim(2)
    assert {row['message_id'] for row in taken} == {clean['message_id'], guarded['message_id']}
    for row in taken:
        client.settle(row, 'nack')  # max_attempts=1：两条原投递都成为死信

    clean_root = clean['fanout']['oscillator']
    clean_child = client.request(f'/deliveries/{clean_root}/redrive', {}, 201)
    picked = client.claim(2)
    assert [row['delivery_id'] for row in picked] == [clean_child['delivery_id']]
    client.settle(picked[0])  # clean 的重投后继已确认，整组终态

    # 改版保留期：之后发布的 late 绑定 v2 的 22 秒，旧两条仍按 v1 的 11 秒
    client.request(CHANNEL + '/policy',
                   POLICY | {'max_attempts': 1, 'retention_seconds': 22, 'expected': 1}, 201)
    late = client.publish('late-expiry', 'lane-late')
    late_taken = client.claim()
    assert late_taken[0]['message_id'] == late['message_id']
    client.settle(late_taken[0], 'nack')  # 一次即死信，保持终态但 22 秒后才到期

    client.tick(11)  # clean、guarded 满 11 秒到期；late 的 22 秒截止尚未到
    preview = client.request(CHANNEL + '/retention')
    assert [row['id'] for row in preview['eligible']] == [clean['message_id'], guarded['message_id']]

    created = client.request(CHANNEL + '/retention/plans', {'limit': 100}, 201)
    plan_id = created['plan_id']
    frozen = created['message_ids']
    assert frozen == [clean['message_id'], guarded['message_id']] and created['status'] == 'prepared'

    client.tick(11)  # late 也满 22 秒：当前候选名单多出一项，但不能进已冻结计划
    changed_preview = client.request(CHANNEL + '/retention')
    assert [row['id'] for row in changed_preview['eligible']] == [
        clean['message_id'], guarded['message_id'], late['message_id']]

    guard_root = guarded['fanout']['oscillator']
    guard_child = client.request(f'/deliveries/{guard_root}/redrive', {}, 201)  # 截图后死信被重投

    apply_path = CHANNEL + f'/retention/plans/{plan_id}/apply'
    barrier = Barrier(2)
    concurrent = {}

    def approve(who):
        barrier.wait(timeout=5)
        concurrent[who] = client.request(apply_path, {'approved_by': who})

    threads = [Thread(target=approve, args=(who,)) for who in ('alice', 'bob')]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not any(thread.is_alive() for thread in threads) and set(concurrent) == {'alice', 'bob'}

    winner = next(item for item in concurrent.values() if not item['repeated'])
    loser = next(item for item in concurrent.values() if item['repeated'])
    assert winner['removed'] == [clean['message_id']]
    assert loser['removed'] == [clean['message_id']] and loser['plan_id'] == plan_id
    assert [row['id'] for row in winner['skipped']] == [guarded['message_id']]
    assert winner['skipped'][0]['redriven'] is True
    assert winner['skipped'][0]['delivery_id'] == guard_child['delivery_id']

    admin_retry = client.request(apply_path, {'approved_by': 'admin-retry'})
    assert admin_retry['repeated'] is True and admin_retry['removed'] == [clean['message_id']]

    client.request(f"/messages/{clean['message_id']}", expected=404)
    assert client.request(f"/messages/{guarded['message_id']}")['message']['id'] == guarded['message_id']
    assert client.request(f"/messages/{late['message_id']}")['message']['id'] == late['message_id']

    claimable = {row['delivery_id'] for row in client.claim(10)}
    assert claimable == {guard_child['delivery_id']}  # 重投后继仍可领取，活跃分发被保住

    stored = client.request(CHANNEL + f'/retention/plans/{plan_id}')
    approvers = [entry['by'] for entry in stored['approvals']]
    assert stored['status'] == 'applied' and stored['removed'] == [clean['message_id']]
    assert [row['id'] for row in stored['skipped']] == [guarded['message_id']]
    assert len(approvers) == 3 and approvers[-1] == 'admin-retry'
    assert set(approvers[:2]) == {'alice', 'bob'}
    assert stored['approved_by'] == approvers[0]
    remaining = client.request(CHANNEL + '/retention')
    assert [row['id'] for row in remaining['eligible']] == [late['message_id']]

    return {
        'plan_id': plan_id,
        'frozen_list': frozen,
        'preview_after_wait': [clean['message_id'], guarded['message_id'], late['message_id']],
        'simultaneous_approvals': {
            who: {'repeated': reply['repeated'], 'removed': reply['removed'],
                  'skipped': [row['id'] for row in reply['skipped']]}
            for who, reply in concurrent.items()},
        'admin_retry': {'repeated': admin_retry['repeated'], 'removed': admin_retry['removed']},
        'deleted': [clean['message_id']],
        'changed_skipped': [{'id': guarded['message_id'], 'reason': winner['skipped'][0]['reason'],
                             'protected_delivery': guard_child['delivery_id']}],
        'new_due_kept': late['message_id'],
        'claimable_successor': guard_child['delivery_id'],
        'plan_status': stored['status'],
        'approvers': approvers,
    }


def export(client):
    facts = inspect(client)
    record = client.request(f"/messages/{facts['message']['id']}/evidence")
    digest = record.pop('sha256')
    assert record['trace'] == facts and fingerprint(pack(record)) == digest
    return {'format': record['format'], 'sha256': digest}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, approval=approval, export=export)
for alias in ('approve', 'retention-approval', 'concurrent-approval'):
    FLOWS[alias] = approval


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
