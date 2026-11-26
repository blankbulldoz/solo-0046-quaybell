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


def _claim_as(client, group_name):
    path = CHANNEL + f'/groups/{group_name}/claim'
    return client.request(path, {'worker': 'compare-watch', 'limit': 1})['items']


def compare(client):
    """两组对照：未分发（后建组/不匹配）、等待重试、终败重投新身份与冻结快照。"""
    from quaybell.codec import fingerprint, pack

    client.channel(POLICY | {'max_attempts': 3})
    client.subscribe('ackers')
    client.subscribe('retry-team')
    client.subscribe('dead-team')
    client.subscribe('mismatch', tags={'feed': 'samples'})
    sent = client.publish('alert-compare', tags={'feed': 'alerts'})
    mid = sent['message_id']
    assert set(sent['fanout']) == {'ackers', 'retry-team', 'dead-team'}

    taken = _claim_as(client, 'ackers')[0]
    client.settle(taken)
    retrying = _claim_as(client, 'retry-team')[0]
    client.settle(retrying, 'nack')

    # 发布之后才建的组；刻意不推进受控时钟，验证事件顺序也能判定“当时不存在”。
    client.subscribe('late-watch')

    # 在“下一次尝试”之前冻结对照导出。
    frozen = client.request(f'/messages/{mid}/compare/evidence?a=retry-team&b=ackers')
    frozen_digest = frozen['sha256']
    assert fingerprint(pack({k: v for k, v in frozen.items() if k != 'sha256'})) == frozen_digest

    pair = client.request(f'/messages/{mid}/compare?left=retry-team&right=ackers')
    retry_side, ack_side = pair['left'], pair['right']
    assert retry_side['status'] == 'waiting' and retry_side['retrying'] is True
    assert retry_side['markers']['waiting'] is True
    assert retry_side['current']['kind'] == 'retry_backoff'
    assert retry_side['current']['retry_in_seconds'] == 3
    assert ack_side['status'] == 'acked'

    absent = client.request(f'/messages/{mid}/compare?groups=late-watch,mismatch')
    late, mismatch = absent['sides']
    assert late['status'] == 'not_dispatched' and late['not_dispatched'] is True
    assert late['absence_reason'] == 'group_created_after_publication' and late['late'] is True
    assert late['existed_at_publication'] is False
    assert mismatch['status'] == 'not_dispatched'
    assert mismatch['absence_reason'] == 'tags_did_not_match'
    assert mismatch['existed_at_publication'] is True

    # 终败：max_attempts=3，按尝试代际推进退避时钟。
    for number in (1, 2, 3):
        items = _claim_as(client, 'dead-team')
        assert items, f'dead-team 第 {number} 次尝试应当可领取'
        client.settle(items[0], 'nack')
        if number < 3:
            client.tick(3 * 2 ** (number - 1))
    dead_row = next(row for row in client.request(CHANNEL + '/dead')['items']
                    if row['group_name'] == 'dead-team')
    redriven = client.request(f"/deliveries/{dead_row['id']}/redrive", {}, 201)

    lineage = client.request(f'/messages/{mid}/compare?a=dead-team&b=late-watch')['left']
    assert lineage['terminal_failure'] is True and dead_row['id'] in lineage['dead_identities']
    assert lineage['redriven'] is True
    successor = next(item for item in lineage['new_identities']
                     if item['delivery_id'] == redriven['delivery_id'])
    assert successor['parent_id'] == dead_row['id']
    assert lineage['successor_id'] == redriven['delivery_id']
    assert any(node['kind'] == 'redrive_identity' and node['new_identity']
               and node['delivery_id'] == redriven['delivery_id'] for node in lineage['path'])

    # 下一次尝试真的发生：重试组领取第 2 次并确认。
    client.tick(11)
    second = _claim_as(client, 'retry-team')[0]
    assert second['lease']['number'] == 2
    client.settle(second)
    live_doc = client.request(f'/messages/{mid}/compare?a=retry-team&b=ackers')
    live = live_doc['left']
    assert live['status'] == 'acked' and live['attempt_count'] == 2

    # 冻结导出仍指向旧时刻：摘要可复算，内容停在第 1 次失败后的等待。
    assert frozen['observed_at'] == pair['observed_at']
    assert live_doc['observed_at'] > frozen['observed_at']
    assert live['tip']['terminal_at'] > frozen['observed_at']
    assert frozen['by_group']['retry-team']['status'] == 'waiting'
    assert frozen['by_group']['retry-team']['attempt_count'] == 1
    assert frozen['sha256'] == frozen_digest
    checked = client.request('/evidence/verify', frozen)
    assert checked['valid'] is True and checked['observed_at'] == frozen['observed_at']

    tampered = dict(frozen)
    tampered['observed_at'] = frozen['observed_at'] + 999
    assert client.request('/evidence/verify', tampered)['valid'] is False

    later_export = client.request(f'/messages/{mid}/compare/evidence?a=retry-team&b=ackers')
    assert later_export['observed_at'] > frozen['observed_at']
    assert later_export['by_group']['retry-team']['status'] == 'acked'
    return {'message_id': mid, 'frozen_at': frozen['observed_at'],
            'frozen_sha256': frozen_digest, 'successor_id': redriven['delivery_id']}


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, compare=compare,
             comparison=compare, investigate=compare)


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
