"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import copy
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


def _error_codes(report):
    return [item['code'] for item in report.get('errors', [])]


def window_scenario(client):
    base = 1_730_310_000
    client.clock[0] = float(base)
    client.channel(dict(lease_seconds=300, retry_seconds=300, max_attempts=3,
                        retention_seconds=3600))
    client.subscribe('blue-audit')
    client.subscribe('gold-audit')

    client.clock[0] = float(base + 60)
    secret = 'window-audit-secret-4172'
    sent = client.publish('window-key', 'window-lane', producer='audit-producer',
                          payload={'secret': secret, 'reading': 4172})
    message_id = sent['message_id']
    blue_id = sent['fanout']['blue-audit']
    gold_id = sent['fanout']['gold-audit']

    # 两个窗内领取；蓝组租约在窗内到期但不回收，金组租约跨到窗外后才确认。
    client.clock[0] = float(base + 180)
    blue = client.request(CHANNEL + '/groups/blue-audit/claim',
                          {'worker': 'blue-worker', 'limit': 1})['items'][0]
    assert blue['delivery_id'] == blue_id
    client.clock[0] = float(base + 450)
    gold = client.request(CHANNEL + '/groups/gold-audit/claim',
                          {'worker': 'gold-worker', 'limit': 1})['items'][0]
    assert gold['delivery_id'] == gold_id

    first_start, first_end = base + 120, base + 720
    client.clock[0] = float(first_end)
    first = client.request(
        f'/messages/{message_id}/evidence?start={first_start}&end={first_end}')
    first_check = client.request('/evidence/verify', first)
    first_counts = first_check['event_counts']
    assert first_check['ok'] is True
    assert first_counts['leased'] == 2
    assert first_counts['lease_deadline_reached'] == 1
    assert 'published' not in first_counts and 'acked' not in first_counts
    assert 'expired' not in first_counts
    deadline = next(event for event in first['trace']['events']
                    if event['kind'] == 'lease_deadline_reached')
    assert deadline['at'] == base + 480
    assert deadline['details']['delivery_id'] == blue_id
    assert deadline['details']['reclamation_recorded_in_window'] is False
    first_encoded = pack(first)
    assert secret not in first_encoded and 'audit-producer' not in first_encoded
    assert first['subject']['published_relation'] == 'before_window'
    assert any(item['kind'] == 'published' and item['time_relation'] == 'before_window'
               for item in first['outside_facts'])

    # 篡改窗内事实但保留旧摘要：接收端首先看到摘要不符。
    tampered_fact = copy.deepcopy(first)
    changed = False
    for event in tampered_fact['trace']['events']:
        if event.get('kind') == 'leased' and event.get('details', {}).get('worker') == 'blue-worker':
            event['details']['worker'] = 'forged-worker'
            changed = True
    assert changed
    tampered_check = client.request('/evidence/verify', tampered_fact)
    assert tampered_check['ok'] is False
    assert 'digest_mismatch' in _error_codes(tampered_check)

    # 把窗外发布改标为窗内且不重算摘要：同样摘要不符。
    mislabeled = copy.deepcopy(first)
    published_marker = next(item for item in mislabeled['outside_facts']
                           if item['kind'] == 'published')
    published_marker['time_relation'] = 'in_window'
    mislabeled_check = client.request('/evidence/verify', mislabeled)
    assert mislabeled_check['ok'] is False
    assert 'digest_mismatch' in _error_codes(mislabeled_check)

    # 即使改动者重算自摘要，时间口径结构检查仍能识别“窗外事实自称窗内”。
    resealed = copy.deepcopy(mislabeled)
    resealed['sha256'] = fingerprint(pack(
        {key: value for key, value in resealed.items() if key != 'sha256'}))
    resealed_check = client.request('/evidence/verify', resealed)
    assert resealed_check['integrity_valid'] is True
    assert resealed_check['time_scope_valid'] is False
    assert 'outside_fact_claimed_inside' in _error_codes(resealed_check)
    assert resealed_check['ok'] is False

    # 下一班：金组在第二个十分钟窗口内确认；蓝组到期租约到这时才实际回收。
    client.clock[0] = float(base + 735)
    settled = client.settle(gold)
    assert settled['state'] == 'acked'
    client.clock[0] = float(base + 900)
    swept = client.request('/sweep', {'limit': 10})
    assert [item['delivery_id'] for item in swept['recovered']] == [blue_id]

    second_start, second_end = base + 720, base + 1320
    client.clock[0] = float(second_end)
    second = client.request(
        f'/messages/{message_id}/evidence?start={second_start}&end={second_end}')
    second_check = client.request('/evidence/verify', second)
    second_counts = second_check['event_counts']
    assert second_check['ok'] is True
    assert second_counts.get('acked') == 1
    assert second_counts.get('expired') == 1
    assert 'leased' not in second_counts and 'published' not in second_counts
    assert 'lease_deadline_reached' not in second_counts
    second_encoded = pack(second)
    assert secret not in second_encoded and 'audit-producer' not in second_encoded
    assert first['subject']['envelope_commitment'] == second['subject']['envelope_commitment']
    assert first['subject']['message_id'] == second['subject']['message_id']

    return {
        'message_id': message_id,
        'first': first,
        'second': second,
        'first_check': first_check,
        'second_check': second_check,
        'tampered_fact': tampered_fact,
        'tampered_check': tampered_check,
        'mislabeled': mislabeled,
        'mislabeled_check': mislabeled_check,
        'resealed': resealed,
        'resealed_check': resealed_check,
    }


def _window_brief(document, check):
    return {
        'sha256': document['sha256'],
        'window': document['window'],
        'event_counts': check['event_counts'],
        'claims': check['claims'],
        'supported': check['supported'],
        'not_supported': check['not_supported'],
        'verify_ok': check['ok'],
    }


def window(client):
    scenario = window_scenario(client)
    return {
        'message_id': scenario['message_id'],
        'window_one': _window_brief(scenario['first'], scenario['first_check']),
        'window_two': _window_brief(scenario['second'], scenario['second_check']),
        'changed_in_window_fact': {
            'verify_ok': scenario['tampered_check']['ok'],
            'errors': _error_codes(scenario['tampered_check'])},
        'outside_event_mislabeled_with_old_digest': {
            'verify_ok': scenario['mislabeled_check']['ok'],
            'errors': _error_codes(scenario['mislabeled_check'])},
        'outside_event_mislabeled_and_resealed': {
            'verify_ok': scenario['resealed_check']['ok'],
            'integrity_valid': scenario['resealed_check']['integrity_valid'],
            'time_scope_valid': scenario['resealed_check']['time_scope_valid'],
            'errors': _error_codes(scenario['resealed_check']),
        },
    }


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, window=window)


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
