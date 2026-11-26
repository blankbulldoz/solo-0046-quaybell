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


def audit(client):
    # A historical publication before audit credentials were enabled.
    client.request('/channels', {
        'name': 'wire-gate', 'policy': POLICY,
        'audit': {'enabled': False, 'retention_seconds': 3600},
    }, 201)
    legacy = client.publish('legacy-before-audit', tags={'era': 'pre-audit'})

    # Later publications have a sealed snapshot of tags and current group rules.
    client.request(CHANNEL + '/audit/config',
                   {'enabled': True, 'retention_seconds': 3600}, 201)
    client.subscribe('rule-red', {'color': 'red'})
    matched = client.publish('explained-match', tags={'color': 'red'})
    old_zero = client.publish('released-key', tags={'color': 'blue'})

    # Disposition lets short retention remove every original message/delivery.
    taken = client.request(CHANNEL + '/groups/rule-red/claim',
                           {'worker': 'audit-worker', 'limit': 1})['items'][0]
    client.request(f"/deliveries/{taken['delivery_id']}/ack",
                   {'token': taken['lease']['token']})
    client.tick(11)
    pruned = client.request(CHANNEL + '/retention', {})
    assert set([legacy['message_id'], matched['message_id'], old_zero['message_id']]) \
        <= set(pruned['removed'])

    # These groups are intentionally created after the original publication.
    client.subscribe('rule-blue-later', {'color': 'blue'})
    client.subscribe('rule-green-new', {'route': 'green'})

    # The production key is reusable after pruning.  This is a genuinely new
    # envelope and therefore must not be treated as a repeated receipt.
    new_hit = client.publish('released-key', expected=201,
                            payload={'revision': 'new-envelope'}, tags={'route': 'green'})
    assert new_hit['repeated'] is False
    assert list(new_hit['fanout']) == ['rule-green-new']

    # A fresh preview of the OLD tags is contaminated by the later blue rule.
    preview_old = client.request(CHANNEL + '/preview',
                                 envelope('preview-only', tags={'color': 'blue'}))
    assert preview_old['groups'] == ['rule-blue-later']

    legacy_audit = client.request(f"/messages/{legacy['message_id']}/audit")
    matched_audit = client.request(f"/messages/{matched['message_id']}/audit")
    zero_audit = client.request(f"/messages/{old_zero['message_id']}/audit")
    new_audit = client.request(f"/messages/{new_hit['message_id']}/audit")

    assert legacy_audit['status'] == 'audit_credential_not_enabled'
    assert legacy_audit['available'] is False
    assert legacy_audit['routing_provable'] is False
    assert legacy_audit['prune_record'] is not None
    assert legacy_audit['tags'] is None

    assert matched_audit['status'] == 'verified' and matched_audit['digest_matches']
    assert matched_audit['original_receipt'] == matched
    assert matched_audit['matched_groups'] == ['rule-red']
    assert matched_audit['original_receipt']['fanout'] == {'rule-red': 1}

    assert zero_audit['status'] == 'verified' and zero_audit['digest_matches']
    assert zero_audit['original_receipt'] == old_zero
    assert zero_audit['matched_groups'] == []
    assert zero_audit['original_receipt']['fanout'] == {}
    original_rules = {item['group'] for item in zero_audit['routing_basis']['groups_at_publication']}
    assert original_rules == {'rule-red'}
    assert zero_audit['current_topology']['current_preview_groups'] == ['rule-blue-later']
    assert zero_audit['current_topology']['would_include_later_groups'] is True
    assert set(zero_audit['current_topology']['groups_created_after_publication']) == \
        {'rule-blue-later', 'rule-green-new'}

    assert new_audit['matched_groups'] == ['rule-green-new']

    by_key = client.request(CHANNEL + '/forensics',
                            {'producer': 'signal-source', 'key': 'released-key'})
    assert [item['message_id'] for item in by_key['records']] == \
        [old_zero['message_id'], new_hit['message_id']]
    assert [item['matched_groups'] for item in by_key['records']] == [[], ['rule-green-new']]

    by_get = client.request(CHANNEL + '/forensics?key=released-key')
    assert by_get['count'] == 2
    by_period = client.request(CHANNEL + '/forensics', {
        'start': 1_730_000_000, 'end': 1_730_000_100,
    })
    assert {item['message_id'] for item in by_period['records']} == \
        {legacy['message_id'], matched['message_id'], old_zero['message_id'], new_hit['message_id']}

    credential = client.request(f"/audit-receipts/{zero_audit['audit_id']}")
    assert credential['status'] == 'verified' and credential['sha256'] == zero_audit['sha256']
    evidence = client.request(f"/messages/{old_zero['message_id']}/evidence")
    assert evidence['format'] == 'quaybell-publication-audit/1'
    return {
        'pruned': pruned['removed'],
        'released_key_generations': [old_zero['message_id'], new_hit['message_id']],
        'old_zero_match_count': 0,
        'new_fanout': new_hit['fanout'],
        'legacy_status': legacy_audit['status'],
        'audit_period_count': by_period['count'],
    }


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, audit=audit,
             forensics=audit, forensic=audit,
             **{'receipt-audit': audit, 'publication-audit': audit})


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
