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


def publish_confirm(client):
    client.channel()
    matched_tags = {'feed': 'samples'}
    client.subscribe('alpha', matched_tags)
    client.subscribe('beta', matched_tags)

    seen_request = envelope('confirmed-set', tags=matched_tags)
    seen = client.request(CHANNEL + '/preview', seen_request)
    assert seen['groups'] == ['alpha', 'beta']

    # A subscription added after preview but which does not match the envelope
    # is irrelevant: the confirmed target set is still the current target set.
    client.subscribe('muted', {'feed': 'alerts'})
    confirmed_request = seen_request | {'expected_groups': seen['groups']}
    accepted = client.request(CHANNEL + '/messages', confirmed_request, 201)
    assert accepted['repeated'] is False
    assert list(accepted['fanout']) == ['alpha', 'beta']

    # Losing the 201 receipt must not create a second publication.  Even a new
    # matching subscription and the now-stale confirmed set yield the original
    # receipt and original scope.
    client.subscribe('late-match', {})
    resent = client.request(CHANNEL + '/messages', confirmed_request, 200)
    assert resent == accepted | {'repeated': True}
    token_retry = client.request(CHANNEL + '/messages',
                                 seen_request | {'confirmation_token': seen['confirmation_token']}, 200)
    assert token_retry == accepted | {'repeated': True}
    receipt_echo = client.request(CHANNEL + '/messages', seen_request | accepted, 200)
    assert receipt_echo == accepted | {'repeated': True}
    stored = client.request(f"/messages/{accepted['message_id']}")
    assert stored['message']['id'] == accepted['message_id']

    # A disjoint confirmed/current set is rejected atomically with a structured,
    # reviewable diff.  Use a separate channel so the example starts from four
    # deliberately disjoint north/south subscriptions.
    drift_channel = '/channels/drift-gate'
    client.request('/channels', {'name': 'drift-gate', 'policy': POLICY}, 201)
    for name, region in (('north-one', 'north'), ('north-two', 'north'),
                         ('south-one', 'south'), ('south-two', 'south')):
        client.request(drift_channel + '/groups',
                       {'name': name, 'tags': {'region': region}}, 201)
    drift_request = envelope('drift-set', tags={'region': 'north'})
    drift_seen = client.request(drift_channel + '/preview', drift_request)
    assert drift_seen['groups'] == ['north-one', 'north-two']
    other_view = client.request(
        drift_channel + '/preview', envelope('drift-other-view', tags={'region': 'south'}))
    assert other_view['groups'] == ['south-one', 'south-two']
    mismatch = client.request(drift_channel + '/messages',
                              drift_request | {'expected_groups': other_view['groups']}, 409)
    assert mismatch['error'] == 'conflict'
    assert mismatch['expected_groups'] == ['south-one', 'south-two']
    assert mismatch['actual_groups'] == ['north-one', 'north-two']
    assert mismatch['diff'] == {
        'added': ['north-one', 'north-two'],
        'removed': ['south-one', 'south-two'],
    }
    assert client.request(drift_channel)['messages'] == []

    # A real post-preview change can turn an empty observed set into two matches.
    fresh_request = envelope('fresh-set', tags={'wave': 'fresh'})
    assert client.request(drift_channel + '/preview', fresh_request)['groups'] == []
    client.request(drift_channel + '/groups',
                   {'name': 'fresh-one', 'tags': {'wave': 'fresh'}}, 201)
    client.request(drift_channel + '/groups',
                   {'name': 'fresh-two', 'tags': {'wave': 'fresh'}}, 201)
    fresh_conflict = client.request(
        drift_channel + '/messages', fresh_request | {'expected_groups': []}, 409)
    assert fresh_conflict['diff'] == {'added': ['fresh-one', 'fresh-two'], 'removed': []}

    reviewed = client.request(drift_channel + '/preview', drift_request)
    republished = client.request(
        drift_channel + '/messages',
        drift_request | {'expected_groups': reviewed['groups']}, 201)
    assert list(republished['fanout']) == ['north-one', 'north-two']

    return {
        'new_nonmatching_subscription': {
            'published': True,
            'fanout': accepted['fanout'],
        },
        'drastically_different_matching_set': mismatch['diff'],
        'lost_receipt_resend': {
            'message_id': resent['message_id'],
            'repeated': resent['repeated'],
            'fanout': resent['fanout'],
            'retrievable': stored['message']['id'] == accepted['message_id'],
        },
        # Compact compatibility aliases used by older local inspection.
        'unrelated_addition': accepted['fanout'],
        'lost_receipt_replay': resent['message_id'],
        'disjoint_diff': mismatch['diff'],
        'changed_empty_diff': fresh_conflict['diff'],
        'reviewed_publication': republished['fanout'],
    }


confirm = publish_confirm
confirmation = publish_confirm
publish_confirmation = publish_confirm


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


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, confirm=publish_confirm,
             confirmation=publish_confirm, publish_confirm=publish_confirm,
             publish_confirmation=publish_confirm,
             **{'publish-confirm': publish_confirm,
                'publish-confirmation': publish_confirm},
             publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
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
