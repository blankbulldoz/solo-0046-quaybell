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


def takeover(client):
    client.channel()
    old_group, new_group = 'oscillator', 'oscillator-v2'
    client.subscribe(old_group)

    alpha_head = client.publish('takeover-alpha-head', 'takeover-alpha')
    alpha_tail = client.publish('takeover-alpha-tail', 'takeover-alpha')
    beta_done = client.publish('takeover-beta-done', 'takeover-beta')
    beta_next = client.publish('takeover-beta-next', 'takeover-beta')
    old_ids = {
        'alpha_head': alpha_head['fanout'][old_group],
        'alpha_tail': alpha_tail['fanout'][old_group],
        'beta_done': beta_done['fanout'][old_group],
        'beta_next': beta_next['fanout'][old_group],
    }

    # The two lane heads are leased together.  Finish beta's head so its next
    # delivery becomes an acknowledged-predecessor + pending state before cutover.
    heads = client.claim(2)
    assert [item['delivery_id'] for item in heads] == [
        old_ids['alpha_head'], old_ids['beta_done']]
    alpha_lease = next(item for item in heads if item['delivery_id'] == old_ids['alpha_head'])
    beta_lease = next(item for item in heads if item['delivery_id'] == old_ids['beta_done'])
    assert client.settle(beta_lease)['state'] == 'acked'
    client.request(GROUP + '/pause', {'paused': True})
    client.subscribe(new_group)

    before = client.request(GROUP + '/takeover/preview')
    before_by_state = before['ownership']['before']['by_state']
    assert before_by_state == {
        'leased': [old_ids['alpha_head']],
        'waiting': [old_ids['alpha_tail'], old_ids['beta_next']],
        'acked': [old_ids['beta_done']],
    }

    payload = {'new_group': new_group, 'takeover_id': 'rollout-r009'}
    changed = client.request(GROUP + '/takeover', payload, 201)
    assert changed['repeated'] is False
    assert changed['summary']['transferred'] == 3
    assert changed['summary']['acknowledged'] == 1
    assert changed['summary']['leases_terminated'] == 1

    mapping = {row['old_delivery_id']: row['new_delivery_id'] for row in changed['mappings']}
    assert set(mapping) == {
        old_ids['alpha_head'], old_ids['alpha_tail'], old_ids['beta_next']}
    assert old_ids['beta_done'] not in mapping
    new_alpha_head = mapping[old_ids['alpha_head']]
    new_alpha_tail = mapping[old_ids['alpha_tail']]
    new_beta_next = mapping[old_ids['beta_next']]

    # The old token arrives after the takeover fence.  The expected=409 argument
    # makes the HTTP status part of this bounded workflow.
    late_ack = client.settle(alpha_lease, expected=409)
    assert late_ack['reason'] == 'lease_superseded'
    assert late_ack['new_delivery_id'] == new_alpha_head
    late_ack_retry = client.settle(alpha_lease, expected=409)
    assert late_ack_retry['reason'] == 'lease_superseded'

    # Losing the first response and repeating the same takeover request returns
    # the ledger: no duplicate deliveries and no second fence.
    repeated = client.request(GROUP + '/takeover', payload, 200)
    assert repeated['repeated'] is True and repeated['retry_count'] == 1
    repeated_mapping = {row['old_delivery_id']: row['new_delivery_id']
                        for row in repeated['mappings']}
    assert repeated_mapping == mapping
    assert repeated['summary']['newly_transferred'] == 0
    assert repeated['summary']['target_delivery_ids'] == [
        new_alpha_head, new_alpha_tail, new_beta_next]
    client.request(GROUP + '/pause', {'paused': False})
    assert client.claim(group=old_group) == []

    # Both independent lane heads are claimable.  Alpha's copied successor stays
    # blocked until the copied alpha head is acknowledged.
    first_new = client.claim(2, group=new_group)
    assert [item['delivery_id'] for item in first_new] == [new_alpha_head, new_beta_next]
    for item in first_new:
        client.settle(item)
    successor = client.claim(1, group=new_group)[0]
    assert successor['delivery_id'] == new_alpha_tail
    client.settle(successor)

    reconciled = client.request(GROUP + '/takeover', payload, 200)
    target_states = {item['new_delivery_id']: item['state_after']
                     for item in reconciled['items']
                     if item['disposition'] == 'transferred'}
    assert set(target_states.values()) == {'acked'}

    compact_before_after = [{
        'lane': item['lane'],
        'before': {'owner': item['before']['owner'], 'id': item['before']['delivery_id'],
                   'state': item['before']['state']},
        'immediate_after': {'owner': item['after']['owner'], 'id': item['after']['delivery_id'],
                            'state': item['after']['state']},
        'disposition': item['disposition'],
    } for item in changed['items']]
    compact_final = [{
        'lane': item['lane'],
        'before': {'owner': item['before']['owner'], 'id': item['before']['delivery_id'],
                   'state': item['before']['state']},
        'after': {'owner': item['after']['owner'], 'id': item['after']['delivery_id'],
                  'state': item['after']['state']},
        'disposition': item['disposition'],
    } for item in reconciled['items']]
    return {
        'old_ids': old_ids,
        'before': before_by_state,
        'ownership_at_takeover': compact_before_after,
        'items': compact_final,
        'mapping': mapping,
        'late_ack': {'http_status': 409, 'error': late_ack['error'],
                     'reason': late_ack['reason'],
                     'new_delivery_id': late_ack['new_delivery_id']},
        'retry': {'repeated': repeated['repeated'], 'retry_count': repeated['retry_count'],
                  'newly_transferred': repeated['summary']['newly_transferred'],
                  'duplicate_copies_created': repeated['summary']['duplicate_copies_created']},
        'new_claim_order': [new_alpha_head, new_beta_next, new_alpha_tail],
        'final_target_states': target_states,
    }


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             inspect=inspect, pause=pause, claim=claim, ack=ack, retry=retry, sweep=sweep, dead=dead,
             redrive=redrive, board=board, retention=retention, export=export, takeover=takeover,
             handoff=takeover, handover=takeover, migrate=takeover, migration=takeover,
             cutover=takeover, switchover=takeover, consumer_takeover=takeover,
             **{'consumer-takeover': takeover, 'take-over': takeover})

handoff = takeover
handover = takeover
migrate = takeover
migration = takeover
cutover = takeover
switchover = takeover
consumer_takeover = takeover


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
