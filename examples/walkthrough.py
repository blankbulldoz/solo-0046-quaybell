"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import logging
import threading

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


def review(client):
    """409 review: resend the old alert vs. confirm a new candidate key.

    Covers two differently labelled alerts taking the same candidate new key
    concurrently while the original alert is pruned in between, plus the
    envelope/destination/receipt binding and the no-silent-publish rule.
    """
    client.channel()
    client.subscribe('alpha', tags={'kind': 'alpha'})
    client.subscribe('beta', tags={'kind': 'beta'})
    original = client.publish('clash-key', tags={'kind': 'alpha'})

    # Identical republish and the explicit resend route both keep the receipt.
    assert client.publish('clash-key', expected=200, tags={'kind': 'alpha'})['message_id'] \
        == original['message_id']
    code, resent = client.call(f"/messages/{original['message_id']}/resend", {})
    assert (code, resent['message_id'], resent['repeated']) == (200, original['message_id'], True)

    alert_a = {'producer': 'signal-source', 'key': 'clash-key', 'lane': 'coil-north',
               'payload': {'alert': 'A'}, 'tags': {'kind': 'alpha'}}
    alert_b = alert_a | {'payload': {'alert': 'B'}, 'tags': {'kind': 'beta'}}
    code, conflict_a = client.call(CHANNEL + '/messages', alert_a)
    assert code == 409 and conflict_a['review']['original']['message_id'] == original['message_id']
    code, conflict_b = client.call(CHANNEL + '/messages', alert_b)
    assert code == 409
    # Two simultaneous reviews are non-reserving, so they suggest the same key.
    assert conflict_a['review']['suggested_key'] == \
        conflict_b['review']['suggested_key'] == 'clash-key-2'

    code, candidate_a = client.call(CHANNEL + '/candidates',
                                    alert_a | {'candidate_key': 'clash-key-2'})
    code2, candidate_b = client.call(CHANNEL + '/candidates',
                                     alert_b | {'candidate_key': 'clash-key-2'})
    assert (code, code2) == (201, 201)
    assert candidate_a['destinations'] == ['alpha']
    assert candidate_b['destinations'] == ['beta']

    # Pending candidates create no messages or deliveries.
    assert [m['dedup_key'] for m in client.request(CHANNEL)['messages']] == ['clash-key']

    # Ack and prune the original while both candidates wait unconfirmed.
    taken = client.request(CHANNEL + '/groups/alpha/claim', {'worker': 'review', 'limit': 1})['items'][0]
    client.settle(taken)
    client.tick(11)
    pruned = client.request(CHANNEL + '/retention', {})
    assert pruned['removed'] == [original['message_id']]
    assert client.request(CHANNEL + f"/candidates/{candidate_a['candidate_id']}")['original_state'] \
        == 'pruned'

    results = {}
    barrier = threading.Barrier(2)

    def confirm(name, candidate_id):
        barrier.wait()
        results[name] = client.call(CHANNEL + f"/candidates/{candidate_id}/confirm", {})

    threads = [threading.Thread(target=confirm, args=('A', candidate_a['candidate_id'])),
               threading.Thread(target=confirm, args=('B', candidate_b['candidate_id']))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(code for code, _ in results.values()) == [201, 409], results
    winner = 'A' if results['A'][0] == 201 else 'B'
    loser = 'B' if winner == 'A' else 'A'
    receipt = results[winner][1]
    assert list(receipt['fanout']) == [{'A': 'alpha', 'B': 'beta'}[winner]]

    # Repeated confirms, including concurrent ones, replay the stable receipt.
    code, again = client.call(CHANNEL + f"/candidates/{receipt['candidate_id']}/confirm", {})
    assert code == 200 and again['repeated'] is True
    assert again['message_id'] == receipt['message_id'] and again['fanout'] == receipt['fanout']
    replay = {}
    replay_barrier = threading.Barrier(2)

    def replay_one(index):
        replay_barrier.wait()
        replay[index] = client.call(f"/candidates/{receipt['candidate_id']}/confirm",
                                    {'fingerprint': receipt['fingerprint']})

    replays = [threading.Thread(target=replay_one, args=(i,)) for i in range(2)]
    for thread in replays:
        thread.start()
    for thread in replays:
        thread.join()
    assert all(code == 200 and body['message_id'] == receipt['message_id']
               for code, body in replay.values()), replay

    # The loser re-reviews the fresh suggestion and confirms under a new key.
    loser_id = candidate_a['candidate_id'] if loser == 'A' else candidate_b['candidate_id']
    code, lost = results[loser]
    assert code == 409 and lost['suggested_key'] == 'clash-key-3'
    code, recovered = client.call(CHANNEL + f"/candidates/{loser_id}/confirm",
                                  {'key': lost['suggested_key']})
    assert code == 201 and list(recovered['fanout']) == [{'A': 'alpha', 'B': 'beta'}[loser]]
    code, repeated_recovery = client.call(CHANNEL + f"/candidates/{loser_id}/confirm", {})
    assert code == 200 and repeated_recovery['message_id'] == recovered['message_id']

    # A fingerprint mismatch blocks confirmation; an abandoned candidate never publishes.
    code, orphan = client.call(CHANNEL + '/candidates', envelope('orphan-key') | {'candidate_key': 'unused-key'})
    assert code == 201
    code, mismatch = client.call(CHANNEL + f"/candidates/{orphan['candidate_id']}/confirm",
                                 {'fingerprint': '0' * 64})
    assert code == 409 and mismatch['reason'] == 'envelope_mismatch'
    board_keys = [m['dedup_key'] for m in client.request(CHANNEL)['messages']]
    assert 'unused-key' not in board_keys
    code, discarded = client.call(CHANNEL + f"/candidates/{orphan['candidate_id']}/discard", {})
    assert code == 200 and discarded['status'] == 'discarded'
    return {'winner': winner, 'receipt': receipt['message_id'],
            'recovered': recovered['message_id'], 'pruned': pruned['removed']}


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


FLOWS = dict(channel=channel, revision=revision, group=group, preview=preview, publish=publish,
             review=review, conflict=review, republish=review, candidates=review,
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
