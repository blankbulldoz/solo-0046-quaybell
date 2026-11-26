"""Run one bounded middleware workflow through the public HTTP listener."""
import argparse
import logging
import threading
import time

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


def wait_active(client, group, count=1):
    """Wait until HTTP wait requests have released their DB transactions."""
    hub = getattr(getattr(client, 'server', None), 'events', None)
    if hub is None:
        time.sleep(0.15)
        return
    key = f'wire-gate/{group}'
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if hub.active_count(key) >= count:
            return
        time.sleep(0.01)
    raise AssertionError(f'等待者未进入等待: {key} {hub.active_count(key)}')


def wait(client):
    client.channel()
    client.subscribe()

    # A delayed delivery becomes ready when the (controlled) clock reaches its
    # availability instant.  Wait a real second before advancing that clock so
    # the workflow also reads as a genuine time-to-readiness demonstration.
    timed = client.publish('timer-key', 'coil-timer', delay_seconds=1)

    def advance_clock():
        time.sleep(1.0)
        client.tick(1)

    threading.Thread(target=advance_clock, daemon=True).start()
    started = time.monotonic()
    timed_reply = client.request(GROUP + '/claim',
                                 {'worker': 'timer-watcher', 'wait_seconds': 3})
    time_elapsed = time.monotonic() - started
    assert [item['message_id'] for item in timed_reply['items']] == [timed['message_id']]
    assert 0.8 <= time_elapsed < 2.0

    # A leased head blocks its ordering key; acking it wakes the waiting poll.
    head_publication = client.publish('ack-head', 'coil-order')
    tail_publication = client.publish('ack-tail', 'coil-order')
    head_lease = client.claim(1, worker='foreground-worker')[0]
    box = {}
    waiter = threading.Thread(target=lambda: box.update(
        reply=client.request(GROUP + '/wait',
                             {'worker': 'ack-watcher', 'max_wait_seconds': 5})), daemon=True)
    waiter.start()
    wait_active(client, 'oscillator')

    started = time.monotonic()
    background = client.publish('publish-while-waiting', 'coil-background', delay_seconds=60)
    publish_elapsed = time.monotonic() - started
    assert background['message_id']

    started = time.monotonic()
    ack_receipt = client.settle(head_lease)
    ack_elapsed = time.monotonic() - started
    waiter.join(2)
    assert not waiter.is_alive() and 'error' not in box
    assert ack_receipt['state'] == 'acked'
    assert [item['message_id'] for item in box['reply']['items']] == [tail_publication['message_id']]
    assert publish_elapsed < 1 and ack_elapsed < 1

    # Publication also wakes a separate idle poll immediately.
    client.subscribe('publisher-watch')
    publication_box = {}
    publication_thread = threading.Thread(target=lambda: publication_box.update(
        reply=client.request(CHANNEL + '/groups/publisher-watch/poll',
                             {'worker': 'publisher-watcher', 'wait_seconds': 3})), daemon=True)
    publication_thread.start()
    wait_active(client, 'publisher-watch')
    started = time.monotonic()
    woken = client.publish('wake-by-publish', 'coil-publish-wake')
    publication_thread.join(2)
    publish_wake_elapsed = time.monotonic() - started
    assert not publication_thread.is_alive() and 'error' not in publication_box
    assert [item['message_id'] for item in publication_box['reply']['items']] == [woken['message_id']]
    assert publish_wake_elapsed < 1

    # Pause must broadcast to and return every waiting poll in that group.
    client.subscribe('pause-watch')
    pause_box = {}
    pause_thread = threading.Thread(target=lambda: pause_box.update(
        reply=client.request(CHANNEL + '/groups/pause-watch/wait',
                             {'worker': 'pause-watcher', 'wait_seconds': 5})), daemon=True)
    pause_thread.start()
    wait_active(client, 'pause-watch')
    started = time.monotonic()
    client.request(CHANNEL + '/groups/pause-watch/pause', {'paused': True})
    pause_thread.join(2)
    pause_elapsed = time.monotonic() - started
    assert not pause_thread.is_alive() and 'error' not in pause_box
    assert pause_box['reply'] == {'paused': True, 'items': []}
    assert pause_elapsed < 1

    # Eight waiters and three independent keys: every lease is distinct.
    client.subscribe('crowd')
    waiters = 8
    replies = [None] * waiters
    errors = []

    def competing(index):
        try:
            replies[index] = client.request(CHANNEL + '/groups/crowd/claim',
                                            {'worker': f'crowd-{index}', 'limit': 1,
                                             'wait_seconds': 2})
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=competing, args=(index,), daemon=True)
               for index in range(waiters)]
    for thread in threads:
        thread.start()
    wait_active(client, 'crowd', waiters)
    expected = set()
    for index in range(3):
        publication = client.publish(f'crowd-key-{index}', f'coil-crowd-{index}')
        expected.add(publication['fanout']['crowd'])
    time.sleep(0.2)
    client.tick(2)
    for thread in threads:
        thread.join(3)
    assert not errors and not any(thread.is_alive() for thread in threads)
    leased = [item for reply in replies for item in reply['items']]
    delivery_ids = [item['delivery_id'] for item in leased]
    lease_tokens = [item['lease']['token'] for item in leased]
    assert len(leased) == 3
    assert len(delivery_ids) == len(set(delivery_ids)) == 3
    assert len(lease_tokens) == len(set(lease_tokens)) == 3
    assert set(delivery_ids) == expected

    return {
        'time_delivery': timed['message_id'],
        'time_wake_seconds': round(time_elapsed, 3),
        'ack_unlocked_delivery': tail_publication['message_id'],
        'publish_while_waiting_seconds': round(publish_elapsed, 3),
        'ack_while_waiting_seconds': round(ack_elapsed, 3),
        'publish_wake_seconds': round(publish_wake_elapsed, 3),
        'pause_seconds': round(pause_elapsed, 3),
        'competing_leases': sorted(delivery_ids),
    }


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
             inspect=inspect, pause=pause, claim=claim, wait=wait, longpoll=wait, **{'long-poll': wait},
             poll=wait, waiting=wait, ack=ack, retry=retry, sweep=sweep, dead=dead,
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
