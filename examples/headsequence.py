"""Cross-check the claim path and the board path on one same-key head.

Sequence for the head delivery of an order key (lane):

1. valid lease            -> board ``leased``, successor ``blocked``;
2. past deadline, unrepaped -> board ``expired``, successor still ``blocked``,
   and a board GET must not reap (read boundary);
3. genuinely reclaimed     -> claim's write path reaps first: backoff makes
   the head ``delayed`` while the successor stays ``blocked``; once the
   backoff elapses the head is claimable again. Exhausting the budget makes
   it ``dead`` and promotes the successor.

Run: python3 -m examples.headsequence
"""
import logging

from quaybell.codec import pack

from .session import CHANNEL, GROUP, session


def _board_group(client):
    return next(group for group in client.request(CHANNEL)['groups'] if group['name'] == 'oscillator')


def _causes(client):
    group = _board_group(client)
    sample = {item['id']: item for item in group['sample']}
    return {key: item['cause'] for key, item in sample.items()}, group['counts'], sample


def headsequence(client):
    client.channel()
    client.subscribe()
    head = client.publish('head-key', 'coil-north')
    tail = client.publish('tail-key', 'coil-north')
    aside = client.publish('aside-key', 'coil-south')
    H = head['fanout']['oscillator']
    T = tail['fanout']['oscillator']
    A = aside['fanout']['oscillator']

    # claim keys are bounded: limit is an int in 1..30
    client.request(GROUP + '/claim', {'worker': 'w', 'limit': 31}, 400)

    # --- phase 1: head holds a valid lease --------------------------------
    taken = client.claim(1)
    assert [item['delivery_id'] for item in taken] == [H]
    causes, counts, sample = _causes(client)
    assert causes[H] == 'leased' and causes[T] == 'blocked' and causes[A] == 'ready', causes
    assert sample[T]['head_id'] == H and sample[H]['head_id'] == H
    assert counts == dict(leased=1, blocked=1, ready=1), counts

    # --- phase 2: past deadline, not reaped -------------------------------
    client.tick(5)  # lease_seconds=4, so expires == T0+4 is in the past
    causes, counts, sample = _causes(client)
    assert causes[H] == 'expired' and causes[T] == 'blocked' and causes[A] == 'ready', causes
    assert counts == dict(expired=1, blocked=1, ready=1), counts
    # board is a read snapshot: merely GET-ing it must not reclaim the head
    trace = client.request(f"/messages/{head['message_id']}")
    assert trace['deliveries'][0]['state'] == 'leased'

    # --- phase 3a: claim write path genuinely reaps, then backs off -------
    picked = client.claim(3)
    assert [item['delivery_id'] for item in picked] == [A], picked
    client.settle(picked[0])
    causes, counts, _ = _causes(client)
    assert causes[H] == 'delayed' and causes[T] == 'blocked', causes
    assert counts == dict(delayed=1, blocked=1), counts

    # backoff origin is the old deadline (T0+4) + retry_seconds(3) = T0+7
    client.tick(2)
    causes, _, _ = _causes(client)
    assert causes[H] == 'ready', causes
    second = client.claim(1)[0]
    assert second['delivery_id'] == H and second['lease']['number'] == 2

    # --- phase 3b: second expiry reclaims into dead; tail promoted --------
    client.tick(5)
    causes, _, _ = _causes(client)
    assert causes[H] == 'expired' and causes[T] == 'blocked', causes
    third = client.claim(1)[0]  # group reap kills H (max_attempts=2), then T is ready
    assert third['delivery_id'] == T and third['lease']['number'] == 1, third
    causes, counts, sample = _causes(client)
    assert H not in causes and causes[T] == 'leased', causes
    assert sample[T]['head_id'] == T and counts == dict(leased=1), counts
    dead = client.request(CHANNEL + '/dead')['items']
    assert len(dead) == 1 and dead[0]['id'] == H
    assert dead[0]['last_attempt']['outcome'] == 'expired'

    client.settle(third)
    assert _board_group(client)['counts'] == {}
    assert client.claim() == []
    return {'head': H, 'successor': T, 'aside': A, 'dead': H}


def main():
    logging.getLogger().setLevel(logging.WARNING)
    with session() as client:
        result = headsequence(client)
    print(f'headsequence 通过 {pack(result)}')


if __name__ == '__main__':
    main()
