"""Repeatable acceptance for the lease-deadline concurrency contract.

Run locally with no third-party packages::

    python3 -m unittest discover -s tests -v

Every scenario drives a real HTTP relay with a controlled clock and uses only
the public listener (the same surface ``examples.walkthrough`` uses).  For one
head delivery with a same-lane successor the suite covers the three requested
inputs:

``before``  now = E-1 (strictly before the deadline)
``at``      now = E   (the documented inclusive expiry edge)
``after``   now > E with a fresh lease already reissued for attempt 2

Assertions are split so that a reviewer can see which observables are fixed
and which are allowed to vary with scheduling; the classification lives in
``tests/contract.py`` and is checked for consistency at the end of the run.
"""
import unittest

from tests.contract import CONTRACT, by_kind
from tests.deadline_support import (
    BASE, ack, at, attempt_rows, claim, contended, delivery_view, journal_kinds,
    nack, open_lease, relay, settle, sweep, trace,
)


class DeadlineBase(unittest.TestCase):
    """Shared helpers; each test opens its own isolated relay and database."""

    def relay(self):
        manager = relay()
        client = manager.__enter__()
        self.addCleanup(manager.__exit__, None, None, None)
        return client

    def head_lease(self, client):
        head, successor, item = open_lease(client)
        return head, successor, item, item['lease']['expires']

    def assert_first_attempt(self, client, head, item, outcome):
        rows = [r for r in attempt_rows(client, head['message_id'], item['delivery_id'])
                if r['number'] == 1]
        self.assertEqual(len(rows), 1, rows)
        self.assertEqual(rows[0]['outcome'], outcome)
        if outcome != 'open':
            self.assertIsNotNone(rows[0]['finished'])
        return rows[0]


class BoundaryInputs(DeadlineBase):
    """The three explicit inputs, one focused test each, no concurrency."""

    def test_before_deadline_ack_is_accepted_and_successor_waits(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires - 1)

        status, result = ack(client, item)
        self.assertEqual(status, 'accepted')
        self.assertEqual((result['attempt'], result['state']), (1, 'acked'))
        self.assertFalse(result['repeated'])
        self.assertEqual(result['effective_at'], expires - 1)

        # The old token cannot declare a second outcome: it replays the receipt.
        status, repeat = ack(client, item)
        self.assertEqual(status, 'accepted')
        self.assertTrue(repeat['repeated'])
        self.assertEqual(
            {k: v for k, v in repeat.items() if k != 'repeated'},
            {k: v for k, v in result.items() if k != 'repeated'},
        )

        # Only now may the same-lane successor be handed out, as attempt 1.
        items = claim(client, 3)
        self.assertEqual([i['message_id'] for i in items], [successor['message_id']])
        self.assertEqual(items[0]['lease']['number'], 1)

    def test_ack_at_deadline_is_rejected(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires)

        status, error = ack(client, item)
        self.assertEqual((status, error), ('rejected', 'conflict'))

        # A rejected ACK is a pure no-op on the attempt ledger.
        self.assertEqual(
            [(r['number'], r['outcome']) for r in
             attempt_rows(client, head['message_id'], item['delivery_id'])],
            [(1, 'open')],
        )

        # Backoff has not elapsed at E, nor at E+2; the retry is attempt 2 at E+3.
        self.assertEqual(claim(client, 3), [])
        at(client, expires + 2)
        self.assertEqual(claim(client, 3), [])
        at(client, expires + 3)
        items = claim(client, 3)
        self.assertEqual([(i['delivery_id'], i['lease']['number']) for i in items],
                         [(item['delivery_id'], 2)])
        self.assert_first_attempt(client, head, item, 'expired')

    def test_after_reap_new_lease_blocks_old_token(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 1)

        recovered = sweep(client)
        self.assertEqual(
            [(r['delivery_id'], r['attempt'], r['outcome']) for r in recovered],
            [(item['delivery_id'], 1, 'expired')],
        )
        self.assertEqual(recovered[0]['effective_at'], expires + 3)  # deadline + backoff

        at(client, expires + 3)
        fresh = claim(client, 3)
        self.assertEqual(len(fresh), 1)
        self.assertEqual((fresh[0]['delivery_id'], fresh[0]['lease']['number']),
                         (item['delivery_id'], 2))
        self.assertNotEqual(fresh[0]['lease']['token'], item['lease']['token'])

        # The old credential is now stale behind a live lease.
        status, error = ack(client, item)
        self.assertEqual((status, error), ('rejected', 'conflict'))
        status, error = nack(client, item)
        self.assertEqual((status, error), ('rejected', 'conflict'))

        self.assertEqual(
            [(r['number'], r['outcome']) for r in
             attempt_rows(client, head['message_id'], item['delivery_id'])],
            [(1, 'expired'), (2, 'open')],
        )
        # The old rejection must not disturb the new open lease.
        self.assertEqual(claim(client, 3), [])
        self.assertEqual(journal_kinds(client, head['message_id']),
                         ['published', 'leased', 'expired', 'leased'])


class NoDualSuccessOrTimeout(DeadlineBase):
    """ACK and recovery interleaved in either order around the boundary."""

    def assert_no_dual(self, client, head, item):
        rows = attempt_rows(client, head['message_id'], item['delivery_id'])
        first = [r for r in rows if r['number'] == 1]
        self.assertEqual(len(first), 1, rows)
        self.assertEqual(len({r['outcome'] for r in first}), 1)
        view = delivery_view(client, head['message_id'], item['delivery_id'])
        kinds = journal_kinds(client, head['message_id'])
        if first[0]['outcome'] == 'acked':
            self.assertEqual(view['state'], 'acked')
            self.assertNotIn('expired', kinds)
        else:
            self.assertEqual(first[0]['outcome'], 'expired')
            self.assertNotIn('acked', kinds)
            self.assertIn(view['state'], ('waiting', 'leased', 'dead'))
        return first[0]

    def test_ack_then_late_sweep_does_not_time_out(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires - 1)
        status, _ = ack(client, item)
        self.assertEqual(status, 'accepted')

        at(client, expires + 5)  # a much later sweep must not resurrect a timeout
        self.assertEqual(sweep(client), [])
        self.assert_no_dual(client, head, item)
        self.assertEqual(
            [r['outcome'] for r in attempt_rows(client, head['message_id'], item['delivery_id'])],
            ['acked'],
        )

    def test_late_sweep_then_ack_does_not_succeed(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 2)
        recovered = sweep(client)
        self.assertEqual([r['outcome'] for r in recovered], ['expired'])

        status, error = ack(client, item)
        self.assertEqual((status, error), ('rejected', 'conflict'))
        self.assert_no_dual(client, head, item)
        self.assertEqual(
            [r['outcome'] for r in attempt_rows(client, head['message_id'], item['delivery_id'])],
            ['expired'],
        )

    def test_claim_recovery_then_old_ack_rejected(self):
        # Recovery triggered by claim's pre-scan rather than by /sweep.
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 3)  # ready for the retry immediately after reaping
        items = claim(client, 3)
        self.assertEqual([(i['delivery_id'], i['lease']['number']) for i in items],
                         [(item['delivery_id'], 2)])

        status, error = ack(client, item)  # old token
        self.assertEqual((status, error), ('rejected', 'conflict'))
        status, result = ack(client, items[0])  # the live attempt settles cleanly
        self.assertEqual(status, 'accepted')
        self.assertEqual((result['attempt'], result['state']), (2, 'acked'))
        self.assertEqual(
            [(r['number'], r['outcome']) for r in
             attempt_rows(client, head['message_id'], item['delivery_id'])],
            [(1, 'expired'), (2, 'acked')],
        )

    def test_ack_at_deadline_then_claim_reaps_without_double_terminal(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires)
        status, error = ack(client, item)
        self.assertEqual((status, error), ('rejected', 'conflict'))
        at(client, expires + 3)
        items = claim(client, 3)
        self.assertEqual([(i['delivery_id'], i['lease']['number']) for i in items],
                         [(item['delivery_id'], 2)])
        self.assertEqual(
            [(r['number'], r['outcome']) for r in
             attempt_rows(client, head['message_id'], item['delivery_id'])],
            [(1, 'expired'), (2, 'open')],
        )
        self.assertEqual(journal_kinds(client, head['message_id']),
                         ['published', 'leased', 'expired', 'leased'])


class SuccessorOrdering(DeadlineBase):
    """A same-key successor never overtakes a head that is still outstanding."""

    def test_successor_never_overtakes_head(self):
        for delta, label in ((-1, 'before'), (0, 'at'), (1, 'after')):
            with self.subTest(boundary=label):
                client = self.relay()
                head, successor, item, expires = self.head_lease(client)
                at(client, expires + delta)
                # Never settle the head.  Whatever recovery happens, the only
                # delivery this lane may hand out is the head itself.
                for _ in range(4):
                    for got in claim(client, 30):
                        self.assertEqual(got['delivery_id'], item['delivery_id'],
                                         f'successor overtook at {label}')
                        self.assertNotEqual(got['message_id'], successor['message_id'])
                    at(client, client.clock[0] + 1)

    def test_successor_after_deadline_retry_lands_on_same_lane_tail(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 1)
        sweep(client)
        at(client, expires + 3)
        first = claim(client, 3)
        self.assertEqual([i['message_id'] for i in first], [head['message_id']])
        self.assertEqual(first[0]['lease']['number'], 2)
        self.assertEqual(claim(client, 3), [])  # lane still blocked by the retry
        ack(client, first[0])
        second = claim(client, 3)
        self.assertEqual([i['message_id'] for i in second], [successor['message_id']])
        self.assertEqual(second[0]['lease']['number'], 1)


class IdentityAndAttempts(DeadlineBase):
    """Identity facts that must hold after every interleaving."""

    def assert_identity(self, client, message_id, delivery_id, numbers):
        rows = attempt_rows(client, message_id, delivery_id)
        self.assertEqual([r['number'] for r in rows], list(range(1, numbers + 1)))
        self.assertLessEqual(sum(1 for r in rows if r['outcome'] == 'open'), 1, rows)
        self.assertEqual({r['delivery_id'] for r in rows}, {delivery_id})

    def test_attempt_number_is_monotonic_and_unique(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires)
        ack(client, item)  # rejected: already at the deadline
        self.assert_identity(client, head['message_id'], item['delivery_id'], 1)

        at(client, expires + 3)
        second = claim(client, 3)[0]
        self.assert_identity(client, head['message_id'], item['delivery_id'], 2)
        self.assertEqual(second['lease']['number'], 2)
        ack(client, second)
        self.assert_identity(client, head['message_id'], item['delivery_id'], 2)
        for row in attempt_rows(client, head['message_id'], item['delivery_id']):
            if row['outcome'] != 'open':
                self.assertIsNotNone(row['finished'], row)

    def test_token_binds_to_exactly_one_attempt(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 1)
        sweep(client)
        at(client, expires + 3)
        second = claim(client, 3)[0]

        # An independent lane gives us a token that belongs to another delivery.
        aside = client.publish('aside', 'coil-south')
        side = [i for i in claim(client, 3) if i['message_id'] == aside['message_id']][0]

        status, error = ack(client, item | {'lease': {'token': side['lease']['token']}})
        self.assertEqual((status, error), ('rejected', 'conflict'))
        status, error = ack(client, item | {'lease': {'token': 'forged-lease-token'}})
        self.assertEqual((status, error), ('rejected', 'conflict'))
        # The stale attempt-1 token cannot settle this delivery any more.
        status, error = ack(client, item)
        self.assertEqual((status, error), ('rejected', 'conflict'))

        # The live attempt is untouched and still settles under its own token.
        status, result = ack(client, second)
        self.assertEqual((status, result['attempt']), ('accepted', 2))
        # And the unrelated lane is unaffected by all of the above.
        status, _ = ack(client, side)
        self.assertEqual(status, 'accepted')

    def test_deadline_is_inclusive(self):
        client = self.relay()
        _h, _s, item, expires = self.head_lease(client)
        at(client, expires - 0.5)
        status, _ = ack(client, item)
        self.assertEqual(status, 'accepted')  # strictly before: accepted

        client2 = self.relay()
        _h2, _s2, item2, expires2 = self.head_lease(client2)
        at(client2, expires2)
        status, error = ack(client2, item2)  # equal: already expired
        self.assertEqual((status, error), ('rejected', 'conflict'))

    def test_no_dual_success_or_timeout(self):
        # -1 is the only offset where ACK wins; 0 and later are timeouts.
        for offset, expected in ((-1, 'acked'), (0, 'expired'), (1, 'expired'), (4, 'expired')):
            with self.subTest(offset=offset):
                client = self.relay()
                head, successor, item, expires = self.head_lease(client)
                at(client, expires + offset)
                status, _ = ack(client, item)
                sweep(client)  # a later sweep must not add a second outcome
                outcome = self.assert_first_attempt(client, head, item, expected)
                self.assertEqual(outcome['outcome'], expected)
                self.assertEqual(status == 'accepted', expected == 'acked')
                kinds = journal_kinds(client, head['message_id'])
                self.assertEqual(kinds.count('acked'), 1 if expected == 'acked' else 0)
                self.assertEqual(kinds.count('expired'), 0 if expected == 'acked' else 1)

    def test_journal_records_each_transition_once(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 1)
        sweep(client)
        sweep(client)  # idempotent: must not duplicate the expired event
        at(client, expires + 3)
        second = claim(client, 3)[0]
        ack(client, second)
        self.assertEqual(journal_kinds(client, head['message_id']),
                         ['published', 'leased', 'expired', 'leased', 'acked'])
        leased = [e for e in trace(client, head['message_id'])['events'] if e['kind'] == 'leased']
        self.assertEqual({e['details']['attempt'] for e in leased}, {1, 2})


class FailureAndDeadLetter(DeadlineBase):
    """NACK and expiry share the attempt ledger without double-counting."""

    def test_nack_before_deadline_then_ack_is_stale(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires - 1)
        status, result = nack(client, item)
        self.assertEqual(status, 'accepted')
        self.assertEqual((result['state'], result['attempt']), ('waiting', 1))
        # NACK backoff starts at the disposition instant, not at the deadline.
        self.assertEqual(result['effective_at'], expires - 1 + 3)

        status, error = ack(client, item)
        self.assertEqual((status, error), ('rejected', 'conflict'))

    def test_expiry_backoff_starts_at_deadline(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 7)  # sweep well after the deadline
        recovered = sweep(client)
        self.assertEqual(recovered[0]['effective_at'], expires + 3)
        self.assertGreaterEqual(recovered[0]['effective_at'], expires)
        at(client, expires + 3)
        items = claim(client, 3)
        self.assertEqual([(i['delivery_id'], i['lease']['number']) for i in items],
                         [(item['delivery_id'], 2)])

    def test_late_sweep_finished_time_is_within_deadline_and_trigger(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        trigger = expires + 6
        at(client, trigger)
        sweep(client)
        row = self.assert_first_attempt(client, head, item, 'expired')
        self.assertGreaterEqual(row['finished'], expires)
        self.assertLessEqual(row['finished'], trigger)

    def test_exhausted_attempts_become_dead_and_unblock_the_lane(self):
        # POLICY has max_attempts=2, so the second expiry is terminal.
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 1)
        sweep(client)
        at(client, expires + 3)
        second = claim(client, 3)[0]
        at(client, second['lease']['expires'] + 1)
        recovered = sweep(client)
        self.assertEqual((recovered[0]['outcome'], recovered[0]['state']), ('expired', 'dead'))

        status, error = ack(client, second)
        self.assertEqual((status, error), ('rejected', 'conflict'))
        # A terminal (dead) head no longer blocks the same-lane successor.
        items = claim(client, 3)
        self.assertEqual([i['message_id'] for i in items], [successor['message_id']])

        # Redrive appends a policy-bound successor that re-processes the head.
        redriven = client.request(f'{BASE}/deliveries/{item["delivery_id"]}/redrive', {}, 201)
        self.assertEqual(redriven['source_id'], item['delivery_id'])
        self.assertGreater(redriven['delivery_id'], successor['message_id'])
        self.assertEqual(redriven['policy'], 1)

    def test_recovery_is_bounded_and_group_scoped(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        other = client.publish('aside', 'coil-south')
        side = [i for i in claim(client, 3) if i['message_id'] == other['message_id']][0]
        at(client, min(expires, side['lease']['expires']) + 5)
        recovered = sweep(client, limit=1)
        self.assertEqual(len(recovered), 1)  # limit honoured
        self.assertEqual(recovered[0]['delivery_id'], item['delivery_id'])
        # The side lane's lease also lapsed; a stale ACK there is still refused.
        status, error = ack(client, side)
        self.assertEqual((status, error), ('rejected', 'conflict'))


class Concurrency(DeadlineBase):
    """ACK racing reclamation on real threads.

    These tests assert only fixed facts: every legal interleaving must respect
    them.  Which thread wins, how many 503 busy replies appear and which actor
    reports the reclamation are scheduling-dependent and are never pinned.
    """

    def test_concurrent_acks_have_one_winner(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires - 1)

        results = contended(*[lambda: ack(client, item) for _ in range(6)])
        accepted = [r for r in results if r[0] == 'accepted']
        rejected = [r for r in results if r[0] == 'rejected']
        self.assertEqual(len(accepted) + len(rejected), 6)
        self.assertGreaterEqual(len(accepted), 1)
        firsts = [r for r in accepted if not r[1]['repeated']]
        self.assertEqual(len(firsts), 1, [r[1]['repeated'] for r in accepted])  # invariant
        for _status, receipt in accepted:
            self.assertEqual((receipt['state'], receipt['attempt']), ('acked', 1))
        self.assertEqual(
            [(r['number'], r['outcome']) for r in
             attempt_rows(client, head['message_id'], item['delivery_id'])],
            [(1, 'acked')],
        )

    def test_concurrent_sweep_and_claim_report_recovery_once(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 1)

        swept, claimed = contended(*[
            lambda: sweep(client),
            lambda: claim(client, 3),
        ])
        reported = [r for r in swept if r['delivery_id'] == item['delivery_id']]
        # Exactly one actor reports the timeout; never two expired outcomes.
        rows = [r for r in attempt_rows(client, head['message_id'], item['delivery_id'])
                if r['number'] == 1]
        self.assertEqual([(r['number'], r['outcome']) for r in rows], [(1, 'expired')])
        self.assertLessEqual(len(reported), 1)
        for got in claimed:
            self.assertEqual(got['delivery_id'], item['delivery_id'])
        kinds = journal_kinds(client, head['message_id'])
        self.assertEqual(kinds.count('expired'), 1)
        self.assertNotIn('acked', kinds)

    def test_concurrent_ack_and_claim_after_deadline_never_both(self):
        # At E+3 the old token is inherently stale; ACK can only lose.
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 3)

        ack_result, claimed = contended(*[
            lambda: ack(client, item),
            lambda: claim(client, 3),
        ])
        self.assertEqual(ack_result, ('rejected', 'conflict'))
        self.assertEqual([(i['delivery_id'], i['lease']['number']) for i in claimed],
                         [(item['delivery_id'], 2)])
        rows = attempt_rows(client, head['message_id'], item['delivery_id'])
        self.assertEqual([(r['number'], r['outcome']) for r in rows],
                         [(1, 'expired'), (2, 'open')])
        kinds = journal_kinds(client, head['message_id'])
        self.assertEqual(kinds.count('expired'), 1)
        self.assertNotIn('acked', kinds)

    def test_concurrent_read_sees_one_consistent_attempt(self):
        client = self.relay()
        head, successor, item, expires = self.head_lease(client)
        at(client, expires + 1)

        observed, swept = contended(*[
            lambda: attempt_rows(client, head['message_id'], item['delivery_id']),
            lambda: sweep(client),
        ])
        # The read snapshot may predate or postdate the reap, never both.
        first = [r for r in observed if r['number'] == 1]
        self.assertEqual(len(first), 1)
        self.assertIn(first[0]['outcome'], ('open', 'expired'))
        final = [r for r in attempt_rows(client, head['message_id'], item['delivery_id'])
                 if r['number'] == 1]
        self.assertEqual(final[0]['outcome'], 'expired')
        if first[0]['outcome'] == 'open':
            self.assertIsNone(first[0]['finished'])
        else:
            self.assertEqual(len([r for r in swept if r['delivery_id'] == item['delivery_id']]), 1)

    def test_clock_crossing_deadline_mid_ack_and_sweep(self):
        """Both in-flight requests read the clock around the boundary.

        ``Clock.arm(E-1, E)`` hands the two values to whichever request takes
        the write lock first (each request reads the clock once, inside its
        transaction), so the boundary is crossed *between* the two calls:

        * ACK reads ``E-1``, SWEEP reads ``E``   -> ACK accepted, nothing to reap
        * SWEEP reads ``E-1``, ACK reads ``E``   -> ACK rejected, attempt stays open

        A third shape (the reaper already finished before the ACK reads) is
        covered deterministically by ``test_late_sweep_then_ack_does_not_succeed``;
        it is accepted defensively here too.  In every shape the invariant is
        the same: attempt 1 is never both acked and expired, and a same-lane
        successor never leads.
        """
        for sweep_first in (True, False):
            with self.subTest(release_order='sweep-then-ack' if sweep_first else 'ack-then-sweep'):
                client = self.relay()
                head, successor, item, expires = self.head_lease(client)
                at(client, expires - 1)
                client.clock.arm(expires - 1, expires)

                def do_ack():
                    return ack(client, item)

                def do_sweep():
                    return sweep(client)

                results = contended(*((do_sweep, do_ack) if sweep_first
                                      else (do_ack, do_sweep)))
                swept, ack_result = (results if sweep_first else results[::-1])

                rows = attempt_rows(client, head['message_id'], item['delivery_id'])
                first = [r for r in rows if r['number'] == 1]
                self.assertEqual(len(first), 1, rows)
                outcome = first[0]['outcome']
                kinds = journal_kinds(client, head['message_id'])
                accepted = ack_result[0] == 'accepted'

                # The whole point: success and timeout are mutually exclusive.
                self.assertFalse(accepted and outcome == 'expired', (ack_result, rows))
                self.assertFalse(accepted and kinds.count('expired'), kinds)
                if accepted:
                    self.assertEqual(outcome, 'acked')
                    self.assertEqual(kinds.count('acked'), 1)
                    self.assertEqual(kinds.count('expired'), 0)
                    self.assertEqual([r for r in swept if r['delivery_id'] == item['delivery_id']],
                                     [])
                else:
                    self.assertEqual(ack_result, ('rejected', 'conflict'))
                    self.assertIn(outcome, ('open', 'expired'))
                    self.assertEqual(kinds.count('acked'), 0)
                    if outcome == 'expired':
                        self.assertEqual(kinds.count('expired'), 1)
                        self.assertEqual(
                            [(r['delivery_id'], r['outcome']) for r in swept
                             if r['delivery_id'] == item['delivery_id']],
                            [(item['delivery_id'], 'expired')],
                        )
                    else:
                        # Rejected because it read E, not because a reaper ran.
                        self.assertEqual(kinds.count('expired'), 0)
                        self.assertEqual(swept, [])

                # Whatever won, the successor cannot jump the outstanding head.
                at(client, expires + 3)
                for got in claim(client, 3):
                    if accepted:
                        self.assertEqual(got['message_id'], successor['message_id'])
                    else:
                        self.assertEqual(got['delivery_id'], item['delivery_id'])
                        self.assertEqual(got['lease']['number'], 2)
                if accepted:
                    self.assertEqual(
                        [r['outcome'] for r in
                         attempt_rows(client, head['message_id'], item['delivery_id'])],
                        ['acked'],
                    )
                else:
                    self.assertEqual(
                        [(r['number'], r['outcome']) for r in
                         attempt_rows(client, head['message_id'], item['delivery_id'])],
                        [(1, 'expired'), (2, 'open')],
                    )


class Repeatability(DeadlineBase):
    """Identical scripts must yield identical observables; permutations stay safe."""

    def script(self, client, first_key, second_key):
        """Publish two same-lane messages, claim, ACK, and record the trace."""
        published = [client.publish(first_key), client.publish(second_key)]
        lane_ids = sorted(msg['fanout']['oscillator'] for msg in published)
        taken = claim(client, 3)
        self.assertEqual(len(taken), 1)
        self.assertEqual(taken[0]['delivery_id'], lane_ids[0])  # min id is the head
        status, receipt = ack(client, taken[0])
        self.assertEqual(status, 'accepted')
        trace_row = trace(client, taken[0]['message_id'])
        return {
            'message_ids': [msg['message_id'] for msg in published],
            'delivery_ids': lane_ids,
            'accepted': receipt,
            'attempts': [(r['number'], r['outcome'], r['worker'], r['reason'])
                         for d in trace_row['deliveries'] for r in d['attempts']],
            'events': [(e['kind'], e['details']) for e in trace_row['events']],
            'journal': [e['kind'] for e in trace_row['events']],
            'states': [(d['id'], d['state'], d['terminal_at']) for d in trace_row['deliveries']],
        }

    def test_same_script_is_identical_across_runs(self):
        runs = []
        for _ in range(2):
            client = self.relay()
            runs.append(self.script(client, 'head', 'tail'))
        self.assertEqual(runs[0], runs[1])

    def test_publish_order_permutation_keeps_min_id_first(self):
        for order in (('head', 'tail'), ('tail', 'head')):
            with self.subTest(order=order):
                client = self.relay()
                observed = self.script(client, *order)
                # The smallest delivery id of the lane is the only claimable one;
                # the later one is never handed out while that head is outstanding.
                self.assertEqual(observed['journal'][0], 'published')
                self.assertEqual(len(observed['delivery_ids']), 2)
                self.assertEqual(observed['delivery_ids'],
                                 sorted(observed['delivery_ids']))
                # Exactly one attempt row was opened before ACK won.
                self.assertEqual([(n, o) for n, o, *_ in observed['attempts']],
                                 [(1, 'acked')])

    def test_boundary_script_is_deterministic(self):
        def boundary_script(client):
            head, successor, item, expires = self.head_lease(client)
            at(client, expires)
            ack_status = ack(client, item)[1]
            at(client, expires + 1)
            sweep_status = [(r['delivery_id'], r['outcome'], r['effective_at'])
                            for r in sweep(client)]
            at(client, expires + 3)
            retry = [(i['delivery_id'], i['lease']['number']) for i in claim(client, 3)]
            return {
                'ack': ack_status,
                'sweep': sweep_status,
                'retry': retry,
                'attempts': [(r['number'], r['outcome'])
                             for r in attempt_rows(client, head['message_id'],
                                                   item['delivery_id'])],
                'journal': journal_kinds(client, head['message_id']),
            }

        runs = []
        for _ in range(2):
            client = self.relay()
            runs.append(boundary_script(client))
        self.assertEqual(runs[0], runs[1])
        self.assertEqual(runs[0]['ack'], 'conflict')
        self.assertEqual(runs[0]['journal'], ['published', 'leased', 'expired', 'leased'])


class ContractMatrix(unittest.TestCase):
    """The published invariant/scheduling classification stays honest."""

    @staticmethod
    def _test_names():
        names = set()
        for test in unittest.defaultTestLoader.loadTestsFromModule(
                __import__(__name__, fromlist=['*'])):
            for case in _walk(test):
                names.add(case.id().rsplit('.', 1)[-1])
        return names

    def test_each_contract_row_names_an_existing_test(self):
        names = self._test_names()
        for row in CONTRACT:
            self.assertIn(row.asserted_by, names, row)

    def test_kinds_are_exactly_invariant_and_scheduling(self):
        self.assertEqual({row.kind for row in CONTRACT}, {'invariant', 'scheduling'})
        self.assertTrue(by_kind('invariant'))
        self.assertTrue(by_kind('scheduling'))

    def test_row_names_are_unique(self):
        names = [row.name for row in CONTRACT]
        self.assertEqual(len(names), len(set(names)))

    def test_report_documents_every_row(self):
        from pathlib import Path
        report = Path(__file__).resolve().parent.parent / 'docs' / 'lease-deadline-acceptance.md'
        self.assertTrue(report.exists(), report)
        text = report.read_text(encoding='utf-8')
        for row in CONTRACT:
            self.assertIn(row.name, text, row)


def _walk(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from _walk(item)
        else:
            yield item


if __name__ == '__main__':
    unittest.main()
