"""Automated acceptance for publication crash/recovery boundaries.

Run with either::

    python3 -m unittest tests.test_publish_crash_recovery -v
    python3 -m unittest discover -s tests -p 'test_*.py' -v

Each case starts a real HTTP + SQLite subprocess.  A test-only fault hook stops
that process at one precise boundary, and a second clean subprocess opens the
same database for restart forensics and recovery.
"""
import http.client
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HELPER = Path(__file__).with_name('crash_server.py')

CHANNEL = 'crash-recovery'
PRODUCER = 'crash-producer'
PRODUCTION_KEY = 'original-production-key'
LANE = 'independent-lane'
ENVELOPE = {
    'producer': PRODUCER,
    'key': PRODUCTION_KEY,
    'lane': LANE,
    'payload': {'sample': 'fanout-crash', 'n': 42},
    'tags': {'feed': 'samples'},
}
POLICY = {
    'lease_seconds': 300,
    'retry_seconds': 5,
    'max_attempts': 3,
    'retention_seconds': 86400,
}
# Subscription id order is the routing order. Alpha and beta match; gamma does not.
GROUPS = (
    ('alpha', {}),
    ('beta', {'feed': 'samples'}),
    ('gamma', {'feed': 'alerts'}),
)


class ServerHandle:
    def __init__(self, process, port, log_path, ready_path, marker_path=None):
        self.process = process
        self.port = port
        self.log_path = log_path
        self.ready_path = ready_path
        self.marker_path = marker_path


class PublishCrashRecoveryAcceptance(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory(prefix='quaybell-crash-acceptance-')
        self.directory = Path(self.temporary_directory.name)
        self.database = self.directory / 'state' / 'relay.sqlite3'
        self.processes = []

    def tearDown(self):
        for handle in self.processes:
            process = handle.process
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        self.temporary_directory.cleanup()

    def _start_server(self, boundary=''):
        ready_path = self.directory / f'ready-{len(self.processes)}.json'
        log_path = self.directory / f'server-{len(self.processes)}.log'
        marker_path = self.directory / f'crash-marker-{len(self.processes)}.json'
        env = os.environ.copy()
        env['PYTHONPATH'] = str(ROOT) + (os.pathsep + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
        if boundary:
            env['QUAYBELL_CRASH_BOUNDARY'] = boundary
            env['QUAYBELL_CRASH_MARKER'] = str(marker_path)
        else:
            env.pop('QUAYBELL_CRASH_BOUNDARY', None)
            env.pop('QUAYBELL_CRASH_MARKER', None)

        log_handle = open(log_path, 'wb')
        process = subprocess.Popen(
            [sys.executable, str(HELPER), '--db', str(self.database), '--ready-file', str(ready_path)],
            cwd=str(ROOT),
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        self.addCleanup(log_handle.close)
        handle = ServerHandle(process, None, log_path, ready_path, marker_path)
        self.processes.append(handle)

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            code = process.poll()
            if code is not None:
                self.fail(f'crash server exited early with {code}; log:\n{self._log(handle)}')
            if ready_path.exists():
                ready = json.loads(ready_path.read_text(encoding='utf-8'))
                handle.port = ready['port']
                return handle
            time.sleep(0.03)
        process.kill()
        process.wait(timeout=5)
        self.fail(f'timed out waiting for server; log:\n{self._log(handle)}')

    @staticmethod
    def _log(handle):
        try:
            return handle.log_path.read_text(encoding='utf-8', errors='replace')
        except OSError as exc:
            return f'<log unavailable: {exc}>'

    def _http(self, handle, method, path, payload=None, expected=200):
        connection = http.client.HTTPConnection('127.0.0.1', handle.port, timeout=8)
        try:
            body = None
            headers = {}
            if payload is not None:
                body = json.dumps(payload, sort_keys=True).encode('utf-8')
                headers['Content-Type'] = 'application/json'
            connection.request(method, f'/v1{path}', body=body, headers=headers)
            response = connection.getresponse()
            raw = response.read()
            try:
                parsed = json.loads(raw.decode('utf-8')) if raw else None
            except json.JSONDecodeError as exc:
                raise AssertionError(f'non-JSON response {response.status}: {raw!r}') from exc
            if expected is not None and response.status != expected:
                raise AssertionError(
                    f'{method} {path}: expected {expected}, got {response.status}: {parsed}'
                )
            return response.status, parsed
        finally:
            connection.close()

    def _setup_topology(self, handle):
        self._http(handle, 'POST', '/channels', {'name': CHANNEL, 'policy': POLICY}, 201)
        for name, tags in GROUPS:
            self._http(
                handle,
                'POST',
                f'/channels/{CHANNEL}/groups',
                {'name': name, 'tags': tags},
                201,
            )

    def _raw_publish_expecting_no_reply(self, handle):
        """Send the original request exactly once, without an HTTP client retry."""
        body = json.dumps(ENVELOPE, sort_keys=True, separators=(',', ':')).encode('utf-8')
        request_bytes = (
            f'POST /v1/channels/{CHANNEL}/messages HTTP/1.1\r\n'
            f'Host: 127.0.0.1:{handle.port}\r\n'
            'Content-Type: application/json\r\n'
            f'Content-Length: {len(body)}\r\n'
            'Connection: close\r\n\r\n'
        ).encode('ascii') + body

        outcome = 'eof'
        chunks = []
        with socket.create_connection(('127.0.0.1', handle.port), timeout=8) as wire:
            wire.sendall(request_bytes)
            # The complete body is already in the kernel buffer before half-closing.
            wire.shutdown(socket.SHUT_WR)
            while True:
                try:
                    chunk = wire.recv(4096)
                except ConnectionResetError:
                    outcome = 'tcp-reset'
                    break
                except socket.timeout:
                    outcome = 'timeout'
                    break
                if chunk == b'':
                    break
                chunks.append(chunk)

        reply = b''.join(chunks)
        return {
            'wire_attempts': 1,
            'http_replies': 0 if not reply else 1,
            'outcome': outcome,
            'reply_bytes': len(reply),
            'reply_prefix': reply[:32].decode('latin-1', errors='replace'),
        }

    def _wait_for_sigkill(self, handle):
        try:
            return handle.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            handle.process.kill()
            handle.process.wait(timeout=5)
            self.fail('fault boundary did not stop the process')

    def _read_marker(self, handle):
        self.assertTrue(handle.marker_path.exists(), 'fault marker was not written')
        return json.loads(handle.marker_path.read_text(encoding='utf-8'))

    def _forensics(self):
        uri = f'{self.database.resolve().as_uri()}?mode=ro'
        db = sqlite3.connect(uri, uri=True, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            message = db.execute(
                '''SELECT id, channel, producer, dedup_key, fingerprint, policy, published, available
                   FROM messages
                   WHERE channel=? AND producer=? AND dedup_key=?''',
                (CHANNEL, PRODUCER, PRODUCTION_KEY),
            ).fetchone()
            message = dict(message) if message else None
            message_id = message['id'] if message else None

            if message_id is None:
                message_deliveries = []
                events = []
                attempts = []
            else:
                message_deliveries = [dict(row) for row in db.execute(
                    '''SELECT d.id, d.message_id, d.subscription_id, s.name AS group_name,
                              d.state, d.attempt_count, d.ready_at, d.terminal_at
                       FROM deliveries d
                       JOIN subscriptions s ON s.id=d.subscription_id
                       WHERE d.message_id=?
                       ORDER BY s.id''',
                    (message_id,),
                )]
                events = [dict(row) for row in db.execute(
                    'SELECT id, kind, at, details FROM journal WHERE message_id=? ORDER BY id',
                    (message_id,),
                )]
                attempts = [dict(row) for row in db.execute(
                    '''SELECT a.id, a.delivery_id, a.number, a.outcome, a.receipt
                       FROM attempts a
                       JOIN deliveries d ON d.id=a.delivery_id
                       WHERE d.message_id=?
                       ORDER BY a.id''',
                    (message_id,),
                )]

            channel_deliveries = [dict(row) for row in db.execute(
                '''SELECT d.id, d.message_id, s.name AS group_name, d.state
                   FROM deliveries d
                   JOIN messages m ON m.id=d.message_id
                   JOIN subscriptions s ON s.id=d.subscription_id
                   WHERE m.channel=?
                   ORDER BY d.id''',
                (CHANNEL,),
            )]
            counts = {
                'messages_for_exact_production_key': db.execute(
                    'SELECT COUNT(*) FROM messages WHERE channel=? AND producer=? AND dedup_key=?',
                    (CHANNEL, PRODUCER, PRODUCTION_KEY),
                ).fetchone()[0],
                'messages_in_channel': db.execute(
                    'SELECT COUNT(*) FROM messages WHERE channel=?', (CHANNEL,)
                ).fetchone()[0],
                'deliveries_in_channel': db.execute(
                    '''SELECT COUNT(*) FROM deliveries d
                       JOIN messages m ON m.id=d.message_id
                       WHERE m.channel=?''',
                    (CHANNEL,),
                ).fetchone()[0],
                'published_events_in_channel': db.execute(
                    "SELECT COUNT(*) FROM journal WHERE channel=? AND kind='published'",
                    (CHANNEL,),
                ).fetchone()[0],
            }
            integrity = db.execute('PRAGMA integrity_check').fetchone()[0]
            subscriptions = [dict(row) for row in db.execute(
                'SELECT id, channel, name, tags, paused FROM subscriptions WHERE channel=? ORDER BY id',
                (CHANNEL,),
            )]
            return {
                'exact_production_key': {'producer': PRODUCER, 'key': PRODUCTION_KEY},
                'subscriptions': subscriptions,
                'message': message,
                'message_deliveries': message_deliveries,
                'channel_deliveries': channel_deliveries,
                'events': [{'id': e['id'], 'kind': e['kind']} for e in events],
                'attempts': [
                    {key: row[key] for key in ('id', 'delivery_id', 'number', 'outcome')}
                    for row in attempts
                ],
                'ack_receipts_present': sum(1 for row in attempts if row['receipt']),
                'counts': counts,
                'sqlite_integrity_check': integrity,
            }
        finally:
            db.close()

    def _claim_and_ack(self, handle, receipt):
        results = {}
        for group in ('alpha', 'beta'):
            _, claim_reply = self._http(
                handle,
                'POST',
                f'/channels/{CHANNEL}/groups/{group}/claim',
                {'worker': f'{group}-recovery-worker', 'limit': 1},
                200,
            )
            items = claim_reply['items']
            self.assertEqual(len(items), 1, f'{group} must independently claim its delivery')
            item = items[0]
            self.assertEqual(item['message_id'], receipt['message_id'])
            self.assertEqual(item['delivery_id'], receipt['fanout'][group])

            _, ack = self._http(
                handle,
                'POST',
                f"/deliveries/{item['delivery_id']}/ack",
                {'token': item['lease']['token']},
                200,
            )
            self.assertFalse(ack['repeated'])
            self.assertEqual(ack['state'], 'acked')
            self.assertEqual(ack['outcome'], 'acked')

            _, ack_retry = self._http(
                handle,
                'POST',
                f"/deliveries/{item['delivery_id']}/ack",
                {'token': item['lease']['token']},
                200,
            )
            self.assertTrue(ack_retry['repeated'])
            self.assertEqual(ack_retry['state'], 'acked')
            self.assertEqual(ack_retry['delivery_id'], ack['delivery_id'])
            results[group] = {'claim': item, 'ack': ack, 'ack_retry': ack_retry}

        _, gamma_claim = self._http(
            handle,
            'POST',
            f'/channels/{CHANNEL}/groups/gamma/claim',
            {'worker': 'gamma-should-not-receive', 'limit': 1},
            200,
        )
        self.assertEqual(gamma_claim['items'], [])
        results['gamma'] = {'claim_items': []}

        # A second claim from either matching group must not find a duplicate.
        for group in ('alpha', 'beta'):
            _, second_claim = self._http(
                handle,
                'POST',
                f'/channels/{CHANNEL}/groups/{group}/claim',
                {'worker': f'{group}-duplicate-check', 'limit': 1},
                200,
            )
            self.assertEqual(second_claim['items'], [])
        return results

    def _assert_common_recovery(self, forensics, receipt, claims_and_acks):
        self.assertEqual(forensics['sqlite_integrity_check'], 'ok')
        self.assertEqual(set(receipt['fanout']), {'alpha', 'beta'})
        self.assertNotIn('gamma', receipt['fanout'])

        rows = {row['group_name']: row for row in forensics['message_deliveries']}
        self.assertEqual(set(rows), {'alpha', 'beta'})
        for group in ('alpha', 'beta'):
            self.assertEqual(rows[group]['id'], receipt['fanout'][group])
            self.assertEqual(rows[group]['state'], 'acked')
            self.assertEqual(rows[group]['attempt_count'], 1)
        self.assertNotIn('gamma', rows)

        self.assertEqual({row['group_name'] for row in forensics['channel_deliveries']}, {'alpha', 'beta'})
        self.assertEqual(len(forensics['attempts']), 2)
        self.assertEqual({row['outcome'] for row in forensics['attempts']}, {'acked'})
        self.assertEqual(forensics['ack_receipts_present'], 2)
        self.assertEqual(claims_and_acks['gamma']['claim_items'], [])

    def test_fanout_crash_after_first_matching_group_rolls_back_entire_publication(self):
        crashed = self._start_server('fanout_after_first')
        self._setup_topology(crashed)

        no_reply = self._raw_publish_expecting_no_reply(crashed)
        return_code = self._wait_for_sigkill(crashed)
        marker = self._read_marker(crashed)

        self.assertEqual(return_code, -signal.SIGKILL)
        self.assertEqual(no_reply['wire_attempts'], 1)
        self.assertEqual(no_reply['http_replies'], 0, no_reply)
        self.assertEqual(no_reply['reply_bytes'], 0, no_reply)
        self.assertIn(no_reply['outcome'], {'eof', 'tcp-reset'}, no_reply)
        self.assertEqual(marker['phase'], 'after_first_matching_delivery_insert_before_second')
        self.assertFalse(marker['committed'])

        # A clean process opening the same DB is the restart. Forensics are collected
        # before the original production key is sent again.
        restarted = self._start_server()
        pre_resend = self._forensics()
        print('\nFANOUT_BOUNDARY_PRE_RESEND_FORENSICS=' + json.dumps(pre_resend, sort_keys=True))

        self.assertIsNone(pre_resend['message'])
        self.assertEqual(pre_resend['message_deliveries'], [])
        self.assertEqual(pre_resend['channel_deliveries'], [])
        self.assertEqual(pre_resend['counts']['messages_for_exact_production_key'], 0)
        self.assertEqual(pre_resend['counts']['messages_in_channel'], 0)
        self.assertEqual(pre_resend['counts']['deliveries_in_channel'], 0)
        self.assertEqual(pre_resend['counts']['published_events_in_channel'], 0)
        subscription_names = {row['id']: row['name'] for row in pre_resend['subscriptions']}
        self.assertEqual(subscription_names[marker['subscription_id']], 'alpha')

        status, receipt = self._http(
            restarted,
            'POST',
            f'/channels/{CHANNEL}/messages',
            ENVELOPE,
            201,
        )
        self.assertEqual(status, 201)
        self.assertFalse(receipt['repeated'])
        self.assertEqual(set(receipt['fanout']), {'alpha', 'beta'})

        claims_and_acks = self._claim_and_ack(restarted, receipt)
        post_ack = self._forensics()
        print('FANOUT_BOUNDARY_POST_ACK_FORENSICS=' + json.dumps(post_ack, sort_keys=True))
        self._assert_common_recovery(post_ack, receipt, claims_and_acks)

    def test_committed_publication_with_lost_response_resends_same_receipt(self):
        crashed = self._start_server('after_commit_before_response')
        self._setup_topology(crashed)

        no_reply = self._raw_publish_expecting_no_reply(crashed)
        return_code = self._wait_for_sigkill(crashed)
        marker = self._read_marker(crashed)

        self.assertEqual(return_code, -signal.SIGKILL)
        self.assertEqual(no_reply['wire_attempts'], 1)
        self.assertEqual(no_reply['http_replies'], 0, no_reply)
        self.assertEqual(no_reply['reply_bytes'], 0, no_reply)
        self.assertIn(no_reply['outcome'], {'eof', 'tcp-reset'}, no_reply)
        self.assertEqual(marker['phase'], 'transaction_committed_before_response_body')
        self.assertTrue(marker['committed'])
        self.assertEqual(set(marker['reply']['fanout']), {'alpha', 'beta'})

        restarted = self._start_server()
        pre_resend = self._forensics()
        print('\nRESPONSE_BOUNDARY_PRE_RESEND_FORENSICS=' + json.dumps(pre_resend, sort_keys=True))

        # Unlike the fanout boundary, the durable transaction is complete. The lost
        # HTTP response is the only thing the first client did not receive.
        self.assertIsNotNone(pre_resend['message'])
        self.assertEqual(pre_resend['counts']['messages_for_exact_production_key'], 1)
        self.assertEqual(pre_resend['counts']['messages_in_channel'], 1)
        self.assertEqual(pre_resend['counts']['deliveries_in_channel'], 2)
        self.assertEqual(pre_resend['counts']['published_events_in_channel'], 1)
        self.assertEqual({row['group_name'] for row in pre_resend['message_deliveries']}, {'alpha', 'beta'})
        self.assertTrue(all(row['state'] == 'waiting' for row in pre_resend['message_deliveries']))
        self.assertEqual(
            {row['group_name']: row['id'] for row in pre_resend['message_deliveries']},
            marker['reply']['fanout'],
        )

        status, receipt = self._http(
            restarted,
            'POST',
            f'/channels/{CHANNEL}/messages',
            ENVELOPE,
            200,
        )
        self.assertEqual(status, 200)
        self.assertTrue(receipt['repeated'])
        self.assertEqual(receipt['message_id'], pre_resend['message']['id'])
        self.assertEqual(receipt['fanout'], marker['reply']['fanout'])
        self.assertEqual(len(pre_resend['message_deliveries']), 2)

        claims_and_acks = self._claim_and_ack(restarted, receipt)
        post_ack = self._forensics()
        print('RESPONSE_BOUNDARY_POST_ACK_FORENSICS=' + json.dumps(post_ack, sort_keys=True))
        self._assert_common_recovery(post_ack, receipt, claims_and_acks)


if __name__ == '__main__':
    unittest.main(verbosity=2)
