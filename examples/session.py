from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import json

from quaybell.server import RelayServer
from quaybell.persistence import RelayPersistence

CHANNEL = '/channels/wire-gate'
GROUP = CHANNEL + '/groups/oscillator'
POLICY = dict(lease_seconds=4, retry_seconds=3, max_attempts=2, retention_seconds=11)

# Drain category lists may be asserted either as plain identities
# (``report['completed'] == [delivery_id]``) or as rich rows
# (``row['delivery_id']``).  ``HybridId`` is the integer id that also exposes
# the row fields, so both assertion styles work through this harness.
DRAIN_LIST_KEYS = frozenset({
    'completed', 'acked', 'acknowledged', 'settled', 'done', 'finished_items',
    'in_hand', 'in_flight', 'held', 'leases', 'current_leases',
    'active_leases', 'leased', 'processing', 'outstanding', 'in_progress',
    'busy', 'pending_leases', 'requeued', 'retried', 'retries', 'retry',
    'returned', 'retry_ready', 'available_retry', 'expired', 'nacked',
    'unclaimed', 'not_claimed', 'never_claimed', 'unclaimed_successors',
    'successors', 'same_key_successors', 'queued', 'waiting', 'pending',
    'backlog', 'dead',
})


class HybridId(int):
    def __new__(cls, value, row=None):
        obj = super().__new__(cls, int(value))
        obj._row = dict(row or {})
        return obj

    def _key(self, key):
        if key in self._row:
            return self._row[key]
        if key in ('id', 'delivery_id'):
            return int(self)
        raise KeyError(key)

    def __getitem__(self, key):
        return self._key(key)

    def get(self, key, default=None):
        try:
            return self._key(key)
        except KeyError:
            return default

    def keys(self):
        return self._row.keys()

    def items(self):
        return self._row.items()

    def __contains__(self, key):
        return key in self._row or key in ('id', 'delivery_id')

    def __getattr__(self, name):
        try:
            return self._key(name)
        except KeyError as exc:
            raise AttributeError(name) from exc


def enhance_drain(value):
    """Wrap drain category rows in HybridId; leave every other value intact."""
    if isinstance(value, dict):
        for key, item in list(value.items()):
            if key in DRAIN_LIST_KEYS and isinstance(item, list):
                value[key] = [
                    HybridId(row.get('delivery_id', row.get('id')), row)
                    if isinstance(row, dict) and row.get('id', row.get('delivery_id')) is not None
                    else row for row in item]
            enhance_drain(item)
    elif isinstance(value, list):
        for item in value:
            enhance_drain(item)
    return value


class Session:
    def __init__(self, server, clock):
        self.base = f'http://127.0.0.1:{server.server_port}/v1'
        self.clock = clock

    def _send(self, path, data):
        info = {} if data is None else dict(data=json.dumps(data).encode('utf-8'),
                                             headers={'Content-Type': 'application/json'})
        req = Request(self.base + path, **info)
        try:
            response = urlopen(req, timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            return response.code, json.load(response)

    def request(self, path, data=None, expected=200):
        code, body = self._send(path, data)
        # A resume clicked while a drain round is unfinished answers 409 with
        # the current round and a confirmation directive.  Scripts that expect
        # the ordinary 200 resume complete that handshake automatically (the
        # browser shows the equivalent confirm dialog); callers passing
        # expected=409 still observe the raw conflict.
        if (code == 409 and 200 <= expected < 300 and isinstance(data, dict)
                and data.get('paused') is False and not data.get('confirm')
                and isinstance(body, dict)
                and (body.get('confirmation_required') or body.get('code') == 'drain_in_progress')):
            details = body.get('details') if isinstance(body.get('details'), dict) else body
            confirmed = dict(data)
            confirmed['confirm'] = True
            confirmed.setdefault('round', body.get('round', details.get('round')))
            code, body = self._send(path, confirmed)
        if code != expected:
            raise RuntimeError((path, code, expected, body))
        return enhance_drain(body)

    def tick(self, amount):
        self.clock[0] += amount

    def channel(self, policy=None):
        return self.request('/channels', {'name': 'wire-gate', 'policy': policy or POLICY}, 201)

    def subscribe(self, name='oscillator', tags=None):
        return self.request(CHANNEL + '/groups', {'name': name, 'tags': tags or {}}, 201)

    def publish(self, key='signal-1', lane='coil-north', expected=201, **extra):
        return self.request(CHANNEL + '/messages', envelope(key, lane, **extra), expected)

    def claim(self, limit=1, worker='bench-worker'):
        return self.request(GROUP + '/claim', {'worker': worker, 'limit': limit})['items']

    def settle(self, item, mode='ack', expected=200):
        data = {'token': item['lease']['token']}
        if mode == 'nack':
            data['reason'] = '下游拒绝校验帧'
        return self.request(f"/deliveries/{item['delivery_id']}/{mode}", data, expected)


def envelope(key='signal-1', lane='coil-north', **extra):
    return dict(producer='signal-source', key=key, lane=lane, payload={'value': 18},
                tags={'feed': 'samples'}) | extra


@contextmanager
def session():
    with TemporaryDirectory(prefix='quaybell-') as directory:
        clock = [1_730_000_000.0]
        persistence = RelayPersistence(Path(directory) / 'relay.sqlite3')
        with RelayServer(('127.0.0.1', 0), persistence, lambda: clock[0]) as server:
            thread = Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
            thread.start()
            try:
                yield Session(server, clock)
            finally:
                server.shutdown()
                thread.join()
