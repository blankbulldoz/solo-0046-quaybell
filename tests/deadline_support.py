"""Deterministic fixture for the lease-deadline acceptance suite.

The suite talks only to the public HTTP listener, exactly like ``examples``.
The wall clock is replaced by a small object so every scenario is a pure
function of the recorded step list, except the tests that deliberately release
requests on real threads.  Nothing here sleeps.
"""
import threading
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from examples.session import Session

from quaybell.persistence import RelayPersistence
from quaybell.server import RelayRequest, RelayServer

# Keep the acceptance output to test results; only the access log line is
# muted, the public HTTP surface is untouched.
RelayRequest.log_message = lambda self, *args, **kwargs: None

# A four second lease keeps the three boundary inputs E-1, E and >E small.
POLICY = dict(lease_seconds=4, retry_seconds=3, max_attempts=2, retention_seconds=3600)
START = 1_730_000_000
BASE = ''  # Session.base already ends in /v1
CHANNEL = BASE + '/channels/wire-gate'
GROUP = CHANNEL + '/groups/oscillator'


class Clock:
    """Mutable injected clock with a list interface for ``Session.tick``.

    ``arm(*values)`` queues the next values to be handed out in read order;
    once the queue is empty every read returns the stable value.  The server
    reads the clock once per request, while holding its write transaction, so
    the queued value a request sees is decided by lock-acquisition order.
    """

    def __init__(self, base):
        self._value = float(base)
        self._queue = []
        self._lock = threading.Lock()

    def __call__(self):
        with self._lock:
            return self._queue.pop(0) if self._queue else self._value

    def __getitem__(self, index):
        assert index == 0
        with self._lock:
            return self._value

    def __setitem__(self, index, value):
        assert index == 0
        with self._lock:
            self._value = float(value)

    def set(self, value):
        with self._lock:
            self._value = float(value)
        return float(value)

    def arm(self, *values):
        with self._lock:
            self._queue = [float(value) for value in values]


@contextmanager
def relay(policy=None):
    with TemporaryDirectory(prefix='quaybell-deadline-') as directory:
        clock = Clock(START)
        persistence = RelayPersistence(Path(directory) / 'relay.sqlite3')
        with RelayServer(('127.0.0.1', 0), persistence, clock) as server:
            thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
            thread.start()
            try:
                client = Session(server, clock)
                client.channel(policy or POLICY)
                client.subscribe()
                yield client
            finally:
                server.shutdown()
                thread.join()


def _classify(exc):
    _path, status, _expected, body = exc.args[0]
    if status == 409:
        return 'rejected', body['error']
    if status == 503:
        return 'busy', body['error']
    raise exc


def settle(client, item, mode='ack', tries=8):
    """Settle and normalise outcomes; retry the documented 503 busy reply.

    Returns ``(status, detail_or_busy_count)`` where status is one of
    ``accepted``, ``rejected`` or ``busy`` (the latter only after all tries).
    """
    busy = 0
    for attempt in range(tries):
        try:
            return 'accepted', client.settle(item, mode)
        except RuntimeError as exc:
            status, error = _classify(exc)
            if status == 'busy':
                busy += 1
                continue
            return status, error
    return 'busy', busy


def ack(client, item, tries=8):
    return settle(client, item, 'ack', tries)


def nack(client, item, tries=8):
    return settle(client, item, 'nack', tries)


def sweep(client, limit=100):
    return client.request(BASE + '/sweep', {'limit': limit})['recovered']


def claim(client, limit=30, worker='deadline-worker'):
    return client.request(GROUP + '/claim', {'worker': worker, 'limit': limit})['items']


def trace(client, message_id):
    return client.request(f'{BASE}/messages/{message_id}')


def delivery_view(client, message_id, delivery_id):
    found = [d for d in trace(client, message_id)['deliveries'] if d['id'] == delivery_id]
    assert len(found) == 1, (message_id, delivery_id)
    return found[0]


def attempt_rows(client, message_id, delivery_id):
    return delivery_view(client, message_id, delivery_id)['attempts']


def journal_kinds(client, message_id):
    return [event['kind'] for event in trace(client, message_id)['events']]


def open_lease(client, key='head', lane='coil-north'):
    """Publish ``key`` plus a same-lane successor, claim the head, return all three."""
    head = client.publish(key, lane)
    successor = client.publish(f'{key}-successor', lane)
    item = claim(client, 1)[0]
    assert item['message_id'] == head['message_id'], item
    return head, successor, item


def at(client, moment):
    """Advance the controlled clock to ``moment`` and return it."""
    return client.clock.set(moment)


def contended(*callables):
    """Run callables on real threads released together; return results in order.

    ``callables`` receive no arguments and must swallow nothing we care about:
    an escaped exception is re-raised in the caller's thread.
    """
    import threading

    barrier = threading.Barrier(len(callables))
    results = [None] * len(callables)
    failures = []

    def run(index, fn):
        try:
            barrier.wait(timeout=10)
            results[index] = fn()
        except BaseException as exc:  # pragma: no cover - surfaced by caller
            failures.append((index, exc))

    threads = [threading.Thread(target=run, args=(i, fn)) for i, fn in enumerate(callables)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)
        assert not thread.is_alive(), 'contended call did not finish'
    if failures:
        index, exc = failures[0]
        raise AssertionError(f'contended call {index} raised') from exc
    return results
