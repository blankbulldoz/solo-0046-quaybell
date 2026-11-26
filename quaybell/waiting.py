"""Long-polling support for claims.

The wait itself never keeps a SQLite transaction open.  A monotonic version in
the in-process hub closes the wake-up race between reading readiness and
entering the condition wait; a short fallback poll also covers controlled
clocks that are advanced by another thread.
"""

import math
import re
import threading
from contextlib import contextmanager

from . import attempts, claims, eligibility, inputs, subscriptions
from .errors import invalid

MAX_WAIT_SECONDS = 60
DEFAULT_WAIT_SECONDS = 5
POLL_SECONDS = 0.02

SECOND_FIELDS = (
    'max_wait_seconds', 'maximum_wait_seconds', 'wait_seconds',
    'max_wait_time_seconds', 'wait_time_seconds', 'max_wait_duration_seconds',
    'wait_duration_seconds', 'max_wait_duration', 'wait_duration',
    'long_poll_seconds', 'long_poll_timeout_seconds', 'poll_seconds',
    'poll_timeout_seconds', 'block_seconds', 'blocking_seconds',
    'block_timeout_seconds', 'wait_timeout_seconds', 'timeout_seconds',
    'max_wait_time', 'wait_time', 'max_wait', 'wait_timeout', 'block_timeout',
    'poll_timeout', 'long_poll_timeout', 'long_poll', 'timeout', 'wait',
    'poll', 'block', 'blocking',
)
FLAG_FIELDS = frozenset(('wait', 'long_poll', 'poll', 'block', 'blocking'))
MILLISECOND_FIELDS = (
    'max_wait_milliseconds', 'wait_milliseconds', 'wait_timeout_milliseconds',
    'timeout_milliseconds', 'max_wait_ms', 'wait_ms', 'timeout_ms',
)
WAIT_FIELDS = SECOND_FIELDS + MILLISECOND_FIELDS


class WaitHub:
    def __init__(self):
        self._condition = threading.Condition()
        self._version = 0
        self._active = {}

    def version(self):
        with self._condition:
            return self._version

    def ping(self):
        with self._condition:
            self._version += 1
            self._condition.notify_all()

    def wait_for_version(self, version, timeout):
        if timeout <= 0:
            return
        timeout = min(float(timeout), POLL_SECONDS)
        with self._condition:
            if self._version == version:
                self._condition.wait(timeout)

    @contextmanager
    def watch(self, key):
        with self._condition:
            self._active[key] = self._active.get(key, 0) + 1
            self._condition.notify_all()
        try:
            yield self
        finally:
            with self._condition:
                remaining = self._active.get(key, 1) - 1
                if remaining <= 0:
                    self._active.pop(key, None)
                else:
                    self._active[key] = remaining

    def active_count(self, key=None):
        with self._condition:
            if key is not None:
                return self._active.get(key, 0)
            return sum(self._active.values())


def _normalized(source):
    if source is None:
        return {}
    if hasattr(source, 'keys'):
        result = {}
        for key in source.keys():
            value = source[key]
            if isinstance(value, (list, tuple)):
                value = value[0] if value else None
            key = re.sub(r'(?<!^)(?=[A-Z])', '_', str(key)).lower().replace('-', '_')
            result[key] = value
        return result
    return {}


def _number(value):
    if isinstance(value, bool) or value is None:
        raise invalid(f'等待秒数须为 0～{MAX_WAIT_SECONDS} 的数字')
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            raise invalid(f'等待秒数须为 0～{MAX_WAIT_SECONDS} 的数字') from None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise invalid(f'等待秒数须为 0～{MAX_WAIT_SECONDS} 的数字')
    value = float(value)
    if not math.isfinite(value) or value < 0:
        raise invalid(f'等待秒数须为 0～{MAX_WAIT_SECONDS} 的数字')
    return value


def _duration(value):
    value = _number(value)
    if value > MAX_WAIT_SECONDS:
        raise invalid(f'等待秒数须为 0～{MAX_WAIT_SECONDS} 的数字')
    return value


def parse_wait(raw=None, query=None, headers=None, *, default=0):
    sources = (_normalized(raw), _normalized(query), _normalized(headers))
    for field in SECOND_FIELDS:
        for source in sources:
            if field in source and source[field] is not None:
                value = source[field]
                if isinstance(value, bool):
                    if field not in FLAG_FIELDS:
                        _duration(value)  # always raises the consistent 400
                    return DEFAULT_WAIT_SECONDS if value else 0
                return _duration(value)
    for field in MILLISECOND_FIELDS:
        for source in sources:
            if field in source and source[field] is not None:
                return _duration(_number(source[field]) / 1000)
    return default


def claim_payload(raw):
    """Remove wait aliases while retaining strict validation for other fields."""
    return {key: value for key, value in raw.items() if key not in WAIT_FIELDS}


def validate(raw):
    payload = claim_payload(raw)
    inputs.require(payload, ('worker',), ('limit',))
    worker = inputs.name(raw['worker'], '工作进程')
    limit = inputs.integer(raw.get('limit', 1), 1, 30, '领取上限')
    return payload, worker, limit


def _probe(db, sub, now, limit):
    if sub['paused']:
        return False, True
    ready = eligibility.ready(db, sub['id'], now, limit)
    expired = attempts.expired(db, now, 1, sub['id'])
    return bool(ready or expired), False


def wait_for_claim(persistence, channel, group, raw, clock, *, hub=None, wait_seconds=None):
    if wait_seconds is None:
        wait_seconds = parse_wait(raw, default=0)
    wait_seconds = _duration(wait_seconds)
    payload, _worker, limit = validate(raw)
    key = f'{channel}/{group}'
    deadline = clock() + wait_seconds

    if hub is None:
        hub = WaitHub()

    with hub.watch(key):
        while True:
            version = hub.version()
            now = clock()
            with persistence.session() as db:
                sub = subscriptions.get(db, channel, group)
                should_claim, paused = _probe(db, sub, now, limit)

            if paused:
                return {'paused': True, 'items': []}

            if should_claim:
                claim_now = clock()
                with persistence.session(write=True) as db:
                    result = claims.claim(db, channel, group, payload, claim_now)
                if result['items'] or result['paused']:
                    return result
                # Another waiter won the race.  Recompute the deadline below.

            now = clock()
            remaining = deadline - now
            if remaining <= 0:
                if should_claim:
                    return result
                # Re-check at the deadline inside the canonical claim transaction.
                with persistence.session(write=True) as db:
                    return claims.claim(db, channel, group, payload, clock())

            hub.wait_for_version(version, min(POLL_SECONDS, remaining))
