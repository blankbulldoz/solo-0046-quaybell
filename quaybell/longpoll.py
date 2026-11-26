"""Compatibility name for callers describing this feature as long polling."""

from .waiting import (
    DEFAULT_WAIT_SECONDS,
    MAX_WAIT_SECONDS,
    WAIT_FIELDS,
    WaitHub,
    parse_wait,
    wait_for_claim,
)

__all__ = [
    'DEFAULT_WAIT_SECONDS', 'MAX_WAIT_SECONDS', 'WAIT_FIELDS', 'WaitHub',
    'parse_wait', 'wait_for_claim',
]
