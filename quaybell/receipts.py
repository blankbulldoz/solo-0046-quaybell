"""Compatibility name for publication receipt forensic helpers."""

from .forensics import *  # noqa: F401,F403
from .forensics import original_receipt as get  # noqa: F401
from .forensics import query_audit_receipts as list_receipts  # noqa: F401
