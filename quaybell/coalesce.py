"""Compatibility spelling for the latest-reading coalescing subsystem."""
from .coalescing import *  # noqa: F401,F403
from .coalescing import (  # noqa: F401
    ALL, LATEST, apply_all_due, apply_due, coalesce, coalesce_delivery,
    coalesce_endpoint, decorate, enrich_delivery, recent, settings_from_input,
    supersede,
)
