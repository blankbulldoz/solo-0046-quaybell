"""Alternate terminology alias for coalescing/conflation callers."""
from .coalescing import *  # noqa: F401,F403
from .coalescing import (  # noqa: F401
    ALL, LATEST, apply_all_due, apply_due, coalesce, coalesce_delivery,
    coalesce_endpoint, decorate, enrich_delivery, recent, settings_from_input,
    supersede,
)

conflate = coalesce
conflate_delivery = supersede
