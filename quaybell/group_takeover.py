"""Descriptive module alias for consumer-group takeover."""
from .takeover import *  # noqa: F401,F403
from .takeover import create, take_over, takeover  # noqa: F401

run = create
