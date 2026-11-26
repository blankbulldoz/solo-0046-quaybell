"""British-spelling alias for the takeover/handoff workflow."""
from .takeover import *  # noqa: F401,F403
from .takeover import create, handover, take_over, transfer  # noqa: F401

run = create
execute = create
