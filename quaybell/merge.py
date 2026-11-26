"""Short alias module for gap-aware source merge mode."""

from .confluence import *  # noqa: F401,F403
from .confluence import (  # noqa: F401
    Confluence, ConfluenceError, MergeCoordinator, MergeMode, MergeSession,
    MergeState, MergeWindow, Merger, SequenceConflict, SequenceConfluence,
    SequenceMerger, SequenceMerge, StreamMerger,
)
from . import confluence_service as service  # noqa: F401
