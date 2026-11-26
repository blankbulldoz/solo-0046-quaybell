"""Compatibility spelling for callers that name this capability "merging"."""

from .confluence import *  # noqa: F401,F403
from .confluence import (  # noqa: F401
    Confluence, ConfluenceError, MergeCoordinator, MergeMode, MergeSession,
    MergeState, MergeWindow, Merger, SequenceConflict, SequenceConfluence,
    SequenceMerger, SequenceMerge, StreamMerger, View,
)
from . import confluence_service  # noqa: F401

merge = SequenceConfluence
merge_coordinator = SequenceConfluence
