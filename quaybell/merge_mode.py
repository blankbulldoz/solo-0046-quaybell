"""The group-facing merge/confluence mode API."""

from .confluence import *  # noqa: F401,F403
from .confluence import (  # noqa: F401
    MergeMode, SequenceConfluence, SequenceMerger, MergeCoordinator,
    merge_sequences, merge_streams,
)
from . import confluence_service as service  # noqa: F401
