"""Explicit per-source sequence merge alias."""

from .confluence import *  # noqa: F401,F403
from .confluence import SequenceConfluence  # noqa: F401

SequenceMergeCoordinator = SequenceConfluence
GapAwareMerger = SequenceConfluence
