"""Per-source sequence merger alias."""

from .confluence import *  # noqa: F401,F403
from .confluence import SequenceConfluence

SequenceMerger = SequenceConfluence
SourceMerger = SequenceConfluence
