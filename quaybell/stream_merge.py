"""Stream-oriented spelling of the gap-aware confluence coordinator."""

from .confluence import *  # noqa: F401,F403
from .confluence import SequenceConfluence

StreamMerge = SequenceConfluence
StreamJoiner = SequenceConfluence
