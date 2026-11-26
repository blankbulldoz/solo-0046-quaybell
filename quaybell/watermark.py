"""Watermark/gap-budget terminology alias."""

from .confluence import *  # noqa: F401,F403
from .confluence import SequenceConfluence

WatermarkTracker = SequenceConfluence
GapWatermark = SequenceConfluence
