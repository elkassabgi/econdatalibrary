"""S1 bulk fetcher — UNCTADstat US.EnvironmentalGoodsTrade (successor batch, #70).

581,063 obs / 24,364 series (environmental goods trade). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.EnvironmentalGoodsTrade", "unctad_environmentalgoodstrade")
