"""S1 bulk fetcher — UNCTADstat US.FTRI (successor batch, #70).

15,432 obs / 1,110 series (freight transport reliability index). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.FTRI", "unctad_ftri")
