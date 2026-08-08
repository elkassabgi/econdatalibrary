"""S1 bulk fetcher — UNCTADstat US.SeaborneTrade (successor batch, #70).

62,821 obs / 2,725 series (seaborne trade, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.SeaborneTrade", "unctad_seabornetrade")
