"""S1 bulk fetcher — UNCTADstat US.LSBCI (successor batch, #70).

655,370 obs / 36,140 series (liner shipping bilateral connectivity). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.LSBCI", "unctad_lsbci")
