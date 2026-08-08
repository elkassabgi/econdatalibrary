"""S1 bulk fetcher — UNCTADstat US.PLSCI (successor batch, #70).

75,063 obs / 1,353 series (port liner shipping connectivity, quarterly). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.PLSCI", "unctad_plsci")
