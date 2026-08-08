"""S1 bulk fetcher — UNCTADstat US.TermsOfTrade (successor #5, #70).

48,318 obs / 2,328 series at first ingest (core trade indices, annual isTime axis).
All machinery shared via _unctad.py; the single parse/key implementation lives in
jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TermsOfTrade", "unctad_termsoftrade")
