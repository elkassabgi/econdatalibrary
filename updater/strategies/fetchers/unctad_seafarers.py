"""S1 bulk fetcher — UNCTADstat US.Seafarers (successor batch, #70).

2,184 obs / 1,176 series (seafarers by nationality). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.Seafarers", "unctad_seafarers")
