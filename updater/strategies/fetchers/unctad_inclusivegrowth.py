"""S1 bulk fetcher — UNCTADstat US.InclusiveGrowth (successor batch, #70).

670 obs / 670 series (inclusive growth indicators). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.InclusiveGrowth", "unctad_inclusivegrowth")
