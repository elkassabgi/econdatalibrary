"""S1 bulk fetcher — UNCTADstat US.GovExpenditures (successor batch, #70).

50,346 obs / 3,142 series (government expenditures, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.GovExpenditures", "unctad_govexpenditures")
