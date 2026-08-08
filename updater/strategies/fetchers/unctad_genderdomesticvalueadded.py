"""S1 bulk fetcher — UNCTADstat US.Gender_DomesticValueAdded (successor batch, #70).

18,630 obs / 1,860 series (gender domestic value added). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.Gender_DomesticValueAdded", "unctad_genderdomesticvalueadded")
