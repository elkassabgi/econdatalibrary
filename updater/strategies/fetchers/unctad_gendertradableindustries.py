"""S1 bulk fetcher — UNCTADstat US.Gender_TradableIndustries (successor batch, #70).

37,922 obs / 3,220 series (gender in tradable industries). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.Gender_TradableIndustries", "unctad_gendertradableindustries")
