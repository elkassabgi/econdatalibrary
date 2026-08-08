"""S1 bulk fetcher — UNCTADstat US.Tariff (successor batch, #70).

358,202 obs / 19,090 series (import tariff rates, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.Tariff", "unctad_tariff")
