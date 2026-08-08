"""S1 bulk fetcher — UNCTADstat US.ExchangeRateCrosstab (successor batch, #70).

2,516,166 obs / 55,513 series (exchange rate cross-tabulation). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.ExchangeRateCrosstab", "unctad_exchangeratecrosstab")
