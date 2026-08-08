"""S1 bulk fetcher — UNCTADstat US.TradeServICT (successor batch, #70).

26,938 obs / 1,545 series (ICT services trade, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradeServICT", "unctad_tradeservict")
