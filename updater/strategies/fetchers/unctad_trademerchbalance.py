"""S1 bulk fetcher — UNCTADstat US.TradeMerchBalance (successor #3, #70).

40,858 obs / 610 series at first ingest (two measure groups: M5015 balance share,
M0100 US$). Annual isTime axis. All machinery shared via _unctad.py; the single
parse/key implementation lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradeMerchBalance", "unctad_trademerchbalance")
