"""S1 bulk fetcher — UNCTADstat US.TradeMerchGR (growth rates; successor #2, #70).

28,863 obs / 1,754 series at first ingest. Period-coded time axis (isTime=false):
annual YoY rows keep the bare Economy.Flow.M4017 key; multi-year averages carry the
|SPAN=<n>Y suffix (see jobs/ingest_unctad_ds.parse_period_code — end-years collide
between the two families, measured 7 duplicates). All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradeMerchGR", "unctad_trademerchgr")
