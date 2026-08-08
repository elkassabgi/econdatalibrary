"""S1 bulk fetcher — UNCTADstat US.TradePriceIndices_Q (successor #6, #70).

21,930 obs / 258 series at first ingest (Product dim only, Quarter axis). All
machinery shared via _unctad.py; the single parse/key implementation lives in
jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradePriceIndices_Q", "unctad_tradepriceindicesq")
