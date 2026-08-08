"""S1 bulk fetcher — UNCTADstat US.CommodityPrice_A (successor batch, #70).

1,361 obs / 50 series (commodity prices, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.CommodityPrice_A", "unctad_commoditypricea")
