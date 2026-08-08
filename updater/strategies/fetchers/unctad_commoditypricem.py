"""S1 bulk fetcher — UNCTADstat US.CommodityPrice_M (successor batch, #70).

16,799 obs / 50 series (commodity prices, monthly). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.CommodityPrice_M", "unctad_commoditypricem")
