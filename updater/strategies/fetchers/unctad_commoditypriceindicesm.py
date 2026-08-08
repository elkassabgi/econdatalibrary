"""S1 bulk fetcher — UNCTADstat US.CommodityPriceIndices_M (successor batch, #70).

15,190 obs / 42 series (commodity price indices, monthly). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.CommodityPriceIndices_M", "unctad_commoditypriceindicesm")
