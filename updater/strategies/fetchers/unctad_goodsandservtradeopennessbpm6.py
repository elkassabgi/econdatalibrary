"""S1 bulk fetcher — UNCTADstat US.GoodsAndServTradeOpennessBpm6 (successor batch, #70).

129,989 obs / 6,672 series (trade openness BPM6). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.GoodsAndServTradeOpennessBpm6", "unctad_goodsandservtradeopennessbpm6")
