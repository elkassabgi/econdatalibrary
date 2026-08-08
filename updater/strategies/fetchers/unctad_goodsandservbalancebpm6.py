"""S1 bulk fetcher — UNCTADstat US.GoodsAndServBalanceBpm6 (successor batch, #70).

65,247 obs / 3,352 series (goods+services balance BPM6, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.GoodsAndServBalanceBpm6", "unctad_goodsandservbalancebpm6")
