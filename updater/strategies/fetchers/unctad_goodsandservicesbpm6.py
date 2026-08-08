"""S1 bulk fetcher — UNCTADstat US.GoodsAndServicesBpm6 (successor batch, #70).

64,789 obs / 3,334 series (goods+services trade BPM6, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.GoodsAndServicesBpm6", "unctad_goodsandservicesbpm6")
