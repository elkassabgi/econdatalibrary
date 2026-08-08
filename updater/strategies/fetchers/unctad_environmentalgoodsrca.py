"""S1 bulk fetcher — UNCTADstat US.EnvironmentalGoodsRCA (successor batch, #70).

160,194 obs / 6,593 series (environmental goods RCA). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.EnvironmentalGoodsRCA", "unctad_environmentalgoodsrca")
