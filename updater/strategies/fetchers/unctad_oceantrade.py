"""S1 bulk fetcher — UNCTADstat US.OceanTrade (giant #6, #70).

82,384,490 obs / 8,984,193 series; depth-3 DOT-prefix table grain (32,374 catalog
ids, _DOT_TABLE_GRAIN — depth-2 does NOT collapse: p50 112,733 rows/table).
All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.OceanTrade", "unctad_oceantrade")
