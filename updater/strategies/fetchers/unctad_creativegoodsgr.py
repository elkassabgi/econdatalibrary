"""S1 bulk fetcher — UNCTADstat US.CreativeGoodsGR (giant #5, #70).

20,054,148 obs / 3,597,379 series; depth-2 DOT-prefix table grain (3,986 catalog
ids, _DOT_TABLE_GRAIN). Period-axis dataset (8-digit YYYYYYYY span codes; keys
carry |SPAN=nY suffixes). All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.CreativeGoodsGR", "unctad_creativegoodsgr")
