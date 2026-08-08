"""S1 bulk fetcher — UNCTADstat US.CreativeGoodsValue (giant #4, #70).

75,560,775 obs / 5,487,666 series; depth-2 DOT-prefix table grain (4,053 catalog
ids, _DOT_TABLE_GRAIN). All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.CreativeGoodsValue", "unctad_creativegoodsvalue")
