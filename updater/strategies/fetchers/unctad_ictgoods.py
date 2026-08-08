"""S1 bulk fetcher — UNCTADstat US.IctGoods (giant #2, #70).

20,504,558 obs / 1,374,779 series; depth-3 DOT-prefix table grain (4,608 catalog
ids, _DOT_TABLE_GRAIN). All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.IctGoods", "unctad_ictgoods")
