"""S1 bulk fetcher — UNCTADstat US.BiotradeMerchRCA (gate case, #70).

2,795,159 obs / 294,674 series; served at depth-1 DOT-prefix table grain (2,222 catalog ids, _DOT_TABLE_GRAIN). All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.BiotradeMerchRCA", "unctad_biotrademerchrca")
