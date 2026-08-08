"""S1 bulk fetcher — UNCTADstat US.GSTP_TradeMatrix (giant #3, #70).

76,643,660 obs / 4,378,337 series; depth-2 DOT-prefix table grain (18,714 catalog
ids, _DOT_TABLE_GRAIN). All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.GSTP_TradeMatrix", "unctad_gstptradematrix")
