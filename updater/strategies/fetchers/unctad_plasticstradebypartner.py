"""S1 bulk fetcher — UNCTADstat US.PlasticsTradebyPartner (giant #1, #70).

14,864,191 obs / 1,177,515 series; depth-2 DOT-prefix table grain (1,615 catalog
ids, _DOT_TABLE_GRAIN). All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.PlasticsTradebyPartner", "unctad_plasticstradebypartner")
