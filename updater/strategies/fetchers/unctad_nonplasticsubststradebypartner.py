"""S1 bulk fetcher — UNCTADstat US.NonPlasticSubstsTradeByPartner (giant #7, #70).

100,233,168 obs / 9,298,529 series — the family's largest series count. Depth-2
DOT-prefix table grain (22,079 catalog ids, _DOT_TABLE_GRAIN). Its Partner x
Product interior is dense enough that ONE Economy x ONE Year exceeds the Facts
size cap, which is why the chunker recurses across every key dim. All machinery
shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.NonPlasticSubstsTradeByPartner",
                               "unctad_nonplasticsubststradebypartner")
