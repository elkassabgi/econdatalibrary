"""S1 bulk fetcher — UNCTADstat US.NonPlasticSubstsTradeByPartner (giant #7, #70).

100,233,168 obs / 9,298,529 series — the family's largest series count. Depth-2
DOT-prefix table grain (22,079 catalog ids, _DOT_TABLE_GRAIN). Its Partner x
Product interior is dense enough that ONE Economy x ONE Year exceeds the Facts
size cap, which is why the chunker recurses across every key dim. All machinery
shared via _unctad.py.

WINDOWED (2026-10-30, PLAN R10.3 step 4): the one unctad source opted into _unctad's windowed update -
a stop before the pass's window end between chunk requests, the chunks kept for a later pass, a
merge-measured change set and a two-phase owed mark. Its stored vintage is NULL, so every due turn is
forced; its 2026-10-08 turn re-walked release 10009 from nothing and was hard-stopped at 8,326 s while
still fetching, before any merge. Whether the store holds 10009 is not known. See _unctad.py, "THE
WINDOWED UPDATE".
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.NonPlasticSubstsTradeByPartner",
                               "unctad_nonplasticsubststradebypartner", window=True)
