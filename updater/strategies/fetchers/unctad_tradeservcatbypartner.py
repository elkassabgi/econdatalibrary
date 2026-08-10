"""S1 bulk fetcher — UNCTADstat US.TradeServCatByPartner (giant #9, #127).

14,337,747 obs / 1,304,958 series. Depth-2 DOT-prefix table grain (9,243 catalog ids,
_DOT_TABLE_GRAIN) — measured 2026-08-10 over the FULL store, all 14 row groups:
depth-1 is 208 prefixes with a 581,450-row maximum (unusable as tables), depth-3 is
18,419 at p50 42 rows, i.e. double the ids for HALF the median table, and depth-4 is
496,132 which alone would be 62% of D1's entire remaining headroom. Depth-2 sits at
p50 82, max 12,675.

Grain is the binding constraint, not a style choice: D1 measured 9.35 GB of its 10 GB
ceiling (819 bytes/row, ~793,990 rows free), so series grain here would overrun the
whole remaining headroom by 64%. At depth-2 this source costs 1.2% of it.

Keys are Economy.Partner.Flow.Category.Measure; the catalogue id is Economy.Partner.
All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradeServCatByPartner",
                               "unctad_tradeservcatbypartner")
