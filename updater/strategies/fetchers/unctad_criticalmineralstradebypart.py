"""S1 bulk fetcher — UNCTADstat US.CriticalMinerals_TradeByPart (giant #8, #70).

109,956,508 obs / 8,589,597 series. Depth-2 DOT-prefix table grain (16,103 catalog
ids, _DOT_TABLE_GRAIN) — measured 2026-08-09 over the full store: depth-1 is 64
prefixes at p50 1,463,126 rows (unusable as tables), depth-3 is 29,459 at p50 2,187,
i.e. nearly double the ids for a SMALLER median table, and D1 rows are the scarce
resource at 93.3% of its ceiling. Depth-2 sits at p50 3,460, max 41,232.

The Facts size cap bites hard here: one Economy x one Year still exceeds it, so the
chunker recurses Years -> CriticalMinerals (64 codes) -> Economies (273 codes). All
machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.CriticalMinerals_TradeByPart",
                               "unctad_criticalmineralstradebypart")
