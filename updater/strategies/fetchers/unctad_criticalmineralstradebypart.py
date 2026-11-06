"""S1 bulk fetcher — UNCTADstat US.CriticalMinerals_TradeByPart (giant #8, #70).

109,956,508 obs / 8,589,597 series. Depth-2 DOT-prefix table grain (16,103 catalog
ids, _DOT_TABLE_GRAIN) — measured 2026-08-09 over the full store: depth-1 is 64
prefixes at p50 1,463,126 rows (unusable as tables), depth-3 is 29,459 at p50 2,187,
i.e. nearly double the ids for a SMALLER median table, and D1 rows are the scarce
resource at 93.3% of its ceiling. Depth-2 sits at p50 3,460, max 41,232.

The Facts size cap bites hard here: one Economy x one Year still exceeds it, so the
chunker recurses Years -> CriticalMinerals (64 codes) -> Economies (273 codes). All
machinery shared via _unctad.py.
WINDOWED (2026-11-06): opted into _unctad's windowed update, like unctad_nonplasticsubststradebypartner -
a stop before the pass's window end between chunk requests, the chunks kept for a later pass, a
merge-measured change set and a two-phase owed mark. Why: the local passes hard-stopped this source's
un-windowed update (no stop of its own) and lost the rest of the pass. Size: one of the three sibling turns the 60-min assembly reserve was measured on.
See _unctad.py, "THE WINDOWED UPDATE".
"""
from __future__ import annotations

from ._unctad import make, windowed_admission_reserve_min, windowed_stop_tail_min

current_vintage, update = make("US.CriticalMinerals_TradeByPart",
                               "unctad_criticalmineralstradebypart", window=True)


def window_admission_reserve_min() -> float:
    """The start gate's reserve (registry `window_stop: true`): margin floor 5 + minimum network time 10 +
    assembly reserve 60 = 75 min by default. See _unctad.windowed_admission_reserve_min."""
    return windowed_admission_reserve_min("unctad_criticalmineralstradebypart")


def window_stop_tail_min() -> float:
    """Where the windowed update stops, before the run ceiling: margin floor 5 + assembly reserve 60 = 65 min by
    default (for tools/measure_ci_lag.py's replay). See _unctad.windowed_stop_tail_min."""
    return windowed_stop_tail_min("unctad_criticalmineralstradebypart")


def window_stop_piece_min() -> float:
    """The window a stop needs to count as progress: 0 - window_stop is "progress" once one Facts POST has
    returned (win.posts), and the first one is made whenever the first network check passes."""
    return 0.0
