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
WINDOWED (2026-11-06): opted into _unctad's windowed update, like unctad_nonplasticsubststradebypartner -
a stop before the pass's window end between chunk requests, the chunks kept for a later pass, a
merge-measured change set and a two-phase owed mark. Why: the local passes hard-stopped this source's
un-windowed update (no stop of its own) and lost the rest of the pass. Size: 14,337,747 rows, inside the 75-110M-row sibling range the 60-min reserve was measured on.
See _unctad.py, "THE WINDOWED UPDATE".
"""
from __future__ import annotations

from ._unctad import make, windowed_admission_reserve_min, windowed_stop_tail_min

current_vintage, update = make("US.TradeServCatByPartner",
                               "unctad_tradeservcatbypartner", window=True)


def window_admission_reserve_min() -> float:
    """The start gate's reserve (registry `window_stop: true`): margin floor 5 + minimum network time 10 +
    assembly reserve 60 = 75 min by default. See _unctad.windowed_admission_reserve_min."""
    return windowed_admission_reserve_min("unctad_tradeservcatbypartner")


def window_stop_tail_min() -> float:
    """Where the windowed update stops, before the run ceiling: margin floor 5 + assembly reserve 60 = 65 min by
    default (for tools/measure_ci_lag.py's replay). See _unctad.windowed_stop_tail_min."""
    return windowed_stop_tail_min("unctad_tradeservcatbypartner")


def window_stop_piece_min() -> float:
    """The window a stop needs to count as progress: 0 - window_stop is "progress" once one Facts POST has
    returned (win.posts), and the first one is made whenever the first network check passes."""
    return 0.0
