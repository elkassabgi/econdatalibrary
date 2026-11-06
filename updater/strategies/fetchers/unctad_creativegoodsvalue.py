"""S1 bulk fetcher — UNCTADstat US.CreativeGoodsValue (giant #4, #70).

75,560,775 obs / 5,487,666 series; depth-2 DOT-prefix table grain (4,053 catalog
ids, _DOT_TABLE_GRAIN). All machinery shared via _unctad.py.
WINDOWED (2026-11-06): opted into _unctad's windowed update, like unctad_nonplasticsubststradebypartner -
a stop before the pass's window end between chunk requests, the chunks kept for a later pass, a
merge-measured change set and a two-phase owed mark. Why: the local passes hard-stopped this source's
un-windowed update (no stop of its own) and lost the rest of the pass. Size: one of the three sibling turns the 60-min assembly reserve was measured on.
See _unctad.py, "THE WINDOWED UPDATE".
"""
from __future__ import annotations

from ._unctad import make, windowed_admission_reserve_min, windowed_stop_tail_min

current_vintage, update = make("US.CreativeGoodsValue", "unctad_creativegoodsvalue", window=True)


def window_admission_reserve_min() -> float:
    """The start gate's reserve (registry `window_stop: true`): margin floor 5 + minimum network time 10 +
    assembly reserve 60 = 75 min by default. See _unctad.windowed_admission_reserve_min."""
    return windowed_admission_reserve_min("unctad_creativegoodsvalue")


def window_stop_tail_min() -> float:
    """Where the windowed update stops, before the run ceiling: margin floor 5 + assembly reserve 60 = 65 min by
    default (for tools/measure_ci_lag.py's replay). See _unctad.windowed_stop_tail_min."""
    return windowed_stop_tail_min("unctad_creativegoodsvalue")


def window_stop_piece_min() -> float:
    """The window a stop needs to count as progress: 0 - window_stop is "progress" once one Facts POST has
    returned (win.posts), and the first one is made whenever the first network check passes."""
    return 0.0
