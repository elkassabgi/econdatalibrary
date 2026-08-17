"""S1 bulk fetcher — UNCTADstat US.TradeFoodCatByProc (giant #14, #127).

394,118,603 obs / 26,579,759 series across 4 measures, pulled 2026-08-16 with
the measure sums matching the written parquet EXACTLY (M4023 99,846,521 +
M0100 117,308,365 + M5066 86,941,816 + M5058 90,021,901). The grid ceiling was
673M per measure; observed density ~17% — the family pattern. Depth-2
DOT-prefix table grain: 23,906 catalog ids (~16.5k obs/table), counted by the
cataloguer with an exact prefixes=inserted=total gate. Keys are
ProcessFoodCategory.Economy.Partner.Flow + Year; machinery shared via
_unctad.py; vintage is the publisher's version|lastUpdated stamp
('10003|2025-03-18T13:57:58' at pull time).
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradeFoodCatByProc", "unctad_tradefoodcatbyproc")
