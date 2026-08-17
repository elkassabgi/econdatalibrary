"""S1 bulk fetcher — UNCTADstat US.TradeFoodProcByCat (giant #15, #127).

336,738,347 obs / 22,682,414 series across 4 measures, pulled 2026-08-17 with
the measure sums matching the written parquet EXACTLY (M4023 85,570,215 +
M0100 100,446,471 + M5066 73,820,788 + M5058 76,900,873). The grid ceiling was
581M per measure; observed density ~14% — the family pattern. Depth-2
DOT-prefix table grain: 20,578 catalog ids (~16.4k obs/table), counted by the
cataloguer with an exact prefixes=inserted=total gate. Keys are
ProcessFoodCategory.Economy.Partner.Flow + Year; machinery shared via
_unctad.py; vintage is the publisher's version|lastUpdated stamp
('10003|2025-03-18T13:57:58' at pull time).
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradeFoodProcByCat", "unctad_tradefoodprocbycat")
