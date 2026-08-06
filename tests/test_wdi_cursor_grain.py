"""worldbank_wdi: cursors fold to INDICATOR grain — the catalog id form.

Pinned 2026-08-05: store keys are 'WDI:<indicator>:<geo>' but the catalog serves one id
per indicator, so store-key cursors left 10,255 changed keys unmapped every run and the
1,486 indicator CSVs never re-derived from CI.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pyarrow as pa

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def test_series_maxes_folds_to_indicator():
    from updater.strategies.fetchers import worldbank_wdi as W
    tbl = pa.table({
        "series_key": ["WDI:AG.CON.FERT.ZS:AFG", "WDI:AG.CON.FERT.ZS:DEU",
                       "WDI:SP.POP.TOTL:USA", "odd-key-form"],
        "obs_date": pa.array([dt.date(2024, 1, 1), dt.date(2025, 1, 1),
                              dt.date(2023, 1, 1), dt.date(2022, 1, 1)], pa.date32()),
    })
    out = W._series_maxes(tbl)
    assert out == {"AG.CON.FERT.ZS": "2025-01-01",   # max over both geos
                   "SP.POP.TOTL": "2023-01-01",
                   "odd-key-form": "2022-01-01"}, out
