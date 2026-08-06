"""fhfa: cursor collection is freshest-file-first, so the cap cannot hide recency.

Pinned 2026-08-06: alphabetical iteration filled the 50,000-cursor cap with annual_*
series (newest 2025-12-31) before hpi_master's monthly series (measured 2026-05-01,
level with the publisher) ever reported — the gate RED-DATA'd a current source at a
218-day phantom age.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def test_hpi_master_first_rest_alphabetical():
    names = ["annual_hpi_at_cbsa.parquet", "hpi_master.parquet",
             "annual_hpi_at_state.parquet", "hpi_at_3zip_quarterly.parquet"]
    ordered = sorted(names, key=lambda n: (n != "hpi_master.parquet", n))
    assert ordered[0] == "hpi_master.parquet"
    assert ordered[1:] == sorted(n for n in names if n != "hpi_master.parquet")


def test_fetcher_uses_the_priority_key():
    import inspect
    from updater.strategies.fetchers import fhfa as F
    src = inspect.getsource(F.update)
    assert 'n != "hpi_master.parquet"' in src, \
        "cursor collection must prioritize hpi_master or the cap hides recency"
