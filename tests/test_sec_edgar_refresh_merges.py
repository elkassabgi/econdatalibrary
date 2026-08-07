"""The nightly SEC refresh must MERGE with the store, never replace it.

WHAT THIS PINS (2026-08-07). `tools/refresh_sec_edgar.py` wrote each company as
`pq.write_table(tbl, path)` — a straight replace with one CIK's companyfacts payload. That is
safe only while a company keeps one CIK forever, and companies do not:

    ticker XOM -> CIK 2115436 (Exxon's 2024 re-registration) -> 274 facts from 2024-12-31
    the predecessor CIK 34088                                -> 20,629 facts from 2006-12-31

The refresh fetched the successor and overwrote the file keyed by TICKER, so
r2://econ-data/clean_grouped/sec_edgar/XOM.parquet went from 20,629 rows to 274 and eighteen
years of Exxon fundamentals left the store. Nothing complained: the served CSV still looked
right because it had been derived from a local mirror three months stale, so the two errors
concealed each other until a footer-level diff of all 17,322 objects put them side by side.

Seven catalogued companies have already had a CIK re-assigned — NVRI, CLBK, CBAT, XOM, GORO,
XPRO, UROY — so the next one is a matter of when, not whether.

WHY THE MULTISET TESTS MATTER. `parse_companyfacts` keeps end/val/filed and drops SEC's `start`,
so a filing's 3-month and 9-month figures for the same period end become identical rows. XOM
holds 20,629 rows but only 20,578 distinct 4-tuples. Any "dedup on the natural key" instinct
silently deletes 51 real facts from XOM alone — the first version of the repair tool did exactly
that and its own superset guard caught it. These tests keep that instinct out.
"""
from __future__ import annotations

import datetime as dt
import inspect
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

D = dt.date


def _facts(rows):
    return {"metric": [r[0] for r in rows], "obs_date": [r[1] for r in rows],
            "value": [r[2] for r in rows], "vintage_date": [r[3] for r in rows]}


def _tuple(rows):
    f = _facts(rows)
    return f["metric"], f["obs_date"], f["value"], f["vintage_date"]


def test_successor_cik_payload_does_not_erase_predecessor_history():
    """The XOM case, in miniature."""
    from tools.refresh_sec_edgar import merge_facts
    old = _facts([("us-gaap:Revenues:USD", D(2006, 12, 31), 1.0, D(2007, 2, 1)),
                  ("us-gaap:Revenues:USD", D(2010, 12, 31), 2.0, D(2011, 2, 1)),
                  ("us-gaap:Assets:USD", D(2010, 12, 31), 3.0, D(2011, 2, 1))])
    successor = _tuple([("us-gaap:Revenues:USD", D(2026, 6, 30), 9.0, D(2026, 8, 1))])
    m = merge_facts(old, successor)
    assert len(m[0]) == 4, (
        f"merged to {len(m[0])} rows; the successor CIK's single fact must ADD to the "
        f"predecessor's three, not replace them — that replacement cost XOM 20,629 rows")
    assert min(m[1]) == D(2006, 12, 31) and max(m[1]) == D(2026, 6, 30)


def test_identical_rows_keep_their_multiplicity():
    """3-month and 9-month figures for one period end are indistinguishable once `start` is
    dropped. Collapsing them is data loss that no row-count check would notice as a shrink
    below the payload — only below the store."""
    from tools.refresh_sec_edgar import merge_facts
    rows = [("us-gaap:AmortizationOfIntangibleAssets:USD", D(2015, 9, 30), 5.0, D(2015, 10, 21)),
            ("us-gaap:AmortizationOfIntangibleAssets:USD", D(2015, 9, 30), 7.0, D(2015, 10, 21))]
    assert len(merge_facts(_facts(rows), _tuple(rows))[0]) == 2


def test_a_superset_payload_does_not_inflate_the_store():
    """The ordinary nightly case: the payload already contains everything stored. The merge
    must be idempotent, or every company grows without bound one filing at a time."""
    from tools.refresh_sec_edgar import merge_facts
    stored = [("a:b:USD", D(2020, 3, 31), 1.0, D(2020, 5, 1))]
    payload = stored + [("a:b:USD", D(2020, 6, 30), 2.0, D(2020, 8, 1))]
    assert len(merge_facts(_facts(stored), _tuple(payload))[0]) == 2


def test_a_brand_new_company_needs_no_store():
    from tools.refresh_sec_edgar import merge_facts
    payload = _tuple([("a:b:USD", D(2026, 3, 31), 1.0, D(2026, 5, 1))])
    assert merge_facts(None, payload) == payload


def test_the_write_path_still_merges_before_writing():
    """A unit test on merge_facts passes even if main() stops calling it. Pin the call site."""
    from tools import refresh_sec_edgar as R
    src = inspect.getsource(R.main)
    assert "merge_facts(prior, (metric, odate, vals, vint))" in src, (
        "main() no longer merges the payload into the store before writing — a plain "
        "pq.write_table is a REPLACE, which is what deleted 18 years of XOM")
    assert "prior_facts(client, path)" in src


def test_the_baseline_is_read_from_r2_not_only_the_local_mirror():
    """CI has no local store. If the baseline is local-only, every merge on the runner
    degenerates to a replace and the bug returns wearing an environment for a disguise."""
    from tools import refresh_sec_edgar as R
    src = inspect.getsource(R.prior_facts)
    assert "get_object" in src and "clean_grouped/sec_edgar/" in src


def test_the_r2_client_exists_for_dry_runs_too():
    """The client used to be created only under --apply, so a merge in report mode had
    nothing to compare against and would have reported every company as new."""
    from tools import refresh_sec_edgar as R
    src = inspect.getsource(R.main)
    i_client, i_apply = src.index("client = r2_util.client()"), src.index("if a.apply:")
    assert i_client < i_apply, "the R2 client is still gated behind --apply"
