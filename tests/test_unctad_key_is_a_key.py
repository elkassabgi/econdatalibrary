"""`pull_rows` must refuse a projection that does not identify a row (R814).

`dataset_layout` derives the series key from the DEFAULT LAYOUT's rowAxe/colAxe/pageAxe, and
`pull_rows` then requests exactly those columns. A dimension the publisher does not place on an
axis is projected away by the `$select` - and a projection does not aggregate, it DUPLICATES.

Measured on `US.TradeFoodProcCat_Cat_RCA`: the fact table carries `Flow` (01 Imports,
02 Exports) on no axis, so both flows arrive under one key. The store holds 648,241 observations
over 362,203 distinct (series_key, obs_date) pairs - max group size exactly 2, never 3, which is
what a binary dimension predicts - and its sibling 712,550 over 395,121. Both figures are the CI
errors verbatim (`refusing shrink 648241->362203`, `712550->395121`): never-shrink has been
correctly refusing to collapse 44% of each file since 2026-08-30, so the fetch was never the
problem and the sources are not broken in the way the digest's "SHRINK" label suggests.

THE GUARD IS IN `pull_rows` AND NOT IN `ingest()` ON PURPOSE. That function's own docstring calls
itself "THE single row-building path - the ingest below and every fetcher call this", and
`updater/strategies/fetchers/_unctad.py:64` calls it directly. A guard in `ingest()` would leave
the nightly path unprotected - the two-producers-one-rule mistake made in defillama the same day
(R805).
"""
from __future__ import annotations
import datetime as dt
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from jobs import ingest_unctad_ds as ing  # noqa: E402

D1 = dt.date(1995, 12, 31)
D2 = dt.date(1996, 12, 31)


def _rows(monkeypatch, keys, dates):
    """Drive the real `pull_rows` with a canned fetch, so the guard runs where it lives."""
    monkeypatch.setattr(ing, "dataset_layout",
                        lambda meta: (["A"], "Year", True, False, ["6046"]))
    monkeypatch.setattr(ing, "dataset_time_dim",
                        lambda meta: {"field": "Year", "isTime": True, "codetype": "number"})

    def _fake_facts(ds_name, select, cid, key, meta, tdim, progress=None):
        head = "A_Code,Year,M6046_Value"
        body = [f"{k},{d.year},{i}" for i, (k, d) in enumerate(zip(keys, dates))]
        return ["\n".join([head] + body)]

    monkeypatch.setattr(ing, "facts_csv_chunked", _fake_facts)
    return ing.pull_rows("US.Test", "cid", "key", {"name": "US.Test"})


def test_a_unique_projection_is_returned(monkeypatch):
    k, d, v = _rows(monkeypatch, ["a", "b", "a"], [D1, D1, D2])
    assert len(k) == 3 and len(set(zip(k, d))) == 3


def test_a_projection_that_DUPLICATES_a_key_is_REFUSED(monkeypatch):
    """The Flow case: the same key and year arriving twice because a dimension was projected
    away. Writing that store is what put two catalogued sources into `partial` for a week."""
    with pytest.raises(ing.UnsupportedLayout) as e:
        _rows(monkeypatch, ["a", "a"], [D1, D1])
    msg = str(e.value)
    assert "does not identify a row" in msg
    assert "2 observations collapse to 1" in msg.replace(",", ""), msg
    assert "$metadata" in msg, "the message must name where the missing dimension is declared"


def test_the_refusal_reports_the_real_scale(monkeypatch):
    keys = ["a"] * 4 + ["b"] * 2
    dates = [D1, D1, D2, D2, D1, D2]
    with pytest.raises(ing.UnsupportedLayout) as e:
        _rows(monkeypatch, keys, dates)
    msg = str(e.value).replace(",", "")
    assert "6 observations collapse to 4" in msg, msg
    assert "2 duplicated" in msg and "33.3%" in msg, msg


def test_an_empty_pull_does_not_trip_the_guard(monkeypatch):
    """A dataset that returns nothing is a different condition entirely, already handled by the
    caller; the guard must not convert it into a layout error."""
    k, _d, _v = _rows(monkeypatch, [], [])
    assert k == []


def test_the_guard_lives_in_pull_rows_not_in_ingest():
    """R805's lesson, pinned: `_unctad.py` calls `pull_rows` directly, so a guard placed in
    `ingest()` would protect the manual job and not the nightly fetcher."""
    import ast
    src = open(os.path.join(ROOT, "jobs", "ingest_unctad_ds.py"), encoding="utf-8").read()
    tree = ast.parse(src)
    fns = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "pull_rows" in fns
    body = ast.unparse(fns["pull_rows"])
    assert "does not identify a row" in body, "the guard is not in pull_rows"
    assert "does not identify a row" not in ast.unparse(fns["ingest"]), \
        "the guard must not be duplicated into ingest(); one rule, one place"
