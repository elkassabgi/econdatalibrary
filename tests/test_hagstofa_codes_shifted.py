"""hagstofa: a re-fetch whose value codes were RENUMBERED is refused, not merged (reviews R1119, R1120).

Hagstofa's value codes are positions in an alphabetical list of labels, not identities: pxen SJA04905
Country 3 is Australia, pxis Land 3 is Azerbaijan, and pxis has since added Liberia (41) and Tokelau
(69), which shifts every code after them. The dimension NAMES stay the same, so the key-scheme guard
cannot see it, and a 'new wins' merge overwrites each stored series with its neighbour's value.
Measured (R1120): SJA04901 fetched from pxis - 134 of 214 stored 2024 pairs differ, 200 of 284 stored
2024 series match ONE CODE HIGHER. The fetch re-reads the newest stored period, so the guard compares
there: values that moved to another key = shifted; values that became new numbers = revisions.
The real update() and merge run; _fetch_table is faked.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import hagstofa as H  # noqa: E402

PATH = "sjavarutvegur/utf/SJA04901.px"
PREFIX = "ICE:Atvinnuvegir:sjavarutvegur:utf:SJA04901.px"
B = dt.date(2024, 12, 31)
NXT = dt.date(2025, 12, 31)


def _k(i):
    return f"{PREFIX}:Fisktegund={i}"


STORED = {_k(i): v for i, v in enumerate([755.0, 333200.0, 2543618.0, 41.5, 9001.0, 17.25])}


def _shift(stored, by=1):
    """The same values one code higher, as a renumbering produces (the last value falls off the end,
    a new code 0 appears)."""
    vals = [stored[_k(i)] for i in range(len(stored))]
    return {_k(i + by): v for i, v in enumerate(vals[:len(vals) - by])} | {_k(0): 12.0}


def _rows(d, date):
    return [(k, date, v) for k, v in d.items()]


# ---- the predicate ----------------------------------------------------------------------------
def test_a_renumbering_is_detected():
    reason = H._codes_shifted(_rows(_shift(STORED), B), STORED, B)
    assert reason and "renumbered" in reason, reason


def test_revisions_are_not_a_shift():
    """UMH51101: all 14 boundary values revised to NEW numbers - a revision, merged as ever."""
    revised = {k: v * 1.013 for k, v in STORED.items()}
    assert H._codes_shifted(_rows(revised, B), STORED, B) is None


def test_an_identical_refetch_and_a_new_period_only_are_not_a_shift():
    assert H._codes_shifted(_rows(STORED, B) + _rows(_shift(STORED), NXT), STORED, B) is None
    assert H._codes_shifted(_rows(_shift(STORED), NXT), STORED, B) is None, "no boundary rows fetched"


def test_values_stored_under_several_keys_prove_nothing():
    """Zeros and repeated totals: a differing value that equals the stored value of several keys is
    not evidence it moved."""
    stored = {_k(i): 0.0 for i in range(6)}
    stored[_k(6)] = 5.0
    fetched = {k: (5.0 if k == _k(6) else 0.0) for k in stored}
    fetched[_k(0)], fetched[_k(1)] = 7.0, 8.0          # two real revisions
    assert H._codes_shifted(_rows(fetched, B), stored, B) is None
    fetched = {_k(i): 0.0 for i in range(7)}
    fetched[_k(0)] = 5.0                                # 5.0 moved from key 6 to key 0: one move only
    assert H._codes_shifted(_rows(fetched, B), stored, B) is None, "one moved value is below the floor"


def test_a_shift_needs_the_moves_to_be_at_least_half_of_the_differences():
    fetched = dict(STORED)
    fetched[_k(0)], fetched[_k(1)] = STORED[_k(1)], STORED[_k(0)]        # 2 swaps
    for i in range(2, 6):
        fetched[_k(i)] = STORED[_k(i)] + 1.0                              # 4 revisions
    assert H._codes_shifted(_rows(fetched, B), STORED, B) is None
    fetched[_k(2)] = STORED[_k(3)]                                        # now 3 moved of 6 differing
    assert H._codes_shifted(_rows(fetched, B), STORED, B)


def test_nan_is_equal_to_nan_and_no_boundary_means_no_verdict():
    stored = dict(STORED) | {_k(9): float("nan")}
    fetched = dict(stored)
    assert H._codes_shifted(_rows(fetched, B), stored, B) is None
    # NaN pairs are not differences, so they cannot dilute a real shift below the half rule
    stored = dict(STORED) | {_k(10 + i): float("nan") for i in range(5)}
    fetched = dict(stored)
    fetched[_k(0)], fetched[_k(1)] = STORED[_k(1)], STORED[_k(0)]
    assert H._codes_shifted(_rows(fetched, B), stored, B)
    assert H._codes_shifted(_rows(_shift(STORED), B), {}, B) is None
    assert H._codes_shifted(_rows(_shift(STORED), B), STORED, None) is None


# ---- through update() ---------------------------------------------------------------------------
def _run(tmp_path, monkeypatch, fetched_rows):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: [{"db": "Atvinnuvegir", "path": PATH, "id": "SJA04901.px",
                                                     "text": "x"}])
    older = {k: v - 1.0 for k, v in STORED.items()}
    # the boundary rows FIRST: only the newest period's values may be compared, whatever the row order
    pq.write_table(pa.table({
        "series_key": list(STORED) + list(older),
        "obs_date": pa.array([B] * len(STORED) + [dt.date(2023, 12, 31)] * len(older)),
        "value": list(STORED.values()) + list(older.values())}), str(tmp_path / "Atvinnuvegir.parquet"))
    monkeypatch.setattr(H, "_fetch_table", lambda sess, db, path, prefix, since, **k: (fetched_rows, "data"))
    unit = types.SimpleNamespace(config={}, key="hagstofa/_all")
    try:
        res = H.update(unit, None)
    except H.DefinitiveError as e:
        res = types.SimpleNamespace(status="structural", error=str(e))
    t = pq.read_table(str(tmp_path / "Atvinnuvegir.parquet")).to_pylist()
    return res, {(r["series_key"], r["obs_date"]): r["value"] for r in t}


def test_update_refuses_a_renumbered_fetch_and_names_it(tmp_path, monkeypatch):
    res, store = _run(tmp_path, monkeypatch, _rows(_shift(STORED), B) + _rows(_shift(STORED), NXT))
    assert res.status == "structural" and "CODES SHIFTED" in res.error and PATH in res.error, res.error
    assert all(store[(k, B)] == v for k, v in STORED.items()), "no stored value overwritten"
    assert not any(d == NXT for _k2, d in store), "nothing merged"


def test_negative_control_a_revision_and_a_new_year_merge(tmp_path, monkeypatch):
    revised = {k: v * 1.013 for k, v in STORED.items()}
    res, store = _run(tmp_path, monkeypatch, _rows(revised, B) + _rows(STORED, NXT))
    assert res.status in ("ok", "no_change"), getattr(res, "error", None)
    assert store[(_k(2), B)] == pytest.approx(STORED[_k(2)] * 1.013)
    assert store[(_k(2), NXT)] == STORED[_k(2)]
