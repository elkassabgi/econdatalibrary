"""hagstofa: a re-fetch whose value codes were RENUMBERED is refused, not merged (reviews R1119, R1120, R1124).

Hagstofa's value codes are positions in an alphabetical list of labels, not identities: pxen SJA04905
Country 3 is Australia, pxis Land 3 is Azerbaijan, and one new label shifts every code after it. The
dimension NAMES stay the same, so the key-scheme guard cannot see it, and a 'new wins' merge overwrites
each stored series with its neighbour's value.

Round 4 compared VALUES and missed a real one (R1124): SKO02102 (pupils per Reykjavik school) - 1,143 of
its 2,275 stored 2024 values changed, every one equal to the stored value ONE school code lower, and the
guard let it through because counts repeat. So:
  - with a label map from the table's last clean merge, the LABEL decides: a label that now sits under a
    different code is a shift, whatever the values say;
  - with no map yet, the SHIFT SIGNATURE decides: most changed values equal the stored value 1-3
    positions away along a positional dimension, on one consistent offset.
The real update() and merge run; _fetch_table is faked (it records the release's labels as the real one does).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import hagstofa as H  # noqa: E402

PATH = "skolamal/2_grunnskolastig/0_gsNemendur/SKO02102.px"
PREFIX = f"ICE:Samfelag:{PATH.replace('/', ':')}"
B = dt.date(2024, 12, 31)
NXT = dt.date(2025, 12, 31)
N = 12                                   # schools 0..11, two sexes


def _k(school, sex=1):
    return f"{PREFIX}:Skóli={school}:Kyn={sex}"


# pupil counts: small, heavily repeated - the case the round-4 value rule missed
COUNTS = [3, 5, 3, 2, 5, 5, 3, 2, 2, 5, 3, 3]
STORED = {_k(s, x): float(COUNTS[(s + x) % N]) for s in range(N) for x in (1, 2)}


def _shifted(stored, at=5):
    """A school inserted at code `at`: codes >= at move one up, the new school gets 7 pupils."""
    out = {}
    for s in range(N):
        for x in (1, 2):
            if s < at:
                out[_k(s, x)] = stored[_k(s, x)]
            elif s == at:
                out[_k(s, x)] = 7.0
            else:
                out[_k(s, x)] = stored[_k(s - 1, x)]
    return out


def _rows(d, date):
    return [(k, date, v) for k, v in d.items()]


def _labels(names):
    return {"Skóli": {str(i): n for i, n in enumerate(names)}, "Kyn": {"1": "Boys", "2": "Girls"}}


SCHOOLS = [f"School {c}" for c in "ABCDEFGHIJKL"]


# ---- the shift signature (no label map yet) ----------------------------------------------------
def test_a_renumbering_of_repeated_counts_is_detected():
    """The R1124 miss: the round-4 rule saw mostly non-unique values and waved it through."""
    why = H._neighbour_shift(_rows(_shifted(STORED), B), STORED, B, PREFIX, {"Skóli", "Kyn"})
    assert why and "1 code(s) lower along 'Skóli'" in why, why


def test_revisions_are_not_a_shift():
    revised = {k: v + 0.5 for k, v in STORED.items()}
    assert H._neighbour_shift(_rows(revised, B), STORED, B, PREFIX, {"Skóli", "Kyn"}) is None


def test_revised_counts_that_match_other_stored_numbers_but_no_neighbour_are_not_a_shift():
    swapped = dict(STORED)
    for a, b in ((0, 6), (2, 9), (4, 11)):                   # far apart: no consistent offset
        swapped[_k(a)], swapped[_k(b)] = STORED[_k(b)], STORED[_k(a)]
        swapped[_k(a)] = STORED[_k(a)] + 10
    assert H._neighbour_shift(_rows(swapped, B), STORED, B, PREFIX, {"Skóli", "Kyn"}) is None


def test_fewer_than_three_changes_and_non_positional_dimensions_give_no_verdict():
    two = dict(STORED)
    two[_k(1)], two[_k(2)] = STORED[_k(0)], STORED[_k(1)]
    assert H._neighbour_shift(_rows(two, B), STORED, B, PREFIX, {"Skóli"}) is None
    assert H._neighbour_shift(_rows(_shifted(STORED), B), STORED, B, PREFIX, set()) is None
    assert H._neighbour_shift(_rows(_shifted(STORED), B), {}, B, PREFIX, {"Skóli"}) is None
    assert H._neighbour_shift(_rows(_shifted(STORED), NXT), STORED, B, PREFIX, {"Skóli"}) is None


def test_nan_equals_nan():
    assert H._same(float("nan"), float("nan")) and not H._same(float("nan"), 1.0)


# ---- identity by label ----------------------------------------------------------------------------
def test_a_label_moved_to_another_code_is_a_shift():
    now = _labels(SCHOOLS[:5] + ["New school"] + SCHOOLS[5:11])
    why = H._labels_moved(_labels(SCHOOLS), now)
    assert why and "'School F': code 5 -> 6" in why, why


def test_a_new_label_at_the_end_and_a_rename_in_place_are_not_shifts():
    assert H._labels_moved(_labels(SCHOOLS), _labels(SCHOOLS + ["New school"])) is None
    renamed = SCHOOLS[:3] + ["School D (merged)"] + SCHOOLS[4:]
    assert H._labels_moved(_labels(SCHOOLS), _labels(renamed)) is None


def test_the_label_decides_even_when_one_change_is_all_the_values_show():
    """An insert at the second-last code moves ONE series: the value signature cannot see it, the label can."""
    now = _labels(SCHOOLS[:10] + ["New school", SCHOOLS[10]])
    assert H._labels_moved(_labels(SCHOOLS[:11]), now)


# ---- through update() -------------------------------------------------------------------------------
def _run(tmp_path, monkeypatch, fetched_rows, labels, label_map=None, placeholder=False):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: [{"db": "Samfelag", "path": PATH, "id": "SKO02102.px",
                                                     "text": "x"}])
    older = {k: v + 1.0 for k, v in STORED.items()}
    keys = list(STORED) + list(older)
    dates = [B] * len(STORED) + [dt.date(2023, 12, 31)] * len(older)
    vals = list(STORED.values()) + list(older.values())
    if placeholder:                     # MAN02007's far-future placeholder row: not the boundary
        keys.append(_k(0, 1))
        dates.append(dt.date(2100, 12, 31))
        vals.append(99.0)
    pq.write_table(pa.table({"series_key": keys, "obs_date": pa.array(dates), "value": vals}),
                   str(tmp_path / "Samfelag.parquet"))
    if label_map is not None:
        (tmp_path / H.LABELS_FILE).write_text(json.dumps({PREFIX: label_map}), encoding="utf-8")

    def _fetch(sess, db, path, prefix, since, **k):
        sess._hagstofa_labels[prefix] = labels
        return fetched_rows, "data"
    monkeypatch.setattr(H, "_fetch_table", _fetch)
    unit = types.SimpleNamespace(config={}, key="hagstofa/_all")
    try:
        res = H.update(unit, None)
    except H.DefinitiveError as e:
        res = types.SimpleNamespace(status="structural", error=str(e))
    t = pq.read_table(str(tmp_path / "Samfelag.parquet")).to_pylist()
    store = {(r["series_key"], r["obs_date"]): r["value"] for r in t}
    lm = tmp_path / H.LABELS_FILE
    return res, store, (json.loads(lm.read_text(encoding="utf-8")) if lm.exists() else {})


NOW_SHIFTED = _labels(SCHOOLS[:5] + ["New school"] + SCHOOLS[5:11])


def test_update_refuses_the_sko02102_shape_with_no_label_map_and_names_it(tmp_path, monkeypatch, capsys):
    rows = _rows(_shifted(STORED), B) + _rows(_shifted(STORED), NXT)
    res, store, lm = _run(tmp_path, monkeypatch, rows, NOW_SHIFTED)
    assert res.status == "structural" and "CODES SHIFTED" in res.error and PATH in res.error, res.error
    assert "CODES SHIFTED" in capsys.readouterr().out, "printed, not only in the clipped result"
    assert all(store[(k, B)] == v for k, v in STORED.items()) and not any(d == NXT for _k2, d in store)
    assert lm == {}, "a refused table seeds no labels"


def test_update_refuses_by_label_once_a_map_exists(tmp_path, monkeypatch):
    rows = _rows(_shifted(STORED, at=10), B)                 # values: only 4 series moved
    res, store, lm = _run(tmp_path, monkeypatch, rows, _labels(SCHOOLS[:10] + ["New school"] + SCHOOLS[10:11]),
                          label_map=_labels(SCHOOLS))
    assert res.status == "structural" and "label(s) of 'Skóli' moved" in res.error, res.error


def test_the_boundary_is_the_newest_real_date_not_a_placeholder(tmp_path, monkeypatch):
    rows = _rows(_shifted(STORED), B)
    res, store, lm = _run(tmp_path, monkeypatch, rows, NOW_SHIFTED, placeholder=True)
    assert res.status == "structural" and "CODES SHIFTED" in res.error, res.error


def test_negative_control_a_revision_merges_and_seeds_the_label_map(tmp_path, monkeypatch):
    revised = {k: v + 0.5 for k, v in STORED.items()}
    res, store, lm = _run(tmp_path, monkeypatch, _rows(revised, B) + _rows(STORED, NXT), _labels(SCHOOLS))
    assert res.status in ("ok", "no_change"), getattr(res, "error", None)
    assert store[(_k(3), B)] == STORED[_k(3)] + 0.5 and store[(_k(3), NXT)] == STORED[_k(3)]
    assert lm == {PREFIX: _labels(SCHOOLS)}, "seeded from this clean merge"


def test_a_table_with_no_stored_value_to_compare_is_named(tmp_path, monkeypatch, capsys):
    res, store, lm = _run(tmp_path, monkeypatch, _rows(STORED, NXT), _labels(SCHOOLS))
    assert "1 table(s) merged with no stored value at their newest period" in capsys.readouterr().out


def test_fetch_with_meta_records_the_release_labels_without_the_time_axis(monkeypatch):
    meta = {"variables": [{"code": "Skóli", "values": ["0", "1"], "valueTexts": ["School A", "School B"]},
                          {"code": "Ár", "values": ["2024", "2025"], "valueTexts": ["2024", "2025"], "time": True}]}
    monkeypatch.setattr(H, "_post_data", lambda sess, url, body: None)
    monkeypatch.setattr(H.time, "sleep", lambda s: None)
    sess = types.SimpleNamespace(_hagstofa_labels={})
    H._fetch_with_meta(sess, "u", meta, PATH, PREFIX, dt.date(2024, 12, 31))
    assert sess._hagstofa_labels == {PREFIX: {"Skóli": {"0": "School A", "1": "School B"}}}


def test_a_tree_search_cut_by_the_budget_is_booked_deferred_not_red(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: [{"db": "Samfelag", "path": PATH, "id": "x", "text": "x"},
                                                     {"db": "Samfelag", "path": PATH + "2", "id": "y", "text": "y"}])
    monkeypatch.setattr(H, "_fetch_table", lambda sess, db, path, prefix, since, **k:
                        ([], "deferred") if path == PATH else ([(f"{prefix}:Skóli=0", B, 1.0)], "data"))
    res = H.update(types.SimpleNamespace(config={}, key="hagstofa/_all"), None)
    assert res.status == "partial" and "1 deferred" in res.error and "table-tree search" in res.error, res.error
