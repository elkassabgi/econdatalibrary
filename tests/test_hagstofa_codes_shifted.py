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


# ---- review R1130 --------------------------------------------------------------------------------
def test_a_recode_that_shares_no_key_with_the_store_is_refused(tmp_path, monkeypatch):
    """VIN00001: codes changed from positions ('Skóli=0') to label text under the SAME names - no stored
    key came back, nothing could be compared, and 60 new series merged beside 60 frozen ones."""
    recoded = [(f"{PREFIX}:Skóli={SCHOOLS[s]}:Kyn={x}", B, 1.0) for s in range(N) for x in (1, 2)]
    res, store, lm = _run(tmp_path, monkeypatch, recoded, _labels(SCHOOLS))
    assert res.status == "structural" and "RE-CODED" in res.error, res.error
    assert not any(k.startswith(f"{PREFIX}:Skóli=School") for k, _d in store) and lm == {}
    assert "second code system" in res.error and "overwrite" not in res.error, "the RE-CODED harm (R1140)"


# ---- review R1140: the re-code rule is a PROPORTION, so one surviving key cannot defeat it ----------
def test_a_recode_that_keeps_one_total_key_is_still_refused(tmp_path, monkeypatch):
    """R1140 P1: a text total ('Alls') keeps its key through a position->label re-code; 'none came back'
    was defeated by that one survivor and 22 re-coded series merged beside the frozen ones."""
    recoded = [(f"{PREFIX}:Skóli={SCHOOLS[s]}:Kyn={x}", B, 1.0) for s in range(1, N) for x in (1, 2)]
    recoded += [(_k(0, 1), B, STORED[_k(0, 1)]), (_k(0, 2), B, STORED[_k(0, 2)])]   # the survivors
    res, store, lm = _run(tmp_path, monkeypatch, recoded, _labels(SCHOOLS))
    assert res.status == "structural" and "RE-CODED" in res.error and "22 of the 24" in res.error, res.error
    assert not any(k.startswith(f"{PREFIX}:Skóli=School") for k, _d in store)


def test_series_reappearing_from_earlier_dates_are_not_a_recode(tmp_path, monkeypatch):
    """SKO00000's shape: keys absent at the boundary but stored at earlier dates come back - they are
    stored series, so they count for nothing."""
    import pyarrow as _pa
    import pyarrow.parquet as _pq
    rows = _rows(STORED, B)
    res, store, lm = _run(tmp_path, monkeypatch, rows, _labels(SCHOOLS))
    assert res.status in ("ok", "no_change"), res.error
    extra = [f"{PREFIX}:Skóli={90 + i}:Kyn=1" for i in range(4)]            # stored in 2023 only
    t = _pq.read_table(str(tmp_path / "Samfelag.parquet"))
    add = _pa.table({"series_key": extra, "obs_date": _pa.array([dt.date(2023, 12, 31)] * 4), "value": [1.0] * 4})
    _pq.write_table(_pa.concat_tables([t, add.cast(t.schema)]), str(tmp_path / "Samfelag.parquet"))
    rows = _rows(STORED, B) + [(k, B, 2.0) for k in extra]
    monkeypatch.setattr(H, "_fetch_table", lambda sess, db, path, prefix, since, **k: (rows, "data"))
    res = H.update(types.SimpleNamespace(config={}, key="hagstofa/_all"), None)
    assert res.status in ("ok", "no_change"), res.error


def test_the_proportion_is_strict_an_equal_count_merges(tmp_path, monkeypatch):
    """New series must OUTNUMBER the stored ones that came back: a table that doubles (24 + 24) merges."""
    rows = _rows(STORED, B) + [(f"{PREFIX}:Skóli={40 + s}:Kyn={x}", B, 1.0) for s in range(N) for x in (1, 2)]
    res, store, lm = _run(tmp_path, monkeypatch, rows, _labels(SCHOOLS))
    assert "RE-CODED" not in (res.error or ""), res.error


def test_a_new_member_is_far_below_the_proportion(tmp_path, monkeypatch):
    rows = _rows(STORED, B) + [(f"{PREFIX}:Skóli=12:Kyn={x}", B, 4.0) for x in (1, 2)]
    res, store, lm = _run(tmp_path, monkeypatch, rows, _labels(SCHOOLS + ["M"]))
    assert res.status in ("ok", "no_change"), res.error
    assert store.get((f"{PREFIX}:Skóli=12:Kyn=1", B)) == 4.0


def test_a_boundary_that_brings_back_only_a_subset_is_not_a_recode(tmp_path, monkeypatch):
    """R1140 minor: 28 tables hold a subset of their series at the newest period; the old rule read a
    dropped subset as RE-CODED."""
    import pyarrow as _pa
    import pyarrow.parquet as _pq
    res, store, lm = _run(tmp_path, monkeypatch, _rows(STORED, B), _labels(SCHOOLS))
    extra = [f"{PREFIX}:Skóli={90 + i}:Kyn=1" for i in range(4)]            # stored in 2023 only
    t = _pq.read_table(str(tmp_path / "Samfelag.parquet"))
    add = _pa.table({"series_key": extra, "obs_date": _pa.array([dt.date(2023, 12, 31)] * 4), "value": [1.0] * 4})
    _pq.write_table(_pa.concat_tables([t, add.cast(t.schema)]), str(tmp_path / "Samfelag.parquet"))
    # the boundary brings back ONLY series stored at earlier dates: none of the boundary's came back,
    # which the old 'none came back' rule refused as RE-CODED
    rows = [(k, B, 2.0) for k in extra]
    monkeypatch.setattr(H, "_fetch_table", lambda sess, db, path, prefix, since, **k: (rows, "data"))
    res = H.update(types.SimpleNamespace(config={}, key="hagstofa/_all"), None)
    assert "RE-CODED" not in (res.error or ""), res.error


def _three_dim(stored_fn):
    """Schools 0..11 x Kyn 1,2 x Aldur 1,2 - four rows per school, values from stored_fn(s, x, a)."""
    return {f"{PREFIX}:Skóli={s}:Kyn={x}:Aldur={a}": float(stored_fn(s, x, a))
            for s in range(N) for x in (1, 2) for a in (1, 2)}


def test_a_revision_confined_to_one_member_is_not_a_shift_even_with_chance_neighbour_matches():
    """R1130: dropping only the misses at the single miss code refused a one-member revision whenever
    3 of its rows matched a neighbour by chance (modelled 12% of one-member revisions)."""
    stored = _three_dim(lambda s, x, a: 10 * s + 2 * x + a)
    fetched = dict(stored)
    k5 = [k for k in stored if ":Skóli=5:" in k]
    for k in k5[:3]:                                                # 3 of school 5's rows now equal
        fetched[k] = stored[k.replace(":Skóli=5:", ":Skóli=4:")]    # school 4's - by chance
    fetched[k5[3]] = 999.0                                          # the 4th is a new number
    assert H._neighbour_shift(_rows(fetched, B), stored, B, PREFIX, {"Skóli", "Kyn", "Aldur"}) is None


def test_a_two_member_insert_is_a_shift_of_two():
    stored = _three_dim(lambda s, x, a: 10 * s + 2 * x + a)
    fetched = {k: (stored[k.replace(f"Skóli={s}:", f"Skóli={s - 2}:")] if s >= 5 else v)
               for k, v, s in ((k, v, int(k.split("Skóli=")[1].split(":")[0])) for k, v in stored.items())}
    why = H._neighbour_shift(_rows(fetched, B), stored, B, PREFIX, {"Skóli"})
    assert why and "2 code(s) lower" in why, why


def test_a_shift_with_some_revisions_elsewhere_is_still_a_shift_at_80_percent():
    stored = _three_dim(lambda s, x, a: 10 * s + 2 * x + a)
    fetched = {k: (stored[k.replace(f"Skóli={s}:", f"Skóli={s - 1}:")] if s >= 2 else v)
               for k, v, s in ((k, v, int(k.split("Skóli=")[1].split(":")[0])) for k, v in stored.items())}
    moved = [k for k in fetched if fetched[k] != stored[k]]                 # 40 changed rows
    for k in moved[:6]:                                                     # 6 of them revised to new
        fetched[k] = fetched[k] + 0.5                                       # numbers: 34/40 = 85%
    assert H._neighbour_shift(_rows(fetched, B), stored, B, PREFIX, {"Skóli"})


def test_two_neighbour_matches_are_not_enough():
    stored = _three_dim(lambda s, x, a: 10 * s + 2 * x + a)
    fetched = dict(stored)
    fetched[f"{PREFIX}:Skóli=7:Kyn=1:Aldur=1"] = stored[f"{PREFIX}:Skóli=6:Kyn=1:Aldur=1"]
    fetched[f"{PREFIX}:Skóli=9:Kyn=2:Aldur=2"] = stored[f"{PREFIX}:Skóli=8:Kyn=2:Aldur=2"]
    assert H._neighbour_shift(_rows(fetched, B), stored, B, PREFIX, {"Skóli"}) is None


def test_two_matches_left_after_the_insert_code_is_dropped_are_not_enough():
    """3 changes: 2 neighbour matches plus 1 miss at a single code (dropped as an insert) - 2 is not 3."""
    stored = _three_dim(lambda s, x, a: 10 * s + 2 * x + a)
    fetched = dict(stored)
    fetched[f"{PREFIX}:Skóli=7:Kyn=1:Aldur=1"] = stored[f"{PREFIX}:Skóli=6:Kyn=1:Aldur=1"]
    fetched[f"{PREFIX}:Skóli=9:Kyn=2:Aldur=2"] = stored[f"{PREFIX}:Skóli=8:Kyn=2:Aldur=2"]
    fetched[f"{PREFIX}:Skóli=3:Kyn=1:Aldur=2"] = 999.0
    assert H._neighbour_shift(_rows(fetched, B), stored, B, PREFIX, {"Skóli"}) is None


def test_update_ignores_a_label_that_moved_among_codes_the_table_does_not_store(tmp_path, monkeypatch):
    """A 13th and 14th school exist in the metadata but not in the store; a label moving between them
    (a new school inserted at 13) must not refuse a table whose stored schools did not move."""
    stored_map = _labels(SCHOOLS + ["School M", "School N"])
    now = _labels(SCHOOLS + ["Newer school", "School M", "School N"])
    revised = {k: v + 0.5 for k, v in STORED.items()}
    res, store, lm = _run(tmp_path, monkeypatch, _rows(revised, B), now, label_map=stored_map)
    assert res.status in ("ok", "no_change"), getattr(res, "error", None)


def test_the_label_check_looks_only_at_codes_the_table_stores():
    now = _labels(SCHOOLS[:5] + ["New school"] + SCHOOLS[5:11])
    assert H._labels_moved(_labels(SCHOOLS), now, only_codes={"Skóli": {"0", "1"}}) is None
    assert H._labels_moved(_labels(SCHOOLS), now, only_codes={"Skóli": {"0", "7"}})


def test_with_a_label_map_the_value_check_still_runs(tmp_path, monkeypatch):
    """R1130: a shift released together with a relabel of every member passes the label check alone."""
    relabelled = _labels([f"Skóli {c}" for c in "ABCDEFGHIJKL"])            # every label new: no move
    rows = _rows(_shifted(STORED), B)
    res, store, lm = _run(tmp_path, monkeypatch, rows, relabelled, label_map=_labels(SCHOOLS))
    assert res.status == "structural" and "CODES SHIFTED" in res.error and "1 code(s) lower" in res.error


def test_a_mostly_digit_dimension_with_a_total_member_is_checked(tmp_path, monkeypatch):
    names = ["Total"] + SCHOOLS[1:]
    labels = {"Skóli": {("Alls" if i == 0 else str(i)): n for i, n in enumerate(names)},
              "Kyn": {"1": "Boys", "2": "Girls"}}
    rows = _rows(_shifted(STORED), B)
    res, store, lm = _run(tmp_path, monkeypatch, rows, labels)
    assert res.status == "structural" and "CODES SHIFTED" in res.error, res.error


def test_positional_dimensions_come_from_the_keys_when_there_are_no_labels(tmp_path, monkeypatch):
    rows = _rows(_shifted(STORED), B)
    res, store, lm = _run(tmp_path, monkeypatch, rows, None)
    assert res.status == "structural" and "CODES SHIFTED" in res.error, res.error


def test_labels_are_not_seeded_from_an_unchecked_merge(tmp_path, monkeypatch):
    res, store, lm = _run(tmp_path, monkeypatch, _rows(STORED, NXT), _labels(SCHOOLS))
    assert res.status in ("ok", "no_change") and lm == {}, (res.status, lm)


def test_labels_are_saved_only_after_the_merge_succeeds(tmp_path, monkeypatch):
    def _boom(*a, **k):
        raise H.DefinitiveError("merge refused: would shrink")
    monkeypatch.setattr(H.merge, "merge_and_write", _boom)
    revised = {k: v + 0.5 for k, v in STORED.items()}
    with pytest.raises(H.DefinitiveError):
        _run_raw(tmp_path, monkeypatch, _rows(revised, B), _labels(SCHOOLS))
    assert not (tmp_path / H.LABELS_FILE).exists()


def _run_raw(tmp_path, monkeypatch, fetched_rows, labels):
    """_run without the DefinitiveError catch, for a merge that raises."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: [{"db": "Samfelag", "path": PATH, "id": "SKO02102.px",
                                                     "text": "x"}])
    pq.write_table(pa.table({"series_key": list(STORED), "obs_date": pa.array([B] * len(STORED)),
                             "value": list(STORED.values())}), str(tmp_path / "Samfelag.parquet"))

    def _fetch(sess, db, path, prefix, since, **k):
        sess._hagstofa_labels[prefix] = labels
        return fetched_rows, "data"
    monkeypatch.setattr(H, "_fetch_table", _fetch)
    return H.update(types.SimpleNamespace(config={}, key="hagstofa/_all"), None)
