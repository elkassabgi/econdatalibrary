"""cso: a never-stored matrix whose time axis cannot be dated must not lead the queue (2026-10-18).

Measured in run 39161839205: 59 of the 60 matrices in every batch were never-stored CPNI*, EAACC*
and EOSA* tables booked "unparsed" transient. Every one has an authoritative TLIST(A1) axis whose
only codes are slash forms ('2021/2022', '2012/17', '2023/2024', '2013/16', '2017/21') that the
grammar does not date. Not stored -> unheld -> sorted first -> failed again, so about one useful
matrix moved per run while 10,844 changed matrices waited.

The bodies in tests/fixtures/cso/ are the REAL ReadDataset responses (probed 2026-10-18, contact
block removed), parsed by the REAL wrapped ingester - not a cube built from the hypothesis (R266).
The real update(), cursor, held and tally logic run; only the network and the subject map are faked.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import health  # noqa: E402
from updater.strategies.fetchers import cso  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "cso")
REAL_INGESTER = cso._ingester          # captured before any test patches it


def _fixture(name):
    with open(os.path.join(FIX, f"{name}.json"), encoding="utf-8") as f:
        return json.load(f)


def _good_body(mtr, years=("2024", "2025")):
    return {"id": ["STATISTIC", "TLIST(A1)"], "size": [1, len(years)],
            "role": {"time": ["TLIST(A1)"]},
            "dimension": {"STATISTIC": {"category": {"index": {f"{mtr}C1": 0}}},
                          "TLIST(A1)": {"category": {"index": {y: i for i, y in enumerate(years)}}}},
            "value": [1.0 + i for i in range(len(years))]}


BODIES = {"CPNI20": _fixture("CPNI20"), "EOSA01": _fixture("EOSA01"), "EAACC22": _fixture("EAACC22"),
          "GOOD1": _good_body("GOOD1"), "GOOD2": _good_body("GOOD2")}
UNDATED = ("CPNI20", "EOSA01", "EAACC22")


def _ingester_on(bodies, asked):
    mod = REAL_INGESTER()               # the fetcher's own wrapped module: same parse_date chain

    def _get_json(url, retries=3):
        mtr = url.split("/")[-4]
        asked.append(mtr)
        return bodies.get(mtr)
    mod.get_json = _get_json
    mod.RATE = 0
    return mod


def _store(path, mtrs):
    pq.write_table(pa.table({
        "series_key": [f"CSO:{m}:STATISTIC={m}C1" for m in mtrs],
        "obs_date": pa.array([dt.date(2023, 12, 31)] * len(mtrs), pa.date32()),
        "value": [0.5] * len(mtrs)}), str(path))


def _run(tmp_path, monkeypatch, stamps, held=("GOOD1", "GOOD2"), cap=60, subject=lambda m: "2_Education"):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.delenv("CSO_ONLY_MATRICES", raising=False)
    monkeypatch.setattr(cso.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(cso, "MAX_TABLES", cap)
    monkeypatch.setattr(cso, "_collection_updates", lambda epoch, timeout=300: (dict(stamps), None))
    monkeypatch.setattr(cso, "search_vintages", lambda *a, **k: {})
    monkeypatch.setattr(cso, "_matrix_subject_map", lambda force=False: {m: subject(m) for m in stamps})
    if not (tmp_path / "_held.json").exists():
        (tmp_path / "_held.json").write_text(json.dumps(sorted(held)))
    asked = []
    ing = _ingester_on(BODIES, asked)
    monkeypatch.setattr(cso, "_ingester", lambda: ing)
    res = cso.update(types.SimpleNamespace(config={}, key="cso/_all"), None)
    cursor = json.loads((tmp_path / "_collupd.json").read_text())
    held_after = set(json.loads((tmp_path / "_held.json").read_text()))
    return res, cursor, held_after, asked


def _stamps(undated="2026-10-01T11:00:00Z", good="2026-09-01T11:00:00Z", goods=("GOOD1",)):
    s = {m: undated for m in UNDATED}
    s.update({g: good for g in goods})
    return s


# ---- the outcome ------------------------------------------------------------------------------

def test_the_live_bodies_are_undated_time_not_unparsed():
    ing = REAL_INGESTER()
    for m in UNDATED:
        rows, outcome = [], None
        rows = ing.parse_jsonstat2(BODIES[m], f"CSO:{m}")
        assert rows == [], (m, rows[:2])
        assert ing._why_unparsed(BODIES[m]) == "undated_time", m


def test_one_datable_code_keeps_it_unparsed():
    ing = REAL_INGESTER()
    body = json.loads(json.dumps(BODIES["CPNI20"]))
    body["dimension"]["TLIST(A1)"]["category"]["index"] = {"2022": 0}
    body["dimension"]["TLIST(A1)"]["category"]["label"] = {"2022": "2022"}
    assert ing._why_unparsed(body) == "unparsed", "a datable code means the gap is not the time axis"


def test_census_2021_2022_is_not_dated_by_the_split_year_rule():
    """'2021/2022' is the joint IE (2022) + NI (2021) census. The split-year rule would date it
    2021-12-31. Neither parser may date any of the slash forms; the control shows the split-year
    rule exists (so this test can tell the two apart)."""
    ing = REAL_INGESTER()
    for code in ("2021/2022", "2001/2002", "2023/2024", "2012/17", "2017/21", "2013/16"):
        assert ing.parse_date(code) is None, code
        assert cso._parse_period(code, ing.parse_date) is None, code
    assert ing.parse_date("2021-2022") == dt.date(2021, 12, 31), "control: the split-year rule"
    assert ing.parse_jsonstat2(BODIES["CPNI20"], "CSO:CPNI20") == []


# ---- the queue --------------------------------------------------------------------------------

def test_an_undatable_never_stored_matrix_does_not_lead_the_queue(tmp_path, monkeypatch):
    """The discriminating test. Cap 3, three never-stored undated matrices with the newest
    stamps, one stored changed matrix behind them. On origin/main the second run asks the same
    three again and GOOD1 is never reached."""
    _store(tmp_path / "1_Sub.parquet", ["GOOD1"])
    subj = lambda m: "1_Sub" if m.startswith("GOOD") else "2_Education"   # noqa: E731
    res1, cur1, held1, asked1 = _run(tmp_path, monkeypatch, _stamps(), cap=3, subject=subj)
    assert sorted(asked1) == sorted(UNDATED), asked1
    assert all(m in cur1 for m in UNDATED), "cursor advanced: not asked again until CSO moves it"
    assert not (set(UNDATED) & held1), "never claimed as held"
    res2, cur2, held2, asked2 = _run(tmp_path, monkeypatch, _stamps(), cap=3, subject=subj)
    assert asked2 == ["GOOD1"], asked2
    assert res2.status == "ok", (res2.status, res2.error)
    rows = pq.read_table(str(tmp_path / "1_Sub.parquet")).num_rows
    assert rows == 3, "the stored matrix still updates: 1 old row + 2 new"
    assert cur2["GOOD1"] == "2026-09-01T11:00:00Z" and "GOOD1" in held2


def test_a_stored_matrix_that_comes_back_undated_stays_transient(tmp_path, monkeypatch):
    _store(tmp_path / "2_Education.parquet", ["CPNI20"])
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch, _stamps(goods=()), held=())
    assert res.status == "partial" and "CPNI20: undated_time" in (res.error or ""), res.error
    assert "CPNI20" not in cursor, "we hold rows of it: retried, never skipped"
    assert "EOSA01" in cursor and "EAACC22" in cursor


def test_skipped_matrices_are_named_and_counted_in_the_note(tmp_path, monkeypatch):
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch, _stamps(goods=()))
    err = res.error or ""
    assert "3 never-stored matrix/matrices held back until CSO re-releases them" in err, err
    assert "time axis not dated: 3" in err, err
    for m in UNDATED:
        assert m in err and f"{m}:" not in err, (m, err)
    assert res.status != "partial", (res.status, err)


def test_skip_note_counts_and_elides_honestly():
    reasons = {f"CPNI{i:03d}": "undated_time" for i in range(150)}
    reasons["HRD51"] = "all_null"
    note = cso.skipped_note(reasons)
    assert note.startswith("; 151 never-stored"), note
    assert "all values null: 1" in note and "time axis not dated: 150" in note
    shown = len(re.search(r"\[(.*)\]", note).group(1).split(", ")) - 1
    assert f"+{151 - shown} more" in note, note
    assert cso.skipped_note({}) == ""


def test_a_re_release_is_fetched_again(tmp_path, monkeypatch):
    _run(tmp_path, monkeypatch, _stamps(goods=()))
    moved = _stamps(goods=())
    moved["CPNI20"] = "2026-10-15T11:00:00Z"
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch, moved)
    assert asked == ["CPNI20"], asked
    assert cursor["CPNI20"] == "2026-10-15T11:00:00Z"


# ---- the drain ---------------------------------------------------------------------------------

def test_the_cap_remainder_is_a_deferral_not_ok(tmp_path, monkeypatch):
    """A capped pass must not report a full-coverage `ok` (R311): `ok` stamps last_success and an
    irregular source then waits 7 days, which at 60 matrices a week never drains."""
    _store(tmp_path / "1_Sub.parquet", ["GOOD1", "GOOD2"])
    stamps = {"GOOD1": "2026-09-02T11:00:00Z", "GOOD2": "2026-09-01T11:00:00Z"}
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch, stamps, cap=1,
                                          subject=lambda m: "1_Sub")
    assert asked == ["GOOD1"], asked
    assert res.status == "partial", (res.status, res.error)
    assert "1 deferred by budget" in res.error and "GOOD2" in res.error, res.error
    assert "GOOD2" not in cursor
    assert health._DEFERRAL_BASE.match(res.error), "a pure deferral still reads ROTATING"
    res2, cursor2, _, asked2 = _run(tmp_path, monkeypatch, stamps, cap=1, subject=lambda m: "1_Sub")
    assert asked2 == ["GOOD2"] and res2.status == "ok", (asked2, res2.status, res2.error)


def test_no_remainder_no_deferral(tmp_path, monkeypatch):
    _store(tmp_path / "1_Sub.parquet", ["GOOD1"])
    res, *_ = _run(tmp_path, monkeypatch, {"GOOD1": "2026-09-02T11:00:00Z"}, cap=5,
                   subject=lambda m: "1_Sub")
    assert res.status == "ok" and "deferred" not in (res.error or ""), (res.status, res.error)


def test_the_store_check_reads_each_subject_once_per_run(tmp_path, monkeypatch):
    _store(tmp_path / "2_Education.parquet", ["OTHER1"])
    reads = []
    real = cso.blob.read_table

    def counting(path, columns=None):
        reads.append(os.path.basename(path))
        return real(path, columns=columns)
    monkeypatch.setattr(cso.blob, "read_table", counting)
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch, _stamps(goods=()))
    assert all(m in cursor for m in UNDATED), res.error
    assert reads.count("2_Education.parquet") == 1, reads


def test_the_memo_keeps_exact_prefix_semantics(tmp_path):
    _store(tmp_path / "S.parquet", ["HRD511", "VSA113"])
    cache = {}
    assert cso._stored_in_subject(str(tmp_path), "S", "HRD51", cache) is False
    assert cso._stored_in_subject(str(tmp_path), "S", "HRD511", cache) is True
    assert cso._stored_in_subject(str(tmp_path), "S", "VSA11", cache) is False
    assert cso._stored_in_subject(str(tmp_path), "S", "VSA113", None) is True, "uncached path"
    (tmp_path / "B.parquet").write_bytes(b"not a parquet")
    assert cso._stored_in_subject(str(tmp_path), "B", "X", cache) is None
    assert cso._stored_in_subject(str(tmp_path), "B", "Y", cache) is None, "unreadable stays unreadable"
