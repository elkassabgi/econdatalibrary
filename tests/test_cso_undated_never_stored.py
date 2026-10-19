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
from updater.strategies.base import NOT_HOSTED_NOTE  # noqa: E402
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
NEW = tuple(f"NEW{i:02d}" for i in range(10))     # never-stored matrices that parse fine
BODIES.update({m: _good_body(m) for m in NEW})
UNDATED = ("CPNI20", "EOSA01", "EAACC22")


def _ingester_on(bodies, asked, real_get=False):
    mod = REAL_INGESTER()               # the fetcher's own wrapped module: same parse_date chain

    def _get_json(url, retries=3):
        mtr = url.split("/")[-4]
        asked.append(mtr)
        return bodies.get(mtr)          # None for a matrix with no body: the no_body outcome
    if not real_get:
        mod.get_json = _get_json
    mod.RATE = 0
    return mod


def _store(path, mtrs):
    pq.write_table(pa.table({
        "series_key": [f"CSO:{m}:STATISTIC={m}C1" for m in mtrs],
        "obs_date": pa.array([dt.date(2023, 12, 31)] * len(mtrs), pa.date32()),
        "value": [0.5] * len(mtrs)}), str(path))


def _run(tmp_path, monkeypatch, stamps, held=("GOOD1", "GOOD2"), cap=60, subject=lambda m: "2_Education",
         real_get=False):
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
    ing = _ingester_on(BODIES, asked, real_get=real_get)
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
    stamps, one never-stored matrix that parses (NEW00) behind them - all class 0, so the held
    reservation plays no part. On origin/main the second run asks the same three again and NEW00
    is never reached."""
    stamps = {m: "2026-10-01T11:00:00Z" for m in UNDATED}
    stamps["NEW00"] = "2026-09-01T11:00:00Z"
    subj = lambda m: "3_New" if m.startswith("NEW") else "2_Education"   # noqa: E731
    res1, cur1, held1, asked1 = _run(tmp_path, monkeypatch, stamps, held=(), cap=3, subject=subj)
    assert sorted(asked1) == sorted(UNDATED), asked1
    assert all(m in cur1 for m in UNDATED), "cursor advanced: not asked again until CSO moves it"
    assert not (set(UNDATED) & held1), "never claimed as held"
    res2, cur2, held2, asked2 = _run(tmp_path, monkeypatch, stamps, held=(), cap=3, subject=subj)
    assert asked2 == ["NEW00"], asked2
    assert res2.status == "ok", (res2.status, res2.error)
    assert pq.read_table(str(tmp_path / "3_New.parquet")).num_rows == 2
    assert cur2["NEW00"] == "2026-09-01T11:00:00Z" and "NEW00" in held2


def test_a_held_revision_is_reached_in_run_one_behind_unheld_matrices(tmp_path, monkeypatch):
    """AR-346 R3, through update(): cap 4, ten never-stored matrices with newer stamps (class 0)
    and one stored revision. Without the held share the run pulls four strangers and the served
    matrix waits; with it GOOD1 is in run 1, and its rows merge."""
    _store(tmp_path / "1_Sub.parquet", ["GOOD1"])
    stamps = {m: "2026-10-01T11:00:00Z" for m in NEW}
    stamps["GOOD1"] = "2026-09-01T11:00:00Z"
    subj = lambda m: "1_Sub" if m.startswith("GOOD") else "3_New"   # noqa: E731
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch, stamps, held=("GOOD1",), cap=4,
                                          subject=subj)
    assert "GOOD1" in asked and len(asked) == 4, asked
    assert sum(m in NEW for m in asked) == 3, "the rest of the batch still fills holes"
    assert cursor["GOOD1"] == "2026-09-01T11:00:00Z"
    assert pq.read_table(str(tmp_path / "1_Sub.parquet")).num_rows == 3, "1 old row + 2 new"
    assert res.status == "partial" and "7 deferred by budget" in (res.error or ""), res.error


def test_the_held_share_reaches_a_held_revision_behind_3000_unheld():
    """The queue-position test (R511: `index < cap`, never `is in the list`), at production scale:
    3,000 unheld matrices with newer stamps, 100 held revisions behind them, cap 60."""
    unheld = [f"U{i:04d}" for i in range(3000)]
    heldm = [f"H{i:03d}" for i in range(100)]
    cur = {m: "2026-10-01T11:00:00Z" for m in unheld}
    cur.update({m: f"2026-09-{1 + i % 28:02d}T11:00:00Z" for i, m in enumerate(heldm)})
    ordered = cso.order_changed(list(cur), cur, set(heldm))
    assert min(ordered.index(m) for m in heldm) == 3000, "the order alone buries them"
    batch = cso.take_batch(ordered, cso.MAX_TABLES, frozenset(), set(heldm))
    assert len(batch) == cso.MAX_TABLES
    got = [m for m in batch if m in heldm]
    assert len(got) == int(cso.MAX_TABLES * cso.HELD_RESERVED_FRAC), len(got)
    assert got == [m for m in ordered if m in heldm][:len(got)], "newest held revisions first"
    assert [m for m in batch if m in unheld] == unheld[:cso.MAX_TABLES - len(got)]


def test_the_held_share_changes_nothing_when_the_head_is_held():
    heldm = [f"H{i:03d}" for i in range(200)]
    assert cso.take_batch(heldm, 60, frozenset(), set(heldm)) == heldm[:60]
    assert cso.take_batch(heldm, 60, frozenset(), frozenset()) == heldm[:60]
    few = ["U1", "U2", "H1"]
    assert cso.take_batch(few, 60, frozenset(), {"H1"}) == ["H1", "U1", "U2"], "never shrinks"


def test_a_never_stored_matrix_with_no_body_stays_transient_and_is_asked_again(tmp_path, monkeypatch):
    """AR-346 R1: a failed or missing body is the publisher's bad hour, not a matrix with nothing
    to store - it must never be skipped for good (mutant: 'no_body' in the skip tuple)."""
    stamps = {"NOBODY1": "2025-07-03T11:00:00Z"}            # CPNI20's stamp
    for run in (1, 2):
        res, cursor, held_after, asked = _run(tmp_path, monkeypatch, stamps, held=())
        assert asked == ["NOBODY1"], (run, asked)
        assert "NOBODY1: no_body" in (res.error or "") and res.status == "partial", res.error
        assert "NOBODY1" not in cursor and "NOBODY1" not in held_after
        assert NOT_HOSTED_NOTE not in (res.error or ""), res.error


def test_a_truncated_body_through_the_real_get_json_is_no_body(tmp_path, monkeypatch):
    """Same, with the REAL ingester get_json: CSO answers 200 with a JSON body cut mid-stream."""
    import requests
    calls = []

    def _get(url, headers=None, timeout=None):
        calls.append(url)
        r = requests.models.Response()
        r.status_code = 200
        r._content = json.dumps(BODIES["CPNI20"]).encode("utf-8")[:900]
        r.encoding = "utf-8"
        return r
    import time as _time
    monkeypatch.setattr(requests, "get", _get)       # the module object every ingester copy uses
    monkeypatch.setattr(_time, "sleep", lambda s: None)
    stamps = {"CPNI20": "2025-07-03T11:00:00Z"}
    for run in (1, 2):
        res, cursor, held_after, asked = _run(tmp_path, monkeypatch, stamps, held=(), real_get=True)
        assert "CPNI20: no_body" in (res.error or ""), (run, res.error)
        assert "CPNI20" not in cursor, "a truncated body never advances the cursor"
    assert len(calls) == 6, "3 tries per run, asked again on run 2"


def test_a_deferral_pass_that_also_skipped_still_reads_rotating(tmp_path, monkeypatch):
    """AR-346 R2: the skip tail carries NOT_HOSTED_NOTE, which health strips segment by segment."""
    _store(tmp_path / "1_Sub.parquet", ["GOOD1", "GOOD2"])
    stamps = {"CPNI20": "2026-10-01T11:00:00Z", "GOOD1": "2026-09-02T11:00:00Z",
              "GOOD2": "2026-09-01T11:00:00Z"}
    subj = lambda m: "1_Sub" if m.startswith("GOOD") else "2_Education"   # noqa: E731
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch, stamps, cap=2, subject=subj)
    err = res.error or ""
    assert sorted(asked) == ["CPNI20", "GOOD1"], asked
    assert res.status == "partial" and "1 deferred by budget" in err and NOT_HOSTED_NOTE in err, err
    assert health._deferral_only([{"status": "partial", "last_error": err}]), err
    assert not health._deferral_only([{"status": "partial", "last_error":
                                       err + "; csv_derive failed 1/2 series [cso:CSO:GOOD1]"}])


def test_a_datable_label_keeps_it_unparsed():
    """AR-346 S6: parse_jsonstat2 falls back to the LABEL; so does undated_time. An undatable code
    with a datable label that still yields no row is ours, not a convention question."""
    ing = REAL_INGESTER()
    body = json.loads(json.dumps(BODIES["CPNI20"]))
    body["dimension"]["TLIST(A1)"]["category"]["label"] = {"2021/2022": "2022"}
    assert ing._why_unparsed(body) == "unparsed"


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
    assert note.startswith(f"{NOT_HOSTED_NOTE} 151 never-stored"), note
    assert "; " not in note, "health drops a note segment by prefix (R1127)"
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
    # the old predicate was starts_with('CSO:<m>:'): a key with no third part, or another
    # prefix, never made a matrix look stored, and the memo must agree
    pq.write_table(pa.table({"series_key": ["CSO:HRD52", "OTH:HRD53:STATISTIC=S1"],
                             "obs_date": pa.array([dt.date(2023, 12, 31)] * 2, pa.date32()),
                             "value": [1.0, 2.0]}), str(tmp_path / "T.parquet"))
    assert cso._stored_in_subject(str(tmp_path), "T", "HRD52", cache) is False
    assert cso._stored_in_subject(str(tmp_path), "T", "HRD53", cache) is False
    (tmp_path / "B.parquet").write_bytes(b"not a parquet")
    assert cso._stored_in_subject(str(tmp_path), "B", "X", cache) is None
    assert cso._stored_in_subject(str(tmp_path), "B", "Y", cache) is None, "unreadable stays unreadable"
