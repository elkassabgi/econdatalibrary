"""tools/refresh_sec_edgar.py after T0 (plan: MOVE refresh_sec_edgar, design draft 2): the SEC fetch and the merge
run without the writer lock and are STAGED; the commit (parquet moved into the local store, CSV into the
self-hosted store - plain, as stored today - catalogue span, freshness in state.db) runs under the lock. No R2,
no D1. Refused: --d1/--audit/--respan, another checkout, and the 13F state rows not yet moved. A company that is
catalogued but has no store file, or whose file cannot be read, is refused - never treated as new."""
import json
import os
import sqlite3
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from core import catalog_path, cutover, r2_util
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
from tools import refresh_sec_edgar as R  # noqa: E402
from blobstore import BlobStore  # noqa: E402

CIK = 34088


def _facts(points):
    return json.dumps({"entityName": "Exxon Mobil", "facts": {"us-gaap": {"Revenues": {"units": {"USD": [
        {"end": e, "val": v, "filed": f} for e, v, f in points]}}}}})


OLD = [("2019-12-31", 1.0, "2020-02-01"), ("2020-12-31", 2.0, "2021-02-01")]
NEW = OLD + [("2021-12-31", 3.0, "2022-02-01")]


@pytest.fixture
def world(tmp_path, monkeypatch):
    live = tmp_path / "live"
    grouped = live / "data" / "clean_grouped" / "sec_edgar"
    grouped.mkdir(parents=True)
    m, o, v, vi = R.parse_companyfacts(json.loads(_facts(OLD)))
    pq.write_table(pa.table({"metric": m, "obs_date": pa.array(o, pa.date32()), "value": v,
                             "vintage_date": pa.array(vi, pa.date32())}), grouped / "XOM.parquet")
    build = live / "data" / "catalog.db"
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, "
                  "unit TEXT, geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, "
                  "last_updated TEXT, metadata TEXT)")                   # core/catalog.py's schema
        c.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
        c.execute("INSERT INTO series (series_id, source_id, title, start_date, end_date) VALUES "
                  "('sec_edgar:XOM', 'sec_edgar', 'Exxon Mobil (XOM)', '2019-12-31', '2020-12-31')")
    c.close()
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    BlobStore(str(tmp_path / "blobs"), create=True)
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "blobs"))
    monkeypatch.setattr(R, "ROOT", str(live))
    monkeypatch.setattr(R, "GROUPED", str(grouped))
    monkeypatch.setattr(R, "SEC_MIN_INTERVAL", 0)
    monkeypatch.setattr(R, "ticker_map", lambda: {CIK: ["XOM"]})
    payload = {"facts": NEW}
    monkeypatch.setattr(R, "_get", lambda url, timeout=180, binary=False: _facts(payload["facts"]))
    monkeypatch.setattr(R, "_thirteen_f_blocker", lambda: None)
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(live))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(blob, "_code_root", lambda: str(live))
    monkeypatch.setattr(updater_config, "ROOT", str(live))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(live / "data" / "clean_full"))
    monkeypatch.setattr(updater_config, "STATE_DIR", str(state_dir))
    monkeypatch.setattr(updater_config, "STATE_DB", str(state_dir / "state.db"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 reached after T0"))
    monkeypatch.setattr(R, "_d1_json", lambda *a, **k: pytest.fail("D1 reached after T0"))
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path, live, grouped, build, payload


def _run(monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["refresh_sec_edgar.py", "--ciks", str(CIK), *argv])
    return R.main()


def _span(build):
    c = sqlite3.connect(f"file:{build}?mode=ro", uri=True)
    try:
        return c.execute("SELECT start_date, end_date FROM series WHERE series_id='sec_edgar:XOM'").fetchone()
    finally:
        c.close()


def test_after_t0_a_refresh_commits_everything_under_the_lock(world, monkeypatch):
    tmp, live, grouped, build, _p = world
    held = []
    real_replace = os.replace
    monkeypatch.setattr(R.os, "replace", lambda a, b: held.append(catalog_path._held is not None) or real_replace(a, b))
    assert _run(monkeypatch, "--apply") == 0
    assert pq.read_table(grouped / "XOM.parquet").num_rows == 3, "the merged facts are in the store"
    assert held and all(held), "the parquet (and the blob store's own file) moved under the writer lock"
    assert catalog_path._held is None, "and the lock is let go"
    stored = blob.SelfhostBlob().store.head("series/sec_edgar%3AXOM.csv")
    assert stored and stored["content_encoding"] is None, "the CSV is stored plain, as this tool always did"
    assert _span(build) == ("2019-12-31", "2021-12-31")
    from updater.state import StateStore
    st = StateStore(path=str(tmp / "state" / "state.db"))
    try:
        row = st.get_source("sec_edgar")
    finally:
        st.close()
    assert (row["strategy"], row["cadence"], row["status"]) == ("edgar_delta", "daily", "ok")
    assert row["last_success_utc"] and row["last_success_utc"] == row["last_attempt_utc"]
    assert not [p for p in os.listdir(live / "data") if p.startswith("sec_edgar_stage_")], "no staging left"


def test_a_dry_run_writes_nothing(world, monkeypatch):
    tmp, live, grouped, build, _p = world
    before = (grouped / "XOM.parquet").read_bytes()
    assert _run(monkeypatch) == 0
    assert (grouped / "XOM.parquet").read_bytes() == before and _span(build) == ("2019-12-31", "2020-12-31")
    assert blob.SelfhostBlob().get("series/sec_edgar%3AXOM.csv") is None


def test_a_catalogue_that_fell_behind_is_caught_up(world, monkeypatch):
    """Facts already equal, span behind (a run that died after its store write): not skipped (R730)."""
    tmp, live, grouped, build, payload = world
    payload["facts"] = OLD                                        # nothing new filed
    with catalog_path.writer_lock():                          # after T0 the catalogue guard requires it
        c = sqlite3.connect(str(build))
        c.execute("UPDATE series SET end_date='2019-12-31' WHERE series_id='sec_edgar:XOM'")
        c.commit()
        c.close()
    assert _run(monkeypatch, "--apply") == 0
    assert _span(build) == ("2019-12-31", "2020-12-31")


def _freshness(tmp):
    from updater.state import StateStore
    st = StateStore(path=str(tmp / "state" / "state.db"))
    try:
        return st.get_source("sec_edgar")
    finally:
        st.close()


def test_catalogued_but_no_store_file_is_refused_not_new(world, monkeypatch, capsys):
    """Refused - and the day is PARTIAL, every day it happens (R1235: counted as a fetch failure, it was
    stamped ok with last_success, rc 0, on day 1 and day 2)."""
    tmp, live, grouped, build, _p = world
    (grouped / "XOM.parquet").unlink()
    for _day in (1, 2):
        assert _run(monkeypatch, "--apply") == 1
        assert not (grouped / "XOM.parquet").exists(), "never written as a NEW company over a catalogued one"
        out = capsys.readouterr().out
        assert "catalogued-but-no-store-file" in out and "store refusals   : 1" in out
        row = _freshness(tmp)
        assert row["status"] == "partial" and row["last_success_utc"] is None and row["last_attempt_utc"]


def test_a_failed_fetch_on_a_small_run_is_partial(world, monkeypatch):
    """The 5% tolerance is for transient SEC failures over a big day; 1 of 1 failed is not a success."""
    tmp, *_ = world
    monkeypatch.setattr(R, "_get", lambda *a, **k: (_ for _ in ()).throw(OSError("SEC down")))
    assert _run(monkeypatch, "--apply") == 1
    row = _freshness(tmp)
    assert row["status"] == "partial" and row["last_success_utc"] is None


def test_the_13f_blocker_opens_state_db_read_only(tmp_path, monkeypatch):
    """R1235 mutant S13 (mode=rwc) survived: the blocker's open must not be able to write, and must not create
    a missing state.db."""
    import types
    seen = {}

    def pending(con):
        try:
            con.execute("CREATE TABLE probe (x)")
            seen["wrote"] = True
        except sqlite3.OperationalError as e:
            seen["wrote"] = False
            seen["why"] = str(e)
        return 0
    monkeypatch.setitem(sys.modules, "updater.state_migrations", types.SimpleNamespace(pending=pending))
    import updater
    monkeypatch.setattr(updater, "state_migrations", sys.modules["updater.state_migrations"], raising=False)
    db = tmp_path / "state.db"
    sqlite3.connect(db).close()
    monkeypatch.setattr(updater_config, "STATE_DB", str(db))
    assert R._thirteen_f_blocker() is None
    assert seen == {"wrote": False, "why": "attempt to write a readonly database"}
    monkeypatch.setattr(updater_config, "STATE_DB", str(tmp_path / "absent.db"))
    with pytest.raises(sqlite3.OperationalError):
        R._thirteen_f_blocker()
    assert not (tmp_path / "absent.db").exists(), "a missing state.db is not created"


@pytest.mark.parametrize("flag", ["--d1", "--audit", "--respan=XOM"])
def test_after_t0_the_unported_modes_are_refused_first(world, monkeypatch, flag):
    monkeypatch.setattr(R, "_get", lambda *a, **k: pytest.fail("fetched before refusing"))
    with pytest.raises(cutover.CutoverRefused, match="after T0"):
        _run(monkeypatch, "--apply", flag)


def test_after_t0_another_checkout_is_refused_before_any_fetch(world, monkeypatch, tmp_path):
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(R, "_get", lambda *a, **k: pytest.fail("fetched before refusing"))
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        _run(monkeypatch, "--apply")


def test_after_t0_the_13f_rows_must_have_moved(world, monkeypatch):
    monkeypatch.setattr(R, "_thirteen_f_blocker", lambda: "3 13F state row(s) are still under sec_edgar")
    monkeypatch.setattr(R, "_get", lambda *a, **k: pytest.fail("fetched before refusing"))
    with pytest.raises(cutover.CutoverRefused, match="13F"):
        _run(monkeypatch, "--apply")


def test_the_13f_blocker_names_the_missing_move_on_this_branch():
    """This branch does not carry updater/state_migrations.py yet (feat/econ-13f-own-key)."""
    import importlib.util
    if importlib.util.find_spec("updater.state_migrations") is None:
        assert "state_migrations.py is missing" in R._thirteen_f_blocker()


INSIDE = OLD + [("2020-06-30", 5.0, "2020-08-01")]            # a new fact INSIDE the catalogued span


def _served_csv():
    return blob.SelfhostBlob().get("series/sec_edgar%3AXOM.csv")


def _expected_csv(grouped):
    t = pq.read_table(grouped / "XOM.parquet")
    return R.csv_bytes(t.column("metric").to_pylist(), t.column("obs_date").to_pylist(), t.column("value").to_pylist())


@pytest.mark.parametrize("crash_at", ["csv", "parquet", "catalogue"])
def test_a_crash_at_any_commit_step_is_repaired_by_the_next_run(world, monkeypatch, crash_at):
    """A new fact that does not move the span: after a crash at any step, the next run must still leave the
    parquet, the served CSV and the catalogue current. Parquet-then-CSV lost the CSV for good: the next run
    saw the new facts in the store and the span unchanged, and skipped the company."""
    tmp, live, grouped, build, payload = world
    payload["facts"] = INSIDE
    boom = RuntimeError("crash")
    real_replace = os.replace

    def replace(a, b):
        if str(b).endswith("XOM.parquet"):
            raise boom
        return real_replace(a, b)
    with monkeypatch.context() as m:
        if crash_at == "csv":
            m.setattr(blob.SelfhostBlob, "put_atomic", lambda *a, **k: (_ for _ in ()).throw(boom))
        elif crash_at == "parquet":
            m.setattr(R.os, "replace", replace)
        else:
            m.setattr(R, "update_catalog", lambda *a, **k: (_ for _ in ()).throw(boom))
        with pytest.raises(RuntimeError, match="crash"):
            _run(monkeypatch, "--apply")
    assert catalog_path._held is None
    assert _run(monkeypatch, "--apply") == 0
    assert pq.read_table(grouped / "XOM.parquet").num_rows == 3
    assert _served_csv() == _expected_csv(grouped), "the served CSV is the store's"
    assert _span(build) == ("2019-12-31", "2020-12-31")


@pytest.mark.parametrize("rows", ["same count", "more rows"])
def test_a_store_file_changed_after_the_merge_is_skipped(world, monkeypatch, capsys, rows):
    """Another writer changed the store file between the merge and the commit. The re-check under the lock
    compares the stored FILE, so a change that keeps the row count is caught too."""
    import contextlib
    tmp, live, grouped, build, _p = world
    real_lock = R._waiting_writer_lock

    @contextlib.contextmanager
    def lock_after_a_rewrite():
        t = pq.read_table(grouped / "XOM.parquet")
        t = t.set_column(2, "value", pa.array([9.0] * t.num_rows)) if rows == "same count" else pa.concat_tables([t, t])
        pq.write_table(t, grouped / "XOM.parquet")
        with real_lock():
            yield
    monkeypatch.setattr(R, "_waiting_writer_lock", lock_after_a_rewrite)

    assert _run(monkeypatch, "--apply") == 1
    after = pq.read_table(grouped / "XOM.parquet")
    assert (after.column("value").to_pylist() == [9.0, 9.0]) if rows == "same count" else after.num_rows == 4, \
        "the other writer's file was not overwritten with a merge of the old one"
    assert "SKIPPED XOM" in capsys.readouterr().out
    assert _served_csv() is None and _span(build) == ("2019-12-31", "2020-12-31")
    row = _freshness(tmp)
    assert row["status"] == "partial" and row["last_success_utc"] is None, "a skipped company is not an ok day"


def test_written_rows_carry_last_updated_and_others_do_not(world, monkeypatch):
    """/v1/series/<id>.metadata.json reads series.last_updated first; after the 13F move sec_edgar has no
    '_all' unit to fall back on, so the refresher stamps the rows it wrote - and only those."""
    tmp, live, grouped, build, _p = world
    with catalog_path.writer_lock():
        c = sqlite3.connect(str(build))
        c.execute("INSERT INTO series (series_id, source_id, title) VALUES ('sec_edgar:OTHER', 'sec_edgar', 'x')")
        c.commit()
        c.close()
    assert _run(monkeypatch, "--apply") == 0
    c = sqlite3.connect(f"file:{build}?mode=ro", uri=True)
    try:
        got = dict(c.execute("SELECT series_id, last_updated FROM series"))
    finally:
        c.close()
    from updater.state import StateStore
    st = StateStore(path=str(tmp / "state" / "state.db"))
    try:
        stamp = st.get_source("sec_edgar")["last_success_utc"]
    finally:
        st.close()
    assert stamp and got == {"sec_edgar:XOM": stamp, "sec_edgar:OTHER": None}


def test_a_catalogue_write_without_last_updated_is_a_failure(world, monkeypatch, capsys):
    """The read-back checks the stamp too: a catalogue that took the span but not last_updated is a FAIL."""
    real = R.update_catalog
    monkeypatch.setattr(R, "update_catalog", lambda spans, d1, last_updated=None: real(spans, d1))
    assert _run(monkeypatch, "--apply") == 1
    assert "FAIL: parquet + CSV written but the catalogue does not carry" in capsys.readouterr().out
    row = _freshness(world[0])
    assert row["status"] == "partial" and row["last_success_utc"] is None and row["last_attempt_utc"]


def test_a_change_just_after_the_merge_read_is_caught(world, monkeypatch, capsys):
    """The digest is taken BEFORE the merge reads the facts (R1235 mutant S14 took it after, and survived): a
    write that lands between the read and a later digest would be hashed as if the merge had seen it."""
    tmp, live, grouped, build, _p = world
    real = R.prior_facts

    def read_then_another_writer(client, path, *a, **k):
        out = real(client, path, *a, **k)
        t = pq.read_table(path)
        pq.write_table(pa.concat_tables([t, t]), path)          # lands right after this read
        return out
    monkeypatch.setattr(R, "prior_facts", read_then_another_writer)
    assert _run(monkeypatch, "--apply") == 1
    assert pq.read_table(grouped / "XOM.parquet").num_rows == 4, "the other writer's file was kept"
    assert "SKIPPED XOM" in capsys.readouterr().out


def _twenty(monkeypatch, fail=(), answer=None, n=20):
    """A day of `n` filers: XOM (catalogued, stored) and n-1 new ones. `fail` = CIKs whose fetch raises; `answer`
    = a payload for every CIK instead of the world's (e.g. one the parser reads as no facts)."""
    ciks = [CIK] + [900000 + i for i in range(n - 1)]
    monkeypatch.setattr(R, "ticker_map", lambda: {c: ["XOM" if c == CIK else f"T{c}"] for c in ciks})

    def get(url, timeout=180, binary=False):
        cik = int(url.rsplit("CIK", 1)[1].split(".")[0])
        if cik in fail:
            raise OSError("SEC down for this one")
        return answer if answer is not None else _facts(NEW)
    monkeypatch.setattr(R, "_get", get)
    monkeypatch.setattr(sys, "argv", ["refresh_sec_edgar.py", "--ciks", ",".join(map(str, ciks)), "--apply"])
    return ciks


def test_an_all_empty_day_is_a_structural_failure(world, monkeypatch, capsys):
    """R1236: 20 of 20 answers that parse to no facts were stamped ok (the econ-updater rule: more than 10 units
    all empty is a definitive failure, not a quiet day)."""
    tmp, *_ = world
    _twenty(monkeypatch, answer=json.dumps({"entityName": "E", "facts": {}}))
    assert R.main() == 1
    assert "STRUCTURAL" in capsys.readouterr().out
    row = _freshness(tmp)
    assert row["status"] == "partial" and row["last_success_utc"] is None


EMPTY = json.dumps({"entityName": "E", "facts": {}})


@pytest.mark.parametrize("n,n_fail,ok", [(10, 0, True), (11, 0, False), (21, 1, False)],
                         ids=["10 of 10 empty is below the floor", "11 of 11 empty",
                              "20 answered empty, 1 failed (within the 5%)"])
def test_the_all_empty_boundary(world, monkeypatch, n, n_fail, ok):
    """The econ-updater rule (updater/strategies/fetchers/_common.py: all empty over MORE than 10 attempted): 10
    empty answers are still ok, 11 are not; and the count is over ANSWERS - a failed fetch is not an answer, so 11
    empty answers with 1 failure is all-empty (R1238 mutants E2/E6 moved the floor, E3 counted fetches)."""
    tmp, *_ = world
    ciks = _twenty(monkeypatch, n=n, answer=EMPTY)
    _twenty(monkeypatch, n=n, answer=EMPTY, fail=set(ciks[1:1 + n_fail]))
    rc = R.main()
    row = _freshness(tmp)
    if ok:
        assert (rc, row["status"]) == (0, "ok")
    else:
        assert (rc, row["status"], row["last_success_utc"]) == (1, "partial", None)


@pytest.mark.parametrize("n_fail,ok", [(1, True), (2, False)])
def test_the_five_percent_fetch_tolerance_boundary(world, monkeypatch, n_fail, ok):
    """1 transient fetch failure in 20 is still an ok day (the pre-T0 rule, 95% fetched); 2 are not (R1236 S19)."""
    tmp, *_ = world
    ciks = _twenty(monkeypatch, fail=set())
    _twenty(monkeypatch, fail=set(ciks[1:1 + n_fail]))
    assert R.main() == (0 if ok else 1)
    row = _freshness(tmp)
    assert (row["status"], row["last_success_utc"] is not None) == (("ok", True) if ok else ("partial", False))


@pytest.mark.parametrize("how", ["unreadable store file", "a merge that would shrink"])
def test_one_store_refusal_in_twenty_is_not_ok(world, monkeypatch, how):
    """A refusal is not a transient fetch failure: ONE in 20 makes the day partial, where one fetch failure in
    20 would not (R1236 S23/S24: a refusal counted as a failure survived)."""
    tmp, live, grouped, build, _p = world
    _twenty(monkeypatch)
    if how == "unreadable store file":
        (grouped / "XOM.parquet").write_bytes(b"not a parquet file")
    else:
        real = R.merge_facts
        monkeypatch.setattr(R, "merge_facts", lambda prior, new: (_ for _ in ()).throw(AssertionError("shrink"))
                            if prior else real(prior, new))
    assert R.main() == 1
    row = _freshness(tmp)
    assert row["status"] == "partial" and row["last_success_utc"] is None


def test_a_reader_briefly_holding_the_parquet_does_not_fail_the_commit(world, monkeypatch):
    """R1236 S21: the move retries a PermissionError (a reader holds the file on Windows), as core.atomic does."""
    tmp, live, grouped, build, _p = world
    real, seen = os.replace, []

    def held_once(a, b):
        if str(b).endswith("XOM.parquet") and not seen:
            seen.append(1)
            raise PermissionError(5, "held by a reader")
        return real(a, b)
    monkeypatch.setattr(R.os, "replace", held_once)
    import core.atomic as atomic
    monkeypatch.setattr(atomic, "BACKOFF_S", (0.01,))
    assert _run(monkeypatch, "--apply") == 0
    assert seen == [1] and pq.read_table(grouped / "XOM.parquet").num_rows == 3
