"""The statcan catalogue tool's --r2-keys gate: advertise exactly what exists in R2, both ways.

What this closes, measured 2026-09-01:

  * the completeness guard refused the WHOLE catalogue because 5 tables the derive had REFUSED
    (no usable splitter; nothing written) had no split-map entry - at the cap the derive really
    ran with, the absent set was exactly those 5. A table with no object cannot be advertised,
    so its absence from the map is the correct state, not a reason to block 8,202 others;
  * the emission contains ids with NO object. The real drop set is 7: the 5 refused giants plus
    18100103 and 34100102 (0 rows survive `value IS NOT NULL AND obs_date IS NOT NULL`, yet the
    sidecar carries start/end dates so they are emitted without a scan). A first projection said
    373 - it came from the split map's `parts` field, which counts dim values on all-null rows
    the catalogue never emits: the wrong instrument (adversarial review, 2026-09-01);
  * the reviewer's three real defects: the gate only tested rows-without-objects, never
    objects-without-rows (R501: test the failure that hurts); an empty or wrong listing DISARMED
    the guard and returned a green zero-row run (R503/R508); "refused by the derive" was printed
    without checking the derive's summary (R219).

The helpers are pure so they can be pinned without the 175 GB store.
"""
import datetime as dt
import json
import os
import sqlite3
import sys
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.catalog_statcan_tables import (  # noqa: E402
    classify_absent, expectation_problems, filter_rows_by_listing, key_prefix, listing_problems,
    load_listing_meta, load_r2_keys, object_key, orphan_keys, pids_with_parts,
    unlisted_catalogue_rows)
from tools.derive_statcan_tables import unit_id  # noqa: E402

REFUSED = {"98100174", "37100277", "37100234", "98100206", "98100023"}


def key(unit, prefix="series"):
    return f"{prefix}/" + urllib.parse.quote(unit, safe="") + ".csv"


NOW = dt.datetime(2026, 9, 1, 22, 0, tzinfo=dt.timezone.utc)


def meta_for(keys, prefix="series", bucket="econ-data", listed="2026-09-01T21:58:58Z"):
    return {"bucket": bucket, "prefix": key_prefix(prefix), "count": len(keys),
            "listed_at_utc": listed}


def problems(keys, meta, prefix="series", bucket="econ-data", max_age_hours=24.0):
    return listing_problems(keys, meta, prefix, bucket, max_age_hours, now=NOW)


def test_object_key_is_the_derive_builder_and_quotes_hash_and_space():
    assert object_key("statcan:12100147#1.9") == "series/statcan%3A12100147%231.9.csv"
    assert (object_key("statcan:14100358#Nova Scotia")
            == "series/statcan%3A14100358%23Nova%20Scotia.csv")


def test_object_key_honours_the_derive_prefix():
    assert object_key("statcan:10100001", "preview") == "preview/statcan%3A10100001.csv"


def test_pids_with_parts_reads_the_pid_before_the_quoted_hash():
    keys = {key("statcan:11100054#uom 1"), key("statcan:11100054#uom 2"), key("statcan:10100001")}
    assert pids_with_parts(keys) == {"11100054"}


def test_pids_with_parts_returns_the_RAW_pid():
    """The split map and filenames are raw; a quoted pid would never match them."""
    keys = {key("statcan:AB C#x")}
    assert pids_with_parts(keys) == {"AB C"}


def test_a_refused_table_with_NO_object_is_acceptable():
    """The 5 refused giants: nothing written, nothing to advertise, must not block the run."""
    keys = {key("statcan:10100001")}
    acc, unref, unacc = classify_absent({"98100174": 314_800_860}, keys, REFUSED)
    assert acc == {"98100174": 314_800_860} and unref == {} and unacc == {}


def test_an_absent_table_the_derive_never_refused_is_NOT_accepted():
    """No object, but not in the derive's refused list: ingested or grown past the cap after the
    derive ran - real data never derived. Must go to the refuse path, never be waved through."""
    keys = {key("statcan:10100001")}
    acc, unref, unacc = classify_absent({"99999999": 5_000_000}, keys, REFUSED)
    assert acc == {} and unref == {"99999999": 5_000_000} and unacc == {}


def test_an_over_cap_table_WITH_a_whole_object_still_refuses():
    """Emitted whole above the cap: an id that may point at an undeliverable object."""
    keys = {key("statcan:37100277")}
    acc, unref, unacc = classify_absent({"37100277": 75_731_377}, keys, REFUSED)
    assert acc == {} and unref == {} and unacc == {"37100277": 75_731_377}


def test_an_over_cap_table_with_PARTS_but_no_map_entry_still_refuses():
    """Parts exist but the map has no dim for them: a lost or stale map, not a clean absence."""
    keys = {key("statcan:37100234#geo 1"), key("statcan:37100234#geo 2")}
    acc, unref, unacc = classify_absent({"37100234": 63_821_210}, keys, REFUSED)
    assert acc == {} and unref == {} and unacc == {"37100234": 63_821_210}


def test_filter_drops_ids_with_no_object_and_names_them():
    rows = [(unit_id("10100001"), "x"), (unit_id("18100103"), "all-null, never written"),
            (unit_id("14100358", "Nova Scotia"), "part")]
    keys = {key("statcan:10100001"), key("statcan:14100358#Nova Scotia")}
    kept, dropped = filter_rows_by_listing(rows, keys)
    assert [r[0] for r in kept] == ["statcan:10100001", "statcan:14100358#Nova Scotia"]
    assert [r[0] for r in dropped] == ["statcan:18100103"]


def test_orphans_are_objects_with_no_row():
    """The direction that hurts: a dim value that disappeared on re-ingest leaves its object in
    R2 with no catalogue row."""
    rows = [(unit_id("10100001"), "x"), (unit_id("14100358", "Nova Scotia"), "part")]
    keys = {key("statcan:10100001"), key("statcan:14100358#Nova Scotia"),
            key("statcan:14100358#Yukon")}
    assert orphan_keys(rows, keys) == {key("statcan:14100358#Yukon")}
    assert orphan_keys(rows, keys - {key("statcan:14100358#Yukon")}) == set()


def test_an_empty_listing_is_refused_not_trusted():
    assert any("EMPTY" in p for p in problems(set(), meta_for(set())))


def test_a_listing_of_another_source_or_prefix_is_refused():
    keys = {"series/eurostat%3Anama_10_gdp.csv"}
    assert any("not under series/statcan%3A" in p for p in problems(keys, meta_for(keys)))
    keys = {key("statcan:10100001", "preview")}
    assert any("not under series/statcan%3A" in p for p in problems(keys, meta_for(keys)))
    assert problems(keys, meta_for(keys, "preview"), "preview") == []


def test_a_listing_without_provenance_or_with_a_wrong_count_is_refused():
    keys = {key("statcan:10100001"), key("statcan:10100002")}
    assert any("provenance" in p for p in problems(keys, None))
    bad = dict(meta_for(keys), count=3)
    assert any("sidecar count" in p for p in problems(keys, bad))
    assert problems(keys, meta_for(keys)) == []


def test_a_listing_of_another_bucket_is_refused():
    """F7: the one sidecar field that names the store must be checked against --bucket."""
    keys = {key("statcan:10100001")}
    assert any("sidecar bucket" in p for p in problems(keys, meta_for(keys, bucket="econ-data-staging")))
    assert any("sidecar bucket" in p for p in problems(keys, {"prefix": key_prefix(), "count": 1,
                                                                "listed_at_utc": "2026-09-01T21:58:58Z"}))
    assert problems(keys, meta_for(keys, bucket="other"), bucket="other") == []


def test_a_stale_or_unstamped_listing_is_refused():
    """F8: objects written after the listing are invisible to the orphan gate, so age matters."""
    keys = {key("statcan:10100001")}
    assert any("h old" in p for p in problems(keys, meta_for(keys, listed="2019-01-01T00:00:00Z")))
    assert any("not an ISO UTC stamp" in p for p in problems(keys, dict(meta_for(keys), listed_at_utc=None)))
    assert problems(keys, meta_for(keys, listed="2026-09-01T00:00:00Z"), max_age_hours=48) == []
    assert any("h old" in p for p in problems(keys, meta_for(keys, listed="2026-09-01T00:00:00Z"),
                                              max_age_hours=12))


def test_the_catalogue_not_the_emission_is_audited_for_unlisted_ids():
    """F6: 20 legacy `statcan:V...` rows with no object survived every emission-level gate."""
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series(series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES(?,?)",
                    [("statcan:10100001", "statcan"), ("statcan:V2132579", "statcan"),
                     ("statcan:14100358#Nova Scotia", "statcan"), ("eurostat:x", "eurostat")])
    keys = {key("statcan:10100001"), key("statcan:14100358#Nova Scotia")}
    assert unlisted_catalogue_rows(con, keys) == ["statcan:V2132579"]
    assert unlisted_catalogue_rows(con, keys | {key("statcan:V2132579")}) == []


def test_load_r2_keys_strips_blanks_and_a_bom(tmp_path):
    p = tmp_path / "keys.txt"
    p.write_text("series/statcan%3A10100001.csv\n\n  series/statcan%3A10100002.csv  \n",
                 encoding="utf-8-sig")
    assert load_r2_keys(str(p)) == {"series/statcan%3A10100001.csv",
                                    "series/statcan%3A10100002.csv"}


def test_load_listing_meta_reads_the_sidecar_or_returns_none(tmp_path):
    p = tmp_path / "keys.txt"
    p.write_text("series/statcan%3A10100001.csv\n", encoding="utf-8")
    assert load_listing_meta(str(p)) is None
    (tmp_path / "keys.txt.meta.json").write_text(json.dumps({"count": 1}), encoding="utf-8")
    assert load_listing_meta(str(p)) == {"count": 1}


def test_the_run_must_match_the_counts_predicted_before_it():
    assert expectation_problems(466_341, 7, 466_341, 7) == []
    assert expectation_problems(466_341, 8, 466_341, 7) == ["dropped 8 != expected 7"]
    assert expectation_problems(466_340, 7, 466_341, None) == ["kept 466,340 != expected 466,341"]
    assert expectation_problems(1, 1, None, None) == []


def _catalogue_with_standalone_fts():
    """The production shape: series_fts is a standalone fts5 table, NO triggers on series."""
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series(series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, "
                "geography TEXT)")
    con.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
    con.executemany("INSERT INTO series VALUES(?,?,?,?)",
                    [("statcan:V2132579", "statcan", "Average hourly wage rate", "Canada"),
                     ("eurostat:x", "eurostat", "GDP", "EU")])
    con.execute("INSERT INTO series_fts(series_id,title,geography) SELECT series_id,title,geography FROM series")
    con.commit()
    return con


def test_fts_rebuild_reads_series_not_its_own_content_store(capsys):
    """R575: INSERT INTO series_fts(series_fts) VALUES('rebuild') never reads `series`."""
    from tools.catalog_statcan_tables import rebuild_fts
    con = _catalogue_with_standalone_fts()
    con.execute("INSERT INTO series VALUES(?,?,?,?)", ("statcan:10100001", "statcan", "Population estimates", "Canada"))
    con.execute("DELETE FROM series WHERE series_id='statcan:V2132579'")
    con.commit()
    # the broken idiom leaves the index stale
    con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
    assert [r[0] for r in con.execute("SELECT series_id FROM series_fts WHERE series_fts MATCH 'wage'")] == ["statcan:V2132579"]
    assert rebuild_fts(con) == 0
    assert con.execute("SELECT count(*) FROM series_fts").fetchone()[0] == 2
    assert [r[0] for r in con.execute("SELECT series_id FROM series_fts WHERE series_fts MATCH 'wage'")] == []
    assert [r[0] for r in con.execute("SELECT series_id FROM series_fts WHERE series_fts MATCH 'Population'")] == ["statcan:10100001"]


def test_purge_backs_up_then_removes_from_both_series_and_fts(tmp_path, monkeypatch):
    from tools import catalog_statcan_tables as t
    monkeypatch.setattr(t, "ROOT", str(tmp_path))
    os.makedirs(tmp_path / "logs")
    con = _catalogue_with_standalone_fts()
    keys = set()  # nothing listed -> the V-id has no object
    assert t.audit_catalogue(con, keys, "series", purge=False) == 1
    assert con.execute("SELECT count(*) FROM series WHERE source_id='statcan'").fetchone()[0] == 1
    assert t.audit_catalogue(con, keys, "series", purge=True) == 0
    assert con.execute("SELECT count(*) FROM series WHERE source_id='statcan'").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM series_fts WHERE series_id='statcan:V2132579'").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM series WHERE source_id='eurostat'").fetchone()[0] == 1  # untouched
    dumps = list((tmp_path / "logs").glob("_catalog_statcan_purged_rows_*.tsv"))
    assert len(dumps) == 1
    lines = dumps[0].read_text(encoding="utf-8").splitlines()
    assert lines[0].split("\t")[:2] == ["series_id", "source_id"] and lines[1].startswith("statcan:V2132579\tstatcan\t")


def test_a_future_stamped_listing_is_refused_too():
    keys = {key("statcan:10100001")}
    assert any("FUTURE" in p for p in problems(keys, meta_for(keys, listed="2099-01-01T00:00:00Z")))


def test_fts_membership_and_delete_use_in_lists_not_per_id_statements():
    """R576: series_fts(series_id UNINDEXED) - every statement is a full scan, so 20 ids must
    cost 1 statement, not 20. Pinned by counting statements through a tracing connection."""
    from tools.catalog_statcan_tables import FTS_IDS_PER_STMT, fts_delete_ids, fts_has
    con = _catalogue_with_standalone_fts()
    ids = [f"statcan:{i}" for i in range(1, 30)] + ["statcan:V2132579"]
    con.executemany("INSERT INTO series_fts(series_id,title,geography) VALUES(?,?,?)",
                    [(i, "t", "Canada") for i in ids if i != "statcan:V2132579"])
    stmts = []
    con.set_trace_callback(lambda s: stmts.append(s))
    got = fts_has(con, ids)
    # fts5 issues its own content-store SELECTs; count only OUR statements against series_fts
    assert got == ids and len([s for s in stmts if s.startswith("SELECT series_id FROM series_fts")]) == 1
    stmts.clear()
    fts_delete_ids(con, ids)
    assert len([s for s in stmts if s.startswith("DELETE FROM series_fts")]) == 1
    assert con.execute("SELECT count(*) FROM series_fts").fetchone()[0] == 1   # eurostat:x remains
    assert FTS_IDS_PER_STMT >= 100


def test_source_scoped_fts_reconcile_touches_only_the_source_and_asserts_counts():
    from tools.catalog_statcan_tables import reconcile_fts_source, source_range
    lo, hi = source_range()
    assert lo == "statcan:" and hi == "statcan;"
    con = _catalogue_with_standalone_fts()
    con.execute("INSERT INTO series VALUES(?,?,?,?)", ("statcan:10100001", "statcan", "Population estimates", "Canada"))
    con.execute("DELETE FROM series WHERE series_id='statcan:V2132579'")
    con.commit()
    assert reconcile_fts_source(con) == 0
    fts = sorted(r[0] for r in con.execute("SELECT series_id FROM series_fts"))
    assert fts == ["eurostat:x", "statcan:10100001"]          # eurostat untouched, V-id gone, new row in
    assert [r[0] for r in con.execute("SELECT series_id FROM series_fts WHERE series_fts MATCH 'Population'")] == ["statcan:10100001"]


def test_backup_is_an_online_sqlite_backup_with_an_integrity_check(tmp_path):
    """R578: a byte copy of a live db in journal_mode=delete is a torn image with the same size."""
    from tools.catalog_statcan_tables import backup_sqlite
    src = str(tmp_path / "live.db"); dst = str(tmp_path / "bak.db")
    con = sqlite3.connect(src)
    con.execute("CREATE TABLE t(x)"); con.executemany("INSERT INTO t VALUES(?)", [(i,) for i in range(1000)])
    con.commit()
    con.execute("BEGIN"); con.execute("INSERT INTO t VALUES(-1)")   # an OPEN write transaction on the live db
    assert backup_sqlite(src, dst) == "ok"
    con.rollback(); con.close()
    n = sqlite3.connect(dst).execute("SELECT count(*) FROM t").fetchone()[0]
    assert n == 1000                                                 # consistent: the uncommitted row is absent


def test_reconcile_rolls_back_instead_of_committing_a_mismatch():
    """R578: verify inside the transaction; a refusal must leave the index exactly as it was."""
    from tools.catalog_statcan_tables import reconcile_fts_source
    con = _catalogue_with_standalone_fts()
    before = sorted(r[0] for r in con.execute("SELECT series_id FROM series_fts"))
    con.execute("INSERT INTO series VALUES(?,?,?,?)", ("V999", "statcan", "no prefix", "Canada"))  # outside the range
    con.commit()
    assert reconcile_fts_source(con) == 1
    assert sorted(r[0] for r in con.execute("SELECT series_id FROM series_fts")) == before
    assert con.in_transaction is False


def test_backup_is_incremental_so_writers_can_commit_during_it(tmp_path):
    """R579: pages=-1 holds the read lock for the whole copy (~7 min at 11.9 GB); with pages the
    lock is released between steps, so a concurrent writer with 1 s patience can commit."""
    import threading
    from tools.catalog_statcan_tables import backup_sqlite
    src = str(tmp_path / "live.db"); dst = str(tmp_path / "bak.db")
    con = sqlite3.connect(src)
    con.execute("CREATE TABLE t(x)")
    con.executemany("INSERT INTO t VALUES(?)", [("x" * 500,) for _ in range(60_000)])   # ~30 MB
    con.commit(); con.close()
    result = {}
    def run():
        result["qc"] = backup_sqlite(src, dst, pages=64, sleep=0.01)
    th = threading.Thread(target=run); th.start()
    w = sqlite3.connect(src, timeout=1.0)
    committed = 0
    # A write by another connection RESTARTS an incremental backup (SQLite semantics), so a
    # writer that never stops would livelock it - bound the writes, then let the backup finish.
    while th.is_alive() and committed < 3:
        try:
            w.execute("INSERT INTO t VALUES('during')"); w.commit(); committed += 1
        except sqlite3.OperationalError:
            pass
    w.close()
    th.join(timeout=120)
    assert not th.is_alive(), "backup did not finish after the writer stopped"
    assert result["qc"] == "ok"
    assert committed > 0, "a 1 s-patience writer could never commit during the backup"


def test_reconcile_refuses_when_fts_holds_an_orphan_of_any_spelling():
    """R579/R580: a stale FTS entry with no series row - 'statcanX' OR a bare legacy 'V2132579'
    - survives a range reconcile invisibly; the anti-join catches every spelling."""
    from tools.catalog_statcan_tables import fts_off_range_count, reconcile_fts_source
    for orphan in ("statcanX", "V2132579"):
        con = _catalogue_with_standalone_fts()
        assert fts_off_range_count(con) == 0
        con.execute("INSERT INTO series_fts(series_id,title,geography) VALUES(?,'stale','Canada')", (orphan,))
        con.commit()
        assert fts_off_range_count(con) == 1
        assert reconcile_fts_source(con) == 1


def test_backup_aborts_on_a_writer_that_never_pauses(tmp_path):
    """R580: an incremental backup restarts after every foreign write; under a steady writer it
    makes ZERO progress forever. It must abort and name the holders, not hang."""
    import threading, time
    from tools.catalog_statcan_tables import backup_sqlite
    src = str(tmp_path / "live.db"); dst = str(tmp_path / "bak.db")
    con = sqlite3.connect(src); con.execute("CREATE TABLE t(x)")
    con.executemany("INSERT INTO t VALUES(?)", [("x" * 500,) for _ in range(60_000)]); con.commit(); con.close()
    stop = threading.Event()
    def writer():
        w = sqlite3.connect(src, timeout=0.2)
        while not stop.is_set():
            try:
                w.execute("INSERT INTO t VALUES('w')"); w.commit()
            except sqlite3.OperationalError:
                pass
            time.sleep(0.005)
        w.close()
    th = threading.Thread(target=writer, daemon=True); th.start()
    t0 = time.time()
    try:
        try:
            backup_sqlite(src, dst, pages=32, sleep=0.005, stall_steps=20, max_restarts=50)
        except RuntimeError as e:
            assert "backup stalled" in str(e) and time.time() - t0 < 60
        else:
            raise AssertionError("backup completed under a steady writer - the stall guard did not fire")
    finally:
        stop.set(); th.join(timeout=5)


def test_db_holders_sees_another_process_holding_the_file(tmp_path):
    import subprocess, sys, time
    from tools.catalog_statcan_tables import db_holders
    p = str(tmp_path / "held.db")
    sqlite3.connect(p).close()
    child = subprocess.Popen([sys.executable, "-c",
                              f"import sqlite3,time; c=sqlite3.connect(r'{p}'); c.execute('create table t(x)'); "
                              f"c.execute('begin'); c.execute('insert into t values(1)'); time.sleep(20)"])
    try:
        deadline = time.time() + 10; seen = []
        while time.time() < deadline and not seen:
            seen = [h for h in db_holders(p, pids={child.pid}) if h[0] == child.pid]
            time.sleep(0.3)
        assert seen, "the child holding the db open was not detected"
    finally:
        child.kill(); child.wait()
    assert all(h[0] != child.pid for h in db_holders(p, pids={child.pid}))


def test_a_single_commit_restarts_the_backup_once_and_it_still_completes(tmp_path):
    """R581: a stall is per ATTEMPT. One ordinary commit mid-copy restarts the backup once; the
    next attempt runs to completion - a global lowest-remaining baseline aborted it falsely."""
    import threading, time
    from tools.catalog_statcan_tables import backup_sqlite
    src = str(tmp_path / "live.db"); dst = str(tmp_path / "bak.db")
    con = sqlite3.connect(src); con.execute("CREATE TABLE t(x)")
    con.executemany("INSERT INTO t VALUES(?)", [("x" * 500,) for _ in range(120_000)]); con.commit(); con.close()
    def one_commit():
        time.sleep(0.4)                       # let the copy get well past the first chunks
        w = sqlite3.connect(src, timeout=5.0); w.execute("INSERT INTO t VALUES('once')"); w.commit(); w.close()
    th = threading.Thread(target=one_commit); th.start()
    assert backup_sqlite(src, dst, pages=16, sleep=0.002) == "ok"
    th.join()
    assert sqlite3.connect(dst).execute("SELECT count(*) FROM t").fetchone()[0] in (120_000, 120_001)


def _big_db(path, rows):
    con = sqlite3.connect(path); con.execute("CREATE TABLE t(x)")
    con.executemany("INSERT INTO t VALUES(?)", [("x" * 500,) for _ in range(rows)]); con.commit(); con.close()


def test_livelock_is_caught_at_the_SHIPPED_page_size_and_leaves_no_torn_copy(tmp_path):
    """R583: at pages=4096 a restarted copy completes its full chunk from the beginning every time,
    so `remaining` never rises and never falls - the restart-only detector was inert. The
    no-progress signal must fire, and the failed copy must not survive under the real name."""
    import threading, time
    from tools.catalog_statcan_tables import backup_sqlite
    src = str(tmp_path / "live.db"); dst = str(tmp_path / "bak.db")
    _big_db(src, 60_000)                                       # ~30 MB > one 4096-page step
    stop = threading.Event()
    def writer():
        w = sqlite3.connect(src, timeout=0.2)
        while not stop.is_set():
            try:
                w.execute("INSERT INTO t VALUES('w')"); w.commit()
            except sqlite3.OperationalError:
                pass
            time.sleep(0.002)
        w.close()
    th = threading.Thread(target=writer, daemon=True); th.start()
    t0 = time.time()
    try:
        try:
            backup_sqlite(src, dst, pages=4096, sleep=0.002, stall_steps=10)
        except RuntimeError as e:
            assert "no progress" in str(e) or "restarts" in str(e), str(e)
            assert time.time() - t0 < 60
        else:
            raise AssertionError("backup completed under a steady writer at pages=4096 - the guard did not fire")
    finally:
        stop.set(); th.join(timeout=5)
    assert not os.path.exists(dst), "a torn copy was left under the authoritative name"
    assert not os.path.exists(dst + ".partial")


def test_one_commit_at_the_shipped_page_size_still_completes(tmp_path):
    import threading, time
    from tools.catalog_statcan_tables import backup_sqlite
    src = str(tmp_path / "live.db"); dst = str(tmp_path / "bak.db")
    _big_db(src, 240_000)                                      # ~120 MB: several 4096-page steps
    def one_commit():
        time.sleep(0.5)
        w = sqlite3.connect(src, timeout=5.0); w.execute("INSERT INTO t VALUES('once')"); w.commit(); w.close()
    th = threading.Thread(target=one_commit); th.start()
    assert backup_sqlite(src, dst, pages=4096, sleep=0.01) == "ok"
    th.join()
    assert os.path.exists(dst) and not os.path.exists(dst + ".partial")


def test_purge_refuses_when_the_unlisted_count_differs_from_the_expectation(tmp_path, monkeypatch):
    """R584/R500: a purge runs only on the population that was measured."""
    from tools import catalog_statcan_tables as t
    monkeypatch.setattr(t, "ROOT", str(tmp_path)); os.makedirs(tmp_path / "logs")
    con = _catalogue_with_standalone_fts()
    assert t.audit_catalogue(con, set(), "series", purge=True, expect_unlisted=20) == 1     # found 1, expected 20
    assert con.execute("SELECT count(*) FROM series WHERE source_id='statcan'").fetchone()[0] == 1
    assert t.audit_catalogue(con, set(), "series", purge=True, expect_unlisted=1) == 0
    assert con.execute("SELECT count(*) FROM series WHERE source_id='statcan'").fetchone()[0] == 0
