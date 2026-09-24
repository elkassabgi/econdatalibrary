"""core/load_d1_rest.py and core/load_d1_chunked.py on core.d1_remote (plan step 1). No network, no wrangler."""
import pytest

from core import cutover, d1_remote, load_d1_chunked, load_d1_rest


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(load_d1_rest.time, "sleep", lambda s: None)
    monkeypatch.setattr(load_d1_chunked.time, "sleep", lambda s: None)


def test_rest_loader_retries_then_succeeds(monkeypatch):
    calls = []

    def query(db, sql, **kw):
        calls.append((db, sql, kw.get("timeout")))
        if len(calls) < 3:
            raise RuntimeError("D1 econ-catalog: HTTP 503")
        return {"results": []}
    monkeypatch.setattr(d1_remote, "query", query)
    load_d1_rest.execute("tok", "INSERT OR REPLACE INTO t VALUES (1);")
    assert len(calls) == 3 and calls[0] == ("econ-catalog", "INSERT OR REPLACE INTO t VALUES (1);", 90)


def test_rest_loader_gives_up_after_its_retries(monkeypatch):
    monkeypatch.setattr(d1_remote, "query", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("HTTP 500")))
    with pytest.raises(RuntimeError, match="HTTP 500"):
        load_d1_rest.execute("tok", "x")


def test_rest_loader_never_retries_a_refusal(monkeypatch):
    calls = []

    def query(*a, **k):
        calls.append(1)
        raise cutover.CutoverRefused("refused: after T0")
    monkeypatch.setattr(d1_remote, "query", query)
    with pytest.raises(cutover.CutoverRefused):
        load_d1_rest.execute("tok", "INSERT INTO t VALUES (1)")
    assert calls == [1]


def test_chunk_loader_counts_error_in_the_output_as_a_failure(monkeypatch):
    outs = iter(["Executed... ERROR: SQLITE_CONSTRAINT", "Executed 40 commands"])
    seen = []
    monkeypatch.setattr(d1_remote, "execute_file", lambda db, p, **kw: seen.append((db, p, kw)) or next(outs))
    assert load_d1_chunked.run_chunk("c1.sql") is True
    assert len(seen) == 2 and seen[0][0] == "econ-catalog" and seen[0][2] == {"timeout": 1800}


def test_chunk_loader_gives_up_after_three(monkeypatch):
    def fail(*a, **k):
        raise d1_remote.D1Unreachable("wrangler timed out after 1800 s")
    monkeypatch.setattr(d1_remote, "execute_file", fail)
    assert load_d1_chunked.run_chunk("c1.sql") is False


def test_chunk_loader_never_retries_a_refusal(monkeypatch):
    calls = []

    def refused(*a, **k):
        calls.append(1)
        raise cutover.CutoverRefused("refused after T0")
    monkeypatch.setattr(d1_remote, "execute_file", refused)
    with pytest.raises(cutover.CutoverRefused):
        load_d1_chunked.run_chunk("c1.sql")
    assert calls == [1]


def test_remote_count_goes_through_the_chokepoint(monkeypatch):
    monkeypatch.setattr(d1_remote, "query", lambda db, sql, **kw: {"results": [{"n": 7}]} if
                        (db, sql) == ("econ-catalog", "SELECT COUNT(*) AS n FROM series") else None)
    assert load_d1_chunked.remote_count("tok", "series") == 7


# ---- tools/rebuild_series_fts.py on core.d1_remote ------------------------------------------------------------
def _rebuild():
    import importlib.util
    import os
    p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "rebuild_series_fts.py")
    spec = importlib.util.spec_from_file_location("rebuild_series_fts_undertest", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_rebuild_fts_sends_a_failed_statement_once(monkeypatch):
    """The old helper retried EVERY failure three times, a chunk INSERT the server had taken included (fts5
    has no key: duplicates). Now d1_remote decides - it retries only wrangler's auth error."""
    m = _rebuild()
    calls = []

    def run_json(db, sql, **kw):
        calls.append((db, sql))
        raise RuntimeError("D1 econ-catalog: wrangler exit 1: stderr='timeout'")
    monkeypatch.setattr(d1_remote, "run_json", run_json)
    with pytest.raises(SystemExit, match="D1 statement failed"):
        m.d1("INSERT INTO series_fts_new SELECT 1")
    assert calls == [("econ-catalog", "INSERT INTO series_fts_new SELECT 1")]


def test_rebuild_fts_returns_the_block_and_refuses_an_unsuccessful_one(monkeypatch):
    m = _rebuild()
    monkeypatch.setattr(d1_remote, "run_json", lambda db, sql, **kw: [{"results": [{"n": 3}], "success": True}])
    assert m.d1("SELECT COUNT(*) AS n FROM series")["results"][0]["n"] == 3
    monkeypatch.setattr(d1_remote, "run_json", lambda db, sql, **kw: [{"results": [], "success": False}])
    with pytest.raises(SystemExit, match="failed"):
        m.d1("SELECT 1")


def test_rebuild_fts_swap_runs_once_through_execute_file():
    src = open(_rebuild().__file__, encoding="utf-8").read()
    assert "d1_remote.execute_file(DB, swap, timeout=600)" in src and "npx" not in src


# ---- tools/refresh_flowgrain_dates.py on core.d1_remote ------------------------------------------------------
def test_flowgrain_dates_reads_and_batch_go_through_d1_remote(monkeypatch):
    import importlib.util
    import os
    p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "refresh_flowgrain_dates.py")
    spec = importlib.util.spec_from_file_location("refresh_flowgrain_dates_undertest", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    seen = []
    monkeypatch.setattr(d1_remote, "run_json", lambda db, sql, **kw: seen.append(("cmd", db)) or
                        [{"results": [{"series_id": "scb:T1", "start_date": "2000-01-01", "end_date": "2020-12-31"}],
                          "meta": {"rows_read": 1}}])
    monkeypatch.setattr(d1_remote, "execute_file", lambda db, path, **kw: seen.append(("file", db, kw)) or
                        [{"results": [], "meta": {"changes": 1}}])
    got, rr = m.d1_rows(["scb:T1"])
    assert got == {"scb:T1": ("2000-01-01", "2020-12-31")} and rr == 1
    assert m._wrangler_json(["--file", "x.sql"])[0]["meta"]["changes"] == 1
    assert seen == [("cmd", "econ-catalog"), ("file", "econ-catalog", {"timeout": 600, "json_out": True})]
    with pytest.raises(ValueError):
        m._wrangler_json(["--oops", "x"])


def test_execute_file_json_out_returns_the_statement_results(monkeypatch):
    import types
    calls = []

    def wrangler(args, **kw):
        calls.append(args)
        return types.SimpleNamespace(returncode=0, stderr="",
                                     stdout='banner\n[{"results": [], "meta": {"changes": 2}, "success": true}]')
    monkeypatch.setattr(d1_remote, "_wrangler", wrangler)
    monkeypatch.setattr(d1_remote, "is_cut_over", lambda: False)
    assert d1_remote.execute_file("econ-catalog", "x.sql", json_out=True)[0]["meta"]["changes"] == 2
    assert calls[-1][-1] == "--json"
    assert "banner" in d1_remote.execute_file("econ-catalog", "x.sql") and calls[-1][-1] != "--json"


# ---- tools/migrate_noaa_shard.py on core.d1_remote -----------------------------------------------------------
def test_noaa_shard_statements_go_through_d1_remote(monkeypatch):
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))
    import migrate_noaa_shard as m
    seen = []
    monkeypatch.setattr(d1_remote, "run_json", lambda db, sql, **kw: seen.append((db, kw.get("timeout"))) or
                        [{"results": [{"n": 5}], "success": True}])
    assert m._shard_fts_count() == 5 and seen == [("econ-catalog-climate", 600)]

    def fails(db, sql, **kw):
        raise RuntimeError("D1 econ-catalog: wrangler exit 1")
    monkeypatch.setattr(d1_remote, "run_json", fails)
    with pytest.raises(SystemExit, match="FATAL: D1 econ-catalog-climate"):
        m._shard_fts_count()

    def refused(db, sql, **kw):
        raise cutover.CutoverRefused("refused: after T0")
    monkeypatch.setattr(d1_remote, "run_json", refused)
    with pytest.raises(cutover.CutoverRefused):
        m._d1("econ-catalog", "DELETE FROM series WHERE 0")
    src = open(m.__file__, encoding="utf-8").read()
    assert "npx" not in src and "subprocess" not in src


# ---- tools/sync_titles_to_d1.py on core.d1_remote ------------------------------------------------------------
def _titles():
    import importlib.util
    import os
    p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "sync_titles_to_d1.py")
    spec = importlib.util.spec_from_file_location("sync_titles_to_d1_undertest", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_sync_titles_a_failed_read_is_none_not_empty(monkeypatch):
    m = _titles()
    seen = []

    def run_json(db, sql, **kw):
        seen.append(db)
        raise RuntimeError("D1 econ-catalog-climate: wrangler exit 1")
    monkeypatch.setattr(d1_remote, "run_json", run_json)
    assert m.raw_ids_in_d1("noaa") is None and seen == ["econ-catalog-climate"], "noaa routes to the shard"
    monkeypatch.setattr(d1_remote, "run_json", lambda db, sql, **kw: [{"results": []}])
    assert m.raw_ids_in_d1("idb") == []
    monkeypatch.setattr(d1_remote, "run_json", lambda db, sql, **kw: [{"results": [{"series_id": "idb:x"}]}])
    assert m.raw_ids_in_d1("idb") == ["idb:x"]


def test_sync_titles_push_collects_failures_and_stops_on_a_refusal(monkeypatch, tmp_path):
    import os
    import sqlite3
    m = _titles()
    monkeypatch.setattr(m, "OUTDIR", str(tmp_path / "out"))
    cat = tmp_path / "catalog.db"
    with sqlite3.connect(cat) as c:
        c.execute("CREATE TABLE series (series_id TEXT, title TEXT)")
        c.execute("INSERT INTO series VALUES ('idb:x', 'A real title')")
    c.close()
    monkeypatch.setattr(m, "CATALOG", str(cat))
    monkeypatch.setattr(d1_remote, "run_json", lambda db, sql, **kw: [{"results": [{"series_id": "idb:x"}]}])
    sent = []

    def execute_file(db, path, **kw):
        sent.append((db, os.path.basename(path), kw.get("tries", 1)))
        raise RuntimeError("wrangler exit 1: busy")
    monkeypatch.setattr(d1_remote, "execute_file", execute_file)
    monkeypatch.setattr(m.sys, "argv", ["x", "idb", "--push"])
    assert m.main() == 1
    assert sent == [("econ-catalog", "idb_00000.sql", 1), ("econ-catalog", "idb_zz_fts.sql", 1)], sent

    def refused(*a, **k):
        raise cutover.CutoverRefused("refused: after T0")
    monkeypatch.setattr(d1_remote, "execute_file", refused)
    with pytest.raises(cutover.CutoverRefused):
        m.main()
