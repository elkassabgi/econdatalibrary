"""core/d1_remote.py run_json / rows / execute_file - the roads the legacy remote-D1 callers move onto (plan
step 1). No network and no wrangler: subprocess.run is replaced and records what it was asked to run."""
import os
import subprocess
import types

import pytest

from core import cutover, d1_remote

PAYLOAD = '[{"results": [{"n": 3}], "meta": {"rows_read": 7}, "success": true}]'


@pytest.fixture
def before_t0(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "absent" / "CUTOVER"))
    js = tmp_path / "wrangler.js"
    js.write_text("")
    monkeypatch.setattr(d1_remote, "WRANGLER_JS", str(js))
    monkeypatch.setattr("time.sleep", lambda s: None)


@pytest.fixture
def after_t0(tmp_path, monkeypatch):
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))


def _fake(monkeypatch, *answers):
    calls = []
    answers = list(answers)

    def run(cmd, **kw):
        calls.append(cmd)
        rc, out, err = answers.pop(0) if len(answers) > 1 else answers[0]
        return types.SimpleNamespace(returncode=rc, stdout=out, stderr=err)
    monkeypatch.setattr(subprocess, "run", run)
    return calls


def test_before_t0_it_runs_wrangler_as_the_callers_did(before_t0, monkeypatch):
    calls = _fake(monkeypatch, (0, PAYLOAD, ""))
    assert d1_remote.rows("econ-catalog", "SELECT COUNT(*) n FROM series") == ([{"n": 3}], 7)
    cmd = calls[0]
    assert cmd[0] == "node" and cmd[1] == d1_remote.WRANGLER_JS
    assert cmd[2:] == ["d1", "execute", "econ-catalog", "--remote", "--json", "--command",
                       "SELECT COUNT(*) n FROM series"]


def test_before_t0_a_write_still_runs(before_t0, monkeypatch):
    """Today's behaviour is kept until T0: stamp_source_data_through writes through this road."""
    calls = _fake(monkeypatch, (0, '[{"results": [], "meta": {"rows_written": 1}}]', ""))
    d1_remote.run_json("econ-catalog", "INSERT INTO t VALUES (1)")
    assert len(calls) == 1


@pytest.mark.parametrize("stdout", [
    'bindings [{"binding": "CATALOG"}]\n' + PAYLOAD,                 # another array first
    PAYLOAD + "\n[1, 2]",                                            # another array after
    "│ progress [##] │\n" + PAYLOAD + "\n",                # a progress line with brackets
    "[not json\n" + PAYLOAD,
])
def test_the_result_array_is_found_whatever_wrangler_prints_around_it(stdout):
    assert d1_remote.statement_results(stdout)[0]["results"] == [{"n": 3}]


def test_no_result_is_an_error_not_an_empty_answer():
    with pytest.raises(RuntimeError, match="no query result"):
        d1_remote.statement_results('[{"binding": "CATALOG"}]')


def test_the_auth_transient_is_retried_twice_then_raised(before_t0, monkeypatch):
    calls = _fake(monkeypatch, (1, "", "Authentication error [code: 10000]"))
    with pytest.raises(RuntimeError, match="10000"):
        d1_remote.run_json("econ-catalog", "SELECT 1")
    assert len(calls) == 3
    calls = _fake(monkeypatch, (1, "", "Authentication error [code: 10000]"), (0, PAYLOAD, ""))
    assert d1_remote.run_json("econ-catalog", "SELECT 1")[0]["results"] == [{"n": 3}]
    assert len(calls) == 2


def test_other_failures_are_not_retried(before_t0, monkeypatch):
    calls = _fake(monkeypatch, (1, "", "no such table: x"))
    with pytest.raises(RuntimeError, match="no such table"):
        d1_remote.run_json("econ-catalog", "SELECT * FROM x")
    assert len(calls) == 1


def test_a_missing_wrangler_is_a_runtime_error(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "absent" / "CUTOVER"))
    monkeypatch.setattr(d1_remote, "WRANGLER_JS", str(tmp_path / "none.js"))
    with pytest.raises(RuntimeError, match="no wrangler"):
        d1_remote.run_json("econ-catalog", "SELECT 1")


def test_only_the_econ_databases(before_t0, monkeypatch):
    _fake(monkeypatch, (0, PAYLOAD, ""))
    for fn in (lambda: d1_remote.run_json("hfdatalibrary-db", "SELECT 1"),
               lambda: d1_remote.execute_file("hfdatalibrary-db", "x.sql")):
        with pytest.raises(ValueError):
            fn()


def test_after_t0_reads_go_over_rest_and_never_through_wrangler(after_t0, monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("wrangler was run after T0"))
    seen = []
    monkeypatch.setattr(d1_remote, "query", lambda db, sql, **k: seen.append((db, sql)) or
                        {"results": [{"n": 1}], "meta": {"rows_read": 2}})
    assert d1_remote.rows("econ-catalog-climate", "SELECT COUNT(*) n FROM series") == ([{"n": 1}], 2)
    assert seen == [("econ-catalog-climate", "SELECT COUNT(*) n FROM series")]


def test_after_t0_a_write_is_refused_before_any_network(after_t0, monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("wrangler was run after T0"))
    monkeypatch.setattr(d1_remote.urllib.request, "urlopen", lambda *a, **k: pytest.fail("network after T0"))
    monkeypatch.setenv("D1_READ_TOKEN", "t")
    with pytest.raises(d1_remote.NotAReadStatement):
        d1_remote.run_json("econ-catalog", "INSERT INTO source_data_through VALUES ('x', NULL)")


def test_execute_file_before_and_after_t0(before_t0, monkeypatch, tmp_path):
    calls = _fake(monkeypatch, (0, "ok", ""))
    assert d1_remote.execute_file("econ-catalog", "load.sql") == "ok"
    assert calls[0][2:] == ["d1", "execute", "econ-catalog", "--remote", "--yes",
                            f"--file={os.path.abspath('load.sql')}"]
    _fake(monkeypatch, (1, "", "SQLITE_TOOBIG"))
    with pytest.raises(RuntimeError, match="SQLITE_TOOBIG"):
        d1_remote.execute_file("econ-catalog", "load.sql")
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("wrangler was run after T0"))
    with pytest.raises(cutover.CutoverRefused):
        d1_remote.execute_file("econ-catalog", "load.sql")


# ---- R1183: the "could not look" contract, and the retry policy ------------------------------------------
@pytest.mark.parametrize("exc", [subprocess.TimeoutExpired(["node"], 900), FileNotFoundError(2, "node")])
def test_not_reaching_d1_is_always_a_runtime_error(before_t0, monkeypatch, exc):
    def run(cmd, **kw):
        raise exc
    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(d1_remote.D1Unreachable):
        d1_remote.run_json("econ-catalog", "SELECT 1")
    assert issubclass(d1_remote.D1Unreachable, RuntimeError) and not issubclass(d1_remote.D1Unreachable, SystemExit)


def test_after_t0_a_network_failure_is_a_runtime_error(after_t0, monkeypatch):
    import urllib.error
    monkeypatch.setenv("D1_READ_TOKEN", "t")

    def down(*a, **k):
        raise urllib.error.URLError("no route")
    monkeypatch.setattr(d1_remote.urllib.request, "urlopen", down)
    with pytest.raises(d1_remote.D1Unreachable, match="URLError"):
        d1_remote.run_json("econ-catalog", "SELECT 1")


def test_the_auth_transient_on_stdout_is_retried_too(before_t0, monkeypatch):
    """wrangler --json prints its errors on STDOUT."""
    calls = _fake(monkeypatch, (1, '{"error": {"text": "Authentication error [code: 10000]"}}', ""), (0, PAYLOAD, ""))
    assert d1_remote.run_json("econ-catalog", "SELECT 1")[0]["results"] == [{"n": 3}] and len(calls) == 2


def test_a_success_is_never_retried(before_t0, monkeypatch):
    calls = _fake(monkeypatch, (0, PAYLOAD + " code: 10000 appears in a title", ""))
    d1_remote.run_json("econ-catalog", "SELECT 1")
    assert len(calls) == 1


def test_an_empty_array_is_not_a_result():
    with pytest.raises(RuntimeError, match="no query result"):
        d1_remote.statement_results("[]")


def test_the_timeout_reaches_wrangler(before_t0, monkeypatch):
    seen = []

    def run(cmd, **kw):
        seen.append(kw.get("timeout"))
        return types.SimpleNamespace(returncode=0, stdout=PAYLOAD, stderr="")
    monkeypatch.setattr(subprocess, "run", run)
    d1_remote.run_json("econ-catalog", "SELECT 1", timeout=300)
    d1_remote.execute_file("econ-catalog", "x.sql", timeout=600)
    assert seen == [300, 600]


def test_execute_file_runs_once_by_default_even_on_the_auth_error(before_t0, monkeypatch):
    """An import that failed after it started may have applied part of the file (R1183 finding 4)."""
    calls = _fake(monkeypatch, (1, "", "Authentication error [code: 10000]"))
    with pytest.raises(RuntimeError, match="after 1 attempt"):
        d1_remote.execute_file("econ-catalog", "x.sql")
    assert len(calls) == 1


def test_execute_file_retries_only_when_the_caller_asks(before_t0, monkeypatch):
    calls = _fake(monkeypatch, (1, "", "busy"), (1, "", "busy"), (0, "done", ""))
    told = []
    assert d1_remote.execute_file("econ-catalog", "x.sql", tries=4, on_retry=lambda n, why: told.append(n)) == "done"
    assert len(calls) == 3 and told == [1, 2]
    _fake(monkeypatch, (1, "", "still busy"))
    with pytest.raises(RuntimeError, match="after 4 attempt"):
        d1_remote.execute_file("econ-catalog", "x.sql", tries=4)


def test_the_daily_sync_keeps_its_four_tries_and_its_fatal_exit(monkeypatch, tmp_path):
    from core import sync_state_d1
    seen = []

    def execute_file(db, path, **kw):
        seen.append((db, kw.get("tries"), kw.get("timeout"), kw.get("retry_timeouts")))
        if path.endswith("bad.sql"):
            e = d1_remote.D1Unreachable("wrangler timed out after 600 s")
            e.stdout, e.stderr = "FULL STDOUT", "FULL STDERR"
            raise e
        return "Executed 3 commands"
    monkeypatch.setattr(d1_remote, "execute_file", execute_file)
    monkeypatch.setattr(sync_state_d1.shutil, "which", lambda n: "node")
    js = tmp_path / "wrangler.js"
    js.write_text("")
    monkeypatch.setattr(d1_remote, "WRANGLER_JS", str(js))
    sync_state_d1.execute_remote([str(tmp_path / "a.sql")], idempotent=True)
    assert seen == [(sync_state_d1.D1_DATABASE, 4, 600, True)]
    sync_state_d1.execute_remote([str(tmp_path / "a.sql")])
    assert seen[-1][3] is False, "only an idempotent caller may retry a timeout (R1185)"
    with pytest.raises(SystemExit, match="remaining chunks NOT executed"):
        sync_state_d1.execute_remote([str(tmp_path / "bad.sql"), str(tmp_path / "never.sql")], database="econ-catalog-climate")
    assert seen[-1][0] == "econ-catalog-climate" and len(seen) == 3, "it stopped at the first failed chunk"


def test_the_fatal_path_writes_wrangler_s_output_in_full(monkeypatch, tmp_path, capsys):
    from core import sync_state_d1

    def execute_file(db, path, **kw):
        e = RuntimeError("D1 econ-catalog: --file x failed after 4 attempt(s): exit 1: SQLITE_TOOBIG")
        e.stdout, e.stderr = "WRANGLER STDOUT LINE", "WRANGLER STDERR LINE"
        raise e
    monkeypatch.setattr(d1_remote, "execute_file", execute_file)
    monkeypatch.setattr(sync_state_d1.shutil, "which", lambda n: "node")
    js = tmp_path / "wrangler.js"
    js.write_text("")
    monkeypatch.setattr(d1_remote, "WRANGLER_JS", str(js))
    with pytest.raises(SystemExit):
        sync_state_d1.execute_remote([str(tmp_path / "a.sql")])
    err = capsys.readouterr().err
    assert "WRANGLER STDOUT LINE" in err and "WRANGLER STDERR LINE" in err and "SQLITE_TOOBIG" in err


def test_no_node_or_no_wrangler_is_fatal_before_any_call(monkeypatch, tmp_path):
    """M16: the checks look at the wrangler that actually runs (R1185 finding 6)."""
    from core import sync_state_d1
    monkeypatch.setattr(d1_remote, "execute_file", lambda *a, **k: pytest.fail("ran without its checks"))
    monkeypatch.setattr(sync_state_d1.shutil, "which", lambda n: None)
    with pytest.raises(SystemExit, match="node not on PATH"):
        sync_state_d1.execute_remote(["a.sql"])
    monkeypatch.setattr(sync_state_d1.shutil, "which", lambda n: "node")
    monkeypatch.setattr(d1_remote, "WRANGLER_JS", str(tmp_path / "missing.js"))
    with pytest.raises(SystemExit, match="no local wrangler install"):
        sync_state_d1.execute_remote(["a.sql"])


def test_the_retry_line_names_wrangler_s_error_not_its_banner(before_t0, monkeypatch, capsys):
    """M14 + R1185 finding 5: the retry line carries wrangler's LAST error line."""
    from core import sync_state_d1
    monkeypatch.setattr(sync_state_d1.shutil, "which", lambda n: "node")
    _fake(monkeypatch, (1, "wrangler 3.114.17 banner", "noise\nERROR: D1 import busy [code: 7500]"),
          (0, "Executed 1 command", ""))
    sync_state_d1.execute_remote(["a.sql"])
    out = capsys.readouterr().out
    assert "ERROR: D1 import busy [code: 7500] - retry 1/3" in out and "banner" not in out


def test_a_timeout_is_retried_only_when_the_caller_says_the_file_is_idempotent(before_t0, monkeypatch):
    calls = []

    def run(cmd, **kw):
        calls.append(1)
        raise subprocess.TimeoutExpired(cmd, 600)
    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(d1_remote.D1Unreachable, match="after 1 attempt"):
        d1_remote.execute_file("econ-catalog", "fts.sql", tries=4)
    assert len(calls) == 1, "a plain INSERT INTO series_fts may have been committed: never re-applied"
    calls.clear()
    with pytest.raises(d1_remote.D1Unreachable, match="after 4 attempt"):
        d1_remote.execute_file("econ-catalog", "freshness.sql", tries=4, retry_timeouts=True)
    assert len(calls) == 4


def test_the_retries_wait_5_10_15_seconds(before_t0, monkeypatch):
    """M5: the back-off between attempts."""
    slept = []
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))
    _fake(monkeypatch, (1, "", "busy"))
    with pytest.raises(RuntimeError):
        d1_remote.execute_file("econ-catalog", "x.sql", tries=4)
    assert slept == [5, 10, 15]


def test_never_reaching_d1_stays_unreachable_to_the_end(before_t0, monkeypatch):
    """M4: the final error of attempts that never reached D1 is D1Unreachable, not a plain RuntimeError."""
    def run(cmd, **kw):
        raise FileNotFoundError(2, "node")
    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(d1_remote.D1Unreachable):
        d1_remote.execute_file("econ-catalog", "x.sql", tries=3)


def test_the_audit_says_could_not_look(monkeypatch):
    """audit_d1_vs_catalog: D1 unreachable is SystemExit from d1_counts and an empty set from d1_ids."""
    import tools.audit_d1_vs_catalog as audit

    def down(db, sql, **kw):
        raise d1_remote.D1Unreachable("no wrangler")
    monkeypatch.setattr(d1_remote, "rows", down)
    with pytest.raises(SystemExit, match="no wrangler"):
        audit.d1_counts()
    assert audit.d1_ids("ecb") == set()


def test_the_worker_dir_is_the_repo_s():
    assert d1_remote.WORKER_DIR == os.path.join(os.path.dirname(os.path.dirname(d1_remote.__file__)), "api",
                                                "worker")
