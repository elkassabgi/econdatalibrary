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
    d1_remote.execute_file("econ-catalog", "load.sql")
    assert calls[0][2:] == ["d1", "execute", "econ-catalog", "--remote", "--file", "load.sql", "--yes"]
    _fake(monkeypatch, (1, "", "SQLITE_TOOBIG"))
    with pytest.raises(RuntimeError, match="SQLITE_TOOBIG"):
        d1_remote.execute_file("econ-catalog", "load.sql")
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("wrangler was run after T0"))
    with pytest.raises(cutover.CutoverRefused):
        d1_remote.execute_file("econ-catalog", "load.sql")


def test_the_worker_dir_is_the_repo_s():
    assert d1_remote.WORKER_DIR == os.path.join(os.path.dirname(os.path.dirname(d1_remote.__file__)), "api",
                                                "worker")
