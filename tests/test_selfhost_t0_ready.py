"""tools/selfhost/t0_ready.py - the check run before the CUTOVER flag is created (AR-153)."""
import json
import os
import subprocess
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
import t0_ready as T  # noqa: E402


def _root(tmp_path, legacy_lines, legacy_d1):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "catalog_db_legacy.txt").write_text("# header\n" + "".join(l + "\n" for l in legacy_lines))
    (tmp_path / "tests" / "test_d1_remote.py").write_text(f"import os\nLEGACY_REMOTE_D1 = {legacy_d1!r}\n")
    return str(tmp_path)


def test_the_legacy_lists_must_be_empty(tmp_path):
    r = _root(tmp_path, ["a.py", "b.py"], {"x.py"})
    assert T.legacy_catalogue(r) == (False, "2 file(s) still open the catalogue outside core.catalog_path")
    assert T.legacy_remote_d1(r)[0] is False and T.legacy_remote_d1(r)[1].startswith("1 file(s)")


def test_empty_legacy_lists_pass(tmp_path):
    r = _root(tmp_path, [], set())
    assert T.legacy_catalogue(r)[0] is True and T.legacy_remote_d1(r)[0] is True


@pytest.mark.parametrize("written", ["frozenset({'a.py'})", "set(['a.py'])", "OTHER", "set(OTHER)", "{*OTHER}",
                                     # R1185: bound once, then changed - the change must be seen
                                     "set()\nLEGACY_REMOTE_D1 |= {'a.py'}", "set()\nLEGACY_REMOTE_D1.add('a.py')",
                                     "set()\nLEGACY_REMOTE_D1 = {'a.py'}", "set()\nLEGACY_REMOTE_D1.update(OTHER)"])
def test_a_list_it_cannot_read_is_a_failure_not_empty(tmp_path, written):
    """R1183: any call used to read as the empty set, so frozenset({...}) printed READY."""
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_d1_remote.py").write_text(f"OTHER = {{'x'}}\nLEGACY_REMOTE_D1 = {written}\n")
    ok, detail = T.legacy_remote_d1(str(tmp_path))
    assert not ok and "cannot tell" in detail


def test_a_bare_empty_set_is_empty(tmp_path):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_d1_remote.py").write_text("LEGACY_REMOTE_D1 = set()\n")
    assert T.legacy_remote_d1(str(tmp_path))[0] is True


def test_a_missing_legacy_list_is_a_failure_not_a_pass(tmp_path):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_d1_remote.py").write_text("OTHER = 1\n")
    assert T.legacy_remote_d1(str(tmp_path))[0] is False


def test_the_real_lists_are_read():
    """Positive control on the repo as it is today: both lists are non-empty, so both checks fail."""
    assert T.legacy_catalogue()[0] is False and T.legacy_remote_d1()[0] is False


@pytest.mark.parametrize("src,ok", [
    ("$env:AQUEDUCT_BACKEND = 'selfhost'\n& $py -m updater.run @args\n", True),
    ("$env:AQUEDUCT_BACKEND = 'r2'\n& $py -m updater.run\n", False),
    ("& $py -m updater.run\n", False),                                            # never set
    ("$env:AQUEDUCT_BACKEND = 'selfhost'\n& $py -m updater.run --pull-state\n", False),
    ("$env:AQUEDUCT_BACKEND = 'selfhost'\n& $py -m updater.run --push-state\n", False),
    ("# $env:AQUEDUCT_BACKEND = 'r2' (old)\n$env:AQUEDUCT_BACKEND = \"selfhost\"\n# --pull-state was here\n", True),
])
def test_the_launcher_check(tmp_path, src, ok):
    p = tmp_path / "run.ps1"
    p.write_text(src)
    assert T.launcher(str(p))[0] is ok


def test_the_real_launcher_is_not_ready_yet():
    ok, detail = T.launcher()
    assert not ok and "r2" in detail and "--pull-state" in detail


def _gh(states):
    def run(cmd, **kw):
        assert cmd[:3] == ["gh", "workflow", "list"]
        out = [{"name": n, "path": f".github/workflows/{n}.yml", "state": s} for n, s in states.items()]
        return types.SimpleNamespace(returncode=0, stdout=json.dumps(out), stderr="")
    return run


def test_ci_writers_must_all_be_disabled():
    off = {n: "disabled_manually" for n in T.CI_WRITERS}
    assert T.ci_writers(_gh({**off, "selfhost-watch": "active"})) == (True, "all disabled")
    ok, detail = T.ci_writers(_gh({**off, "updater-heavy": "active"}))
    assert not ok and "updater-heavy=active" in detail
    ok, detail = T.ci_writers(_gh({n: s for n, s in off.items() if n != "sec-edgar-daily"}))
    assert not ok and "sec-edgar-daily=MISSING" in detail


def test_gh_failing_is_a_failure():
    fail = lambda *a, **k: types.SimpleNamespace(returncode=1, stdout="", stderr="auth required")  # noqa: E731
    assert T.ci_writers(fail)[0] is False


def test_the_preflight_check_restores_what_it_changed(monkeypatch):
    from core import cutover
    before = cutover.is_cut_over
    monkeypatch.setenv("AQUEDUCT_BACKEND", "r2")
    ok, detail = T.preflight()
    assert not ok and "must be" in detail, "a worktree is not the production checkout"
    assert cutover.is_cut_over is before and os.environ["AQUEDUCT_BACKEND"] == "r2"


def test_ready_only_when_every_check_passes(monkeypatch, capsys):
    monkeypatch.setattr(T, "CHECKS", [("a", lambda: (True, "x")), ("b", lambda: (True, "y"))])
    assert T.main() == 0 and capsys.readouterr().out.strip().endswith("READY")
    monkeypatch.setattr(T, "CHECKS", [("a", lambda: (True, "x")), ("b", lambda: (False, "y"))])
    assert T.main() == 1 and "NOT READY" in capsys.readouterr().out


def test_a_check_that_cannot_run_is_a_failure(monkeypatch, capsys):
    def boom():
        raise OSError("no network")
    monkeypatch.setattr(T, "CHECKS", [("a", lambda: (True, "x")), ("edge-state", boom)])
    assert T.main() == 1
    out = capsys.readouterr().out
    assert "FAIL  edge-state" in out and "could not check" in out


def test_the_state_db_check(tmp_path):
    import sqlite3
    assert T.state_db(str(tmp_path / "none.db"))[0] is False
    p = tmp_path / "state.db"
    c = sqlite3.connect(p)
    c.executescript("CREATE TABLE unit_state (source_id TEXT, unit_id TEXT);"
                    "CREATE TABLE source_state (source_id TEXT);")
    c.commit()
    assert T.state_db(str(p))[0] is False, "empty tables: /v1/last-updates would be empty"
    c.execute("INSERT INTO unit_state VALUES ('ecb', '_all')")
    c.execute("INSERT INTO source_state VALUES ('ecb')")
    c.commit()
    c.close()
    assert T.state_db(str(p))[0] is True


def test_a_d1_only_source_without_a_local_writer_is_not_ready(monkeypatch):
    """R1191 finding 1: sec_edgar's freshness lives only in D1 (R737: the catalogue copy is not its truth)."""
    from core import sync_state_d1
    ok, detail = T.d1_only_sources()
    assert ok is False and "sec_edgar" in detail, "the real state today: no local sec_edgar writer yet"
    monkeypatch.setattr(sync_state_d1, "LOCAL_FRESHNESS_WRITERS", {"sec_edgar": "no.such.module"})
    ok, detail = T.d1_only_sources()
    assert ok is False and "ModuleNotFoundError" in detail, "R1195: a name alone passed"
    monkeypatch.setattr(sync_state_d1, "LOCAL_FRESHNESS_WRITERS", {"sec_edgar": "json"})
    assert T.d1_only_sources() == (False, "writers that cannot run: sec_edgar=json (no data_through)")
    fake = types.ModuleType("fake_sec_writer")
    fake.data_through = lambda conn: "2026-09-04"
    monkeypatch.setitem(sys.modules, "fake_sec_writer", fake)
    monkeypatch.setattr(sync_state_d1, "LOCAL_FRESHNESS_WRITERS", {"sec_edgar": "fake_sec_writer"})
    assert T.d1_only_sources()[0] is True
    monkeypatch.setattr(sync_state_d1, "DATA_THROUGH_FROM_D1", frozenset({"sec_edgar", "other"}))
    ok, detail = T.d1_only_sources()
    assert ok is False and "other" in detail and "sec_edgar" not in detail
    assert ("d1-only-sources", T.d1_only_sources) in T.CHECKS


# ---- R1207: the 13F gate and the drained CI, as checks, not plan prose ------------------------------------------
def _runs(by_workflow, rc=0):
    def run(cmd, **kw):
        assert cmd[:3] == ["gh", "run", "list"] and "--workflow" in cmd
        wf = cmd[cmd.index("--workflow") + 1].replace(".yml", "")
        out = [{"databaseId": i, "status": s, "headSha": "abc"} for i, s in enumerate(by_workflow.get(wf, []))]
        return types.SimpleNamespace(returncode=rc, stdout=json.dumps(out), stderr="auth required" if rc else "")
    return run


def test_ci_drained_needs_every_run_completed():
    done = {n: ["completed", "completed"] for n in T.CI_WRITERS}
    assert T.ci_drained(_runs(done)) == (True, "no run left to happen")
    for status in ("queued", "in_progress", "pending", "waiting", "requested"):
        ok, detail = T.ci_drained(_runs({**done, "updater-heavy": ["completed", status]}))
        assert not ok and f"updater-heavy#1={status}" in detail, (status, detail)
    assert T.ci_drained(_runs(done, rc=1))[0] is False, "gh failing is a failure, never a pass"


def _registry(tmp_path, ids, with_module=True):
    (tmp_path / "updater").mkdir(exist_ok=True)
    (tmp_path / "updater" / "registry.yaml").write_text(
        "sources:\n" + "".join(f"- source_id: {i}\n" for i in ids), encoding="utf-8")
    if with_module:
        (tmp_path / "updater" / "state_migrations.py").write_text("# the move\n", encoding="utf-8")
    return str(tmp_path)


@pytest.fixture
def fake_move(monkeypatch, tmp_path):
    """updater.state_migrations as the 13F branch ships it: pending(conn) -> rows left to move."""
    left = {"n": 0}
    mod = types.SimpleNamespace(pending=lambda con: left["n"])
    import updater
    monkeypatch.setitem(sys.modules, "updater.state_migrations", mod)
    monkeypatch.setattr(updater, "state_migrations", mod, raising=False)
    db = tmp_path / "state.db"
    import sqlite3
    sqlite3.connect(db).close()
    return left, str(db)


def test_thirteen_f_needs_the_rename_in_this_checkout(tmp_path, fake_move):
    left, db = fake_move
    ok, detail = T.thirteen_f(_registry(tmp_path, ["sec_edgar", "ecb"]), db)
    assert not ok and "not in this checkout" in detail, "an entry still named sec_edgar is the R1193 collision"
    ok, detail = T.thirteen_f(_registry(tmp_path, ["ecb"]), db)
    assert not ok and "sec_edgar_13f=False" in detail


def test_thirteen_f_needs_the_move_module_and_nothing_left_to_move(tmp_path, fake_move):
    left, db = fake_move
    root = tmp_path / "noshim"
    root.mkdir()
    ok, detail = T.thirteen_f(_registry(root, ["sec_edgar_13f"], with_module=False), db)
    assert not ok and "state_migrations.py is missing" in detail
    root = _registry(tmp_path, ["sec_edgar_13f", "ecb"])
    left["n"] = 13
    ok, detail = T.thirteen_f(root, db)
    assert not ok and "13 13F row(s) still under sec_edgar" in detail
    left["n"] = 0
    assert T.thirteen_f(root, db) == (True, f"{db}: moved")


def test_this_checkout_is_not_ready_until_the_13f_rename_merges():
    """While this branch's registry still names the 13F entry sec_edgar, T0 must be refused - the ordering
    R1207 asked for, measured on the real file (flip this when the 13F branch has merged)."""
    import yaml
    ids = {s.get("source_id") for s in yaml.safe_load(open(os.path.join(ROOT, "updater", "registry.yaml"),
                                                            encoding="utf-8"))["sources"]}
    if "sec_edgar" in ids:
        ok, detail = T.thirteen_f()
        assert not ok and "not in this checkout" in detail
    assert ("thirteen-f", T.thirteen_f) in T.CHECKS and ("ci-drained", T.ci_drained) in T.CHECKS


def test_it_writes_nothing():
    """No write verb in the tool: it only reads files, runs pytest and gh list, and GETs the edge."""
    src = open(os.path.join(ROOT, "tools", "selfhost", "t0_ready.py"), encoding="utf-8").read()
    for verb in ("open(p, \"w\"", "write_text", ".write(", "os.remove", "shutil", "execute_wrangler",
                 "workflow disable", "workflow enable"):
        assert verb not in src, verb


def test_thirteen_f_refuses_a_registry_that_still_names_sec_edgar_too(tmp_path, fake_move):
    """R1211: the "no sec_edgar entry" half was untested - the only case with sec_edgar lacked sec_edgar_13f,
    so the other half refused it. Both present is still the collision."""
    left, db = fake_move
    ok, detail = T.thirteen_f(_registry(tmp_path, ["sec_edgar", "sec_edgar_13f"]), db)
    assert not ok and "sec_edgar=True" in detail


def test_thirteen_f_never_creates_a_missing_state_db(tmp_path, fake_move):
    """R1211: opened read-write, a missing state.db was created empty and read as "moved" - a false pass."""
    root = _registry(tmp_path, ["sec_edgar_13f"])
    missing = tmp_path / "no" / "state.db"
    missing.parent.mkdir()
    with pytest.raises(Exception):
        T.thirteen_f(root, str(missing))
    assert not missing.exists(), "the check created the file it reads"


WF_STEP = ("      - name: Check the workstation watchdog's heartbeat\n"
           "        if: ${{ always() && vars.GUARD_HEARTBEAT_URL != '' }}\n"
           "        run: python -B tools/guard_heartbeat.py --check --from-url \"${{ vars.GUARD_HEARTBEAT_URL }}\"\n")
WF_WITH = "name: w\non: push\njobs:\n  watch:\n    runs-on: x\n    steps:\n      - run: echo hi\n" + WF_STEP


def _gh_hb(var, watch_state, main_wf=WF_WITH, var_rc=None, fetch_rc=0, main_only=True):
    def run(cmd, **kw):
        if cmd[:3] == ["gh", "variable", "get"]:
            rc = var_rc if var_rc is not None else (0 if var else 1)
            return types.SimpleNamespace(returncode=rc, stdout=(var or "") + "\n", stderr="HTTP 401" if rc else "")
        if cmd[:2] == ["git", "-C"] and "fetch" in cmd:
            assert cmd[-2:] == ["origin", "main"]
            return types.SimpleNamespace(returncode=fetch_rc, stdout="", stderr="offline" if fetch_rc else "")
        if cmd[:2] == ["git", "-C"] and "show" in cmd:
            ref_ok = cmd[-1] == "origin/main:.github/workflows/selfhost-watch.yml" or not main_only
            return types.SimpleNamespace(returncode=0 if (main_wf is not None and ref_ok) else 128,
                                         stdout=main_wf or "", stderr="")
        return _gh({"selfhost-watch": watch_state})(cmd, **kw)
    return run


def test_the_heartbeat_needs_a_reader_before_t0():
    """R1226: T0 switches off the beat's only reader; nothing required the new one."""
    url = "https://edge.example/v1/guard-heartbeat"
    ok, detail = T.heartbeat_reader(_gh_hb(None, "active"), check=lambda u, a: 0)
    assert not ok and "not set, or gh cannot tell" in detail
    ok, detail = T.heartbeat_reader(_gh_hb("", "active", var_rc=0), check=lambda u, a: 0)
    assert not ok and "is empty" in detail
    ok, detail = T.heartbeat_reader(_gh_hb(url, "disabled_manually"), check=lambda u, a: 0)
    assert not ok and "selfhost-watch is disabled_manually" in detail
    assert T.heartbeat_reader(_gh_hb(url, "active"), check=lambda u, a: 1)[0] is False, "a stale beat fails"
    seen = []
    assert T.heartbeat_reader(_gh_hb(url, "active"), check=lambda u, a: seen.append(u) or 0)[0] is True
    assert seen == [url]
    assert ("heartbeat-reader", T.heartbeat_reader) in T.CHECKS
    # R1228: main's workflow must hold the step; a gh failure is reported as one; the age limit is 45 minutes
    ok, detail = T.heartbeat_reader(_gh_hb(url, "active", main_wf="steps: []\n"), check=lambda u, a: 0)
    assert not ok and "does not run guard_heartbeat.py" in detail
    ok, detail = T.heartbeat_reader(_gh_hb(url, "active", main_wf=None), check=lambda u, a: 0)
    assert not ok
    ok, detail = T.heartbeat_reader(_gh_hb(url, "active", var_rc=1), check=lambda u, a: 0)
    assert not ok and "failed (HTTP 401)" in detail, "a gh failure with a URL on stdout is still a failure"
    ages = []
    T.heartbeat_reader(_gh_hb(url, "active"), check=lambda u, a: ages.append(a) or 0)
    assert ages == [45.0]
    # R1230: a failed fetch is "cannot tell", and the step must really run the check
    ok, detail = T.heartbeat_reader(_gh_hb(url, "active", fetch_rc=128), check=lambda u, a: 0)
    assert not ok and "fetch origin main failed" in detail
    base = WF_WITH.replace(WF_STEP, "")
    for bad in (base + "".join("#" + ln + "\n" for ln in WF_STEP.splitlines()),          # commented out
                WF_WITH.replace("if: ${{ always() && vars.GUARD_HEARTBEAT_URL != '' }}", "if: false"),
                WF_WITH.replace("run: python -B tools/", "run: echo python -B tools/"),
                WF_WITH.replace("  watch:\n    runs-on: x\n", "  watch:\n    if: false\n    runs-on: x\n")):
        ok, detail = T.heartbeat_reader(_gh_hb(url, "active", main_wf=bad), check=lambda u, a: 0)
        assert not ok and "does not run guard_heartbeat.py" in detail, bad


def test_the_stats_object_must_be_in_the_served_store():
    class S:
        def __init__(self, raw):
            self.raw = raw

        def get(self, key):
            assert key == "_aqueduct/stats.json"
            return self.raw
    assert T.served_stats(S(None))[0] is False
    assert T.served_stats(S(b'{"as_of": "2026-09'))[0] is False
    ok, detail = T.served_stats(S(b'{"as_of": "2026-09-24"}'))
    assert ok and "2026-09-24" in detail
    assert ("served-stats", T.served_stats) in T.CHECKS
