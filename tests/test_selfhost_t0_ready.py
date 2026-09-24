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


def test_it_writes_nothing():
    """No write verb in the tool: it only reads files, runs pytest and gh list, and GETs the edge."""
    src = open(os.path.join(ROOT, "tools", "selfhost", "t0_ready.py"), encoding="utf-8").read()
    for verb in ("open(p, \"w\"", "write_text", ".write(", "os.remove", "shutil", "execute_wrangler",
                 "workflow disable", "workflow enable"):
        assert verb not in src, verb
