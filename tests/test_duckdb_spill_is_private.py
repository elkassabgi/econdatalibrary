"""Every DuckDB connection must get its OWN spill directory, or the process segfaults.

DuckDB names spill files by block size - `duckdb_temp_storage_DEFAULT-3.tmp`,
`duckdb_temp_storage_S128K-2.tmp` - with no instance id in the name. Two instances pointed at the
same `temp_directory` therefore open each other's files and the process dies with no traceback.
R612 measured it: N = 2, 8 and 16 connections all exit 139.

`tools/mirror_sync.py` was fixed then. Four other tools kept pointing at the shared root
`logs/_duckspill`, so any two of them running together were exactly the measured configuration.
That is the R1061 shape - a fix applied to one call site and not its twins - and this file exists
so the five cannot drift apart again.

It also pins the cleanup. On 2026-09-22, 32 orphaned spill files holding 73.37 GB - 97.8% of
`logs/` - were found left behind by processes that died on 2026-09-09 without removing their temp.

Offline: no DuckDB connection is opened and no query runs.
"""
from __future__ import annotations

import os
import re
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import duck_spill  # noqa: E402

# every tool that sets temp_directory, and the tag each should use
TOOLS = [
    "tools/mirror_sync.py",
    "tools/audit_store_vs_catalog.py",
    "tools/catalog_census_tables.py",
    "tools/catalog_usda_tables.py",
    "tools/derive_usda_tables.py",
]
SETTER = re.compile(r"SET temp_directory='\{([^}]+)\}'")
# The literal name, not duck_spill.ENV_VAR: a renamed variable in the module must fail here.
VAR = "ECONDL_DUCKDB_TMP"


def _src(rel):
    return open(os.path.join(ROOT, rel.replace("/", os.sep)), encoding="utf-8").read()


@pytest.fixture(autouse=True)
def _no_spill_variable(monkeypatch):
    """Every test here starts with ECONDL_DUCKDB_TMP unset. With it set, spill_root() ignores a
    patched SPILL_ROOT, so a test that patches SPILL_ROOT would sweep the machine's folder instead.
    The tests that want the variable set it themselves."""
    monkeypatch.delenv(VAR, raising=False)


@pytest.mark.parametrize("rel", TOOLS)
def test_no_tool_points_temp_directory_at_the_shared_root(rel):
    """The literal shared path is the bug. It must not appear as a temp_directory value."""
    src = _src(rel)
    for expr in SETTER.findall(src):
        assert "_duckspill'" not in expr and '_duckspill"' not in expr, (
            f"{rel} sets temp_directory to the shared root via {expr!r}; two runs sharing it "
            f"exit 139 (R612)")


@pytest.mark.parametrize("rel", TOOLS)
def test_every_temp_directory_expression_resolves_to_a_private_path(rel):
    """Whatever the expression is, it must trace back to a per-process path.

    Pinning the VALUE rather than the spelling: the tool builds the path through the helper
    (`ensure_spill`, or `spill_path` directly), or it mixes in the pid itself (mirror_sync
    predates the helper).
    """
    src = _src(rel)
    exprs = SETTER.findall(src)
    assert exprs, f"{rel} no longer sets temp_directory - update this test or the tool"
    private = (("duck_spill.ensure_spill" in src) or ("duck_spill.spill_path" in src)
               or ("os.getpid()" in src))
    assert private, (
        f"{rel} sets temp_directory {exprs!r} but builds no per-process path; a shared spill "
        f"directory is the R612 crash")


def test_spill_path_is_different_every_call():
    """One process opens several connections; the pid alone is not enough to separate them."""
    a, b = duck_spill.spill_path("t"), duck_spill.spill_path("t")
    assert a != b
    assert str(os.getpid()) in a
    assert "\\" not in a, "DuckDB wants posix slashes in temp_directory"


def test_connection_spill_removes_the_directory_even_on_an_exception():
    seen = {}
    with pytest.raises(RuntimeError):
        with duck_spill.connection_spill("t") as p:
            seen["p"] = p
            assert os.path.isdir(p), "the directory must exist inside the block"
            raise RuntimeError("boom")
    assert not os.path.exists(seen["p"]), "an exception must not leak the spill directory"


def test_sweep_orphans_leaves_young_directories_alone(tmp_path, monkeypatch):
    """A long-running query must not lose its temp. Age is the guard, not a pid check."""
    monkeypatch.setattr(duck_spill, "SPILL_ROOT", str(tmp_path))
    young = tmp_path / "young_1_aaaa"
    young.mkdir()
    (young / "duckdb_temp_storage_DEFAULT-0.tmp").write_bytes(b"x" * 10)
    removed, freed = duck_spill.sweep_orphans(older_than_hours=24)
    assert removed == 0 and freed == 0
    assert young.exists()


def test_sweep_orphans_removes_an_old_directory(tmp_path, monkeypatch):
    """Negative control for the test above - otherwise 'it left things alone' proves nothing."""
    monkeypatch.setattr(duck_spill, "SPILL_ROOT", str(tmp_path))
    old = tmp_path / "old_1_bbbb"
    old.mkdir()
    (old / "duckdb_temp_storage_DEFAULT-0.tmp").write_bytes(b"x" * 10)
    # not epoch 0: Windows rejects it for a directory (WinError 87). A real, old time works.
    old_ts = time.time() - 100 * 3600
    os.utime(old, (old_ts, old_ts))
    removed, freed = duck_spill.sweep_orphans(older_than_hours=24)
    assert removed == 1 and freed >= 10
    assert not old.exists()


def test_ensure_spill_creates_the_directory_and_returns_a_private_path(tmp_path, monkeypatch):
    monkeypatch.setattr(duck_spill, "SPILL_ROOT", str(tmp_path))
    monkeypatch.setattr(duck_spill, "_SWEPT", False)
    p = duck_spill.ensure_spill("t")
    assert os.path.isdir(p), "ensure_spill must create the directory, not just name it"
    assert str(os.getpid()) in p


def test_ensure_spill_reaps_what_a_sigkill_left_behind(tmp_path, monkeypatch):
    """atexit does not run on SIGKILL, and SIGKILL is how 83.73 GB accumulated. The sweep is the
    only thing that reaches those."""
    monkeypatch.setattr(duck_spill, "SPILL_ROOT", str(tmp_path))
    monkeypatch.setattr(duck_spill, "_SWEPT", False)
    orphan = tmp_path / "killed_999_dead"
    orphan.mkdir()
    (orphan / "duckdb_temp_storage_DEFAULT-0.tmp").write_bytes(b"x" * 64)
    old = time.time() - 100 * 3600
    os.utime(orphan, (old, old))
    duck_spill.ensure_spill("t")
    assert not orphan.exists(), "an old orphan must be reaped on the first ensure_spill of a process"


def test_ensure_spill_sweeps_only_once_per_process(tmp_path, monkeypatch):
    """A sweep per connection would rescan the root on every call of a long run."""
    monkeypatch.setattr(duck_spill, "SPILL_ROOT", str(tmp_path))
    monkeypatch.setattr(duck_spill, "_SWEPT", False)
    calls = []
    monkeypatch.setattr(duck_spill, "sweep_orphans", lambda *a, **k: calls.append(1) or (0, 0))
    duck_spill.ensure_spill("t")
    duck_spill.ensure_spill("t")
    duck_spill.ensure_spill("t")
    assert len(calls) == 1, f"swept {len(calls)} times, expected once per process"


def test_ensure_spill_still_returns_a_path_when_the_sweep_fails(tmp_path, monkeypatch):
    """Reaping is best-effort. A failure to tidy must never stop the real work."""
    monkeypatch.setattr(duck_spill, "SPILL_ROOT", str(tmp_path))
    monkeypatch.setattr(duck_spill, "_SWEPT", False)

    def boom(*a, **k):
        raise OSError("disk said no")

    monkeypatch.setattr(duck_spill, "sweep_orphans", boom)
    p = duck_spill.ensure_spill("t")
    assert os.path.isdir(p)


def test_sweep_is_skippable(tmp_path, monkeypatch):
    """Negative control for the sweep tests - if sweep=False also swept, they would prove nothing."""
    monkeypatch.setattr(duck_spill, "SPILL_ROOT", str(tmp_path))
    monkeypatch.setattr(duck_spill, "_SWEPT", False)
    calls = []
    monkeypatch.setattr(duck_spill, "sweep_orphans", lambda *a, **k: calls.append(1) or (0, 0))
    duck_spill.ensure_spill("t", sweep=False)
    assert calls == []


# --------------------------------------------------------------------------------------------- #
# ECONDL_DUCKDB_TMP (T0 plan B30, review condition C-1). The variable's folder is shared: insee's
# persistent staging, derive_csv's pid folders and the bounded-merge spill folders live there too.
# So the spill goes to its OWN subfolder, and sweep_orphans must never list the variable's folder.
# --------------------------------------------------------------------------------------------- #

# stand-ins for what the other readers of the variable keep beside the spill:
# _melodi_state.staging_dir, derive_csv._duck_spill_dir (pid<N>), merge._bounded_spill_dir
SIBLINGS = ("insee_melodi_staging", "pid4242", "merge_pid4242_deadbeef")


def _norm(p):
    return os.path.normcase(os.path.normpath(str(p)))


def _make_old(path, hours=100):
    # not epoch 0: Windows rejects it for a directory (WinError 87). A real, old time works.
    ts = time.time() - hours * 3600
    os.utime(path, (ts, ts))


def _plant_siblings(base):
    """Each sibling is OLDER than the sweep guard, so only the root rule can save it, not the age."""
    for name in SIBLINGS:
        d = base / name
        d.mkdir(parents=True)
        (d / "keep.bin").write_bytes(b"k" * 32)
        _make_old(d)


def _assert_siblings_intact(base):
    for name in SIBLINGS:
        assert (base / name / "keep.bin").is_file(), (
            f"{name} beside the spill was deleted; the spill root must be <ECONDL_DUCKDB_TMP>/_duckspill, "
            f"never the variable's folder itself (C-1)")


@pytest.fixture
def env_base(tmp_path, monkeypatch):
    """ECONDL_DUCKDB_TMP -> tmp_path/duckdb_tmp. SPILL_ROOT points at a second tmp folder, so a regression
    that ignores the variable writes and sweeps THERE - never in the checkout's logs/."""
    base = tmp_path / "duckdb_tmp"
    base.mkdir()
    monkeypatch.setattr(duck_spill, "SPILL_ROOT", str(tmp_path / "default_root"))
    monkeypatch.setenv(VAR, str(base))
    return base


def test_default_root_is_unchanged_when_the_variable_is_unset(monkeypatch):
    """No variable -> <repo>/logs/_duckspill, as before. An empty value counts as unset (the rule of the
    other three readers: derive_csv, merge, _melodi_state)."""
    default = os.path.join(ROOT, "logs", "_duckspill")
    assert _norm(duck_spill.SPILL_ROOT) == _norm(default)
    assert _norm(duck_spill.spill_root()) == _norm(default)
    assert _norm(os.path.dirname(duck_spill.spill_path("t"))) == _norm(default)
    monkeypatch.setenv(VAR, "")
    assert _norm(duck_spill.spill_root()) == _norm(default)
    assert _norm(os.path.dirname(duck_spill.spill_path("t"))) == _norm(default)


def test_variable_set_gives_its_own_subfolder_never_the_folder_itself(env_base):
    root = duck_spill.spill_root()
    assert _norm(root) == _norm(env_base / "_duckspill")
    assert _norm(root) != _norm(env_base), "the variable's folder itself is the C-1 defect"
    p = duck_spill.spill_path("t")
    assert _norm(os.path.dirname(p)) == _norm(env_base / "_duckspill")
    assert "\\" not in p, "DuckDB wants posix slashes in temp_directory"
    assert str(os.getpid()) in os.path.basename(p)


def test_variable_is_read_at_use_time_not_at_import(tmp_path, monkeypatch):
    """The module was imported before this test set anything. Each call must see the current value."""
    a, b = tmp_path / "a", tmp_path / "b"
    monkeypatch.setenv(VAR, str(a))
    assert _norm(duck_spill.spill_root()) == _norm(a / "_duckspill")
    monkeypatch.setenv(VAR, str(b))
    assert _norm(duck_spill.spill_root()) == _norm(b / "_duckspill")
    monkeypatch.delenv(VAR)
    assert _norm(duck_spill.spill_root()) == _norm(duck_spill.SPILL_ROOT)


def test_sweep_orphans_leaves_old_siblings_of_the_subfolder_in_place(env_base, tmp_path):
    _plant_siblings(env_base)
    orphan = env_base / "_duckspill" / "killed_999_dead"
    orphan.mkdir(parents=True)
    (orphan / "duckdb_temp_storage_DEFAULT-0.tmp").write_bytes(b"x" * 64)
    _make_old(orphan)
    removed, freed = duck_spill.sweep_orphans(older_than_hours=24)
    # positive control: the sweep DID run in the subfolder, so "the siblings survived" is not vacuous
    assert (removed, freed) == (1, 64)
    assert not orphan.exists()
    assert (env_base / "_duckspill").is_dir()
    _assert_siblings_intact(env_base)
    assert not (tmp_path / "default_root").exists()


def test_ensure_spill_with_the_variable_reaps_only_inside_the_subfolder(env_base, tmp_path, monkeypatch):
    """ensure_spill sweeps on its first call in a process (audit_store_vs_catalog, catalog_census_tables,
    catalog_usda_tables, derive_usda_tables). That sweep is the C-1 risk."""
    _plant_siblings(env_base)
    orphan = env_base / "_duckspill" / "killed_999_dead"
    orphan.mkdir(parents=True)
    _make_old(orphan)
    monkeypatch.setattr(duck_spill, "_SWEPT", False)
    p = duck_spill.ensure_spill("t")
    assert os.path.isdir(p)
    assert _norm(os.path.dirname(p)) == _norm(env_base / "_duckspill")
    assert not orphan.exists(), "positive control: the first ensure_spill must reap the old orphan"
    _assert_siblings_intact(env_base)
    assert not (tmp_path / "default_root").exists()


def test_connection_spill_with_the_variable_creates_and_removes_only_its_own_folder(env_base, tmp_path):
    _plant_siblings(env_base)
    live = env_base / "_duckspill" / "other_run_1_aaaa"          # a sibling run's spill, in use now
    live.mkdir(parents=True)
    (live / "duckdb_temp_storage_DEFAULT-0.tmp").write_bytes(b"x" * 8)
    base_before = sorted(os.listdir(env_base))
    with duck_spill.connection_spill("t") as p:
        assert _norm(os.path.dirname(p)) == _norm(env_base / "_duckspill")
        assert os.path.isdir(p)
        assert sorted(os.listdir(env_base / "_duckspill")) == sorted(["other_run_1_aaaa", os.path.basename(p)])
        with open(os.path.join(p, "duckdb_temp_storage_DEFAULT-0.tmp"), "wb") as fh:
            fh.write(b"y" * 8)
    assert not os.path.exists(p)
    assert os.listdir(env_base / "_duckspill") == ["other_run_1_aaaa"]
    assert (live / "duckdb_temp_storage_DEFAULT-0.tmp").is_file()
    assert sorted(os.listdir(env_base)) == base_before
    _assert_siblings_intact(env_base)
    assert not (tmp_path / "default_root").exists()


def test_connection_spill_in_an_empty_variable_folder_adds_only_the_subfolder(env_base, tmp_path):
    with duck_spill.connection_spill("t") as p:
        assert _norm(os.path.dirname(p)) == _norm(env_base / "_duckspill")
    assert not os.path.exists(p)
    assert os.listdir(env_base) == ["_duckspill"]
    assert os.listdir(env_base / "_duckspill") == []
    assert not (tmp_path / "default_root").exists()
