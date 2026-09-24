"""EVERY catalogue writer's entry holds the single-writer lock when it writes - measured by RUNNING it (R1194).

R1192 and then R1194: two writer batches were committed with "tests pass" as the evidence while no test
executed a changed line; 9 of 12 mutants in the second batch survived (a lock on the wrong argv slice, a
dry run opening read-write, a lock moved into dead code, a script locking after its open ...). A static
ratchet only matches the shapes it was written for (R1194 finding 3). So this file RUNS each writer's
entry, after a simulated T0, with the parts that need data stubbed out:

  * a file with an `if __name__ == "__main__":` block: the module is imported (not as __main__), then that
    block's own code is executed with `main` replaced by a probe that records whether THIS process holds
    the lock. In a write mode the probe must see the lock; in a dry run it must not.
  * a top-level script (write_session_for_process): run as __main__ with catalog_path.connect replaced by a
    probe that records the lock and stops the script - the lock must already be held at its open.

The writer list is DISCOVERED (every file that opens the catalogue for write through core.catalog_path),
so a new writer is covered without editing this file; only its arguments may need an entry in ARGS."""
import ast
import os
import runpy
import sys
import types

import pytest

from core import catalog_path as cp
from core import cutover

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests"))
import _repo_walk  # noqa: E402

# argv (after the program name) for a WRITE run and, where the tool has one, a DRY run.
ARGS = {
    "tools/catalog_complete.py": {"write": ["some_source"]},
    "tools/catalog_penn_world_table.py": {"write": ["--apply"], "dry": ["--dry-run"]},
    "core/broaden_catalog.py": {"write": [], "dry": ["--dry-run"]},
    "tools/catalog_pxweb_flowgrain.py": {"write": [], "dry": ["--dry-run"]},
    "tools/catalog_who_api.py": {"write": ["who_sdg"], "dry": ["who_sdg", "--dry-run"]},
}


def _is_write_open(n) -> bool:
    if not (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr in ("connect", "connect_path") and ast.unparse(n.func.value).endswith("catalog_path")):
        return False
    w = next((k.value for k in n.keywords if k.arg == "write"), None)
    return w is not None and not (isinstance(w, ast.Constant) and w.value is False)


def _writers():
    out = []
    for rel, p in _repo_walk.code_files((".py",), ROOT):
        if rel == "core/catalog_path.py":
            continue
        try:
            tree = ast.parse(open(p, encoding="utf-8").read())
        except SyntaxError:
            continue
        if any(_is_write_open(n) for n in ast.walk(tree)):
            mains = [n for n in tree.body if isinstance(n, ast.If) and "__main__" in ast.unparse(n.test)]
            out.append((rel, p, mains[0] if mains else None))
    return sorted(out, key=lambda t: t[0])


WRITERS = _writers()


@pytest.fixture
def t0(tmp_path, monkeypatch):
    # a writer's main() may os.environ.setdefault("AQUEDUCT_BACKEND", "r2") before its open (catalog_complete
    # does); monkeypatch restores the variable afterwards, or every later test in the run sees the R2 backend
    # (setenv first: delenv of an ABSENT variable records nothing, so nothing would be restored)
    monkeypatch.setenv("AQUEDUCT_BACKEND", "unset-by-test")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    db = tmp_path / "catalog.db"
    db.write_bytes(b"")
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(db))
    monkeypatch.setattr(cp, "BUILD_PATH", str(db))
    monkeypatch.setattr(cp, "LOCK_PATH", str(tmp_path / "state" / "writer.lock"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path


def _load(rel, path):
    name = "_writer_under_test_" + rel.replace("/", "_").replace(".", "_")
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _run_main_block(rel, path, block, argv, monkeypatch):
    """Execute the file's __main__ block with main() replaced by a lock probe. Returns what the probe saw."""
    m = _load(rel, path)
    seen = []

    def probe(*a, **k):
        seen.append(cp._held is not None)
        return 0
    g = dict(vars(m))
    g["main"] = probe
    monkeypatch.setattr(sys, "argv", [path] + list(argv))
    code = compile(ast.Module(body=block.body, type_ignores=[]), path, "exec")
    try:
        exec(code, g)                                          # noqa: S102 - the file's own __main__ code
    except SystemExit:
        pass
    assert cp._held is None, f"{rel}: the lock is still held after its entry returned"
    return seen


def test_the_discovery_finds_the_writers():
    names = {r for r, _p, _b in WRITERS}
    assert {"core/apply_title_wave.py", "tools/catalog_statcan_tables.py", "tools/_cat_bea.py"} <= names
    assert len(WRITERS) >= 49, f"{len(WRITERS)} writers found - the discovery lost some"
    assert set(ARGS) <= names, f"ARGS names a file that is no longer a writer: {set(ARGS) - names}"


@pytest.mark.parametrize("rel,path,block", [w for w in WRITERS if w[2] is not None], ids=lambda v: v if isinstance(v, str) and "/" in v and not os.path.isabs(v) else "")
def test_a_write_run_holds_the_lock(t0, monkeypatch, rel, path, block):
    seen = _run_main_block(rel, path, block, ARGS.get(rel, {}).get("write", []), monkeypatch)
    assert seen == [True], f"{rel}: its write run called main() with the lock held = {seen}"


@pytest.mark.parametrize("rel", sorted(r for r, a in ARGS.items() if "dry" in a))
def test_a_dry_run_takes_no_lock(t0, monkeypatch, rel):
    _r, path, block = next(w for w in WRITERS if w[0] == rel)
    seen = _run_main_block(rel, path, block, ARGS[rel]["dry"], monkeypatch)
    assert seen == [False], f"{rel}: its dry run called main() with the lock held = {seen}"


class _Stop(Exception):
    pass


# ---- the open MODE inside main(): a dry run opens read-only, a write run read-write -----------------------
def _open_mode(rel, argv, monkeypatch, prepare=None, call=None):
    _r, path, _b = next(w for w in WRITERS if w[0] == rel)
    m = _load(rel, path)
    seen = []

    def spy(*a, **k):
        seen.append(k.get("write"))
        raise _Stop()
    monkeypatch.setattr(cp, "connect", spy)
    monkeypatch.setattr(cp, "connect_path", spy)
    monkeypatch.setattr(sys, "argv", [path] + argv)
    if prepare:
        prepare(m, monkeypatch)
    with pytest.raises(_Stop):
        (call or (lambda mod: mod.main()))(m)
    return seen


def _no_store(m, monkeypatch):
    monkeypatch.setattr(m, "_require_store", lambda: None)


def _penn_inputs(m, monkeypatch):
    monkeypatch.setattr(m, "_country_names", lambda: {})
    monkeypatch.setattr(m, "_store_series", lambda: {})


@pytest.mark.parametrize("rel,argv,want,prepare", [
    ("tools/catalog_who_api.py", ["who_sdg", "--dry-run"], False, None),
    ("tools/catalog_who_api.py", ["who_sdg"], True, None),
    ("tools/catalog_pxweb_flowgrain.py", ["--dry-run"], False, _no_store),
    ("tools/catalog_pxweb_flowgrain.py", [], True, _no_store),
    ("tools/catalog_penn_world_table.py", ["--dry-run"], False, _penn_inputs),
    ("tools/catalog_penn_world_table.py", ["--apply"], True, _penn_inputs),
    ("core/broaden_catalog.py", ["--dry-run"], False, None),
    ("core/broaden_catalog.py", [], True, None),
])
def test_main_opens_in_the_right_mode(t0, monkeypatch, rel, argv, want, prepare):
    assert _open_mode(rel, argv, monkeypatch, prepare) == [want]


def test_catalog_complete_opens_read_write(t0, monkeypatch):
    assert _open_mode("tools/catalog_complete.py", [], monkeypatch, call=lambda m: m.main(["x"])) == [True]


def test_statcan_backup_reads_only_the_build_after_t0(t0, tmp_path):
    _r, path, _b = next(w for w in WRITERS if w[0] == "tools/catalog_statcan_tables.py")
    m = _load("tools/catalog_statcan_tables.py", path)
    import sqlite3
    other = tmp_path / "not_the_build.db"
    c = sqlite3.connect(other)
    c.execute("CREATE TABLE t (x)")
    c.close()
    with pytest.raises(cutover.CutoverRefused):
        m.backup_sqlite(str(other), str(tmp_path / "bak.db"))


@pytest.mark.parametrize("rel,path,block", [w for w in WRITERS if w[2] is None], ids=lambda v: v if isinstance(v, str) and "/" in v and not os.path.isabs(v) else "")
def test_a_script_holds_the_lock_at_its_open(t0, monkeypatch, rel, path, block):
    seen = []

    def probe(*a, **k):
        seen.append((k.get("write"), cp._held is not None))
        raise _Stop()
    import atexit
    at_exit = []
    monkeypatch.setattr(atexit, "register", lambda fn, *a, **k: at_exit.append((fn, a, k)))
    monkeypatch.setattr(cp, "connect", probe)
    monkeypatch.setattr(cp, "connect_path", probe)
    monkeypatch.setattr(sys, "argv", [path])
    stopped_early = None
    try:
        runpy.run_path(path, run_name="__main__")
    except _Stop:
        pass
    except (FileNotFoundError, OSError) as e:                 # its data store is not in a test checkout
        stopped_early = e
    finally:
        for fn, a, k in at_exit:                              # what interpreter exit would run
            fn(*a, **k)
    assert cp._held is None, f"{rel}: its exit handler did not release the lock"
    if stopped_early is None:
        assert seen == [(True, True)], f"{rel}: its catalogue open saw (write, lock held) = {seen}"
        return
    # It read its store before its open, so the run could not reach the open. Then the ORDER is checked:
    # the lock is taken by a top-level statement BEFORE the first top-level statement that opens the
    # catalogue for write (R1194: a script that locked after its open survived every test).
    assert seen == [], f"{rel}: stopped early ({stopped_early!r}) yet reached an open: {seen}"
    tree = ast.parse(open(path, encoding="utf-8").read())
    lock_at = next((i for i, s in enumerate(tree.body) if "write_session_for_process" in ast.unparse(s)), None)
    open_at = next((i for i, s in enumerate(tree.body) if any(_is_write_open(n) for n in ast.walk(s))), None)
    assert lock_at is not None and open_at is not None and lock_at < open_at, \
        f"{rel}: the lock (statement {lock_at}) must come before the first write open (statement {open_at})"
