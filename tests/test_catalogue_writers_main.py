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

What is RUN is the ENTRY: every writer's __main__ block (or top-level script) runs, with main() replaced by
the probe. main() itself runs only for the few writers in the open-mode tests below; the rest of main() is
covered by the structural rules (the lock first, around every main() call; no rebinding of catalog_path;
every plain open marked) - R1203 found "every writer is RUN" overstated.

The writer list is DISCOVERED (every file that opens the catalogue for write through core.catalog_path) and
then compared with EXPECTED_WRITERS: a new writer fails that test until it is added there (one line) and,
if it needs arguments, to ARGS."""
import ast
import os
import runpy
import sys

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
        tree = _repo_walk.parse_code(p)              # utf-8-sig; a file that does not parse fails (R1209)
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


# THE COMMITTED SET (R1199): a floor of ">= 49" stopped protecting the moment a 50th writer arrived, and a
# writer whose open turned read-only - or became a from-import - left discovery silently. Now the set is
# exact: a file that leaves it or joins it is a deliberate edit here.
EXPECTED_WRITERS = {
    "core/apply_title_wave.py", "core/broaden_catalog.py", "core/build_series_metadata.py",
    "tools/_cat_bea.py", "tools/_cat_biotrademerch.py", "tools/_cat_critmin.py", "tools/_cat_efw.py",
    "tools/_cat_nonplastic.py", "tools/_cat_tradefoodcatbyproc.py", "tools/_cat_tradefoodprocbycat.py",
    "tools/_cat_tradeservcatbypartner.py", "tools/apply_license_class.py", "tools/catalog_census_tables.py",
    "tools/catalog_cepii_baci.py", "tools/catalog_complete.py", "tools/catalog_dip_tables.py",
    "tools/catalog_eia_tables.py", "tools/catalog_fdic.py", "tools/catalog_fed_board.py", "tools/catalog_fhfa.py",
    "tools/catalog_ilostat_indicators.py", "tools/catalog_imf_direct.py", "tools/catalog_imts_tables.py",
    "tools/catalog_istat_flows.py", "tools/catalog_mfs_tables.py", "tools/catalog_noaa.py",
    "tools/catalog_penn_world_table.py", "tools/catalog_pip_tables.py", "tools/catalog_pxweb_flowgrain.py",
    "tools/catalog_statcan_tables.py", "tools/catalog_table_grain.py", "tools/catalog_unsdg_flows.py",
    "tools/catalog_usda_tables.py", "tools/catalog_whr.py", "tools/catalog_who_api.py",
    "tools/catalog_worldbank_esg_gaps.py", "tools/enrich_sec_edgar_tickers.py",   # joined 2026-09-24 (plan step 1)
    "tools/rekey_fao_series.py", "tools/title_bea_from_api.py",
    "tools/title_damodaran_margins.py", "tools/title_eia_eba_all.py", "tools/title_eia_nuclear_status.py",
    "tools/title_idb_from_ckan.py", "tools/title_noaa_from_siblings.py", "tools/title_rba_from_csv.py",
    "tools/title_riksbank_fx.py", "tools/title_unctad_span_variants.py", "tools/title_unesco_dem_wb_codes.py",
    "tools/title_unhcr_from_siblings.py", "tools/title_vdem_from_codebook.py",
}


def test_the_discovery_finds_exactly_the_committed_writers():
    names = {r for r, _p, _b in WRITERS}
    assert names == EXPECTED_WRITERS, (f"left the writer set: {sorted(EXPECTED_WRITERS - names)}; "
                                       f"joined it: {sorted(names - EXPECTED_WRITERS)} - a writer that stopped "
                                       "opening for write (or moved to a from-import) is suspect; a new one is added here")
    assert set(ARGS) <= names, f"ARGS names a file that is no longer a writer: {set(ARGS) - names}"


def test_a_writer_with_no_dry_mode_locks_its_whole_entry():
    """Structure, so the lock cannot depend on argv (R1199 M5: the runtime run used no arguments, and a lock
    taken only when no argument was given survived): the __main__ block's first statement is
    `with catalog_path.write_session():` and every call of main() is inside it."""
    bad = []
    for rel, _p, block in WRITERS:
        if block is None or "dry" in ARGS.get(rel, {}):
            continue
        first = block.body[0]
        ok = (isinstance(first, ast.With) and len(first.items) == 1
              and ast.unparse(first.items[0].context_expr) == "catalog_path.write_session()")
        outside = [n for s in block.body[1:] for n in ast.walk(s)
                   if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "main"]
        if not ok or outside:
            bad.append(rel)
    assert not bad, f"these __main__ blocks do not start with the lock around main(): {bad}"


def test_a_script_takes_the_lock_unconditionally_at_top_level():
    """R1199 M1/M2: a lock on a branch never taken, or inside a function never called, passed the order check."""
    bad = []
    for rel, path, block in WRITERS:
        if block is not None:
            continue
        tree = _repo_walk.parse_code(path)
        calls = [s for s in tree.body if isinstance(s, ast.Expr) and isinstance(s.value, ast.Call)
                 and ast.unparse(s.value.func) == "catalog_path.write_session_for_process"]
        if len(calls) != 1:
            bad.append(rel)
    assert not bad, f"these scripts do not call catalog_path.write_session_for_process() at top level: {bad}"


def test_a_writer_never_rebinds_catalog_path():
    """R1203 N4: the structural rules read `catalog_path.write_session...` as text, so a writer that rebinds the
    name (an assignment, a loop or with target, a def, or an attribute set on it) could make the lock a no-op."""
    bad = []
    for rel, path, _b in WRITERS:
        tree = _repo_walk.parse_code(path)
        for n in ast.walk(tree):
            targets = []
            if isinstance(n, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
                targets = n.targets if isinstance(n, ast.Assign) else [n.target]
            elif isinstance(n, (ast.For, ast.AsyncFor)):
                targets = [n.target]
            elif isinstance(n, (ast.With, ast.AsyncWith)):
                targets = [i.optional_vars for i in n.items if i.optional_vars is not None]
            elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and n.name == "catalog_path":
                bad.append(f"{rel}:{n.lineno}")
            elif isinstance(n, ast.NamedExpr):
                targets = [n.target]
            for t in targets:
                for x in ast.walk(t):
                    if (isinstance(x, ast.Name) and x.id == "catalog_path") or \
                            (isinstance(x, ast.Attribute) and ast.unparse(x.value) == "catalog_path"):
                        bad.append(f"{rel}:{n.lineno}")
            if isinstance(n, (ast.Import, ast.ImportFrom)):
                for a in n.names:
                    bound = a.asname or a.name.split(".")[0]
                    if bound == "catalog_path" and not (
                            (isinstance(n, ast.ImportFrom) and n.module == "core" and a.name == "catalog_path")
                            or (isinstance(n, ast.Import) and a.name == "core.catalog_path")):
                        bad.append(f"{rel}:{n.lineno}")
    assert not bad, f"these writers rebind the name catalog_path (the lock could become a no-op): {bad}"


def test_the_rebinding_rule_can_fail(tmp_path, monkeypatch):
    for code in ("from core import catalog_path\ncatalog_path.write_session_for_process = lambda: None\n",
                 "from core import catalog_path\nimport types\ncatalog_path = types.SimpleNamespace()\n",
                 "from mylib import catalog_path\n",
                 "from core import catalog_path\nfor catalog_path in []:\n    pass\n"):
        p = tmp_path / "w.py"
        p.write_text(code + "c = catalog_path.connect(write=True)\n")
        monkeypatch.setattr(sys.modules[__name__], "WRITERS", [("w.py", str(p), None)])
        with pytest.raises(AssertionError, match="rebind"):
            test_a_writer_never_rebinds_catalog_path()


def test_a_dry_mode_writer_reads_its_flags_exactly():
    """R1203: the lock choice in __main__ reads the exact flag ("--apply" in sys.argv), while argparse also
    accepts a prefix ("--app"); the two disagree unless the parser refuses prefixes."""
    bad = []
    for rel in (r for r, a in ARGS.items() if "dry" in a):
        tree = _repo_walk.parse_code(os.path.join(ROOT, rel))
        parsers = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and ast.unparse(n.func).endswith("ArgumentParser")]
        if not parsers or not all(any(k.arg == "allow_abbrev" and isinstance(k.value, ast.Constant)
                                      and k.value.value is False for k in p.keywords) for p in parsers):
            bad.append(rel)
    assert not bad, f"these dry-mode writers' parsers accept flag prefixes: {bad}"


def test_a_writer_opens_nothing_with_plain_sqlite3_unless_it_says_why():
    """R1199 M7: `sqlite3.connect(variable)` in a writer's locked path escaped the plain-open ratchet (which
    only sees a resolver expression as the argument). In a writer file every plain open carries a reason."""
    bad = []
    for rel, path, _b in WRITERS:
        for i, line in enumerate(_repo_walk.read_code(path).splitlines(), 1):
            if "sqlite3.connect(" in line and "plain-open:" not in line and not line.lstrip().startswith("#"):
                bad.append(f"{rel}:{i}")
    assert not bad, f"plain sqlite3 opens in writers without a '# plain-open: <reason>': {bad}"


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


def test_catalog_complete_opens_read_write(t0, monkeypatch, tmp_path):
    # after T0 catalog_complete writes the live build only from the LIVE checkout (R1249) - be that checkout here
    from updater import blob
    from updater import config as ucfg
    live = str(tmp_path / "live")
    monkeypatch.setattr(cp, "LIVE_STORE_ROOT", live)
    monkeypatch.setattr(blob, "_code_root", lambda: live)
    monkeypatch.setattr(ucfg, "ROOT", live)
    monkeypatch.setattr(ucfg, "DATA_ROOT", os.path.join(live, "data", "clean_full"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
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
    # R1203 N4: the REAL lock function must be what the script calls - a name rebound to a no-op survived
    # the order check below, which reads text
    lock_calls = []
    real_lock = cp.write_session_for_process

    def lock_spy(*a, **k):
        lock_calls.append(1)
        return real_lock(*a, **k)
    monkeypatch.setattr(cp, "write_session_for_process", lock_spy)
    monkeypatch.setattr(cp, "connect", probe)
    monkeypatch.setattr(cp, "connect_path", probe)
    monkeypatch.setattr(sys, "argv", [path])
    stopped_early = None
    # R1209 S4: a script that rebinds these modules' functions through an alias (is_cut_over, say) must not
    # leak that into later tests - where it broke four other scripts' runs and hid its own
    snapshot = {m: dict(vars(m)) for m in (cp, cutover)}
    try:
        runpy.run_path(path, run_name="__main__")
    except _Stop:
        pass
    except (FileNotFoundError, OSError) as e:                 # its data store is not in a test checkout
        stopped_early = e
    finally:
        for fn, a, k in at_exit:                              # what interpreter exit would run
            fn(*a, **k)
        changed = []
        for mod, before in snapshot.items():
            for k in set(vars(mod)) - set(before):
                changed.append(f"{mod.__name__}.{k} added")
                delattr(mod, k)
            for k, v in before.items():
                if vars(mod).get(k) is not v:
                    changed.append(f"{mod.__name__}.{k} replaced")
                    setattr(mod, k, v)
    assert not changed, f"{rel}: the script rebound the lock modules - {changed}"
    assert cp._held is None, f"{rel}: its exit handler did not release the lock"
    if stopped_early is None:
        assert seen == [(True, True)], f"{rel}: its catalogue open saw (write, lock held) = {seen}"
        return
    # It read its store before its open, so the run could not reach the open. Then the ORDER is checked:
    # the lock is taken by a top-level statement BEFORE the first top-level statement that opens the
    # catalogue for write (R1194: a script that locked after its open survived every test).
    assert seen == [], f"{rel}: stopped early ({stopped_early!r}) yet reached an open: {seen}"
    assert lock_calls == [1], f"{rel}: stopped early without calling the real write_session_for_process once"
    tree = _repo_walk.parse_code(path)
    lock_at = next((i for i, s in enumerate(tree.body) if "write_session_for_process" in ast.unparse(s)), None)
    open_at = next((i for i, s in enumerate(tree.body) if any(_is_write_open(n) for n in ast.walk(s))), None)
    assert lock_at is not None and open_at is not None and lock_at < open_at, \
        f"{rel}: the lock (statement {lock_at}) must come before the first write open (statement {open_at})"
