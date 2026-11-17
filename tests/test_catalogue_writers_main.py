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
    "tools/clean_catalogue_titles.py": {"write": ["--apply"], "dry": []},
    "tools/statcan_new_cubes.py": {"write": ["--apply"], "dry": []},
    "tools/fix_statcan_frequency.py": {"write": ["--apply"], "dry": []},
    "tools/sync_source_rows_d1_to_local.py": {"write": ["--source", "zz", "--apply"], "dry": ["--source", "zz"]},
    "tools/flowgrain_insee_melodi.py": {"write": ["--catalog"], "dry": ["--dry-run"]},
    "tools/flowgrain_ons_uk.py": {"write": ["--catalog"], "dry": ["--dry-run"]},
    "tools/delist_timeless_tables.py": {"write": ["--apply"], "dry": []},
    "tools/apply_series_names.py": {"write": ["--apply"], "dry": ["--measure"]},
    "tools/refresh_flowgrain_dates.py": {"write": ["--source", "zz", "--apply"], "dry": ["--source", "zz", "--dry-run"]},
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
    "tools/clean_catalogue_titles.py",
    "tools/catalog_eia_tables.py", "tools/catalog_fdic.py", "tools/catalog_fed_board.py", "tools/catalog_fhfa.py",
    "tools/catalog_ilostat_indicators.py", "tools/catalog_imf_direct.py", "tools/catalog_imts_tables.py",
    "tools/catalog_istat_flows.py", "tools/catalog_mfs_tables.py", "tools/catalog_noaa.py",
    "tools/catalog_penn_world_table.py", "tools/catalog_pip_tables.py", "tools/catalog_pxweb_flowgrain.py",
    "tools/catalog_statcan_tables.py", "tools/catalog_table_grain.py", "tools/catalog_unsdg_flows.py",
    "tools/catalog_usda_tables.py", "tools/catalog_whr.py", "tools/catalog_who_api.py",
    "tools/catalog_worldbank_esg_gaps.py", "tools/enrich_sec_edgar_tickers.py",   # joined 2026-09-24 (plan step 1)
    "tools/statcan_new_cubes.py",                                                  # joined 2026-10-17
    "tools/fix_statcan_frequency.py",                                              # joined 2026-10-18 (R1615)
    "tools/rekey_fao_series.py", "tools/title_bea_from_api.py",
    "tools/title_damodaran_margins.py", "tools/title_eia_eba_all.py", "tools/title_eia_nuclear_status.py",
    "tools/title_idb_from_ckan.py", "tools/title_noaa_from_siblings.py", "tools/title_rba_from_csv.py",
    "tools/title_riksbank_fx.py", "tools/title_unctad_span_variants.py", "tools/title_unesco_dem_wb_codes.py",
    "tools/title_unhcr_from_siblings.py", "tools/title_vdem_from_codebook.py",
    "tools/refresh_sec_edgar.py",                                                  # joined 2026-11-16 (t0_ready)
    "tools/sync_source_rows_d1_to_local.py",                                       # joined 2026-11-17 (t0_ready)
    "core/catalog.py",                                       # joined 2026-11-17 (t0_ready): a LIBRARY writer, below
    "tools/flowgrain_insee_melodi.py", "tools/flowgrain_ons_uk.py",
    "tools/delist_timeless_tables.py",                                             # joined 2026-11-17 (t0_ready)
    "core/backfill_coverage.py", "pipeline/title_bea.py", "tools/apply_series_names.py",
    "tools/refresh_flowgrain_dates.py",                                            # joined 2026-11-17 (t0_ready)
}

# Writers whose entry must NOT hold the lock for the whole run, each with the premise that makes it safe pinned
# by test_the_lock_inside_main_premise below. tools/refresh_sec_edgar.py: its daily run spends most of its time
# fetching SEC, and writer_lock() refuses at once rather than waiting - a lock around main() would refuse the
# updater for the whole fetch. So it WAITS for the lock and holds it only around its writes (_waiting_writer_lock
# in _refresh_local); after T0 its other write roads (--respan, and the pre-T0 R2 path) are refused before any
# open, and connect_path(write=True) refuses any write without the lock.
LOCK_INSIDE_MAIN = {"tools/refresh_sec_edgar.py"}


# LIBRARY writers: a module (no __main__ block, no script body) whose FUNCTION opens the catalogue for write for
# whoever imports it. It cannot take the lock itself (a library holding the machine-wide lock until the process
# exits would block the updater; a lock inside connect() would be let go before the caller writes).
# core/catalog.py's importers are RETIRED legacy scripts (UPDATER_BUILD_PLAN D-1; nothing schedules them) whose
# writes would damage the live build after T0 - upsert_source drops terms_url, rebuild_fts rewrites the WHOLE
# series_fts (review AR-281 C281-3). So each importer REFUSES AT ENTRY after T0, before any read and before any
# lock, rather than becoming a lock-holding writer: the first statement of its __main__ block, or, for a
# top-level script, a top-level statement before anything but imports, path set-up and constants.
# DISCOVERY IS STATIC (C281-1): it sees `from core import catalog`, `import core.catalog`, `from core.catalog
# import ...`, a relative `from . import catalog` in core/ and a plain `import catalog` anywhere - not an
# importlib call or a transitive caller. Those are the early warning. THE GUARD is connect_path refusing an
# unlocked write to the build after T0, which test_the_library_writer_premise RUNS.
LIBRARY_WRITERS = {
    "core/catalog.py": {"core/apply_stage0b.py", "core/build_registry.py", "jobs/ingest_eurostat.py",
                        "jobs/ingest_sec_edgar.py", "jobs/run_connector.py"},
}


def _imports_core_catalog(rel, tree) -> bool:
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core" and not n.level \
                and any(a.name == "catalog" for a in n.names):
            return True
        if isinstance(n, ast.ImportFrom) and n.module == "core.catalog":
            return True
        if isinstance(n, ast.Import) and any(a.name in ("core.catalog", "catalog") for a in n.names):
            return True                                        # plain `import catalog` (core/ on the path)
        if isinstance(n, ast.ImportFrom) and n.module == "catalog" and not n.level:
            return True
        if rel.startswith("core/") and isinstance(n, ast.ImportFrom) and n.level == 1 \
                and (n.module == "catalog" or (n.module is None and any(a.name == "catalog" for a in n.names))):
            return True                                        # `from . import catalog` / `from .catalog import`
    return False


def _library_importers():
    out = set()
    for rel, p in _repo_walk.code_files((".py",), ROOT):
        if rel in LIBRARY_WRITERS:
            continue
        if _imports_core_catalog(rel, _repo_walk.parse_code(p)):
            out.add(rel)
    return out


def _is_refusal(stmt) -> bool:
    return (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)
            and ast.unparse(stmt.value.func) == "cutover.refuse_if_cut_over")


def _quiet(stmt) -> bool:
    """A top-level statement that reads, writes or opens nothing: an import, the docstring, sys.path set-up, or an
    assignment whose only calls are os.path.*, re.compile or len (ROOT = os.path.dirname(...), a regex constant,
    OUT = sys.argv[1] if len(sys.argv) > 1 else ...)."""
    if isinstance(stmt, (ast.Import, ast.ImportFrom)):
        return True
    if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant):
        return True
    calls = [ast.unparse(c.func) for c in ast.walk(stmt) if isinstance(c, ast.Call)]
    if isinstance(stmt, ast.Expr) and calls and calls[0] == "sys.path.insert":
        return all(c == "sys.path.insert" or c.startswith("os.path.") for c in calls)
    return isinstance(stmt, ast.Assign) and all(c.startswith("os.path.") or c in ("re.compile", "len") for c in calls)


def _calls_are_pure(nodes) -> bool:
    return all(ast.unparse(c.func).startswith("os.path.") or ast.unparse(c.func) in ("re.compile", "len")
               for x in nodes if x is not None for c in ast.walk(x) if isinstance(c, ast.Call))


def _quiet_def(stmt) -> bool:
    """A def or class whose load-time parts do nothing: no call (other than the pure ones) in its decorators,
    default arguments or annotations; for a class also its bases and keywords, and every body statement quiet
    (or itself a quiet def, or `pass`)."""
    if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
        a = stmt.args
        anns = [x.annotation for x in a.posonlyargs + a.args + a.kwonlyargs + [a.vararg, a.kwarg] if x is not None]
        return _calls_are_pure(stmt.decorator_list + a.defaults + a.kw_defaults + anns + [stmt.returns])
    if isinstance(stmt, ast.ClassDef):
        return (_calls_are_pure(stmt.decorator_list + stmt.bases + [k.value for k in stmt.keywords])
                and all(isinstance(s, ast.Pass) or _quiet(s) or _quiet_def(s) for s in stmt.body))
    return False


def _entry_refuses_after_t0(tree) -> bool:
    mains = [n for n in tree.body if isinstance(n, ast.If) and "__main__" in ast.unparse(n.test)]
    if mains:
        # the refusal is the block's first statement, AND nothing above it does anything when the module is
        # loaded (review AR-281 C281-4: a module-level makedirs, logging file or touch ran before the refusal)
        # a def or class counts too: its decorators, default arguments and annotations, and a class's bases and
        # body, run when the module loads (review AR-284 C284-1, P1-P6)
        above = [s for s in tree.body if s is not mains[0]]
        return bool(mains[0].body) and _is_refusal(mains[0].body[0]) and all(_quiet(s) or _quiet_def(s) for s in above)
    for s in tree.body:
        if _is_refusal(s):
            return True
        if not _quiet(s):
            return False
    return False


def test_every_library_writer_s_importer_refuses_at_entry_after_t0():
    want = set().union(*LIBRARY_WRITERS.values())
    found = _library_importers()
    assert found == want, (f"importers of a library writer changed - joined: {sorted(found - want)}; left: "
                           f"{sorted(want - found)}. A new importer must refuse at entry after T0 (or be reviewed "
                           f"as a writer of the live build), then join here")
    bad = [rel for rel in sorted(found) if not _entry_refuses_after_t0(_repo_walk.parse_code(os.path.join(ROOT, rel)))]
    assert not bad, f"these importers of a library writer do not refuse at their entry after T0: {bad}"


def test_the_library_importer_rule_can_fail():
    R = 'cutover.refuse_if_cut_over("x")\n'
    for code in ('if __name__ == "__main__":\n    main()\n',
                 'if __name__ == "__main__":\n    main()\n    ' + R,
                 'if __name__ == "__main__":\n    with catalog_path.write_session():\n        main()\n',
                 'os.makedirs(OUT)\n' + R,
                 'ct = json.load(open(p))\n' + R,
                 'db = catalog.connect()\n' + R,
                 'os.makedirs(OUT)\nif __name__ == "__main__":\n    ' + R + '    main()\n',
                 'logging.basicConfig(filename="x.log")\nif __name__ == "__main__":\n    ' + R + '    main()\n',
                 'open("x", "a").close()\nif __name__ == "__main__":\n    ' + R + '    main()\n',
                 'X = json.load(open("x"))\nif __name__ == "__main__":\n    ' + R + '    main()\n',
                 '@(lambda f: (open("x", "a"), f)[1])\ndef g():\n    pass\nif __name__ == "__main__":\n    ' + R + '    main()\n',
                 'def g(x=os.open("x", 1)):\n    pass\nif __name__ == "__main__":\n    ' + R + '    main()\n',
                 'class C:\n    os.makedirs("x")\nif __name__ == "__main__":\n    ' + R + '    main()\n',
                 'class C(base(open("x", "w"))):\n    pass\nif __name__ == "__main__":\n    ' + R + '    main()\n',
                 'import os\nx = 1\n'):
        assert not _entry_refuses_after_t0(ast.parse(code)), code
    for code in ('if __name__ == "__main__":\n    ' + R + '    main()\n',
                 'import re\nRX = re.compile("a")\ndef f():\n    open("x", "w")\n'
                 'if __name__ == "__main__":\n    ' + R + '    main()\n',
                 '"""doc"""\nimport os, sys\nROOT = os.path.dirname(os.path.abspath(__file__))\n'
                 'sys.path.insert(0, ROOT)\nfrom core import catalog\n' + R + 'db = catalog.connect()\n'):
        assert _entry_refuses_after_t0(ast.parse(code)), code
    assert _imports_core_catalog("jobs/x.py", ast.parse("from core import catalog, storage"))
    assert _imports_core_catalog("core/x.py", ast.parse("import catalog as cat"))
    assert _imports_core_catalog("tools/x.py", ast.parse("import catalog"))
    assert _imports_core_catalog("core/x.py", ast.parse("from . import catalog"))
    assert _imports_core_catalog("core/x.py", ast.parse("from .catalog import connect"))
    assert not _imports_core_catalog("jobs/x.py", ast.parse("from core import catalog_path"))
    assert not _imports_core_catalog("tools/x.py", ast.parse("from catalog_tools import catalog"))


def _importer_entry(rel):
    path = os.path.join(ROOT, rel)
    tree = _repo_walk.parse_code(path)
    mains = [n for n in tree.body if isinstance(n, ast.If) and "__main__" in ast.unparse(n.test)]
    return path, (mains[0] if mains else None)


@pytest.mark.parametrize("rel", sorted(set().union(*LIBRARY_WRITERS.values())))
def test_a_library_importer_refuses_after_t0_before_anything_and_runs_before(t0, monkeypatch, rel, tmp_path):
    """RUN: after T0 the entry raises CutoverRefused before its work starts (main()/run() never called, no
    folder made, no file read, no catalogue opened); before T0 it reaches its work as before."""
    import builtins
    from core import catalog
    path, block = _importer_entry(rel)
    monkeypatch.syspath_prepend(os.path.join(ROOT, "core"))        # core/ scripts import `catalog` by its bare name
    called = []

    def trap(name):
        def f(*a, **k):
            called.append(name)
            raise _Stop()
        return f
    monkeypatch.setattr(catalog, "connect", trap("catalog.connect"))
    monkeypatch.setattr(cp, "connect_path", trap("connect_path"))
    monkeypatch.setattr(sys, "argv", [path])
    for before_t0 in (False, True):
        if before_t0:
            (t0 / "CUTOVER").unlink()
        called.clear()
        if block is not None:
            # C281-4: loading the module (everything above __main__) must make no folder and write no file
            # C284-1: also os.open with a creating or writing flag and io.open (pathlib's touch/write_text, and a
            # module the importer imports - P9)
            import io
            real_open, real_makedirs, real_mkdir = builtins.open, os.makedirs, os.mkdir
            real_os_open, real_io_open = os.open, io.open
            writing = os.O_CREAT | os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_TRUNC

            def opener(real):
                return lambda f, mode="r", *a, **k: trap("open for write at load")() \
                    if isinstance(mode, str) and any(c in mode for c in "wax+") else real(f, mode, *a, **k)
            monkeypatch.setattr(os, "makedirs", trap("os.makedirs at load"))
            monkeypatch.setattr(os, "mkdir", trap("os.mkdir at load"))
            monkeypatch.setattr(os, "open", lambda f, flags, *a, **k: trap("os.open for write at load")()
                                if flags & writing else real_os_open(f, flags, *a, **k))
            monkeypatch.setattr(builtins, "open", opener(real_open))
            monkeypatch.setattr(io, "open", opener(real_io_open))
            try:
                m = _load(rel, path)
            finally:
                monkeypatch.setattr(os, "makedirs", real_makedirs)
                monkeypatch.setattr(os, "mkdir", real_mkdir)
                monkeypatch.setattr(os, "open", real_os_open)
                monkeypatch.setattr(builtins, "open", real_open)
                monkeypatch.setattr(io, "open", real_io_open)
            g = dict(vars(m))
            g.update(main=trap("main"), run=trap("run"))
            code = compile(ast.Module(body=block.body, type_ignores=[]), path, "exec")
            run_it = lambda: exec(code, g)                         # noqa: E731,S102 - the file's own __main__ code
        else:
            real_open, real_makedirs = builtins.open, os.makedirs
            monkeypatch.setattr(os, "makedirs", trap("os.makedirs"))
            monkeypatch.setattr(builtins, "open", lambda f, *a, **k: real_open(f, *a, **k) if str(f).endswith(".py")
                                else trap("open")())
            run_it = lambda: runpy.run_path(path, run_name="__main__")  # noqa: E731
        try:
            if not before_t0:
                with pytest.raises(cutover.CutoverRefused, match="retired legacy"):
                    run_it()
                assert called == [], f"{rel}: after T0 it reached {called} before refusing"
            else:
                with pytest.raises(_Stop):
                    run_it()
                assert called, f"{rel}: before T0 it never reached its work"
        finally:
            if block is None:
                monkeypatch.setattr(os, "makedirs", real_makedirs)
                monkeypatch.setattr(builtins, "open", real_open)
    assert cp._held is None


def test_the_library_writer_premise(t0, monkeypatch):
    """core.catalog.connect() after T0: refused without the lock, read-write with it; never creates a missing
    file; before T0 its default is the checkout's data/ catalogue; 600 s, as before. The writer ratchets of
    tests/test_catalogue_writers_lock.py skip exactly these files."""
    assert set(LIBRARY_WRITERS) == {"core/catalog.py"}, "a new library writer needs its own premise test"
    assert set(LIBRARY_WRITERS) <= {r for r, _p, _b in WRITERS} and all(b is None for r, _p, b in WRITERS
                                                                          if r in LIBRARY_WRITERS)
    import test_catalogue_writers_lock as L
    assert L.LIBRARY_WRITERS == set(LIBRARY_WRITERS), "the two ratchet files exempt the same library writers"
    import sqlite3
    from core import catalog
    db = t0 / "catalog.db"                      # the fixture's build (BUILD_PATH), an empty file
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        catalog.connect(str(db))
    with cp.write_session():
        con = catalog.connect(str(db))
        catalog.init(con)
        catalog.upsert_source(con, "zz", "ZZ", "cc0", "attr")
        assert con.execute("SELECT source_id FROM source").fetchall()[0]["source_id"] == "zz"
        con.close()
    missing = t0 / "data" / "catalog.db"
    missing.parent.mkdir()
    (t0 / "CUTOVER").unlink()
    with pytest.raises(FileNotFoundError):
        catalog.connect(str(missing))
    assert not missing.exists()
    seen = []
    monkeypatch.setattr(cp, "connect_path", lambda path, **kw: seen.append((path, kw)) or sqlite3.connect(":memory:"))
    catalog.connect().close()
    assert seen == [(cp.under(catalog._ROOT), {"write": True, "timeout": 600})]


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
        if block is None or "dry" in ARGS.get(rel, {}) or rel in LOCK_INSIDE_MAIN:
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
        if block is not None or rel in LIBRARY_WRITERS:
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


@pytest.mark.parametrize("rel,path,block", [w for w in WRITERS if w[2] is not None and w[0] not in LOCK_INSIDE_MAIN],
                         ids=lambda v: v if isinstance(v, str) and "/" in v and not os.path.isabs(v) else "")
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


@pytest.mark.parametrize("rel,path,block", [w for w in WRITERS if w[2] is None and w[0] not in LIBRARY_WRITERS], ids=lambda v: v if isinstance(v, str) and "/" in v and not os.path.isabs(v) else "")
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

def test_the_lock_inside_main_premise(t0, monkeypatch):
    """LOCK_INSIDE_MAIN is exactly one file, and its premise holds when RUN after T0: a catalogue write
    without the lock is refused (the open itself refuses), and the daily refresh writes under the lock it
    waited for (tests/test_refresh_sec_edgar_selfhost.py::test_after_t0_a_refresh_commits_everything_under_the_lock
    runs that path end to end)."""
    assert LOCK_INSIDE_MAIN == {"tools/refresh_sec_edgar.py"}, "a new exemption needs its own premise test"
    assert LOCK_INSIDE_MAIN <= {r for r, _p, _b in WRITERS}
    _r, path, _b = next(w for w in WRITERS if w[0] == "tools/refresh_sec_edgar.py")
    R = _load("tools/refresh_sec_edgar.py", path)
    root = t0 / "live"
    (root / "data").mkdir(parents=True)
    db = root / "data" / "catalog.db"
    monkeypatch.setattr(cp, "BUILD_PATH", str(db))
    db.write_bytes(b"")
    monkeypatch.setattr(R, "ROOT", str(root))
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        R.update_catalog([("ACME", "2020-01-01", "2026-06-30", "Acme", 1)], apply_d1=False)
    src = _repo_walk.read_code(path)
    assert "with _waiting_writer_lock():" in src, "the daily refresh no longer waits for the lock around its writes"
    # the after-T0 refusal of --d1 / --audit / --respan is RUN (review C269-1): tests/test_sec_edgar_d1_chokepoint.py
    # ::test_after_t0_main_refuses_the_d1_audit_and_respan_roads_before_any_open
