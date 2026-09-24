"""Catalogue WRITERS after T0: every read-write open holds the single-writer lock (plan step 1).

Review R1192: the three core writers moved in 2d49d8a40 were never RUN by any test - seven mutants in them
(a plain read-write sqlite3.connect of the resolver's path; write_session removed from __main__) passed all
78 cited tests. So:
  - the three core writers are run after a simulated T0: without the lock their write is refused, with it
    it lands and every read-write open held the lock; a dry run opens read-only and takes no lock;
  - a static ratchet over the repo: a file that opens the catalogue for WRITE through the resolver also
    takes the lock (write_session / writer_lock / write_session_for_process) - the resolver refuses such a
    write after T0 without it, so a missing lock is a writer that stops working at T0;
  - a static ratchet: no plain sqlite3.connect of a path the resolver hands out (it bypasses both the
    read-only mode and the lock check)."""
import ast
import os
import re
import runpy
import sqlite3
import sys
import types

import pytest

from core import catalog_path as cp
from core import cutover

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests"))
import _repo_walk  # noqa: E402

SCHEMA = """
CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, url TEXT, reservable INTEGER, commercial_ok INTEGER,
  attribution_required INTEGER, no_modify INTEGER);
CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT);
CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
  geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, last_updated TEXT, metadata TEXT);
CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
INSERT INTO license (license_id, reservable) VALUES ('L1', 1);
INSERT INTO source VALUES ('revsrc', 'Rev Source', 'https://example.org', 'L1');
INSERT INTO series (series_id, source_id, title, metadata) VALUES ('revsrc:A', 'revsrc', 'old title', '{}');
INSERT INTO series_fts VALUES ('revsrc:A', 'old title', NULL);
"""


@pytest.fixture
def t0(tmp_path, monkeypatch):
    """A catalogue that is BOTH the checkout's and the build (so either resolution finds it), the flag SET,
    and every sqlite3.connect recorded as (mode, lock held)."""
    db = tmp_path / "catalog.db"
    c = sqlite3.connect(db)
    c.executescript(SCHEMA)
    c.close()
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(db))
    monkeypatch.setattr(cp, "BUILD_PATH", str(db))
    monkeypatch.setattr(cp, "LOCK_PATH", str(tmp_path / "state" / "writer.lock"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    opens = []
    real = sqlite3.connect

    def spy(database, *a, **kw):
        s = str(database)
        opens.append(("ro" if "mode=ro" in s else "rw" if "mode=rw" in s else "PLAIN", cp._held is not None))
        return real(database, *a, **kw)
    monkeypatch.setattr(sqlite3, "connect", spy)
    return types.SimpleNamespace(db=str(db), opens=opens, tmp=tmp_path, real=real)


def _read(env, sql):
    # READ-ONLY: after the simulated T0 core.catalog_path's audit hook refuses a plain read-write open of the
    # build without the lock (R1209) - and a check of a result has no reason to open it for writing
    import pathlib
    c = env.real(pathlib.Path(env.db).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        return c.execute(sql).fetchone()[0]
    finally:
        c.close()


def test_apply_title_wave_needs_the_lock_after_t0(t0):
    from core import apply_title_wave as w
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        w.apply_local({"revsrc:A": "new title"})
    with cp.write_session():
        assert w.apply_local({"revsrc:A": "new title"}) == [("revsrc:A", "new title")]
    assert _read(t0, "SELECT title FROM series WHERE series_id='revsrc:A'") == "new title"
    assert t0.opens[-1] == ("rw", True) and ("PLAIN", False) not in t0.opens and ("PLAIN", True) not in t0.opens


def test_build_series_metadata_runs_as_main_with_the_lock_after_t0(t0, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["core/build_series_metadata.py"])
    runpy.run_path(os.path.join(ROOT, "core", "build_series_metadata.py"), run_name="__main__")
    assert '"citation_short"' in _read(t0, "SELECT metadata FROM series WHERE series_id='revsrc:A'")
    assert t0.opens and all(o == ("rw", True) for o in t0.opens), t0.opens
    assert cp._held is None, "released at the end"


def test_build_series_metadata_s_main_alone_is_refused_after_t0(t0):
    from core import build_series_metadata as b
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        b.main()


@pytest.fixture
def broaden(t0, monkeypatch):
    from core import broaden_catalog as bc
    store = t0.tmp / "store"
    store.mkdir()
    monkeypatch.setattr(bc, "STORE", str(store))
    monkeypatch.setattr(bc, "OUTDIR", str(t0.tmp / "out"))
    monkeypatch.setattr(bc, "_served_ids", lambda: set())
    return bc


def test_broaden_dry_run_reads_only_and_takes_no_lock(t0, broaden, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["broaden_catalog.py", "--dry-run"])
    broaden.main()
    assert t0.opens == [("ro", False)], t0.opens


def test_broaden_writes_only_with_the_lock_after_t0(t0, broaden, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["broaden_catalog.py"])
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        broaden.main()
    with cp.write_session():
        broaden.main()
    assert t0.opens[-1] == ("rw", True)
    assert _read(t0, "SELECT COUNT(*) FROM series_fts") == 1, "the FTS rebuild ran on the catalogue"


# ---- static ratchets over the whole repo ---------------------------------------------------------------------
LOCKERS = {"write_session", "writer_lock", "write_session_for_process"}
# WHAT COUNTS AS A VALUE FROM core.catalog_path: ANY attribute of the module, under any alias, and any name
# imported from it. The first merge narrowed this to a list of five names to let t0_ready's state.db opens
# through, and catalog_path.ROOT - joined with data/catalog.db, the catalogue itself - then passed every rule
# (R1216). A list of allowed names also lets through every attribute added later. So the source stays broad,
# and the state.db opens are exempted one by one below (_EXEMPT_OPENS), each proven read-only and proven to
# take its taint from LIVE_STATE_DIR alone.
_MODULE = "core.catalog_path"


def _catalog_bindings(tree) -> tuple[set, dict]:
    """(names bound to the module, {local name: original name} for names imported from it)."""
    mods, names = {"catalog_path", _MODULE}, {}
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            mods |= {a.asname or a.name for a in n.names if a.name == "catalog_path"}
        elif isinstance(n, ast.ImportFrom) and n.module == _MODULE:
            names.update({a.asname or a.name: a.name for a in n.names})
        elif isinstance(n, ast.Import):
            mods |= {a.asname for a in n.names if a.name == _MODULE and a.asname}
    return mods, names


def _is_source(node, bindings, ignore=frozenset()) -> bool:
    mods, names = bindings
    for x in ast.walk(node):
        if isinstance(x, ast.Attribute) and ast.unparse(x.value) in mods and x.attr not in ignore:
            return True
        if isinstance(x, ast.Name) and x.id in names and names[x.id] not in ignore:
            return True
    return False


def _calls(tree):
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
            yield n


def _resolver_write_opens(tree):
    """catalog_path.connect(...) / connect_path(...) whose write= is anything but the literal False."""
    for n in _calls(tree):
        if n.func.attr in ("connect", "connect_path") and ast.unparse(n.func.value).endswith("catalog_path"):
            w = next((k.value for k in n.keywords if k.arg == "write"), None)
            if w is not None and not (isinstance(w, ast.Constant) and w.value is False):
                yield n


def _python_files():
    for rel, p in _repo_walk.code_files((".py",)):
        if rel == "core/catalog_path.py":
            continue
        yield rel, _repo_walk.parse_code(p)          # utf-8-sig; a file that does not parse fails (R1209)


def test_every_resolver_writer_takes_the_lock():
    bad = []
    for rel, tree in _python_files():
        if any(True for _ in _resolver_write_opens(tree)):
            if not any(n.func.attr in LOCKERS for n in _calls(tree)):
                bad.append(rel)
    assert not bad, ("these open the catalogue for WRITE through core.catalog_path but never take the lock - "
                     "after T0 the resolver refuses their write: wrap the entry in catalog_path.write_session() "
                     "(or write_session_for_process() for a top-level script):\n  " + "\n  ".join(bad))


def test_the_lock_ratchet_can_fail(tmp_path):
    tree = ast.parse("from core import catalog_path\ncon = catalog_path.connect(write=True)\n")
    assert any(True for _ in _resolver_write_opens(tree)) and not any(n.func.attr in LOCKERS for n in _calls(tree))
    ro = ast.parse("con = catalog_path.connect_path(p, write=False)\n")
    assert not any(True for _ in _resolver_write_opens(ro))


def _plain_opens_of_resolver_paths(tree):
    b = _catalog_bindings(tree)
    for n in _calls(tree):
        if n.func.attr == "connect" and ast.unparse(n.func.value) == "sqlite3" and n.args:
            if _is_source(n.args[0], b):
                yield n


def test_no_plain_sqlite3_open_of_a_resolver_path():
    bad = [f"{rel}:{n.lineno}" for rel, tree in _python_files() for n in _plain_opens_of_resolver_paths(tree)]
    assert not bad, ("plain sqlite3.connect of a path from core.catalog_path bypasses its read-only mode and its "
                     "lock check - use catalog_path.connect() / connect_path():\n  " + "\n  ".join(bad))


def test_the_plain_open_ratchet_can_fail():
    tree = ast.parse("import sqlite3\nc = sqlite3.connect(catalog_path.catalog_path())\n"
                     "d = sqlite3.connect(catalog_path.under(ROOT), timeout=5)\n")
    assert len(list(_plain_opens_of_resolver_paths(tree))) == 2


# ---- R1203: a plain open in ANY file that uses core.catalog_path ----------------------------------------------
# The marker rule in test_catalogue_writers_main covered only the 49 writer files, and the ratchet above sees
# only a `catalog_path.` expression written as the argument. So `p = catalog_path.catalog_path();
# sqlite3.connect(p)` in any other file passed every test and, after T0, wrote the build with no lock (R1203
# probe_n1). Two rules close it:
#   1. TAINT: a plain open whose path comes from a `catalog_path.` value through variables is refused,
#      marker or not - that is the resolver's file opened around its lock and its read-only mode.
#   2. SCOPE: in every file that imports core.catalog_path, every plain open (any alias of sqlite3, or
#      `from sqlite3 import connect`) carries `# plain-open: <reason>` on one of its lines. The reason is
#      self-certified - rule 1 is what catches the resolver's own path.

def _imports_catalog_path(tree) -> bool:
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core" and any(a.name == "catalog_path" for a in n.names):
            return True
        if isinstance(n, ast.ImportFrom) and n.module == "core.catalog_path":
            return True
        if isinstance(n, ast.Import) and any(a.name == "core.catalog_path" for a in n.names):
            return True
    return False


def _sqlite_opens(tree):
    """Every call that opens a database with plain sqlite3: `<alias>.connect(...)` for any alias of the sqlite3
    module, and `<name>(...)` for any name imported as `from sqlite3 import connect [as name]`."""
    mods = {a.asname or a.name for n in ast.walk(tree) if isinstance(n, ast.Import)
            for a in n.names if a.name == "sqlite3"}
    funcs = {a.asname or a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module == "sqlite3"
             for a in n.names if a.name == "connect"}
    for n in ast.walk(tree):
        if not isinstance(n, ast.Call):
            continue
        f = n.func
        if isinstance(f, ast.Attribute) and f.attr == "connect" and isinstance(f.value, ast.Name) and f.value.id in mods:
            yield n
        elif isinstance(f, ast.Name) and f.id in funcs:
            yield n


def _tainted_names(tree, ignore=frozenset()) -> set:
    """Names assigned (anywhere in the file) from a value that comes from core.catalog_path (_is_source) or
    from another tainted name - a fixpoint, so `p = catalog_path.x(); q = f"file:{p}"` taints both."""
    b = _catalog_bindings(tree)
    assigns = []
    for n in ast.walk(tree):
        if isinstance(n, (ast.Assign, ast.AnnAssign, ast.AugAssign)) and n.value is not None:
            targets = n.targets if isinstance(n, ast.Assign) else [n.target]
            names = {t.id for tg in targets for t in ast.walk(tg) if isinstance(t, ast.Name)}
            assigns.append((names, n.value))
        elif isinstance(n, ast.NamedExpr):
            assigns.append(({n.target.id}, n.value))
    tainted: set = set()
    while True:
        grew = False
        for names, value in assigns:
            if names <= tainted:
                continue
            used = {x.id for x in ast.walk(value) if isinstance(x, ast.Name)}
            if _is_source(value, b, ignore) or used & tainted:
                tainted |= names
                grew = True
        if not grew:
            return tainted


def _open_arg(n):
    return n.args[0] if n.args else next((k.value for k in n.keywords if k.arg == "database"), None)


def _tainted_opens(tree, ignore=frozenset()):
    b = _catalog_bindings(tree)
    tainted = _tainted_names(tree, ignore)
    for n in _sqlite_opens(tree):
        arg = _open_arg(n)
        if arg is None:
            continue
        if _is_source(arg, b, ignore) or {x.id for x in ast.walk(arg) if isinstance(x, ast.Name)} & tainted:
            yield n


# The ONLY plain opens of a catalog_path-derived path allowed: t0_ready reading the updater's state.db. Each is
# named by (file, the open's argument as written) and must (a) be read-only - mode=ro in a file: URI opened
# with uri=True - and (b) take its taint from LIVE_STATE_DIR ALONE: with LIVE_STATE_DIR not counted as a
# source the open is clean, so a catalogue value (ROOT, BUILD_PATH, ...) mixed into it is still caught. The
# count is exact, so a new open with the same text is not silently covered.
_EXEMPT_OPENS = {("tools/selfhost/t0_ready.py", "f'file:{p}?mode=ro'"): 2}
_STATE_ONLY = frozenset({"LIVE_STATE_DIR"})


def _exempt(rel, tree, n) -> bool:
    arg = _open_arg(n)
    if (rel, ast.unparse(arg)) not in _EXEMPT_OPENS or "mode=ro" not in ast.unparse(arg):
        return False
    if not any(k.arg == "uri" and isinstance(k.value, ast.Constant) and k.value.value is True for k in n.keywords):
        return False
    return all(m.lineno != n.lineno for m in _tainted_opens(tree, ignore=_STATE_ONLY))


def _violations(rel, tree):
    """(tainted opens that are not exempt, how many exempt opens were used per key)."""
    bad, used = [], {}
    for n in _tainted_opens(tree):
        if _exempt(rel, tree, n):
            key = (rel, ast.unparse(_open_arg(n)))
            used[key] = used.get(key, 0) + 1
        else:
            bad.append(n)
    return bad, used


_MARKER = re.compile(r"#\s*plain-open:\s*\S")


def _unmarked_opens(tree, src):
    lines = src.splitlines()
    for n in _sqlite_opens(tree):
        span = lines[n.lineno - 1:(n.end_lineno or n.lineno)]
        if not any(_MARKER.search(ln) for ln in span):
            yield n


def _code_files_with_source():
    for rel, p in _repo_walk.code_files((".py",)):
        if rel == "core/catalog_path.py" or rel.startswith("tests/"):
            continue
        src = _repo_walk.read_code(p)                # utf-8-sig; a file that does not parse fails (R1209)
        yield rel, ast.parse(src, filename=p), src


def test_no_plain_open_of_a_path_that_came_from_catalog_path():
    bad, used = [], {}
    for rel, tree, _s in _code_files_with_source():
        b, u = _violations(rel, tree)
        bad += [f"{rel}:{n.lineno}" for n in b]
        for k, v in u.items():
            used[k] = used.get(k, 0) + v
    assert not bad, ("a plain sqlite3 open of a path taken from core.catalog_path bypasses its read-only mode "
                     "and its lock - use catalog_path.connect() / connect_path():\n  " + "\n  ".join(bad))
    assert used == _EXEMPT_OPENS, "each exemption is used exactly as often as it is listed"


def test_every_plain_open_in_a_catalog_path_file_says_why():
    bad = [f"{rel}:{n.lineno}" for rel, tree, src in _code_files_with_source() if _imports_catalog_path(tree)
           for n in _unmarked_opens(tree, src)]
    assert not bad, ("plain sqlite3 opens in files that use core.catalog_path, without a "
                     "'# plain-open: <reason>' on the call:\n  " + "\n  ".join(bad))


def test_the_r1203_rules_can_fail():
    n1 = ("from core import catalog_path\nimport sqlite3\np = catalog_path.catalog_path()\n"
          "c = sqlite3.connect(p)  # plain-open: says anything\n")
    assert len(list(_tainted_opens(ast.parse(n1)))) == 1, "N1: a variable from catalog_path, even marked"
    n1b = ("from core import catalog_path\nimport sqlite3 as sq\nroot = catalog_path.under(R)\n"
           "uri = f'file:{root}?mode=ro'\nc = sq.connect(uri, uri=True)\n")
    assert len(list(_tainted_opens(ast.parse(n1b)))) == 1, "two hops and an alias"
    n1c = ("from core import catalog_path\nfrom sqlite3 import connect as op\n"
           "c = op(database=catalog_path.catalog_path())\n")
    assert len(list(_tainted_opens(ast.parse(n1c)))) == 1, "from-import, keyword argument"
    ok = ("from core import catalog_path\nimport sqlite3\n"
          "c = sqlite3.connect(state_db)  # plain-open: the updater's state.db\n")
    assert not list(_tainted_opens(ast.parse(ok))) and not list(_unmarked_opens(ast.parse(ok), ok))
    unmarked = "from core import catalog_path\nimport sqlite3\nc = sqlite3.connect(\n    other_db)\n"
    assert len(list(_unmarked_opens(ast.parse(unmarked), unmarked))) == 1
    empty_reason = "import sqlite3\nc = sqlite3.connect(x)  # plain-open:\n"
    assert len(list(_unmarked_opens(ast.parse(empty_reason), empty_reason))) == 1, "a marker needs a reason"
    assert _imports_catalog_path(ast.parse("import core.catalog_path\n"))
    assert not _imports_catalog_path(ast.parse("# core.catalog_path\nimport os\n"))


@pytest.mark.parametrize("src", [
    # R1216's planted escapes: the catalogue reached through names the narrowed rule did not list
    "c = sqlite3.connect(os.path.join(catalog_path.ROOT, 'data', 'catalog.db'))  # plain-open: read-only lookup\n",
    "c = sqlite3.connect(os.path.join(catalog_path.LIVE_STATE_DIR, '..', 'catalog.db'))  # plain-open: x\n",
    "d = os.path.dirname(catalog_path.LIVE_STATE_DIR)\nc = sqlite3.connect(os.path.join(d, 'catalog.db'))\n",
    "c = sqlite3.connect(catalog_path.A_NAME_ADDED_LATER)\n",
    # the two gaps that predate the merge
    "from core.catalog_path import BUILD_PATH\nc = sqlite3.connect(BUILD_PATH)\n",
    "from core.catalog_path import BUILD_PATH as B\nq = f'file:{B}'\nc = sqlite3.connect(q, uri=True)\n",
    "import core.catalog_path as cp\nc = sqlite3.connect(cp.BUILD_PATH)\n",
    "from core import catalog_path as cp\nc = sqlite3.connect(cp.CHECKOUT_PATH)\n",
])
def test_the_r1216_taint_rule_can_fail(src):
    src = "import os\nimport sqlite3\nfrom core import catalog_path\n" + src
    bad, _used = _violations("tools/x.py", ast.parse(src))
    assert len(bad) == 1, src


def test_the_state_db_exemption_is_exact():
    head = "import os\nimport sqlite3\nfrom core import catalog_path\n"
    ok = head + ("p = os.path.join(catalog_path.LIVE_STATE_DIR, 'state.db')\n"
                 "c = sqlite3.connect(f'file:{p}?mode=ro', uri=True)  # plain-open: state.db\n")
    assert _violations("tools/selfhost/t0_ready.py", ast.parse(ok)) == \
        ([], {("tools/selfhost/t0_ready.py", "f'file:{p}?mode=ro'"): 1})
    assert len(_violations("tools/other.py", ast.parse(ok))[0]) == 1, "another file is not exempt"
    mixed = ok.replace("catalog_path.LIVE_STATE_DIR, 'state.db'", "catalog_path.ROOT, 'data', 'catalog.db'")
    assert len(_violations("tools/selfhost/t0_ready.py", ast.parse(mixed))[0]) == 1, "a catalogue value in it"
    no_uri = ok.replace(", uri=True", "")
    assert len(_violations("tools/selfhost/t0_ready.py", ast.parse(no_uri))[0]) == 1, "mode=ro needs uri=True"
    also = ok + "p = catalog_path.BUILD_PATH\n"
    assert len(_violations("tools/selfhost/t0_ready.py", ast.parse(also))[0]) == 1, "p tainted elsewhere too"
