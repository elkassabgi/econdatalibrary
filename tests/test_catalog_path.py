"""core/catalog_path.py - the one catalogue resolver (plan 4a). The real E:/ProgramData paths are never
touched: the module constants are monkeypatched to temporary paths."""
import os
import re
import sqlite3
import subprocess
import sys
import time

import pytest

from core import catalog_path as cp
from core import cutover

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def paths(tmp_path, monkeypatch):
    checkout, build = tmp_path / "checkout" / "catalog.db", tmp_path / "live" / "catalog" / "catalog.db"
    for p, name in ((checkout, "checkout"), (build, "build")):
        p.parent.mkdir(parents=True)
        with sqlite3.connect(p) as c:
            c.execute("CREATE TABLE which (name TEXT)")
            c.execute("INSERT INTO which VALUES (?)", (name,))
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(checkout))
    monkeypatch.setattr(cp, "BUILD_PATH", str(build))
    monkeypatch.setattr(cp, "LOCK_PATH", str(tmp_path / "live" / "state" / "writer.lock"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    return tmp_path


def _which(conn):
    return conn.execute("SELECT name FROM which").fetchone()[0]


def test_before_t0_the_checkout_after_t0_the_build(paths):
    assert _which(cp.connect()) == "checkout"
    (paths / "CUTOVER").write_text("")
    assert _which(cp.connect()) == "build"


def test_read_connections_cannot_write(paths):
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        cp.connect().execute("INSERT INTO which VALUES ('x')")


def test_a_missing_catalogue_is_an_error_never_created(paths, monkeypatch):
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(paths / "nowhere" / "catalog.db"))
    for write in (False, True):
        with pytest.raises(FileNotFoundError):
            cp.connect(write=write)
    assert not (paths / "nowhere").exists()


def test_the_uri_never_creates_the_file(paths, monkeypatch):
    """Even past the isfile() check (a file removed in between), the mode=ro / mode=rw URI must not create a
    database (finding 7: 'rw' changed to 'rwc' survived because isfile() answered first)."""
    missing = paths / "gone" / "catalog.db"
    missing.parent.mkdir()
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(missing))
    monkeypatch.setattr(cp.os.path, "isfile", lambda p: True)
    for write in (False, True):
        with pytest.raises(sqlite3.OperationalError):
            cp.connect(write=write).execute("SELECT 1").fetchone()
    assert not missing.exists()


def test_before_t0_a_write_needs_no_lock(paths):
    c = cp.connect(write=True)
    c.execute("INSERT INTO which VALUES ('w')")
    c.commit()


def test_after_t0_a_write_needs_the_lock(paths):
    (paths / "CUTOVER").write_text("")
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        cp.connect(write=True)
    with cp.writer_lock():
        c = cp.connect(write=True)
        c.execute("INSERT INTO which VALUES ('w')")
        c.commit()
        c.close()
    with pytest.raises(cutover.CutoverRefused):
        cp.connect(write=True)                           # released again


HOLDER = r"""
import sys, time
sys.path.insert(0, sys.argv[1])
from core import catalog_path as cp, cutover
cp.LOCK_PATH, cutover.FLAG_PATH, cp.BUILD_PATH, cp.CHECKOUT_PATH = sys.argv[2:6]   # never the machine's (R1230)
print("|".join((cp.LOCK_PATH, cutover.FLAG_PATH, cp.BUILD_PATH, cp.CHECKOUT_PATH)), flush=True)
with cp.writer_lock():
    print("held", flush=True)
    time.sleep(30)
"""


def test_a_second_process_is_refused_not_queued(paths):
    lock = str(paths / "live" / "state" / "writer.lock")
    mine = (lock, cutover.FLAG_PATH, cp.BUILD_PATH, cp.CHECKOUT_PATH)
    p = subprocess.Popen([sys.executable, "-B", "-c", HOLDER, ROOT, *mine], stdout=subprocess.PIPE, text=True)
    try:
        # the child runs on THIS test's paths, every one of them (R1235 mutant C6: a child given the lock alone
        # passed - and would have read the machine's flag and build)
        assert p.stdout.readline().strip() == "|".join(mine)
        assert p.stdout.readline().strip() == "held"
        t0 = time.monotonic()
        with pytest.raises(cutover.CutoverRefused, match="another process"):
            with cp.writer_lock():
                pass
        assert time.monotonic() - t0 < 5, "refused at once, not after waiting"
    finally:
        p.kill()
        p.wait()
    with cp.writer_lock():                               # free again once the holder is gone
        pass


TORN = r"""
import sqlite3, sys, time
c = sqlite3.connect(sys.argv[1], isolation_level=None)
c.execute("PRAGMA cache_size=10")                   # tiny cache: changed pages spill into the file
c.execute("BEGIN IMMEDIATE")
c.executemany("INSERT INTO which VALUES (?)", [("torn" + "x" * 900,) for _ in range(3000)])
print("mid-transaction", flush=True)
time.sleep(60)                                      # killed here: no COMMIT, no ROLLBACK
"""


def test_taking_the_lock_recovers_a_writer_killed_mid_transaction(paths):
    """R1176 finding 4: a hot journal made every read-only open fail and any copy torn, while the lock was
    free. The next lock holder must roll it back before anyone reads or copies the catalogue."""
    (paths / "CUTOVER").write_text("")
    build = cp.BUILD_PATH
    p = subprocess.Popen([sys.executable, "-B", "-c", TORN, build], stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "mid-transaction"
    finally:
        p.kill()
        p.wait()
    assert os.path.exists(build + "-journal"), "the crash left a hot journal (the test's own precondition)"
    with pytest.raises(sqlite3.OperationalError):
        cp.connect().execute("SELECT count(*) FROM which").fetchone()
    with cp.writer_lock():
        pass
    assert not os.path.exists(build + "-journal")
    assert cp.connect().execute("SELECT name FROM which").fetchall() == [("build",)], "the torn rows are gone"


def test_a_persist_mode_journal_is_not_hot(paths):
    """AR-153: journal_mode=PERSIST keeps the journal after a commit with a zeroed header - not hot. The lock
    refused every such catalogue when it tested only "exists and is not empty"."""
    build = cp.BUILD_PATH
    c = sqlite3.connect(build)                      # the setup is a writer: before the flag (R1209's hook)
    c.execute("PRAGMA journal_mode=PERSIST")
    c.execute("INSERT INTO which VALUES ('committed')")
    c.commit()
    c.close()
    (paths / "CUTOVER").write_text("")
    j = build + "-journal"
    assert os.path.getsize(j) > 0 and not cp.journal_is_hot(j), "precondition: a kept, zeroed journal"
    with cp.writer_lock():
        pass
    assert sorted(r[0] for r in cp.connect().execute("SELECT name FROM which")) == ["build", "committed"]


class _NoRollback:
    """A connection that opens fine but leaves the journal where it is (as a live writer's journal stays)."""
    def execute(self, *_a):
        return self

    def fetchone(self):
        return (1,)

    def close(self):
        pass


def test_a_live_journal_is_hot_and_refused(paths, monkeypatch):
    """Positive control for the magic check, and the message names the likely cause."""
    (paths / "CUTOVER").write_text("")
    j = cp.BUILD_PATH + "-journal"
    assert not cp.journal_is_hot(j), "no journal: not hot"
    with open(j, "wb") as fh:
        fh.write(cp.JOURNAL_MAGIC + bytes(504))
    assert cp.journal_is_hot(j)
    monkeypatch.setattr(cp.sqlite3, "connect", lambda *a, **k: _NoRollback())
    with pytest.raises(RuntimeError, match="WITHOUT the writer lock"):
        with cp.writer_lock():
            pass


def test_a_busy_catalogue_names_the_unlocked_writer(paths, monkeypatch):
    (paths / "CUTOVER").write_text("")

    class Busy(_NoRollback):
        def execute(self, *_a):
            raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(cp.sqlite3, "connect", lambda *a, **k: Busy())
    with pytest.raises(RuntimeError, match="WITHOUT the writer lock"):
        with cp.writer_lock():
            pass


# ---- connect_path: the one-line replacement for a tool's own sqlite3.connect(<catalogue>) ---------------
def test_connect_path_before_t0_opens_the_tool_s_own_path(paths, tmp_path):
    other = tmp_path / "a_test_catalogue.db"
    with sqlite3.connect(other) as c:
        c.execute("CREATE TABLE which (name TEXT)")
        c.execute("INSERT INTO which VALUES ('other')")
    assert _which(cp.connect_path(other, write=False)) == "other", "a --db argument or a test's copy still works"
    with pytest.raises(sqlite3.OperationalError):
        cp.connect_path(other, write=False).execute("INSERT INTO which VALUES ('x')")
    w = cp.connect_path(str(other), write=True)
    w.execute("INSERT INTO which VALUES ('x')")
    w.commit()
    with pytest.raises(FileNotFoundError):
        cp.connect_path(tmp_path / "missing.db", write=True)
    assert not (tmp_path / "missing.db").exists(), "never created"


def test_connect_path_after_t0_opens_only_the_build(paths, tmp_path):
    (paths / "CUTOVER").write_text("")
    with pytest.raises(cutover.CutoverRefused, match="not"):
        cp.connect_path(cp.CHECKOUT_PATH, write=False)               # a worktree's data/catalog.db
    assert _which(cp.connect_path(cp.BUILD_PATH, write=False)) == "build"
    spelled = os.path.join(os.path.dirname(cp.BUILD_PATH), ".", os.path.basename(cp.BUILD_PATH))
    assert _which(cp.connect_path(spelled.upper() if os.name == "nt" else spelled, write=False)) == "build"
    with pytest.raises(cutover.CutoverRefused, match="lock"):
        cp.connect_path(cp.BUILD_PATH, write=True)
    with cp.writer_lock():
        cp.connect_path(cp.BUILD_PATH, write=True).execute("INSERT INTO which VALUES ('y')").connection.commit()


def test_write_session_before_t0_takes_no_lock(paths):
    lock_dir = os.path.dirname(cp.LOCK_PATH)
    with cp.write_session():
        c = cp.connect(write=True)
        c.execute("INSERT INTO which VALUES ('w')")
        c.commit()
        c.close()
    assert not os.path.exists(lock_dir), "no lock folder is made before T0 (CI's Linux runner)"


def test_write_session_after_t0_holds_the_lock_and_is_reentrant(paths):
    (paths / "CUTOVER").write_text("")
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        cp.connect(write=True)
    with cp.write_session():
        with cp.write_session():                          # the updater holds it; a cataloguer inside it
            c = cp.connect(write=True)
            c.execute("INSERT INTO which VALUES ('w')")
            c.commit()
            c.close()
        assert cp._held is not None, "the inner session did not release the outer one's lock"
    assert cp._held is None
    with pytest.raises(cutover.CutoverRefused):
        cp.connect(write=True)                           # released again


SCRIPT = r"""
import sys, time
sys.path.insert(0, {root!r})
from core import catalog_path as cp, cutover
cp.LOCK_PATH, cp.BUILD_PATH, cutover.FLAG_PATH = {lock!r}, {build!r}, {flag!r}
cp.write_session_for_process()
c = cp.connect(write=True)
c.execute("INSERT INTO which VALUES ('script')")
c.commit()
print("HELD", flush=True)
time.sleep(float(sys.argv[1]))
"""


def test_a_top_level_script_holds_the_lock_until_it_exits(paths):
    """write_session_for_process: a cataloguing script with no main() holds the lock for its whole life, and
    its exit - clean or killed - releases it."""
    (paths / "CUTOVER").write_text("")
    code = SCRIPT.format(root=ROOT, lock=cp.LOCK_PATH, build=cp.BUILD_PATH, flag=str(paths / "CUTOVER"))
    for how in ("exit", "kill"):
        p = subprocess.Popen([sys.executable, "-B", "-c", code, "2" if how == "exit" else "60"],
                             stdout=subprocess.PIPE, text=True)
        assert p.stdout.readline().strip() == "HELD"
        with pytest.raises(cutover.CutoverRefused, match="another process"):
            with cp.writer_lock():
                pass
        if how == "kill":
            p.kill()
        p.wait(30)
        with cp.writer_lock():                               # free again
            pass
    import pathlib
    c = sqlite3.connect(pathlib.Path(cp.BUILD_PATH).resolve().as_uri() + "?mode=ro", uri=True)   # a check reads
    assert [r[0] for r in c.execute("SELECT name FROM which WHERE name='script'")] == ["script", "script"]
    c.close()


def test_under_names_the_checkout_s_catalogue():
    """The real constants: a tool's own ROOT names the checkout's file, production's the build. CHECKOUT_PATH
    and BUILD_PATH are what the source builds from those roots (tests/conftest.py moves the attributes)."""
    src = open(cp.__file__, encoding="utf-8").read()
    assert 'CHECKOUT_PATH = os.path.join(ROOT, "data", "catalog.db")' in src
    assert 'BUILD_PATH = os.path.join(LIVE_STORE_ROOT, "data", "catalog.db")' in src
    assert cp.under(cp.ROOT) == os.path.join(cp.ROOT, "data", "catalog.db")
    assert cp.under(cp.LIVE_STORE_ROOT) == os.path.join(cp.LIVE_STORE_ROOT, "data", "catalog.db"), \
        "production's own ROOT names the build"


def test_connect_path_passes_sqlite_options(paths, tmp_path):
    sqlite3.register_converter("cp_test_marked", lambda b: ("converted", b.decode()))   # a type only this test uses
    with sqlite3.connect(cp.CHECKOUT_PATH) as w:
        w.execute("CREATE TABLE stamped (d cp_test_marked)")
        w.execute("INSERT INTO stamped VALUES ('2026-09-24')")
    w.close()
    c = cp.connect_path(cp.CHECKOUT_PATH, write=False, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
    assert _which(c) == "checkout"
    got = c.execute("SELECT d FROM stamped").fetchone()[0]
    assert got == ("converted", "2026-09-24"), f"detect_types did not reach sqlite3.connect: {got!r}"


def test_after_t0_a_link_to_the_build_opens_it(paths, tmp_path):
    """A junction (Windows) or symlink to the build's folder names the same file: realpath, not abspath."""
    link = tmp_path / "link_to_build"
    target = os.path.dirname(cp.BUILD_PATH)
    if os.name == "nt":
        made = subprocess.run(["cmd", "/c", "mklink", "/J", str(link), target], capture_output=True).returncode == 0
    else:
        os.symlink(target, link)
        made = True
    if not made:
        pytest.skip("could not make a junction here")
    (paths / "CUTOVER").write_text("")
    assert _which(cp.connect_path(link / "catalog.db", write=False)) == "build"


def test_the_lock_is_not_reentrant(paths):
    with cp.writer_lock():
        with pytest.raises(RuntimeError):
            with cp.writer_lock():
                pass


def test_the_resolver_has_no_override():
    src = open(cp.__file__, encoding="utf-8").read()
    code = re.sub(r'"""[\s\S]*?"""', "", src)
    code = "\n".join(l for l in code.splitlines() if not l.lstrip().startswith("#"))
    for banned in ("environ", "getenv", "argv"):
        assert banned not in code, f"core/catalog_path.py reads {banned!r}: the paths must not be overridable"
    # the DEFAULTS, read from the source: tests/conftest.py points LOCK_PATH, BUILD_PATH and CHECKOUT_PATH at
    # temporary paths for every test (R1227/R1230), so the live attributes are not the defaults here
    assert 'LIVE_STORE_ROOT = r"E:\\research\\econfindatalibrary"' in src
    assert 'LOCK_PATH = r"E:\\econ_live\\state\\writer.lock"' in src
    assert cp.LIVE_STORE_ROOT == r"E:\research\econfindatalibrary"
    assert 'BUILD_PATH = os.path.join(LIVE_STORE_ROOT, "data", "catalog.db")' in src, "the production checkout IS the build"
    assert cp.LIVE_STATE_DIR == os.path.join(cp.LIVE_STORE_ROOT, "data", "_aqueduct")


# ---- the ratchet: files that name catalog.db outside the resolver may only become fewer ---------------
LEGACY = os.path.join(ROOT, "tests", "catalog_db_legacy.txt")
# clients/ IS scanned (R1176: the updater's own catalogue open lives in clients/python/econdl/_catalog.py).
# Code that names the catalogue: the literal, the variable that overrides it, the name split in two, or
# the module constant several files import instead of naming the file (AR-153).
NAMES_CATALOGUE = re.compile(r"catalog\.db|ECONDL_CATALOG|\bCATALOG_DB\b|[\"']catalog[\"']\s*[+,]\s*[\"']\.db[\"']")
# A narrow exemption: only the named function of the named file is left out of the scan.
EXEMPT_FUNCTIONS = {"updater/run.py": frozenset({"_selfhost_preflight"}),   # names ECONDL_CATALOG to REFUSE it
                    "updater/blob.py": frozenset({"refuse_unless_live_checkout"})}   # the same refusal (R1217)
# Whole files outside the resolver BY DESIGN, each with the premise that makes it safe pinned below.
EXEMPT_FILES = {
    # The PUBLISHED client (pip install econdl): standalone, so it cannot import core.catalog_path, and its
    # users point it at their own copy. It opens mode=ro only (pinned by the test below), and in-repo the
    # updater's post-T0 preflight refuses to run unless econdl's default_db() IS the build
    # (updater/run.py _selfhost_preflight).
    "clients/python/econdl/_catalog.py",
}


def _code_only(src: str, ext: str, rel: str = "") -> str:
    """Only CODE counts, so a migrated file that still MENTIONS catalog.db in a comment or docstring leaves
    the list (R1176). Python goes through the parser (tests/_repo_walk.code_text, AR-153): other strings,
    triple-quoted ones included, are kept and implicit concatenation is joined. Shell: '#' lines dropped."""
    import _repo_walk
    if ext == ".py":
        return _repo_walk.code_text(src, EXEMPT_FUNCTIONS.get(rel, frozenset()))
    return "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))


def _naming_files():
    found = set()
    import _repo_walk                                  # the one shared walk (R1178)
    for rel, p in _repo_walk.code_files((".py", ".ps1", ".sh"), ROOT):
        with open(p, encoding="utf-8", errors="replace") as fh:
            if NAMES_CATALOGUE.search(_code_only(fh.read(), os.path.splitext(p)[1], rel)):
                found.add(rel)
    found.discard("core/catalog_path.py")
    return found - EXEMPT_FILES


def test_the_exempt_client_opens_read_only_and_the_preflight_checks_it(tmp_path):
    """The premises of EXEMPT_FILES: econdl's catalogue open cannot write, and updater/run.py's preflight
    compares econdl's default_db() with the build."""
    sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
    from econdl import _catalog
    db = tmp_path / "c.db"
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE series (series_id TEXT)")
    c.close()
    con = _catalog.connect(str(db))
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        con.execute("INSERT INTO series VALUES ('x')")
    con.close()
    run_src = open(os.path.join(ROOT, "updater", "run.py"), encoding="utf-8").read()
    pre = run_src[run_src.index("def _selfhost_preflight"):]
    body = pre[:pre.index("\ndef ")]
    assert "econdl_db = econdl_catalog.default_db()" in body and "econdl_db, BUILD_PATH" in body
    assert EXEMPT_FILES == {"clients/python/econdl/_catalog.py"}, "a new exemption needs its own premise test"


def test_the_exempt_client_answers_only_the_build_after_t0(tmp_path, monkeypatch):
    """R1194 finding 2: the preflight guards updater/run.py only, yet core/derive_csv.py and other in-repo
    users read through econdl's default_db(). After the cutover it answers the build or refuses."""
    sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
    from econdl import _catalog
    # the DEFAULTS, from the sources (tests/conftest.py moves the attributes of both, R1230)
    client = open(_catalog.__file__, encoding="utf-8").read()
    core_src = open(cutover.__file__, encoding="utf-8").read() + open(cp.__file__, encoding="utf-8").read()
    assert '_CUTOVER_FLAG = r"C:\\ProgramData\\econ\\CUTOVER"' in client and \
        'FLAG_PATH = r"C:\\ProgramData\\econ\\CUTOVER"' in core_src, "the client's copy of the flag drifted from core"
    assert '_BUILD_DB = r"E:\\research\\econfindatalibrary\\data\\catalog.db"' in client and \
        'LIVE_STORE_ROOT = r"E:\\research\\econfindatalibrary"' in core_src, \
        "the client's copy of the build drifted from core"
    assert _catalog._CUTOVER_FLAG == cutover.FLAG_PATH and _catalog._BUILD_DB == cp.BUILD_PATH, \
        "and the guard moved both copies together"
    build, other = tmp_path / "build.db", tmp_path / "other.db"
    for p in (build, other):
        p.write_bytes(b"")
    monkeypatch.setattr(_catalog, "_CUTOVER_FLAG", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(_catalog, "_BUILD_DB", str(build))
    monkeypatch.setattr(_catalog, "_DEFAULT_DB", str(other))
    monkeypatch.setenv("ECONDL_CATALOG", str(other))
    assert _catalog.default_db() == str(other), "before the cutover: unchanged"
    (tmp_path / "CUTOVER").write_text("")
    with pytest.raises(RuntimeError, match="refused"):
        _catalog.default_db()                                  # the override
    spelled = os.path.join(str(tmp_path), ".", "BUILD.DB" if os.name == "nt" else "build.db")
    monkeypatch.setenv("ECONDL_CATALOG", spelled)
    assert _catalog.default_db() == str(build), "the build under another spelling is the build (realpath/normcase)"
    monkeypatch.delenv("ECONDL_CATALOG")
    assert _catalog.default_db() == str(build), \
        "a checkout whose own copy is not the build gets the build - core.catalog_path's answer (R1199)"


def test_the_exempt_clients_flag_rule_is_cores(tmp_path, monkeypatch):
    """R1199: econdl used os.path.exists; core.cutover counts an UNREADABLE flag as cut over (fail closed)."""
    sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
    from econdl import _catalog
    monkeypatch.setattr(_catalog, "_CUTOVER_FLAG", str(tmp_path / "CUTOVER"))
    assert _catalog._cut_over() is False
    real = os.stat

    def unreadable(p, *a, **k):
        if str(p) == str(tmp_path / "CUTOVER"):
            raise PermissionError(13, "denied")
        return real(p, *a, **k)
    monkeypatch.setattr(_catalog.os, "stat", unreadable)
    assert _catalog._cut_over() is True


def test_the_catalogue_ratchet_can_fail():
    for code in ('p = os.path.join(ROOT, "data", "catalog.db")', 'os.environ.get("ECONDL_CATALOG")',
                 'name = "catalog" + ".db"', 'os.path.join(d, "catalog", ".db")'):
        assert NAMES_CATALOGUE.search(_code_only(code, ".py")), code
    assert not NAMES_CATALOGUE.search(_code_only('# the old catalog.db road\nx = 1\n"""uses catalog.db"""', ".py"))
    # AR-153: what the regex-based reader missed or hid
    for code in ('p = "catalog" ".db"',                                   # implicit concatenation
                 "SQL = '''ATTACH \"data/catalog.db\" AS c'''\nx = 1",   # a triple-quoted string that is CODE
                 'from core.paths import CATALOG_DB',                      # the module constant
                 'def f():\n    """doc"""\n    return "catalog.db"'):    # code after a docstring
        assert NAMES_CATALOGUE.search(_code_only(code, ".py")), code
    assert not NAMES_CATALOGUE.search(_code_only('def f():\n    """uses catalog.db"""\n    return 1', ".py"))
    run = 'def _selfhost_preflight(a):\n    os.environ.get("ECONDL_CATALOG")\n\ndef other():\n    return "catalog.db"'
    assert NAMES_CATALOGUE.search(_code_only(run, ".py", "updater/run.py")), "the rest of run.py is scanned"
    assert not NAMES_CATALOGUE.search(_code_only(run.split("\n\n")[0], ".py", "updater/run.py"))
    assert NAMES_CATALOGUE.search(_code_only("x = (", ".py") + "catalog.db"), "unparsable: scanned raw"
    nested = 'class C:\n    def _selfhost_preflight(self):\n        return "catalog.db"'
    assert NAMES_CATALOGUE.search(_code_only(nested, ".py", "updater/run.py")), "only the module-level function is exempt"


def test_no_new_file_names_catalog_db_outside_the_resolver():
    legacy = {l.strip() for l in open(LEGACY, encoding="utf-8") if l.strip() and not l.startswith("#")}
    found = _naming_files()
    assert found - legacy == set(), "new files name catalog.db: open it through core.catalog_path.connect"
    gone = legacy - found
    assert not gone, f"these no longer name catalog.db - remove them from {LEGACY}: {sorted(gone)}"
