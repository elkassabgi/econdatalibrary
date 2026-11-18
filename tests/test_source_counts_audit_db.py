"""tools/audit_d1_source_counts.py --db: the free local-truth mode against a catalogue COPY.

WHY. The local mode compared D1's `source_counts` with `LOCAL = catalog_path.catalog_path()` only - the
production checkout's live data/catalog.db before T0, the build after - and session rules forbid opening the
live file in place. core/catalog_path.py's plan step 1 is the sanctioned answer: the tool keeps a --db argument
and opens it with connect_path(path, write=False), which before T0 opens the tool's own path and after T0 opens
only the build. These pin that, hermetically: temporary sqlite catalogues, no network, no wrangler - the remote
read is faked at the tool's own `d1()` seam, which also lets the tests prove the mode adds no D1 query.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tools.audit_d1_source_counts as mod  # noqa: E402
from core import catalog_path as cp  # noqa: E402
from core import cutover  # noqa: E402

SOURCE_COUNTS_SQL = "SELECT source_id, n FROM source_counts"


def _catalogue(path, n_rows: int, source: str = "abc"):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY)")
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY)")
        c.execute("INSERT INTO source VALUES (?)", (source,))
        c.executemany("INSERT INTO series VALUES (?)", [(f"{source}:s{i}",) for i in range(n_rows)])
    return path


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    """Three catalogues that differ in ONE number, so the count read says which file was opened:
    the resolver's default (2 rows), a copy (5 rows) and the post-T0 build (7 rows)."""
    default = _catalogue(tmp_path / "checkout" / "data" / "catalog.db", 2)
    copy = _catalogue(tmp_path / "copies" / "catalog_copy.db", 5)
    build = _catalogue(tmp_path / "live" / "data" / "catalog.db", 7)
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(default))
    monkeypatch.setattr(cp, "BUILD_PATH", str(build))
    monkeypatch.setattr(cp, "LOCK_PATH", str(tmp_path / "live" / "state" / "writer.lock"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(mod, "LOCAL", str(default))          # what the import-time resolver names here
    return {"default": default, "copy": copy, "build": build, "flag": tmp_path / "CUTOVER"}


def _fake_d1(monkeypatch, cached_n: int):
    """The remote read, faked: each database answers its source_counts; anything else fails the test."""
    calls = []

    def d1(db, sql):
        calls.append((db, sql))
        if sql != SOURCE_COUNTS_SQL:
            pytest.fail(f"unexpected D1 query on {db}: {sql}")
        rows = [{"source_id": "abc", "n": cached_n}] if db == mod.DBS[0] else []
        return rows, len(rows)

    monkeypatch.setattr(mod, "d1", d1)
    return calls


def test_db_is_honoured(fleet, monkeypatch, capsys):
    """--db COPY counts the copy: the cache agrees with the copy (5) and not with the default (2)."""
    _fake_d1(monkeypatch, cached_n=5)
    assert mod.main(["--db", str(fleet["copy"])]) == 0
    assert str(fleet["copy"]) in capsys.readouterr().out, "the run must name the file it read"
    assert mod.main([]) == 1, "control: without --db the default catalogue (2 rows) disagrees with 5"


def _spy_connect_path(monkeypatch):
    opened, real = [], cp.connect_path

    def spy(path, *, write, **kw):
        opened.append((os.fspath(path), write))
        return real(path, write=write, **kw)

    monkeypatch.setattr(mod.catalog_path, "connect_path", spy)
    return opened


def test_the_default_is_unchanged(fleet, monkeypatch, capsys):
    """No --db opens LOCAL, read-only, through connect_path - exactly as before."""
    _fake_d1(monkeypatch, cached_n=2)
    opened = _spy_connect_path(monkeypatch)
    assert mod.main([]) == 0
    assert opened == [(mod.LOCAL, False)]
    assert str(fleet["default"]) in capsys.readouterr().out


def test_db_is_opened_read_only_through_connect_path(fleet, monkeypatch):
    _fake_d1(monkeypatch, cached_n=5)
    opened = _spy_connect_path(monkeypatch)
    assert mod.main(["--db", str(fleet["copy"])]) == 0
    assert opened == [(str(fleet["copy"]), False)]


def test_local_is_still_the_resolver_s_catalogue():
    """The default is decided by core.catalog_path, not by this tool (plan step 1): LOCAL is assigned
    from catalog_path.catalog_path() and nothing else. Read from the source - a reload would leak
    conftest's patched paths into the module for the rest of the session."""
    import ast
    tree = ast.parse(open(mod.__file__, encoding="utf-8").read())
    assigns = [n for n in tree.body if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == "LOCAL" for t in n.targets)]
    assert len(assigns) == 1
    assert ast.unparse(assigns[0].value) == "catalog_path.catalog_path()"


def test_a_missing_db_is_could_not_look_and_never_created(fleet, monkeypatch, tmp_path):
    _fake_d1(monkeypatch, cached_n=5)
    absent = tmp_path / "absent.db"
    assert mod.main(["--db", str(absent)]) == 2
    assert not absent.exists()


def test_after_t0_a_non_build_db_is_refused(fleet, monkeypatch, capsys):
    """After T0 connect_path opens only the build: a copy is refused, and the refusal is exit 2 (could not
    look), never 0 (agree) or 1 (a finding)."""
    _fake_d1(monkeypatch, cached_n=5)
    fleet["flag"].write_text("")
    assert cutover.is_cut_over()
    assert mod.main(["--db", str(fleet["copy"])]) == 2
    err = capsys.readouterr().err
    assert "COULD NOT LOOK" in err and "refused" in err, err


def test_after_t0_the_build_is_accepted_through_db(fleet, monkeypatch):
    """Control for the refusal: the same call with the build's path is opened (7 rows)."""
    _fake_d1(monkeypatch, cached_n=7)
    fleet["flag"].write_text("")
    assert mod.main(["--db", str(fleet["build"])]) == 0


def test_db_adds_no_d1_query(fleet, monkeypatch):
    """Cost unchanged: one free source_counts read per database, and nothing else."""
    calls = _fake_d1(monkeypatch, cached_n=5)
    mod.main(["--db", str(fleet["copy"])])
    assert calls == [(db, SOURCE_COUNTS_SQL) for db in mod.DBS]


def test_db_with_remote_truth_is_a_usage_error(fleet, monkeypatch):
    """--db names the local truth; with --remote-truth it would be silently ignored, so it is refused
    before any D1 read."""
    calls = _fake_d1(monkeypatch, cached_n=5)
    with pytest.raises(SystemExit) as e:
        mod.main(["--remote-truth", "--db", str(fleet["copy"])])
    assert e.value.code == 2
    assert calls == []


def test_a_fresh_copy_of_a_stale_catalogue_says_its_age_cannot_be_judged(fleet, monkeypatch, capsys):
    """A plain copy gets a fresh mtime, so a stale catalogue copied today reads "0d old" and the >=2-day
    warning stays silent. With --db the tool must say the age is the copy's mtime and cannot vouch for
    the content - even at 0 days; without --db (the resolver's own file) that note must not appear."""
    _fake_d1(monkeypatch, cached_n=5)
    old = fleet["copy"].stat().st_mtime - 30 * 86400
    os.utime(fleet["default"], (old, old))                     # the "real" catalogue is 30 days stale
    os.utime(fleet["copy"], None)                              # its copy was taken just now
    assert mod.main(["--db", str(fleet["copy"])]) == 0
    out = capsys.readouterr().out
    assert "0d old" in out, "precondition: the copy's mtime makes it look fresh"
    assert "copy's mtime is its copy time" in out
    assert "cannot be judged" in out
    assert "WARNING: local catalogue is" not in out, "control: the mtime warning is silent here"

    _fake_d1(monkeypatch, cached_n=2)
    assert mod.main([]) == 0
    out = capsys.readouterr().out
    assert "cannot be judged" not in out, "the note is for --db only"
    # not an exact day count: the tool subtracts naive local datetimes, so 30 days across a DST change
    # can read as 29 - the control is only that the >=2-day warning still fires on the default file.
    assert "WARNING: local catalogue is" in out, "control: the default keeps its mtime warning"


def test_a_failing_stat_after_the_open_closes_the_connection(fleet, monkeypatch):
    """The stats run after connect_path; if one raises, the connection must still be closed."""
    _fake_d1(monkeypatch, cached_n=5)
    closed, real = [], cp.connect_path

    class Spy:
        def __init__(self, con):
            self._con = con

        def execute(self, *a, **k):
            return self._con.execute(*a, **k)

        def close(self):
            closed.append(True)
            self._con.close()

    monkeypatch.setattr(mod.catalog_path, "connect_path",
                        lambda path, *, write, **kw: Spy(real(path, write=write, **kw)))

    def boom(_p):
        raise OSError("stat failed")

    monkeypatch.setattr(mod.os.path, "getsize", boom)
    assert mod.main(["--db", str(fleet["copy"])]) == 2, "a failed stat is could-not-look"
    assert closed == [True], "the connection leaked"
