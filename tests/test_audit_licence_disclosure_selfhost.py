"""tools/audit_licence_disclosure.py after T0 (plan step 6d): the served licences are the catalogue BUILD's (the origin
serves its copies; D1 is frozen and is not asked)."""
import os
import sqlite3
import sys

import pytest

from core import catalog_path, cutover, d1_remote

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import audit_licence_disclosure as L  # noqa: E402


def test_after_t0_the_build_is_the_serving_catalogue(tmp_path, monkeypatch, capsys):
    build = tmp_path / "live" / "data" / "catalog.db"
    build.parent.mkdir(parents=True)
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, license_id TEXT)")
        c.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, commercial_ok INT, no_modify INT, reservable INT)")
        c.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT)")
        c.execute("INSERT INTO series VALUES ('zz:a', 'zz', 'cc-by-4.0')")
        c.execute("INSERT INTO license VALUES ('cc-by-4.0', 1, 0, 1)")
    c.close()
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(d1_remote, "rows", lambda *a, **k: pytest.fail("D1 asked after T0"))
    monkeypatch.setattr(L, "classifications", lambda: {"zz": "redistributable_attribution"})
    monkeypatch.setattr(L, "granted", lambda: {})
    (tmp_path / "CUTOVER").write_text("")
    real, sql = catalog_path.connect, []

    def traced(*a, **k):
        con = real(*a, **k)
        con.set_trace_callback(sql.append)
        return con
    monkeypatch.setattr(catalog_path, "connect", traced)
    monkeypatch.setattr(sys, "argv", ["audit_licence_disclosure.py"])
    L.main()
    # R1243 LD2: after T0 main() must take the CHUNKED read - one GROUP BY over the live build holds its lock
    series_reads = [q for q in sql if "FROM series" in q]
    assert series_reads and not any("GROUP BY" in q for q in series_reads), series_reads
    out = capsys.readouterr().out
    assert "read from: the catalogue BUILD (the origin serves the copy made at the last swap" in out, out


def _build(tmp_path):
    build = tmp_path / "b.db"
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, license_id TEXT)")
        c.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, commercial_ok INT, no_modify INT, reservable INT)")
        c.executemany("INSERT INTO series VALUES (?,?,?)", [
            ("aa:1", "aa", "cc-by-4.0"), ("aa:2", "aa", "cc-by-4.0"), ("aa:3", "aa", "odc"),
            ("bb:1", "bb", "cc-by-4.0"), ("bb:2", "bb", None), ("odd:1", "zz", "cc-by-4.0"), ("zz:1", "zz", "gone")])
        c.executemany("INSERT INTO license VALUES (?,?,?,?)", [("cc-by-4.0", 1, 0, 1), ("odc", 1, None, 0)])
    c.close()
    return build


def test_the_chunked_build_read_equals_the_group_by(tmp_path, monkeypatch):
    """R1243: served_from_build (chunked, no long lock) must give exactly served_from_local's rows - a missing
    licence row and a NULL column read -1, an id whose prefix is not its source_id still counts to its source."""
    monkeypatch.setattr(catalog_path, "CHECKOUT_PATH", str(_build(tmp_path)))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "NO_FLAG"))
    want = sorted(L.served_from_local(), key=repr)
    for chunk in (1, 2, 3, 1000):
        assert sorted(L.served_from_build(chunk=chunk), key=repr) == want, chunk


def test_iter_series_reads_every_row_once_including_null_and_empty_ids(tmp_path):
    """R1249 finding 4: `series_id > ''` never reaches '' or NULL - a TEXT PRIMARY KEY allows both."""
    import collections
    b = _build(tmp_path)
    with sqlite3.connect(b) as c:
        c.executemany("INSERT INTO series VALUES (?,?,?)", [("", "aa", "odc"), (None, "bb", "odc")])
    c.close()
    con = sqlite3.connect(b)
    want = collections.Counter(con.execute("SELECT source_id, license_id FROM series").fetchall())
    for chunk in (1, 2, 7, 1000):
        got = collections.Counter(catalog_path.iter_series(con, ("source_id", "license_id"), chunk=chunk))
        assert got == want, chunk
    with pytest.raises(ValueError):
        list(catalog_path.iter_series(con, ("source_id; DROP TABLE series",)))
    con.close()


def test_the_build_is_read_in_bounded_chunks_never_one_group_by(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog_path, "CHECKOUT_PATH", str(_build(tmp_path)))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "NO_FLAG"))
    real, sql = catalog_path.connect, []

    def traced(*a, **k):
        con = real(*a, **k)
        con.set_trace_callback(sql.append)
        return con
    monkeypatch.setattr(catalog_path, "connect", traced)
    L.served_from_build(chunk=3)
    reads = [q for q in sql if "FROM series" in q]
    chunks = [q for q in reads if "LIMIT 3" in q]
    nulls = [q for q in reads if "IS NULL" in q]
    assert len(chunks) == 4 and len(nulls) == 1 and len(reads) == 5, reads
    assert not any("GROUP BY" in q for q in reads), reads
