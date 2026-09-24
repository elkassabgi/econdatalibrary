"""tools/selfhost/origin_copies.py - the origin's primary and climate copies from the one catalogue."""
import hashlib
import os
import re
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
import origin_copies as oc  # noqa: E402

SERIES = [("noaa:a", "noaa"), ("noaa:b", "noaa"), ("noaa_direct:c", "noaa_direct"), ("ecb:x", "ecb"), ("ecb:y", "ecb")]


def _catalogue(path, drop_fts=False):
    c = sqlite3.connect(path)
    c.executescript("""
      CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT);
      CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, license_id TEXT);
      CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT, license_id TEXT);
      CREATE INDEX ix_series_source_id ON series(source_id);
      CREATE TABLE source_counts(source_id TEXT PRIMARY KEY, n INTEGER NOT NULL);
      CREATE TABLE unit_state(source_id TEXT, unit_id TEXT);
    """)
    if not drop_fts:
        c.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
    c.executemany("INSERT INTO license VALUES (?, ?)", [("pd", "public"), ("cc", "cc-by")])
    c.executemany("INSERT INTO source VALUES (?, ?, ?)", [("noaa", "NOAA", "pd"), ("noaa_direct", "N2", "cc"), ("ecb", "ECB", "cc")])
    c.executemany("INSERT INTO series VALUES (?, ?, ?, 'g', 'pd')", [(i, s, f"t {i}") for i, s in SERIES])
    if not drop_fts:
        c.executemany("INSERT INTO series_fts VALUES (?, ?, 'g')", [(i, f"t {i}") for i, _ in SERIES])
    c.executemany("INSERT INTO source_counts VALUES (?, ?)", [("noaa", 42), ("ecb", 2), ("noaa_direct", 1)])  # drifted
    c.execute("INSERT INTO unit_state VALUES ('ecb', 'u1')")
    c.commit()
    c.close()


def _digest(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def test_the_split_is_the_d1_sync_s(tmp_path):
    cat = tmp_path / "catalog.db"
    _catalogue(cat)
    before = _digest(cat)
    report = oc.build(str(cat), str(tmp_path / "out"))
    assert report == {"catalogue_series": 5, "primary": {"series": 3, "series_fts": 3}, "climate": {"series": 2, "series_fts": 2}}
    assert _digest(cat) == before, "the catalogue is never written"
    p = sqlite3.connect(tmp_path / "out" / "primary.sqlite")
    assert sorted(r[0] for r in p.execute("SELECT series_id FROM series")) == ["ecb:x", "ecb:y", "noaa_direct:c"]
    assert sorted(r[0] for r in p.execute("SELECT series_id FROM series_fts")) == ["ecb:x", "ecb:y", "noaa_direct:c"]
    assert dict(p.execute("SELECT source_id, n FROM source_counts")) == {"ecb": 2, "noaa_direct": 1}, \
        "recounted from series: the drifted noaa row is gone from the primary"
    assert p.execute("SELECT COUNT(*) FROM unit_state").fetchone()[0] == 1, "every other table is kept"
    assert p.execute("SELECT COUNT(*) FROM source WHERE source_id='noaa'").fetchone()[0] == 1, "parents stay in the primary"
    c = sqlite3.connect(tmp_path / "out" / "climate.sqlite")
    assert sorted(r[0] for r in c.execute("SELECT series_id FROM series")) == ["noaa:a", "noaa:b"]
    assert sorted(r[0] for r in c.execute("SELECT series_id FROM series_fts")) == ["noaa:a", "noaa:b"]
    assert c.execute("SELECT series_id FROM series_fts WHERE series_fts MATCH 'noaa'").fetchall(), "the FTS index works"
    assert dict(c.execute("SELECT source_id, n FROM source_counts")) == {"noaa": 2}
    assert [r[0] for r in c.execute("SELECT source_id FROM source")] == ["noaa"]
    assert [r[0] for r in c.execute("SELECT license_id FROM license")] == ["pd"]


def test_a_failed_build_leaves_no_copy(tmp_path):
    cat = tmp_path / "catalog.db"
    _catalogue(cat, drop_fts=True)
    with pytest.raises(RuntimeError, match="series_fts"):
        oc.build(str(cat), str(tmp_path / "out"))
    assert not (tmp_path / "out" / "primary.sqlite").exists() and not (tmp_path / "out" / "climate.sqlite").exists()


@pytest.mark.parametrize("damage,needle", [
    ("DELETE FROM series_fts WHERE series_id='ecb:x'", "series_fts has"),
    ("INSERT INTO series VALUES ('noaa:z', 'noaa', 't', 'g', 'pd')", "series_fts has"),
    ("UPDATE source_counts SET n = n + 1", "source_counts sums"),
])
def test_the_checks_catch_a_damaged_primary(tmp_path, damage, needle):
    cat = tmp_path / "catalog.db"
    _catalogue(cat)
    oc.build(str(cat), str(tmp_path / "out"))
    p = sqlite3.connect(tmp_path / "out" / "primary.sqlite")
    p.execute(damage)
    p.commit()
    p.close()
    with pytest.raises(RuntimeError, match=needle):
        oc.check(str(tmp_path / "out" / "primary.sqlite"), str(tmp_path / "out" / "climate.sqlite"), 5)


def test_the_checks_catch_a_shard_series_in_the_primary_and_a_lost_series(tmp_path):
    cat = tmp_path / "catalog.db"
    _catalogue(cat)
    oc.build(str(cat), str(tmp_path / "out"))
    p = sqlite3.connect(tmp_path / "out" / "primary.sqlite")
    p.execute("INSERT INTO series VALUES ('noaa:z', 'noaa', 't', 'g', 'pd')")
    p.execute("INSERT INTO series_fts VALUES ('noaa:z', 't', 'g')")
    p.execute("INSERT OR REPLACE INTO source_counts VALUES ('noaa', 1)")
    p.commit()
    p.close()
    with pytest.raises(RuntimeError, match="primary still holds"):
        oc.check(str(tmp_path / "out" / "primary.sqlite"), str(tmp_path / "out" / "climate.sqlite"), 6)
    oc.build(str(cat), str(tmp_path / "out2"))
    with pytest.raises(RuntimeError, match="!= catalogue"):
        oc.check(str(tmp_path / "out2" / "primary.sqlite"), str(tmp_path / "out2" / "climate.sqlite"), 6)


def test_each_copy_s_search_index_is_rebuilt_from_its_series(tmp_path):
    """R1183: writers change `series` without touching series_fts. A stale catalogue index - a retitle, a
    series with no index row, an orphan index row - must not reach the copies."""
    cat = tmp_path / "catalog.db"
    _catalogue(cat)
    c = sqlite3.connect(cat)
    c.execute("UPDATE series SET title='zebra retitled' WHERE series_id='ecb:x'")            # index still 't ecb:x'
    c.execute("UPDATE series SET title='walrus climate' WHERE series_id='noaa:a'")
    c.execute("DELETE FROM series_fts WHERE series_id='ecb:y'")                            # no index row
    c.execute("INSERT INTO series_fts VALUES ('ecb:gone', 't gone', 'g')")                 # an orphan
    c.commit()
    c.close()
    oc.build(str(cat), str(tmp_path / "out"))

    def q(name, sql, *a):
        con = sqlite3.connect(tmp_path / "out" / name)
        try:
            return con.execute(sql, a).fetchall()
        finally:
            con.close()
    match = "SELECT series_id FROM series_fts WHERE series_fts MATCH ?"
    assert q("primary.sqlite", match, "zebra") == [("ecb:x",)], "the new title is searchable"
    assert q("primary.sqlite", match, '"t ecb:x"') == [], "the old title is gone"
    assert q("primary.sqlite", "SELECT COUNT(*) FROM series_fts WHERE series_id='ecb:y'") == [(1,)]
    assert q("primary.sqlite", "SELECT COUNT(*) FROM series_fts WHERE series_id='ecb:gone'") == [(0,)]
    assert q("climate.sqlite", match, "walrus") == [("noaa:a",)], "the climate copy too"
    assert q("primary.sqlite", "SELECT sql FROM sqlite_master WHERE name='series_fts'")[0][0].startswith(
        "CREATE VIRTUAL TABLE series_fts USING fts5"), "the catalogue's own FTS definition"


def _state_db(path, rows):
    s = sqlite3.connect(path)
    s.executescript("CREATE TABLE source_state (source_id TEXT PRIMARY KEY, cadence TEXT, status TEXT, "
                    "last_success_utc TEXT);"
                    "CREATE TABLE unit_state (source_id TEXT, unit_id TEXT, status TEXT, last_success_utc TEXT, "
                    "upstream_vintage TEXT, last_obs_date TEXT, obs_count INTEGER, PRIMARY KEY (source_id, unit_id));")
    for src in rows:
        s.execute("INSERT INTO source_state VALUES (?,?,?,?)", (src, "daily", "ok", "2026-09-01T00:00:00+00:00"))
        s.execute("INSERT INTO unit_state VALUES (?,?,?,?,?,?,?)", (src, "_all", "ok", "2026-09-01T00:00:00+00:00",
                                                                    None, None, 1))
    s.commit()
    s.close()


def _dated_catalogue(path):
    _catalogue(path)
    c = sqlite3.connect(path)
    c.execute("ALTER TABLE series ADD COLUMN end_date TEXT")
    c.execute("UPDATE series SET end_date = '2026-06-30'")
    c.commit()
    c.close()


def test_the_primary_copy_carries_the_freshness_projection(tmp_path):
    """R1186: the production catalog.db has no unit_state / source_state / source_data_through - they
    lived in D1 alone. The copies build them with the D1 sync's own emitter."""
    cat, st = tmp_path / "catalog.db", tmp_path / "state.db"
    _dated_catalogue(cat)
    _state_db(st, ["ecb", "noaa"])
    report = oc.build(str(cat), str(tmp_path / "out"), state_db=str(st))
    assert report["freshness"]["unit_state"] == 2 and report["freshness"]["source_state"] == 2
    assert report["freshness"]["source_data_through"] >= 1
    con = sqlite3.connect(tmp_path / "out" / "primary.sqlite")
    try:
        assert con.execute("SELECT data_through FROM source_data_through WHERE source_id='ecb'").fetchone() == ("2026-06-30",)
    finally:
        con.close()
    assert sorted(os.listdir(tmp_path / "out")) == ["climate.sqlite", "primary.sqlite"], "no SQL left behind"


def test_an_empty_state_db_fails_the_build(tmp_path):
    cat, st = tmp_path / "catalog.db", tmp_path / "state.db"
    _dated_catalogue(cat)
    _state_db(st, [])
    with pytest.raises(RuntimeError, match="freshness projection refused"):
        oc.build(str(cat), str(tmp_path / "out"), state_db=str(st))
    assert not os.path.exists(tmp_path / "out" / "primary.sqlite"), "a failed build leaves no copy"


def test_the_freshness_check_can_fail(tmp_path):
    cat = tmp_path / "catalog.db"
    _catalogue(cat)
    oc.build(str(cat), str(tmp_path / "out"))                      # no state_db: no projection built
    with pytest.raises(RuntimeError, match="freshness table"):
        oc.check(str(tmp_path / "out" / "primary.sqlite"), str(tmp_path / "out" / "climate.sqlite"), 5,
                 freshness=True)


def _with_sec_edgar(path):
    c = sqlite3.connect(path)
    c.execute("INSERT INTO source VALUES ('sec_edgar', 'SEC', 'pd')")
    c.execute("INSERT INTO series VALUES ('sec_edgar:AAPL', 'sec_edgar', 't', 'g', 'pd', '2026-06-30')")
    c.execute("INSERT INTO series_fts VALUES ('sec_edgar:AAPL', 't', 'g')")
    c.commit()
    c.close()


def test_a_d1_only_source_without_its_local_writer_fails_the_copy(tmp_path):
    """R1195: the gate read a dict; the copy then served no data_through for sec_edgar. Now the RESULT is
    checked: a served source with a dated series and no data_through row fails the build."""
    cat, st = tmp_path / "catalog.db", tmp_path / "state.db"
    _dated_catalogue(cat)
    _with_sec_edgar(cat)
    _state_db(st, ["ecb", "noaa", "sec_edgar"])
    with pytest.raises(RuntimeError, match=r"no data_through for \['sec_edgar'\]"):
        oc.build(str(cat), str(tmp_path / "out"), state_db=str(st))


def test_a_registered_local_writer_supplies_the_value(tmp_path, monkeypatch):
    from core import sync_state_d1
    (tmp_path / "fake_sec_writer.py").write_text("def data_through(conn):\n    return '2026-09-04'\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(sync_state_d1, "LOCAL_FRESHNESS_WRITERS", {"sec_edgar": "fake_sec_writer"})
    cat, st = tmp_path / "catalog.db", tmp_path / "state.db"
    _dated_catalogue(cat)
    _with_sec_edgar(cat)
    _state_db(st, ["ecb", "noaa", "sec_edgar"])
    oc.build(str(cat), str(tmp_path / "out"), state_db=str(st))
    con = sqlite3.connect(tmp_path / "out" / "primary.sqlite")
    try:
        got = dict(con.execute("SELECT source_id, data_through FROM source_data_through").fetchall())
    finally:
        con.close()
    assert got["sec_edgar"] == "2026-09-04", "the writer's value, not a statistic over the copy (R737)"


def test_a_writer_name_that_does_not_import_fails_the_build(tmp_path, monkeypatch):
    from core import sync_state_d1
    monkeypatch.setattr(sync_state_d1, "LOCAL_FRESHNESS_WRITERS", {"sec_edgar": "no.such.module"})
    cat, st = tmp_path / "catalog.db", tmp_path / "state.db"
    _dated_catalogue(cat)
    _with_sec_edgar(cat)
    _state_db(st, ["ecb", "noaa", "sec_edgar"])
    with pytest.raises(ModuleNotFoundError):
        oc.build(str(cat), str(tmp_path / "out"), state_db=str(st))


def test_a_gated_source_gets_no_data_through_and_is_not_demanded(tmp_path, monkeypatch):
    """R1195 mutant V17: the licence gate dropped from the copy's data_through survived every test."""
    from core import sync_state_d1
    monkeypatch.setattr(sync_state_d1, "_gated_ids", lambda: {"ecb"})
    cat, st = tmp_path / "catalog.db", tmp_path / "state.db"
    _dated_catalogue(cat)
    _state_db(st, ["ecb", "noaa"])
    oc.build(str(cat), str(tmp_path / "out"), state_db=str(st))
    con = sqlite3.connect(tmp_path / "out" / "primary.sqlite")
    try:
        got = {r[0] for r in con.execute("SELECT source_id FROM source_data_through")}
    finally:
        con.close()
    assert "ecb" not in got and "noaa" in got


def test_emit_sql_honours_data_through_false_with_a_catalogue_present(tmp_path):
    """R1195 mutant V26: the tests had no catalogue, so emit_sql skipped data_through anyway; in production
    that flag alone keeps the 1,833 s GROUP BY out of the writer lock."""
    from core import sync_state_d1
    cat, st = tmp_path / "catalog.db", tmp_path / "state.db"
    _dated_catalogue(cat)
    _state_db(st, ["ecb"])
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    _files, counts = sync_state_d1.emit_sql(str(st), str(tmp_path / "a"), gated=set(), catalogue=str(cat))
    assert counts.get("source_data_through", 0) >= 1, "positive control: a catalogue IS read by default"
    _files, counts = sync_state_d1.emit_sql(str(st), str(tmp_path / "b"), gated=set(), catalogue=str(cat),
                                            data_through=False)
    assert "source_data_through" not in counts


def test_the_check_refuses_an_empty_projection(tmp_path):
    """R1191 mutant N4: check()'s empty-projection branch had no test - the tables present, with no rows."""
    cat = tmp_path / "catalog.db"
    _catalogue(cat)
    oc.build(str(cat), str(tmp_path / "out"))
    p = tmp_path / "out" / "primary.sqlite"
    con = sqlite3.connect(p)
    con.executescript("DELETE FROM unit_state; CREATE TABLE source_state (source_id TEXT);"
                      "CREATE TABLE source_data_through (source_id TEXT, data_through TEXT);")
    con.commit()
    con.close()
    with pytest.raises(RuntimeError, match="empty freshness projection"):
        oc.check(str(p), str(tmp_path / "out" / "climate.sqlite"), 5, freshness=True)
    con = sqlite3.connect(p)
    con.execute("INSERT INTO unit_state VALUES ('ecb', 'u1')")
    con.execute("INSERT INTO source_state VALUES ('ecb')")
    con.commit()
    con.close()
    assert oc.check(str(p), str(tmp_path / "out" / "climate.sqlite"), 5, freshness=True)["freshness"]["unit_state"] == 1


def test_state_db_is_read_inside_the_lock_and_data_through_from_the_copy_after_it(tmp_path, monkeypatch):
    """R1191 finding 5: the data_through GROUP BY over 13.9M rows (1,833 s cold) ran inside the writer lock.
    Only state.db is read under the lock now; data_through comes from the primary copy after it, while
    the copy still holds the shard's rows (noaa keeps its row)."""
    from core import sync_state_d1
    cat, st = tmp_path / "catalog.db", tmp_path / "state.db"
    _dated_catalogue(cat)
    _state_db(st, ["ecb", "noaa"])
    held, seen = [False], {}

    class Lock:
        def __enter__(self):
            held[0] = True

        def __exit__(self, *exc):
            held[0] = False

    real_emit, real_rows = sync_state_d1.emit_sql, sync_state_d1.data_through_rows

    def emit(*a, **kw):
        seen["emit"] = (held[0], kw.get("data_through"))
        return real_emit(*a, **kw)

    def rows(con, gated):
        seen["rows"] = (held[0], os.path.basename(con.execute("PRAGMA database_list").fetchone()[2]))
        return real_rows(con, gated)
    monkeypatch.setattr(sync_state_d1, "emit_sql", emit)
    monkeypatch.setattr(sync_state_d1, "data_through_rows", rows)
    oc.build(str(cat), str(tmp_path / "out"), lock=Lock, state_db=str(st))
    assert seen["emit"] == (True, False), "state.db inside the lock, and without the data_through query"
    assert seen["rows"] == (False, "primary.sqlite"), "data_through after the lock, from the copy"
    con = sqlite3.connect(tmp_path / "out" / "primary.sqlite")
    try:
        got = dict(con.execute("SELECT source_id, data_through FROM source_data_through").fetchall())
    finally:
        con.close()
    assert got.get("ecb") == "2026-06-30" and got.get("noaa") == "2026-06-30", got


def test_the_lock_covers_the_reads_and_only_the_reads(tmp_path):
    """R1185: the catalogue is read inside the lock - so a writer that commits the moment the lock is
    released changes nothing in the copies - and the rebuild runs after it is released."""
    cat = tmp_path / "catalog.db"
    _catalogue(cat)
    events = []

    class Lock:
        def __enter__(self):
            events.append("held")

        def __exit__(self, *exc):
            events.append("released")
            c = sqlite3.connect(cat)                       # a writer gets in the moment it is free
            c.execute("INSERT INTO series VALUES ('ecb:late', 'ecb', 't late', 'g', 'pd')")
            c.execute("INSERT INTO series_fts VALUES ('ecb:late', 't late', 'g')")
            c.commit()
            c.close()

    report = oc.build(str(cat), str(tmp_path / "out"), lock=Lock)
    assert events == ["held", "released"]
    assert report["catalogue_series"] == 5 and report["primary"]["series"] + report["climate"]["series"] == 5, \
        "the copies are the state read under the lock, not the late row"
    con = sqlite3.connect(tmp_path / "out" / "primary.sqlite")
    try:
        assert con.execute("SELECT COUNT(*) FROM series WHERE series_id='ecb:late'").fetchone() == (0,)
    finally:
        con.close()


def test_the_shard_list_is_the_worker_s():
    util = open(os.path.join(ROOT, "api", "worker", "src", "util.ts"), encoding="utf-8").read()
    m = re.search(r"SHARDED_SOURCES: ReadonlySet<string> = new Set\(\[([^\]]*)\]\)", util)
    assert m, "util.ts SHARDED_SOURCES not found"
    assert tuple(sorted(re.findall(r'"([^"]+)"', m.group(1)))) == oc.SHARD_SOURCES == ("noaa",)
