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


def test_the_shard_list_is_the_worker_s():
    util = open(os.path.join(ROOT, "api", "worker", "src", "util.ts"), encoding="utf-8").read()
    m = re.search(r"SHARDED_SOURCES: ReadonlySet<string> = new Set\(\[([^\]]*)\]\)", util)
    assert m, "util.ts SHARDED_SOURCES not found"
    assert tuple(sorted(re.findall(r'"([^"]+)"', m.group(1)))) == oc.SHARD_SOURCES == ("noaa",)
