"""tools/selfhost/reconcile_catalogue.py - the B24 catalogue reconcile tool (T0 plan (b2), rulings k and l, the
coordinator's rulings of 2026-11-28 and the design review R2249; design F:/work/econ_finish/b24_reconcile/DESIGN.md
section 6).

Every test plants tiny SQLite catalogues in pytest's tmp folder: a LOCAL catalogue with the real schema
(core.catalog.SCHEMA + the real fts5 table) and a FAKE D1 (two SQLite files with the SAME typed DDL, answering in
core.d1_remote.query's shape with a meta.rows_read, every value passed through JSON with JS-number semantics as the
D1 REST answer is - review M3). No test reads the real catalogue, D1, R2 or a .env. The expected classes are
written by hand (ORACLE), never derived from the tool. A GATED source is planted in EVERY table and every output
is grepped for it, with a positive control.
"""
import json
import os
import shutil
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
import reconcile_catalogue as rc  # noqa: E402
from core import catalog as corecat  # noqa: E402 - a test may import the library writer (tests/ is not walked)
from core import catalog_path as cp  # noqa: E402
from core import cutover  # noqa: E402

SERIES_COLS = ["series_id", "source_id", "title", "frequency", "unit", "geography", "category", "license_id",
               "start_date", "end_date", "last_updated", "metadata"]
SOURCE_COLS = ["source_id", "name", "homepage", "license_id", "attribution", "terms_url"]
LICENSE_COLS = ["license_id", "name", "reservable", "commercial_ok", "attribution_required", "no_modify", "url"]
GATE = {"gatedsrc"}
SECRET = "zzsecretzz"                  # appears only in gated rows: any output byte holding it is a leak


# ---- fixtures ---------------------------------------------------------------------------------------------------

def srow(sid, **kw):
    r = {c: None for c in SERIES_COLS}
    r.update(series_id=sid, source_id=sid.split(":", 1)[0], title="T " + sid, frequency="A", unit="u",
             geography="World", category="c", license_id="lic-open", start_date="2000-12-31", end_date="2020-12-31",
             metadata='{"k":1}')
    r.update(kw)
    return r


def src(s, **kw):
    r = {"source_id": s, "name": s.upper(), "homepage": "h", "license_id": "lic-open", "attribution": "a",
         "terms_url": "t"}
    r.update(kw)
    return r


def lic(lid, **kw):
    r = {"license_id": lid, "name": lid.upper(), "reservable": 1, "commercial_ok": 1, "attribution_required": 0,
         "no_modify": 0, "url": "u"}
    r.update(kw)
    return r


def _con(p):
    con = sqlite3.connect(p)
    con.execute("PRAGMA synchronous=OFF")                         # planted files: no fsync (the disk is slow)
    return con


def _insert(con, table, cols, rows):
    for r in rows:
        cs = [c for c in cols if c in r]
        con.execute(f"INSERT INTO {table} ({','.join(cs)}) VALUES ({','.join('?' * len(cs))})", [r[c] for c in cs])


def make_local(path, series, sources=None, licences=None, auto_parents=True):
    os.makedirs(os.path.dirname(str(path)), exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("PRAGMA synchronous=OFF")
    con.executescript(corecat.SCHEMA)
    corecat.init(con)
    _insert(con, "series", SERIES_COLS, series)
    if sources is None and auto_parents:
        sources = [src(s) for s in sorted({r["source_id"] for r in series})]
    if licences is None and auto_parents:
        licences = [lic(x) for x in sorted({r.get("license_id") for r in series if r.get("license_id")})]
    _insert(con, "source", SOURCE_COLS, sources or [])
    _insert(con, "license", LICENSE_COLS, licences or [])
    con.commit()
    corecat.rebuild_fts(con, True)
    con.close()
    return str(path)


def js(v):
    """A value as the D1 REST answer carries it: JSON from a JS runtime (an integral float is an integer; an integer
    beyond 2^53 loses precision)."""
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return v
    if isinstance(v, int):
        return int(float(v)) if abs(v) > 2 ** 53 else v
    if isinstance(v, float) and v.is_integer() and abs(v) < 1e21:
        return int(v)
    return v


class FakeD1:
    """Two fake D1 databases with the local typed DDL. __call__ has core.d1_remote.query's signature and shape."""

    def __init__(self, tmp, e=1, series_ddl=None):
        os.makedirs(tmp, exist_ok=True)
        self.paths = {db: os.path.join(str(tmp), f"fake_{db}.sqlite") for db in rc.DATABASES}
        self.e, self.calls, self.shuffle, self.drop_meta, self.raise_with = e, [], False, False, None
        self.inject = None
        for p in self.paths.values():
            con = _con(p)
            con.execute("PRAGMA synchronous=OFF")
            con.executescript(corecat.SCHEMA.replace("CREATE TABLE IF NOT EXISTS series (", "CREATE TABLE series (")
                              if series_ddl is None else series_ddl)
            con.execute("CREATE TABLE source_counts (source_id TEXT PRIMARY KEY, n INTEGER NOT NULL)")
            con.execute("CREATE TABLE unit_state (unit_id TEXT PRIMARY KEY, v TEXT)")
            con.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
            con.commit()
            con.close()

    def add(self, db, table, rows):
        con = _con(self.paths[db])
        cols = {"series": SERIES_COLS, "source": SOURCE_COLS, "license": LICENSE_COLS}[table]
        names = [r[1] for r in con.execute(f"PRAGMA table_info({table})")]
        _insert(con, table, [c for c in cols if c in names], rows)
        if table == "series":
            for r in rows:
                con.execute("INSERT INTO series_fts (series_id, title, geography) VALUES (?,?,?)",
                            (r["series_id"], r.get("title"), r.get("geography")))
        con.commit()
        con.close()

    def raw(self, db, sql, params=()):
        con = _con(self.paths[db])
        con.execute(sql, params)
        con.commit()
        con.close()

    def finish(self):
        for p in self.paths.values():
            con = _con(p)
            con.execute("DELETE FROM source_counts")
            con.execute("INSERT INTO source_counts SELECT source_id, COUNT(*) FROM series GROUP BY source_id")
            con.commit()
            con.close()

    def __call__(self, db, sql, params):
        self.calls.append((db, sql, list(params)))
        if self.raise_with and "FROM series WHERE" in sql:
            raise RuntimeError(self.raise_with)
        con = _con(self.paths[db])
        try:
            cur = con.execute(sql, params)
            names = [d[0] for d in cur.description] if cur.description else []
            rows = [{k: js(v) for k, v in zip(names, r)} for r in cur]
        finally:
            con.close()
        rows = json.loads(json.dumps(rows))
        if self.inject:
            rows = self.inject(sql, rows)
        if self.shuffle and "LIMIT" in sql and len(rows) > 1:
            rows = rows[::-1]
        meta = {} if self.drop_meta else {"rows_read": len(rows) + (self.e if "LIMIT" in sql else 0)}
        return {"results": rows, "meta": meta}


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(rc, "OWN_SYNC", "OFF")                    # the planted files need no fsync
    monkeypatch.setattr(rc, "OUT_ROOTS", (str(tmp_path),))
    monkeypatch.setattr(rc, "F_ANCHOR", str(tmp_path))
    monkeypatch.setattr(rc, "ECON_LIVE", str(tmp_path / "econ_live"))
    monkeypatch.setattr(rc, "SERVED_INDEX", str(tmp_path / "econ_live" / "blobs" / "index.db"))
    monkeypatch.setattr(rc, "load_gate", lambda: set(GATE))
    monkeypatch.setattr(rc, "gate_sha", lambda: "gate-sha-1")
    monkeypatch.setattr(rc, "load_run_locations", lambda: ({"localsrc": "local", "alpha": None}, "reg-sha-1"))
    fake = FakeD1(tmp_path / "fake")
    monkeypatch.setattr(rc, "D1_QUERY", fake)
    return tmp_path, fake


def set_t0():
    os.makedirs(os.path.dirname(cutover.FLAG_PATH), exist_ok=True)
    open(cutover.FLAG_PATH, "w").close()


def allow_for(local, snaps=None, c3n=("sec_edgar",)):
    a = rc._allow_from_catalogue(local, set(GATE), set(c3n))
    for p in (snaps or {}).values():
        a.licence_refs |= rc.d1_licence_refs(p)
    a.licence_refs = {x for x in a.licence_refs if not a.names_gated(x)}
    return a


def snapshot(fake, local, out, page_rows=3, probe_e=None, budget=10 ** 6, c3n=("sec_edgar",)):
    os.makedirs(out, exist_ok=True)
    b = rc.Budget(budget, os.path.join(str(out), "budget.sqlite"))
    paths = rc.snapshot_d1(b, str(out), page_rows, allow_for(local, None, c3n), None if probe_e is None else
                           {"databases": {db: {"e": probe_e} for db in rc.DATABASES}})
    return paths, b


def run_classify(local, snaps, out, *, c1=(), c3n=("sec_edgar",), c7=None, deletes=(), runloc=None, served=None,
                 manifest=None):
    os.makedirs(out, exist_ok=True)
    return rc.classify(catalogue=local, snapshots=snaps, out_dir=str(out), allow=allow_for(local, snaps, c3n),
                       runloc=runloc if runloc is not None else {"localsrc": "local"},
                       shard={"noaa": rc.CLIMATE}, c1=set(c1), c3n=set(c3n), c7=c7 or rc.C7(), deletes=set(deletes),
                       manifest=manifest, served=served)


def q(out, sql, *params):
    con = sqlite3.connect(os.path.join(str(out), "classify.sqlite"))
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def cells(out):
    return {(t, k, c): (cls, dec, sub) for t, k, c, cls, dec, sub in
            q(out, "SELECT tbl, key, col, cls, decision, sublabels FROM cells")}


def rows(out):
    return {(t, k, cls): (dec, json.loads(fl or "{}")) for t, k, cls, dec, fl in
            q(out, "SELECT tbl, key, cls, decision, flags FROM rows")}


def agg(out, metric):
    return {(t, s, c): n for t, s, c, n in q(out, "SELECT tbl, source, col, n FROM agg WHERE metric=?", metric)}


# ---- the planted catalogue P1 -------------------------------------------------------------------------------

LIC_LOCAL = [lic("lic-open", attribution_required=1), lic("lic-closed", reservable=0, commercial_ok=0,
                                                          attribution_required=1, no_modify=1),
             lic("lic-a"), lic("lic-b", reservable=0), lic("lic-c"), lic("lic-d"),
             lic("lic-g", name=f"Terms of gatedsrc {SECRET}"),            # referenced only by the gated source
             lic("lic-n", name="licence of the gatedsrc programme")]      # referenced by an allowed row, names gate


def plant_p1(tmp, fake):
    L = [
        srow("alpha:01", title="GDP"), srow("alpha:02", metadata='{"a":true}', unit=None), srow("alpha:03", frequency="A"),
        srow("alpha:04", title="a\nb"), srow("alpha:05"), srow("alpha:06"), srow("alpha:11", license_id="lic-open"),
        srow("alpha:12", metadata='{"a":1,"b":2}'), srow("alpha:13", title="mentions gatedsrc here"),
        srow("alpha:14", license_id="lic-a"), srow("alpha:15", license_id="lic-b"), srow("alpha:16", license_id="lic-c"),
        srow("alpha:17", license_id="lic-d"), srow("alpha:18", license_id="lic-n"),
        srow("localsrc:01", title="local title"), srow("sec_edgar:A", end_date="2026-06-30"),
        srow("noaa:01", geography="Here"), srow("noaa:02"), srow("noaa:03", unit="m"),
        srow("c1src:01", unit="x"), srow("c1src:02", unit="x"), srow("gapsrc:01", unit="x"),
        srow("c5src:01", unit="u1"), srow("c5src:02", unit="u2"),
        srow("gatedsrc:01", title=f"{SECRET} local", license_id="lic-g"), srow("gatedsrc:03", title=SECRET),
    ]
    D = [
        srow("alpha:01", title="GDP "), srow("alpha:02", metadata='{"a":1}', unit="kg"), srow("alpha:03", frequency="Q"),
        srow("alpha:04", title="a b"), srow("alpha:07", title="removed upstream"),
        srow("alpha:08", title="removed locally"), srow("alpha:11", license_id="lic-closed"),
        srow("alpha:12", metadata='{"b":2,"a":1}'), srow("alpha:13", title="mentions gatedsrc there"),
        srow("alpha:14", license_id="lic-a"), srow("alpha:15", license_id="lic-b"), srow("alpha:16", license_id="lic-c"),
        srow("alpha:17", license_id="lic-d"), srow("alpha:18", license_id="lic-n"),
        srow("alpha:19", title=f"d1 only, names gatedsrc {SECRET}"),
        srow("localsrc:01", title="d1 title"), srow("sec_edgar:A", end_date="2026-09-30"), srow("sec_edgar:B"),
        srow("c1src:01", unit="y"), srow("c1src:02", unit=None), srow("gapsrc:01", unit="y"),
        srow("c5src:01", unit=None), srow("c5src:02", unit=None),
        srow("gatedsrc:01", title=f"{SECRET} d1", license_id="lic-g"), srow("gatedsrc:02", title=SECRET),
        srow("ghostsrc:01", title=f"not hosted locally {SECRET}"),     # H2: a source outside the allow-list
    ]
    fake.add(rc.PRIMARY, "series", D)
    fake.add(rc.CLIMATE, "series", [srow("noaa:01", geography="There"), srow("noaa:03", unit="m"),
                                    srow("gatedsrc:09", title=SECRET)])
    fake.add(rc.PRIMARY, "series", [srow("noaa:02"), srow("noaa:03", unit="WRONG")])   # misplaced, duplicate
    sources = [src(s) for s in ("alpha", "localsrc", "sec_edgar", "noaa", "c1src", "gapsrc", "c5src")]
    sources_local = sources + [src("gatedsrc", name=f"Gated {SECRET}")]
    src_d1 = [dict(r) for r in sources]
    src_d1[0]["name"] = "ALPHA (D1)"
    src_d1 += [src("ghost"), src("gatedsrc", name=f"Gated D1 {SECRET}")]
    fake.add(rc.PRIMARY, "source", src_d1)
    d = {r["license_id"]: dict(r) for r in LIC_LOCAL}
    d["lic-a"]["reservable"] = 0              # D1 tighter  -> HELD
    d["lic-b"]["reservable"] = 1              # D1 looser   -> HELD
    d["lic-c"]["attribution_required"] = 1    # D1 tighter obligation -> HELD
    d["lic-d"]["reservable"] = "yes"          # undecidable -> HELD
    d["lic-g"]["name"] = f"Terms of gatedsrc D1 {SECRET}"
    d["lic-n"]["url"] = "changed"
    d["lic-x"] = lic("lic-x", name=f"orphan of gatedsrc {SECRET}")   # D1 only, referenced by nothing allowed
    fake.add(rc.PRIMARY, "license", list(d.values()))
    fake.finish()
    return make_local(tmp / "local.db", L, sources_local, LIC_LOCAL)


ORACLE_CELLS = {
    ("series", "alpha:01", "title"): ("DEFAULT", "local"),
    ("series", "alpha:02", "metadata"): ("DEFAULT", "local"),
    ("series", "alpha:02", "unit"): ("LOCAL-NULL", "local"),
    ("series", "alpha:03", "frequency"): ("C7", "local"),
    ("series", "alpha:04", "title"): ("DEFAULT", "local"),
    ("series", "alpha:11", "license_id"): ("LIC-HELD", "held"),
    ("series", "localsrc:01", "title"): ("C2", "local"),
    ("series", "sec_edgar:A", "end_date"): ("C6", "verify"),
    ("series", "noaa:01", "geography"): ("DEFAULT", "local"),
    ("series", "c1src:01", "unit"): ("C1", "d1"),
    ("series", "c1src:02", "unit"): ("C1", "d1"),
    ("series", "gapsrc:01", "unit"): ("C1-GAP", "local"),
    ("series", "c5src:01", "unit"): ("C5", "local"),
    ("series", "c5src:02", "unit"): ("C5", "local"),
    ("license", "lic-a", "reservable"): ("LIC-HELD", "held"),
    ("license", "lic-b", "reservable"): ("LIC-HELD", "held"),
    ("license", "lic-c", "attribution_required"): ("LIC-HELD", "held"),
    ("license", "lic-d", "reservable"): ("LIC-HELD", "held"),
    ("source", "alpha", "name"): ("DEFAULT", "local"),
}
ORACLE_ROWS = {
    ("series", "alpha:05", "C4"), ("series", "alpha:06", "C4-DEL"), ("series", "alpha:07", "C3"),
    ("series", "alpha:08", "C3"), ("series", "sec_edgar:B", "C3-TAKEN"), ("series", "noaa:02", "C4"),
    ("series", "noaa:02", "MISPLACED"), ("series", "noaa:03", "DUPLICATE"),
}


def p1_c7():
    c7 = rc.C7()
    c7.add_ids(["alpha:03", "alpha:08"], "queue")
    c7.gaps["gapsrc"] = [{"writer": "tools/x.py", "utc": "2026-09-01T00:00:00Z", "booking": "NUMBERS.md:1",
                          "sources": ["gapsrc"]}]
    return c7


C1_P1 = {("c1src", "series", "unit"), ("gapsrc", "series", "unit"), ("sec_edgar", "series", "end_date")}


def p1_run(tmp, fake, served=None):
    local = plant_p1(tmp, fake)
    snaps, _ = snapshot(fake, local, tmp / "snap")
    out = tmp / "cls"
    res = run_classify(local, snaps, out, c1=C1_P1, c7=p1_c7(), deletes={"alpha:06"}, served=served)
    return local, snaps, out, res


# ---- T1: the normalisation pairs (DESIGN.md section 2, restated for the JSON road, review M3) -------------------

N_PAIRS = [
    ("N1", "numeric", 1, 1, "raw"), ("N2", "numeric", "1", 1, "norm"), ("N3", "numeric", 1, 1.0, "norm"),
    ("N4", "numeric", "1.0", 1, "norm"), ("N5", "numeric", 0, 1, "diff"), ("N6", "numeric", " 1", 1, "diff"),
    ("N7", "numeric", "yes", "yes", "raw"), ("N8", "numeric", None, 0, "diff"), ("N9", "numeric", None, None, "raw"),
    ("N10", "numeric", "12345678901234567890", 12345678901234567890, "norm"),
    ("N11", "text", "GDP", "GDP ", "diff"), ("N12", "text", "gdp", "GDP", "diff"), ("N13", "text", "", None, "diff"),
    ("N14", "text", "2024", 2024, "norm"), ("N15", "text", "1.0", 1.0, "norm"), ("N16", "text", "1", 1.0, "diff"),
    ("N17", "text", "Caf\u00e9", "Cafe\u0301", "diff"),
    ("N18", "json", '{"a":1,"b":2}', '{"b":2,"a":1}', "norm"), ("N19", "json", '{"a": 1}', '{"a":1}', "norm"),
    ("N20", "json", '{"a":1}', '{"a":1.0}', "norm"), ("N21", "json", '{"a":true}', '{"a":1}', "diff"),
    ("N22", "json", '{"a":[1,2]}', '{"a":[2,1]}', "diff"), ("N23", "json", '{"a":"1"}', '{"a":1}', "diff"),
    ("N24", "json", "{bad", "{bad", "raw"), ("N25", "json", "{bad", '{"bad":1}', "diff"),
    ("N26", "json", '{"a":1,"a":2}', '{"a":2}', "diff"), ("N27", "json", '{"a":NaN}', '{"a":NaN}', "raw"),
    ("N28", "json", None, "{}", "diff"), ("N29", "json", '{"t":{"fr":"x","de":"y"}}', '{"t":{"de":"y","fr":"x"}}', "norm"),
    ("N30", "text", "a\nb", "a b", "diff"), ("N31", "json", '{"a":true}', '{"a":true}', "raw"),
    ("N32", "json", '{"a":12345678901234567890}', '{"a":12345678901234567890.0}', "norm"),
]


@pytest.mark.parametrize("name,kind,a,b,want", N_PAIRS, ids=[p[0] for p in N_PAIRS])
def test_t1_normalisation_pairs(name, kind, a, b, want):
    # N14-N16 describe a TYPELESS D1 column only; with D1's TEXT columns a number cannot arrive (the T1 road test)
    assert rc.compare(kind, a, b) == want
    assert rc.compare(kind, b, a) == want


def test_t1_column_kinds_and_unsafe_numbers():
    assert rc.column_kind("license", "reservable", "INTEGER", None) == "numeric"
    assert rc.column_kind("series", "title", "TEXT", "TEXT") == "text"
    assert rc.column_kind("series", "metadata", "TEXT", "TEXT") == "json"
    assert rc.column_kind("x", "y", "TEXT", "REAL") == "numeric"
    assert rc.affinity("VARCHAR(5)") == "TEXT" and rc.affinity("") == "BLOB" and rc.affinity("DECIMAL") == "NUMERIC"
    assert rc.unsafe_number(2 ** 53 + 1) and not rc.unsafe_number(2 ** 53) and not rc.unsafe_number(True)


def test_t1_the_json_road_and_a_text_column_anomaly(env, monkeypatch):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:1", start_date="2024")])
    fake.add(rc.PRIMARY, "license", [lic("lic-open", reservable=1.0, commercial_ok="1")])   # INTEGER affinity
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1", start_date="2024")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    run_classify(local, snaps, tmp / "out")
    assert cells(tmp / "out") == {}                                    # 1.0 arrives as 1; '1' stored as 1
    assert agg(tmp / "out", "equal_raw")[("license", None, "reservable")] == 1
    # a number in a column D1 declares TEXT is impossible under TEXT affinity: a STOP, never normalised
    fake.inject = lambda sql, rows: [{**r, "start_date": 2024} if "start_date" in r else r for r in rows]
    with pytest.raises(rc.Stop, match="TEXT column"):
        snapshot(fake, local, tmp / "snap2")


def test_t1_an_integer_beyond_2_53_is_roundtrip_not_equal(env):
    tmp, fake = env
    big = 2 ** 60 + 1
    fake.add(rc.PRIMARY, "license", [lic("lic-open", reservable=big)])
    fake.add(rc.PRIMARY, "series", [srow("alpha:1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1")], licences=[lic("lic-open", reservable=big)])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    run_classify(local, snaps, tmp / "out")
    got = cells(tmp / "out")[("license", "lic-open", "reservable")]
    assert got[0] == "LIC-HELD"                                        # licence first; the number never decides


def test_t1_planted_pairs_end_to_end(env):
    tmp, fake = env
    _l, _s, out, _r = p1_run(tmp, fake)
    assert agg(out, "equal_norm").get(("series", "alpha", "metadata")) == 1      # N18: key order
    assert ("series", "alpha:12", "metadata") not in cells(out)


# ---- T2-T9: the classes, against the hand-written oracle --------------------------------------------------------

def test_t2_to_t9_every_planted_cell_and_row_has_its_oracle_class(env):
    tmp, fake = env
    _l, _s, out, res = p1_run(tmp, fake)
    got = {k: v[:2] for k, v in cells(out).items()}
    assert got == ORACLE_CELLS, {k: (got.get(k), ORACLE_CELLS.get(k)) for k in set(got) | set(ORACLE_CELLS)
                                 if got.get(k) != ORACLE_CELLS.get(k)}
    got_rows = set(rows(out))
    assert got_rows == ORACLE_ROWS, (got_rows ^ ORACLE_ROWS)
    w = res["plan"]
    assert sorted(x[1] for x in w["cells"]) == ["c1src:01", "c1src:02"]           # licence cells are never written
    assert [x[1] for x in w["inserts"]] == ["sec_edgar:B"] and [x[1] for x in w["deletes"]] == ["alpha:06"]
    st = res["stats"]
    assert st["c6_cells"] == 1 and st["c6_rows"] == 1 and st["licence_held"] == 5 and st["withheld_values"] is True


def test_t2_c7_from_queue_receipt_source_cell_and_hand_list(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:1", unit="d"), srow("alpha:2", unit="d"), srow("beta:1", unit="d"),
                                    srow("gamma:1", unit="d"), srow("alpha:3", unit="d", title="x2"),
                                    srow("alpha:9", title="only in D1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1"), srow("alpha:2"), srow("beta:1"), srow("gamma:1"),
                                      srow("alpha:3", title="x1")])
    rec = tmp / "receipts"
    rec.mkdir()
    (rec / "sync_rows_alpha.json").write_text(json.dumps({"source": "alpha", "missing": [], "differing": ["alpha:2"],
                                                          "before_after": {}, "null_over_local": {}}))
    (rec / "x.txt").write_text("a source-grain row reads no ids\n")
    runs = tmp / "runs.tsv"
    runs.write_text("run_key\twriter\tutc\tsources\treceipt\tgrain\tbooking\n"
                    "r1\ttools/sync_source_rows_d1_to_local.py\t2026-11-17T10:00:00Z\talpha\tsync_rows_alpha.json\tids\tN:5\n"
                    "r2\ttools/fix_statcan_frequency.py\t2026-10-18T07:23:48Z\tbeta\tx.txt\tsource\tF:9\n"
                    "r3\ttools/apply_series_names.py\t2026-08-20T00:00:00Z\tgamma\t\tnone\tN:7\n")
    hand = tmp / "hand.tsv"
    hand.write_text("key\tgrain\treason\tevidence\nalpha:1\tid\tplanted\ttest\nalpha:3|unit\tcell\tplanted\ttest\n")
    c7 = rc.C7()
    c7.add_ids(["alpha:9"], "queue")
    rc.load_c7_runs(c7, str(runs), str(rec), os.path.join(rc.LISTS_DIR, "writers.tsv"))
    rc.load_c7_hand(c7, str(hand))
    snaps, _ = snapshot(fake, local, tmp / "snap")
    run_classify(local, snaps, tmp / "out", c7=c7)
    got = {k: v[:2] for k, v in cells(tmp / "out").items()}
    assert got[("series", "alpha:1", "unit")] == ("C7", "local")       # hand list, row
    assert got[("series", "alpha:2", "unit")] == ("C7", "local")       # receipt ids
    assert got[("series", "beta:1", "unit")] == ("C7", "local")        # source-grain receipt
    assert got[("series", "gamma:1", "unit")] == ("DEFAULT", "local")  # before the cutover: no C7, no gap
    assert got[("series", "alpha:3", "unit")] == ("C7", "local")       # hand list, cell
    assert got[("series", "alpha:3", "title")] == ("DEFAULT", "local") # the cell entry covers one column only
    assert "gamma" not in c7.gaps
    assert rows(tmp / "out")[("series", "alpha:9", "C3")][1]["local_removal_evidence"] == ["queue"]


def test_t2_run_list_errors_and_gaps(env, tmp_path):
    runs = tmp_path / "runs.tsv"
    runs.write_text("run_key\twriter\tutc\tsources\treceipt\tgrain\tbooking\n"
                    "r1\ttools/apply_series_names.py\t2026-09-02T00:00:00Z\talpha\t\tnone\tN:7\n")
    c7 = rc.C7()
    rc.load_c7_runs(c7, str(runs), None, os.path.join(rc.LISTS_DIR, "writers.tsv"))
    assert list(c7.gaps) == ["alpha"]
    (tmp_path / "rec").mkdir()
    for receipt, match in (("nope.json", "missing"), ("../../etc.json", "outside")):
        runs.write_text("run_key\twriter\tutc\tsources\treceipt\tgrain\tbooking\n"
                        f"r1\ttools/sync_source_rows_d1_to_local.py\t2026-09-02T00:00:00Z\talpha\t{receipt}\tids\tx\n")
        with pytest.raises(rc.Refused, match=match):
            rc.load_c7_runs(rc.C7(), str(runs), str(tmp_path / "rec"), os.path.join(rc.LISTS_DIR, "writers.tsv"))


def test_t2_the_queue_is_checked_against_its_s4c_receipt(env, tmp_path):
    qf = tmp_path / "pending.txt"
    qf.write_text("a:1\na:1\nb:2\n")
    good = {"sha256": rc.file_sha256(str(qf)), "size": qf.stat().st_size, "lines": 3}
    r = tmp_path / "s4c.json"
    r.write_text(json.dumps(good))
    c7 = rc.C7()
    rc.load_queue(c7, str(qf), str(r))
    assert set(c7.ids) == {"a:1", "b:2"}
    r.write_text(json.dumps({**good, "lines": 4}))
    with pytest.raises(rc.Refused, match="lines"):
        rc.load_queue(rc.C7(), str(qf), str(r))


def test_t3_c5_absent_column_all_null_and_some_null(env, tmp_path, monkeypatch):
    ddl = corecat.SCHEMA.replace("CREATE TABLE IF NOT EXISTS series (", "CREATE TABLE series (").replace(
        "  last_updated TEXT, metadata TEXT);", "  metadata TEXT);")
    assert "last_updated" not in ddl.split("CREATE TABLE series")[1].split(";")[0]
    fake = FakeD1(tmp_path / "f2", series_ddl=ddl)
    monkeypatch.setattr(rc, "D1_QUERY", fake)
    fake.add(rc.PRIMARY, "series", [srow("s:1", unit=None), srow("s:2", unit=None), srow("t:1", unit=None),
                                    srow("t:2", unit="v")])
    fake.finish()
    local = make_local(tmp_path / "l.db", [srow("s:1", unit="a", last_updated="2026-01-01"), srow("s:2", unit="b"),
                                           srow("t:1", unit="a"), srow("t:2", unit="v")])
    snaps, _ = snapshot(fake, local, tmp_path / "snap")
    run_classify(local, snaps, tmp_path / "out")
    got = {k: v[:2] for k, v in cells(tmp_path / "out").items()}
    assert got[("series", "s:1", "unit")] == ("C5", "local") and got[("series", "s:2", "unit")] == ("C5", "local")
    assert got[("series", "t:1", "unit")] == ("DEFAULT", "local")
    assert agg(tmp_path / "out", "c5_absent_local_nonnull")[("series", "s", "last_updated")] == 1


def test_t8_class_order(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("k:1", unit=None), srow("k:2", unit="b"), srow("sec_edgar:1", unit="b"),
                                    srow("k:3", unit="b")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("k:1", unit="a"), srow("k:2", unit="a"), srow("sec_edgar:1", unit="a"),
                                      srow("k:3", unit=None)])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    c7 = rc.C7()
    c7.add_ids(["k:1"], "queue")
    run_classify(local, snaps, tmp / "out", c7=c7, runloc={}, c1={("k", "series", "unit"), ("sec_edgar", "series", "unit")})
    got = {k: v[:2] for k, v in cells(tmp / "out").items()}
    assert got[("series", "k:1", "unit")] == ("C7", "local")
    assert got[("series", "k:2", "unit")] == ("C1", "d1")
    assert got[("series", "sec_edgar:1", "unit")] == ("C6", "verify")      # C6 before C1
    assert got[("series", "k:3", "unit")] == ("C1", "d1")                  # C1 before LOCAL-NULL
    run_classify(local, snaps, tmp / "out2", runloc={"k": "local"}, c1={("k", "series", "unit")})
    got2 = {k: v[:2] for k, v in cells(tmp / "out2").items()}
    assert got2[("series", "k:2", "unit")] == ("C2", "local") and got2[("series", "k:3", "unit")] == ("C2", "local")


def test_t8_c7_beats_c5(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("z:1", unit=None)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("z:1", unit="a")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    c7 = rc.C7()
    c7.add_ids(["z:1"], "hand list")
    run_classify(local, snaps, tmp / "out", c7=c7)
    assert cells(tmp / "out")[("series", "z:1", "unit")][:2] == ("C7", "local")
    run_classify(local, snaps, tmp / "out2")
    assert cells(tmp / "out2")[("series", "z:1", "unit")][:2] == ("C5", "local")


def test_t7_title_clean_only_is_a_label_not_a_decision(env):
    tmp, fake = env
    _l, _s, out, _r = p1_run(tmp, fake)
    cls, dec, sub = cells(out)[("series", "alpha:04", "title")]
    assert (cls, dec) == ("DEFAULT", "local") and "TITLE-CLEAN-ONLY" in sub


def test_t9_licence_cells_are_held_with_direction_and_audit_line(env):
    tmp, fake = env
    local, snaps, out, res = p1_run(tmp, fake)
    s = rc.summarise(str(out), res, p1_c7(), {"gate_sha256": "g"}, None, allow_for(local, snaps))
    dirs = {(x["key"], x["column"]): x["direction"] for x in s["licence_held_rows"]}
    assert dirs == {("lic-a", "reservable"): "D1 tighter", ("lic-b", "reservable"): "local tighter",
                    ("lic-c", "attribution_required"): "D1 tighter", ("lic-d", "reservable"): "undecidable"}
    assert all(x["audit"] for x in s["licence_held_rows"])
    held = {(x["table"], x["source"], x["column"]): x for x in s["licence_held"]}
    assert held[("series", "alpha", "license_id")]["direction"] == "D1 tighter"
    assert held[("series", "alpha", "license_id")]["audit"].startswith(("DATABASE_LICENSES_VERBATIM.md:", "no audit"))
    assert res["plan"]["cells"] and all(c[0] != "license" and c[6] != "LIC-HELD" for c in res["plan"]["cells"])


def test_t9_served_object_flag(env):
    tmp, fake = env
    idx = tmp / "econ_live" / "blobs"
    idx.mkdir(parents=True)
    con = sqlite3.connect(idx / "index.db")
    con.execute("CREATE TABLE blobs (key TEXT PRIMARY KEY, size INT)")
    con.execute("INSERT INTO blobs VALUES ('series/alpha%3A07.csv', 1)")
    con.commit()
    con.close()
    _l, _s, out, _r = p1_run(tmp, fake, served=rc.ServedIndex(str(idx / "index.db")))
    r = rows(out)
    assert r[("series", "alpha:07", "C3")][1]["served_object"] is True
    assert r[("series", "alpha:08", "C3")][1]["served_object"] is False


def test_t9_served_key_matches_the_derive_layout():
    try:
        sys.path.insert(0, os.path.join(ROOT, "tools"))
        import derive_csv_bulk  # noqa: PLC0415
    except Exception:                                              # noqa: BLE001 - optional deps (duckdb)
        pytest.skip("tools/derive_csv_bulk.py is not importable here")
    import urllib.parse  # noqa: PLC0415
    for sid in ("wid:WID:a b:p0p100", "fhfa:x!y'(z)*", "alpha:01"):
        s, key = sid.split(":", 1)
        assert "series/" + urllib.parse.quote(sid, safe="") + ".csv" == derive_csv_bulk.csv_key("series", s, key)


def test_t9_m9_an_item17_id_on_both_sides_is_a_failed_check(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1"), srow("a:2")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:1"), srow("a:2")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    res = run_classify(local, snaps, tmp / "out", deletes={"a:1"})
    assert res["stats"]["del_both"] == 1 and ("series", "a:1", "C4-DEL-BOTH") in rows(tmp / "out")
    assert not res["plan"]["deletes"]


def test_t9_m8_c3_taken_without_parent_rows_is_refused_before_any_write(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("sec_edgar:N", license_id="lic-new")])
    fake.add(rc.PRIMARY, "license", [lic("lic-new")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("sec_edgar:A")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    res = run_classify(local, snaps, tmp / "out")
    assert res["stats"]["c3_parent_missing"] == 1
    plan = {"counts": {"c3_parent_missing": 1}, "writes": res["plan"]}
    con = cp.connect_path(local, write=True, isolation_level=None)
    with pytest.raises(rc.Refused, match="parent"):
        rc.apply_writes(con, plan, str(tmp), "t")
    con.close()
    assert not os.path.exists(tmp / "t_before.json")              # refused before the plan file and any write


# ---- S4d / B32 (T0 plan rev 6.5): local has no unreferenced licence rows; D1 may still have them --------------

def _plant_s4d(tmp, fake):
    """The build after S4d: lic-open and lic-a only. alpha:3 keeps a DANGLING reference to lic-dangle. D1 still has
    lic-z (only a D1 value of alpha:2's held license_id cell references it), lic-dangle, lic-take (referenced by
    the C3-TAKEN row sec_edgar:N), lic-old (referenced only by the not-taken D1-only row alpha:4) and lic-q
    (referenced by nothing: never admitted)."""
    fake.add(rc.PRIMARY, "series", [srow("alpha:1"), srow("alpha:2", license_id="lic-z"),
                                    srow("alpha:3", license_id="lic-dangle"), srow("alpha:4", license_id="lic-old"),
                                    srow("alpha:5", license_id="lic-a"), srow("sec_edgar:N", license_id="lic-take")])
    fake.add(rc.PRIMARY, "source", [src("alpha"), src("sec_edgar")])
    fake.add(rc.PRIMARY, "license", [lic(x) for x in ("lic-open", "lic-a", "lic-z", "lic-dangle", "lic-take",
                                                      "lic-old", "lic-q")])
    fake.finish()
    return make_local(tmp / "l.db", [srow("alpha:1"), srow("alpha:2", license_id="lic-a"),
                                     srow("alpha:3", license_id="lic-dangle"), srow("alpha:5"), srow("sec_edgar:A")],
                      [src("alpha"), src("sec_edgar")], [lic("lic-open"), lic("lic-a")])


def test_s4d_a_d1_only_licence_row_is_never_inserted_and_listed_only_when_referenced_after(env):
    tmp, fake = env
    local = _plant_s4d(tmp, fake)
    snaps, _ = snapshot(fake, local, tmp / "snap")
    out = tmp / "out"
    res = run_classify(local, snaps, out)
    lic_rows = {(k, c): dec for (t, k, c), (dec, _fl) in rows(out).items() if t == "license"}
    # (1) listed in the HELD class only when a row that exists after the reconcile references it
    assert lic_rows == {("lic-dangle", "LIC-HELD-ROW"): "held", ("lic-take", "LIC-HELD-ROW"): "held"}, lic_rows
    assert res["stats"]["licence_rows_held"] == 2
    assert all(t == "series" for t, _k, _r in res["plan"]["inserts"])   # never inserted automatically
    assert ("series", "sec_edgar:N", "C3-TAKEN") in rows(out) and res["stats"]["c3_parent_missing"] == 1
    # (3) lic-z and lic-old: dropped, YES only - no row, no count
    assert res["stats"]["d1_only_licence_dropped"] is True
    assert not q(out, "SELECT 1 FROM rows WHERE tbl='license' AND key IN ('lic-z','lic-old','lic-q')")
    assert q(out, "SELECT DISTINCT metric FROM agg WHERE tbl='license' AND metric LIKE 'rows%'") == \
        [("rows_LIC-HELD-ROW",)]
    assert q(out, "SELECT SUM(n) FROM agg WHERE tbl='license' AND metric='rows_LIC-HELD-ROW'") == [(2,)]
    # (2) the held licence-reference cell whose D1 target is absent locally carries the refusal; a target that
    # exists locally does not (control)
    c = cells(out)
    assert c[("series", "alpha:2", "license_id")][0] == "LIC-HELD"
    assert "TAKE-D1-REFUSED:TARGET-LICENCE-ABSENT-LOCALLY" in c[("series", "alpha:2", "license_id")][2]
    assert c[("series", "alpha:5", "license_id")][0] == "LIC-HELD"
    assert "TAKE-D1-REFUSED" not in c[("series", "alpha:5", "license_id")][2]
    assert res["stats"]["licence_take_refused"] == 1
    s = rc.summarise(str(out), res, rc.C7(), {"gate_sha256": "g"}, None, allow_for(local, snaps),
                     rc.dropped_flags(snaps))
    assert [x["key"] for x in s["licence_rows_only_in_d1_held"]] == ["lic-dangle", "lic-take"]
    md = open(out / "summary.md", encoding="utf-8").read()
    assert "nothing references after the reconcile were dropped: YES" in md
    assert "a 'take D1' answer is REFUSED): 1" in md


def test_s4d_no_d1_only_licence_says_no(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:1")])
    fake.add(rc.PRIMARY, "license", [lic("lic-open"), lic("lic-q")])  # lic-q: referenced by nothing, never admitted
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    res = run_classify(local, snaps, tmp / "out")
    assert res["stats"]["d1_only_licence_dropped"] is False and res["stats"]["licence_rows_held"] == 0


def test_s4d_preflight_refuses_a_licence_row_or_a_licence_reference_write(env):
    tmp, _fake = env
    local = make_local(tmp / "l.db", [srow("alpha:1")])
    con = cp.connect_path(local, write=True, isolation_level=None)
    try:
        ok = {"counts": {}, "writes": {"cells": [], "inserts": [], "deletes": []}}
        rc.preflight_writes(con, ok, apply=False)                     # positive control: an empty plan passes
        for bad in ({"inserts": [["license", "lic-z", {"license_id": "lic-z"}]]},
                    {"deletes": [["license", "lic-open", {"license_id": "lic-open"}]]},
                    {"inserts": [["source", "newsrc", {"source_id": "newsrc"}]]}):
            plan = {"counts": {}, "writes": {"cells": [], "inserts": [], "deletes": [], **bad}}
            with pytest.raises(rc.Refused, match="licence or source row"):
                rc.preflight_writes(con, plan, apply=False)
        cell = {"counts": {}, "writes": {"cells": [["series", "alpha:1", "license_id", "text", "lic-open", "lic-z",
                                                    "C1"]], "inserts": [], "deletes": []}}
        with pytest.raises(rc.Refused, match="licence cell"):
            rc.preflight_writes(con, cell, apply=False)
    finally:
        con.close()


# ---- T10: the allow-list - nothing outside it in ANY output, a traceback included ----------------------------

def _grep_outputs(dirs, needles):
    hits, control = [], False
    for d in dirs:
        for base, _ds, fs in os.walk(d):
            for name in fs:
                b = open(os.path.join(base, name), "rb").read()
                for n in needles:
                    if n.encode() in b:
                        hits.append((name, n))
                control = control or b"alpha:01" in b
    return hits, control


def test_t10_no_output_holds_a_gated_or_unhosted_row(env, capsys):
    tmp, fake = env
    local, snaps, out, res = p1_run(tmp, fake)
    s = rc.summarise(str(out), res, p1_c7(), {"gate_sha256": "g"}, None, allow_for(local, snaps),
                     rc.dropped_flags(snaps))
    rc.write_json(str(out / "plan.json"), {"writes": res["plan"], "digests": res["digests"]})
    hits, control = _grep_outputs([out, tmp / "snap"], ["gatedsrc", SECRET, "ghostsrc", "ghost", "lic-g", "lic-x"])
    assert control, "the positive control (a non-gated id) was not found: the check would be blind"
    assert hits == [], hits
    assert s["outside_the_allow_list_dropped"] == {rc.PRIMARY: "YES", rc.CLIMATE: "YES"}
    assert "gatedsrc" not in capsys.readouterr().out
    # alpha:19 is D1-only and its title names the gate: the snapshot holds it redacted, and classify must neither
    # list it nor carry the redaction marker into a result or a plan (M42/M43: the text grep alone is blind here,
    # because the marker holds no gated text)
    assert [k for (_t, k, _c) in rows(out) if k == "alpha:19"] == []
    assert rows(out), "control: classify listed rows at all"
    marker, _ = _grep_outputs([out], [rc.WITHHELD.decode("ascii")])
    assert marker == [], marker
    snap_marker, _ = _grep_outputs([tmp / "snap"], [rc.WITHHELD.decode("ascii")])
    assert snap_marker, "positive control: the snapshot does hold the redaction marker"


def test_t10_a_d1_only_hosted_row_is_listed_and_an_unhosted_source_is_only_a_yes(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("fhfa:legacy1"), srow("otherhost:1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("fhfa:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    run_classify(local, snaps, tmp / "out")
    assert ("series", "fhfa:legacy1", "C3") in rows(tmp / "out")             # control: a hosted source is listed
    hits, _ = _grep_outputs([tmp / "out", tmp / "snap"], ["otherhost"])
    assert hits == [] and rc.dropped_flags(snaps)[rc.PRIMARY] == "YES"


def test_t10_stop_messages_and_tracebacks_hold_no_key(env, capsys):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"gatedsrc:{i}", title=SECRET) for i in range(5)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1")])
    fake.shuffle = True                                              # out of order -> STOP (positions only)
    code = rc.main(["read-d1", "--out", str(tmp / "r1"), "--catalogue", local, "--budget-rows", "100000",
                    "--page-rows", "3"])
    fake.shuffle = False
    fake.raise_with = f"D1 error mentioning gatedsrc:1 {SECRET}"      # an exception whose text holds a key
    code2 = rc.main(["read-d1", "--out", str(tmp / "r2"), "--catalogue", local, "--budget-rows", "100000",
                     "--page-rows", "3"])
    printed = capsys.readouterr().out
    assert code == 3 and code2 == 1
    assert "gatedsrc" not in printed and SECRET not in printed and "withheld" in printed
    hits, _ = _grep_outputs([tmp / "r1", tmp / "r2"], ["gatedsrc", SECRET])
    assert hits == []


def test_t10_the_page_cursor_is_the_last_allowed_key(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:1"), srow("alpha:2"), srow("gatedsrc:1", title=SECRET)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap", page_rows=3)
    con = sqlite3.connect(snaps[rc.PRIMARY])
    assert json.loads(con.execute("SELECT v FROM _state WHERE k='series_cursor'").fetchone()[0]) == "alpha:2"
    con.close()


# ---- T11: shards -------------------------------------------------------------------------------------------------

def test_t11_noaa_is_compared_against_the_climate_shard(env):
    tmp, fake = env
    _l, _s, out, res = p1_run(tmp, fake)
    assert ("series", "noaa:03", "unit") not in cells(out)
    assert res["stats"]["misplaced"] == 1 and res["stats"]["duplicate"] == 1


# ---- T12: paging -------------------------------------------------------------------------------------------------

def test_t12_keyset_pages_with_awkward_ids_and_exact_multiples(env, monkeypatch):
    tmp, fake = env
    ids = ["ab:x", "ab_c:x", "q:a'b", 'q:a"b', "q:\u00e9", "q:x;y", "q:zz"]
    fake.add(rc.PRIMARY, "series", [srow(i) for i in ids])
    fake.finish()
    local = make_local(tmp / "l.db", [srow(i) for i in ids])
    snaps, _ = snapshot(fake, local, tmp / "snap", page_rows=3)
    con = sqlite3.connect(snaps[rc.PRIMARY])
    assert sorted(r[0] for r in con.execute("SELECT series_id FROM series")) == sorted(ids)
    con.close()
    assert len([c for c in fake.calls if c[0] == rc.PRIMARY and "FROM series WHERE" in c[1]]) == 3   # 3, 3, 1
    fake2 = FakeD1(tmp / "x")
    fake2.add(rc.PRIMARY, "series", [srow(i) for i in ids[:6]])
    fake2.finish()
    monkeypatch.setattr(rc, "D1_QUERY", fake2)
    snapshot(fake2, local, tmp / "snap2", page_rows=3)
    assert len([c for c in fake2.calls if c[0] == rc.PRIMARY and "FROM series WHERE" in c[1]]) == 3  # 3, 3, 0


def test_t12_out_of_order_or_unmetered_pages_stop(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i}") for i in range(5)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:0")])
    fake.shuffle = True
    with pytest.raises(rc.Stop, match="position"):
        snapshot(fake, local, tmp / "s1", page_rows=3)
    fake.shuffle = False
    fake.drop_meta = True
    with pytest.raises(rc.Stop, match="rows_read"):
        snapshot(fake, local, tmp / "s2", page_rows=3)


def test_t12_an_answer_too_large_halves_the_page(env, monkeypatch):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i:03d}") for i in range(250)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:000")])
    real = fake.__call__

    def picky(db, sql, params):
        if "FROM series WHERE" in sql and params[1] > 100:
            raise RuntimeError("D1 econ-catalog: HTTP 413 response too large")
        return real(db, sql, params)
    monkeypatch.setattr(rc, "D1_QUERY", picky)
    snaps, _ = snapshot(fake, local, tmp / "snap", page_rows=400)
    con = sqlite3.connect(snaps[rc.PRIMARY])
    assert con.execute("SELECT COUNT(*) FROM series").fetchone()[0] == 250
    assert json.loads(con.execute("SELECT v FROM _state WHERE k='page_rows_halved'").fetchone()[0]) is True
    con.close()


# ---- T13: cost ------------------------------------------------------------------------------------------------------

def test_t13_projection_formula():
    assert rc.projection({"a": 10, "b": 6}, 3, {"a": 1, "b": 0}, 100, 7) == 10 + 4 + 6 + 0 + 100 + 7
    assert rc.projection({"a": 10_814_855, "b": 3_138_159}, 5_000, {"a": 1, "b": 1}, 0, 0) == 13_953_014 + 2_163 + 628


def test_t13_budget_refusals_stops_and_a_cumulative_resume(env, monkeypatch):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i:02d}") for i in range(20)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:00")])
    with pytest.raises(rc.Refused, match="required"):
        rc.Budget(None)
    with pytest.raises(rc.Refused, match="ceiling"):
        rc.Budget(rc.MAX_BUDGET_ROWS + 1)
    code = rc.main(["read-d1", "--out", str(tmp / "run"), "--catalogue", local, "--budget-rows", "6000",
                    "--page-rows", "3"])
    assert code == 2 and not [c for c in fake.calls if "FROM series WHERE" in c[1]]   # refused before any page
    monkeypatch.setattr(rc, "SMALL_EXPECTED", 10)
    fake.calls.clear()
    with pytest.raises(rc.Stop, match="STOP before"):
        snapshot(fake, local, tmp / "s", page_rows=3, budget=40)
    first = [c[2][0] for c in fake.calls if "FROM series WHERE" in c[1]]
    spent_first = rc.Budget(40, os.path.join(str(tmp / "s"), "budget.sqlite")).spent
    fake.calls.clear()
    with pytest.raises(rc.Stop):                                     # the budget is CUMULATIVE: 40 is already spent
        snapshot(fake, local, tmp / "s", page_rows=3, budget=40)
    assert spent_first > 0 and not [c for c in fake.calls if "FROM series WHERE" in c[1]]
    snapshot(fake, local, tmp / "s", page_rows=3, budget=10 ** 6)  # resume: no page twice
    lows = [c[2][0] for c in fake.calls if "FROM series WHERE" in c[1] and c[0] == rc.PRIMARY]
    assert first and len(lows) == len(set(lows)) and not set(lows) & set(first)
    con = sqlite3.connect(tmp / "s" / f"d1_{rc.PRIMARY}.sqlite")
    assert con.execute("SELECT COUNT(*) FROM series").fetchone()[0] == 20
    con.close()


def test_t13_rows_read_drift_on_page_one_stops(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i}") for i in range(9)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:0")])
    fake.e = 5
    with pytest.raises(rc.Stop, match="differs from the probe"):
        snapshot(fake, local, tmp / "s", page_rows=3, probe_e=1)


def test_t13_every_statement_is_checked_as_a_read_before_it_is_sent(env):
    tmp, fake = env
    b = rc.Budget(1000)
    for sql in ("DELETE FROM series", "SELECT 1; SELECT 2", "INSERT INTO x SELECT 1"):
        with pytest.raises(rc.Refused, match="not one plain read"):
            rc.d1(b, rc.PRIMARY, sql, [], 1, "x")
    assert fake.calls == []


def test_t13_probe_measures_one_page_and_projects_allowed_rows_only(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i}") for i in range(10)] + [srow(f"gatedsrc:{i}") for i in range(7)])
    fake.add(rc.CLIMATE, "series", [srow(f"noaa:{i}") for i in range(4)])
    fake.raw(rc.PRIMARY, "CREATE TABLE surprise (a)")
    fake.raw(rc.PRIMARY, "CREATE TABLE gatedsrc_extra (a)")
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:0"), srow("noaa:0")])
    assert rc.main(["probe", "--out", str(tmp / "p"), "--catalogue", local, "--budget-rows", "100000",
                    "--page-rows", "3"]) == 0
    p = json.load(open(tmp / "p" / "probe.json"))
    dbp = p["databases"][rc.PRIMARY]
    assert dbp["e"] == 1 and dbp["allowed_rows_by_source_counts"] == 10      # the 7 gated rows are not counted
    assert dbp["unknown_tables"] == ["surprise"] and dbp["a_table_name_matches_the_gate"] == "YES"
    assert p["projection"]["R_total"] == rc.projection({rc.PRIMARY: 10, rc.CLIMATE: 4}, p["projection"]["P"],
                                                       {rc.PRIMARY: 1, rc.CLIMATE: 1}, p["projection"]["R_small_est"],
                                                       300_000)
    hits, _ = _grep_outputs([tmp / "p"], ["gatedsrc"])
    assert hits == []


# ---- T14: refusals (review M7: the F: volume, nothing under the E: repository, no stat of E:'s data) --------------

def test_t14_refusals(env, monkeypatch, capsys):
    tmp, fake = env
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:1")])
    assert rc.guard_catalogue(local) == local                     # positive control
    for p in (os.path.join(cp.LIVE_STORE_ROOT, "data", "catalog.db"), os.path.join(cp.LIVE_STORE_ROOT, "logs", "x.json"),
              cp.LIVE_STORE_ROOT):
        with pytest.raises(rc.Refused, match="E: repository"):
            rc.guard_input(p, "--c7-queue")
        with pytest.raises(rc.Refused):
            rc.guard_catalogue(p)
    other = tmp / "elsewhere"
    monkeypatch.setattr(rc, "F_ANCHOR", "Z:\\no-such-volume" if os.name == "nt" else "/proc")
    with pytest.raises(rc.Refused, match="F: volume"):
        rc.guard_input(str(other), "--manifest")
    monkeypatch.setattr(rc, "F_ANCHOR", str(tmp))
    with pytest.raises(rc.Refused, match="econ_live"):
        rc.guard_input(str(tmp / "econ_live" / "x.db"), "--manifest")
    assert rc.guard_input(str(tmp / "econ_live" / "blobs" / "index.db"), "--served-index")
    with pytest.raises(rc.Refused, match="must be on"):
        rc.guard_out(r"Z:\elsewhere" if os.name == "nt" else "/elsewhere")
    capsys.readouterr()
    assert rc.main(["apply", "--out", str(tmp / "o"), "--plan", local, "--plan-sha", "x", "--fts-measure", local]) == 2
    assert "apply runs only at S11" in capsys.readouterr().out    # refused for the FLAG, not by a later guard (M19)
    with pytest.raises(rc.Refused, match="apply runs only at S11"):
        rc.guard_catalogue(local, for_apply=True)                  # a named file is refused for apply too
    link = tmp / "linked.db"
    os.link(local, link)
    assert "hard links" in rc.plain_copy_problem(str(link))
    assert rc.plain_copy_problem(str(tmp / "fresh.db")) is None
    with pytest.raises(SystemExit):
        rc.main(["classif", "--out", str(tmp / "o"), "--snapshot-dir", str(tmp)])
    monkeypatch.setattr(rc, "ROOT", os.path.join(cp.LIVE_STORE_ROOT, "x"))
    with pytest.raises(rc.Refused, match="F: clone"):
        rc.refuse_e_checkout()


def test_t14_the_e_refusal_touches_nothing_on_e(env, monkeypatch):
    seen = []
    for mod, name in ((os, "stat"), (os.path, "exists"), (os.path, "realpath"), (os.path, "isfile"),
                      (os.path, "samefile")):
        real = getattr(mod, name)

        def spy(p, *a, _real=real, **k):
            seen.append(str(p))
            return _real(p, *a, **k)
        monkeypatch.setattr(mod, name, spy)
    for p in (os.path.join(cp.LIVE_STORE_ROOT, "data", "catalog.db"), os.path.join(cp.LIVE_STORE_ROOT, "logs", "r.json"),
              "E:\\elsewhere\\x.db"):
        with pytest.raises(rc.Refused):
            rc.guard_input(p, "--x")
    assert not [p for p in seen if p.lower().startswith("e:")], seen


def test_t14_classify_after_t0_needs_the_s4c_inputs(env):
    tmp, fake = env
    fake.finish()
    make_local(cp.BUILD_PATH, [srow("a:1")])
    snapshot(fake, cp.BUILD_PATH, tmp / "snap")
    set_t0()
    assert rc.main(["classify", "--out", str(tmp / "c"), "--snapshot-dir", str(tmp / "snap")]) == 2


# ---- T15/T16: rehearse on a copy, apply after the flag ---------------------------------------------------------

def _plan_pipeline(tmp, local, extra=()):
    out = tmp / "cls"
    code = rc.main(["classify", "--out", str(out), "--catalogue", local, "--snapshot-dir", str(tmp / "snap"),
                    "--c1", str(tmp / "c1.tsv"), *extra])
    assert code == 0
    return out, open(str(out / "plan.json.sha256")).read().strip()


def _c1(tmp, rows_):
    (tmp / "c1.tsv").write_text("source\ttable\tcolumn\tevidence\n" + "".join(f"{s}\tseries\t{c}\ttest\n" for s, c in rows_))


def test_t15_t16_rehearse_writes_the_plan_rebuilds_fts_both_ways_and_verifies(env):
    tmp, fake = env
    many = [srow(f"bulk:{i:05d}", title=f"bulk title {i} wages") for i in range(3000)]
    fake.add(rc.PRIMARY, "series", [srow("c1src:1", unit="new"), srow("sec_edgar:N", title="new filer"), *many])
    fake.finish()
    local = make_local(tmp / "copy.db", [srow("c1src:1", unit="old"), srow("alpha:del"), srow("sec_edgar:A"), *many])
    snapshot(fake, local, tmp / "snap", page_rows=1000)
    _c1(tmp, [("c1src", "unit")])
    (tmp / "del.txt").write_text("alpha:del\n")
    out, sha = _plan_pipeline(tmp, local, ("--approved-deletes", str(tmp / "del.txt")))
    code = rc.main(["rehearse", "--out", str(tmp / "reh"), "--catalogue", local, "--plan", str(out / "plan.json"),
                    "--plan-sha", sha, "--fts-both", "--sample-every", "0.001"])
    rec = json.load(open(tmp / "reh" / "rehearse.json"))
    assert code == 0, rec.get("verify")                          # before T0 the rehearsal takes sec_edgar too
    con = sqlite3.connect(local)
    assert con.execute("SELECT unit FROM series WHERE series_id='c1src:1'").fetchone()[0] == "new"
    assert con.execute("SELECT 1 FROM series WHERE series_id='sec_edgar:N'").fetchone()
    assert con.execute("SELECT COUNT(*) FROM series_fts WHERE series_fts MATCH '\"filer\"'").fetchone()[0] == 1
    assert not con.execute("SELECT 1 FROM series WHERE series_id='alpha:del'").fetchone()
    assert con.execute("SELECT COUNT(*) FROM series_fts WHERE series_fts MATCH '\"wages\"'").fetchone()[0] == 3000
    con.close()
    assert rec["fts_A"]["series_rows"] == rec["fts_A"]["series_fts_rows"] and "growth_bytes" in rec["fts_A"]
    assert rec["fts_equivalence"]["equivalent"] is True and rec["fts_equivalence"]["ddl_equal"] is True
    assert rec["verify"]["ok"] is True
    assert rc.main(["rehearse", "--out", str(tmp / "reh2"), "--catalogue", local, "--plan", str(out / "plan.json"),
                    "--plan-sha", sha]) == 2                       # the copy changed: the plan no longer fits


def test_t15_verify_catches_a_stale_index_with_equal_counts(env, tmp_path):
    """V5 counts alone pass a rebuild that did nothing when one row was added and one deleted (mutant M23 survived
    that way); V5_content compares the index's text with series."""
    local = make_local(tmp_path / "v.db", [srow("a:1", title="wages"), srow("a:2", title="prices")])
    plan = {"writes": {"cells": [], "inserts": [], "deletes": []}, "digests": {"after": {}}}
    con = cp.connect_path(local, write=True, isolation_level=None)
    allow = rc.build_allow(con, set(), set())
    assert "V5_content" not in rc.verify(con, plan, allow)["failed"]          # positive control: a fresh index
    con.execute("UPDATE series SET title='rents' WHERE series_id='a:1'")      # stale now; the counts are equal
    res = rc.verify(con, plan, allow)
    assert "V5_content" in res["failed"] and "V5" not in res["failed"] and res["ok"] is False
    rc.fts_rebuild(con, "rebuild_fts")
    assert "V5_content" not in rc.verify(con, plan, allow)["failed"]
    con.close()


def test_t16_m2_the_drop_form_is_one_transaction_and_checks_its_ddl(env, tmp_path):
    local = make_local(tmp_path / "c.db", [srow("a:1", title="wages")])
    con = cp.connect_path(local, write=True)                       # legacy transaction control: refused
    with pytest.raises(rc.Refused, match="isolation_level"):
        rc.fts_rebuild(con, "drop_create")
    con.close()
    con = cp.connect_path(local, write=True, isolation_level=None)

    class Boom(Exception):
        pass
    real = con.execute
    calls = []

    class Wrapped:
        def __getattr__(self, n):
            return getattr(con, n)

        isolation_level = None

        def execute(self, sql, *a):
            calls.append(sql)
            if sql.startswith("INSERT INTO series_fts"):
                raise Boom()
            return real(sql, *a)
    with pytest.raises(Boom):
        rc.fts_rebuild(Wrapped(), "drop_create")
    # the DROP was rolled back with the failed INSERT: the index is still there and still full
    assert con.execute("SELECT COUNT(*) FROM series_fts").fetchone()[0] == 1 and "DROP TABLE series_fts" in calls
    con.close()


def test_t15_a_row_changed_after_the_plan_rolls_back_its_chunk(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("c1src:1", unit="new")])
    fake.finish()
    local = make_local(tmp / "copy.db", [srow("c1src:1", unit="old")])
    snapshot(fake, local, tmp / "snap")
    _c1(tmp, [("c1src", "unit")])
    out, _sha = _plan_pipeline(tmp, local)
    plan = json.load(open(out / "plan.json"))
    con = sqlite3.connect(local)
    con.execute("UPDATE series SET unit='edited' WHERE series_id='c1src:1'")
    con.commit()
    c2 = cp.connect_path(local, write=True, isolation_level=None)
    with pytest.raises(rc.Refused, match="changed since the plan"):
        rc.apply_writes(c2, plan, str(tmp), "t")
    c2.close()
    assert con.execute("SELECT unit FROM series WHERE series_id='c1src:1'").fetchone()[0] == "edited"
    con.close()
    assert os.path.exists(tmp / "t_before.json")


def test_t15_verify_catches_an_out_of_plan_change(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1"), srow("b:1")])
    fake.finish()
    local = make_local(tmp / "copy.db", [srow("a:1"), srow("b:1")])
    snapshot(fake, local, tmp / "snap")
    _c1(tmp, [])
    out, _sha = _plan_pipeline(tmp, local)
    plan = json.load(open(out / "plan.json"))
    con = cp.connect_path(local, write=True, isolation_level=None)
    allow = rc._allow_for_plan(con, plan, set(GATE), {"sec_edgar"})
    assert rc.verify(con, plan, allow)["ok"] is True
    con.execute("UPDATE series SET title='sneaky' WHERE series_id='b:1'")
    v = rc.verify(con, plan, allow)
    con.close()
    assert v["ok"] is False and v["failed_buckets"] == ["b"]


def _t0_build(tmp, fake, local_rows, d1_rows):
    fake.add(rc.PRIMARY, "series", d1_rows)
    fake.finish()
    make_local(cp.BUILD_PATH, local_rows)
    set_t0()
    snapshot(fake, cp.BUILD_PATH, tmp / "snap")
    qf = tmp / "queue.txt"
    qf.write_text("a:1\n")
    s4c = tmp / "s4c.json"
    s4c.write_text(json.dumps({"sha256": rc.file_sha256(str(qf)), "size": qf.stat().st_size, "lines": 1}))
    runs = tmp / "runs.tsv"
    runs.write_text("run_key\twriter\tutc\tsources\treceipt\tgrain\tbooking\n")
    reh = tmp / "reh.json"
    reh.write_text(json.dumps({"fts_A": {"journal_peak_bytes": 0, "growth_bytes": 0}}))
    return ["--c7-queue", str(qf), "--s4c-receipt", str(s4c), "--c7-runs", str(runs)], reh


def test_t15_apply_after_the_flag_with_the_lock_and_a_pinned_plan(env):
    tmp, fake = env
    extra, reh = _t0_build(tmp, fake, [srow("c1src:1", unit="old"), srow("a:1")], [srow("c1src:1", unit="new"),
                                                                                  srow("a:1")])
    _c1(tmp, [("c1src", "unit")])
    out = tmp / "cls"
    assert rc.main(["classify", "--out", str(out), "--snapshot-dir", str(tmp / "snap"), "--c1", str(tmp / "c1.tsv"),
                    *extra]) == 0
    sha = open(str(out / "plan.json.sha256")).read().strip()
    argv = ["apply", "--out", str(tmp / "ap"), "--plan", str(out / "plan.json"), "--plan-sha", sha,
            "--fts-measure", str(reh), "--sample-every", "0.01"]
    assert rc.main(argv) == 2                                    # no lock held: refused
    with cp.writer_lock():
        assert rc.main(argv[:6] + ["0" * 64] + argv[7:]) == 2    # wrong plan sha: refused
        assert rc.main(argv) == 0
    con = sqlite3.connect(cp.BUILD_PATH)
    assert con.execute("SELECT unit FROM series WHERE series_id='c1src:1'").fetchone()[0] == "new"
    con.close()
    with cp.writer_lock():
        assert rc.main(argv) == 2                                # the build changed: the plan no longer fits


def test_t15_m6_v7_counts_sec_edgar_even_under_a_source_level_c7(env):
    tmp, fake = env
    extra, reh = _t0_build(tmp, fake, [srow("sec_edgar:1", start_date="2001-01-01"), srow("a:1")],
                           [srow("sec_edgar:1", start_date="2002-01-01"), srow("a:1")])
    _c1(tmp, [])
    hand = tmp / "hand.tsv"
    hand.write_text("key\tgrain\treason\tevidence\nsec_edgar\tsource\ta local sec_edgar run\ttest\n")
    out = tmp / "cls"
    code = rc.main(["classify", "--out", str(out), "--snapshot-dir", str(tmp / "snap"), "--c1", str(tmp / "c1.tsv"),
                    "--c7-hand", str(hand), *extra])
    assert code == 1                                             # V7 fails although the cell's class is C7
    assert cells(out)[("series", "sec_edgar:1", "start_date")][0] == "C7"
    with cp.writer_lock():
        assert rc.main(["apply", "--out", str(tmp / "ap"), "--plan", str(out / "plan.json"), "--plan-sha",
                        open(str(out / "plan.json.sha256")).read().strip(), "--fts-measure", str(reh)]) == 2


def test_t15_m6_road_b_null_over_local_pairs_are_the_only_v7_exemption(env):
    tmp, fake = env
    extra, _reh = _t0_build(tmp, fake, [srow("sec_edgar:1", metadata='{"cik":"1"}'), srow("a:1")],
                            [srow("sec_edgar:1", metadata=None), srow("a:1")])
    _c1(tmp, [])
    rec = tmp / "rec"
    rec.mkdir()
    (rec / "roadb.json").write_text(json.dumps({"source": "sec_edgar", "missing": [], "differing": [],
                                                "before_after": {}, "null_over_local": {"sec_edgar:1": {"metadata": ["x", None]}}}))
    runs = tmp / "runs.tsv"
    runs.write_text("run_key\twriter\tutc\tsources\treceipt\tgrain\tbooking\troad_b\n"
                    "rb\ttools/sync_source_rows_d1_to_local.py\t2026-12-14T10:00:00Z\tsec_edgar\troadb.json\tids\tS5\tyes\n")
    extra[extra.index("--c7-runs") + 1] = str(runs)
    assert rc.main(["classify", "--out", str(tmp / "cls"), "--snapshot-dir", str(tmp / "snap"), "--c1",
                    str(tmp / "c1.tsv"), "--receipts-dir", str(rec), *extra]) == 0


# ---- T17: the search comparison ---------------------------------------------------------------------------------

def test_t17_search_differences_are_explained_or_fail(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("s:1", title="wages common"), srow("s:2", title="wages old title"),
                                    srow("s:3", title="wages only in d1"), srow("s:5", title="fresh title"),
                                    srow("s:6", title="wages six")])
    fake.raw(rc.PRIMARY, "UPDATE series_fts SET title='stale wages index' WHERE series_id='s:5'")
    fake.raw(rc.PRIMARY, "DELETE FROM series_fts WHERE series_id='s:6'")                 # a MISSING index row
    fake.finish()
    local = make_local(tmp / "l.db", [srow("s:1", title="wages common"), srow("s:2", title="new name"),
                                      srow("s:4", title="wages local only"), srow("s:5", title="fresh title"),
                                      srow("s:6", title="wages six")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    run_classify(local, snaps, tmp / "out")
    con = cp.connect_path(local, write=False)
    allow = allow_for(local, snaps)
    res = rc.search_compare(con, ['"wages"'], str(tmp / "out" / "classify.sqlite"), rc.Budget(10 ** 5), allow, snaps)
    why = {d["id"]: (d["expected"], d["why"]) for d in res["queries"][0]["diffs"]}
    assert why["s:2"][0] and "kept local" in why["s:2"][1]
    assert why["s:3"][0] and why["s:3"][1].startswith("C3")
    assert why["s:4"][0] and why["s:4"][1].startswith("C4")
    assert why["s:5"][0] and "STALE" in why["s:5"][1]
    assert why["s:6"][0] and "MISSING" in why["s:6"][1]
    assert res["pass"] is True
    fake.raw(rc.PRIMARY, "INSERT INTO series_fts (series_id, title, geography) VALUES ('s:1', 'zebra', 'World')")
    fake.raw(rc.PRIMARY, "UPDATE series SET title='zebra' WHERE series_id='s:1'")
    res3 = rc.search_compare(con, ['"zebra"'], str(tmp / "out" / "classify.sqlite"), rc.Budget(10 ** 5), allow, snaps)
    assert res3["pass"] is False                                  # D1 answers s:1 for 'zebra'; nothing explains it
    origin = make_local(tmp / "origin.db", [srow("s:1", title="wages common")])
    res4 = rc.search_compare(con, ['"wages"'], str(tmp / "out" / "classify.sqlite"), rc.Budget(10 ** 5), allow, snaps,
                             [origin])
    assert res4["pass"] is False and res4["queries"][0]["origin_differs"]
    with pytest.raises(rc.Refused, match="double-quoted"):
        rc.search_compare(con, ["wages OR x"], str(tmp / "out" / "classify.sqlite"), rc.Budget(10 ** 5), allow, snaps)
    con.close()


# ---- T18: determinism and the side-by-side -----------------------------------------------------------------------

def test_t18_classify_twice_gives_the_same_plan_and_summary(env):
    tmp, fake = env
    local = plant_p1(tmp, fake)
    snaps, _ = snapshot(fake, local, tmp / "snap")
    outs = []
    for k in ("a", "b"):
        res = run_classify(local, snaps, tmp / k, c7=p1_c7(), deletes={"alpha:06"})
        s = rc.summarise(str(tmp / k), res, p1_c7(), {"gate_sha256": "g"}, None, allow_for(local, snaps))
        outs.append((json.dumps(res["plan"], sort_keys=True), json.dumps(res["digests"], sort_keys=True),
                     json.dumps(s["totals"], sort_keys=True)))
    assert outs[0] == outs[1]


def test_t18_side_by_side_is_refiltered_and_lists_new_c1_cells(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("c1src:1", unit="n"), srow("c1src:2", unit="n")])
    fake.finish()
    local1 = make_local(tmp / "l1.db", [srow("c1src:1", unit="o"), srow("c1src:2", unit="n")])
    local2 = make_local(tmp / "l2.db", [srow("c1src:1", unit="o"), srow("c1src:2", unit="o")])
    snaps, _ = snapshot(fake, local1, tmp / "snap")
    r1 = run_classify(local1, snaps, tmp / "dry", c1={("c1src", "series", "unit")})
    rc.summarise(str(tmp / "dry"), r1, rc.C7(), {"gate_sha256": "g"}, None, allow_for(local1, snaps))
    dry = json.load(open(tmp / "dry" / "summary.json"))
    dry["per_source"]["laterGated"] = {k: 99 for k in rc.SUMMARY_METRICS}       # a source gated since the dry run
    rc.write_json(str(tmp / "dry" / "summary.json"), dry)
    r2 = run_classify(local2, snaps, tmp / "s11", c1={("c1src", "series", "unit")})
    s = rc.summarise(str(tmp / "s11"), r2, rc.C7(), {"gate_sha256": "g"}, str(tmp / "dry" / "summary.json"),
                     allow_for(local2, snaps))
    assert s["side_by_side"]["c1_cells_new_since_dry_run"] == [["series", "c1src:2", "unit"]]
    assert s["side_by_side"]["totals"]["cells_taken_d1"] == {"dry_run": 1, "s11": 2, "delta": 1}
    assert b"laterGated" not in open(tmp / "s11" / "summary.md", "rb").read()


# ---- T19: the repository's rules ------------------------------------------------------------------------------------

def test_t19_the_fts_statements_are_the_core_catalogue_modules():
    srcf = open(os.path.join(ROOT, "core", "catalog.py"), encoding="utf-8").read()
    body = srcf.split("def rebuild_fts", 1)[1].split("\ndef ", 1)[0]
    assert f'"{rc.FTS_DELETE}"' in body and f'"{rc.FTS_INSERT}"' in body
    tool = open(rc.__file__, encoding="utf-8").read()
    assert "from core import catalog\n" not in tool and "import core.catalog\n" not in tool


def test_t19_the_writer_map_names_exactly_the_catalogue_writers():
    import subprocess  # noqa: PLC0415
    r = subprocess.run(["git", "grep", "-l", "-E", r"connect\(write=True|connect_path\(.*write=True|write_session\(\)",
                        "--", "*.py"], cwd=ROOT, capture_output=True, text=True)
    if r.returncode not in (0, 1):
        pytest.skip("not a git checkout")
    found = {p for p in r.stdout.split() if not p.startswith("tests/")}
    found |= {"tools/delist_source_rows.py", "tools/retire_source.py", "core/licence_targets.py",
              "tools/selfhost/reconcile_catalogue.py"}
    listed = {w["writer"] for w in rc.read_tsv(os.path.join(rc.LISTS_DIR, "writers.tsv"),
                                                ("writer", "parser", "grain", "evidence"))}
    assert listed == found, (sorted(found - listed), sorted(listed - found))


def test_t19_the_shipped_lists_parse():
    assert rc.load_c1(os.path.join(rc.LISTS_DIR, "c1_columns.tsv")) <= {("sec_edgar", "series", c)
                                                                        for c in ("start_date", "end_date", "title")}
    assert rc.load_c3(os.path.join(rc.LISTS_DIR, "c3_writers.tsv")) == {"sec_edgar"}
    c7 = rc.C7()
    rc.load_c7_hand(c7, os.path.join(rc.LISTS_DIR, "c7_hand.tsv"))
    assert "insee_melodi:DS_RP_MENAGES_PRINC" in c7.ids and "statcan" in c7.srcs


def test_t19_the_token_is_loaded_in_process_and_never_printed(tmp_path, monkeypatch):
    for k in ("CLOUDFLARE_API_TOKEN", "D1_READ_TOKEN"):        # setenv first, so teardown removes what the tool sets
        monkeypatch.setenv(k, "restored-by-teardown")
        monkeypatch.delenv(k)
    envf = tmp_path / ".env"
    envf.write_text("OTHER=1\nCLOUDFLARE_API_TOKEN=\"planted-secret-value\"\n")
    msg = rc.ensure_token(str(envf))
    assert os.environ["CLOUDFLARE_API_TOKEN"] == "planted-secret-value" and "planted-secret-value" not in msg
    monkeypatch.delenv("CLOUDFLARE_API_TOKEN")
    envf.write_text("CLOUDFLARE_API_TOKEN=...\n")
    with pytest.raises(rc.Refused):
        rc.ensure_token(str(envf))
    set_t0()
    envf.write_text("CLOUDFLARE_API_TOKEN=write-capable\n")
    with pytest.raises(rc.Refused, match="D1_READ_TOKEN"):
        rc.ensure_token(str(envf))                               # after T0 the write-capable token is never used


def test_t19_c7_candidates_lists_dated_writer_lines(env, tmp_path):
    b = tmp_path / "NUMBERS.md"
    b.write_text("| 2026-09-05 ran tools/fix_statcan_frequency.py |\n| 2026-08-01 catalog old |\n| no date catalog |\n")
    assert rc.main(["c7-candidates", "--out", str(tmp_path / "o"), "--booking", str(b)]) == 0
    lines = open(tmp_path / "o" / "c7_candidates.tsv", encoding="utf-8").read().splitlines()
    assert len(lines) == 2 and "fix_statcan_frequency" in lines[1]


def test_t19_copy_makes_a_byte_equal_plain_copy(env):
    tmp, _ = env
    s = tmp / "src.db"
    s.write_bytes(os.urandom(200_000))
    assert rc.main(["copy", "--out", str(tmp / "o"), "--from", str(s), "--to", str(tmp / "dst.db")]) == 0
    rec = json.load(open(tmp / "o" / "copy.json"))
    assert rec["sha256_from_before"] == rec["sha256_from_after"] == rec["sha256_to"] and rec["nlink_to"] == 1
    assert rc.main(["copy", "--out", str(tmp / "o"), "--from", str(s), "--to", str(tmp / "dst.db")]) == 2


def test_t19_the_manifest_copy_must_be_static(env, tmp_path):
    m = tmp_path / "sent.db"
    con = sqlite3.connect(m)
    con.execute("CREATE TABLE sent(series_id TEXT PRIMARY KEY, row_hash TEXT NOT NULL, fts_hash TEXT)")
    con.commit()
    con.close()
    rc.Manifest(str(m))                                            # positive control
    (tmp_path / "sent.db-wal").write_bytes(b"x")
    with pytest.raises(rc.Refused, match="static"):
        rc.Manifest(str(m))


def _resolver_opens(ast, tree):
    out = []
    for f in ast.walk(tree):
        if isinstance(f, ast.FunctionDef):
            for n in ast.walk(f):
                if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                        and n.func.attr in ("connect", "connect_path") and ast.unparse(n.func.value) == "catalog_path"):
                    out.append(f.name)
    return out


def _plain_catalogue_opens(ast, tree):
    held = {"catalogue", "build", "where", "cat_con"}
    return [n.lineno for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "connect" and ast.unparse(n.func.value) == "sqlite3" and n.args
            and {x.id for x in ast.walk(n.args[0]) if isinstance(x, ast.Name)} & held]


def test_t19_the_catalogue_is_opened_only_by_the_two_helpers():
    """Every resolver open sits in _catalogue_ro / _catalogue_rw (each returns the connection at once), and no
    plain sqlite3 open takes a name that holds the catalogue. Planted opens in parsed snippets are the positive
    controls."""
    import ast                                                         # noqa: PLC0415
    with open(rc.__file__, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    assert sorted(_resolver_opens(ast, tree)) == ["_catalogue_ro", "_catalogue_rw"]
    assert _plain_catalogue_opens(ast, tree) == []
    planted = ast.parse("import sqlite3" + chr(10) + "def f(catalogue):" + chr(10)
                        + "    return sqlite3.connect(catalogue)" + chr(10))
    assert len(_plain_catalogue_opens(ast, planted)) == 1              # positive control
    planted2 = ast.parse("def g(catalogue):" + chr(10)
                         + "    con = catalog_path.connect_path(catalogue, write=False)" + chr(10))
    assert _resolver_opens(ast, planted2) == ["g"]                     # positive control
