"""tools/stat_estonia_grain_repair.py: the reviewed D2 store repair (one table's whole history at the table's
own grain) and the metadata-only census of the class (analysis F:/work/econ_finish/estonia_fetch/analysis/
REPORT.md, 2026-11-27).

The publisher is a stub that answers what each POSTed query asks; everything else is real: the fetcher's
_plan_table / _compare_subject / update(), the ingester's parser, merge_and_write on a local store, and the
served byte contract (core.derive_csv._series_csv_bytes). FULL has TT065's real dimension sizes (14 years x
Kuu 13 x region 101 x sex 3 x age 4 = 220,584 cells, 15,756 per period), so a full pull is three POSTs; it
pins the analysis's expected receipt (new series 15,743). One FULL fetch costs ~12 s, so the other tests use
SCALED: the same dimensions with 6 regions under MAX_CELLS 2,000 (936 cells per period, 13,104 whole) - the
same cell class. The planted store holds the 13 Kuu-keyed national-total keys the analysis inferred (180 rows).
"""
from __future__ import annotations

import contextlib
import csv
import datetime as dt
import hashlib
import importlib.util
import itertools
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import catalog_path, cutover  # noqa: E402
from updater.strategies.fetchers import stat_estonia as S  # noqa: E402

_spec = importlib.util.spec_from_file_location("stat_estonia_grain_repair",
                                               os.path.join(ROOT, "tools", "stat_estonia_grain_repair.py"))
T = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(T)

Y = dt.date.today().year            # the fetcher's cold rule reads today's date: keep the table hot
END = lambda y: dt.date(y, 12, 31)  # noqa: E731
BASE = "https://x"

TT = "sotsiaalelu/tooturg/tootud/luhiajastatistika/TT065.PX"
PRE = S._table_prefix(TT)
CTRL = "EE:sotsiaalelu:tooturg:tootud:luhiajastatistika:TT999.PX"      # another table in the same file
REGION = "Piirkond/Haldusüksus"
AGE = "Vanuserühm"
YEARS = [str(y) for y in range(Y - 13, Y + 1)]
KUU = ["00"] + [str(m) for m in range(1, 13)]
REG = [str(i) for i in range(1, 101)] + ["unk"]
SEX = ["1", "2", "3"]
AGES = ["1", "2", "3", "4"]
_KI = {k: i for i, k in enumerate(KUU)}
_RI = {r: i for i, r in enumerate(REG)}


def tt_vars(years=YEARS, reg=REG):
    return [{"code": "Aasta", "time": True, "values": list(years)}, {"code": "Kuu", "values": KUU},
            {"code": REGION, "values": list(reg)}, {"code": "Sugu", "values": SEX}, {"code": AGE, "values": AGES}]


class Shape:
    """A TT065-shaped table: per period = Kuu 13 x regions x 3 x 4; the current year has 11 Kuu codes."""

    def __init__(self, reg, max_cells):
        self.reg, self.max_cells = list(reg), max_cells
        self.per = 13 * len(reg) * 12
        self.n_fetched = 13 * self.per + 11 * (self.per // 13)
        self.n_new = self.per - 13
        self.boundary_withheld = 11 * (self.per // 13) - 11      # the current year's rows under new series


FULL = Shape(REG, 100_000)
SCALED = Shape(REG[:5] + ["unk"], 2_000)


def tt_value(c):
    """The current year has no November/December yet; every other cell has a value."""
    if c["Aasta"] == str(Y) and c["Kuu"] in ("11", "12"):
        return None
    return float(int(c["Aasta"][:4]) % 100 * 100000 + _KI[c["Kuu"]] * 500 + _RI[c[REGION]] * 4
                 + int(c["Sugu"]) * 2 + int(c[AGE]))


def agg_key(kuu):
    return f"{PRE}:Kuu={kuu}:{REGION}=1:Sugu=1:{AGE}=1"


N_STORED = 13 * 13 + 11                       # the 13 stored keys: 180 rows
assert (FULL.n_fetched, FULL.n_new) == (218160, 15743)   # 13 x 15,756 + 11 x 1,212; the analysis's 15,743
N_NEW = SCALED.n_new                          # 923


class Pub:
    """A stub andmed.stat.ee. tables {path: (variables, value_fn)}, folders {path: [items]} for the crawl.
    no_post: a data POST fails the test (the census reads metadata only). `gone` raises TransientError."""

    def __init__(self, tables, folders=None, no_post=False, unreachable=()):
        self.tables, self.folders, self.no_post = tables, folders or {}, no_post
        self.unreachable, self.gets, self.posts = set(unreachable), [], []

    def meta(self, sess, url):
        path = url[len(BASE):].lstrip("/")
        self.gets.append(path)
        if path in self.unreachable:
            raise S.TransientError("stub 503")
        if path in self.folders:
            return list(self.folders[path])
        return {"variables": [dict(v) for v in self.tables[path][0]]} if path in self.tables else None

    def post(self, sess, url, body):
        if self.no_post:
            raise AssertionError(f"a data POST was sent: {url}")
        path = url[len(BASE):].lstrip("/")
        self.posts.append((path, body))
        variables, fn = self.tables[path]
        q = body["query"]
        ids = [x["code"] for x in q]
        sels = [list(x["selection"]["values"]) for x in q]
        vals = [fn(dict(zip(ids, combo))) for combo in itertools.product(*sels)]
        tcode = next((v["code"] for v in variables if v.get("time")), None)
        return {"id": ids, "size": [len(s) for s in sels], "value": vals,
                "role": {"time": [tcode]} if tcode else {},
                "dimension": {i: {"category": {"index": {c: k for k, c in enumerate(s)}}}
                              for i, s in zip(ids, sels)}}


def _sha(p):
    with open(p, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def _rows(p, prefix):
    t = pq.read_table(p)
    return {(k, d): v for k, d, v in zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                                         t.column("value").to_pylist()) if k.startswith(prefix + ":")}


@pytest.fixture
def world(tmp_path, monkeypatch, capsys):
    for k in ("AQUEDUCT_BACKEND", "ECONDL_DATA", S.BOUNDARY_ENV, S.REVISION_ENV):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(S, "OPERATOR_ACTIVATED_ON", None, raising=False)
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))      # before T0, whatever the machine
    real = S._ingester()            # a fresh module per test: MAX_CELLS set here cannot leak
    real.BASE = BASE
    real.MAX_CELLS = SCALED.max_cells
    monkeypatch.setattr(S, "_ingester", lambda: real)
    store_root = tmp_path / "clean_full"
    sdir = store_root / S.SOURCE
    sdir.mkdir(parents=True)
    monkeypatch.setattr(S.config, "source_dir", lambda s: str(sdir))
    monkeypatch.setattr(S.time, "sleep", lambda s: None)
    out = tmp_path / "out"
    box = types.SimpleNamespace(tmp=tmp_path, store_root=store_root, sdir=sdir, out=out, mp=monkeypatch,
                                subject=str(sdir / "sotsiaalelu.parquet"), ing=real)

    def use(shape):
        real.MAX_CELLS = shape.max_cells
    box.use = use

    def plant(extra=()):
        rows = [(agg_key(k), END(int(y)), tt_value({"Aasta": y, "Kuu": k, REGION: "1", "Sugu": "1", AGE: "1"}))
                for y in YEARS for k in KUU]
        rows = [r for r in rows if r[2] is not None] + list(extra)
        rows += [(f"{CTRL}:Sugu=1", END(Y - 1), 5.0), (f"{CTRL}:Sugu=1", END(Y), 6.0)]
        pq.write_table(pa.table({"series_key": pa.array([r[0] for r in rows], pa.string()),
                                 "obs_date": pa.array([r[1] for r in rows], pa.date32()),
                                 "value": pa.array([r[2] for r in rows], pa.float64())}), box.subject)
        return box.subject

    def tool(pub, *args):
        monkeypatch.setattr(S, "_get_meta", pub.meta)
        monkeypatch.setattr(S, "_post_data", pub.post)
        capsys.readouterr()
        rc = T.main(list(args) + ["--store-root", str(store_root), "--out-dir", str(out), "--rate", "0.25"],
                    session=object())
        return rc, capsys.readouterr().out

    def update(pub):
        (sdir / "_catalog.json").write_text(json.dumps([{"path": TT}]), encoding="utf-8")
        monkeypatch.setattr(S, "_get_meta", pub.meta)
        monkeypatch.setattr(S, "_post_data", pub.post)
        capsys.readouterr()
        res = S.update(types.SimpleNamespace(config={}, key="stat_estonia/_all"), None)
        return res, capsys.readouterr().out

    def receipt():
        js = sorted(p for p in out.glob("*.json"))
        assert len(js) == 1, js
        return json.loads(js[0].read_text(encoding="utf-8"))

    box.plant, box.tool, box.update, box.receipt = plant, tool, update, receipt
    return box


def tt_pub(shape=SCALED, **kw):
    return Pub({TT: (tt_vars(reg=shape.reg), tt_value)}, **kw)


def _line(out, path):
    return next(ln for ln in out.splitlines() if f"{path}: grain=" in ln and "withheld_new_series=" in ln)


# ---- the receipt -------------------------------------------------------------------------------------------
def test_dry_run_receipt_on_the_full_tt065_shape_writes_nothing(world):
    """The analysis's expected dry-run receipt (REPORT.md 'Repair path'): TT065 new series 15,743, held 0,
    dup 0 - on TT065's real dimension sizes and MAX_CELLS 100,000."""
    world.use(FULL)
    p = world.plant()
    before = _sha(p)
    pub = tt_pub(FULL)
    rc, out = world.tool(pub, "--tables", TT)
    assert rc == T.OK, out
    r = world.receipt()
    assert (r["fetched_rows"], r["fetched_keys"], r["new_series"], r["rows_under_new_series"]) == \
        (218160, 15756, 15743, 218160 - N_STORED), r
    assert (r["held"], r["dup_fetched"], r["dup_stored"], r["changed"], r["added"], r["same"]) == \
        (0, 0, 0, 0, 218160 - N_STORED, N_STORED), r
    assert (r["stored_rows"], r["stored_keys"], r["grain"], r["posts"], r["d1_shape"], r["d2_class"]) == \
        (N_STORED, 13, "full", 3, True, True), r
    assert (r["cells_per_period"], r["cells_whole_history"], r["max_cells"]) == (15756, 220584, 100000), r
    assert r["withheld_new_series"] == 0, "the full-pull path skips the growth bound - hence --expect-new-series"
    assert len(pub.posts) == 3 and max(
        len(list(itertools.product(*[x["selection"]["values"] for x in b["query"]]))) for _, b in pub.posts
    ) <= 100000, "chunked under MAX_CELLS"
    assert "--apply --expect-new-series 15743" in out, out
    assert _sha(p) == before, "a dry run writes nothing"


# ---- the planted positive, and the receipt that the hold is gone ------------------------------------------
def test_apply_repairs_the_table_and_a_boundary_pass_then_withholds_nothing(world):
    p = world.plant()
    # CONTROL: before the repair the fetcher's own boundary pass withholds the detail (the instrument can fail)
    res0, out0 = world.update(tt_pub())
    w = SCALED.boundary_withheld                                         # 781
    assert f"withheld_new_series={w}" in _line(out0, TT), _line(out0, TT)
    assert f"WITHHELD {w}" in res0.error, res0.error
    assert len(_rows(p, PRE)) == N_STORED, "withheld: nothing of the detail merged"

    rc, out = world.tool(tt_pub(), "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    assert rc == T.OK, out
    st = _rows(p, PRE)
    assert len(st) == SCALED.n_fetched and len({k for k, _ in st}) == SCALED.per
    assert st[(f"{PRE}:Kuu=5:{REGION}=5:Sugu=2:{AGE}=3", END(Y))] == tt_value(
        {"Aasta": str(Y), "Kuu": "5", REGION: "5", "Sugu": "2", AGE: "3"})
    assert _rows(p, CTRL) == {(f"{CTRL}:Sugu=1", END(Y - 1)): 5.0, (f"{CTRL}:Sugu=1", END(Y)): 6.0}, \
        "the other table in the file is untouched"
    r = world.receipt()
    assert r["result"] == "merged and read back" and r["read_back_problems"] == [], r
    assert (r["new_series"], r["held"], r["posts"]) == (N_NEW, 0, 7), r
    assert r["rows_after"] == r["rows_before"] + SCALED.n_fetched - N_STORED, r
    with open(r["derived_csv"], encoding="utf-8", newline="") as fh:
        lines = list(csv.reader(fh))
    assert lines[0] == ["series_id", "obs_date", "value"] and len(lines) == SCALED.n_fetched + 1
    ids = list(world.out.glob("*.ids.txt"))
    assert ids and ids[0].read_text(encoding="utf-8") == f"stat_estonia:{PRE}\n"
    assert "NEXT 1" in out and "core.derive_csv" in out, out

    res1, out1 = world.update(tt_pub())
    ln = _line(out1, TT)
    assert "withheld_new_series=0" in ln and "changed=0 added=0 held=0" in ln, ln
    assert "WITHHELD" not in (res1.error or ""), res1.error


# ---- the refusals ------------------------------------------------------------------------------------------
def test_a_count_mismatch_refuses_and_writes_nothing(world):
    p = world.plant()
    before = _sha(p)
    rc, out = world.tool(tt_pub(), "--tables", TT, "--apply", "--expect-new-series", str(N_NEW - 1))
    assert rc == T.REFUSED, out
    assert f"--expect-new-series {N_NEW - 1:,} != the {N_NEW:,} new series measured now" in out, out
    assert _sha(p) == before and not list(world.out.glob("*.csv"))


def test_apply_without_the_count_refuses_before_any_request(world):
    world.plant()
    pub = tt_pub()
    rc, out = world.tool(pub, "--tables", TT, "--apply")
    assert rc == T.REFUSED and "--expect-new-series" in out, out
    assert pub.gets == [] and pub.posts == []


def test_apply_takes_one_table(world):
    pub = tt_pub()
    rc, out = world.tool(pub, "--tables", TT, TT.replace("TT065", "TT066"), "--apply", "--expect-new-series", "1")
    assert rc == T.REFUSED and "ONE table" in out and pub.gets == [], out


def _dup_fetched_pub():
    """A second time code that parses to a date already listed: (series_key, obs_date) is not a key."""
    years = YEARS + [f"{Y - 1}-12-31"]
    return Pub({TT: (tt_vars(years, SCALED.reg), tt_value)})


@pytest.mark.parametrize("where", ["fetched", "stored"])
def test_a_duplicate_refuses_with_the_right_count(world, where):
    extra = [(agg_key("3"), END(Y - 2), tt_value({"Aasta": str(Y - 2), "Kuu": "3", REGION: "1", "Sugu": "1",
                                                     AGE: "1"}))] if where == "stored" else []
    p = world.plant(extra)
    before = _sha(p)
    pub = _dup_fetched_pub() if where == "fetched" else tt_pub()
    rc, out = world.tool(pub, "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    refused = [ln for ln in out.splitlines() if "REFUSED" in ln]
    assert rc == T.REFUSED, out
    assert any("duplicate (series_key, obs_date)" in ln for ln in refused), refused
    assert not any("new series measured now" in ln for ln in refused), "the count is right: the dup gate fired"
    assert _sha(p) == before


@pytest.mark.parametrize("extra", [
    (agg_key("00"), END(Y - 14), 1.0),                                   # a period the publisher no longer lists
    (f"{PRE}:Kuu=99:{REGION}=1:Sugu=1:{AGE}=1", END(Y - 1), 1.0),          # a key the publisher does not return
], ids=["old-period", "orphan-key"])
def test_a_stored_row_the_publisher_did_not_return_refuses(world, extra):
    p = world.plant([extra])
    before = _sha(p)
    rc, out = world.tool(tt_pub(), "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    assert rc == T.REFUSED and "held 1:" in out, out
    assert _sha(p) == before


def test_a_table_outside_the_d2_class_refuses(world):
    small = "majandus/x/RAA0012.PX"
    pub = Pub({small: ([{"code": "Aasta", "time": True, "values": [str(Y - 1), str(Y)]},
                        {"code": "Kvartal", "values": ["1", "2", "3", "4"]}], lambda c: 1.0)})
    rc, out = world.tool(pub, "--tables", small, "--apply", "--expect-new-series", "8")
    assert rc == T.REFUSED and "not the D2 class" in out, out


def test_apply_measures_and_merges_inside_the_store_writer_session(world):
    world.plant()
    seen = {"inside": False, "entered": 0, "compare": [], "merge": []}

    @contextlib.contextmanager
    def session():
        seen["entered"] += 1
        seen["inside"] = True
        try:
            yield
        finally:
            seen["inside"] = False
    world.mp.setattr(catalog_path, "write_session", session)
    real_cmp, real_merge = S._compare_subject, T.merge.merge_and_write
    world.mp.setattr(S, "_compare_subject", lambda *a, **k: seen["compare"].append(seen["inside"]) or real_cmp(*a, **k))
    world.mp.setattr(T.merge, "merge_and_write",
                     lambda *a, **k: seen["merge"].append(seen["inside"]) or real_merge(*a, **k))
    rc, out = world.tool(tt_pub(), "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    assert rc == T.OK, out
    assert seen["entered"] == 1 and seen["compare"] == [True] and seen["merge"] == [True], seen


def test_a_store_root_copy_needs_the_local_backend(world):
    world.mp.setenv("AQUEDUCT_BACKEND", "r2")
    pub = tt_pub()
    rc, out = world.tool(pub, "--tables", TT)
    assert rc == T.REFUSED and "AQUEDUCT_BACKEND=local" in out and pub.gets == [], out


# ---- the census --------------------------------------------------------------------------------------------
SMALL = "majandus/x/SMALL.PX"           # D1 shape, whole history under MAX_CELLS
WIDE = "majandus/x/WIDE.PX"             # D1 shape, ONE period over MAX_CELLS (the table's grain is aggregate)
MONTHLY = "majandus/x/MONTHLY.PX"       # same cells as the class, but a monthly time axis: not D1
NOTIME = "rahvaloendus/x/NOTIME.PX"
GONE = "majandus/x/GONE.PX"


def census_tables():
    months = [f"{Y - 1}M{m:02d}" for m in range(1, 13)] + [f"{Y}M{m:02d}" for m in range(1, 13)]
    return {
        TT: (tt_vars(), None),
        SMALL: ([{"code": "Aasta", "time": True, "values": [str(Y - 1), str(Y)]},
                 {"code": "Kvartal", "values": ["1", "2", "3", "4"]}], None),
        WIDE: ([{"code": "Aasta", "time": True, "values": [str(Y - 1), str(Y)]},
                {"code": "Kvartal", "values": ["1", "2", "3", "4"]},
                {"code": "Kaup", "values": [str(i) for i in range(30000)]}], None),
        MONTHLY: ([{"code": "Vaatlusperiood", "time": True, "values": months}, {"code": REGION, "values": REG},
                   {"code": "Sugu", "values": SEX}, {"code": AGE, "values": AGES},
                   {"code": "Haridus", "values": [str(i) for i in range(15)]}], None),
        NOTIME: ([{"code": "Sugu", "values": SEX}, {"code": REGION, "values": REG}], None),
    }


def _verdicts(out_dir):
    tsv = sorted(out_dir.glob("*_census.tsv"))
    assert len(tsv) == 1, tsv
    with open(tsv[0], encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh, delimiter="\t"))
    assert rows[0][:2] == ["table", "verdict"]
    return {r[0]: r for r in rows[1:]}


def test_census_lists_the_planted_class_and_not_the_negative_controls(world):
    world.use(FULL)                     # the real MAX_CELLS: the controls are sized against 100,000
    pub = Pub(census_tables(), no_post=True)
    rc, out = world.tool(pub, "--census", "--tables", TT, SMALL, WIDE, MONTHLY, NOTIME, GONE)
    assert rc == T.OK, out
    v = _verdicts(world.out)
    assert {p: r[1] for p, r in v.items()} == {TT: "MEMBER", SMALL: "OUTSIDE", WIDE: "OUTSIDE",
                                              MONTHLY: "EXCLUDED_NOT_D1", NOTIME: "NO_TIME", GONE: "GONE"}, v
    assert v[TT][2:6] == ["True", "14", "15756", "220584"], v[TT]
    assert pub.posts == [] and sorted(pub.gets) == sorted([TT, SMALL, WIDE, MONTHLY, NOTIME, GONE])
    assert "MEMBER (D1-shape, per period <= MAX_CELLS < whole) 1" in out and "EXCLUDED (same cells, not " \
        "D1-shape) 1" in out, out


def test_census_crawl_walks_the_tree_with_gets_only(world):
    folders = {"": [{"id": "sotsiaalelu", "type": "l"}, {"id": "majandus", "type": "l"}],
               "sotsiaalelu": [{"id": "tooturg", "type": "l"}],
               "sotsiaalelu/tooturg": [{"id": "tootud", "type": "l"}],
               "sotsiaalelu/tooturg/tootud": [{"id": "luhiajastatistika", "type": "l"}],
               "sotsiaalelu/tooturg/tootud/luhiajastatistika": [{"id": "TT065.PX", "type": "t"}],
               "majandus": [{"id": "x", "type": "l"}],
               "majandus/x": [{"id": "SMALL.PX", "type": "t"}]}
    world.use(FULL)
    tabs = census_tables()
    pub = Pub({TT: tabs[TT], SMALL: tabs[SMALL]}, folders=folders, no_post=True)
    rc, out = world.tool(pub, "--census", "--crawl")
    assert rc == T.OK, out
    assert {p: r[1] for p, r in _verdicts(world.out).items()} == {TT: "MEMBER", SMALL: "OUTSIDE"}
    crawled = json.loads(next(world.out.glob("*_catalog_crawled.json")).read_text(encoding="utf-8"))
    assert sorted(t["path"] for t in crawled) == sorted([TT, SMALL])


def test_census_with_an_unreachable_table_is_a_lower_bound(world):
    world.use(FULL)
    pub = Pub(census_tables(), no_post=True, unreachable={SMALL})
    rc, out = world.tool(pub, "--census", "--tables", TT, SMALL)
    assert rc == T.FETCH_FAILED and "CENSUS INCOMPLETE" in out, out
    assert _verdicts(world.out)[SMALL][1] == "UNREACHABLE"


def test_census_takes_no_apply(world):
    pub = Pub(census_tables(), no_post=True)
    rc, out = world.tool(pub, "--census", "--tables", TT, "--apply")
    assert rc == T.REFUSED and pub.gets == [], out
