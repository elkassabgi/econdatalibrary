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
The CI writer gate is stubbed CLEAR (a test never reaches GitHub); R2 is emulated by FakeR2 on a folder, with
the ETag = md5 of the bytes (a single-part R2 PUT) and put_if as the only write it accepts.
Review AR of PR #340 (F:/work/econ_finish/estonia_repair_pr/review/VERDICT.md): C1-C3 and m1-m6 are pinned below,
one test per gate and per check after the write, so each of the review's mutants turns one test red.
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
import shutil
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import catalog_path, cutover  # noqa: E402
from updater import blob, config  # noqa: E402
from updater.strategies.fetchers import stat_estonia as S  # noqa: E402

_spec = importlib.util.spec_from_file_location("stat_estonia_grain_repair",
                                               os.path.join(ROOT, "tools", "stat_estonia_grain_repair.py"))
T = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(T)
REAL_GATE = T._ci_writer_gate

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
    no_post: a data POST fails the test (the census reads metadata only). unreachable: the GET raises
    TransientError; errors: the GET raises DefinitiveError (an unexpected HTTP code). reject / transient: the
    n-th POST (0-based) answers 400 (None) / raises TransientError."""

    def __init__(self, tables, folders=None, no_post=False, unreachable=(), errors=(), reject=(), transient=()):
        self.tables, self.folders, self.no_post = tables, folders or {}, no_post
        self.unreachable, self.errors, self.gets, self.posts = set(unreachable), set(errors), [], []
        self.reject, self.transient = set(reject), set(transient)

    def meta(self, sess, url):
        path = url[len(BASE):].lstrip("/")
        self.gets.append(path)
        if path in self.unreachable:
            raise S.TransientError("stub 503")
        if path in self.errors:
            raise S.DefinitiveError("stub HTTP 418")
        if path in self.folders:
            return list(self.folders[path])
        return {"variables": [dict(v) for v in self.tables[path][0]]} if path in self.tables else None

    def post(self, sess, url, body):
        if self.no_post:
            raise AssertionError(f"a data POST was sent: {url}")
        path = url[len(BASE):].lstrip("/")
        i = len(self.posts)
        self.posts.append((path, body))
        if i in self.transient:
            raise S.TransientError("stub 503 on a data POST")
        if i in self.reject:
            return None
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
    monkeypatch.setattr(T, "_ci_writer_gate", lambda: (0, "CLEAR: stub"))        # never GitHub from a test
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

    def raw(pub, *args, out_dir=None):
        """main() with exactly these arguments (no --store-root added)."""
        monkeypatch.setattr(S, "_get_meta", pub.meta)
        monkeypatch.setattr(S, "_post_data", pub.post)
        capsys.readouterr()
        rc = T.main(list(args) + ["--out-dir", str(out_dir or out), "--rate", "0.25"], session=object())
        return rc, capsys.readouterr().out

    def tool(pub, *args, out_dir=None):
        return raw(pub, *args, "--store-root", str(store_root), out_dir=out_dir)

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

    box.plant, box.tool, box.raw, box.update, box.receipt = plant, tool, raw, update, receipt
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
    assert "CELL class - the stored grain is not checked) 1;" in out and "EXCLUDED (same cells, not " \
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


# ---- review C3: the all-or-nothing chunk rule ---------------------------------------------------------------
@pytest.mark.parametrize("how", ["reject", "transient"])
def test_a_failed_chunk_ends_the_table_and_nothing_merges(world, how):
    """A rejected (400/403) or transient first chunk ends the table at once: no later POST, nothing merges,
    exit 1. Skipping the chunk instead would POST the other six and then meet `held` (exit 2)."""
    p = world.plant()
    before = _sha(p)
    pub = Pub({TT: (tt_vars(reg=SCALED.reg), tt_value)}, **{how: {0}})
    rc, out = world.tool(pub, "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    assert rc == T.FETCH_FAILED, out
    assert ("rejected (400/403)" if how == "reject" else "data POST 1 of 7 failed") in out, out
    assert len(pub.posts) == 1, "no POST after the failed chunk"
    assert _sha(p) == before


# ---- review C3: the exact expectation ------------------------------------------------------------------------
@pytest.mark.parametrize("delta", [-1, 1])
def test_expect_new_series_is_an_exact_equality(world, delta):
    p = world.plant()
    before = _sha(p)
    rc, out = world.tool(tt_pub(), "--tables", TT, "--apply", "--expect-new-series", str(N_NEW + delta))
    assert rc == T.REFUSED and f"!= the {N_NEW:,} new series measured now" in out, out
    assert _sha(p) == before


# ---- review C3: every check after the write is shown to fail -------------------------------------------------
def _wrap_merge(world, after):
    """merge_and_write as it is, then `after(result)` - to plant what a broken merge would leave."""
    real = T.merge.merge_and_write

    def wrapped(*a, **k):
        return after(real(*a, **k))
    world.mp.setattr(T.merge, "merge_and_write", wrapped)


def _apply(world):
    rc, out = world.tool(tt_pub(), "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    return rc, out, [ln for ln in out.splitlines() if "READ-BACK FAILED" in ln]


def test_a_merge_row_count_that_does_not_add_up_is_a_defect(world):
    world.plant()
    _wrap_merge(world, lambda r: (r[0] + 1, r[1], r[2]))
    rc, out, bad = _apply(world)
    assert rc == T.DEFECT and any("merge: " in b and "added !=" in b for b in bad), out
    assert world.receipt()["result"] == "read-back failed"


def test_a_changed_key_report_that_does_not_match_is_a_defect(world):
    world.plant()
    _wrap_merge(world, lambda r: (r[0], r[1], dict(list(r[2].items())[1:])))
    rc, out, bad = _apply(world)
    assert rc == T.DEFECT and any("changed-key report" in b for b in bad), out


def test_a_store_that_reads_back_another_value_is_a_defect(world):
    p = world.plant()

    def corrupt(r):
        t = pq.read_table(p)
        vals = t.column("value").to_pylist()
        i = t.column("series_key").to_pylist().index(agg_key("00"))
        vals[i] += 1.0
        pq.write_table(t.set_column(2, "value", pa.array(vals, pa.float64())), p)
        return r
    _wrap_merge(world, corrupt)
    rc, out, bad = _apply(world)
    assert rc == T.DEFECT and any("store read-back: 0 publisher row(s) missing, 1 with another value" in b
                                  for b in bad), out


def test_a_derived_csv_that_differs_is_a_defect(world):
    world.plant()
    from core import derive_csv
    real = derive_csv._series_csv_bytes
    world.mp.setattr(derive_csv, "_series_csv_bytes",
                     lambda cid: b"".join(real(cid).splitlines(keepends=True)[:-1]))    # one row short
    rc, out, bad = _apply(world)
    assert rc == T.DEFECT and any("derived CSV read-back" in b for b in bad), out
    assert not any("store read-back" in b for b in bad), "the store is right; only the CSV differs"


def test_a_merge_that_would_shrink_the_file_is_refused_by_the_exact_floor(world):
    """Another table's repeated row in the same file collapses in the merge's dedup: the file would end one row
    short of stored + added. The floor is exactly stored + added, so the merge refuses and nothing changes."""
    p = world.plant([(f"{CTRL}:Sugu=1", END(Y - 1), 5.0)])
    before = _sha(p)
    rc, out, _ = _apply(world)
    assert rc == T.REFUSED and "the merge refused (refusing shrink" in out, out
    assert _sha(p) == before


def test_a_crash_after_the_write_leaves_a_receipt_and_the_ids_file(world):
    """m2: an exception after the write is re-raised, but only after the receipt and the ids file are written."""
    p = world.plant()

    def boom(*a, **k):
        raise RuntimeError("read-back blew up")
    world.mp.setattr(T, "read_back_store", boom)
    with pytest.raises(RuntimeError, match="read-back blew up"):
        world.tool(tt_pub(), "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    r = world.receipt()
    assert r["result"].startswith("crashed during or after the write: RuntimeError"), r["result"]
    assert next(world.out.glob("*.ids.txt")).read_text(encoding="utf-8") == f"stat_estonia:{PRE}\n"
    assert len(_rows(p, PRE)) == SCALED.n_fetched, "the write had happened: the receipt says so"
    assert T._AFTER_WRITE == {}


# ---- review C2: changed rows ---------------------------------------------------------------------------------
REVISED = {(str(Y - 5), "00"), (str(Y - 6), "3"), (str(Y - 7), "7")}


def _revised_value(c):
    v = tt_value(c)
    if (c["Aasta"], c["Kuu"]) in REVISED and c[REGION] == "1" and c["Sugu"] == "1" and c[AGE] == "1":
        return v + 1.0
    return v


def test_changed_rows_are_listed_and_need_their_exact_count(world):
    p = world.plant()
    pub = Pub({TT: (tt_vars(reg=SCALED.reg), _revised_value)})
    rc, out = world.tool(pub, "--tables", TT)
    r = world.receipt()
    assert rc == T.OK and r["changed"] == 3 and len(r["changed_examples"]) == 3, (rc, r)
    k, d, old, new = r["changed_examples"][0]
    assert k == agg_key("00") and new == old + 1.0, r["changed_examples"]
    assert out.count("changed row ") == 3 and "--expect-changed 3" in out, out
    before = _sha(p)
    for args in ((), ("--expect-changed", "0"), ("--expect-changed", "2")):
        rc, out = world.tool(pub, "--tables", TT, "--apply", "--expect-new-series", str(N_NEW), *args)
        assert rc == T.REFUSED and "changed row(s) measured now" in out, out
        assert _sha(p) == before
    rc, out = world.tool(pub, "--tables", TT, "--apply", "--expect-new-series", str(N_NEW), "--expect-changed", "3")
    assert rc == T.OK, out
    assert _rows(p, PRE)[(agg_key("00"), END(Y - 5))] == _revised_value(
        {"Aasta": str(Y - 5), "Kuu": "00", REGION: "1", "Sugu": "1", AGE: "1"})


# ---- review m1, and the absent subject file ------------------------------------------------------------------
def test_a_dry_run_exits_2_when_apply_would_be_refused(world):
    world.plant([(agg_key("00"), END(Y - 14), 1.0)])                     # held 1
    rc, out = world.tool(tt_pub(), "--tables", TT)
    assert rc == T.REFUSED and "--apply would be REFUSED: held 1:" in out and "apply with:" not in out, out


def test_an_absent_subject_file_refuses(world):
    rc, out = world.tool(tt_pub(), "--tables", TT)
    assert rc == T.REFUSED and "is absent from the store" in out, out


# ---- review C1 and m3: who else writes the file ---------------------------------------------------------------
def test_apply_refuses_when_the_ci_writer_gate_is_not_clear(world):
    p = world.plant()
    before = _sha(p)
    world.mp.setattr(T, "_ci_writer_gate", lambda: (3, "BLOCKED: updater-daily run in flight"))
    pub = tt_pub()
    rc, out = world.tool(pub, "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    assert rc == T.REFUSED and "CI writer gate is not CLEAR" in out and "BLOCKED: updater-daily" in out, out
    assert pub.gets == [] and pub.posts == [], "the gate runs FIRST"
    assert _sha(p) == before


def test_the_gate_runs_tools_ci_writer_gate_and_reads_its_exit_code(monkeypatch):
    import subprocess
    seen = []

    def fake_run(args, **kw):
        seen.append((args, kw))
        return types.SimpleNamespace(returncode=3, stdout="a note\nBLOCKED: heavy pending\n", stderr="")
    monkeypatch.setattr(subprocess, "run", fake_run)
    assert REAL_GATE() == (3, "BLOCKED: heavy pending")
    args, kw = seen[0]
    assert args[0] == sys.executable and args[1] == os.path.join(ROOT, "tools", "ci_writer_gate.py") and len(args) == 2
    assert kw["timeout"] == T.GATE_TIMEOUT_S

    def no_run(*a, **k):
        raise FileNotFoundError("no gh")
    monkeypatch.setattr(subprocess, "run", no_run)
    rc, why = REAL_GATE()
    assert rc is None and "FileNotFoundError" in why, "a gate that did not run is not CLEAR"


def test_apply_before_t0_needs_r2_or_a_store_root(world):
    world.plant()
    pub = tt_pub()
    rc, out = world.raw(pub, "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    assert rc == T.REFUSED and "before T0 the store of record is R2" in out and pub.gets == [], out


def test_apply_after_t0_refuses_the_frozen_r2_copy(world):
    (world.tmp / "CUTOVER").write_text("")
    assert cutover.is_cut_over()
    world.mp.setenv("AQUEDUCT_BACKEND", "r2")
    pub = tt_pub()
    rc, out = world.raw(pub, "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    assert rc == T.REFUSED and "after T0 the R2 copy is frozen" in out and pub.gets == [], out
    assert catalog_path._held is None


class FakeR2(blob.LocalBlob):
    """R2 on a folder: the ETag is the md5 of the bytes (a single-part PUT), put_if is the conditional write, and an
    unconditional PUT fails the test. before_put(fake, key) runs first - another writer landing at that moment."""

    def __init__(self, root, before_put=None):
        super().__init__(root=root)
        self.before_put, self.puts = before_put, []

    def put_atomic(self, key, data, **kw):
        raise AssertionError(f"an unconditional PUT reached the store of record: {key}")

    def put_if(self, key, data, *, if_match=None, if_none_match=False):
        if self.before_put:
            self.before_put(self, key)
        cur = self.etag(key)
        self.puts.append((key, if_match, cur))
        if if_match is None or cur != if_match.strip('"'):
            return None
        blob.LocalBlob.put_atomic(self, key, data)
        return self.etag(key)


def _r2(world, before_put=None):
    """The store of record emulated: the planted subject file in a bucket folder, an absent local mirror."""
    subject = world.plant()
    obj = world.tmp / "bucket" / "clean_full" / S.SOURCE / "sotsiaalelu.parquet"
    obj.parent.mkdir(parents=True)
    shutil.copy(subject, obj)
    data_root = world.tmp / "mirror" / "data" / "clean_full"
    fake = FakeR2(str(world.tmp / "bucket"), before_put)
    world.mp.setattr(config, "DATA_ROOT", str(data_root))
    world.mp.setattr(blob, "_r2_routed", lambda: fake)
    world.mp.setenv("AQUEDUCT_BACKEND", "r2")
    return fake, obj, data_root / S.SOURCE / "sotsiaalelu.parquet"


def test_apply_under_r2_writes_one_conditional_put_and_mirrors_it(world):
    fake, obj, mirror = _r2(world)
    etag0 = hashlib.md5(obj.read_bytes()).hexdigest()
    rc, out = world.raw(tt_pub(), "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    assert rc == T.OK, out
    assert fake.puts == [("clean_full/stat_estonia/sotsiaalelu.parquet", etag0, etag0)], fake.puts
    assert len(_rows(str(obj), PRE)) == SCALED.n_fetched
    assert mirror.read_bytes() == obj.read_bytes(), "the local mirror holds the bytes R2 holds"
    r = world.receipt()
    assert (r["etag_at_measure"], r["etag_after_write"]) == (etag0, hashlib.md5(obj.read_bytes()).hexdigest()), r
    assert r["ci_writer_gate"]["exit"] == 0 and r["result"] == "merged and read back", r


def test_apply_under_r2_refuses_when_another_writer_moved_the_object(world):
    def other_writer(fake, key):
        t = pq.read_table(fake._path(key))
        pq.write_table(t.slice(1), fake._path(key))        # a CI pass lands between the measure and the PUT
    fake, obj, mirror = _r2(world, before_put=other_writer)
    rc, out = world.raw(tt_pub(), "--tables", TT, "--apply", "--expect-new-series", str(N_NEW))
    assert rc == T.REFUSED and "changed after it was measured" in out, out
    assert len(fake.puts) == 1 and fake.puts[0][1] != fake.puts[0][2], fake.puts
    assert len(_rows(str(obj), PRE)) < N_STORED, "the other writer's file stands; this tool wrote nothing"
    assert not mirror.exists()
    assert world.receipt()["result"] == "refused: the object changed after the measure"


# ---- review m5 and m6: the census's errors, limit and resume ---------------------------------------------------
def test_census_counts_an_unexpected_answer_and_an_unlisted_folder(world):
    world.use(FULL)
    pub = Pub(census_tables(), no_post=True, errors={SMALL})
    rc, out = world.tool(pub, "--census", "--tables", TT, SMALL)
    assert rc == T.FETCH_FAILED and "ERROR (unexpected answer) 1" in out, out
    assert _verdicts(world.out)[SMALL][1] == "ERROR"
    folders = {"": [{"id": "majandus", "type": "l"}, {"id": "gone_folder", "type": "l"}],
               "majandus": [{"id": "x", "type": "l"}], "majandus/x": [{"id": "SMALL.PX", "type": "t"}]}
    pub2 = Pub({SMALL: census_tables()[SMALL]}, folders=folders, no_post=True)
    rc, out = world.tool(pub2, "--census", "--crawl", out_dir=world.tmp / "out2")
    assert rc == T.FETCH_FAILED and "crawl folders not listed 1" in out and "CENSUS INCOMPLETE" in out, out


def test_census_limit_and_resume_continue_where_it_stopped(world):
    world.use(FULL)
    tabs = [TT, SMALL, WIDE, MONTHLY]
    res = str(world.tmp / "census_resume.json")
    pub = Pub(census_tables(), no_post=True)
    rc, out = world.tool(pub, "--census", "--tables", *tabs, "--limit", "2", "--resume", res,
                         out_dir=world.tmp / "o1")
    assert rc == T.FETCH_FAILED and "CENSUS PARTIAL: 2 table(s) not read yet" in out and pub.gets == tabs[:2], out
    pub2 = Pub(census_tables(), no_post=True)
    rc, out = world.tool(pub2, "--census", "--tables", *tabs, "--resume", res, out_dir=world.tmp / "o2")
    assert rc == T.OK and pub2.gets == tabs[2:], out
    assert {p: r[1] for p, r in _verdicts(world.tmp / "o2").items()} == {
        TT: "MEMBER", SMALL: "OUTSIDE", WIDE: "OUTSIDE", MONTHLY: "EXCLUDED_NOT_D1"}
    rc, out = world.tool(Pub(census_tables(), no_post=True), "--census", "--tables", TT, "--resume", res,
                         out_dir=world.tmp / "o3")
    assert rc == T.REFUSED and "another table list" in out, out
