"""fhfa: the CSV phase reads a DIFF-MEASURED changed set, so a routine rebuild books no debt (2026-11-11).

Before: every fhfa rebuild overwrote all 9 cubes, reported every series as a cursor, hit CURSOR_CAP
(89,706 series vs 50,000), set Result.cursor_cap_hit and booked full_rederive_owed - on EVERY rebuild,
the forced 30-day one included, whose printed payer (derive_csv_bulk) cannot run on fhfa's wide store.
The 882 hpi_at_3zip_quarterly ids sat beyond the cap on every pass (run 40873456722, 2026-10-29).

Now update() reads the previous cubes through blob BEFORE the rebuild, diffs each cube per
(series_key, obs_date) after it, and reports Result.changed_keys = {"<cube>:<key>": max date}. Any
failure or bound overrun falls back to the old flag, which books the debt.

  T1  identical rebuild -> {} and no flag; run_once books no owed row and derives nothing
  T2  same-length revision -> exactly the revised keys (kills a max-date-only diff)
  T3  NULL<->value detected, NULL==NULL and NaN==NaN not (kills '<>' / string-concat comparisons)
  T4  an obs deleted inside a continuing series is reported; an old-only series is counted, not reported
  T5  an absent previous cube reports that whole cube; another cube still diffs exactly
  T6  under a fake r2 store the previous cubes are read through blob.read_table BEFORE build()
  T7  diff failure / unreadable previous cube / bound overrun -> changed_keys None, flag set, debt booked
  T8  a complete changed set with a CAPPED cursor set (50,010 series) books nothing
  T9  a changed set holding only an uncatalogued key demotes for that run (today's behaviour, pinned)
  T10 existing fhfa pins still hold (cursor order, WU-2b call-site pins, no cap= keyword)
"""
from __future__ import annotations

import datetime as dt
import math
import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import blob as blob_mod, config, derive, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.fetchers import fhfa as F  # noqa: E402
from updater.strategies.fetchers import _common  # noqa: E402

D = [dt.date(2026, m, 1) for m in range(1, 7)]


def _master(keys=("K1", "K2", "K3", "K4", "K5"), dates=D, nsa=None, sa=None, drop=()):
    rows = [(k, d) for k in keys for d in dates if (k, d) not in set(drop)]
    n = len(rows)
    nsa = nsa or {}
    sa = sa or {}
    return pa.table({
        "dataset": pa.array(["hpi_master"] * n),
        "series_key": pa.array([k for k, _ in rows]),
        "obs_date": pa.array([d for _, d in rows], type=pa.date32()),
        # values depend on (key, date) only, so dropping a row never moves another row's value
        "index_nsa": pa.array([nsa.get((k, d), 100.0 + 0.01 * d.toordinal() + len(k)) for k, d in rows],
                              type=pa.float64()),
        "index_sa": pa.array([sa.get((k, d), 200.0 + 0.01 * d.toordinal() + len(k)) for k, d in rows],
                             type=pa.float64()),
        "rstderr": pa.array([None] * n, type=pa.float64()),
    })


def _tract(n_keys=3, hpi1990=None):
    keys = [f"T{i:06d}" for i in range(n_keys)]
    dates = [dt.date(2024, 12, 31), dt.date(2025, 12, 31)]
    rows = [(k, d) for k in keys for d in dates]
    hpi1990 = hpi1990 or {}
    return pa.table({
        "dataset": pa.array(["annual_tract"] * len(rows)),
        "series_key": pa.array([k for k, _ in rows]),
        "obs_date": pa.array([d for _, d in rows], type=pa.date32()),
        "hpi": pa.array([1.0] * len(rows), type=pa.float64()),
        "hpi_1990": pa.array([hpi1990.get((k, d), 2.0) for k, d in rows], type=pa.float64()),
        "hpi_2000": pa.array([3.0] * len(rows), type=pa.float64()),
        "annual_change": pa.array([0.5] * len(rows), type=pa.float64()),
    })


def _series(name, keys):
    return pa.table({"dataset": [name] * len(keys), "series_key": list(keys)})


class Rig:
    """fhfa.update() over a local store in tmp_path; ig.download/build and the HEAD probes are faked."""

    def __init__(self, tmp_path, monkeypatch):
        self.out = tmp_path / "store" / "fhfa"
        self.out.mkdir(parents=True)
        self.mp = monkeypatch
        self.events = []
        self.cubes = {}
        self.n = 0
        for k in list(os.environ):
            if k.startswith("AQUEDUCT_") or k.startswith("GITHUB_"):
                monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
        monkeypatch.setattr(config, "source_dir", lambda s: str(self.out))
        monkeypatch.setattr(F.ig, "download", lambda: None)
        monkeypatch.setattr(F.ig, "build", self._build)

    def _build(self):
        self.events.append(("build",))
        for name, tbl in self.cubes.items():
            pq.write_table(tbl, str(self.out / f"{name}.parquet"))
            pq.write_table(_series(name, sorted(set(tbl.column("series_key").to_pylist()))),
                           str(self.out / f"{name}__series.parquet"))

    def rebuild(self, cubes):
        """One rebuild of `cubes` ({cube: table}); the validators move every call, so it always rebuilds."""
        self.cubes = dict(cubes)
        self.n += 1
        self.mp.setattr(F, "_validators", lambda sess, n=self.n: {"hpi_master.csv": f"v{n}"})
        return F.update(None, None)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    return Rig(tmp_path, monkeypatch)


def _keys(res):
    return sorted((res.changed_keys or {}).keys())


# --------------------------------------------------------------------------------------------- T1
def test_t1_an_identical_rebuild_reports_nothing_changed(rig):
    rig.rebuild({"hpi_master": _master(), "annual_tract": _tract()})
    res = rig.rebuild({"hpi_master": _master(), "annual_tract": _tract()})
    assert res.status == "ok", res.error
    assert res.changed_keys == {} and res.cursor_cap_hit is False
    assert res.series_cursors, "the freshness cursors are still reported"


# --------------------------------------------------------------------------------------------- T2
def test_t2_a_same_length_revision_reports_exactly_the_revised_keys(rig):
    """The case MAX_AGE_DAYS exists for: values move, no row is added, no max date moves."""
    rig.rebuild({"hpi_master": _master(), "annual_tract": _tract()})
    rev = {("K2", D[2]): 1.5, ("K4", D[0]): 1.5, ("K5", D[5]): 1.5}
    res = rig.rebuild({"hpi_master": _master(nsa=rev), "annual_tract": _tract()})
    assert _keys(res) == ["hpi_master:K2", "hpi_master:K4", "hpi_master:K5"], res.changed_keys
    assert res.changed_keys["hpi_master:K2"] == D[-1].isoformat()
    assert res.cursor_cap_hit is False


def test_t2b_a_new_period_reports_its_key(rig):
    rig.rebuild({"hpi_master": _master()})
    res = rig.rebuild({"hpi_master": _master(dates=D + [dt.date(2026, 7, 1)], keys=("K1", "K2", "K3", "K4", "K5"))})
    # every key gained a row, so every key changed
    assert _keys(res) == [f"hpi_master:K{i}" for i in range(1, 6)]
    assert res.changed_keys["hpi_master:K1"] == "2026-07-01"


# --------------------------------------------------------------------------------------------- T3
def test_t3_null_to_value_and_value_to_null_are_changes(rig):
    t1, t2 = dt.date(2024, 12, 31), dt.date(2025, 12, 31)
    rig.rebuild({"annual_tract": _tract(hpi1990={("T000000", t1): None, ("T000001", t2): 7.0})})
    res = rig.rebuild({"annual_tract": _tract(hpi1990={("T000000", t1): 9.0, ("T000001", t2): None})})
    assert _keys(res) == ["annual_tract:T000000", "annual_tract:T000001"], res.changed_keys


def test_t3b_null_equals_null_and_nan_equals_nan(rig):
    t1 = dt.date(2024, 12, 31)
    same = {("T000000", t1): None, ("T000002", t1): math.nan}
    rig.rebuild({"annual_tract": _tract(hpi1990=same)})
    res = rig.rebuild({"annual_tract": _tract(hpi1990=same)})
    assert res.changed_keys == {}, res.changed_keys


# --------------------------------------------------------------------------------------------- T4
def test_t4_an_obs_deleted_inside_a_continuing_series_is_a_change(rig):
    rig.rebuild({"hpi_master": _master()})
    res = rig.rebuild({"hpi_master": _master(drop=[("K3", D[1])])})
    assert _keys(res) == ["hpi_master:K3"], res.changed_keys


def test_t4b_a_series_only_in_the_old_cube_is_counted_not_reported(rig, capsys):
    rig.rebuild({"hpi_master": _master()})
    capsys.readouterr()
    res = rig.rebuild({"hpi_master": _master(keys=("K1", "K2", "K3", "K4"))})
    assert res.changed_keys == {}, res.changed_keys
    assert "1 series present only in the previous cube" in capsys.readouterr().out


# --------------------------------------------------------------------------------------------- T5
def test_t5_an_absent_previous_cube_reports_that_whole_cube_only(rig, capsys):
    rig.rebuild({"hpi_master": _master()})
    capsys.readouterr()
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 0.0}), "annual_tract": _tract(n_keys=3)})
    assert _keys(res) == (["annual_tract:T000000", "annual_tract:T000001", "annual_tract:T000002",
                           "hpi_master:K1"]), res.changed_keys
    assert "annual_tract: no previous cube in the store" in capsys.readouterr().out
    assert res.cursor_cap_hit is False


# --------------------------------------------------------------------------------------------- T6
def test_t6_previous_cubes_are_read_through_blob_before_the_build(rig, monkeypatch):
    """A fake r2 store: the runner's local dir is EMPTY, the previous cubes exist only behind blob."""
    old = {"hpi_master": _master(), "annual_tract": _tract()}
    reads = []

    def read_table(path, columns=None):
        name = os.path.basename(path)[:-len(".parquet")]
        rig.events.append(("read", name))
        reads.append(name)
        if name not in old:
            raise FileNotFoundError(path)
        return old[name]
    monkeypatch.setattr(F.blob, "list_parquets",
                        lambda d, recursive=False: sorted(f"{n}{s}.parquet" for n in old for s in ("", "__series")))
    monkeypatch.setattr(F.blob, "read_table", read_table)
    monkeypatch.setattr(F.blob, "read_bytes", lambda p: None)
    monkeypatch.setattr(F.blob, "write_bytes_atomic", lambda p, b: None)
    monkeypatch.setattr(F.blob, "publish_file", lambda p: 0)
    assert not list(rig.out.iterdir()), "the runner's local dir starts empty"
    res = rig.rebuild({"hpi_master": _master(nsa={("K2", D[3]): 5.0}), "annual_tract": _tract()})
    assert sorted(reads) == ["annual_tract", "hpi_master"], "every previous obs cube, never a __series file"
    first_build = rig.events.index(("build",))
    assert all(i < first_build for i, e in enumerate(rig.events) if e[0] == "read"), rig.events
    assert _keys(res) == ["hpi_master:K2"], res.changed_keys


# --------------------------------------------------------------------------------------------- T7
def _owed_after_run(tmp_path, monkeypatch, res, catalogued):
    """The REAL run_once over a fake strategy returning `res`; returns (owed rows, ids derived)."""
    for k in list(os.environ):
        if k.startswith("GITHUB_") or k.startswith("AQUEDUCT_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_cutover_flag"))
    cat = tmp_path / "catalog.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(i, "fhfa") for i in catalogued])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    data = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    orchestrate._REG_ENTRIES = None
    asked = []

    def fake_derive(ids, blob, **kw):
        ids = list(dict.fromkeys(ids))
        asked.extend(ids)
        return {"put": len(ids), "put_ids": ids, "failed": [], "deferred": 0, "deferred_ids": [],
                "failed_reasons": {}, "skipped_identical": 0, "deferred_large": {}}
    monkeypatch.setattr(derive, "derive_and_put", fake_derive)

    class _S:
        def is_due(self, unit, us, **k):
            return True

        def detect_change(self, unit, us):
            return "v2"

        def run(self, unit, since=None):
            return res
    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: _S())
    st = StateStore(path=str(tmp_path / "state.db"))
    orchestrate.run_once(sources=["fhfa"], store=st, blob=blob_mod.R2Blob())
    return [r for r in st.full_rederives_owed() if r["source_id"] == "fhfa"], asked


@pytest.mark.parametrize("how", ["diff_raises", "prior_unreadable", "over_bound"])
def test_t7_any_failure_falls_back_to_the_flag_and_books_the_debt(rig, monkeypatch, tmp_path, how):
    rig.rebuild({"hpi_master": _master()})
    if how == "diff_raises":
        def boom(old, new):
            raise RuntimeError("duckdb exploded")
        monkeypatch.setattr(F, "_changed_in_cube", boom)
    elif how == "prior_unreadable":
        def bad(path, columns=None):
            raise OSError("R2 GET failed after retries")
        monkeypatch.setattr(F.blob, "read_table", bad)
    else:
        monkeypatch.setattr(F, "CHANGED_KEYS_BOUND", 2)
    monkeypatch.setattr(F, "CURSOR_CAP", 3)            # the flag carries the (here: tiny) cap check
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 0.0, ("K2", D[0]): 0.0, ("K3", D[0]): 0.0})})
    assert res.changed_keys is None, res.changed_keys
    assert res.cursor_cap_hit is True
    owed, _ = _owed_after_run(tmp_path, monkeypatch, res, [f"fhfa:hpi_master:K{i}" for i in range(1, 6)])
    assert len(owed) == 1 and "cursor cap hit" in owed[0]["note"], owed


def test_t1b_run_once_books_no_debt_and_derives_nothing_for_an_identical_rebuild(rig, monkeypatch, tmp_path):
    rig.rebuild({"hpi_master": _master()})
    res = rig.rebuild({"hpi_master": _master()})
    owed, asked = _owed_after_run(tmp_path, monkeypatch, res, [f"fhfa:hpi_master:K{i}" for i in range(1, 6)])
    assert owed == [] and asked == [], (owed, asked)


# --------------------------------------------------------------------------------------------- T8
def test_t8_a_complete_changed_set_with_capped_cursors_books_nothing(rig, monkeypatch, tmp_path):
    n = _common.CURSOR_CAP + 10
    rig.rebuild({"hpi_master": _master(), "annual_tract": _tract(n_keys=n)})
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 0.0}), "annual_tract": _tract(n_keys=n)})
    assert len(res.series_cursors) == _common.CURSOR_CAP, "the cursors really are capped"
    assert _keys(res) == ["hpi_master:K1"] and res.cursor_cap_hit is False
    owed, asked = _owed_after_run(tmp_path, monkeypatch, res,
                                  [f"fhfa:hpi_master:K{i}" for i in range(1, 6)] + ["fhfa:annual_tract:T000000"])
    assert owed == [], owed
    assert asked == ["fhfa:hpi_master:K1"], asked


def test_t8b_the_zero_mapped_classifier_sees_an_untruncated_set():
    res = types.SimpleNamespace(changed_keys={"hpi_master:NEW": "2026-07-01"}, cursor_cap_hit=False)
    migrated = res.changed_keys is not None
    sat = ((not migrated) and 1 >= _common.CURSOR_CAP) or bool(getattr(res, "cursor_cap_hit", None))
    assert sat is False
    src = open(os.path.join(os.path.dirname(orchestrate.__file__), "orchestrate.py"), encoding="utf-8").read()
    assert ('cap_saturated=((not migrated) and len(unmapped) >= _CCAP)\n'
            '                or bool(getattr(res, "cursor_cap_hit", None))') in src, \
        "the orchestrator's saturation predicate changed - re-check fhfa's flag contract"


# --------------------------------------------------------------------------------------------- T9
def test_t9_a_pass_changing_only_an_uncatalogued_key_demotes_that_run(rig, monkeypatch, tmp_path):
    """Residual 3, pinned as today's behaviour: fhfa is not catalog_scope: subset."""
    rig.rebuild({"hpi_master": _master()})
    res = rig.rebuild({"hpi_master": _master(keys=("K1", "K2", "K3", "K4", "K5", "NEW"))})
    assert _keys(res) == ["hpi_master:NEW"]
    unit = types.SimpleNamespace(key="fhfa/_all", source_id="fhfa", unit_id="_all")
    cat = tmp_path / "cat.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(f"fhfa:hpi_master:K{i}", "fhfa") for i in range(1, 6)])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    monkeypatch.setattr(orchestrate.config, "BACKEND", "r2")
    orchestrate._REG_ENTRIES = None
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, None, store=None)
    assert note and note.startswith("csv coherence unmet"), note


# --------------------------------------------------------------------------------------------- T10
def test_t10_the_existing_pins_still_hold():
    src = open(F.__file__, encoding="utf-8").read()
    assert 'n != "hpi_master.parquet"' in src
    assert "cap_hit = len(cursors) >= CURSOR_CAP" in src
    assert "res.cursor_cap_hit = cap_hit" in src
    assert "one wasted rebuild" not in src, "the docstring's cost claim was corrected (30-48 min csv phase)"
