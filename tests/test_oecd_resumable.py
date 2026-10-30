"""oecd is resumable (2026-10-29): checkpoint_every + bounded_merge + report_changed_flows + retry_slice +
owed_marks + flow_log on oecd.update, and an oecd tier in the orchestrator's catalogue mapping.

WHAT WAS WRONG. oecd.update called run_giant with defaults. save_state ran once, after the flow loop, and an
oecd turn on the desktop usually ends in run_local_heavy.ps1's HARD STOP (taskkill /F): no except, no
finally, no final save, no CSV step. So a killed pass lost every flow's recorded progress, and the next pass
re-selected the same flows in the same order (R273). The only timing in the log was one progress line per
25 flows (R244: no line covered the 1,792,000,000-row flow).

WHAT THESE TESTS HOLD.
  (1) A REAL process death (os._exit in a subprocess, the way a taskkill ends the pass) after flow k leaves
      _giant_state.json (read through blob) with flows 1..k-1 settled; the next pass starts AFTER flow k,
      which comes back only through the retry back-off (the R313 wedge, AR-404 stall / stall_first).
  (2) A flow merged with a change and then lost to the kill carries a `csv_owed` mark in the same row as its
      new vintage; the next pass hands it to the CSV step through the REAL orchestrate.run_once, and the
      served ids are PUT. The mark is cleared only in the final save of a pass that returns it.
  (3) The orchestrator maps a changed flow to exactly the catalogued ids the resolver serves from that file
      (the inverse of econdl._resolve._OECD_TEMPLATES, on the 28 ids measured from the catalogue); a flow
      the tier misses while the catalogue's own metadata says it is served DEMOTES and books a durable
      debt, never a green coverage note.
  (4) Owed marks: a quiet pass that carries one runs the CSV step (never no_change); a failed final save
      keeps them; they never count as this pass's progress for the capped no-progress guard.
  (5) flow_log prints a start and a done line on every outcome branch, and neither looks like the 25-flow
      progress line tools/measure_ci_lag.py reads.
  (6) owed_marks stays out of eurostat's path.
Nothing goes to the network; every path is under tmp_path.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
import subprocess
import sys
import textwrap
import types

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import blob, config, derive, health, orchestrate, registry  # noqa: E402
from updater.errors import DefinitiveError, TransientError  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.fetchers import _giant, oecd  # noqa: E402

QNA = "OECD.SDD.NAD__DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD"
PRICES = "OECD.SDD.TPS__DSD_PRICES@DF_PRICES_ALL"
QNA_FLOW_META = "OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD,1.1"
PRICES_FLOW_META = "OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL"

# The 28 catalogued oecd ids, measured 2026-10-29 from an immutable copy of the production catalogue
# (receipt: F:/work/econ_oecd_resume_tmp/measure_oecd_ids.out - 28 ids, 26 on the QNA file, 2 on PRICES,
# 0 without a template).
QOQ = ["AUS", "CAN", "CHE", "DEU", "ESP", "FRA", "GBR", "ITA", "JPN", "KOR", "MEX", "NLD", "OECD", "SWE", "USA"]
YOY = ["AUS", "CAN", "DEU", "ESP", "FRA", "GBR", "ITA", "JPN", "KOR", "OECD", "USA"]
CPI = ["GBR", "USA"]
REAL_IDS = ([f"oecd:GDP_GROWTH_QOQ:{g}" for g in QOQ] + [f"oecd:GDP_GROWTH_YOY:{g}" for g in YOY]
            + [f"oecd:CPI_YOY:{g}" for g in CPI])


def _meta_for(sid):
    ind, geo = sid.split(":")[1:]
    flow = PRICES_FLOW_META if ind == "CPI_YOY" else QNA_FLOW_META
    return json.dumps({"indicator": ind, "dataflow": flow, "ref_area": geo})


def _catalog(path, ids, metadata=True):
    con = sqlite3.connect(path)
    if metadata:
        con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, metadata TEXT)")
        con.executemany("INSERT INTO series VALUES (?,?,?)", [(i, "oecd", _meta_for(i)) for i in ids])
    else:
        con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        con.executemany("INSERT INTO series VALUES (?,?)", [(i, "oecd") for i in ids])
    con.commit()
    con.close()


def _native(sid):
    from econdl import _resolve as R
    ind, geo = sid.split(":")[1:]
    return R._OECD_TEMPLATES[ind][1].format(geo=geo)


def _tbl(keys, value=1.0):
    keys = list(keys)
    return pa.table({"series_key": pa.array(keys, pa.string()),
                     "obs_date": pa.array([dt.date(2025, 12, 31)] * len(keys), pa.date32()),
                     "value": pa.array([float(value)] * len(keys), pa.float64()),
                     "obs_status": pa.array(["A"] * len(keys), pa.string()),
                     "time_raw": pa.array(["2025"] * len(keys), pa.string())})


def _cat(flows, vintage="v1"):
    return {f: {"vintage": vintage, "filename": f + ".parquet", "agency": "A", "id": f,
                "version": "1.0", "root": "x"} for f in flows}


class _Unit:
    source_id, unit_id = "oecd", "_all"

    def __init__(self, d):
        self.out_paths = [str(d)]


def _sidecar(d):
    raw = blob.read_bytes(os.path.join(str(d), "_giant_state.json"))
    return json.loads(raw.decode("utf-8")) if raw is not None else None


@pytest.fixture
def local(monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(oecd, "RATE", 0)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)


# ---- (3) the orchestrator's map back --------------------------------------------------------------

def test_every_template_file_maps_to_every_catalogued_id_of_its_indicators(tmp_path, monkeypatch):
    """(a) of AR-404 O1, on the 28 measured ids, through the resolver's own table (imported)."""
    from econdl import _resolve as R
    _catalog(tmp_path / "catalog.db", REAL_IDS)
    con = sqlite3.connect(tmp_path / "catalog.db")
    idx = orchestrate._oecd_index(con)
    want = {}
    for sid in REAL_IDS:
        want.setdefault(R._OECD_TEMPLATES[sid.split(":")[1]][0][:-len(".parquet")], []).append(sid)
    assert {k: sorted(v) for k, v in idx.items()} == {k: sorted(v) for k, v in want.items()}
    assert sorted(idx) == [QNA, PRICES] and len(idx[QNA]) == 26 and len(idx[PRICES]) == 2
    # and every claimed id resolves, through the REAL resolver, to the file it was claimed from
    for stem in idx:
        os.makedirs(tmp_path / "data" / "oecd", exist_ok=True)
        (tmp_path / "data" / "oecd" / (stem + ".parquet")).write_bytes(b"")
    for stem, cids in idx.items():
        for cid in cids:
            res = R.resolve(cid, root=str(tmp_path / "data"))
            assert os.path.basename(res.parquet_path) == stem + ".parquet"


def test_a_changed_flow_claims_only_the_ids_served_from_its_file(tmp_path, monkeypatch):
    _catalog(tmp_path / "catalog.db", REAL_IDS)
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "catalog.db"))
    # _map_changed with merge_measured=True: the derive path's own call (a merge-measured set never widens
    # to derive-all, AR-380)
    ids, unmapped = orchestrate._map_changed("oecd", [PRICES, "OECD.X__UNSERVED"], merge_measured=True)
    assert sorted(ids) == ["oecd:CPI_YOY:GBR", "oecd:CPI_YOY:USA"]
    assert list(unmapped) == ["OECD.X__UNSERVED"]


def test_negative_control_an_uncatalogued_id_is_not_claimed(tmp_path, monkeypatch):
    _catalog(tmp_path / "catalog.db", ["oecd:GDP_GROWTH_QOQ:USA"])
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "catalog.db"))
    ids, unmapped = orchestrate._map_changed("oecd", [PRICES], merge_measured=True)
    assert ids == [] and list(unmapped) == [PRICES]


def test_the_registry_declares_oecd_a_catalogue_subset():
    orchestrate._REG_ENTRIES = None
    assert orchestrate._catalog_scope("oecd") == "subset"


def _derive(tmp_path, monkeypatch, changed, ids=REAL_IDS, metadata=True):
    _catalog(tmp_path / "catalog.db", ids, metadata=metadata)
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "catalog.db"))
    orchestrate._REG_ENTRIES = None
    asked, owed = [], []
    monkeypatch.setattr(derive, "derive_and_put", lambda ids, blob, **k: asked.extend(ids) or {})
    store = types.SimpleNamespace(note_full_rederive_owed=lambda *a, **k: owed.append((a, k)))
    unit = types.SimpleNamespace(key="oecd/_all", source_id="oecd", unit_id="_all")
    res = types.SimpleNamespace(obs=10, series_cursors=None, new_vintage="cat:x", status="ok",
                                changed_keys={k: "2025-12-31" for k in changed})
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, object(), store=store)
    return asked, note, owed, failed


def test_a_pass_moving_only_uncatalogued_flows_is_coverage_not_a_demotion(tmp_path, monkeypatch):
    asked, note, owed, failed = _derive(tmp_path, monkeypatch, [f"OECD.X__F{i}" for i in range(50)])
    assert asked == [] and failed == [] and owed == []
    assert note.startswith("csv coverage note:"), note


def test_a_served_flow_changing_is_derived(tmp_path, monkeypatch):
    asked, note, owed, failed = _derive(tmp_path, monkeypatch, [QNA, "OECD.X__F1"])
    assert sorted(asked) == sorted(i for i in REAL_IDS if "CPI_YOY" not in i) and owed == []
    assert note is None or note.startswith("csv coverage note:"), note


@pytest.mark.parametrize("extra", [[], ["OECD.X__F1"]], ids=["alone", "beside-an-uncatalogued-flow"])
def test_mutant_a_template_missing_from_the_index_turns_the_run_red(tmp_path, monkeypatch, extra):
    """(b) of AR-404 O1: drop GDP_GROWTH_* from the table the tier reads; the changed QNA flow must DEMOTE
    and book a durable debt - with and without a mapped flow beside it - never a green coverage note."""
    from econdl import _resolve as R
    monkeypatch.setattr(R, "_OECD_TEMPLATES", {"CPI_YOY": R._OECD_TEMPLATES["CPI_YOY"]})
    changed = [QNA] + extra + ([PRICES] if extra else [])
    asked, note, owed, failed = _derive(tmp_path, monkeypatch, changed)
    assert note.startswith("csv coherence unmet:") and QNA in note, note
    assert len(owed) == 1 and owed[0][0][0] == "oecd", owed


def test_unreadable_metadata_fails_closed(tmp_path, monkeypatch):
    asked, note, owed, failed = _derive(tmp_path, monkeypatch, ["OECD.X__F1"], metadata=False)
    assert note.startswith("csv coherence unmet:"), note
    assert owed, "a durable debt, not a vanishing note"


# ---- (1)+(2) a REAL process death, then the real orchestrator --------------------------------------

_CHILD = textwrap.dedent('''
    import datetime as dt, json, os, sys
    ROOT, STORE, LOG, KILL, FLOWS = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5].split(",")
    sys.path.insert(0, ROOT)
    os.environ.pop("AQUEDUCT_BACKEND", None)
    import pyarrow as pa
    from updater.strategies.fetchers import oecd
    oecd.RATE = 0
    oecd.fetch_catalog = lambda: {f: {"vintage": "v1", "filename": f + ".parquet", "agency": "A", "id": f,
                                      "version": "1.0", "root": "x"} for f in FLOWS}
    KEYS = {"%(qna)s": ["Q.Y.USA.S1.S1.B1GQ._Z._Z._Z.PC.L.G1.T0102", "Q.Y.USA.S1.S1.B1GQ._Z._Z._Z.PC.L.GY.T0102"]}
    def fetch(fid, meta, since, session):
        with open(LOG, "a") as fh:
            fh.write(fid + "\\n")
        if fid == KILL:
            os._exit(9)                                   # a hard kill: no except, no finally, no save
        keys = KEYS.get(fid, ["k." + fid])
        return pa.table({"series_key": pa.array(keys, pa.string()),
                         "obs_date": pa.array([dt.date(2025, 12, 31)] * len(keys), pa.date32()),
                         "value": pa.array([2.5] * len(keys), pa.float64()),
                         "obs_status": pa.array(["A"] * len(keys), pa.string()),
                         "time_raw": pa.array(["2025"] * len(keys), pa.string())}), "ok"
    oecd.fetch_flow = fetch
    class U:
        out_paths = [STORE]; source_id = "oecd"; unit_id = "_all"
    print("CHILD_STATUS", oecd.update(U(), None).status, flush=True)
''') % {"qna": QNA}


def _child(tmp_path, store, kill, flows, tag):
    script = tmp_path / "child.py"
    script.write_text(_CHILD, encoding="utf-8")
    log = tmp_path / f"{tag}.log"
    env = {k: v for k, v in os.environ.items() if k != "AQUEDUCT_BACKEND"}
    p = subprocess.run([sys.executable, "-B", str(script), ROOT, str(store), str(log), kill, ",".join(flows)],
                       capture_output=True, text=True, cwd=str(tmp_path), env=env, timeout=300)
    fetched = log.read_text().split() if log.exists() else []
    return p.returncode, p.stdout + p.stderr, fetched


FLOWS = ["OECD.X__F1", QNA, "OECD.X__F3", "OECD.X__F4", "OECD.X__F5"]


def test_a_hard_kill_keeps_the_checkpoint_and_the_next_pass_starts_after_the_dead_flow(tmp_path, local):
    store = tmp_path / "store"
    os.makedirs(store)
    rc, out, fetched = _child(tmp_path, store, "OECD.X__F4", FLOWS, "p1")
    assert rc == 9, out
    assert fetched == FLOWS[:4]
    st = _sidecar(store)                                   # the ARTEFACT, read through blob (R273)
    assert st is not None, "no sidecar after the kill: nothing was saved during the loop"
    assert {f for f, s in st.items() if s.get("status") == "ok"} == {"OECD.X__F1", QNA, "OECD.X__F3"}
    assert st[QNA]["vintage"] == "v1"
    dead = st["OECD.X__F4"]
    assert dead["status"] == "transient_fail" and dead["fail_kind"] == "interrupted" and dead["fails"] == 1
    assert "[oecd] flow 4/5 start OECD.X__F4" in out and "flow 4/5 done" not in out
    rc, out, fetched = _child(tmp_path, store, "", FLOWS, "p2")
    assert rc == 0, out
    assert fetched == ["OECD.X__F5"], "the next pass starts AFTER the dead flow; F4 waits out its back-off"


def test_stall_first_the_dead_first_flow_does_not_head_every_pass(tmp_path, local):
    """AR-404 stall_first: a flow 1 that kills every pass was never recorded, so it headed every pass."""
    store = tmp_path / "store"
    os.makedirs(store)
    for i in range(3):
        rc, out, fetched = _child(tmp_path, store, "OECD.X__F1", FLOWS, f"p{i}")
        if i == 0:
            assert rc == 9 and fetched == ["OECD.X__F1"], out
        else:
            assert "OECD.X__F1" not in fetched, (i, fetched)
    assert _sidecar(store)["OECD.X__F1"]["fail_kind"] == "interrupted"


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """The REAL orchestrate.run_once for the registry entry `oecd`, whose strategy calls the REAL
    oecd.update; the REAL mapping, derive_and_put and resolver; the CSV PUT goes to a list."""
    for k in ("AQUEDUCT_BACKEND", "AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_BUDGET_MIN",
              "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_DERIVE_BUDGET_MIN", "AQUEDUCT_FLOW_DERIVE_MAX_ROWS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    monkeypatch.setattr(oecd, "RATE", 0)
    data = tmp_path / "data"
    store_dir = data / "clean_full" / "oecd"
    os.makedirs(store_dir)
    os.makedirs(data / "_aqueduct")
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    monkeypatch.setenv("ECONDL_DATA", str(data / "clean_full"))
    ids = ["oecd:GDP_GROWTH_QOQ:USA", "oecd:GDP_GROWTH_YOY:USA", "oecd:CPI_YOY:USA"]
    _catalog(tmp_path / "catalog.db", ids)
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "catalog.db"))
    orchestrate._REG_ENTRIES = None

    class _Blob:
        def __init__(self):
            self.keys = []

        def put_atomic(self, key, data):
            self.keys.append(key)

    r = types.SimpleNamespace(store_dir=store_dir, blob=_Blob(), ids=ids,
                              state=StateStore(str(data / "_aqueduct" / "state.db")))
    unit = next(u for u in registry.all_units() if u.source_id == "oecd")
    real = orchestrate.get_strategy(unit.strategy)

    class _Strategy:                                     # is_due / detect_change forced; run is REAL
        def is_due(self, unit, us, **k):
            return True

        def detect_change(self, unit, us):
            return "cat:test"

        def run(self, unit, since=None):
            return real.run(unit, since=since)

    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: _Strategy())

    def run():
        r.blob.keys.clear()
        return orchestrate.run_once(sources=["oecd"], store=r.state, blob=r.blob)

    def put_ids():
        want = {derive.r2_key(i): i for i in ids}
        return sorted(want[k] for k in r.blob.keys if k in want)

    r.run, r.put_ids = run, put_ids
    yield r
    r.state.close()


def _served_rows(sid, value):
    return _tbl([_native(sid)], value)


def test_real_run_once_derives_the_served_ids_of_a_changed_flow(rig, monkeypatch):
    """AR-404 P1: a changed DF_QNA flow and the catalogue row oecd:GDP_GROWTH_QOQ:USA -> that id is PUT."""
    pq.write_table(_served_rows("oecd:CPI_YOY:USA", 1.0), rig.store_dir / (PRICES + ".parquet"))
    qna = pa.concat_tables([_served_rows("oecd:GDP_GROWTH_QOQ:USA", 3.0),
                            _served_rows("oecd:GDP_GROWTH_YOY:USA", 3.0)])
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat([QNA, PRICES, "OECD.X__F1"]))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, s: (
        (qna, "ok") if fid == QNA else (_tbl(["k"]), "ok") if fid == "OECD.X__F1"
        else (_served_rows("oecd:CPI_YOY:USA", 1.0), "ok")))
    results = rig.run()
    assert results == [("oecd/_all", "ok")], results
    # QNA changed (3.0 is new); PRICES re-fetched its stored rows (idempotent), so CPI is NOT derived
    assert rig.put_ids() == ["oecd:GDP_GROWTH_QOQ:USA", "oecd:GDP_GROWTH_YOY:USA"], rig.put_ids()
    row = rig.state.get_unit("oecd", "_all")
    assert row["status"] == "ok" and row["last_success_utc"]
    assert not rig.state.db.execute("SELECT 1 FROM full_rederive_owed WHERE source_id='oecd'").fetchone()


def test_kill_after_a_served_flow_merged_then_the_next_pass_derives_it(rig, tmp_path, local):
    """AR-404 P2: the kill drops the changed set; the owed mark carries it to the next pass's CSV step."""
    store = rig.store_dir
    pq.write_table(pa.concat_tables([_served_rows("oecd:GDP_GROWTH_QOQ:USA", 1.0),
                                     _served_rows("oecd:GDP_GROWTH_YOY:USA", 1.0)]),
                   store / (QNA + ".parquet"))
    rc, out, fetched = _child(tmp_path, store, "OECD.X__F3", FLOWS, "p1")   # QNA merges 2.5, then F3 dies
    assert rc == 9 and fetched == FLOWS[:3], out
    st = _sidecar(store)
    assert st[QNA]["status"] == "ok" and st[QNA]["vintage"] == "v1", st[QNA]
    assert "csv_owed" in st[QNA], "the mark rides in the same row as the new vintage"
    # pass 2, through the REAL orchestrator: QNA is settled (not re-fetched), and is still derived
    pass2 = []

    def fetch(fid, meta, since, s):
        pass2.append(fid)
        return _tbl(["k." + fid]), "ok"

    import pytest as _p
    mp = _p.MonkeyPatch()
    try:
        mp.setattr(oecd, "fetch_catalog", lambda: _cat(FLOWS))
        mp.setattr(oecd, "fetch_flow", fetch)
        results = rig.run()
    finally:
        mp.undo()
    assert QNA not in pass2 and pass2 == ["OECD.X__F4", "OECD.X__F5"], pass2
    assert rig.put_ids() == ["oecd:GDP_GROWTH_QOQ:USA", "oecd:GDP_GROWTH_YOY:USA"], rig.put_ids()
    assert results[0][1] in ("partial", "ok"), results           # F3 waits its back-off: never ok
    row = rig.state.get_unit("oecd", "_all")
    assert row["status"] == "partial" and row["last_success_utc"] is None, dict(row)
    assert "csv_owed" not in _sidecar(store)[QNA], "cleared once a pass handed it to the CSV step"


# ---- (4) owed marks: the edges -----------------------------------------------------------------

def _seed(d, rows):
    os.makedirs(d, exist_ok=True)
    (d / "_giant_state.json").write_text(json.dumps(rows), encoding="utf-8")


def test_a_quiet_pass_that_carries_a_mark_runs_the_csv_step(tmp_path, local, monkeypatch):
    """AR-404 O3: 1 marked flow + 3 quiet flows. no_change runs no CSV step and the marks are cleared in the
    final save, so the pass must not read no_change."""
    flows = [QNA, "a", "b", "c"]
    _seed(tmp_path, {QNA: {"status": "ok", "vintage": "v1", "csv_owed": "2025-12-31"}})
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat(flows))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, s: (None, "empty"))
    res = oecd.update(_Unit(tmp_path), None)
    assert res.status == "ok" and res.changed_keys == {QNA: "2025-12-31"}, (res.status, res.changed_keys)
    assert orchestrate._should_derive_csvs(res.status, res.changed_keys)
    assert "csv_owed" not in _sidecar(tmp_path)[QNA]


def test_negative_control_a_quiet_pass_without_marks_is_no_change(tmp_path, local, monkeypatch):
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat(["a", "b", "c"]))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, s: (None, "empty"))
    res = oecd.update(_Unit(tmp_path), None)
    assert res.status == "no_change" and res.changed_keys == {}


def test_a_failed_final_save_keeps_the_marks_and_the_checkpoints_carry_them(tmp_path, local, monkeypatch):
    """AR-404 O6: the checkpoints during the loop still hold the mark; only the final save clears it, and when
    that save fails the stored copy keeps the mark for the next pass."""
    _seed(tmp_path, {QNA: {"status": "ok", "vintage": "v1", "csv_owed": ""}})
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat([QNA, "a", "b"]))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, s: (_tbl(["k." + fid]), "ok"))
    saves = []
    real = _giant.save_state

    def save(d, st):
        saves.append(json.loads(json.dumps(st)))
        if len(saves) == 3:                       # before flow 1, before flow 2 (periodic), final
            raise OSError("PUT of the state file failed")
        real(d, st)

    monkeypatch.setattr(_giant, "save_state", save)
    res = oecd.update(_Unit(tmp_path), None)
    assert all("csv_owed" in s.get(QNA, {}) for s in saves[:2]), "every checkpoint carries the mark"
    assert "csv_owed" not in saves[2].get(QNA, {}), "the final save is the one that clears it"
    assert "csv_owed" in _sidecar(tmp_path)[QNA], "it failed, so the stored copy keeps the mark"
    assert QNA in res.changed_keys


def test_carried_marks_are_not_this_pass_s_progress_for_the_capped_guard(tmp_path, local, monkeypatch):
    """AR-404 O4: an all-failed capped pass that carries a mark keeps the partial clock (not due every run),
    and still hands the mark to the CSV step."""
    flows = [f"f{i}" for i in range(5)]
    _seed(tmp_path, {QNA: {"status": "ok", "vintage": "v1", "csv_owed": ""}})
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat(flows))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, s: (None, "transient"))
    monkeypatch.setitem(_giant.run_giant.__kwdefaults__, "max_flows", 2)
    res = oecd.update(_Unit(tmp_path), None)
    assert res.status == "partial", res.status
    assert res.changed_keys == {QNA: None}


# ---- P10 / structural, on oecd.update -------------------------------------------------------------

def test_a_structural_flow_beside_a_changed_one_is_partial_with_the_set(tmp_path, local, monkeypatch):
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat(["s", QNA]))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, sess: (
        (None, "structural") if fid == "s" else (_tbl(["k"]), "ok")))
    res = oecd.update(_Unit(tmp_path), None)
    assert res.status == "partial" and QNA in res.changed_keys


def test_a_carried_mark_survives_a_pass_whose_only_flow_is_structural(tmp_path, local, monkeypatch):
    """finalize raises on the structural flow; the pass merged nothing itself, but its final save cleared the
    carried mark, so it must still return partial WITH the mark (never the bare raise, which runs no CSV step)."""
    _seed(tmp_path, {QNA: {"status": "ok", "vintage": "v1", "csv_owed": "2025-12-31"}})
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat(["s"]))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, sess: (None, "structural"))
    res = oecd.update(_Unit(tmp_path), None)
    assert res.status == "partial" and res.changed_keys == {QNA: "2025-12-31"}


def test_only_structural_is_raised_as_before(tmp_path, local, monkeypatch):
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat(["s"]))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, sess: (None, "structural"))
    with pytest.raises(DefinitiveError):
        oecd.update(_Unit(tmp_path), None)


# ---- (5) the timing lines ---------------------------------------------------------------------

_BRANCHES = {"ok": lambda: (_tbl(["k"]), "ok"), "empty": lambda: (None, "empty"),
             "transient": lambda: (None, "transient"), "structural": lambda: (None, "structural"),
             "notime": lambda: (None, "no_time_dimension"),
             "raises_transient": lambda: (_ for _ in ()).throw(TransientError("x")),
             "raises_definitive": lambda: (_ for _ in ()).throw(DefinitiveError("x")),
             "unexpected": lambda: (_ for _ in ()).throw(KeyError("x")),
             "guard": lambda: (_tbl(["k"]), "ok")}


def test_every_outcome_branch_prints_a_start_and_a_done_line(tmp_path, local, monkeypatch, capsys):
    flows = list(_BRANCHES)
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat(flows))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, s: _BRANCHES[fid]())
    from updater import merge
    real = merge.merge_and_write_bounded

    def bounded(out_path, table, **kw):
        if out_path.endswith("guard.parquet"):
            raise DefinitiveError("refusing shrink")
        return real(out_path, table, **kw)

    monkeypatch.setattr(merge, "merge_and_write_bounded", bounded)
    try:
        oecd.update(_Unit(tmp_path), None)
    except DefinitiveError:
        pass
    out = capsys.readouterr().out
    n = len(flows)
    for k, fid in enumerate(flows, 1):
        assert re.search(rf"^\[oecd\] flow {k}/{n} start {fid} at \d{{4}}-\d\d-\d\dT\d\d:\d\d:\d\dZ "
                         rf"\(unit \+\d+s\)$", out, re.M), fid
        assert re.search(rf"^\[oecd\] flow {k}/{n} done {fid}: \S+, (\d+|-) rows fetched, \d+\.\ds$",
                         out, re.M), fid
    from tools import measure_ci_lag as M
    prog = re.compile(M._GIANT_PROGRESS.format(sid="oecd"))
    flow_lines = [x for x in out.splitlines() if x.startswith("[oecd] flow ")]
    assert flow_lines and not any(prog.match(x) for x in flow_lines)


def test_without_flow_log_no_timing_line_is_printed(tmp_path, local, capsys):
    _giant.run_giant(_Unit(tmp_path), source="zz", fetch_catalog=lambda: _cat(["a"]),
                     fetch_flow=lambda *a: (_tbl(["k"]), "ok"), csv_accept="", rate=0, timeout=1)
    assert "[zz] flow " not in capsys.readouterr().out


# ---- retry note, health ------------------------------------------------------------------------

def test_a_flow_that_keeps_outlasting_the_limit_is_named_and_shows_in_health(tmp_path, local, monkeypatch,
                                                                          capsys):
    # killed in flight three times; the last kill an hour ago, so it waits out its 48 h back-off
    recent = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).isoformat(timespec="seconds")
    _seed(tmp_path, {"big": {"status": "transient_fail", "fail_kind": "interrupted", "fails": 3,
                             "failed_at": recent, "failed_vintage": "v1"}})
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat(["big", "a"]))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, s: (_tbl(["k." + fid]), "ok")
                        if fid == "a" else (None, "transient"))
    res = oecd.update(_Unit(tmp_path), None)
    out = capsys.readouterr().out
    assert res.status == "partial" and "outlasts the limit: 1 [big]" in res.error, res.error
    assert "too big for the runner" not in res.error and "too big for the runner" not in out
    assert "[oecd] retry note:" in out
    # what health prints for it: ATTENTION, with the note
    st = StateStore(str(tmp_path / "state.db"))
    st.upsert_unit("oecd", "_all", strategy="giant_changed_units", upstream_vintage=None, status=res.status,
                   last_obs_date=res.last_obs_date, obs_count=res.obs, last_success_utc=None,
                   last_attempt_utc=dt.datetime.now(dt.timezone.utc).isoformat(), last_error=res.error)
    rep = health.assess(store=st)
    row = next(r for r in rep["sources"] if r["source"] == "oecd")
    print("HEALTH ROW", json.dumps({k: row.get(k) for k in ("source", "health", "live", "run_location")}))
    assert row["health"] == "ATTENTION", row
    st.close()


# ---- P8: the bounded merge on oecd's own 5-column shape -------------------------------------------

@pytest.mark.parametrize("stored_type", [pa.string(), pa.large_string()], ids=["string", "large_string"])
def test_the_bounded_merge_on_oecd_s_shape_writes_what_the_in_memory_merge_writes(tmp_path, stored_type):
    """17 of the 1,451 local oecd files store large_string (AR-404 F6); fetch_flow always builds pa.string().
    Forced onto the DuckDB path, the bounded merge must equal the in-memory merge in rows, order and types."""
    from updater import merge
    stored = pa.table({
        "series_key": pa.array(["A.1", "A.1", "B.2", "C.3"], stored_type),
        "obs_date": pa.array([dt.date(2023, 12, 31), dt.date(2024, 12, 31), dt.date(2024, 12, 31),
                              dt.date(2024, 12, 31)], pa.date32()),
        "value": pa.array([1.0, 2.0, 3.0, 4.0], pa.float64()),
        "obs_status": pa.array(["A", "A", "E", "A"], stored_type),
        "time_raw": pa.array(["2023", "2024", "2024", "2024"], stored_type)})
    new = pa.table({
        "series_key": pa.array(["A.1", "B.2", "D.4"], pa.string()),
        "obs_date": pa.array([dt.date(2024, 12, 31), dt.date(2025, 12, 31), dt.date(2025, 12, 31)], pa.date32()),
        "value": pa.array([2.5, 3.5, 9.0], pa.float64()),
        "obs_status": pa.array(["P", "A", "A"], pa.string()),
        "time_raw": pa.array(["2024", "2025", "2025"], pa.string())})
    a, b = tmp_path / "mem.parquet", tmp_path / "bounded.parquet"
    for p in (a, b):
        pq.write_table(stored, p, row_group_size=2)
    want = merge.merge_and_write(str(a), new, mode="merge", report_changed_keys=True)
    got = merge.merge_and_write_bounded(str(b), new, report_changed_keys=True, batch_rows=2,
                                        in_memory_max_bytes=-1, in_memory_max_new_rows=-1)
    assert got == want
    ta, tb = pq.read_table(a), pq.read_table(b)
    assert [(f.name, f.type) for f in tb.schema] == [(f.name, f.type) for f in ta.schema]
    assert tb.to_pylist() == ta.to_pylist()


# ---- (6) the opt-in stays where it was measured ------------------------------------------------

def test_owed_marks_and_the_retry_slice_reach_only_the_callers_that_declare_them():
    fdir = os.path.join(ROOT, "updater", "strategies", "fetchers")
    src = {f: open(os.path.join(fdir, f), encoding="utf-8").read()
           for f in os.listdir(fdir) if f.endswith(".py") and f != "_giant.py"}
    assert sorted(f for f, s in src.items() if "owed_marks=True" in s) == ["oecd.py"]
    assert sorted(f for f, s in src.items() if "flow_log=True" in s) == ["oecd.py"]
    assert "owed_marks" not in src["eurostat.py"]


def test_owed_marks_need_the_changed_set_and_checkpoints(tmp_path):
    with pytest.raises(ValueError, match="owed_marks requires"):
        _giant.run_giant(_Unit(tmp_path), source="zz", fetch_catalog=lambda: _cat(["a"]),
                         fetch_flow=lambda *a: (_tbl(["k"]), "ok"), csv_accept="", rate=0, timeout=1,
                         owed_marks=True)
