"""gus_dbw refresh parts carry a SWEEP IDENTITY, and a rebuild merges only the current sweep's parts.

parts/_refresh/area_<id>/ outlives the sweep that wrote it. Before this, finish_area merged EVERY
*.parquet there, so an incomplete earlier sweep's parts, a year that now returns 0 rows (no part is
written for it) and a year that left the window all came back in the next rebuild (ledger R1554
class: a cache completed later holds two vintages). Hermetic: local files only, no network.
"""
from __future__ import annotations

import datetime as dt
import inspect
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from jobs import gus_dbw_refresh as R          # noqa: E402
from jobs import ingest_gus_dbw as G           # noqa: E402
from tools import gus_dbw_refresh_exclusions as X   # noqa: E402

TAIL = 2025
HISTORY = [(f"GUSDBW:1:1:h{i}", dt.date(2010 + i % 10, 12, 31), float(i)) for i in range(30)]
HIST_2024 = [(f"GUSDBW:1:1:t{i}", dt.date(2024, 12, 31), float(i)) for i in range(10)]
S1_2025 = [(f"GUSDBW:1:1:t{i}", dt.date(2025, 12, 31), float(i)) for i in range(10)]
S1_2026 = [(f"GUSDBW:1:1:t{i}", dt.date(2026, 6, 30), float(i)) for i in range(10)]
S2_2025 = [(f"GUSDBW:1:2:w{i}", dt.date(2025, 12, 31), float(i)) for i in range(5)]


def _write(path, rows, sweep=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    t = pa.table({"series_key": pa.array([r[0] for r in rows], pa.string()),
                  "obs_date": pa.array([r[1] for r in rows], pa.date32()),
                  "value": pa.array([r[2] for r in rows], pa.float64())})
    if sweep:
        t = t.replace_schema_metadata(R.sweep_metadata(sweep))
    pq.write_table(t, path, compression="zstd")
    return path


def _rows(path):
    t = pq.read_table(path)
    return sorted(zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                      t.column("value").to_pylist()))


@pytest.fixture
def env(tmp_path, monkeypatch):
    out = tmp_path / "gus_dbw"
    parts = out / "parts"
    monkeypatch.setattr(G, "OUT", str(out))
    monkeypatch.setattr(G, "REFRESH_PARTS", str(parts / "_refresh"))
    monkeypatch.setattr(G, "REFUSED_PARTS", str(parts / "_refresh_refused"))
    monkeypatch.setattr(G, "GUARD_SPILL", str(out / "_tmp" / "guard_spill"))
    final = _write(str(out / "area_1.parquet"), HISTORY + HIST_2024 + S1_2025)
    return {"out": str(out), "final": final, "dir": str(parts / "_refresh" / "area_1"),
            "refused": str(parts / "_refresh_refused" / "area_1")}


def _sweep(env, years: dict) -> tuple[str, set]:
    """One COMPLETE sweep. years = {(var, sec, year): rows}; rows [] is a zero-row year. Returns the
    sweep id and the attempted keys - exactly what refresh_pass hands finish_area."""
    sid = R.begin_sweep(env["dir"], 1)
    for (v, s, y), rows in years.items():
        name = R.part_name(v, s, y)
        if rows:
            G.write_part(os.path.join(env["dir"], name), rows, metadata=R.sweep_metadata(sid))
        R.record_sweep_year(env["dir"], sid, v, s, y, len(rows), name if rows else None)
    return sid, {R.year_key(v, s, y) for (v, s, y) in years}


def _owed(env):
    p = os.path.join(env["out"], R.OWED_FILE)
    return [json.loads(x) for x in open(p, encoding="utf-8")] if os.path.exists(p) else []


# ------------------------------------------------------------------ the defect, three shapes
def test_stale_parts_from_an_older_sweep_are_excluded(env):
    """An older sweep's y2024 part (the year has since left the window) and a part written before
    sweep identities both sit in the dir. Merging them duplicates 2024 history and resurrects rows."""
    _write(os.path.join(env["dir"], "v1_s1_y2024.parquet"), HIST_2024, sweep="a1-OLD-1-0000")
    legacy = [("GUSDBW:1:9:gone", dt.date(2025, 3, 1), 1.0)]
    _write(os.path.join(env["dir"], "v1_s9_y2025.parquet"), legacy)              # no sweep id at all
    _, keys = _sweep(env, {(1, 1, 2025): S1_2025, (1, 1, 2026): S1_2026})
    sel = R.select_tail_parts(1, G.REFRESH_PARTS, TAIL)
    assert sorted(x["file"] for x in sel["excluded"]) == ["v1_s1_y2024.parquet", "v1_s9_y2025.parquet"]
    assert G.finish_area(1, env["final"], {}, TAIL, expected_keys=keys) is True
    assert _rows(env["final"]) == sorted(HISTORY + HIST_2024 + S1_2025 + S1_2026)
    g = _owed(env)[0]["guard"]["sweep"]
    assert g["excluded"] == 2 and g["included"] == 2


def test_a_year_returning_zero_rows_does_not_resurrect_its_old_part(env):
    """An earlier incomplete sweep fetched section 2 for 2025 (rows the final never got); this
    complete sweep asks again and GUS returns nothing. The old part must not come back."""
    _write(os.path.join(env["dir"], "v1_s2_y2025.parquet"), S2_2025, sweep="a1-OLD-1-0000")
    _, keys = _sweep(env, {(1, 1, 2025): S1_2025, (1, 2, 2025): []})
    state = {}
    assert G.finish_area(1, env["final"], state, TAIL, expected_keys=keys) is True
    got = _rows(env["final"])
    assert not any(k.startswith("GUSDBW:1:2:") for k, _d, _v in got), "a zero-row year resurrected"
    assert got == sorted(HISTORY + HIST_2024 + S1_2025)
    g = _owed(env)[0]["guard"]["sweep"]
    assert g["superseded_by_zero"] == 1 and g["superseded_by_zero_rows"] == len(S2_2025)
    assert g["superseded_by_zero_eg"][0]["file"] == "v1_s2_y2025.parquet"


def test_normal_multi_pass_accumulation_still_works(env):
    """Pass 1 is INCOMPLETE (not rebuilt), pass 2 is complete: the rebuild is pass 2's sweep, and
    every section and year of that one sweep accumulates into it."""
    _sweep(env, {(1, 1, 2025): [(k, d, v + 100) for k, d, v in S1_2025]})        # pass 1, no finish
    new25 = [(k, d, v + 0.5) for k, d, v in S1_2025]
    _, keys = _sweep(env, {(1, 1, 2025): new25, (1, 1, 2026): S1_2026, (1, 2, 2025): S2_2025})
    sel = R.select_tail_parts(1, G.REFRESH_PARTS, TAIL)
    assert len(sel["included"]) == 3 and sel["excluded"] == [] and sel["problems"] == []
    assert G.finish_area(1, env["final"], {}, TAIL, expected_keys=keys) is True
    assert _rows(env["final"]) == sorted(HISTORY + HIST_2024 + new25 + S1_2026 + S2_2025)


# ------------------------------------------------------------------ untrusted manifests refuse, never move
def _assert_untouched(env, before, state, n_parts):
    assert _rows(env["final"]) == before
    assert not os.path.exists(env["final"] + G.CANDIDATE_SUFFIX)
    assert _owed(env) == [] and "1" not in state
    assert not os.path.exists(env["refused"]), "a manifest problem must NOT quarantine (it moves parts)"
    assert len(R.tail_parts_for_area(1, G.REFRESH_PARTS)) == n_parts


def test_legacy_parts_with_no_manifest_refuse_then_a_new_sweep_excludes_them(env):
    _write(os.path.join(env["dir"], "v1_s1_y2025.parquet"), S1_2025)
    _write(os.path.join(env["dir"], "v1_s3_y2025.parquet"), [("GUSDBW:1:3:x", dt.date(2025, 1, 1), 1.0)])
    before, state = _rows(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_untouched(env, before, state, 2)
    _, keys = _sweep(env, {(1, 1, 2025): S1_2025})                     # the first sweep under new code
    assert G.finish_area(1, env["final"], state, TAIL, expected_keys=keys) is True
    assert not any(k.startswith("GUSDBW:1:3:") for k, _d, _v in _rows(env["final"]))


@pytest.mark.parametrize("break_it", ["torn", "missing", "rowcount", "foreign_footer", "below_window"])
def test_an_untrusted_manifest_refuses_without_moving_anything(env, break_it):
    sid, keys = _sweep(env, {(1, 1, 2025): S1_2025})
    man = os.path.join(env["dir"], R.SWEEP_FILE)
    p = os.path.join(env["dir"], "v1_s1_y2025.parquet")
    n = 1
    if break_it == "torn":
        open(man, "a", encoding="utf-8").write('{"kind": "year", "swe')
    elif break_it == "missing":
        os.remove(p)
        n = 0
    elif break_it == "rowcount":
        _write(p, S1_2025[:3], sweep=sid)
    elif break_it == "foreign_footer":
        _write(p, S1_2025, sweep="a1-OTHER-1-0000")
    elif break_it == "below_window":
        R.record_sweep_year(env["dir"], sid, 1, 1, 2024, 0, None)
        keys = keys | {R.year_key(1, 1, 2024)}
    before, state = _rows(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL, expected_keys=keys) is False
    _assert_untouched(env, before, state, n)


def test_manifest_keys_must_equal_the_keys_the_sweep_attempted(env):
    _, keys = _sweep(env, {(1, 1, 2025): S1_2025})
    before, state = _rows(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL, expected_keys=keys | {"v1:s1:y2026"}) is False
    _assert_untouched(env, before, state, 1)
    assert G.finish_area(1, env["final"], state, TAIL, expected_keys=keys) is True


# ------------------------------------------------------------------ fetch_section writes the identity
def _fake_fetch(monkeypatch, data):
    calls = []

    def fetch_year(var, sec, year, cands, pmap, out_rows, seen):
        calls.append((var, sec, year))
        out_rows.extend(data.get((var, sec, year), []))
        return {cands[0]: len(data.get((var, sec, year), []))}

    monkeypatch.setattr(G, "fetch_year", fetch_year)
    monkeypatch.setattr(G, "save_ckpt", lambda ck: None)
    return calls


def test_a_section_listed_under_two_frequencies_records_each_year_once(env, monkeypatch):
    """area_46 lists sections twice (freq Year 2004-2025 and freq Month 2004-2026). year_key and
    part_name omit the frequency, so the second entry must skip the years the first did, giving one
    manifest line per year and no 'recorded twice' problem."""
    calls = _fake_fetch(monkeypatch, {(1, 1, 2025): S1_2025, (1, 1, 2026): S1_2026})
    sid = R.begin_sweep(env["dir"], 1)
    ryears, ck = set(), {}
    G.fetch_section(1, 1, 1, (TAIL, 2025), [282], {}, ck, ryears, section_dir=env["dir"],
                    ckpt_key="refresh_year_sections", sweep_id=sid)
    G.fetch_section(1, 1, 1, (TAIL, 2026), [247], {}, ck, ryears, section_dir=env["dir"],
                    ckpt_key="refresh_year_sections", sweep_id=sid)
    assert calls == [(1, 1, 2025), (1, 1, 2026)]
    man = R.read_sweep_manifest(env["dir"])
    assert man["problems"] == [] and sorted(man["years"]) == ["v1:s1:y2025", "v1:s1:y2026"]
    sel = R.select_tail_parts(1, G.REFRESH_PARTS, TAIL)
    assert sel["problems"] == [] and len(sel["included"]) == 2
    assert set(sel["keys"]) == ryears
    assert R._footer(sel["included"][0])[0] == sid


def test_a_zero_row_year_is_recorded_as_a_marker_and_writes_no_part(env, monkeypatch):
    _fake_fetch(monkeypatch, {})
    sid = R.begin_sweep(env["dir"], 1)
    G.fetch_section(1, 1, 1, (TAIL, 2025), [282], {}, {}, set(), section_dir=env["dir"],
                    ckpt_key="refresh_year_sections", sweep_id=sid)
    rec = R.read_sweep_manifest(env["dir"])["years"]["v1:s1:y2025"]
    assert rec["rows"] == 0 and rec["file"] is None
    assert R.tail_parts_for_area(1, G.REFRESH_PARTS) == []


def test_the_backfill_path_writes_no_identity(tmp_path, monkeypatch):
    _fake_fetch(monkeypatch, {(1, 1, 2025): S1_2025})
    d = str(tmp_path / "area_1")
    G.fetch_section(1, 1, 1, (TAIL, 2025), [282], {}, {}, set(), section_dir=d)
    assert not os.path.exists(os.path.join(d, R.SWEEP_FILE))
    assert R._footer(os.path.join(d, "v1_s1_y2025.parquet"))[0] is None


def test_every_area_sweep_begins_before_its_first_fetch_and_hands_its_keys_to_finish():
    src = inspect.getsource(G.refresh_pass)
    assert src.index("R.begin_sweep(area_dir, aid)") < src.index("fetch_section(")
    assert src.index("R.begin_sweep(area_dir, aid)") > src.index("for a in due:")
    assert "sweep_id=sweep_id" in src and "expected_keys=ryears" in src


def test_begin_sweep_replaces_the_old_manifest(env):
    s1, _ = _sweep(env, {(1, 1, 2025): S1_2025})
    s2 = R.begin_sweep(env["dir"], 1)
    man = R.read_sweep_manifest(env["dir"])
    assert s1 != s2 and man["sweep"] == s2 and man["years"] == {}


def test_write_part_retries_a_transient_permission_error(tmp_path, monkeypatch):
    real, calls = os.replace, []

    def flaky(a, b):
        calls.append(1)
        if len(calls) < 3:
            raise PermissionError("WinError 5")
        return real(a, b)

    monkeypatch.setattr(R.os, "replace", flaky)
    monkeypatch.setattr(R, "SAVE_STATE_WAIT_S", 0)
    p = str(tmp_path / "x.parquet")
    G.write_part(p, S1_2025)
    assert len(calls) == 3 and pq.read_metadata(p).num_rows == len(S1_2025)


# ------------------------------------------------------------------ nothing on the beat reads the sweep files
def test_the_digest_summary_and_the_beat_never_read_sweep_files():
    """C3: on Windows a reader's handle blocks the crawler's replace. publish_summary runs on the beat."""
    for src in (inspect.getsource(R.publish_summary), inspect.getsource(R.owed_areas),
                inspect.getsource(R.refused_still_due),
                open(os.path.join(ROOT, "tools", "guard_heartbeat.py"), encoding="utf-8").read()):
        assert "select_tail_parts" not in src and "SWEEP_FILE" not in src and "_sweep.jsonl" not in src


# ------------------------------------------------------------------ the read-only report
def test_the_stat_only_report_opens_no_file(env, monkeypatch):
    _write(os.path.join(env["dir"], "v1_s1_y2024.parquet"), HIST_2024)
    _write(os.path.join(env["dir"], "v1_s1_y2025.parquet"), S1_2025)
    other = os.path.join(os.path.dirname(env["dir"]), "area_2")
    _write(os.path.join(other, "v2_s1_y2025.parquet"), S1_2025)
    open(os.path.join(other, R.SWEEP_FILE), "w").write("{}\n")
    import builtins

    def no(*a, **k):
        raise AssertionError("the stat-only report opened a file")

    monkeypatch.setattr(builtins, "open", no)
    monkeypatch.setattr(pq, "read_metadata", no)
    monkeypatch.setattr(pq, "read_schema", no)
    monkeypatch.setattr(pq, "ParquetFile", no)
    recs = {r["area"]: r for r in X.report(G.REFRESH_PARTS, TAIL)}
    assert recs["area_1"]["would_exclude"] == 2 and recs["area_1"]["below_window"] == 1
    assert recs["area_2"]["would_exclude"] is None and recs["area_2"]["verdict"] == "needs --read"


def test_the_report_read_mode_refuses_without_the_crawler_stopped_flag(capsys):
    assert X.main(["--read"]) == 2
