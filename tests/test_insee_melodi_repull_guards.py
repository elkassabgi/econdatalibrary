"""AR-330 first-production-run findings F1-F8 (run 39024352275 doubled 8 DD_CNA flows): one test or more each.

Same harness as tests/test_insee_melodi_repull.py (fake paged Melodi, fake clock, local store)."""
from __future__ import annotations

import json
import re

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from test_insee_melodi_repull import (env, FakeMelodi, series, obs, _control_flows, _seed_store,  # noqa: F401
                                      run_until_quiet, run_until_chunk, P)
from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S


def respelled(rows, dim="AREA", value="FR"):
    """The same observations with one constant dimension added (INSEE's GEO=2025-FRANCE-F, 2026-10-17)."""
    out = []
    for o in rows:
        d = dict(o["dimensions"])
        d[dim] = value
        out.append({"dimensions": d, "measures": o["measures"]})
    return out


def null_obs(n, start=0, year=2023):
    return [{"dimensions": {"GEO": f"2026-COM-{90000 + start + i:05d}", "SEX": "_T", "TIME_PERIOD": str(year)},
             "measures": {"OBS_VALUE_NIVEAU": {"value": None}}} for i in range(n)]


def _pass(env, fake, minutes=45):
    c = S.Clock(now=env.clock, window_s=minutes * 60, source="test")
    return M._Pass(fake, c, S.FlowState.load(), set())


def _write_state(env, st):
    (env.store / S.STATE_FILE).write_text(json.dumps(st, indent=1, sort_keys=True))


def _chunks(env, code):
    return [c for c in env.chunks() if f"/{code}/" in c]


# ------------------------------------------------------------------ F1: a respelling is HELD, never merged
def test_F1_a_respelled_tail_is_held_with_its_pages_kept(env, capsys):
    flows = _control_flows()
    old = series(8, year=2023) + series(8, year=2024)
    flows["DS_T"] = list(old)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_T", old)
    run_until_quiet(env, fake)
    assert env.state()["flows"]["DS_T"]["status"] in S.WALKABLE
    fake.flows["DS_T"] = respelled(old)                              # INSEE adds a constant dimension
    fake.modified["DS_T"] = "2026-10-17T00:00:00"
    capsys.readouterr()
    r = env.run(fake)
    out = capsys.readouterr().out
    fe = env.state()["flows"]["DS_T"]
    assert env.rows("DS_T") == len(old)                               # nothing merged
    assert fe["status"] == S.MERGE_PENDING and fe["hold"] == "spelling" and _chunks(env, "DS_T")
    assert "SPELLING DRIFT? merge HELD" in r.error and "AREA is constant (AREA=FR)" in r.error
    assert "an exact respelling" in r.error
    assert "DS_T: merge plan - staged 8 = new keys 8 (8 keys) + new dates 0 + revised 0 + unchanged 0" in out
    assert fe["hold_detail"]["covered"] == 0 and fe["hold_detail"]["expected"] == 8


def test_F1_a_respelled_origin_pull_is_held(env):
    flows = _control_flows()
    flows["DS_RP_TD_X"] = respelled(series(25, extra={"AGE": "Y"}))
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_RP_TD_X", series(25, extra={"AGE": "Y"})[:P])        # one page in the OLD spelling
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_RP_TD_X"]
    assert env.rows("DS_RP_TD_X") == P and fe["hold"] == "spelling" and fe["status"] == S.MERGE_PENDING


def test_F1_a_geo_vintage_drift_is_held_and_named(env):
    flows = _control_flows()
    flows["DS_RP_TD_V"] = series(25, geo="2026", extra={"AGE": "V"})
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_RP_TD_V", series(P, geo="2025", extra={"AGE": "V"}))
    rs = run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_RP_TD_V"]
    assert fe["hold"] == "spelling" and env.rows("DS_RP_TD_V") == P
    assert "GEO vintage(s) ['2026'] new to the store (it holds ['2025'])" in fe["hold_detail"]["diagnosis"]
    assert any("DS_RP_TD_V" in (r.error or "") and "HELD" in (r.error or "") for r in rs)


def test_F1_a_truncation_repair_with_many_new_keys_is_merged(env):
    """The case NK/S alone would block: page 1 is stored, the rest of the flow is new keys - but the pull
    carries every stored key again (coverage 1)."""
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert env.rows("DS_RP_TD_SEED") == 25 and fe["status"] == S.COMPLETE and not fe.get("hold")


def test_F1_a_new_year_on_known_keys_is_merged(env):
    flows = _control_flows()
    flows["DS_Y"] = series(6, year=2022) + series(6, year=2023)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_Y", flows["DS_Y"])
    run_until_quiet(env, fake)
    fake.flows["DS_Y"] = flows["DS_Y"] + series(6, year=2024)
    fake.modified["DS_Y"] = "2026-10-17T00:00:00"
    env.run(fake)
    assert env.rows("DS_Y") == 18 and not env.state()["flows"]["DS_Y"].get("hold")


def test_F1_a_held_flow_is_skipped_does_not_wedge_the_queue_and_keeps_the_source_loud(env, capsys):
    flows = _control_flows()
    flows["DS_RP_TD_X"] = respelled(series(25, extra={"AGE": "Y"}))
    flows["DS_RP_TD_Y"] = series(25, extra={"AGE": "W"})
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_RP_TD_X", series(25, extra={"AGE": "Y"})[:P])
    env.put("DS_RP_TD_Y", series(25, extra={"AGE": "W"})[:P])
    run_until_quiet(env, fake)
    fl = env.state()["flows"]
    assert fl["DS_RP_TD_X"]["hold"] and fl["DS_RP_TD_Y"]["status"] == S.COMPLETE   # the queue moved on
    capsys.readouterr()
    r = env.run(fake)
    out = capsys.readouterr().out
    assert "DS_RP_TD_X: merge plan" not in out                         # a held finalize is not re-run
    assert r.status not in ("ok", "no_change", "transient_fail")       # loud, and not "due" for ever
    assert "HELD (spelling drift?): DS_RP_TD_X" in r.error
    assert _chunks(env, "DS_RP_TD_X")


def test_F1_every_merge_logs_its_split_and_the_served_rows_before_and_after(env, capsys):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    out = capsys.readouterr().out
    assert "DS_RP_TD_SEED: merge plan - staged 25 = new keys 15 (15 keys) + new dates 0 + revised 0 + unchanged 10" in out
    assert re.search(r"DS_RP_TD_SEED: merge took [0-9.]+s for 25 staged rows \(15 new or changed\); served 10 -> 25 rows", out)


def _parquet(tmp_path, name, rows):
    p = tmp_path / f"{name}.parquet"
    pq.write_table(pa.table({"flow": ["F"] * len(rows), "series_key": [r[0] for r in rows],
                             "obs_date": pa.array([r[1] for r in rows], pa.date32()),
                             "value": [r[2] for r in rows]}), str(p))
    return str(p).replace("\\", "/")


def test_F1_the_split_counts_each_row_once_and_the_tail_coverage_uses_the_start_year(tmp_path):
    import datetime as dt
    d = dt.date
    stored = _parquet(tmp_path, "s", [("A", d(2022, 12, 31), 1.0), ("A", d(2023, 12, 31), 2.0),
                                      ("B", d(2023, 12, 31), 3.0), ("C", d(2020, 12, 31), 4.0)])
    staged = _parquet(tmp_path, "n", [("A", d(2023, 12, 31), 2.0),       # unchanged
                                      ("B", d(2023, 12, 31), 9.0),       # revised
                                      ("A", d(2024, 12, 31), 5.0),       # new date, known key
                                      ("Z", d(2024, 12, 31), 6.0)])      # new key
    con = duckdb.connect()
    sp = M._merge_split(con, staged, stored, {"mode": "tail", "start": 2023})
    assert (sp["S"], sp["NK"], sp["ND"], sp["RV"], sp["U"]) == (4, 1, 1, 1, 1)
    assert (sp["expected"], sp["covered"]) == (2, 2)                     # A and B have 2023+ data; C does not
    sp_o = M._merge_split(con, staged, stored, {"mode": "origin", "start": None})
    assert (sp_o["expected"], sp_o["covered"]) == (3, 2)                 # an origin pull should carry C too


def test_F1_respelled_rows_are_duplicates_or_split_series_and_a_known_partition_is_not_new(tmp_path):
    import datetime as dt
    d = dt.date
    stored = _parquet(tmp_path, "s", [("G=2026-C-1:S=_T", d(2023, 12, 31), 1.0),
                                      ("G=2026-C-2:S=_T", d(2022, 12, 31), 2.0),
                                      ("GEO=2025-C-7:S=_T", d(2020, 12, 31), 7.0),
                                      ("GEO=2026-C-8:S=_T", d(2021, 12, 31), 8.0)])
    staged = _parquet(tmp_path, "n", [("G=2026-C-1:REF=X:S=_T", d(2023, 12, 31), 1.0),  # duplicate (a)
                                      ("G=2026-C-2:REF=X:S=_T", d(2023, 12, 31), 2.5),  # split series (b)
                                      ("G=2026-C-9:REF=X:S=_T", d(2023, 12, 31), 9.0),  # a new series
                                      ("GEO=2026-C-7:S=_T", d(2021, 12, 31), 7.5)])     # vintage 2026 is stored
    con = duckdb.connect()
    sp = M._merge_split(con, staged, stored, {"mode": "origin", "start": None})
    assert (sp["NK"], sp["respelled_duplicates"], sp["respelled_split"], sp["respelled"]) == (4, 1, 1, 2)


def test_F1_the_hold_threshold_is_strict_and_needs_a_store():
    base = {"stored_rows": 10, "S": 100, "respelled_duplicates": 0, "respelled_split": 0}
    assert not M._spelling_hold(dict(base, respelled=5))                 # 5% exactly: not above
    assert M._spelling_hold(dict(base, respelled=6))
    assert not M._spelling_hold(dict(base, respelled=0))
    assert not M._spelling_hold(dict(base, respelled=50, stored_rows=0))  # nothing stored: nothing to re-spell


# ------------------------------------------------------------------ F2: classification, probe, control lines
def test_F2_classification_probes_and_controls_each_leave_a_line(env, capsys):
    flows = _control_flows()
    flows["DS_OK"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_OK", flows["DS_OK"])
    env.run(fake)
    out = capsys.readouterr().out
    assert re.search(r"classify DS_OK: 13 rows, s_norm 13, 1 partition\(s\) \(largest 13\) -> legacy", out)
    assert re.search(r"classify DS_RP_TD_SEED: 10 rows.* -> needs_origin: total 10 is an exact multiple", out)
    assert re.search(r"probe controls PASS: positive DS_CTRL page 2 10 obs \(needs > 0\); negative "
                     r"DS_COEFF_EURO_FRANC page 2 0 obs \(needs 0\)", out)
    assert re.search(r"probe DS_OK page 3: 0 obs, 0 parsed, 0 new, http .* -> empty", out)


# ------------------------------------------------------------------ F3: confirmed hits, queue order, drop reasons
def test_F3_a_probe_page_of_empty_cells_is_not_a_hit(env, capsys):
    flows = _control_flows()
    flows["DS_E"] = series(13) + null_obs(20)                          # INSEE pages carry empty cells
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_E", series(13))
    env.run(fake)
    fe = env.state()["flows"]["DS_E"]
    assert fe["probe"]["raw"] > 0 and fe["probe"]["parsed"] == 0 and fe["probe"]["new_pairs"] == 0
    assert fe["probe"]["dropped"] == {"no_value": fe["probe"]["raw"]}
    assert "prio" not in fe and not fe.get("repair")                  # never sent to an origin re-pull
    assert re.search(r"probe DS_E page 3: 10 obs, 0 parsed, 0 new, .* -> nothing new", capsys.readouterr().out)


def test_F3_a_probe_page_with_new_pairs_is_a_hit(env):
    flows = _control_flows()
    flows["DS_H"] = series(40)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_H", series(13))
    env.run(fake)
    fe = env.state()["flows"]["DS_H"]
    assert fe["probe"]["new_pairs"] > 0 and fe.get("prio") == 5
    assert fe["status"] in (S.NEEDS_ORIGIN, S.IN_PROGRESS, S.MERGE_PENDING, S.COMPLETE)


def test_F3_proven_truncations_are_repaired_before_probe_hits(env):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    st = env.state()
    st["flows"]["DS_AAA"] = {"status": S.NEEDS_ORIGIN, "why": "probe page 3 returned 4 new", "prio": 5, "repair": True}
    st["flows"]["DS_ZZZ"] = {"status": S.NEEDS_ORIGIN, "why": "total 20,000 is an exact multiple", "prio": 2,
                             "repair": True}
    _write_state(env, st)
    assert _pass(env, fake)._next_repair() == "DS_ZZZ"
    assert M._origin_priority("a CI-logged delta was an exact multiple") < M._origin_priority("total 10")
    assert M._origin_priority("zero rows or no stored object") < M._origin_priority("probe page 3 returned")


def test_F3_dropped_observations_are_counted_by_reason_and_kept_on_the_flow(env, capsys):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(25) + null_obs(4, year=2023)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe["dropped"] == {"no_value": 4} and env.rows("DS_RP_TD_SEED") == 25
    assert "dropped observations {'no_value': 4}" in capsys.readouterr().out
    assert M._drop_counts([{"dimensions": {"TIME_PERIOD": "2023-A1"}, "measures": {}}]) == {"no_date": 1}


# ------------------------------------------------------------------ F4: a walk tail that runs out of slot is parked
def test_F4_a_walk_tail_stopped_by_the_slot_end_is_parked_and_queues_after_needs_origin(env):
    flows = _control_flows()
    flows["DS_G"] = series(5, year=2023)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_G", flows["DS_G"])
    run_until_quiet(env, fake)
    fake.flows["DS_G"] = series(29, year=2023)                         # 3 pages, one chunk is 3 pages
    p = _pass(env, fake)
    p.st.flows["DS_NEEDS"] = {"status": S.NEEDS_ORIGIN, "why": "total 10 is an exact multiple", "prio": 2,
                              "repair": True}
    res = p.work("DS_G", env.clock() + 1.5 * fake.step, in_walk=True, modified="2026-10-17T00:00:00")
    fe = p.st.flows["DS_G"]
    assert res == "progress" and fe["status"] == S.NEEDS_REFRESH
    assert (fe.get("attempt") or {}).get("chunks", 0) >= 1 and _chunks(env, "DS_G")   # committed pages kept
    assert p._next_repair() == "DS_NEEDS"                               # not resumed ahead of needs_origin


# ------------------------------------------------------------------ F6: one denominator, digest-width first
def test_F6_the_note_starts_with_n_of_N_and_the_pages(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    r = env.run(fake)
    head = r.error[:80]
    assert re.match(r"repair \d+/\d+ complete, \+\d+p, DS_RP_TD_SEED@p\d+", head), head
    m = re.search(r"of the (\d+): (\d+) open, (\d+) held", r.error)
    done = int(re.match(r"repair (\d+)/(\d+)", r.error).group(1))
    assert int(m.group(2)) + int(m.group(3)) + done <= int(m.group(1))


# ------------------------------------------------------------------ F7: chunk, state and resume lines
def test_F7_chunks_state_writes_and_resumes_are_logged(env, capsys):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    at = run_until_chunk(env, fake, "DS_RP_TD_SEED")
    out = capsys.readouterr().out
    assert re.search(r"DS_RP_TD_SEED: chunk 0 uploaded - pages 1-3, 30 rows of 30 obs, [0-9,]+ bytes", out)
    assert re.search(r"state written #\d+ \(token [0-9a-f]{12}\)", out)
    assert re.search(r"DS_RP_TD_SEED: attempt a\d{14}-[0-9a-f]{6} origin - started at page 1", out)
    env.run(fake)
    out = capsys.readouterr().out
    nxt = int(at["next_page"])
    assert f"DS_RP_TD_SEED: resume check ok - page {nxt - 1} fingerprint" in out
    assert re.search(rf"DS_RP_TD_SEED: attempt {at['id']} origin - resumed at page {nxt} ", out)


# ------------------------------------------------------------------ F8: residue and start_ignored printed
def test_F8_residue_and_start_ignored_are_printed_when_non_zero(env, capsys):
    flows = _control_flows()
    flows["DS_RP_TD_OLD"] = series(25, geo="2026")
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_RP_TD_OLD", series(P, geo="2025", year=2018) + series(25, geo="2026")[:15])
    run_until_quiet(env, fake)
    out = capsys.readouterr().out
    assert re.search(r"DS_RP_TD_OLD: residue 10 stored rows no longer served \(GEO:SEX\|2025 10\); "
                     r"start_ignored False", out)


def test_F1_a_tail_that_lost_series_but_gained_no_keys_is_merged(env):
    """Low coverage alone is not a respelling: INSEE stopped serving most series, the rest gained a year."""
    flows = _control_flows()
    old = series(10, year=2023)
    flows["DS_L"] = list(old)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_L", old + series(2, year=2022))
    run_until_quiet(env, fake)
    fake.flows["DS_L"] = series(3, year=2023) + series(3, year=2024)  # 7 of 10 series discontinued
    fake.modified["DS_L"] = "2026-10-17T00:00:00"
    env.run(fake)
    fe = env.state()["flows"]["DS_L"]
    assert not fe.get("hold") and env.rows("DS_L") == 12 + 3


def test_F3_a_probe_page_of_rows_already_stored_is_not_a_hit(env):
    both = series(13, geo="2026") + series(13, geo="2025")            # s_norm 13: the vintages collapse
    flows = _control_flows()
    flows["DS_D"] = list(both)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_D", both)
    env.run(fake)
    fe = env.state()["flows"]["DS_D"]
    assert fe["probe"]["page"] == 3 and fe["probe"]["parsed"] > 0 and fe["probe"]["new_pairs"] == 0
    assert "prio" not in fe and not fe.get("repair")


# ------------------------------------------------------------------ AR-334: the exits from a hold
def _held_tail(env):
    flows = _control_flows()
    old = series(8, year=2023) + series(8, year=2024)
    flows["DS_T"] = list(old)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_T", old)
    run_until_quiet(env, fake)
    fake.flows["DS_T"] = respelled(old)
    fake.modified["DS_T"] = "2026-10-17T00:00:00"
    env.run(fake)
    assert env.state()["flows"]["DS_T"]["hold"] == "spelling"
    return fake, old


def test_release_holds_merges_a_held_flow_once_keeping_both_spellings(env):
    fake, old = _held_tail(env)
    (env.store / S.FORCE_FILE).write_text(json.dumps({"release_holds": ["DS_T"]}), encoding="utf-8")
    r = env.run(fake)
    fe = env.state()["flows"]["DS_T"]
    assert "release_holds - the held merge is accepted" in r.error
    assert not fe.get("hold") and env.rows("DS_T") == len(old) + 8 and not _chunks(env, "DS_T")
    assert fe.get("hold_release_key") == "listed"


def test_dropping_a_held_attempt_clears_its_hold(env):
    fake, _ = _held_tail(env)
    p = _pass(env, fake)
    p._drop_attempt("DS_T", "test")
    fe = p.st.flows["DS_T"]
    assert not fe.get("hold") and not fe.get("hold_detail") and not _chunks(env, "DS_T")


def test_an_unconfirmed_probe_hit_from_before_this_change_is_probed_again(env):
    flows = _control_flows()
    flows["DS_OLDHIT"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_OLDHIT", flows["DS_OLDHIT"])
    run_until_quiet(env, fake)
    st = env.state()
    st["flows"]["DS_OLDHIT"].update(status=S.NEEDS_ORIGIN, why="probe page 3 returned 10000 obs", repair=True,
                                    probe={"page": 3, "raw": 10000, "http": None, "utc": "2026-10-17T11:14:00+00:00"})
    _write_state(env, st)
    r = env.run(fake)
    fe = env.state()["flows"]["DS_OLDHIT"]
    assert "is probed again under the confirmed rule" in r.error
    assert fe["status"] != S.NEEDS_ORIGIN and not fe.get("repair") and "new_pairs" in fe.get("probe", {})


def test_the_parser_names_three_drop_reasons():
    base = {"dimensions": {"GEO": "X", "TIME_PERIOD": "2023"}}
    assert M._obs_parse(dict(base, measures={"OBS_VALUE_NIVEAU": {"value": None}}))[1] == "no_value"
    assert M._obs_parse(dict(base, measures={"OBS_VALUE_NIVEAU": {"value": "n/a"}}))[1] == "non_numeric"
    assert M._obs_parse({"dimensions": {"TIME_PERIOD": "2023-A1"}, "measures": {}})[1] == "no_date"
    assert M._obs_parse(dict(base, measures={"OBS_VALUE_NIVEAU": {"value": "1.5"}}))[0][2] == 1.5


def test_F3_a_probe_page_that_only_respells_stored_rows_is_not_a_hit(env):
    """INSEE serves the stored 2025-vintage rows again under the 2026 vintage: the probe page past the parsed
    count holds pairs the store lacks EXACTLY, but holds under another spelling - not new."""
    flows = _control_flows()
    flows["DS_RV"] = series(13, geo="2025") + series(13, geo="2026")
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_RV", series(13, geo="2025"))
    env.run(fake)
    fe = env.state()["flows"]["DS_RV"]
    assert fe["probe"]["page"] == 3 and fe["probe"]["parsed"] > 0 and fe["probe"]["new_pairs"] == 0
    assert "prio" not in fe and not fe.get("repair")


def test_a_held_flow_does_not_cut_the_walk_at_minute_15(env):
    fake, _ = _held_tail(env)
    p = _pass(env, fake)
    assert not any(fe.get("status") in S.REPAIR_PENDING and not fe.get("hold") for fe in p.st.flows.values())
    ends = []
    real_work = M._Pass.work

    def spy(self, code, slot_end, **kw):
        ends.append(slot_end)
        return real_work(self, code, slot_end, **kw)
    p.work = spy.__get__(p)
    for c in ("DS_CTRL", M.PROBE_EMPTY_CONTROL):
        p.st.flows[c]["last_tail_utc"] = "2000-01-01T00:00:00+00:00"   # due by the backstop
    p.walk_slot(sorted(p.st.flows))
    assert ends and all(e == p.clock.soft_end for e in ends)
