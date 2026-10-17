"""Pins for AR-331's binding conditions 3-9 (the fixes after X1-X4), one test or more per condition.

Same harness as tests/test_insee_melodi_repull.py (fake paged Melodi, fake clock, local store)."""
from __future__ import annotations

import json
import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from test_insee_melodi_repull import (env, FakeMelodi, series, obs, _control_flows,  # noqa: F401
                                      _seed_store, run_until_quiet, run_until_chunk, P)
from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S


def _force(env, flows, utc=None):
    d = {"flows": flows}
    if utc:
        d["utc"] = utc
    (env.store / S.FORCE_FILE).write_text(json.dumps(d), encoding="utf-8")


def _write_state(env, st):
    (env.store / S.STATE_FILE).write_text(json.dumps(st, indent=1, sort_keys=True))


def _origin_pulls(fake, code):
    return len([u for u in fake.calls if f"/data/{code}?page=1" in u])


# ------------------------------------------------------------------ condition 3 (X1): one pull per listing
def test_a_force_entry_makes_one_origin_pull_and_a_new_per_flow_value_makes_another(env):
    flows = _control_flows()
    flows["DS_W"] = series(13)
    flows["DS_V"] = series(14, extra={"AGE": "Y"})
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    env.put("DS_V", flows["DS_V"])
    run_until_quiet(env, fake)
    (env.store / S.FORCE_FILE).write_text(json.dumps({"flows": {"DS_W": "2026-10-16T22:00:00Z",
                                                                "DS_V": "2026-10-16T22:00:00Z"}}))
    w0, v0 = _origin_pulls(fake, "DS_W"), _origin_pulls(fake, "DS_V")
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_W"]
    assert fe["status"] == S.COMPLETE and fe.get("basis") == "origin"
    assert (_origin_pulls(fake, "DS_W"), _origin_pulls(fake, "DS_V")) == (w0 + 1, v0 + 1)   # one pull each
    for _ in range(2):
        env.run(fake)
    assert (_origin_pulls(fake, "DS_W"), _origin_pulls(fake, "DS_V")) == (w0 + 1, v0 + 1)   # not re-used
    (env.store / S.FORCE_FILE).write_text(json.dumps({"flows": {"DS_W": "2026-10-17T08:00:00Z",
                                                                "DS_V": "2026-10-16T22:00:00Z"}}))
    run_until_quiet(env, fake)
    assert (_origin_pulls(fake, "DS_W"), _origin_pulls(fake, "DS_V")) == (w0 + 2, v0 + 1)   # only DS_W again


def test_list_form_forces_again_only_after_the_flow_left_the_list_and_an_unreadable_file_clears_nothing(env):
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    _force(env, ["DS_W"])
    run_until_quiet(env, fake)
    n0 = _origin_pulls(fake, "DS_W")
    (env.store / S.FORCE_FILE).write_text("{not json")                  # unreadable: keys are kept
    env.run(fake)
    _force(env, ["DS_W"])
    env.run(fake)
    assert _origin_pulls(fake, "DS_W") == n0 and env.state()["flows"]["DS_W"].get("forced_key") == "listed"
    _force(env, [])                                                     # off the list for one pass
    env.run(fake)
    assert "forced_key" not in env.state()["flows"]["DS_W"]
    _force(env, ["DS_W"])                                               # listed again: forced again
    run_until_quiet(env, fake)
    assert _origin_pulls(fake, "DS_W") == n0 + 1


def test_force_does_not_disturb_an_origin_pull_already_in_progress(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    at0 = run_until_chunk(env, fake, "DS_RP_TD_SEED")
    assert at0.get("mode") == "origin"
    _force(env, ["DS_RP_TD_SEED"])
    r = env.run(fake)
    assert "dropped" not in (r.error or "") and "restarted" not in (r.error or "")
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    at1 = fe.get("attempt") or {}
    assert at1.get("id") == at0["id"] or (fe["status"] == S.COMPLETE and fe.get("basis") == "origin")
    assert fe.get("forced_key")                                        # the listing is recorded as used


# ------------------------------------------------------------------ condition 4 (X2): exits for every stuck status
def test_force_takes_unstable_and_withdrawn_flows_out(env):
    flows = _control_flows()
    flows["DS_U"] = series(13)
    flows["DS_V"] = series(14, extra={"AGE": "Y"})
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_U", flows["DS_U"])
    env.put("DS_V", flows["DS_V"])
    run_until_quiet(env, fake)
    st = env.state()
    st["flows"]["DS_U"].update(status=S.UNSTABLE, why="3 restarts: x")
    st["flows"]["DS_V"].update(status=S.WITHDRAWN, why="origin page 1 empty")
    _write_state(env, st)
    _force(env, ["DS_U", "DS_V"])
    run_until_quiet(env, fake)
    fl = env.state()["flows"]
    assert fl["DS_U"]["status"] == S.COMPLETE and fl["DS_V"]["status"] == S.COMPLETE


def test_the_runbook_documents_every_status_and_the_operator_files():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    text = open(os.path.join(root, "docs", "runbook", "insee_melodi.md"), encoding="utf-8").read()
    for s in sorted(S.STATUSES):
        assert f"`{s}`" in text, s
    for word in (S.STATE_FILE, S.FORCE_FILE, "unblock_repair_after_utc", "T0"):
        assert word in text, word


# ------------------------------------------------------------------ condition 5: no orphaned chunks
def test_a_flow_that_goes_broken_leaves_no_staged_chunks(env, monkeypatch):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    run_until_chunk(env, fake, "DS_RP_TD_SEED")
    assert [c for c in env.chunks() if "DS_RP_TD_SEED" in c]

    def boom(self, code, at):
        raise RuntimeError("synthetic per-flow fault")
    monkeypatch.setattr(M._Pass, "_resume_check", boom)
    for _ in range(M.MAX_FLOW_FAILURES):
        env.run(fake)
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe["status"] == S.BROKEN and not fe.get("attempt")
    assert not [c for c in env.chunks() if "DS_RP_TD_SEED" in c]


def test_a_flow_that_goes_unstable_leaves_no_staged_chunks(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    base = series(95)
    for i in range(10):
        run_until_chunk(env, fake, "DS_RP_TD_SEED", passes=2)
        if env.state()["flows"]["DS_RP_TD_SEED"]["status"] == S.UNSTABLE:
            break
        fake.flows["DS_RP_TD_SEED"] = list(reversed(base)) if i % 2 == 0 else base
    assert env.state()["flows"]["DS_RP_TD_SEED"]["status"] == S.UNSTABLE
    assert not [c for c in env.chunks() if "DS_RP_TD_SEED" in c]


# ------------------------------------------------------------------ condition 6: the counter resets on progress
def test_failures_separated_by_committed_progress_do_not_make_a_flow_broken(env, monkeypatch):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(400)            # 40 pages: a pass commits chunks but cannot finish it
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    run_until_chunk(env, fake, "DS_RP_TD_SEED")
    st = env.state()
    st["flows"]["DS_RP_TD_SEED"]["failures"] = M.MAX_FLOW_FAILURES - 1      # two failures so far
    _write_state(env, st)
    chunks0 = env.state()["flows"]["DS_RP_TD_SEED"]["attempt"]["chunks"]
    env.run(fake)                                                          # a pass that commits a chunk
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe["status"] == S.IN_PROGRESS and fe["attempt"]["chunks"] > chunks0   # progress, not completion
    assert "failures" not in fe                                            # the flush reset the counter
    real = M._Pass._resume_check

    def once(self, code, at):
        monkeypatch.setattr(M._Pass, "_resume_check", real)
        raise RuntimeError("one more fault")
    monkeypatch.setattr(M._Pass, "_resume_check", once)
    env.run(fake)
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe["status"] != S.BROKEN and fe.get("failures") == 1


# ------------------------------------------------------------------ condition 7: bounded profile, same numbers
def _arrow_profile(path):
    """The 87799e8 arrow implementation, kept here as the reference the DuckDB profile must equal."""
    t = pq.read_table(path, columns=["series_key", "obs_date"])
    keys = t.column("series_key")
    dims = pc.replace_substring_regex(keys, r"=[^:]*", "")
    vint = pc.coalesce(pc.struct_field(pc.extract_regex(keys, r"GEO=(?P<v>\d{4})-"), [0]),
                       pa.scalar("", pa.string()))
    part = pc.binary_join_element_wise(dims, vint, "|")
    vc = {str(d["values"]): int(d["counts"]) for d in pc.value_counts(part).to_pylist()}
    norm = pc.replace_substring_regex(keys, r"(^|:)FREQ=[^:]*", "")
    norm = pc.replace_substring_regex(norm, r"^:", "")
    norm = pc.replace_substring_regex(norm, r"GEO=\d{4}-", "GEO=")
    s_norm = pa.table({"k": norm, "d": t.column("obs_date")}).group_by(["k", "d"]).aggregate([]).num_rows
    mx = pc.max(t.column("obs_date")).as_py() if t.num_rows else None
    maxyear = int(pc.sum(pc.equal(pc.year(t.column("obs_date")), mx.year)).as_py() or 0) if mx else 0
    return {"rows": t.num_rows, "partitions": vc, "s_norm": s_norm,
            "max_obs": mx.isoformat() if mx else None, "maxyear_rows": maxyear}


def test_the_duckdb_profile_equals_the_arrow_one_under_a_small_memory_limit(env, monkeypatch):
    rows = []
    for y in (2017, 2019, 2023):
        rows += series(300, geo="2025", year=y)                            # old GEO vintage
        rows += series(300, geo="2026", year=y)                            # new vintage: same normalised key
        rows += series(200, year=y, extra={"FREQ": "A"})                   # FREQ spelling
        rows += series(100, year=y, extra={"AGE": "Y_LT5", "GEO": "FRANCE"})   # no vintage in GEO
    env.put("DS_PROF", rows)
    path = str(env.store / "DS_PROF.parquet")
    monkeypatch.setenv("AQUEDUCT_INSEE_DUCKDB_MEMORY", "64MB")
    got = M._partition_profile(path)
    assert got == _arrow_profile(path)
    assert got["s_norm"] < got["rows"]                                     # the vintages DID collapse
    assert not [d for d in os.listdir(env.tmp / "spill") if d.startswith("merge_pid")]   # spill removed


def test_a_bad_memory_limit_is_refused(env, monkeypatch):
    env.put("DS_PROF", series(5))
    monkeypatch.setenv("AQUEDUCT_INSEE_DUCKDB_MEMORY", "1GB'; DROP")
    try:
        M._partition_profile(str(env.store / "DS_PROF.parquet"))
    except ValueError as e:
        assert "memory_limit" in str(e)
    else:
        raise AssertionError("an unsafe memory_limit was accepted")


# ------------------------------------------------------------------ condition 8: a Result in a tiny window
def test_a_five_minute_window_returns_a_result(env):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    r = env.run(fake, window_s=5 * 60)
    assert r is not None and r.status == "transient_fail"
    assert "CATALOGUE NOT READ" in r.error


def test_an_unreadable_catalogue_returns_a_result(env):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    orig = fake.get

    def down(url, headers=None, timeout=None):
        if "/dataflow/all" in url:
            from test_insee_melodi_repull import Resp
            return Resp(503)
        return orig(url, headers, timeout)
    fake.get = down
    r = env.run(fake)
    assert r.status == "transient_fail" and "CATALOGUE NOT READ" in r.error


# ------------------------------------------------------------------ condition 9: no un-resumable parked tail
def test_a_walk_tail_without_a_known_modified_is_queued_not_parked(env, monkeypatch):
    flows = _control_flows()
    flows["DS_GROW"] = series(15)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_GROW", flows["DS_GROW"])
    run_until_quiet(env, fake)
    fake.flows["DS_GROW"] = series(45)
    fake.modified["DS_GROW"] = None                                        # catalogue entry unreadable
    real_repair = M._Pass.repair_slot
    monkeypatch.setattr(M._Pass, "repair_slot", lambda self: None)
    env.run(fake)
    fe = env.state()["flows"]["DS_GROW"]
    assert fe["status"] == S.NEEDS_REFRESH and not (fe.get("attempt") or {}).get("id")
    assert not [c for c in env.chunks() if "DS_GROW" in c]
    monkeypatch.setattr(M._Pass, "repair_slot", real_repair)
    fake.modified["DS_GROW"] = "2026-10-12T00:00:00"
    rs = run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_GROW"]
    assert env.rows("DS_GROW") == 45 and fe["status"] != S.UNSTABLE
    assert not any("DS_GROW: attempt restarted" in (r.error or "") for r in rs)


# ------------------------------------------------------------------ X3: the absence is loud
def test_a_missing_served_object_is_named_in_the_pass_note(env):
    flows = _control_flows()
    flows["DS_C2"] = series(40, extra={"AGE": "Y"})
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_C2", flows["DS_C2"])
    run_until_quiet(env, fake)
    os.remove(env.store / "DS_C2.parquet")
    r = env.run(fake)
    assert "SERVED OBJECT MISSING: DS_C2" in r.error and r.status not in ("ok", "no_change")
    assert "s_norm" not in env.state()["flows"]["DS_C2"] or env.rows("DS_C2") == 40


# ------------------------------------------------------------------ each X4 / X3 layer on its own
def _pass(env, fake):
    c = S.Clock(now=env.clock, window_s=45 * 60, source="test")
    return M._Pass(fake, c, S.FlowState.load(), set())


def _parked_tail(env, monkeypatch):
    """DS_T legacy with a parked TAIL attempt (needs_refresh, chunks on the store)."""
    flows = _control_flows()
    base = series(10, year=2020) + series(10, year=2021) + series(3, year=2022)
    flows["DS_T"] = list(base)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_T", base)
    run_until_quiet(env, fake)
    extra = [obs({"GEO": f"2026-COM-{i:05d}", "SEX": "F"}, 2022, 1.0 + i) for i in range(40)]
    fake.flows["DS_T"] = base + extra
    fake.modified["DS_T"] = "2026-10-12T00:00:00"
    real = M._Pass.repair_slot
    monkeypatch.setattr(M._Pass, "repair_slot", lambda self: None)
    env.run(fake)
    monkeypatch.setattr(M._Pass, "repair_slot", real)
    fe = env.state()["flows"]["DS_T"]
    assert fe["status"] == S.NEEDS_REFRESH and fe["attempt"]["mode"] == "tail"
    assert [c for c in env.chunks() if "DS_T/" in c]
    return fake, base, extra


def test_X4_layer_identity_drops_the_attempt_of_a_changed_flow(env, monkeypatch):
    fake, base, _ = _parked_tail(env, monkeypatch)
    env.put("DS_T", base[:10])
    p = _pass(env, fake)
    p.check_identity(["DS_T"])
    fe = p.st.flows["DS_T"]
    assert fe["status"] == S.UNCLASSIFIED and not fe.get("attempt")
    assert not [c for c in env.chunks() if "DS_T/" in c]
    assert not env.state()["flows"]["DS_T"].get("attempt")             # the state was saved FIRST


def test_X4_layer_classify_drops_an_attempt_it_finds(env, monkeypatch):
    fake, base, _ = _parked_tail(env, monkeypatch)
    st = env.state()
    st["flows"]["DS_T"]["status"] = S.UNCLASSIFIED                     # as a state load of an unknown status does
    _write_state(env, st)
    p = _pass(env, fake)
    p.classify(["DS_T"])
    assert not p.st.flows["DS_T"].get("attempt")
    assert not [c for c in env.chunks() if "DS_T/" in c]


def test_X4_layer_work_refuses_a_tail_attempt_under_needs_origin(env, monkeypatch):
    fake, base, extra = _parked_tail(env, monkeypatch)
    st = env.state()
    st["flows"]["DS_T"].update(status=S.NEEDS_ORIGIN, why="operator", repair=True)   # verdict changed, attempt kept
    _write_state(env, st)
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_T"]
    assert fe.get("basis") == "origin" and fe["status"] == S.COMPLETE
    assert env.rows("DS_T") == len(base) + len(extra)


def test_X4_layer_finalize_never_rests_a_needs_origin_flow_on_a_tail(env, monkeypatch):
    fake, base, extra = _parked_tail(env, monkeypatch)
    st = env.state()
    at = st["flows"]["DS_T"]["attempt"]
    at["prior_status"] = S.NEEDS_ORIGIN                               # a tail attempt that a needs_origin started
    st["flows"]["DS_T"]["status"] = S.MERGE_PENDING
    _write_state(env, st)
    p = _pass(env, fake)
    p._finalize("DS_T")
    assert p.st.flows["DS_T"]["status"] == S.NEEDS_ORIGIN


def test_X3_rule_order_absent_beats_the_origin_exemption():
    assert M._rule_hits("DS_X", None, set(), {"last_full_utc": "2026-10-01T00:00:00+00:00"}) == \
        "zero rows or no stored object"
    assert M._rule_hits("DS_X", {"rows": 0, "partitions": {}, "s_norm": 0, "max_obs": None}, set(),
                        {"last_full_utc": "x"}) == "zero rows or no stored object"


def test_X3_layer_classify_clears_a_stale_count(env):
    flows = _control_flows()
    flows["DS_C2"] = series(40, extra={"AGE": "Y"})
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_C2", flows["DS_C2"])
    run_until_quiet(env, fake)
    assert env.state()["flows"]["DS_C2"].get("s_norm") == 40
    os.remove(env.store / "DS_C2.parquet")
    p = _pass(env, fake)
    p.check_identity(["DS_C2"])
    p.classify(["DS_C2"])
    fe = p.st.flows["DS_C2"]
    assert fe["status"] == S.NEEDS_ORIGIN and "s_norm" not in fe and "partitions" not in fe


# ------------------------------------------------------------------ condition 7: the limit is really set
def test_the_duckdb_connection_carries_the_memory_limit_and_a_private_spill(env, monkeypatch):
    monkeypatch.setenv("AQUEDUCT_INSEE_DUCKDB_MEMORY", "96MB")
    con, spill = M._duck()
    try:
        lim = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        tmp = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    finally:
        con.close()
    assert lim.replace(" ", "") in ("91.5MiB", "96.0MB", "96MB") or lim.startswith("91"), lim
    assert os.path.normcase(os.path.abspath(tmp)) == os.path.normcase(os.path.abspath(spill))
    assert os.path.basename(spill).startswith("merge_pid")


def test_finalize_queries_run_on_the_limited_connection(env, monkeypatch):
    import inspect
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    callers = []
    real = M._duck

    def spy():
        callers.append(inspect.stack()[1].function)
        return real()
    monkeypatch.setattr(M, "_duck", spy)
    run_until_quiet(env, fake)
    assert env.state()["flows"]["DS_RP_TD_SEED"]["status"] == S.COMPLETE
    assert "_finalize" in callers and "_partition_profile" in callers


# ------------------------------------------------------------------ R2-A: merge_pending never ends in deletion
def test_any_fault_while_merge_pending_keeps_the_staged_pull(env, monkeypatch):
    """A fault that is not a merge refusal (here: the chunk reads fail) reaches flow_failed; a merge_pending
    flow must still never become broken, and its chunks stay."""
    from updater.errors import DefinitiveError
    fake = FakeMelodi(env.clock, _control_flows())
    _seed_store(env, fake)
    real_merge = M.merge.merge_and_write_bounded
    monkeypatch.setattr(M.merge, "merge_and_write_bounded",
                        lambda *a, **k: (_ for _ in ()).throw(DefinitiveError("refused (test)")))
    env.run(fake)
    assert env.state()["flows"]["DS_RP_TD_SEED"]["status"] == S.MERGE_PENDING
    monkeypatch.setattr(M.merge, "merge_and_write_bounded", real_merge)
    real_read = M.blob.read_bytes

    def flaky(path):
        if str(path).endswith(".chunk"):
            raise OSError("R2 503 on a chunk GET (test)")
        return real_read(path)
    monkeypatch.setattr(M.blob, "read_bytes", flaky)
    rs = [env.run(fake) for _ in range(M.MAX_FLOW_FAILURES + 1)]
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe["status"] == S.MERGE_PENDING and not fe.get("failures")
    assert [c for c in env.chunks() if "/DS_RP_TD_SEED/" in c]
    assert "NEEDS THE OWNER" in rs[-1].error and "MERGE REFUSED" in rs[-1].error
    monkeypatch.setattr(M.blob, "read_bytes", real_read)
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe["status"] == S.COMPLETE and "merge_refusals" not in fe and env.rows("DS_RP_TD_SEED") == 25


def test_repeated_merge_refusals_escalate_in_the_note(env, monkeypatch):
    from updater.errors import DefinitiveError
    fake = FakeMelodi(env.clock, _control_flows())
    _seed_store(env, fake)
    monkeypatch.setattr(M.merge, "merge_and_write_bounded",
                        lambda *a, **k: (_ for _ in ()).throw(DefinitiveError("spill cap (test)")))
    rs = [env.run(fake) for _ in range(M.MAX_FLOW_FAILURES)]
    assert "MERGE REFUSED 1 pass(es)" in rs[0].error and "NEEDS THE OWNER" not in rs[0].error
    assert f"MERGE REFUSED {M.MAX_FLOW_FAILURES} pass(es)" in rs[-1].error and "NEEDS THE OWNER" in rs[-1].error
    assert env.state()["flows"]["DS_RP_TD_SEED"]["merge_refusals"] == M.MAX_FLOW_FAILURES


# ------------------------------------------------------------------ R2-B: the alarm outside merge_pending
def test_the_hard_limit_during_classification_stops_the_unit_and_is_not_a_flow_failure(env, monkeypatch):
    """The alarm arriving through DuckDB ('Query interrupted') in the CLASSIFICATION profile - where no
    merge_pending guard applies - must end the unit, not count as one more failure of that flow."""
    import pytest
    from updater import orchestrate
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    real = M._partition_profile

    def alarm(path):
        if path.endswith("DS_W.parquet"):
            orchestrate.UNIT_TIMEOUT_FIRED = True
            raise RuntimeError("Query interrupted")
        return real(path)
    monkeypatch.setattr(M, "_partition_profile", alarm)
    with pytest.raises(RuntimeError, match="Query interrupted"):
        env.run(fake)
    fe = (env.state()["flows"].get("DS_W") or {}) if (env.store / S.STATE_FILE).exists() else {}
    assert not fe.get("failures")
