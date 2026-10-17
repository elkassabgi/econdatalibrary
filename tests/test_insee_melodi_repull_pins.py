"""Pins for the reviewer's surviving mutants of the post-build review (V1-V18): each test kills one.

Same harness as tests/test_insee_melodi_repull.py (fake paged Melodi, fake clock, local store)."""
from __future__ import annotations

import datetime as dt
import json
import os

import pyarrow.parquet as pq
import pytest

from test_insee_melodi_repull import (env, FakeMelodi, FakeR2, series, obs, _control_flows,  # noqa: F401
                                      _seed_store, run_until_quiet, run_until_chunk, P)
from updater import blob, config
from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S


def _value(env, code, key, year=2023):
    for r in pq.read_table(str(env.store / f"{code}.parquet")).to_pylist():
        if r["series_key"] == key and r["obs_date"].year == year:
            return r["value"]
    return None


def test_V1_a_revised_value_is_a_change_and_is_served(env):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    key = "AGE=Z:GEO=2026-COM-00003:SEX=_T"
    rev = series(30, extra={"AGE": "Z"})
    rev[3]["measures"]["OBS_VALUE_NIVEAU"]["value"] = 999.0               # same key and date, new value
    fake.flows["DS_CTRL"] = rev
    fake.modified["DS_CTRL"] = "2026-10-12T00:00:00"
    r = env.run(fake)
    assert "DS_CTRL" in (r.changed_keys or {})
    assert _value(env, "DS_CTRL", key) == 999.0


def test_V2_V3_a_finished_tail_of_a_legacy_flow_stays_legacy(env):
    flows = _control_flows()
    flows["DS_MY"] = series(6, year=2017) + series(5, year=2023)        # 11 rows: no exact multiple
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_MY", flows["DS_MY"])
    run_until_quiet(env, fake)
    fake.flows["DS_MY"] = flows["DS_MY"] + [obs({"GEO": "2026-COM-88888", "SEX": "_T"}, 2023, 1.0)]
    fake.modified["DS_MY"] = "2026-10-12T00:00:00"
    env.run(fake)
    st = env.state()["flows"]["DS_MY"]
    assert any("startPeriod=2023" in u for u in fake.calls if "/data/DS_MY" in u)
    assert not st.get("start_ignored")                                  # V2: the server honoured startPeriod
    assert st["status"] == S.LEGACY and st.get("basis") == "tail"       # V3: a tail proves no history
    assert env.rows("DS_MY") == 12


def test_V4_an_outside_rewrite_with_other_rows_is_re_classified(env, monkeypatch):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    env.put("DS_CTRL", flows["DS_CTRL"][:20])                          # someone else rewrote it: 20 rows
    seen = []
    real = M._partition_profile
    monkeypatch.setattr(M, "_partition_profile", lambda p: seen.append(os.path.basename(p)) or real(p))
    env.run(fake)
    assert "DS_CTRL.parquet" in seen


def test_V5_the_30_day_backstop_tails_an_unchanged_flow(env):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    st = env.state()
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=M.BACKSTOP_DAYS + 1)).replace(microsecond=0)
    fe = st["flows"]["DS_CTRL"]
    for k in ("last_tail_utc", "last_full_utc"):
        if k in fe:
            fe[k] = old.isoformat()
    fe.setdefault("last_tail_utc", old.isoformat())
    (env.store / S.STATE_FILE).write_text(json.dumps(st, indent=1, sort_keys=True))
    n0 = len([u for u in fake.calls if "/data/DS_CTRL" in u])
    env.run(fake)                                                       # `modified` unchanged
    assert len([u for u in fake.calls if "/data/DS_CTRL" in u]) > n0


def test_V6_on_r2_the_token_chains_across_two_saves(tmp_path, monkeypatch):
    fake = FakeR2()
    monkeypatch.setattr(config, "source_dir", lambda s: str(tmp_path / "data" / "clean_full" / s))
    monkeypatch.setattr(blob, "_r2_routed", lambda: fake)
    st = S.FlowState.load()
    st.save()
    st.flows["X"] = {"status": S.LEGACY}
    st.save()                                                           # must use the token the first save got
    st.flows["Y"] = {"status": S.LEGACY}
    st.save()
    assert b'"Y"' in fake.objs["clean_full/insee_melodi/_flows.json"]


def test_V7_on_r2_a_deleted_chunk_also_leaves_the_runner_mirror(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "source_dir", lambda s: str(tmp_path / "data" / "clean_full" / s))
    monkeypatch.setattr(blob, "_r2_routed", lambda: FakeR2())
    deleted = []
    monkeypatch.setattr(blob, "delete_store_object", lambda p: deleted.append(p))
    p = tmp_path / "data" / "clean_full" / "insee_melodi" / "_repull" / "DS_X" / "a20261016120000-abcdef" / "00000.chunk"
    p.parent.mkdir(parents=True)
    p.write_bytes(b"x")
    assert S.delete_chunk(str(p), flow="DS_X", current_attempt=None)
    assert deleted and not p.exists()


def test_V8_a_deferred_page_plus_1_confirmation_refuses_a_non_empty_page(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(20)                                 # two full pages, isLast on page 2
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    fake.faults[("DS_RP_TD_SEED", 3)] = 503                             # the confirmation cannot be read
    env.run(fake)
    assert env.state()["flows"]["DS_RP_TD_SEED"]["attempt"].get("confirm_page") == 3
    del fake.faults[("DS_RP_TD_SEED", 3)]
    fake.flows["DS_RP_TD_SEED"] = series(25)                            # page 3 now carries data
    r = env.run(fake)
    assert "contradicted" in r.error
    assert env.state()["flows"]["DS_RP_TD_SEED"]["status"] != S.COMPLETE


def test_V10_the_merge_waits_for_its_margin(env, monkeypatch):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    monkeypatch.setattr(S, "merge_margin_s", lambda rows, stored_rows=0: 10.0 ** 9)
    merged = []
    real = M.merge.merge_and_write_bounded
    monkeypatch.setattr(M.merge, "merge_and_write_bounded", lambda *a, **k: merged.append(a) or real(*a, **k))
    r = env.run(fake)
    assert merged == []                                                 # no merge starts without its margin
    # AR-334 R3-1: a margin above the whole window is BLOCKED (loud, the slot moves on), not deferred - the
    # ordinary deferral is pinned by test_a_finalize_deferred_by_the_margin_names_staged_and_stored_rows
    assert "more than this pass can give" in r.error
    assert any(fe.get("status") == S.MERGE_PENDING for fe in env.state()["flows"].values())


def test_V11_a_moved_modified_restarts_the_attempt(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    run_until_chunk(env, fake, "DS_RP_TD_SEED")
    fake.modified["DS_RP_TD_SEED"] = "2026-11-01T00:00:00"
    r = env.run(fake)
    assert "moved" in r.error and "restarted" in r.error


def test_V12_a_footer_sum_that_disagrees_restarts(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    run_until_chunk(env, fake, "DS_RP_TD_SEED")
    st = env.state()
    fe = st["flows"]["DS_RP_TD_SEED"]
    fe["status"] = S.MERGE_PENDING
    fe["attempt"]["rows_staged"] = int(fe["attempt"]["rows_staged"]) + 5
    (env.store / S.STATE_FILE).write_text(json.dumps(st, indent=1, sort_keys=True))
    r = env.run(fake)
    assert "footers sum" in r.error
    assert env.rows("DS_RP_TD_SEED") == P


def test_V13_three_restarts_make_a_flow_unstable(env):
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


def test_V14_no_request_starts_that_cannot_finish(monkeypatch):
    calls = []

    class Sess:
        def get(self, *a, **k):
            calls.append(a)
            raise AssertionError("must not be called")
    c = S.Clock(now=lambda: 0.0, window_s=S.HARD_MARGIN_S + 60.0, source="test")   # 60 s left: no room
    with pytest.raises(M._Stop):
        M._request(Sess(), "https://api.insee.fr/melodi/data/X?page=1", c)
    assert calls == []


def test_V15_a_failed_positive_control_voids_the_probes(env):
    flows = _control_flows()
    flows["DS_OK"] = series(11)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_OK", flows["DS_OK"])
    fake.faults[("DS_CTRL", 2)] = "empty"                                # the positive control answers empty
    r = env.run(fake)
    assert "PROBE VOID" in r.error and "positive control" in r.error
    assert env.state()["flows"]["DS_OK"]["probe"].get("void")


def test_V17_while_repairing_the_walk_stops_at_minute_15(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(400)
    for i in range(40):
        flows[f"DS_W{i:02d}"] = series(5)
    fake = FakeMelodi(env.clock, flows, step=30.0)
    _seed_store(env, fake)
    for i in range(40):
        env.put(f"DS_W{i:02d}", flows[f"DS_W{i:02d}"])
    times = []
    orig = fake.get

    def spy(url, headers=None, timeout=None):
        if "/catalog/DS_W" in url:
            times.append(env.clock.t)
        return orig(url, headers, timeout)
    fake.get = spy
    c = S.Clock(now=env.clock, window_s=45 * 60, source="test")
    M.update(None, None, _session=fake, _clock=c)
    assert times and max(times) <= c.t0 + M.WALK_UNTIL_MIN * 60 + 2 * fake.step


def test_V18_a_walk_tail_that_outgrows_a_chunk_is_parked_not_left_in_progress(env, monkeypatch):
    flows = _control_flows()
    flows["DS_GROW"] = series(15)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_GROW", flows["DS_GROW"])
    run_until_quiet(env, fake)
    fake.flows["DS_GROW"] = series(45)                                  # INSEE grew it to 5 pages
    fake.modified["DS_GROW"] = "2026-10-12T00:00:00"
    monkeypatch.setattr(M._Pass, "repair_slot", lambda self: None)
    env.run(fake)
    st = env.state()["flows"]["DS_GROW"]
    assert st["status"] == S.NEEDS_REFRESH
    assert (st.get("attempt") or {}).get("chunks", 0) >= 1


# --------------------------------------------------------------------------------------------- round-2 pins
def test_N1_a_truncated_body_is_retried_inside_the_request(env):
    import requests
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    orig = fake.get
    n = {"k": 0}

    def flaky(url, headers=None, timeout=None):
        if "/data/DS_RP_TD_SEED" in url and "page=2" in url and n["k"] == 0:
            n["k"] += 1
            raise requests.exceptions.ChunkedEncodingError("IncompleteRead(4096 bytes read)")
        return orig(url, headers, timeout)
    fake.get = flaky
    r = env.run(fake)
    assert n["k"] == 1
    assert "FLOW FAILURE" not in (r.error or "") and "PASS ERROR" not in (r.error or "")
    assert env.state()["flows"]["DS_RP_TD_SEED"]["status"] == S.COMPLETE
    assert env.rows("DS_RP_TD_SEED") == 25


def test_N2_a_flow_that_keeps_failing_becomes_broken_and_frees_the_queue(env, monkeypatch):
    flows = _control_flows()
    flows["DS_RP_TD_ZZZ"] = series(25, extra={"AGE": "Q"})
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_RP_TD_ZZZ", flows["DS_RP_TD_ZZZ"][:P])
    real = M._Pass._pages

    def boom(self, code, *a, **k):
        if code == "DS_RP_TD_SEED":
            raise RuntimeError("synthetic per-flow fault")
        return real(self, code, *a, **k)
    monkeypatch.setattr(M._Pass, "_pages", boom)
    rs = [env.run(fake) for _ in range(M.MAX_FLOW_FAILURES + 1)]
    fl = env.state()["flows"]
    assert fl["DS_RP_TD_SEED"]["status"] == S.BROKEN
    assert fl["DS_RP_TD_ZZZ"]["status"] == S.COMPLETE                  # the queue head did not block it
    assert "BROKEN" in rs[-1].error and "FLOW FAILURE" in rs[0].error


def test_N4_a_published_merge_whose_result_was_lost_is_re_emitted_next_pass(env, monkeypatch):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    real = M._result

    def lost(*a, **k):
        raise KeyboardInterrupt("killed after the merge, before the Result")
    monkeypatch.setattr(M, "_result", lost)
    with pytest.raises(KeyboardInterrupt):
        env.run(fake)
    assert env.rows("DS_RP_TD_SEED") == 25                             # the merge was published
    monkeypatch.setattr(M, "_result", real)
    r = env.run(fake)
    assert "DS_RP_TD_SEED" in (r.changed_keys or {})
    r2 = env.run(fake)
    assert "DS_RP_TD_SEED" not in (r2.changed_keys or {})              # handed over once, then cleared


def test_N6_the_bootstrap_rule_does_not_apply_after_an_origin_pull():
    pg = M.PAGE_SIZE
    prof = {"rows": pg + 25, "partitions": {"GEO:SEX|2025": pg, "GEO:SEX|2026": 25}, "s_norm": pg + 25,
            "max_obs": None}
    assert M._rule_hits("DS_X", prof, set(), {}) is not None            # bootstrap: a one-page partition hits
    assert M._rule_hits("DS_X", prof, set(), {"last_full_utc": "2026-10-01T00:00:00+00:00"}) is None
    assert M._rule_hits("DS_X", prof, {"DS_X"}, {"last_full_utc": "x"}) == "operator force_origin"


def test_N8_no_new_attempt_while_another_writer_holds_one_in_progress(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    flows["DS_RP_TD_ZZZ"] = series(25, extra={"AGE": "Q"})
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    env.put("DS_RP_TD_ZZZ", flows["DS_RP_TD_ZZZ"][:P])
    run_until_chunk(env, fake, "DS_RP_TD_SEED")
    st = env.state()
    seed = st["flows"]["DS_RP_TD_SEED"]
    assert seed["status"] == S.IN_PROGRESS
    seed["attempt"]["owner"], seed["attempt"]["heartbeat_utc"] = "gh:other", S.utcnow()
    st["flows"]["DS_RP_TD_ZZZ"]["status"] = S.NEEDS_ORIGIN
    (env.store / S.STATE_FILE).write_text(json.dumps(st, indent=1, sort_keys=True))
    n0 = len([u for u in fake.calls if "/data/DS_RP_TD_ZZZ" in u])
    r = env.run(fake)
    fl = env.state()["flows"]
    assert fl["DS_RP_TD_ZZZ"]["status"] == S.NEEDS_ORIGIN
    assert len([u for u in fake.calls if "/data/DS_RP_TD_ZZZ" in u]) == n0
    assert "leased by gh:other" in r.error
