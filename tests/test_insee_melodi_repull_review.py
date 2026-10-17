"""ADVERSARIAL REVIEW tests (post-build review of 032b94c, ported unchanged in intent: setup runs until a chunk exists).

Each test asserts the behaviour the binding design (AR-330 C1-C15, R2-1..R2-8) or plain correctness REQUIRES.
A FAILING test here is a demonstrated defect in the commit; a passing one is a protocol property that held.
"""
from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
import requests

from test_insee_melodi_repull import (env, FakeMelodi, series, obs, _control_flows, _seed_store,  # noqa: F401
                                      run_until_quiet, run_until_chunk, P)
from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S


# --------------------------------------------------------------------------------------------- containment
def test_ADV1_a_corrupt_chunk_at_the_head_of_the_queue_is_a_per_flow_failure_not_a_crash(env):
    """C11/R44: a per-flow fault must stay per-flow. A MERGE_PENDING flow is picked FIRST every pass, so an
    uncontained exception there repeats every pass (wedge) and loses the Result (note + changed_keys)."""
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    run_until_chunk(env, fake, "DS_RP_TD_SEED")
    ch = sorted(env.store.rglob("00000.chunk"))[0]
    ch.write_bytes(b"PAR1 not really a parquet")
    st = env.state()
    st["flows"]["DS_RP_TD_SEED"]["status"] = S.MERGE_PENDING
    (env.store / S.STATE_FILE).write_bytes(json.dumps(st, indent=1, sort_keys=True).encode())
    r = env.run(fake)                                   # must RETURN a Result
    assert r.status == "transient_fail"


def test_ADV2_a_truncated_body_is_retried_not_a_whole_pass_crash(env):
    """_request catches only Timeout/ConnectionError: requests' ChunkedEncodingError (IncompleteRead on a
    multi-MB page body) escapes every handler and ends the whole pass with no Result."""
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
    assert r is not None


def test_ADV3_changed_keys_of_a_merge_survive_a_later_crash_in_the_same_pass(env):
    """A walk merge publishes a parquet; a later uncontained exception in the SAME pass loses the Result, so
    the orchestrator derives no CSV. The flow's `modified` is saved, so no later pass reports it changed:
    the served CSV stays behind the parquet until INSEE changes the data again."""
    flows = _control_flows()
    flows["DS_W"] = series(5)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", series(4))
    fake.ignore_start.add("DS_W")
    run_until_quiet(env, fake)                          # everything classified / complete
    fake.flows["DS_W"] = series(5)[:4] + [obs({"GEO": "2026-COM-77777", "SEX": "_T"}, 2023, 9.0)]
    fake.modified["DS_W"] = "2026-10-10T00:00:00"       # the walk will tail DS_W and merge a new row
    # a NEW one-page flow appears: classified needs_origin this pass, pulled in the REPAIR slot, i.e. AFTER
    # the walk has merged DS_W; its page 2 body is truncated in transit
    fake.flows["DS_RP_TD_X"] = series(25, extra={"AGE": "Y"})
    fake.modified["DS_RP_TD_X"] = "2026-06-25T00:00:00"
    env.put("DS_RP_TD_X", fake.flows["DS_RP_TD_X"][:P])
    orig = fake.get

    def crash_after_dsw(url, headers=None, timeout=None):
        if "/data/DS_RP_TD_X" in url and "page=2" in url and "/data/DS_W" in str(fake.calls):
            raise requests.exceptions.ChunkedEncodingError("IncompleteRead")
        return orig(url, headers, timeout)
    fake.get = crash_after_dsw
    try:
        r1 = env.run(fake)
        ck1 = set((r1.changed_keys or {}))
    except Exception:                                   # noqa: BLE001
        ck1 = set()
    fake.get = orig
    later = set()
    for _ in range(3):
        later |= set((env.run(fake).changed_keys or {}))
    assert env.rows("DS_W") == 6                        # the merge DID publish the new row
    assert "DS_W" in (ck1 | later), "DS_W was merged but its changed key never reached the orchestrator"


# --------------------------------------------------------------------------------------------- one in progress
def test_ADV4_at_most_one_attempt_in_progress(env, monkeypatch):
    """Accepted design: ONE flow in progress at a time; C5: a pull larger than N pages takes the single
    in-progress slot or is deferred. Two big single-year legacy tails (startPeriod = their only year) both
    start in the walk and both stay in_progress."""
    flows = _control_flows()
    flows["DS_BIGA"] = series(45)
    flows["DS_BIGB"] = series(45, prefix="B", extra={"AGE": "Y"})
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_BIGA", flows["DS_BIGA"])                # stored == served: probe empty -> legacy
    env.put("DS_BIGB", flows["DS_BIGB"])
    monkeypatch.setattr(M._Pass, "repair_slot", lambda self: None)
    env.run(fake)
    st = env.state()["flows"]
    inprog = sorted(c for c, fe in st.items() if fe.get("status") == S.IN_PROGRESS)
    assert len(inprog) <= 1, inprog


# --------------------------------------------------------------------------------------------- C9 lease / CAS
def test_ADV5_a_restart_that_loses_the_CAS_race_must_not_delete_the_winners_chunks(env, monkeypatch):
    """_restart sweeps the attempt's chunks BEFORE its conditional save. When another writer saved in between
    (its cursor still names those chunks), our save is refused - but the chunks are already gone, so the
    winner's attempt is destroyed (C9: a second run must not damage the leased attempt)."""
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    run_until_chunk(env, fake, "DS_RP_TD_SEED")
    at = env.state()["flows"]["DS_RP_TD_SEED"]["attempt"]
    assert at["chunks"] >= 1
    fake.flows["DS_RP_TD_SEED"] = list(reversed(series(95)))     # makes OUR resume check fail -> _restart
    real = M._Pass._restart
    path = env.store / S.STATE_FILE

    def other_writer_lands_first(self, code, why):
        d = json.loads(path.read_text())
        d["flows"][code]["attempt"]["heartbeat_utc"] = S.utcnow()   # the other writer's flush/heartbeat
        d["writer"] = "gh:other"
        path.write_text(json.dumps(d, indent=1, sort_keys=True))
        return real(self, code, why)
    monkeypatch.setattr(M._Pass, "_restart", other_writer_lands_first)
    env.run(fake)
    win = json.loads(path.read_text())
    assert win["writer"] == "gh:other"                               # our save was refused
    still = win["flows"]["DS_RP_TD_SEED"]["attempt"]
    assert [c for c in env.chunks() if still["id"] in c], "the winning state's chunks were deleted"


def test_ADV6_a_resumed_attempt_persists_its_lease_before_working(env, monkeypatch):
    """C9 lease: when a pass resumes an attempt, the heartbeat on the STORE must be fresh before pages are
    fetched; otherwise a second run started in the meantime reads the previous pass's heartbeat as stale."""
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    env.run(fake)
    st = env.state()
    old = "2026-01-01T00:00:00+00:00"
    st["flows"]["DS_RP_TD_SEED"]["attempt"]["heartbeat_utc"] = old
    (env.store / S.STATE_FILE).write_text(json.dumps(st, indent=1, sort_keys=True))
    seen = {}
    real = M._fetch_page

    def spy(sess, flow, start, page, clock):
        if flow == "DS_RP_TD_SEED" and "hb" not in seen and page > 1:
            seen["hb"] = env.state()["flows"]["DS_RP_TD_SEED"]["attempt"]["heartbeat_utc"]
        return real(sess, flow, start, page, clock)
    monkeypatch.setattr(M, "_fetch_page", spy)
    env.run(fake)
    assert seen.get("hb") != old, "the stored lease was still the previous pass's while this pass worked"


# --------------------------------------------------------------------------------------------- C1 probe silence
def test_ADV7_a_probe_that_cannot_run_is_never_silent(env):
    """C1: every legacy flow gets a page-beyond probe; one that cannot be controlled is VOID and must be loud.
    With no one-page DS_RP_TD_* seed left (all repaired, or none classified yet) every probe is voided with no
    label and the pass reports ok/no_change."""
    flows = {M.PROBE_EMPTY_CONTROL: series(3), "DS_OK": series(11)}
    fake = FakeMelodi(env.clock, flows)
    env.put(M.PROBE_EMPTY_CONTROL, flows[M.PROBE_EMPTY_CONTROL])
    env.put("DS_OK", flows["DS_OK"])
    r = env.run(fake)
    st = env.state()["flows"]["DS_OK"]
    assert st["probe"].get("void")
    assert "PROBE VOID" in (r.error or "") or r.status not in ("ok", "no_change", "partial") \
        or "probe" in (r.error or "").lower(), (r.status, r.error)


# --------------------------------------------------------------------------------------------- parse loss
def test_ADV8_an_origin_pull_that_parses_nothing_is_not_complete(env):
    """The old fetcher raised a structural break when a populated flow's body parsed to 0 rows. Here an
    origin pull whose every observation fails to parse (TIME_PERIOD format drift) stages 0 rows and is
    certified 'complete per INSEE paging' with no label."""
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    drift = []
    for o in fake.flows["DS_RP_TD_SEED"]:
        d = dict(o["dimensions"])
        d["TIME_PERIOD"] = "2023-A1"                         # parse_date -> None
        drift.append({"dimensions": d, "measures": o["measures"]})
    fake.flows["DS_RP_TD_SEED"] = drift
    rs = run_until_quiet(env, fake)
    st = env.state()["flows"]["DS_RP_TD_SEED"]
    assert st["status"] != S.COMPLETE, (st, rs[-1].status, rs[-1].error)


# --------------------------------------------------------------------------------------------- C4
def test_ADV9_resume_with_an_unreadable_catalog_does_not_accept_revised_earlier_pages(env):
    """C4: `modified` must not have changed since the attempt started; either mismatch means restart. When
    the catalog read fails at resume the check silently passes, so earlier staged pages INSEE has since
    revised are merged and the flow is certified complete with the stale values."""
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    run_until_chunk(env, fake, "DS_RP_TD_SEED")
    at = env.state()["flows"]["DS_RP_TD_SEED"]["attempt"]
    assert at["chunks"] >= 1 and at["next_page"] > 2
    rev = series(95)
    for i in range(P):                                      # INSEE revises page 1 only
        rev[i]["measures"]["OBS_VALUE_NIVEAU"]["value"] += 1000.0
    fake.flows["DS_RP_TD_SEED"] = rev
    fake.modified["DS_RP_TD_SEED"] = None                    # catalog unreadable at resume
    fake.step = 3.0
    run_until_quiet(env, fake, passes=6)
    tbl = pq.read_table(str(env.store / "DS_RP_TD_SEED.parquet")).to_pylist()
    v0 = {r["series_key"]: r["value"] for r in tbl}
    k0 = "GEO=2026-COM-00000:SEX=_T"
    st = env.state()["flows"]["DS_RP_TD_SEED"]
    assert not (st["status"] == S.COMPLETE and v0[k0] < 1000), (st["status"], v0[k0])


# --------------------------------------------------------------------------------------------- C6 kill point
def test_ADV10_kill_between_chunk_upload_and_cursor_save_reruns_idempotently(env, monkeypatch):
    """Protocol property that SHOULD hold: a kill after the chunk PUT and before the cursor save rewrites
    the same chunk index next pass - no double count, no gap."""
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    real = M.blob.write_bytes_atomic
    n = {"k": 0}

    def put_then_die(path, data):
        real(path, data)
        if str(path).endswith(".chunk"):
            n["k"] += 1
            if n["k"] == 2:
                raise KeyboardInterrupt("killed after the chunk PUT")
    monkeypatch.setattr(M.blob, "write_bytes_atomic", put_then_die)
    with pytest.raises(KeyboardInterrupt):
        run_until_quiet(env, fake, passes=20)
    monkeypatch.setattr(M.blob, "write_bytes_atomic", real)
    run_until_quiet(env, fake, passes=20)
    seen = pq.read_table(str(env.store / "DS_RP_TD_SEED.parquet")).to_pylist()
    assert len(seen) == 95 and len({(r["series_key"], r["obs_date"]) for r in seen}) == 95


def test_ADV11_obs_is_the_store_total_not_the_classified_subset(env, monkeypatch):
    """Result.obs becomes unit_state.obs_count: on the first passes only the classified flows carry
    served.rows, so obs reports a fraction of the store."""
    flows = _control_flows()
    for i in range(6):
        flows[f"DS_Z{i}"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    for i in range(6):
        env.put(f"DS_Z{i}", flows[f"DS_Z{i}"])
    real = M._partition_profile

    def slow(path):
        env.clock.t += 90.0                                  # a big R2 download per flow
        return real(path)
    monkeypatch.setattr(M, "_partition_profile", slow)
    r = env.run(fake)
    stored = sum(env.rows(c) for c in flows)
    assert r.obs in (0, stored), (r.obs, stored)


# --------------------------------------------------------------------------------------------- residue re-trigger
def test_ADV12_a_complete_flow_whose_RESIDUE_is_one_page_is_not_re_pulled_on_reclassification(env):
    """After an origin MERGE the old-spelling seed (exactly one page) stays as residue. Any later identity
    mismatch (an outside write, or T0's identity kind change etag -> size/mtime_ns) re-classifies the flow,
    and the partition rule sees that residue partition as an exact multiple: needs_origin again, a full
    re-pull, the same residue, for ever."""
    import os as _os
    flows = _control_flows()
    flows["DS_RP_TD_OLD"] = series(25, geo="2026")
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    # one page of the OLD spelling, beside 15 rows INSEE still serves. Re-based for AR-330 first-run F1: a
    # store holding ONLY the old spelling is a spelling drift and is HELD (pinned in
    # test_insee_melodi_repull_guards.py); this test is about the residue never re-triggering a re-pull.
    env.put("DS_RP_TD_OLD", series(P, geo="2025", year=2018) + series(25, geo="2026")[:15])
    run_until_quiet(env, fake)
    st = env.state()["flows"]["DS_RP_TD_OLD"]
    assert st["status"] == S.COMPLETE and st["residue"]["rows"] == P
    p = env.store / "DS_RP_TD_OLD.parquet"
    stt = _os.stat(p)
    _os.utime(p, ns=(stt.st_atime_ns, stt.st_mtime_ns + 10**9))  # identity changes, bytes do not
    n0 = len([u for u in fake.calls if "/data/DS_RP_TD_OLD" in u])
    env.run(fake)
    st2 = env.state()["flows"]["DS_RP_TD_OLD"]
    n1 = len([u for u in fake.calls if "/data/DS_RP_TD_OLD" in u])
    assert st2.get("status") not in (S.NEEDS_ORIGIN, S.IN_PROGRESS) and n1 - n0 <= 1, (st2.get("status"), st2.get("why"), n1 - n0)


def test_ADV13_T0_identity_kind_change_does_not_reclassify_every_flow(env, monkeypatch):
    """Pre-T0 identities are {size, etag} (R2 HEAD); post-T0 {size, mtime_ns} (stat). same_identity then
    compares etag with nothing, so EVERY classified flow becomes unclassified at T0."""
    flows = _control_flows()
    flows["DS_OK"] = series(11)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_OK", flows["DS_OK"])
    run_until_quiet(env, fake)
    st = env.state()
    for c, fe in st["flows"].items():                           # what an R2-era state holds
        if (fe.get("served") or {}).get("identity"):
            fe["served"]["identity"] = {"size": fe["served"]["identity"]["size"], "etag": "0" * 32}
    (env.store / S.STATE_FILE).write_text(json.dumps(st, indent=1, sort_keys=True))
    profiled = []
    real = M._partition_profile
    monkeypatch.setattr(M, "_partition_profile", lambda path: profiled.append(path) or real(path))
    env.run(fake)
    assert profiled == [], f"{len(profiled)} flow(s) re-classified (whole-object reads) by the identity-kind change"


def test_ADV14_one_unreadable_stored_object_does_not_freeze_classification_and_the_walk(env):
    """classify() runs before the walk, in a fixed order, with no per-flow except: one stored object that
    cannot be read (corrupt, or a non-404 R2 error after retries) ends EVERY pass at the same flow, so the
    flows after it are never classified and the walk (CPI etc.) never runs again."""
    flows = _control_flows()
    flows["DS_AAA_BROKEN"] = series(5)
    flows["DS_W"] = series(5)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    (env.store / "DS_AAA_BROKEN.parquet").write_bytes(b"not a parquet at all")
    env.put("DS_W", series(4))
    r = env.run(fake)
    assert r is not None and "DS_W" in env.state()["flows"] and \
        env.state()["flows"]["DS_W"].get("status") != S.UNCLASSIFIED
