"""AR-334 adversarial tests (the parallel reviewer's, ported into the branch; ADV2 restated for the new rule).

Harness: tests/test_insee_melodi_repull.py (FakeMelodi, env, series(), obs()); PAGE_SIZE 10, CHUNK_PAGES 3.
"""
from __future__ import annotations

import json

import pytest

from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S
from test_insee_melodi_repull import (P, FakeMelodi, env, obs, series, _control_flows,  # noqa: F401
                                      _seed_store, run_until_quiet)


def _years(n, years, extra=None, geo="2026"):
    out = []
    for y in years:
        for i in range(n):
            d = {"GEO": f"{geo}-COM-{i:05d}", "SEX": "_T"}
            if extra:
                d.update(extra)
            out.append(obs(d, y, float(i) + y / 10000))
    return out


def _edit_state(env, fn):
    p = env.store / S.STATE_FILE
    data = json.loads(p.read_text(encoding="utf-8"))
    fn(data)
    p.write_text(json.dumps(data, indent=1, sort_keys=True), encoding="utf-8")


# ------------------------------------------------------------------------------------------ F1
def test_ADV1_multi_vintage_truncation_repair_is_not_held(env):
    """DS_SIDE_CREA_DEP_REG_NAT / DS_RP_MENAGES_PRINC shape (mirror, vintage_keys.log): the store holds the
    OLD GEO vintage in full plus ONE page of the current vintage; INSEE serves only the current vintage. The
    origin pull carries every current-vintage stored key again (a repair), but the coverage denominator counts
    the old-vintage keys too: 10 of 40 = 0.25 < 0.5, and NK/S = 15/25 = 0.6 > 0.5 -> HOLD. The merge adds no
    key-spelling partition the store lacks (2026 is already stored)."""
    flows = _control_flows()
    flows["DS_SIDE_X"] = series(25, geo="2026", year=2023)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_SIDE_X", series(30, geo="2025", year=2022) + flows["DS_SIDE_X"][:P])
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_SIDE_X"]
    assert not fe.get("hold"), fe.get("hold_detail")
    assert env.rows("DS_SIDE_X") == 30 + 25


def test_ADV2_unmeasurable_coverage_fails_closed():
    """expected == 0 (no stored key the pull should carry) with every staged row on a new key: the rule
    returns False - a merge - instead of treating the inability to measure as a hold.
    RESTATED by the builder for the respelled-rows rule that replaced the coverage rule (AR-334 condition 3):
    the hold no longer divides by `expected`, so an empty denominator cannot open it; every staged row
    re-spelling the store holds the merge."""
    split = {"S": 100, "NK": 100, "stored_rows": 500, "expected": 0, "covered": 0, "respelled": 100,
             "respelled_duplicates": 100, "respelled_split": 0}
    assert M._spelling_hold(split) is True


def test_ADV3_tail_drift_on_a_small_tail_year_denominator_passes(env):
    """Two flash series carry the newest year; INSEE then respells the other 28 (a constant REF=X dimension
    added, the DD_CNA GEO=2025-FRANCE-F pattern) when it publishes their newest year. Tail from that year:
    NK/S = 28/30, but expected = the 2 flash keys and both are carried -> coverage 1.0 -> merged. The store
    then splits 28 series across two spellings."""
    old = _years(30, [2022]) + _years(2, [2023])
    flows = _control_flows()
    flows["DS_TAILX"] = list(old)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_TAILX", old)
    run_until_quiet(env, fake)
    assert env.state()["flows"]["DS_TAILX"]["status"] in (S.LEGACY, S.COMPLETE)
    respelled = [o for o in _years(30, [2023], extra={"REF": "X"})
                 if int(o["dimensions"]["GEO"][-5:]) >= 2]
    fake.flows["DS_TAILX"] = old + respelled
    fake.modified["DS_TAILX"] = "2026-10-01T00:00:00"
    for _ in range(3):
        env.run(fake)
    fe = env.state()["flows"]["DS_TAILX"]
    assert fe.get("hold") or env.rows("DS_TAILX") == len(old), (env.rows("DS_TAILX"), fe.get("why"))


def test_ADV3b_partial_drift_under_half_passes(env):
    """45% of the series respelled (constant added dimension) in an origin re-pull: NK/S = 0.45 is not > 0.5,
    so it merges and the store doubles those series."""
    base = series(20, year=2023)
    flows = _control_flows()
    flows["DS_PART"] = base
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_PART", base)
    run_until_quiet(env, fake)
    resp = [obs({**o["dimensions"], "REF": "X"}, 2023, o["measures"]["OBS_VALUE_NIVEAU"]["value"])
            for o in base[:9]]
    for o in resp:
        o["dimensions"]["TIME_PERIOD"] = "2023"
    fake.flows["DS_PART"] = resp + base[9:]
    (env.store / S.FORCE_FILE).write_text(json.dumps({"flows": ["DS_PART"]}), encoding="utf-8")
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_PART"]
    assert fe.get("hold") or env.rows("DS_PART") == 20, (env.rows("DS_PART"), fe.get("why"))


def _drift_and_hold(env):
    base = series(25, year=2023)
    flows = _control_flows()
    flows["DS_DRIFT"] = base
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_DRIFT", base)
    run_until_quiet(env, fake)
    new = [obs({**{k: v for k, v in o["dimensions"].items() if k != "TIME_PERIOD"}, "REF": "X"}, 2023,
               o["measures"]["OBS_VALUE_NIVEAU"]["value"]) for o in base]
    fake.flows["DS_DRIFT"] = new
    fake.modified["DS_DRIFT"] = "2026-10-01T00:00:00"
    for _ in range(3):
        env.run(fake)
        if env.state()["flows"]["DS_DRIFT"].get("hold"):
            break
    fe = env.state()["flows"]["DS_DRIFT"]
    assert fe.get("hold") == "spelling", fe
    assert env.rows("DS_DRIFT") == 25 and env.chunks()
    return fake, new


def test_ADV4_a_held_flow_is_re_measured_after_the_owner_supersedes_the_store(env):
    """The exit the hold message points to: the owner supersedes the old spelling in the served file. The
    held flow must then merge (nothing new) and leave the hold; today it is skipped by the repair slot and by
    check_identity (merge_pending), so it stays held - never walked, never tailed - forever."""
    fake, new = _drift_and_hold(env)
    env.put("DS_DRIFT", new)                                          # the owner's supersede
    for _ in range(4):
        env.run(fake)
    fe = env.state()["flows"]["DS_DRIFT"]
    assert not fe.get("hold") and fe["status"] in (S.LEGACY, S.COMPLETE), (fe["status"], fe.get("hold"))
    assert env.chunks() == []


def test_ADV4b_force_on_a_held_flow_releases_it_or_says_why_not(env):
    """A single-year flow's tail IS an origin pull (start None), so its held attempt has mode origin, and
    apply_force keeps a merge_pending ORIGIN attempt as it is - silently (the label exists only for a
    merge_refusals count). The operator's only lever does nothing and says nothing."""
    fake, _ = _drift_and_hold(env)
    (env.store / S.FORCE_FILE).write_text(json.dumps({"flows": ["DS_DRIFT"]}), encoding="utf-8")
    r = env.run(fake)
    fe = env.state()["flows"]["DS_DRIFT"]
    released = not fe.get("hold")
    explained = "DS_DRIFT" in (r.error or "") and "force" in (r.error or "").lower() and "hold" in (r.error or "").lower()
    assert released or explained, (fe["status"], fe.get("hold"), (r.error or "")[:300])


# ------------------------------------------------------------------------------------------ F3
def test_ADV6_queue_priority_holds_for_flows_classified_before_this_change():
    """Production _flows.json: 73 needs_origin flows classified by the PR #160 code carry no `prio`. The sort
    defaults them all to 5, so the queue stays alphabetical - F3(b) is not delivered to the live queue."""
    data = S.FlowState.empty()
    data["flows"] = {
        "DS_A_PROBE": {"status": S.NEEDS_ORIGIN, "why": "probe page 3 returned 10000 obs", "repair": True},
        "DS_Z_CI": {"status": S.NEEDS_ORIGIN, "why": "a CI-logged delta was an exact multiple of 10,000",
                    "repair": True},
    }
    st = S.FlowState(data, None, [])
    pa_ = M._Pass(None, None, st, set())
    assert pa_._next_repair() == "DS_Z_CI"


# ------------------------------------------------------------------------------------------ F6
def test_ADV7_the_one_denominator_adds_up():
    data = S.FlowState.empty()
    data["flows"] = {
        "T1": {"status": S.COMPLETE, "repair": True}, "T2": {"status": S.COMPLETE, "repair": True},
        "T3": {"status": S.NEEDS_ORIGIN, "repair": True},
        "W1": {"status": S.MERGE_PENDING, "hold": "spelling", "attempt": {"id": "a"}},   # a held walk tail
        "T4": {"status": S.BROKEN, "repair": True},
    }
    st = S.FlowState(data, None, [])
    pa_ = M._Pass(None, None, st, set())
    note = pa_.note()
    import re
    m = re.search(r"repair (\d+)/(\d+) complete.*?of the \d+: (\d+) open, (\d+) held", note)
    done, n, open_, held = map(int, m.groups())
    # every one of the N flows is either complete, open, held or named terminal (broken/unstable/withdrawn)
    terminal = 1
    assert done + open_ + held + terminal == n, note[:200]


# ------------------------------------------------------------------------------------------ F4
class SlowFake(FakeMelodi):
    """Per-flow extra seconds per data page, and a one-shot clock jump on (flow, page)."""

    def __init__(self, *a, slow=None, **k):
        super().__init__(*a, **k)
        self.slow = slow or {}
        self.jump = None

    def get(self, url, headers=None, timeout=None):
        r = super().get(url, headers, timeout)
        for fl, s in self.slow.items():
            if f"/data/{fl}?" in url:
                self.clock.t += s
        if self.jump and f"/data/{self.jump[0]}?" in url and f"page={self.jump[1]}" in url:
            self.clock.t += self.jump[2]
            self.jump = None
        return r


def test_ADV5_a_parked_walk_tail_is_not_starved_behind_the_origin_backlog(env):
    """F4 parks a walk tail the slot end cut as needs_refresh. needs_refresh is worked only after EVERY
    needs_origin flow, and the walk never touches it (not WALKABLE): a 3-page tail of a healthy flow waits for
    the whole backlog (~195 passes in production)."""
    flows = _control_flows()
    flows["DS_W"] = _years(25, [2022, 2023])[1:]                      # 49 rows, tail = 25 rows of 2023 = 3 pages
    flows["DS_ZBIG"] = series(2500)                                   # 250 pages, slow: a ~8-pass backlog
    fake = SlowFake(env.clock, flows, slow={"DS_ZBIG": 60.0})
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    env.put("DS_ZBIG", flows["DS_ZBIG"][:P])
    env.run(fake)
    assert env.state()["flows"]["DS_W"]["status"] in (S.LEGACY, S.COMPLETE)
    # INSEE revises DS_W's 2023 values; the next pass's walk is cut after its tail's page 2
    fake.flows["DS_W"] = [o if o["dimensions"]["TIME_PERIOD"] != "2023" else
                          obs({k: v for k, v in o["dimensions"].items() if k != "TIME_PERIOD"}, 2023, 99.0)
                          for o in flows["DS_W"]]
    fake.modified["DS_W"] = "2026-10-01T00:00:00"
    fake.jump = ("DS_W", 2, 15 * 60)
    env.run(fake)
    fe = env.state()["flows"]["DS_W"]
    assert fe["status"] == S.NEEDS_REFRESH, fe["status"]              # F4 parked it
    for _ in range(2):
        env.run(fake)
    fe = env.state()["flows"]["DS_W"]
    big = env.state()["flows"]["DS_ZBIG"]["status"]
    assert fe["status"] in (S.LEGACY, S.COMPLETE), (fe["status"], "backlog:", big)


def test_ADV8_the_plus_pages_head_counts_every_repair_page_of_the_pass(env):
    """F6 puts '+p pages' first. repair_pages is OVERWRITTEN by each _progress call (walk bails included) and a
    flow that completes never calls _progress: a pass that completes DS_RP_TD_SEED (3 pages) and then stages k
    pages of DS_ZBIG reports +k, not +(3+k)."""
    import re
    flows = _control_flows()
    flows["DS_ZBIG"] = series(2500)
    fake = SlowFake(env.clock, flows, slow={"DS_ZBIG": 60.0})
    _seed_store(env, fake)
    env.put("DS_ZBIG", flows["DS_ZBIG"][:P])
    r = env.run(fake)
    st = env.state()["flows"]
    assert st["DS_RP_TD_SEED"]["status"] == S.COMPLETE                 # 3 origin pages this pass
    k = int(st["DS_ZBIG"]["attempt"]["next_page"]) - 1
    m = re.match(r"repair \d+/\d+ complete, \+(\d+)p", r.error)
    # builder (porting): DS_CTRL, the fixture's 30-row control flow, is an exact multiple too and is pulled from
    # origin in the same repair slot (3 pages), so the pass stages 3 + 3 + k repair pages
    assert st["DS_CTRL"]["status"] == S.COMPLETE and st["DS_CTRL"].get("basis") == "origin"
    assert m and int(m.group(1)) == 3 + 3 + k, (r.error[:80], k)
