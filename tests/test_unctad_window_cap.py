"""unctad_nonplasticsubststradebypartner: the WINDOWED update (PLAN R10.3 step 4, 2026-10-30).

What main did, measured with the real fetcher against a faked API and a real os._exit (receipts in the PR):
it never read the window end, one request could take ~6 x 900 s plus sleeps, a kill after the merge left a
debt nothing recorded, a revision with no new row read no_change, a same-version re-issue re-used old chunks,
and a new key dimension merged silently. Each test below pins one of those, against the REAL module and the
REAL job, with a fake API (tests/_unctad_fake.py) and a PINNED clock (a FakeClock, so the window arithmetic
is exact). Every other unctad module keeps main's update: the first two tests pin that.
"""
import glob
import json
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import _unctad_fake as F  # noqa: E402
import jobs.ingest_unctad_ds as j  # noqa: E402
from updater import orchestrate as _ORCH  # noqa: E402  (the real one; the fixture swaps in a stand-in)
from updater.strategies.fetchers import _unctad as U  # noqa: E402

FETCHERS = os.path.join(ROOT, "updater", "strategies", "fetchers")
MIN = 60.0


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.delenv("AQUEDUCT_UNCTAD_MARGIN_MIN", raising=False)
    monkeypatch.delenv("AQUEDUCT_UNCTAD_ASSEMBLY_RESERVE_MIN", raising=False)
    clock = F.FakeClock()
    api = F.FakeApi(clock)
    F.install(j, U, str(tmp_path), api, clock, setattr_=monkeypatch.setattr)
    orch = type(sys)("updater.orchestrate")
    orch._RUN_DEADLINE_TS = None
    monkeypatch.setitem(sys.modules, "updater.orchestrate", orch)
    return {"root": str(tmp_path), "api": api, "clock": clock, "orch": orch}


def _np():
    from updater.strategies.fetchers import unctad_nonplasticsubststradebypartner as NP
    return NP


def _run(env, up="base", post_s=0.0, ceiling_s=None, status=None):
    env["api"].up, env["api"].post_s, env["api"].status = up, post_s, status
    env["api"].log.clear()
    env["orch"]._RUN_DEADLINE_TS = None if ceiling_s is None else env["clock"].time() + ceiling_s
    t0 = env["clock"].time()
    res = _np().update(None, None)
    return res, env["clock"].time() - t0


def _seed(env):
    """The store a clean base pull builds, and its marks settled (no state.db = outside the orchestrator)."""
    res, _ = _run(env, "base")
    assert res.status == "ok", res.error
    res, _ = _run(env, "base")                   # settles the 'handed' mark left by the seed; nothing changed
    assert res.status == "no_change", res.error
    return res


# ---------------------------------------------------------------- the other 95 modules keep main's update

def test_only_nonplastics_opts_in():
    opted = []
    for p in sorted(glob.glob(os.path.join(FETCHERS, "unctad_*.py"))):
        src = open(p, encoding="utf-8").read()
        assert "from ._unctad import make" in src, p
        if "window=True" in src:
            opted.append(os.path.basename(p)[:-3])
    assert opted == ["unctad_nonplasticsubststradebypartner"]
    from updater.strategies.fetchers import unctad_tradefoodcatbyproc as T
    assert T.update.__qualname__ == "make.<locals>.update"
    assert _np().update.__qualname__ == "_windowed.<locals>.update"


def test_a_module_that_does_not_opt_in_ignores_the_ceiling(env, monkeypatch):
    """The un-opted update pulls the whole release whatever the ceiling says, request for request."""
    from updater.strategies.fetchers import unctad_tradefoodcatbyproc as T
    monkeypatch.setattr(j, "_spill_dir", lambda ds: os.path.join(env["root"], "spill_t", j.source_id_for(ds)))
    env["api"].up = "base"
    r1 = T.update(None, None)
    log1 = list(env["api"].log)
    import shutil
    shutil.rmtree(os.path.join(env["root"], "spill_t"))
    shutil.rmtree(os.path.join(env["root"], "clean_full", "unctad_tradefoodcatbyproc"))
    env["api"].log.clear()
    env["orch"]._RUN_DEADLINE_TS = env["clock"].time() - 3600      # long past
    env["api"].post_s = 30.0
    r2 = T.update(None, None)
    assert r1.status == r2.status == "ok"
    assert env["api"].log == log1 and len(env["api"].leaves()) == 60
    assert r2.changed_keys is None and r1.series_cursors == r2.series_cursors


# ---------------------------------------------------------------- (a)(g)(h) the window

def test_a_pass_stops_before_the_window_end_minus_the_reserve_and_keeps_its_chunks(env):
    _seed(env)
    before = F.store_of(env["root"])
    # every POST 60 s; ceiling 75 min; reserve 60 min, floor 5 min -> 15 min of network time
    res, took = _run(env, "rev", post_s=60.0, ceiling_s=75 * MIN)
    assert res.status == "partial" and res.merged_rows == 0
    assert "deferred by budget, not reached in this pass" in res.error
    assert res.new_vintage == "date-tail"                          # never the release token
    assert took <= 15 * MIN, took                                  # the reserve was never touched
    assert F.store_of(env["root"]) == before                        # nothing merged
    n = len(env["api"].leaves())
    assert n > 0 and len(glob.glob(os.path.join(env["root"], "spill", F.SID, "*.csv"))) >= n


def test_a_chain_of_short_passes_resumes_and_converges(env):
    _seed(env)
    asked, statuses = [], []
    for _ in range(60):
        res, took = _run(env, "rev", post_s=60.0, ceiling_s=75 * MIN)
        statuses.append(res.status)
        assert took <= 15 * MIN
        asked += env["api"].leaves()
        if res.status != "partial":
            break
    assert statuses[-1] == "ok", statuses
    assert len(asked) == len(set(asked)) == 60, "every leaf asked exactly once across the chain"
    assert F.store_of(env["root"]) == F.truth("rev")


def test_the_whole_retry_ladder_ends_before_the_reserve(env):
    _seed(env)
    # every POST answers 503 after 100 s: main's ladder is 6 x (100 s + 20..120 s) per request, then splits
    res, took = _run(env, "rev", post_s=100.0, ceiling_s=75 * MIN, status=503)
    assert res.status == "partial" and "stopped before the window end" in res.error
    assert took <= 15 * MIN, took


def test_no_ceiling_nothing_stops(env):
    _seed(env)
    res, _ = _run(env, "rev", post_s=60.0)
    assert res.status == "ok"


# ---------------------------------------------------------------- (c) a revision is a change

def test_a_revision_without_a_new_row_reads_ok_and_names_exactly_the_revised_keys(env):
    _seed(env)
    res, _ = _run(env, "rev")
    assert res.status == "ok" and "revised stored values" in res.error
    tr = F.truth("rev")
    assert sorted(res.changed_keys) == sorted(tr)                   # every key carries a 2005 obs
    assert all(v == "2005-12-31" for v in res.changed_keys.values() if v is not None)
    assert res.new_vintage == "10010|2026-10-22T00:00:00"
    again, _ = _run(env, "rev")                                     # the same release again: nothing changed
    assert again.status == "no_change" and again.changed_keys == {}


# ---------------------------------------------------------------- (b)(n) the owed mark

def _child(root, up, kill):
    p = subprocess.run([sys.executable, "-B", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                          "_unctad_fake.py"), root, up, kill],
                       capture_output=True, text=True, timeout=300,
                       env=dict(os.environ, PYTHONIOENCODING="utf-8"))
    return p.returncode, p.stdout + p.stderr


@pytest.mark.parametrize("kill", ["after_merge", "after_owed_save:2"])
def test_a_kill_after_the_merge_is_handed_over_by_the_next_pass(env, kill):
    _seed(env)
    rc, out = _child(env["root"], "rev", kill)
    assert rc == 137 and "PLANTED KILL" in out, out[-600:]
    assert F.store_of(env["root"]) == F.truth("rev")                # the merge published
    res, _ = _run(env, "rev")                                       # identical merge, but the debt is owed
    assert res.status == "ok"
    assert sorted(res.changed_keys) == sorted(F.truth("rev"))


def test_an_absent_mark_beside_an_existing_store_is_owed_once(env):
    _seed(env)
    os.remove(os.path.join(env["root"], "clean_full", F.SID, U.OWED_FILE))
    res, _ = _run(env, "base")                                      # the same release: identical merge
    assert res.status == "ok" and len(res.changed_keys) == len(F.truth("base"))


def test_the_pre_merge_mark_fails_closed(env, monkeypatch):
    _seed(env)
    real = U.blob.write_bytes_atomic

    def w(path, data, *a, **kw):
        if str(path).endswith(U.OWED_FILE) and json.loads(data.decode()).get("form") in ("merging", "owed"):
            raise OSError("planted")
        return real(path, data, *a, **kw)
    monkeypatch.setattr(U.blob, "write_bytes_atomic", w)
    before = F.store_of(env["root"])
    res, _ = _run(env, "rev")
    assert res.status == "partial" and "pre-merge owed mark not saved" in res.error
    assert F.store_of(env["root"]) == before


def test_a_mark_that_is_not_a_mark_fails_closed(env):
    _seed(env)
    with open(os.path.join(env["root"], "clean_full", F.SID, U.OWED_FILE), "w") as fh:
        fh.write("[1, 2]")
    with pytest.raises(U.DefinitiveError, match="is present but is not a mark"):
        _run(env, "rev")


# ---------------------------------------------------------------- (k) the spill gate

def _record_path(env):
    return os.path.join(env["root"], "clean_full", F.SID, f"{F.SID}.parquet.release.json")


def _forget_release(env):
    """Remove the release record, as a store main built (no record) has none - so the walk runs."""
    if os.path.exists(_record_path(env)):
        os.remove(_record_path(env))

def _killed_walk(env, up, n_leaves):
    """A pass that persisted some chunks of `up` from an empty spill and then stopped (the clock-limited
    ceiling stops it cleanly before the walk ends)."""
    import shutil
    shutil.rmtree(os.path.join(env["root"], "spill", F.SID), ignore_errors=True)
    _forget_release(env)
    res, _ = _run(env, up, post_s=60.0, ceiling_s=(65 + n_leaves) * MIN)
    assert res.status == "partial"
    return len(env["api"].leaves())


def test_a_same_version_reissue_discards_the_old_chunks(env):
    _seed(env)
    k = _killed_walk(env, "base", 10)                       # chunks of release 10009|2026-08-05 kept
    assert k > 0
    res, _ = _run(env, "reissue")                           # 10009 re-issued: lastUpdated moved
    assert len(env["api"].leaves()) == 60, "every chunk asked again"
    assert res.status == "ok" and F.store_of(env["root"]) == F.truth("reissue")


@pytest.mark.parametrize("old_spills, expect_leaves", [(False, None), (True, 60)])
def test_a_legacy_bare_version_token(env, old_spills, expect_leaves):
    """main wrote the bare version. Accepted only when the release's lastUpdated is earlier than every chunk."""
    _seed(env)
    k = _killed_walk(env, "base", 10)
    sd = os.path.join(env["root"], "spill", F.SID)
    with open(os.path.join(sd, "_token.txt"), "w") as fh:
        fh.write("10009")                                   # main's form
    if old_spills:
        for p in glob.glob(os.path.join(sd, "*.csv")):
            os.utime(p, (1754006400.0, 1754006400.0))       # 2025-08-01: before lastUpdated 2026-08-05
    res, _ = _run(env, "base")
    assert res.status in ("ok", "no_change")
    assert len(env["api"].leaves()) == (expect_leaves if expect_leaves is not None else 60 - k)


# ---------------------------------------------------------------- (l)(j) the key shape

def test_a_new_key_dimension_is_refused_by_name(env):
    _seed(env)
    with pytest.raises(U.DefinitiveError, match="re-grain|re-key"):
        _run(env, "regrain")
    assert {k.count(".") for k in F.store_of(env["root"])} == {4}


def test_a_refusal_with_a_debt_hands_the_debt_over(env):
    _seed(env)
    rc, out = _child(env["root"], "rev", "after_merge")
    assert rc == 137
    res, _ = _run(env, "regrain")
    assert res.status == "partial" and "R22" in res.error
    assert sorted(res.changed_keys) == sorted(F.truth("rev"))


# ---------------------------------------------------------------- U1: the probe-gated residual, pinned

def test_a_probe_matched_pass_neither_calls_update_nor_drains_the_retry_queue(env, monkeypatch):
    """STATED RESIDUAL (review AR-411 U1), pinned through the REAL probe path: run_once -> the real
    bulk_snapshot_if_changed detect_change -> this module's current_vintage -> the (faked) metadata.
    When the probe matches the stored vintage, run_once books no_change and goes on: update() is not
    called, so an owed mark - here a 'handed' one whose CSV phase was cut at its fence - is NOT re-handed,
    and csv_retry_queue is NOT drained. Both wait for UNCTAD's next release. If this test starts failing
    because the orchestrator drains on probe-matched passes, the residual is closed: update the docstring."""
    from updater.state import StateStore

    orchestrate = _ORCH                                                      # the REAL orchestrator
    monkeypatch.delenv("AQUEDUCT_LIVE_ONLY", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("AQUEDUCT_RUN_BUDGET_MIN", raising=False)
    monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)
    monkeypatch.setitem(sys.modules, "updater.orchestrate", orchestrate)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    calls = []
    monkeypatch.setattr(_np(), "update", lambda unit, since: calls.append(unit) or (_ for _ in ()).throw(
        AssertionError("update() must not be called on a probe-matched pass")))
    store = StateStore(os.path.join(env["root"], "state.db"))
    token = U._token(F.meta(F.release("base")))
    hand = "hand-over 2026-10-30T00:00:00Z-000000000000"
    store.upsert_unit(F.SID, "_all", strategy="bulk_snapshot_if_changed", upstream_vintage=token,
                      status="ok", last_success_utc="2026-01-01T00:00:00+00:00",
                      last_attempt_utc="2026-01-01T00:00:00+00:00",
                      last_error=f"{hand}: +1 new rows; csv phase exceeded its 45-min fence")
    store.enqueue_csv_retry(F.SID, [f"{F.SID}:C1.E1"], error="derive budget spent (deferred)")
    out_dir = os.path.join(env["root"], "clean_full", F.SID)
    os.makedirs(out_dir, exist_ok=True)
    mark = {"ds": F.DS, "form": "handed", "utc": "2026-10-30T00:00:00Z", "token": hand}
    with open(os.path.join(out_dir, U.OWED_FILE), "w") as fh:
        json.dump(mark, fh)
    env["api"].up = "base"
    results = orchestrate.run_once(sources=[F.SID], store=store)
    assert (f"{F.SID}/_all", "no_change") in results, results
    assert calls == [], "update() ran: the probe did not gate it"
    assert [k for k, _ in env["api"].log] == ["meta"], "only the release probe touched the API"
    q = store.db.execute("SELECT series_id FROM csv_retry_queue WHERE source_id=?", (F.SID,)).fetchall()
    assert [r[0] for r in q] == [f"{F.SID}:C1.E1"], "the retry queue was drained on a probe-matched pass"
    assert json.load(open(os.path.join(out_dir, U.OWED_FILE))) == mark, "the owed mark was touched"
    note = store.db.execute("SELECT note FROM runs WHERE source_id=?", (F.SID,)).fetchall()
    assert note and note[-1][0].startswith("vintage probe matched stored")


# ---------------------------------------------------------------- U2: a fully spilled walk asks nothing

def test_a_fully_spilled_pass_in_a_tight_window_reaches_the_assembly_and_stamps(env):
    """Review AR-411 U2: with every chunk spilled, the walk used to ask dim_codes once per measure, so a tight
    window stopped it after measure 1's replay and the next pass replayed measure 1 again, for ever. The
    dimension lists are cached under the release token now: the pass asks only the release metadata."""
    _seed(env)
    _forget_release(env)                                    # no record: the walk itself must reach the assembly
    res, took = _run(env, "base", post_s=60.0, ceiling_s=66 * MIN)  # 1 min over floor + reserve
    assert [k for k, _ in env["api"].log] == ["meta"], env["api"].log
    assert res.status == "no_change" and res.new_vintage == "10009|2026-08-05T13:55:36"
    assert "assembly timing" in (res.error or "") or res.changed_keys == {}


def test_a_merged_release_with_a_lost_stamp_is_stamped_without_a_walk(env):
    """Review AR-411 P3r/P3L: the store already holds the release (its record says so) but the vintage was
    never stamped. However short the window - here just over floor + reserve - the pass asks only the
    metadata, walks and assembles nothing, and returns the token; with a debt it hands every key over."""
    _seed(env)
    rec = json.load(open(_record_path(env)))
    assert rec["token"] == "10009|2026-08-05T13:55:36"
    import shutil
    shutil.rmtree(os.path.join(env["root"], "spill", F.SID))            # not even a spill is needed
    res, _ = _run(env, "base", post_s=60.0, ceiling_s=66 * MIN)
    assert [k for k, _ in env["api"].log] == ["meta"]
    assert res.status == "no_change" and res.new_vintage == rec["token"] and res.changed_keys == {}
    os.remove(os.path.join(env["root"], "clean_full", F.SID, U.OWED_FILE))  # a debt: owed once
    res, _ = _run(env, "base", post_s=60.0, ceiling_s=66 * MIN)
    assert [k for k, _ in env["api"].log] == ["meta"]
    assert res.status == "ok" and res.new_vintage == rec["token"]
    assert sorted(res.changed_keys) == sorted(F.truth("base"))
    res, _ = _run(env, "rev")                                            # another release walks as usual
    assert res.status == "ok" and len(env["api"].leaves()) == 60
    assert json.load(open(_record_path(env)))["token"] == "10010|2026-10-22T00:00:00"


def _slow_replay(env, monkeypatch, per_read_s):
    """Every re-read of a spilled chunk costs per_read_s on the pinned clock (the 2.7 GB replay)."""
    import builtins
    real_open = builtins.open
    sd = os.path.join(env["root"], "spill")

    def slow_open(p, *a, **kw):
        if str(p).startswith(sd) and str(p).endswith(".csv") and not (a and "w" in a[0]) \
                and "w" not in kw.get("mode", ""):
            env["clock"].sleep(per_read_s)
        return real_open(p, *a, **kw)
    monkeypatch.setattr(j, "open", slow_open, raising=False)


def test_the_switch_state_from_main_merges_and_stamps_on_pass_one(env, monkeypatch):
    """Review AR-411 U3 (its pk_t5 shape): main did the first ingest, every chunk is spilled under main's bare
    version token, the vintage is NULL and there is no release record. The replay alone outlasts the window
    minus the reserve. Main assembles regardless and stamps; stopping before the assembly would leave every
    pass exactly where the last one was. The pass that POSTed nothing assembles, stamps, and the next pass
    is the cheap already-merged one."""
    _seed(env)
    _forget_release(env)                                    # main writes no record
    sd = os.path.join(env["root"], "spill", F.SID)
    os.remove(os.path.join(sd, j.DIMS_FILE))                # main caches no dimension lists
    with open(os.path.join(sd, "_token.txt"), "w") as fh:
        fh.write("10009")                                   # main's bare version token
    _slow_replay(env, monkeypatch, 30.0)                    # 60 chunks x 30 s = 30 min of replay
    res, took = _run(env, "base", post_s=60.0, ceiling_s=70 * MIN)   # 10 min of window past the reserve
    assert res.status == "no_change", res.error
    assert res.new_vintage == "10009|2026-08-05T13:55:36"
    assert [k for k, _ in env["api"].log if k in ("leaf", "cap")] == [], "nothing was POSTed: a replay"
    assert took >= 30 * MIN                                 # it did replay, past the reserve, as main does
    assert json.load(open(_record_path(env)))["token"] == res.new_vintage
    again, took2 = _run(env, "base", post_s=60.0, ceiling_s=70 * MIN)
    assert again.status == "no_change" and took2 < MIN and [k for k, _ in env["api"].log] == ["meta"]


def test_chunks_missing_late_in_a_long_replay_are_all_fetched_in_pass_one(env, monkeypatch):
    """Review AR-411 B2/B3: three chunks lost at the END of the walk, every re-read costs 30 s (a 30-min
    replay), 10 min of window past the reserve. The walk goes by PRESENCE (a stat per reused chunk), so it
    reaches the missing chunks at once, fetches all three in the network window, and the 30-min re-read
    happens in the parse, after the reserve check - one pass, merged and stamped."""
    _seed(env)
    _forget_release(env)
    sd = os.path.join(env["root"], "spill", F.SID)
    for p in sorted(glob.glob(os.path.join(sd, "*.csv")), key=os.path.getmtime)[-3:]:
        os.remove(p)                                        # the last three chunks the walk wrote
    _slow_replay(env, monkeypatch, 30.0)
    res, took = _run(env, "base", post_s=1.0, ceiling_s=70 * MIN)
    assert len(env["api"].leaves()) == 3
    assert res.status == "no_change" and res.new_vintage == "10009|2026-08-05T13:55:36", res.error
    assert took >= 28 * MIN                                 # the re-read happened, in the parse
    assert F.store_of(env["root"]) == F.truth("base")
    again, _ = _run(env, "base")
    assert [k for k, _ in env["api"].log] == ["meta"]       # the release record: nothing walked


def test_a_spill_unreadable_in_the_parse_is_removed_and_fetched_again(env):
    _seed(env)
    _forget_release(env)
    sd = os.path.join(env["root"], "spill", F.SID)
    bad = sorted(glob.glob(os.path.join(sd, "*.csv")))[7]
    with open(bad, "wb") as fh:
        fh.write(b"\xff\xfe\xfa not utf-8")
    res, _ = _run(env, "base")                              # the same release: its chunks are reused
    assert res.status == "partial" and "unreadable" in res.error and not os.path.exists(bad)
    assert len(env["api"].leaves()) == 0
    res, _ = _run(env, "base")
    assert len(env["api"].leaves()) == 1                    # only that chunk is fetched again
    assert res.status == "no_change" and F.store_of(env["root"]) == F.truth("base")


def test_a_kill_inside_the_store_publish_loses_nothing(env):
    """The merge publishes by one atomic replace and the 'merging' mark is stored before it: a process killed
    between writing the new parquet and replacing the store leaves the OLD store readable, and the next
    pass merges the release and hands its keys over."""
    _seed(env)
    before = F.store_of(env["root"])
    rc, out = _child(env["root"], "rev", "in_publish")
    assert rc == 137 and "PLANTED KILL inside the store publish" in out, out[-600:]
    assert F.store_of(env["root"]) == before                 # the old store, intact
    mark = json.load(open(os.path.join(env["root"], "clean_full", F.SID, U.OWED_FILE)))
    assert mark["form"] == "merging"
    res, _ = _run(env, "rev")
    assert res.status == "ok" and F.store_of(env["root"]) == F.truth("rev")
    assert sorted(res.changed_keys) == sorted(F.truth("rev"))
    left = [f for f in os.listdir(os.path.join(env["root"], "clean_full", F.SID)) if f.endswith(".tmp")]
    assert left == [], left


def test_a_dimension_cache_under_another_token_is_never_used(env):
    _seed(env)
    _forget_release(env)
    sd = os.path.join(env["root"], "spill", F.SID)
    with open(os.path.join(sd, "_token.txt"), "w") as fh:
        fh.write("10008|2026-01-01T00:00:00")                # another release
    with open(os.path.join(sd, j.DIMS_FILE), "w") as fh:
        json.dump({"Years": [1999], "Categories": ["C9"]}, fh)
    res, _ = _run(env, "base")
    assert ("dim", "Years") in env["api"].log, "the lists were asked again"
    assert len(env["api"].leaves()) == 60 and F.store_of(env["root"]) == F.truth("base")
    assert json.load(open(os.path.join(sd, j.DIMS_FILE)))["Years"] == list(range(2000, 2010))
