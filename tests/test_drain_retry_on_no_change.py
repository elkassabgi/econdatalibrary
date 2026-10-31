"""No_change and NOT DUE sources drain their csv_retry_queue AFTER the unit loop, in leftover time.

WHAT WAS WRONG. run_once called _drain_csv_retry_queue only inside the CSV phase of a pass that ran update() and
merged (ok/partial, or a transient_fail with a changed set). A source whose vintage probe matched the stored one
(no_change), or that was NOT DUE, kept its parked ids until its upstream next changed: months for a quarterly or
annual publisher. The resumable fetchers hand every key over on their first pass, and on the desktop most of
those ids are budget-deferred into the queue.

WHAT THESE TESTS HOLD (review AR-413, decisions D1-D6), all through the REAL run_once on real registry entries,
with the strategy replaced by a fake:
  (a) after the loop, a probe-matched source and a NOT DUE source both drain: successes are PUT, cleared and
      recorded for the catalog sync; the no_change record and its run row are exactly what the loop wrote
      (last_error kept byte for byte, dur_s untouched, no extra runs row); a `>>> ... (csv retry drain)` /
      `<<< ... took Ns` pair is printed;
  (b) ONE leftover budget for the pass: the run remainder minus the margin; with no time left nothing drains;
      sources go oldest-enqueued first and a source the budget did not reach is named; _CSV_RETRY_CAP holds;
  (c) attempts: a TRIED refailure stays queued with attempts + 1 and enqueued_utc kept; an id the derive budget
      never reached keeps its counter; least-tried ids go first, so a queue whose head always fails reaches its
      tail on the next pass;
  (d) nothing drains for served_by lane, on a cloud runner, on a dry run, after an undeterminable probe, for a
      source that ran update() this pass (its in-loop drain already ran, once), or when another run holds a
      lease - that skip writes no `locked` row; the drain's own lease lives for its budget plus the margin;
  (e) under AQUEDUCT_BACKEND=r2 a file whose local copy is not R2's object holds its ids back: no PUT, still
      queued, attempts unchanged; the same file equal to R2 is derived;
  (f) an error (any BaseException but KeyboardInterrupt/SystemExit) in one source's drain is printed and the
      next source drains; KeyboardInterrupt leaves;
  (g) tools/record_killed_unit.py records nothing for a kill inside the drain.
Nothing goes to the network: the CSV body comes from a stand-in for core.derive_csv's builder, the PUT goes to a
list, R2 is a fake client. No path outside tmp_path is written.
"""
from __future__ import annotations

import hashlib
import os
import sys
import time
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import config, derive, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.base import Result  # noqa: E402

SRC = "cnb"                      # real registry sources with a fetcher module; none of their code is called
SRC2 = "frankfurter"
TOKEN = "handed:imts:2026-10-30T00:00:00Z|planted last_error that a drain must never rewrite"


class _Blob:
    def __init__(self):
        self.keys = []

    def put_atomic(self, key, data, plain=False):
        self.keys.append(key)

    def get(self, key):                                    # pragma: no cover - not csv_merge_served
        return None


class _Strategy:
    """is_due: `due`. detect_change: `vintage` (None = unchanged), with probe token `token` (None = cannot tell).
    run: `result`."""

    def __init__(self, token="v1", result=None, vintage=None, due=True):
        self.token, self.result, self.vintage, self.due = token, result, vintage, due
        self.ran = 0

    def is_due(self, unit, us, **k):
        return self.due

    def detect_change(self, unit, us):
        self._cur = self.token
        return self.vintage

    def run(self, unit, since=None):
        self.ran += 1
        if self.result is None:
            raise AssertionError("no update() expected")
        return self.result


class _Rig:
    def __init__(self, tmp_path, monkeypatch):
        self.tmp, self.mp = tmp_path, monkeypatch
        self.blob = _Blob()
        self.state = StateStore(str(tmp_path / "data" / "_aqueduct" / "state.db"))
        self.fail = set()               # ids whose derive fails
        self.slow = 0.0                 # seconds per derived id
        self.derived = []

        def body(sid):
            self.derived.append(sid)
            if self.slow:
                time.sleep(self.slow)
            if sid in self.fail:
                raise KeyError(f"no store rows for {sid}")
            return f"series_id,obs_date,value\n{sid.split(':', 1)[1]},2026-01-01,1.0\n".encode()
        monkeypatch.setattr(derive, "_series_csv_bytes", body)

    def seed(self, source, ids, when=None):
        self.state.upsert_unit(source, "_all", strategy="extend_by_date", status="ok", upstream_vintage="v1",
                               last_success_utc="2026-10-01T00:00:00Z", last_attempt_utc="2026-10-01T00:00:00Z",
                               last_error=TOKEN)
        if ids:
            self.state.enqueue_csv_retry(source, ids, "derive budget spent — deferred, not failed")
            if when:
                self.state.db.execute("UPDATE csv_retry_queue SET enqueued_utc=? WHERE source_id=?", (when, source))
                self.state.db.commit()

    def run(self, strategy=None, sources=(SRC,), dry=False):
        self.strategy = strategy or _Strategy()
        self.mp.setattr(orchestrate, "get_strategy", lambda name: self.strategy)
        return orchestrate.run_once(sources=list(sources), store=self.state, blob=self.blob, dry=dry)

    def queue(self, source=SRC):
        return {r["series_id"]: r for r in self.state.csv_retries(source)}

    def runs(self, source=SRC):
        return self.state.db.execute("SELECT status, obs, dur_s, note FROM runs WHERE source_id=? ORDER BY id",
                                     (source,)).fetchall()


@pytest.fixture
def rig(tmp_path, monkeypatch):
    for k in ("AQUEDUCT_BACKEND", "AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_LOCATION",
              "AQUEDUCT_RUN_BUDGET_MIN", "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_DERIVE_BUDGET_MIN",
              "AQUEDUCT_RETRY_DRAIN_MARGIN_MIN", "ECONDL_DATA"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)   # run_once sets it
    data = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    os.makedirs(data / "_aqueduct", exist_ok=True)
    r = _Rig(tmp_path, monkeypatch)
    yield r
    r.state.close()


def _ids(n, source=SRC):
    return [f"{source}:S{i:03d}" for i in range(n)]


def _puts(rig, ids):
    return sorted(k for k in rig.blob.keys if k in {derive.r2_key(s) for s in ids})


# ---- (a) a probe-matched and a NOT DUE source drain after the loop -----------------------------------

def test_a_probe_matched_pass_drains_after_the_loop_and_leaves_its_record_alone(rig, capsys):
    rig.seed(SRC, _ids(3))
    before = rig.state.get_unit(SRC, "_all")
    results = rig.run()
    assert results == [(f"{SRC}/_all", "no_change")], results
    assert _puts(rig, _ids(3)) == sorted(derive.r2_key(s) for s in _ids(3)), "every parked id was derived and PUT"
    assert rig.queue() == {}
    row = rig.state.get_unit(SRC, "_all")
    assert row["status"] == "no_change" and row["upstream_vintage"] == "v1"
    assert row["last_error"] == TOKEN, "the drain never writes last_error (imf_imts_direct settles on it)"
    assert {k: v for k, v in row.items() if not k.endswith("_utc") and k != "status"} == \
           {k: v for k, v in before.items() if not k.endswith("_utc") and k != "status"}
    (status, obs, dur, note), = rig.runs()
    assert (status, obs, dur) == ("no_change", 0, 0.0), "one run row, the loop's own, with no drain time in it"
    assert note.startswith("vintage probe matched stored")
    out = capsys.readouterr().out
    assert f"[orchestrator] >>> {SRC}/_all (csv retry drain)" in out
    assert f"[orchestrator] <<< {SRC}/_all took " in out
    assert "csv retry queue 3 -> attempted 3, cleared 3, still queued 0" in out
    synced = (rig.tmp / "data" / "_aqueduct" / "pending_catalog_sync.txt").read_text().split()
    assert sorted(synced) == _ids(3), "cleared ids are recorded for the D1 catalog sync"
    assert rig.strategy.ran == 0, "no update() on a probe-matched pass"
    assert rig.state.lease_holder(f"{SRC}/_all") is None, "the drain released its lease"


def test_a_not_due_source_drains_too_and_gets_no_state_row_from_it(rig):
    rig.seed(SRC, _ids(2))
    before = rig.state.get_unit(SRC, "_all")
    assert rig.run(strategy=_Strategy(due=False)) == []
    assert rig.queue() == {} and len(_puts(rig, _ids(2))) == 2
    assert rig.state.get_unit(SRC, "_all") == before, "a NOT DUE unit's state is byte-identical after the drain"
    assert rig.runs() == []


def test_an_empty_queue_drains_nothing_and_prints_no_drain_line(rig, capsys):
    rig.seed(SRC, [])
    assert rig.run() == [(f"{SRC}/_all", "no_change")]
    assert rig.derived == [] and rig.blob.keys == []
    assert "csv retry drain" not in capsys.readouterr().out


# ---- (b) one leftover budget ------------------------------------------------------------------------

def test_no_leftover_time_means_no_drain(rig, monkeypatch, capsys):
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "10")              # 10 min left, margin 15: nothing left
    rig.seed(SRC, _ids(2))
    rig.run()
    assert rig.derived == [] and len(rig.queue()) == 2
    assert "no leftover time" in capsys.readouterr().out


def test_the_margin_is_kept_from_the_remainder(rig, monkeypatch):
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "20")
    monkeypatch.setenv("AQUEDUCT_RETRY_DRAIN_MARGIN_MIN", "15")
    seen = {}
    real = orchestrate._drain_csv_retry_queue

    def spy(unit, blob, store, budget_min=None, id_gate=None):
        seen["budget"] = budget_min
        return real(unit, blob, store, budget_min=budget_min, id_gate=id_gate)
    monkeypatch.setattr(orchestrate, "_drain_csv_retry_queue", spy)
    rig.seed(SRC, _ids(1))
    rig.run()
    assert 4.0 < seen["budget"] <= 5.0, seen       # 20-min run, ~0 spent, 15-min margin


def test_sources_drain_oldest_enqueued_first_under_one_pass_budget(rig, monkeypatch, capsys):
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "15.2")            # ~0.2 min (12 s) of leftover time in all
    monkeypatch.setattr(orchestrate, "_DRAIN_FLOOR_MIN", 0.05)
    rig.slow = 0.1
    rig.seed(SRC, _ids(400), when="2026-10-20T00:00:00Z")            # newer
    rig.seed(SRC2, _ids(3, SRC2), when="2026-09-01T00:00:00Z")       # older: goes first
    for s_ in (SRC, SRC2):                                            # a 1 s cost on record: both units admitted
        rig.state.log_run(s_, "_all", "ok", dur_s=1.0)
    rig.run(sources=(SRC, SRC2))
    out = capsys.readouterr().out
    assert out.index(f">>> {SRC2}/_all (csv retry drain)") < out.index(f">>> {SRC}/_all (csv retry drain)")
    assert rig.queue(SRC2) == {}
    assert 0 < len([s for s in rig.derived if s.startswith(SRC + ":")]) < 400, "the pass budget bounds the second"
    assert len(rig.queue(SRC)) > 0


def test_a_source_the_pass_budget_did_not_reach_is_named(rig, monkeypatch, capsys):
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "15.2")
    monkeypatch.setattr(orchestrate, "_DRAIN_FLOOR_MIN", 0.05)
    rig.slow = 0.1
    rig.seed(SRC2, _ids(400, SRC2), when="2026-09-01T00:00:00Z")
    rig.seed(SRC, _ids(2), when="2026-10-20T00:00:00Z")
    for s_ in (SRC, SRC2):
        rig.state.log_run(s_, "_all", "ok", dur_s=1.0)
    rig.run(sources=(SRC, SRC2))
    out = capsys.readouterr().out
    assert "leftover time spent - 1 source(s) not drained this pass: cnb" in out
    assert len(rig.queue(SRC)) == 2


def test_the_per_pass_cap_holds(rig, monkeypatch):
    monkeypatch.setattr(orchestrate, "_CSV_RETRY_CAP", 2)
    rig.seed(SRC, _ids(5))
    rig.run()
    assert len(rig.derived) == 2 and len(rig.queue()) == 3


# ---- (c) attempts -----------------------------------------------------------------------------------

def test_a_tried_refailure_counts_and_keeps_its_age(rig):
    ids = _ids(3)
    rig.seed(SRC, ids)
    rig.fail = {ids[1]}
    q0 = rig.queue()
    rig.run()
    q = rig.queue()
    assert set(q) == {ids[1]}
    assert q[ids[1]]["attempts"] == q0[ids[1]]["attempts"] + 1
    assert q[ids[1]]["enqueued_utc"] == q0[ids[1]]["enqueued_utc"], "the debt keeps its age"
    assert "KeyError" in (q[ids[1]]["last_error"] or "")
    assert rig.state.get_unit(SRC, "_all")["status"] == "no_change", "a refailure never demotes (R359)"
    rig.run()
    assert rig.queue()[ids[1]]["attempts"] == q0[ids[1]]["attempts"] + 2


def test_ids_the_derive_budget_never_reached_keep_their_counter(rig, monkeypatch):
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "15.03")           # ~0.03 min = ~1.8 s of drain
    monkeypatch.setattr(orchestrate, "_DRAIN_FLOOR_MIN", 0.01)
    rig.slow = 0.2
    rig.seed(SRC, _ids(40))
    rig.run()
    q = rig.queue()
    assert 0 < len(rig.derived) < 40, len(rig.derived)
    assert set(q) == set(_ids(40)) - set(rig.derived)
    assert all(r["attempts"] == 1 for r in q.values()), "an id never tried keeps its attempts counter"


def test_a_queue_whose_head_always_fails_reaches_its_tail_next_pass(rig, monkeypatch):
    """AR-413 P10 at small scale: cap 4, 5 queued, the first 4 always fail."""
    monkeypatch.setattr(orchestrate, "_CSV_RETRY_CAP", 4)
    ids = _ids(5)
    rig.seed(SRC, ids)
    rig.fail = set(ids[:4])
    rig.run()
    assert ids[4] not in rig.derived
    rig.derived.clear()
    rig.run()
    assert ids[4] in rig.derived, "the never-tried tail goes first once the head has refailed"
    assert ids[4] not in rig.queue()


# ---- (d) where nothing drains -----------------------------------------------------------------------

def test_a_served_by_lane_source_does_not_drain(rig):
    assert orchestrate._served_by_lane("statcan")
    ids = _ids(2, "statcan")
    rig.seed("statcan", ids)
    rig.run(sources=("statcan",))
    assert rig.derived == [] and set(rig.queue("statcan")) == set(ids)


def test_a_cloud_runner_does_not_drain(rig, monkeypatch, capsys):
    monkeypatch.setenv("AQUEDUCT_RUN_LOCATION", "cloud")
    rig.seed(SRC, _ids(2))
    rig.run()
    assert rig.derived == [] and all(r["attempts"] == 1 for r in rig.queue().values())
    assert "NOT on a cloud runner" in capsys.readouterr().out


def test_a_dry_run_does_not_drain(rig):
    rig.seed(SRC, _ids(2))
    rig.run(dry=True)
    assert rig.derived == [] and len(rig.queue()) == 2


def test_an_undeterminable_probe_does_not_drain(rig):
    rig.seed(SRC, _ids(2))
    rig.run(strategy=_Strategy(token=None))
    assert rig.derived == [] and len(rig.queue()) == 2


def test_a_source_that_ran_update_drains_once_in_the_loop_not_again(rig):
    rig.seed(SRC, _ids(2))
    rig.fail = {_ids(2)[0]}
    rig.run(strategy=_Strategy(vintage="v2", result=Result(status="partial", obs=1, changed_keys={})))
    assert sorted(rig.derived) == _ids(2), "each queued id tried exactly once: the in-loop drain only"
    assert rig.state.get_unit(SRC, "_all")["status"] == "partial"
    assert rig.queue()[_ids(2)[0]]["attempts"] == 2, "the in-loop drain counts a tried refailure too"


def test_a_plain_transient_fail_does_not_drain(rig):
    rig.seed(SRC, _ids(2))
    rig.run(strategy=_Strategy(vintage="v2", result=Result(status="transient_fail", error="503")))
    assert rig.derived == [] and len(rig.queue()) == 2


def test_a_held_lease_skips_the_source_and_writes_no_locked_row(rig, capsys):
    rig.seed(SRC, _ids(2))
    assert rig.state.claim_lease(f"{SRC}/_all", owner="orch-other", ttl_s=3600)
    assert rig.run() == [(f"{SRC}/_all", "no_change")]
    assert rig.derived == [] and len(rig.queue()) == 2
    assert rig.state.lease_holder(f"{SRC}/_all")["owner"] == "orch-other"
    assert [r[0] for r in rig.runs()] == ["no_change"], "no `locked` row for a drain that could not lease"
    assert "csv retry drain skipped for cnb" in capsys.readouterr().out


def test_the_drain_lease_lives_for_its_budget_plus_the_margin(rig, monkeypatch):
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "40")
    ttls = []
    real = rig.state.claim_lease

    def spy(key, owner, ttl_s=3600):
        ttls.append((key, ttl_s))
        return real(key, owner=owner, ttl_s=ttl_s)
    monkeypatch.setattr(rig.state, "claim_lease", spy)
    rig.seed(SRC, _ids(1))
    rig.run()
    drain_ttls = [t for k, t in ttls if k == f"{SRC}/_all"]
    assert drain_ttls and 24 * 60 < drain_ttls[-1] <= 40 * 60 + 2, drain_ttls   # (40-15) + 15 min


# ---- (e) the mirror check under r2 ------------------------------------------------------------------

class _S3:
    def __init__(self, etag, size):
        self.etag, self.size, self.heads = etag, size, []

    def head_object(self, Bucket, Key):
        self.heads.append(Key)
        return {"ETag": f'"{self.etag}"', "ContentLength": self.size, "LastModified": "2026-10-30T00:00:00Z"}


@pytest.fixture
def r2rig(rig, monkeypatch):
    import core.cutover as cutover
    import core.r2_util as r2_util
    from econdl import _resolve
    store = rig.tmp / "data" / "clean_full"
    (store / SRC).mkdir(parents=True)
    f = store / SRC / "cnb.parquet"
    f.write_bytes(b"planted store bytes - the local mirror's copy")
    monkeypatch.setenv("AQUEDUCT_BACKEND", "r2")
    monkeypatch.setenv("ECONDL_DATA", str(store))
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: types.SimpleNamespace(parquet_path=str(f)))
    md5 = hashlib.md5(f.read_bytes()).hexdigest()                    # noqa: S324
    holder = {}

    def client(write=False, pool=None):
        return holder["s3"]
    monkeypatch.setattr(r2_util, "client", client)
    rig.r2 = (f, md5, holder)
    return rig


def test_a_mirror_behind_r2_holds_its_ids_back(r2rig, capsys):
    f, md5, holder = r2rig.r2
    holder["s3"] = _S3("0" * 32, f.stat().st_size)                   # R2 holds other bytes: the mirror is behind
    r2rig.seed(SRC, _ids(2))
    r2rig.run()
    assert r2rig.blob.keys == [] and r2rig.derived == [], "no PUT from a mirror that is not R2's object"
    q = r2rig.queue()
    assert set(q) == set(_ids(2)) and all(r["attempts"] == 1 for r in q.values())
    out = capsys.readouterr().out
    assert "HOLDS BACK" in out and "cnb.parquet" in out
    assert holder["s3"].heads == ["clean_full/cnb/cnb.parquet"], "one HEAD per file, not per id"


def test_a_mirror_equal_to_r2_derives(r2rig):
    f, md5, holder = r2rig.r2
    holder["s3"] = _S3(md5, f.stat().st_size)
    r2rig.seed(SRC, _ids(2))
    r2rig.run()
    assert len(r2rig.blob.keys) == 2 and r2rig.queue() == {}


def test_no_r2_client_holds_everything_back(r2rig, monkeypatch):
    import core.r2_util as r2_util

    def boom(write=False, pool=None):
        raise RuntimeError("R2 read credentials are not set")
    monkeypatch.setattr(r2_util, "client", boom)
    r2rig.seed(SRC, _ids(2))
    r2rig.run()
    assert r2rig.blob.keys == [] and len(r2rig.queue()) == 2


def test_after_t0_or_local_no_mirror_check_is_made():
    import core.cutover as cutover
    orig = cutover.is_cut_over
    try:
        os.environ["AQUEDUCT_BACKEND"] = "local"
        assert orchestrate._r2_mirror_gate(SRC) is None
        os.environ["AQUEDUCT_BACKEND"] = "r2"
        cutover.is_cut_over = lambda: True
        assert orchestrate._r2_mirror_gate(SRC) is None
        cutover.is_cut_over = lambda: False
        assert orchestrate._r2_mirror_gate(SRC) is not None
    finally:
        cutover.is_cut_over = orig
        os.environ.pop("AQUEDUCT_BACKEND", None)


# ---- (f) errors -------------------------------------------------------------------------------------

class _Odd(BaseException):
    """Not an Exception: a drain must still not end the pass over it."""


@pytest.mark.parametrize("exc", [RuntimeError("drain exploded (test)"), _Odd("odd base exception (test)")])
def test_an_error_in_one_drain_is_printed_and_the_next_source_drains(rig, monkeypatch, capsys, exc):
    rig.seed(SRC, _ids(2), when="2026-09-01T00:00:00Z")              # older: drained (and fails) first
    rig.seed(SRC2, _ids(1, SRC2), when="2026-10-01T00:00:00Z")
    real = orchestrate._drain_csv_retry_queue

    def maybe(unit, blob, store, **kw):
        if unit.source_id == SRC:
            raise exc
        return real(unit, blob, store, **kw)
    monkeypatch.setattr(orchestrate, "_drain_csv_retry_queue", maybe)
    results = rig.run(sources=(SRC, SRC2))
    assert sorted(results) == [(f"{SRC}/_all", "no_change"), (f"{SRC2}/_all", "no_change")]
    assert len(rig.queue(SRC)) == 2 and rig.queue(SRC2) == {}
    assert rig.state.lease_holder(f"{SRC}/_all") is None, "the failed drain released its lease"
    assert rig.state.get_unit(SRC, "_all")["last_error"] == TOKEN
    assert "csv retry drain FAILED for cnb" in capsys.readouterr().out


def test_keyboard_interrupt_in_the_drain_leaves(rig, monkeypatch):
    rig.seed(SRC, _ids(1))

    def stop(*a, **k):
        raise KeyboardInterrupt
    monkeypatch.setattr(orchestrate, "_drain_csv_retry_queue", stop)
    with pytest.raises(KeyboardInterrupt):
        rig.run()


# ---- (g) a kill inside the drain --------------------------------------------------------------------

def test_record_killed_unit_records_nothing_for_a_kill_in_the_drain():
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import record_killed_unit as rk
    log = "\n".join([
        "[orchestrator] >>> wid/_all (strategy=overwrite_if_changed, cadence=irregular)",
        "[orchestrator] >>> eia/_all (strategy=extend_by_date, cadence=daily)",
        "[orchestrator] <<< eia/_all took 1,200s",
        "[orchestrator] >>> wid/_all (csv retry drain)",
    ])
    assert rk.parse_killed(log, 3000.0) is None
    assert rk.killed_in_drain(log, 3000.0) == ("wid", "_all")
    closed = log + "\n[orchestrator] <<< wid/_all took 600s (csv retry drain: attempted 5, cleared 5, still queued 0)"
    assert rk.parse_killed(closed, 3000.0) is None and rk.killed_in_drain(closed, 3000.0) is None
    unit_kill = "\n".join(["[orchestrator] >>> wid/_all (csv retry drain)", "[orchestrator] <<< wid/_all took 5s",
                           "[orchestrator] >>> eia/_all (strategy=extend_by_date, cadence=daily)"])
    assert rk.parse_killed(unit_kill, 100.0)[:2] == ("eia", "_all"), "a unit kill is still recorded"


# ---- the call sites (D3) ----------------------------------------------------------------------------

def _module_ast():
    import ast
    import inspect
    return ast.parse(inspect.getsource(orchestrate))


def test_every_call_of_the_drain_in_the_module_and_its_gates():
    """_drain_csv_retry_queue is called from exactly two functions: run_once (once, under `not _csv_fence_tripped`,
    inside the CSV gate) and _drain_leftover_retries (once, inside its per-source loop, after the lease claim).
    _drain_leftover_retries is called once, from run_once, under `not dry`, after the unit loop."""
    import ast
    tree = _module_ast()
    callers = {}
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for n in ast.walk(fn):
                if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "_drain_csv_retry_queue":
                    callers.setdefault(fn.name, []).append(n)
    assert sorted(callers) == ["_drain_leftover_retries", "run_once"], sorted(callers)
    assert all(len(v) == 1 for v in callers.values()), {k: len(v) for k, v in callers.items()}
    fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    run_once = fns["run_once"]
    gates = [n for n in ast.walk(run_once) if isinstance(n, ast.If) and ast.unparse(n.test) == "not _csv_fence_tripped"
             and any(c is callers["run_once"][0] for s in n.body for c in ast.walk(s))]
    assert len(gates) == 1
    left = [n for n in ast.walk(run_once) if isinstance(n, ast.Call)
            and getattr(n.func, "id", None) == "_drain_leftover_retries"]
    assert len(left) == 1
    dry_gate = [n for n in run_once.body if isinstance(n, ast.If) and ast.unparse(n.test) == "not dry"
                and any(c is left[0] for s in n.body for c in ast.walk(s))]
    assert len(dry_gate) == 1, "the leftover drain is a top-level `if not dry:` of run_once, after the loop"
    loop = [i for i, n in enumerate(run_once.body) if isinstance(n, ast.For) and ast.unparse(n.target) == "unit"]
    assert len(loop) == 1 and run_once.body.index(dry_gate[0]) == loop[0] + 1, "right after the unit loop"
    ld = fns["_drain_leftover_retries"]
    src = ast.unparse(ld)
    assert src.index("store.claim_lease") < src.index("_drain_csv_retry_queue(")
    assert "_here() == 'cloud'" in src and "_served_by_lane" in src
