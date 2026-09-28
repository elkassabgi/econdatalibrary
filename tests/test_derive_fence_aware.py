"""derive_and_put must END when the orchestrator's hard fence fires - and say honestly what it did (review R1246).

Before: SIGALRM raised UnitTimeout in the main thread, but (a) the thread pool's `with` exit waited for every running
worker AND ran the queued ids (a 1 s alarm surfaced at 12 s behind one wedged PUT), and (b) on the serial path
_put_with_retry's `except Exception` caught it, slept, retried, and derive returned normally - the handler never ran.

The alarm is EMULATED here (Windows has no SIGALRM): a SIGINT handler that raises the real orchestrate.UnitTimeout,
fired by _thread.interrupt_main() from a timer - the same delivery (main thread, between bytecodes) as SIGALRM.
"""
from __future__ import annotations
import _thread
import os
import signal
import sys
import threading
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import derive, orchestrate  # noqa: E402


class _Blob:
    """put_atomic sleeps in 0.05 s slices (so the main thread can take the alarm) when told to wedge an id."""

    def __init__(self, wedge=(), wedge_s=10.0):
        self.wedge, self.wedge_s, self.put = set(wedge), wedge_s, []
        self.lock = threading.Lock()

    def put_atomic(self, key, body, **kw):
        if any(w in key for w in self.wedge):
            t = time.monotonic()
            while time.monotonic() - t < self.wedge_s:
                time.sleep(0.05)
        with self.lock:
            self.put.append(key)


@pytest.fixture
def alarm(monkeypatch):
    """alarm(seconds): after `seconds`, raise orchestrate.UnitTimeout in the main thread."""
    prev = signal.getsignal(signal.SIGINT)

    def _fire(signum, frame):
        raise orchestrate.UnitTimeout("emulated SIGALRM (test)")
    signal.signal(signal.SIGINT, _fire)
    timers = []

    def arm(seconds):
        t = threading.Timer(seconds, _thread.interrupt_main)
        t.daemon = True
        t.start()
        timers.append(t)
    yield arm
    for t in timers:
        t.cancel()
    signal.signal(signal.SIGINT, prev)


@pytest.fixture(autouse=True)
def _fake_csv(monkeypatch):
    monkeypatch.setattr(derive, "_series_csv_bytes", lambda sid: f"series_id,obs_date,value\n{sid},2024-01-01,1\n".encode())
    monkeypatch.delenv("AQUEDUCT_DERIVE_BUDGET_MIN", raising=False)


def _heartbeats_alive():
    return [t for t in threading.enumerate()
            if getattr(getattr(t, "_target", None), "__name__", "") == "_heartbeat" and t.is_alive()]


def test_the_pool_path_ends_at_the_fence_not_behind_a_wedged_put(monkeypatch, alarm):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")
    blob = _Blob(wedge={"wedged"}, wedge_s=10.0)
    ids = ["zz:wedged"] + [f"zz:q{i}" for i in range(40)]      # more than the 4x-workers queue
    alarm(1.0)
    t0 = time.monotonic()
    with pytest.raises(orchestrate.UnitTimeout) as trip:
        derive.derive_and_put(ids, blob, budget_min=0)
    took = time.monotonic() - t0
    assert took < 4.0, f"the fence surfaced after {took:.1f} s - it waited for the wedged PUT"
    part = trip.value.derive_partial
    assert "zz:wedged" not in part["put_ids"], "a PUT that never finished was booked as derived"
    assert set(part["put_ids"]) <= set(ids)
    assert len(part["put_ids"]) < len(ids), "the queued ids ran after the trip"


def test_the_serial_path_lets_the_fence_through_instead_of_retrying(monkeypatch, alarm):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    blob = _Blob(wedge={"wedged"}, wedge_s=10.0)
    alarm(1.0)
    t0 = time.monotonic()
    with pytest.raises(orchestrate.UnitTimeout) as trip:
        derive.derive_and_put(["zz:wedged", "zz:after"], blob, budget_min=0)
    assert time.monotonic() - t0 < 4.0
    assert trip.value.derive_partial["put_ids"] == [], trip.value.derive_partial
    assert not any("after" in k for k in blob.put), "derive carried on after the fence"


def test_a_wedged_DERIVE_on_the_serial_path_lets_the_fence_through(monkeypatch, alarm):
    """The resolve/derive step has its own `except Exception` (a store-coverage gap is booked failed) - it must not
    book the fence as one more failed id and carry on."""
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")

    def slow_csv(sid):
        if "wedged" in sid:
            t = time.monotonic()
            while time.monotonic() - t < 10:
                time.sleep(0.05)
        return b"series_id,obs_date,value\n"
    monkeypatch.setattr(derive, "_series_csv_bytes", slow_csv)
    blob = _Blob()
    alarm(1.0)
    t0 = time.monotonic()
    with pytest.raises(orchestrate.UnitTimeout):
        derive.derive_and_put(["zz:wedged", "zz:after"], blob, budget_min=0)
    assert time.monotonic() - t0 < 4.0 and blob.put == [], blob.put


def test_after_a_trip_the_queued_ids_do_not_run_in_the_background(monkeypatch, alarm):
    """cancel_futures: without it the ~15 ids already queued in the pool keep running after the phase ended."""
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")

    class _SlowBlob(_Blob):
        def put_atomic(self, key, body, **kw):
            time.sleep(0.3)                      # every PUT takes a while, so the queue is still full at the trip
            super().put_atomic(key, body, **kw)
    blob = _SlowBlob()
    ids = [f"zz:s{i}" for i in range(60)]
    alarm(1.0)
    with pytest.raises(orchestrate.UnitTimeout):
        derive.derive_and_put(ids, blob, budget_min=0)
    at_trip = len(blob.put)
    time.sleep(2.0)
    assert len(blob.put) - at_trip <= 4, f"{len(blob.put) - at_trip} PUTs ran after the trip (the running workers are 4)"


def test_the_heartbeat_stops_with_the_phase(monkeypatch, alarm):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")
    before = len(_heartbeats_alive())
    alarm(0.5)
    with pytest.raises(orchestrate.UnitTimeout):
        derive.derive_and_put(["zz:wedged", "zz:b", "zz:c"], _Blob(wedge={"wedged"}), budget_min=0)
    time.sleep(0.3)
    assert len(_heartbeats_alive()) == before, "derive's heartbeat outlived the tripped phase"


def test_a_put_that_finished_before_the_fence_is_reported_as_put(monkeypatch, alarm):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    blob = _Blob(wedge={"wedged"}, wedge_s=10.0)
    alarm(1.0)
    with pytest.raises(orchestrate.UnitTimeout) as trip:
        derive.derive_and_put(["zz:first", "zz:wedged", "zz:third"], blob, budget_min=0)
    assert trip.value.derive_partial["put_ids"] == ["zz:first"], trip.value.derive_partial


def test_negative_control_without_a_fence_everything_is_derived(monkeypatch):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")
    ids = [f"zz:n{i}" for i in range(12)]
    out = derive.derive_and_put(ids, _Blob(), budget_min=0)
    assert out["put"] == 12 and sorted(out["put_ids"]) == sorted(ids) and out["failed"] == []


def test_the_heartbeat_stops_after_a_normal_run_too(monkeypatch):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")
    before = len(_heartbeats_alive())
    derive.derive_and_put(["zz:a", "zz:b"], _Blob(), budget_min=0)
    time.sleep(0.3)
    assert len(_heartbeats_alive()) == before


# ---- review R1254: the alarm inside wait(), and the alarm in DuckDB's clothes ----------------------------------------

@pytest.mark.skipif(not hasattr(signal, "pthread_sigmask") or not hasattr(signal, "SIGALRM"),
                    reason="POSIX only: no SIGALRM, no fence (CI runs it)")
def test_every_wait_slice_runs_with_sigalrm_blocked(monkeypatch):
    """R1254 item 2: an alarm raised inside wait()'s lock-taking __enter__ left condition locks held and the process
    hung for ever at exit. Each slice must run with SIGALRM blocked, and the mask must be restored after."""
    import concurrent.futures as cf
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")
    real, seen = cf.wait, []

    def spy(*a, **k):
        if threading.current_thread() is threading.main_thread():
            seen.append(signal.SIGALRM in signal.pthread_sigmask(signal.SIG_BLOCK, []))
        return real(*a, **k)
    monkeypatch.setattr(cf, "wait", spy)
    out = derive.derive_and_put([f"zz:w{i}" for i in range(20)], _Blob(), budget_min=0)
    assert out["put"] == 20 and seen and all(seen), seen
    assert signal.SIGALRM not in signal.pthread_sigmask(signal.SIG_BLOCK, []), "the mask was not restored"


def test_a_wait_slice_off_posix_or_off_the_main_thread_is_a_plain_wait(monkeypatch):
    """Where there is no SIGALRM (Windows) the slice is a plain 1 s wait - and it still returns what finished."""
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(1) as ex:
        f = ex.submit(lambda: 7)
        done, _ = derive._wait_slice({f: "x"}, 5.0)
    assert f in done and f.result() == 7


def _fire(monkeypatch):
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", True)


@pytest.mark.parametrize("where", ["series", "flow_rows", "flow_stream"])
def test_an_error_after_the_alarm_fired_is_the_fence(monkeypatch, where):
    """R1254 item 6: DuckDB takes the signal and raises RuntimeError('Query interrupted'); `except fence` missed it
    and the id was booked failed while the phase carried on. Once UNIT_TIMEOUT_FIRED is set, it is the fence."""
    import core.derive_csv as dc
    import econdl._resolve as rs
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)

    def interrupted(*a, **k):
        _fire(monkeypatch)
        raise RuntimeError("Query interrupted")
    monkeypatch.setattr(rs, "resolve", lambda sid: sid)
    monkeypatch.setattr(dc, "resolved_paths", interrupted if where == "flow_rows" else (lambda r: []))
    monkeypatch.setattr(dc, "_series_csv_to_file_sorted", interrupted)
    if where == "series":
        monkeypatch.setattr(derive, "_series_csv_bytes", interrupted)
    blob = _Blob()
    with pytest.raises(orchestrate.UnitTimeout, match="RuntimeError") as trip:
        derive.derive_and_put(["zz:x", "zz:after"], blob, budget_min=0, flow_grain=(where != "series"))
    assert trip.value.derive_partial["put_ids"] == [] and trip.value.derive_partial["failed"] == [], \
        trip.value.derive_partial
    assert blob.put == [], "derive carried on after the fence"


@pytest.mark.parametrize("flow", [False, True])
def test_negative_control_the_same_error_without_the_alarm_is_one_failed_id(monkeypatch, flow):
    import core.derive_csv as dc
    import econdl._resolve as rs
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)

    def broken(*a, **k):
        raise RuntimeError("Query interrupted")
    monkeypatch.setattr(rs, "resolve", lambda sid: sid)
    monkeypatch.setattr(dc, "resolved_paths", lambda r: [])
    monkeypatch.setattr(dc, "_series_csv_to_file_sorted", broken)
    monkeypatch.setattr(derive, "_series_csv_bytes",
                        broken if not flow else (lambda sid: b"series_id,obs_date,value\n"))
    out = derive.derive_and_put(["zz:x"], _Blob(), budget_min=0, flow_grain=flow)
    assert out["failed"] == ["zz:x"] and out["put"] == 0, out
