"""The unit's hard fence must not be LOST when its UnitTimeout is raised inside a GC weakref callback or a __del__.

The defect (found in the review of PR #98, ledger AR-169, 2026-09-29): _unit_deadline arms a ONE-SHOT timer; the
SIGALRM handler (_deliver_alarm) sets UNIT_TIMEOUT_FIRED and raises UnitTimeout. When the signal lands while CPython
runs a weakref callback or a __del__, CPython cannot propagate the raise: it prints "Exception ignored in ..." through
sys.unraisablehook and carries on. The timer never fires again, so the unit runs on with no fence - only code that
polls UNIT_TIMEOUT_FIRED reacts. Seen in a slowed run of tests/test_derive_fence_aware.py
(PytestUnraisableExceptionWarning from WeakSet._remove; F:\\econ_selfhost_probe\\rev_ff_work\\slow06c.log).

Hermetic and platform-independent: the alarm is driven through the REAL _unit_deadline, with signal.setitimer
emulated by a threading.Timer that interrupts the main thread (SIGALRM mapped onto SIGINT for the test), and the
swallow is produced deterministically - the handler body runs inside a weakref callback, which is exactly where the
signal landed in the observed case."""
from __future__ import annotations

import _thread
import gc
import os
import signal
import sys
import threading
import time
import weakref

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import orchestrate  # noqa: E402


@pytest.fixture
def emulated_itimer(monkeypatch):
    """signal.setitimer(ITIMER_REAL, s) -> after s seconds interrupt the main thread with SIGINT, which the test
    maps to SIGALRM. setitimer(ITIMER_REAL, 0) cancels. Records every arming so a test can see a re-arm."""
    armed = []
    state = {"t": None}

    def setitimer(which, seconds, interval=0.0):
        if state["t"] is not None:
            state["t"].cancel()
            state["t"] = None
        armed.append(seconds)
        if seconds > 0:
            t = threading.Timer(seconds, _thread.interrupt_main)
            t.daemon = True
            t.start()
            state["t"] = t
    monkeypatch.setattr(signal, "SIGALRM", signal.SIGINT, raising=False)
    monkeypatch.setattr(signal, "ITIMER_REAL", 0, raising=False)
    monkeypatch.setattr(signal, "setitimer", setitimer, raising=False)
    prev = signal.getsignal(signal.SIGINT)
    yield armed
    if state["t"] is not None:
        state["t"].cancel()
    signal.signal(signal.SIGINT, prev)


class _Obj:
    pass


def _swallowed_alarm(key):
    """What the kernel did in the observed case: run the SIGALRM handler's body inside a weakref callback."""
    o = _Obj()
    keep = weakref.ref(o, lambda _r: orchestrate._deliver_alarm(key, 1))
    del o                                               # the callback runs now; its raise cannot propagate
    gc.collect()
    return keep


def _spin(seconds):
    t = time.monotonic()
    while time.monotonic() - t < seconds:
        time.sleep(0.01)


def test_a_timeout_swallowed_in_a_weakref_callback_is_delivered_again(emulated_itimer, capsys):
    """The unit must NOT carry on: the lost raise is re-delivered in ordinary code within a moment."""
    reached_end = False
    with pytest.raises(orchestrate.UnitTimeout):
        with orchestrate._unit_deadline("zz/_all", 60.0):            # the real timer is far off
            _swallowed_alarm("zz/_all")
            assert orchestrate.UNIT_TIMEOUT_FIRED, "the handler body did run and set the flag"
            _spin(3.0)                                               # the unit's work, carrying on
            reached_end = True
    assert not reached_end, "the unit carried on past its hard limit with no fence"
    assert any(0 < s < 1 for s in emulated_itimer[1:]), f"no prompt re-arm: {emulated_itimer}"
    assert "was swallowed" in capsys.readouterr().out


def test_a_re_delivery_inside_a_deferred_window_is_held_not_raised(emulated_itimer):
    """_DEFER_ALARM (derive's wait slice) still wins: swallowed OUTSIDE the window, the alarm's re-delivery lands
    inside it and is recorded in _ALARM_PENDING for derive to raise just after wait() - never raised inside."""
    with orchestrate._unit_deadline("zz/_all", 60.0):
        _swallowed_alarm("zz/_all")
        orchestrate._DEFER_ALARM = True
        try:
            _spin(1.0)
            assert isinstance(orchestrate._ALARM_PENDING, orchestrate.UnitTimeout)
        finally:
            orchestrate._DEFER_ALARM = False
            orchestrate._ALARM_PENDING = None


def test_negative_control_another_unraisable_is_passed_to_the_previous_hook(emulated_itimer, monkeypatch):
    """Only the fence's own UnitTimeout is taken over; every other swallowed error still reaches the hook it did."""
    seen = []
    monkeypatch.setattr(sys, "unraisablehook", lambda u: seen.append(type(u.exc_value).__name__))

    def boom(_r):
        raise ValueError("an ordinary callback error")
    with orchestrate._unit_deadline("zz/_all", 60.0):
        o = _Obj()
        keep = weakref.ref(o, boom)
        del o
        gc.collect()
        assert keep() is None
    assert seen == ["ValueError"], seen
    assert sys.unraisablehook is not None and sys.unraisablehook.__name__ == "<lambda>", "the hook is restored"


def test_negative_control_no_swallow_means_no_re_arm(emulated_itimer):
    with orchestrate._unit_deadline("zz/_all", 60.0):
        _spin(0.3)
    assert emulated_itimer == [3600.0, 0], emulated_itimer


@pytest.mark.skipif(not hasattr(signal, "setitimer") or not hasattr(signal, "SIGALRM"),
                    reason="a REAL SIGALRM needs POSIX; CI's Linux runner executes this")
def test_a_REAL_sigalrm_handled_inside_a_weakref_callback_is_delivered_again(capsys):
    """No emulation: the real handler _unit_deadline installs, the real signal, the real timer. raise_signal inside
    the callback plus bytecode after it makes CPython run the Python handler INSIDE the callback (the eval breaker
    is checked on the loop's backward jumps) - where its raise cannot propagate. The re-armed REAL timer must then
    deliver it in ordinary code."""
    def cb(_r):
        signal.raise_signal(signal.SIGALRM)
        for _ in range(100_000):
            pass
    reached_end = False
    with pytest.raises(orchestrate.UnitTimeout):
        with orchestrate._unit_deadline("zz/_all", 60.0):
            o = _Obj()
            keep = weakref.ref(o, cb)
            del o
            gc.collect()
            assert orchestrate.UNIT_TIMEOUT_FIRED and keep() is None
            _spin(3.0)
            reached_end = True
    assert not reached_end, "the unit carried on past its hard limit with no fence (real SIGALRM)"
    assert "was swallowed" in capsys.readouterr().out


def test_a_fence_that_fired_but_ended_without_an_exception_is_said_out_loud(emulated_itimer, capsys):
    """If a raise is lost and the unit still finishes before any re-delivery, the log must say the limit fired."""
    with orchestrate._unit_deadline("zz/_all", 60.0):
        orchestrate.UNIT_TIMEOUT_FIRED = True                         # fired; its raise went nowhere
    assert "hard limit FIRED" in capsys.readouterr().out
