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
import operator
import os
import signal
import sys
import threading
import time
import types
import weakref

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import orchestrate  # noqa: E402


class _Armed(list):
    """Every arming, in order. With `manual = True` a RE-ARM (0 < seconds < 1) starts no timer: the test fires it
    with fire() at the exact point it wants the alarm to land - no wall clock decides (2026-10-02)."""
    manual = False

    @staticmethod
    def fire():
        _thread.interrupt_main()


@pytest.fixture
def emulated_itimer(monkeypatch):
    """signal.setitimer(ITIMER_REAL, s) -> after s seconds interrupt the main thread with SIGINT, which the test
    maps to SIGALRM. setitimer(ITIMER_REAL, 0) cancels. Records every arming so a test can see a re-arm."""
    armed = _Armed()
    state = {"t": None}

    def setitimer(which, seconds, interval=0.0):
        if state["t"] is not None:
            state["t"].cancel()
            state["t"] = None
        armed.append(seconds)
        if seconds > 0 and not (armed.manual and seconds < 1):
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
    del o                                               # the callback runs NOW (refcount); its raise cannot propagate
    assert keep() is None                               # no gc.collect: on a full-suite heap it outlasted the re-arm
    return keep


def _spin(seconds):
    t = time.monotonic()
    while time.monotonic() - t < seconds:
        time.sleep(0.01)


def _wait_until(cond, seconds):
    """Ordinary code that runs until cond() is true. The limit is only so that a defect ends the test."""
    t = time.monotonic()
    while not cond():
        if time.monotonic() - t > seconds:
            return False
        time.sleep(0.01)
    return True


class _SlowOut:
    """A stdout whose write takes `seconds` of ordinary Python code - what a captured stdout on a busy disk did."""
    def __init__(self, seconds):
        self.seconds, self.text = seconds, []

    def write(self, s):
        _spin(self.seconds)
        self.text.append(s)
        return len(s)

    def flush(self):
        pass


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
    inside it and is recorded in _ALARM_PENDING for derive to raise just after wait() - never raised inside.

    The re-delivery is fired BY THE TEST, after the window is open. It was left to the 0.05 s timer and a 1.0 s
    spin; on 2026-10-02, on a busy machine, the alarm landed before the window - inside the hook, in its print or
    in _rearm's own `except Exception` after a stall (a Timer starved for 1 s fits the same evidence; AR-205) - and
    the test failed for a reason that was not its subject."""
    emulated_itimer.manual = True
    with orchestrate._unit_deadline("zz/_all", 60.0):
        _swallowed_alarm("zz/_all")
        assert [s for s in emulated_itimer[1:] if 0 < s < 1], f"the swallow did not re-arm: {emulated_itimer}"
        orchestrate._DEFER_ALARM = True
        try:
            emulated_itimer.fire()                                    # a UnitTimeout raised here fails the test
            assert _wait_until(lambda: orchestrate._ALARM_PENDING is not None, 30.0), "the alarm never ran"
            assert isinstance(orchestrate._ALARM_PENDING, orchestrate.UnitTimeout)
        finally:
            orchestrate._DEFER_ALARM = False
            orchestrate._ALARM_PENDING = None


def test_control_the_same_fire_outside_a_deferred_window_raises(emulated_itimer):
    """The control of the test above: the same manual fire with NO window open is raised, not recorded."""
    emulated_itimer.manual = True
    with pytest.raises(orchestrate.UnitTimeout):
        with orchestrate._unit_deadline("zz/_all", 60.0):
            _swallowed_alarm("zz/_all")
            emulated_itimer.fire()
            _wait_until(lambda: False, 30.0)
    assert orchestrate._ALARM_PENDING is None


def test_a_re_delivery_that_lands_inside_the_hooks_own_print_is_not_lost(emulated_itimer, monkeypatch):
    """2026-10-02. The hook re-arms the timer (R1301: before it prints) and then prints. A print slower than
    _REFIRE_S lets the re-armed alarm fire INSIDE the print. Its raise was caught by the hook's own
    `except Exception: pass`: no unraisable hook, no second re-arm - the unit ran on with no fence. Seen as a
    one-off failure of the deferred-window test while pytest's captured stdout sat on a busy disk; reproduced on
    the unchanged code with this slow stdout (armed [3600.0, 0.05, 0], nothing pending, the unit reached its end)."""
    monkeypatch.setattr(orchestrate, "_TIMEOUT_WARNED", True)         # __enter__ prints nothing through the slow stdout
    slow = _SlowOut(0.3)                                              # two writes: 0.6 s, twelve times _REFIRE_S
    reached_end = False
    with pytest.raises(orchestrate.UnitTimeout):
        with orchestrate._unit_deadline("zz/_all", 60.0):
            monkeypatch.setattr(sys, "stdout", slow)
            _swallowed_alarm("zz/_all")
            _spin(3.0)                                                # the unit's work, carrying on
            reached_end = True
    assert not reached_end, "the re-delivery landed in the hook's print and was lost: no fence"
    rearms = [s for s in emulated_itimer[1:] if 0 < s < 1]
    assert len(rearms) >= 2, f"no delivery landed inside the hook, so this run proved nothing: {emulated_itimer}"
    assert "was swallowed" in "".join(slow.text), "the hook's print was cut short by the alarm"


def test_a_re_delivery_that_lands_inside_the_re_arm_itself_is_not_lost(emulated_itimer, monkeypatch):
    """AR-205: the main thread stalls right after setitimer returns, still inside _rearm's own
    `try: ... except Exception: pass` - the same loss and armed list [3600.0, 0.05, 0], no slow print."""
    monkeypatch.setattr(orchestrate, "_TIMEOUT_WARNED", True)
    emulated = signal.setitimer
    stalled = []

    def stalling(which, seconds, interval=0.0):
        emulated(which, seconds, interval)
        if 0 < seconds < 1 and not stalled:
            stalled.append(seconds)
            _spin(0.3)                                    # the re-armed alarm lands here
    monkeypatch.setattr(signal, "setitimer", stalling)
    reached_end = False
    with pytest.raises(orchestrate.UnitTimeout):
        with orchestrate._unit_deadline("zz/_all", 60.0):
            _swallowed_alarm("zz/_all")
            _spin(3.0)
            reached_end = True
    assert not reached_end
    assert len([s for s in emulated_itimer[1:] if 0 < s < 1]) >= 2, emulated_itimer


class _Trip:
    """x.trip calls _thread.interrupt_main() entirely in C (property -> methodcaller -> staticmethod). LOAD_ATTR has
    no eval-breaker check, so the trip is still pending at the RESUME of the next Python function called."""
    interrupt_main = staticmethod(_thread.interrupt_main)
    trip = property(operator.methodcaller("interrupt_main"))


def test_a_delivery_at_the_hooks_first_instruction_is_held(emulated_itimer, monkeypatch):
    """AR-205 W1: a delivery handled at the hook's RESUME, before `_HOOK_BUSY = True`, must be held and re-armed -
    raised there, CPython drops it ("Exception ignored in sys.unraisablehook") with no re-arm. Only the frame check
    (_in_fence_hook) covers this point; this test fails without it, and without its walk up the stack."""
    monkeypatch.setattr(orchestrate, "_TIMEOUT_WARNED", True)
    emulated_itimer.manual = True
    u = types.SimpleNamespace(exc_value=orchestrate.UnitTimeout("poller in a callback"), object=None)
    x = _Trip()
    with orchestrate._unit_deadline("zz/_all", 60.0) as d:
        hook, n0 = d._unraisable, len(emulated_itimer)
        x.trip; hook(u)                                   # noqa: E702 - no CALL between the trip and the hook
        assert len([s for s in emulated_itimer[n0:] if 0 < s < 1]) >= 2, emulated_itimer
        orchestrate.UNIT_TIMEOUT_FIRED = False


def test_the_hold_inside_the_hook_ends_with_the_hook(emulated_itimer, monkeypatch):
    """_HOOK_BUSY must be False again after the hook - also when its print raises - or every later alarm is held."""
    class _Broken:
        def write(self, s):
            raise OSError("stdout is gone")

        def flush(self):
            raise OSError("stdout is gone")
    class _Interrupted:
        def write(self, s):
            raise KeyboardInterrupt          # leaves the hook; CPython drops it ("Exception ignored in sys.unraisablehook")

        def flush(self):
            pass
    monkeypatch.setattr(orchestrate, "_TIMEOUT_WARNED", True)
    emulated_itimer.manual = True
    for out in (_SlowOut(0.0), _Broken(), _Interrupted()):
        with orchestrate._unit_deadline("zz/_all", 60.0):
            monkeypatch.setattr(sys, "stdout", out)
            _swallowed_alarm("zz/_all")
            monkeypatch.setattr(sys, "stdout", sys.__stdout__)
            assert orchestrate._HOOK_BUSY is False, type(out).__name__
            orchestrate.UNIT_TIMEOUT_FIRED = False                    # this test is not about the exit message


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
    assert "hard limit FIRED but the unit ended normally" in capsys.readouterr().out


def test_a_fence_that_fired_but_ended_with_ANOTHER_exception_is_said_out_loud(emulated_itimer, capsys):
    """Review R1301: a __del__'s own `except Exception` ate the UnitTimeout, then a RuntimeError ended the unit -
    booked UNEXPECTED, the flag cleared, and nothing said the limit had fired."""
    with pytest.raises(RuntimeError):
        with orchestrate._unit_deadline("zz/_all", 60.0):
            orchestrate.UNIT_TIMEOUT_FIRED = True
            raise RuntimeError("something else")
    assert "hard limit FIRED but the unit ended with RuntimeError" in capsys.readouterr().out


def test_a_re_delivery_never_cuts_short_the_cleanup_of_a_UnitTimeout_already_in_flight(emulated_itimer,
                                                                                        monkeypatch):
    """Review R1301 (d): the swallow re-arms the timer, a flag poller (dst._fence_check's shape) raises UnitTimeout at
    once, and the unit's cleanup runs - the re-armed alarm lands INSIDE that cleanup. It must be held, not raised:
    a second UnitTimeout would cut a _giant checkpoint or a rotation save short. The re-arm is stretched to 0.3 s so
    it lands in the 1.0 s cleanup and never before the poller's raise (a slow gc.collect on a big heap took >50 ms)."""
    monkeypatch.setattr(orchestrate, "_REFIRE_S", 0.3)
    saved = []
    with pytest.raises(orchestrate.UnitTimeout, match="poller"):
        with orchestrate._unit_deadline("zz/_all", 60.0):
            _swallowed_alarm("zz/_all")
            try:
                raise orchestrate.UnitTimeout("poller: the flag says the fence fired")
            finally:
                _spin(1.0)                                            # the re-armed alarm fires in here
                saved.append("checkpoint")
    assert saved == ["checkpoint"], "the cleanup was interrupted by a second UnitTimeout"
    assert any(0 < s < 1 for s in emulated_itimer[1:]), "the scenario did re-arm (else it proves nothing)"


def test_a_held_re_delivery_whose_UnitTimeout_is_then_swallowed_is_delivered_later(emulated_itimer, monkeypatch):
    """Review AR-174 r2: swallowed in a callback, then a poller's UnitTimeout is caught by an ordinary
    `except Exception` whose body is still running when the re-arm fires - the delivery is HELD (a UnitTimeout is in
    flight). Holding must re-arm, or once that except swallows the poller's exception the unit runs on unfenced."""
    monkeypatch.setattr(orchestrate, "_REFIRE_S", 0.2)
    reached_end = False
    with pytest.raises(orchestrate.UnitTimeout):
        with orchestrate._unit_deadline("zz/_all", 60.0):
            _swallowed_alarm("zz/_all")
            try:
                raise orchestrate.UnitTimeout("poller")
            except Exception:                                         # noqa: BLE001 - the swallow under test
                _spin(0.5)                                            # the re-arm fires here: held
            _spin(2.0)                                                # the unit carries on
            reached_end = True
    assert not reached_end, "held once, then lost: the unit ran on with no fence"


def test_a_print_that_fails_does_not_lose_the_re_delivery(emulated_itimer, monkeypatch):
    """Review R1301: the hook printed BEFORE re-arming, so a failing write lost the timeout a second time."""
    class _Broken:
        def write(self, s):
            raise OSError("stdout is gone")

        def flush(self):
            raise OSError("stdout is gone")
    monkeypatch.setattr(orchestrate, "_TIMEOUT_WARNED", True)
    reached_end = False
    with pytest.raises(orchestrate.UnitTimeout):
        with orchestrate._unit_deadline("zz/_all", 60.0):
            monkeypatch.setattr(sys, "stdout", _Broken())
            _swallowed_alarm("zz/_all")
            _spin(2.0)
            reached_end = True
    assert not reached_end


def test_a_timer_that_cannot_be_armed_leaves_no_hook_behind(emulated_itimer, monkeypatch):
    """Review R1301: the hook was installed BEFORE setitimer; when setitimer raised, the unit was never armed and its
    __exit__ never removed the hook."""
    def refuse(which, seconds, interval=0.0):
        raise ValueError("timer refused")
    monkeypatch.setattr(signal, "setitimer", refuse)
    monkeypatch.setattr(orchestrate, "_TIMEOUT_WARNED", True)
    before = sys.unraisablehook
    with orchestrate._unit_deadline("zz/_all", 60.0) as d:
        assert not d.armed
    assert sys.unraisablehook is before


def test_a_hook_left_chained_after_the_unit_goes_inert(emulated_itimer, monkeypatch):
    """Review R1301: another hook chained over ours keeps ours reachable after __exit__, when SIGALRM is back to its
    default - a stale re-arm then killed the process (rc 14). After exit ours must pass everything through."""
    seen = []
    monkeypatch.setattr(sys, "unraisablehook", lambda u: seen.append("prev"))     # the hook ours chains to
    with orchestrate._unit_deadline("zz/_all", 60.0):
        ours = sys.unraisablehook
        monkeypatch.setattr(sys, "unraisablehook", lambda u: (seen.append("outer"), ours(u)))
    n_armed = len(emulated_itimer)

    def late(_r):
        raise orchestrate.UnitTimeout("swallowed AFTER the unit ended")
    o = _Obj()
    keep = weakref.ref(o, late)
    del o
    gc.collect()
    assert keep() is None
    assert seen == ["outer", "prev"], f"inert means FORWARDED, never taken over: {seen}"
    assert len(emulated_itimer) == n_armed, emulated_itimer
