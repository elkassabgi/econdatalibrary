"""Shared fakes for the ISTAT pacing tests of the crawler and the tools (not a test module).

A fake clock, a recording pacer (installed through jobs/_istat_pacer._TEST_OVERRIDE, so every import style of the
pacer reaches it), a fake HTTP reply, and the check that every send was paid for by the token right before it.
Nothing here opens a socket."""
from __future__ import annotations

import time as _real_time

from jobs import _istat_pacer

EPOCH = 1_800_000_000.0


class Clock:
    """time(), monotonic() and sleep() on one fake timeline; anything else is the real time module."""

    def __init__(self):
        self.t = 0.0
        self.slept = []

    def time(self):
        return EPOCH + self.t

    def monotonic(self):
        return self.t

    def sleep(self, s):
        self.slept.append(float(s))
        self.t += max(0.0, float(s))

    def __getattr__(self, name):
        return getattr(_real_time, name)


class Recorder:
    """A recording pacer. acquire() waits `wait_s` fake seconds and refuses (WindowEnd) a slot later than
    deadline - 1 s; every call is appended to `events`, which the fake network shares."""

    def __init__(self, clock, events, wait_s=0.0):
        self.clock, self.events, self.wait_s, self.n = clock, events, float(wait_s), 0

    def acquire(self, participant, host, kind, path_class, *, deadline_ts=None, flow=None, clock=None):
        slot = self.clock.time() + self.wait_s
        if deadline_ts is not None and slot > float(deadline_ts) - 1.0:
            raise _istat_pacer.WindowEnd("recorder: slot after deadline - 1 s", slot=slot, deadline_ts=deadline_ts)
        self.clock.sleep(self.wait_s)
        self.n += 1
        h = _istat_pacer.host_of(host)
        self.events.append(("token", participant, h, kind, path_class, flow, self.clock.time()))
        return _istat_pacer.Token(f"rec-{self.n}", slot, self.wait_s, "recorder", participant, h, kind, path_class,
                                  flow, start=self.clock.time())

    def outcome(self, token, text, elapsed_s):
        self.events.append(("outcome", getattr(token, "id", None), text))

    def penalize(self, seconds, participant, clock=None):
        self.events.append(("penalize", float(seconds), participant))
        return self.clock.time() + float(seconds)

    def begin_turn(self, end_ts=None, clock=None):
        self.events.append(("begin_turn", end_ts))
        return True

    def end_turn(self, clock=None):
        self.events.append(("end_turn",))
        return True

    def yield_to_turn(self, log=None, clock=None):
        self.events.append(("yield_to_turn",))
        return 0.0

    def describe(self):
        return "[istat-pacer] state (recorder); mode recorder; gap 16 s; budget 4/min"


class Resp:
    def __init__(self, status, content=b"", headers=None):
        self.status_code, self.content, self.headers = status, content, dict(headers or {})


def sends_are_paced(events):
    """Every send / connect is IMMEDIATELY preceded by a token for the same host and kind. -> the offences."""
    bad = []
    for i, e in enumerate(events):
        if e[0] not in ("send", "connect"):
            continue
        kind = "http" if e[0] == "send" else "tcp"
        prev = [p for p in events[:i] if p[0] in ("token", "send", "connect")]
        last = prev[-1] if prev else None
        if not (last and last[0] == "token" and last[2] == e[1] and last[3] == kind):
            bad.append((i, e[:3], last[:4] if last else None))
    return bad


def max_in_closed_window(starts, window=60.0):
    """The most starts in any CLOSED window of `window` seconds."""
    s = sorted(starts)
    best, j = 0, 0
    for i in range(len(s)):
        j = max(j, i)
        while j + 1 < len(s) and s[j + 1] <= s[i] + window:
            j += 1
        best = max(best, j - i + 1)
    return best
