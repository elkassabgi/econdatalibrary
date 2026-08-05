"""The orchestrator's UnitTimeout must pass THROUGH ingest_imf_direct's broad handlers.

UnitTimeout is deliberately an Exception (the unit handler demotes one source, never aborts
the run) — which meant every broad `except Exception` between the SIGALRM and the orchestrator
consumed the kill: the retry loops slept and RETRIED it, and the streamed→sliced fallback
turned it into the starting gun for a full re-pull (PIP ran 80+ minutes past its 45-minute
deadline). Each site now re-raises by class NAME (import-cycle-free); these tests plant a
fake-named exception exactly where the SIGALRM would land and assert immediate propagation —
no retry sleeps, no fallback (R346: each guard shown able to fire).
"""
from __future__ import annotations

import os
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from jobs import ingest_imf_direct as ing  # noqa: E402


class UnitTimeout(Exception):
    """Same NAME as updater.orchestrate.UnitTimeout — the handlers match by name."""


def test_http_get_reraises_unit_timeout_without_retrying(monkeypatch):
    calls = {"n": 0}

    def boom(*a, **k):
        calls["n"] += 1
        raise UnitTimeout("SIGALRM landed here")

    monkeypatch.setattr(ing.urllib.request, "urlopen", boom)
    slept = []
    monkeypatch.setattr(time, "sleep", lambda s: slept.append(s))
    t0 = time.monotonic()
    with pytest.raises(UnitTimeout):
        ing.http_get("https://api.imf.org/external/sdmx/2.1/dataflow")
    assert calls["n"] == 1, f"retried {calls['n']} times — the kill was consumed"
    assert not slept, "slept before re-raising — the retry path ran"
    assert time.monotonic() - t0 < 1.0


def test_http_get_to_file_reraises_unit_timeout(monkeypatch, tmp_path):
    calls = {"n": 0}

    def boom(*a, **k):
        calls["n"] += 1
        raise UnitTimeout("SIGALRM landed mid-stream")

    monkeypatch.setattr(ing.urllib.request, "urlopen", boom)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    with pytest.raises(UnitTimeout):
        ing.http_get_to_file("https://api.imf.org/x", str(tmp_path / "d.bin"))
    assert calls["n"] == 1


def test_pull_does_not_fall_back_to_sliced_on_timeout(monkeypatch):
    def boom(*a, **k):
        raise UnitTimeout("kill during the streamed pull")

    sliced = {"called": False}
    monkeypatch.setattr(ing, "_pull_streamed", boom)
    monkeypatch.setattr(ing, "_pull_sliced",
                        lambda *a, **k: sliced.__setitem__("called", True))
    with pytest.raises(UnitTimeout):
        ing.pull("PIP", "IMF.STA", "imf_pip_direct")
    assert not sliced["called"], (
        "the belt-and-braces fallback ran on a TIMEOUT — the kill became the starting gun "
        "for a full sliced re-pull, the exact 80-minute failure")


def test_an_ordinary_error_still_retries_and_falls_back(monkeypatch):
    """Negative control: the fix must not break the paths that were correct."""
    calls = {"n": 0}

    def flaky(*a, **k):
        calls["n"] += 1
        raise OSError("transient network burp")

    monkeypatch.setattr(ing.urllib.request, "urlopen", flaky)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError):
        ing.http_get("https://api.imf.org/x")
    assert calls["n"] == ing.RETRIES, "ordinary errors must still use the full retry budget"
