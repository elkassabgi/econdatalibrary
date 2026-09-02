"""A replace that a reader's open handle would have broken now waits for it.

On Windows `os.replace(tmp, dest)` raises `PermissionError [WinError 5]` while another process
holds `dest` open, and every footer read in this repository does exactly that. A reviewer
demonstrated it in both directions on 2026-09-02: a plain read handle and a
`pyarrow.ParquetFile` handle each break the writer, a DuckDB cursor does not.

That is why the gus_dbw repair could not be run at all - its crawler writes continuously, so
reading the footers to decide what to repair might stop the process that maintains them, and 16
areas stayed on a nine-day-old vintage instead.

The tests are platform-honest: the real-handle case only exercises the collision on Windows,
because POSIX renames regardless. The RETRY LOGIC itself is tested everywhere with an injected
failure, so the behaviour that matters is pinned on both platforms rather than skipped into
non-existence on the one that runs CI.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.atomic import BACKOFF_S, atomic_replace, replaced_after  # noqa: E402


def _pair(tmp_path, body=b"new"):
    dest = tmp_path / "dest.bin"
    dest.write_bytes(b"old")
    tmp = tmp_path / "tmp.bin"
    tmp.write_bytes(body)
    return str(tmp), str(dest)


def test_the_ordinary_case_still_replaces(tmp_path):
    tmp, dest = _pair(tmp_path)
    atomic_replace(tmp, dest)
    assert open(dest, "rb").read() == b"new"
    assert not os.path.exists(tmp)


def test_it_retries_a_PermissionError_and_then_succeeds(tmp_path, monkeypatch):
    """The whole point: a reader holding the target for a few milliseconds must not fail a
    write. Injected rather than raced, so this pins the behaviour on every platform."""
    tmp, dest = _pair(tmp_path)
    real = os.replace
    calls = {"n": 0}

    def flaky(a, b, *args, **kw):
        calls["n"] += 1
        if calls["n"] <= 3:
            raise PermissionError(13, "another process holds it")
        return real(a, b, *args, **kw)

    monkeypatch.setattr(os, "replace", flaky)
    n = replaced_after(tmp, dest)
    assert calls["n"] == 4
    assert n == 3, f"reported {n} retries, took 3"
    assert open(dest, "rb").read() == b"new"


def test_a_handle_that_never_lets_go_RAISES(tmp_path, monkeypatch):
    """A replace that quietly did not happen is worse than one that failed: the caller goes on
    to report the new data as written, which is R641's defect exactly."""
    tmp, dest = _pair(tmp_path)

    def never(a, b, *args, **kw):
        raise PermissionError(13, "held forever")

    monkeypatch.setattr(os, "replace", never)
    with pytest.raises(PermissionError):
        atomic_replace(tmp, dest, retries=(0.0, 0.0))
    assert open(dest, "rb").read() == b"old", "the destination changed despite the failure"


def test_every_attempt_is_made_before_giving_up(tmp_path, monkeypatch):
    """len(retries) waits means len(retries) + 1 attempts. An off-by-one here is the
    difference between clearing a collision and reporting one."""
    tmp, dest = _pair(tmp_path)
    n = {"c": 0}

    def never(a, b, *args, **kw):
        n["c"] += 1
        raise PermissionError(13, "held")

    monkeypatch.setattr(os, "replace", never)
    with pytest.raises(PermissionError):
        atomic_replace(tmp, dest, retries=(0.0, 0.0, 0.0))
    assert n["c"] == 4, n["c"]


def test_a_NON_permission_error_is_raised_at_once(tmp_path, monkeypatch):
    """A missing directory will not resolve itself in three seconds, and waiting on it hides
    the real fault behind a delay."""
    tmp, dest = _pair(tmp_path)
    n = {"c": 0}

    def missing(a, b, *args, **kw):
        n["c"] += 1
        raise FileNotFoundError(2, "no such directory")

    monkeypatch.setattr(os, "replace", missing)
    with pytest.raises(FileNotFoundError):
        atomic_replace(tmp, dest)
    assert n["c"] == 1, "a structural error was retried"


def test_the_backoff_is_short_enough_to_report_and_long_enough_to_clear():
    """3.1 s total: three orders of magnitude more than a footer read holds a file, and short
    enough that a genuinely stuck handle is reported rather than waited on."""
    total = sum(BACKOFF_S)
    assert 1.0 < total < 10.0, total
    assert BACKOFF_S[0] <= 0.1, "the first retry must be fast; most collisions clear at once"


@pytest.mark.skipif(os.name != "nt", reason=(
    "POSIX renames regardless of open handles, so there is no collision to clear there. The "
    "retry logic itself is tested above on every platform with an injected failure, rather "
    "than skipped into non-existence on the platform that runs CI."))
def test_a_REAL_open_handle_is_cleared(tmp_path):
    """The actual Windows collision, with a real handle, released by a timer."""
    import threading

    tmp, dest = _pair(tmp_path)
    fh = open(dest, "rb")
    threading.Timer(0.25, fh.close).start()
    try:
        n = replaced_after(tmp, dest)
    finally:
        if not fh.closed:
            fh.close()
    assert n >= 1, "the handle did not actually block the replace; this proved nothing"
    assert open(dest, "rb").read() == b"new"
