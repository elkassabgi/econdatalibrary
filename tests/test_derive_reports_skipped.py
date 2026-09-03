"""`derive_and_put` must report how many CSVs the skip-identical guard did NOT upload.

WHY THIS EXISTS. `R2Blob.put_atomic` compares the gzipped body against the stored object's
ETag and returns without uploading when they match, incrementing `blob.SKIPPED_IDENTICAL`. The
counter was incremented and NOTHING EVER READ IT — no print, no return value, no caller. The
saving was real and permanently invisible, so the one number that decides whether the guard is
worth its HeadObject per upload could never be measured from a live run.

Worse, `put` kept its old name while changing meaning: it counts CSVs HANDLED, and after the
guard some of those were not sent. A summary line reading "derived+put 50,000 CSVs" then names
one thing and counts another, which is the exact defect R628 and R652 are about.

WHAT THESE TESTS PIN, and each is a mutation that passed before:
  1. the count is a DIFF across the call, not the module counter's running total - otherwise a
     second call in the same process inherits the first call's skips;
  2. the percentage divides by CSVs handled, not by the skipped count or the id count;
  3. the line prints even when NOTHING was skipped - a line that appears only on a non-zero
     count cannot distinguish "no redundant uploads" from "the guard is not running";
  4. the returned dict carries `skipped_identical`, so a caller can act on it.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import blob as blob_mod          # noqa: E402
from updater import derive as derive_mod      # noqa: E402


class FakeBlob:
    """Stands in for R2Blob. `skip_every` of N marks every Nth put as already current."""

    def __init__(self, skip_every=0):
        self.skip_every = skip_every
        self.n = 0
        self.uploaded = []

    def put_atomic(self, key, data, **kw):
        self.n += 1
        if self.skip_every and self.n % self.skip_every == 0:
            blob_mod.SKIPPED_IDENTICAL[0] += 1
            return
        self.uploaded.append(key)


@pytest.fixture(autouse=True)
def _reset_counter():
    before = blob_mod.SKIPPED_IDENTICAL[0]
    blob_mod.SKIPPED_IDENTICAL[0] = 0
    yield
    blob_mod.SKIPPED_IDENTICAL[0] = before


@pytest.fixture
def fake_bytes(monkeypatch):
    """Fixed body for every series, so the test is about counting and not about parquet.

    Patch the name BOUND INTO updater.derive (`from core.derive_csv import _series_csv_bytes`
    at line 38), not the one in core.derive_csv - patching the source module leaves the
    already-bound reference untouched and the test silently exercises the real deriver.

    AQUEDUCT_DERIVE_WORKERS=1 is load-bearing, not tidiness: above one worker `_blob()` builds
    its own handle per thread with `blob_mod.from_env()` and the FakeBlob passed in is never
    used at all.
    """
    monkeypatch.setattr(derive_mod, "_series_csv_bytes",
                        lambda sid: b"date,value\n2020,1\n")
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setenv("AQUEDUCT_DERIVE_BUDGET_MIN", "0")
    return None


def _run(ids, blob, capsys):
    res = derive_mod.derive_and_put(ids, blob)
    return res, capsys.readouterr().out


def test_reports_the_skipped_count_and_share(fake_bytes, capsys):
    blob = FakeBlob(skip_every=2)                      # every second put is already current
    res, out = _run([f"src:{i}" for i in range(10)], blob, capsys)
    assert res["put"] == 10, res
    assert res["skipped_identical"] == 5, res
    assert "5 were ALREADY CURRENT" in out, out
    # the share must divide by CSVs HANDLED (10), not by the skipped count
    assert "50.0%" in out, out


def test_the_line_prints_even_when_nothing_was_skipped(fake_bytes, capsys):
    """A line that only appears on a non-zero count hides a guard that has stopped working."""
    blob = FakeBlob(skip_every=0)
    res, out = _run([f"src:{i}" for i in range(4)], blob, capsys)
    assert res["skipped_identical"] == 0, res
    assert "0 were ALREADY CURRENT" in out, out
    assert "0.0%" in out, out


def test_a_second_call_does_not_inherit_the_first_calls_skips(fake_bytes, capsys):
    """The counter is module-level; reporting it raw double-counts on the second call."""
    blob = FakeBlob(skip_every=2)
    first, _ = _run([f"src:a{i}" for i in range(10)], blob, capsys)
    assert first["skipped_identical"] == 5

    blob2 = FakeBlob(skip_every=2)
    second, out2 = _run([f"src:b{i}" for i in range(4)], blob2, capsys)
    assert second["skipped_identical"] == 2, (
        "the second call reported %r - it inherited the first call's total instead of "
        "diffing across its own run" % second["skipped_identical"])
    assert "2 were ALREADY CURRENT" in out2, out2
    assert blob_mod.SKIPPED_IDENTICAL[0] == 7, "the module counter should still be cumulative"


def test_the_counter_survives_concurrent_increments():
    """`derive_and_put` runs 8 worker threads by default and `x[0] += 1` is not atomic.

    Under the GIL a lost increment is unlikely rather than impossible - but this runs on
    Python 3.14, whose free-threaded build removes that accident of safety entirely. An
    undercount here would understate the guard's saving in a COST REPORT and argue for the
    wrong decision, which is the whole reason the number exists.

    This also pins the call site: replacing `_count_skip()` with a bare `SKIPPED_IDENTICAL[0]
    += 1` puts the raw read-modify-write back.
    """
    import threading

    start = threading.Barrier(8)
    per_thread = 2_000

    def hammer():
        start.wait()
        for _ in range(per_thread):
            blob_mod._count_skip()

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert blob_mod.SKIPPED_IDENTICAL[0] == 8 * per_thread, (
        "lost %d increment(s) across 8 threads"
        % (8 * per_thread - blob_mod.SKIPPED_IDENTICAL[0]))


def test_the_skip_path_uses_the_locked_helper():
    """A structural pin, because the threaded test above cannot fail reliably under the GIL."""
    import inspect

    src = inspect.getsource(blob_mod.R2Blob.put_atomic)
    assert "_count_skip()" in src, "the skip path must go through the locked helper"
    assert "SKIPPED_IDENTICAL[0] +=" not in src, (
        "the raw read-modify-write is back in put_atomic")


def test_skipped_never_exceeds_handled(fake_bytes, capsys):
    """A share above 100% is the signature of the wrong denominator or the wrong snapshot."""
    blob = FakeBlob(skip_every=1)                      # every put is already current
    res, out = _run([f"src:{i}" for i in range(6)], blob, capsys)
    assert res["skipped_identical"] == 6
    assert res["skipped_identical"] <= res["put"]
    assert "100.0%" in out, out
