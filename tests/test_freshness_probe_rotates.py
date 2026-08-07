"""The rotating freshness probe must actually rotate, and its bookmark must persist.

WHAT THIS PINS (2026-08-07). `tools/probe_csv_freshness.py` samples served bytes against the
store for a few sources per run and ROTATES, so that over enough runs the whole fleet is covered.
Its bookmark was written through `updater.blob`, which derives an R2 key from a STORE path and
rejects anything without a `/data/<tier>/` segment. `_aqueduct/csv_freshness_cursor.json` has no
such segment, so every save raised, the cursor was never written, and the probe re-selected

    abs, adb, barro_lee, bcb, bcrp

on every single run. It had never reached a source past 'b'. That is exactly the R190 failure the
probe's own docstring says it exists to avoid: a bounded pass over a fixed order with no working
bookmark re-walks the same prefix forever while the tail is never checked at all.

The tool printed a WARNING naming the consequence every time. I wired it into daily CI and did not
read its output. So the test below is not about the exception — it is about the outcome: give the
probe a cursor and it must select something AFTER it.
"""
from __future__ import annotations

import inspect
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def test_the_bookmark_round_trips_without_the_store_path_helper():
    """NOTE THE TEST KEY. The first version of this wrote its sentinel to the REAL bookmark.
    It restored the local file and forgot that _save_cursor also writes to R2 — so the next
    live probe opened with "rotating after 'zz_probe_sentinel'", i.e. a test had reached into
    production state. (It wrapped to 'abs' and lost nothing, because _rotate_after treats an
    unknown cursor as the start, but that was luck.) Both the key and the local path are
    redirected here, and the R2 object is deleted afterwards."""
    from tools import probe_csv_freshness as P
    real_key, real_local = P.BOOKMARK, P._cursor_local
    P.BOOKMARK = "_aqueduct/_test_csv_freshness_cursor.json"
    P._cursor_local = lambda: os.path.join(ROOT, "data", "_aqueduct",
                                           "_test_csv_freshness_cursor.json")
    try:
        P._save_cursor(None, "zz_probe_sentinel")
        assert P._load_cursor(None) == "zz_probe_sentinel", (
            "the cursor did not survive a save/load round trip — the probe will restart at the "
            "top of the alphabet on every run and never check the tail")
    finally:
        try:
            os.remove(P._cursor_local())
        except OSError:
            pass
        try:
            from core import r2_util
            r2_util.client().delete_object(Bucket="econ-data", Key=P.BOOKMARK)
        except Exception:                                             # noqa: BLE001
            pass
        P.BOOKMARK, P._cursor_local = real_key, real_local


def test_the_bookmark_does_not_go_through_blob_path_keying():
    """`blob.write_bytes_atomic(BOOKMARK, ...)` is the exact call that always raised."""
    from tools import probe_csv_freshness as P
    src = inspect.getsource(P._save_cursor) + inspect.getsource(P._load_cursor)
    assert "blob.write_bytes_atomic" not in src and "blob.read_bytes" not in src, (
        "the bookmark is routed through updater.blob again; it derives an R2 key from a store "
        "path and rejects _aqueduct/..., so the probe stops rotating")
    assert "put_object" in src and "get_object" in src


def test_a_cursor_selects_sources_after_it():
    """The rotation itself, independent of persistence."""
    from tools.probe_csv_freshness import _rotate_after
    order = ["abs", "adb", "bcb", "bcrp", "bea", "wid", "zillow"]
    assert _rotate_after(order, "bcrp")[0] == "bea"
    assert _rotate_after(order, "")[0] == "abs"
    # Past the end it must wrap, or the last run of a sweep would return nothing and the probe
    # would silently stop checking anything at all.
    assert _rotate_after(order, "zillow")[0] == "abs"
