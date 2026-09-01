"""istat must be time-bounded AND must not re-walk the same prefix (AR-017 SHOULD-FIX 3).

Measured 2026-09-01: a local heavy pass sat on istat for 33+ minutes at 5.3 CPU-SECONDS —
blocked on a socket, no per-flow logging, and no bound of any kind, while 16 due sources
waited. The orchestrator's hard per-unit timeout does not exist on Windows, so on the local
runner this source was unbounded.

A budget ALONE would be R190: Deadline's own docstring records that a budget over a fixed
order re-walks the same prefix every run and the tail never drains. These tests pin BOTH
halves, and pin that the resume point is only saved for flows actually reached.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import istat  # noqa: E402
from updater.strategies.fetchers._common import rotate_after  # noqa: E402

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "updater", "strategies", "fetchers", "istat.py")


def _src():
    return open(SRC, encoding="utf-8").read()


def test_istat_declares_a_budget():
    assert hasattr(istat, "BUDGET_MIN"), "istat has no wall-clock budget"
    assert istat.BUDGET_MIN > 0


def test_the_loop_stops_when_the_budget_is_spent():
    s = _src()
    i = s.find("for fn in files:")
    assert i != -1
    body = s[i:i + 700]
    assert "if dl.spent():" in body, "the flow loop no longer checks the deadline FIRST"
    assert "break" in body


def test_the_budget_is_paired_with_rotation_not_a_fixed_order():
    """R190: a budget over a stable order drains only the prefix. _flow_files is sorted, so
    the run must resume after the previous run's bookmark."""
    s = _src()
    assert "load_rotation(out_dir)" in s, "no rotation bookmark is loaded"
    assert "rotate_after(files, bookmark)" in s, "the flow list is not rotated"
    assert "save_rotation(out_dir, last_attempted)" in s, "the resume point is never saved"


def test_the_bookmark_is_only_saved_for_flows_actually_reached():
    """Saving a bookmark for a flow the run never got to would skip it for ever — the
    silent half of R190."""
    s = _src()
    assert "if last_attempted:" in s
    i = s.find("last_attempted = fn")
    j = s.find("tally.added_unit(m)")
    assert i != -1 and j != -1 and i > j, (
        "last_attempted must be set AFTER the flow's rows are merged, not before it is tried")


def test_rotate_after_actually_resumes(tmp_path):
    """Drive the SHIPPED rotation helper, not a re-implementation: a bookmark in the middle
    yields the tail first and wraps to the head, so the whole set drains across runs."""
    items = ["a.parquet", "b.parquet", "c.parquet", "d.parquet"]
    assert rotate_after(items, "b.parquet") == ["c.parquet", "d.parquet",
                                                "a.parquet", "b.parquet"]
    # an unknown or empty bookmark must not lose anything
    assert sorted(rotate_after(items, "zzz")) == sorted(items)
    assert sorted(rotate_after(items, "")) == sorted(items)
