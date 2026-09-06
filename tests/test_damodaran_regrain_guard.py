"""The damodaran fetcher must refuse to MERGE a key re-grain (R22/R333).

The key fix for R516 qualifies labels that used to collide, so the fixed parser emits
`DAMODARAN:evmultiples:EV_EBITDA__All_firms:Advertising` where the store holds
`DAMODARAN:evmultiples:EV_EBITDA:Advertising`. Those two never collide, and that is exactly the
danger: `merge_and_write` ADDS the new keys beside the old ones, the file only GROWS, and both
never-shrink and the fetcher's `min_ratio=0.92` wave it through. ons_uk reached 20,198,302 rows
for 10,099,151 observations this way.

The fetcher's own comment records that no merge has ever completed for damodaran, so the first
one to complete is the dangerous one. `_stale_grain_keys` is what stops it: for every qualified
key the pull produced, it asks whether the UNQUALIFIED form is still in the store. It needs no
sidecar and self-clears — once `python jobs/ingest_damodaran.py` has rewritten the file with a
plain `pq.write_table` (an overwrite, i.e. the clean re-pull), no unqualified form remains.

Four cases, because a guard with only its happy path tested is not a guard:
  * a pre-fix store must FIRE
  * no store at all must NOT fire (never block a first publish)
  * a store that EXISTS but cannot be read must FAIL CLOSED (R503 — the except-branch that
    fails open is how a guard becomes decoration)
  * a pull with no qualified keys has nothing to check
"""
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater.strategies.fetchers.damodaran import _stale_grain_keys   # noqa: E402

OLD = "DAMODARAN:evmultiples:EV_EBITDA:Advertising"
NEW_A = "DAMODARAN:evmultiples:EV_EBITDA__All_firms:Advertising"
NEW_B = "DAMODARAN:evmultiples:EV_EBITDA__Only_positive_EBITDA_firms:Advertising"
UNRELATED = "DAMODARAN:betas:Beta:Advertising"


def _store(tmp_path, keys):
    p = os.path.join(str(tmp_path), "damodaran.parquet")
    pq.write_table(pa.table({"series_key": pa.array(keys, pa.string())}), p)
    return p


def test_a_pre_fix_store_is_refused(tmp_path):
    path = _store(tmp_path, [OLD, UNRELATED])
    stale = _stale_grain_keys(path, [NEW_A, NEW_B, UNRELATED], before=2)
    assert stale == [OLD], stale


def test_a_store_already_on_the_new_grain_passes(tmp_path):
    """Self-clearing: after the clean re-pull no unqualified form remains."""
    path = _store(tmp_path, [NEW_A, NEW_B, UNRELATED])
    assert _stale_grain_keys(path, [NEW_A, NEW_B, UNRELATED], before=3) == []


def test_no_store_does_not_block_a_first_publish(tmp_path):
    missing = os.path.join(str(tmp_path), "absent.parquet")
    assert _stale_grain_keys(missing, [NEW_A], before=0) == []


def test_an_unreadable_store_FAILS_CLOSED(tmp_path):
    """before>0 means there is something to protect; a read failure must not wave a merge through.

    This is the branch that decides whether the guard is real. R503 is the entry about a guard
    whose except-branch failed open.
    """
    missing = os.path.join(str(tmp_path), "absent.parquet")
    out = _stale_grain_keys(missing, [NEW_A], before=26_536)
    assert out, "an unreadable store with rows must be reported, not passed"
    assert "could not be read" in out[0], out


def test_a_pull_with_no_qualified_keys_checks_nothing(tmp_path):
    path = _store(tmp_path, [OLD, UNRELATED])
    assert _stale_grain_keys(path, [OLD, UNRELATED], before=2) == []


def test_the_guard_is_called_before_the_merge():
    """A predicate nobody calls is not a guard (R109)."""
    src = open(os.path.join(ROOT, "updater", "strategies", "fetchers", "damodaran.py"),
               encoding="utf-8").read()
    i = src.find("_stale_grain_keys(path, all_keys")
    j = src.find("merge.merge_and_write(")
    assert i > 0, "the guard is defined but never called"
    assert j > 0 and i < j, "the guard must run BEFORE merge_and_write, not after"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
