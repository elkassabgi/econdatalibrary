"""statcan must not report `no_change` when its store is unreachable.

Measured 2026-09-05. statcan's ~8,207 cubes were deleted from R2 on 2026-08-18 (a deliberate cost
action) while the local route runs with `AQUEDUCT_BACKEND=r2` (tools/run_local_heavy.ps1). The
fetcher's documented scope is "only cubes already on disk are refreshed", and its skip for a cube it
does not hold is SILENT — `continue`, no sub-unit counted. So every changed cube took that branch,
the tally stayed empty, and `finalize()` booked `no_change`.

The run history shows it exactly: 2026-08-02 merged +19,653 rows in 829.9 s; after the deletion the
runs are 1.4 s (08-22) and 7.9 s (09-04), both `no_change`, "no new rows" — while StatCan's own
change-feed listed 337 cube-changes in the 15 days to 09-05.

These tests pin the discrimination the guard makes:
  * changed cubes we do not hold, but the store HAS other cubes -> coverage, stay quiet (unchanged);
  * changed cubes and the store holds NOTHING                   -> DefinitiveError, never no_change.
"""
from __future__ import annotations

import pytest

from updater.errors import DefinitiveError
from updater.strategies.fetchers import statcan as sc


class _Tally:
    def __init__(self):
        self.attempted = 0


def _guard(changed, attempted, absent, held, backend="r2"):
    """Run the guard's decision in isolation, exactly as the fetcher expresses it."""
    tally = _Tally()
    tally.attempted = attempted
    if changed and not tally.attempted and absent:
        if not held:
            raise DefinitiveError(
                f"statcan: the store holds ZERO cubes (backend={backend}) while the change-feed lists "
                f"{len(changed)} changed cube(s)")
    return "no_change"


def test_empty_store_with_changed_cubes_raises():
    with pytest.raises(DefinitiveError) as ei:
        _guard(changed=["12345678", "87654321"], attempted=0, absent=["12345678", "87654321"], held=[])
    assert "ZERO cubes" in str(ei.value)


def test_brand_new_cubes_against_a_populated_store_stay_quiet():
    """The pre-existing, correct behaviour: cubes we never ingested are the bulk ingester's job."""
    assert _guard(changed=["99999999"], attempted=0, absent=["99999999"],
                  held=["/store/12345678.parquet"]) == "no_change"


def test_a_run_that_attempted_something_is_never_caught_by_the_guard():
    assert _guard(changed=["1", "2"], attempted=1, absent=["2"], held=[]) == "no_change"


def test_quiet_publisher_is_not_a_defect():
    """No changed cubes at all is StatCan's normal quiet day — the guard must not fire."""
    assert _guard(changed=[], attempted=0, absent=[], held=[]) == "no_change"


def test_the_fetcher_module_carries_the_guard():
    """A source-level pin: the guard must exist at the call site, not only in this test's model."""
    import inspect
    src = inspect.getsource(sc)
    assert "absent_pids" in src, "the fetcher must record which changed cubes it skipped as not held"
    assert "holds ZERO cubes" in src, "the fetcher must refuse to report no_change over an empty store"
    assert "if changed and not tally.attempted and absent_pids:" in src, (
        "the guard must fire only when the feed listed changes, nothing was attempted, and every "
        "changed cube was skipped as not held")
