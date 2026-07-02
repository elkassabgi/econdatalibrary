"""Shared helpers for S2 fetchers: an HONEST ok/no_change/partial decision.

The dominant oversight finding: fetchers laundered sub-unit failures (a per-endpoint
timeout, a wholesale 404, a 200 with an unparseable/empty body) into no_change/ok, so
the orchestrator stamped last_success and the source looked fresh while actually frozen.

Tally + finalize make the returned status truthful:
  - STRUCTURAL break (a 200 that parsed 0 rows from a non-trivial body)  -> DefinitiveError
  - a large all-empty/404 window (likely a structural break, not a quiet period) -> DefinitiveError
  - any TRANSIENT sub-failure                                            -> status='partial'
        (the orchestrator does NOT stamp last_success on 'partial'; the unit re-runs next tick)
  - otherwise                                                            -> 'ok' (added>0) / 'no_change'
In every failure case the caller has already left existing data untouched (merge_and_write
only publishes good data), so this is about correct STATUS, never data loss.
"""
from __future__ import annotations
import datetime as _dt

from ..base import Result
from ...errors import DefinitiveError


def sane_since(stored_max, *, max_future_days=400):
    """Guard a date-tail boundary against CORRUPT far-future obs_dates.

    PxWeb (and some SDMX) time-dimension heuristics can mis-store a sentinel like
    year 9999/6000/2584. If a fetcher then filters `period > stored_max`, it selects
    NOTHING and the series freezes forever. This returns None when stored_max is more
    than max_future_days beyond today (so the caller should fall back to a trailing
    window / full re-pull instead of a delta), else returns stored_max unchanged.
    """
    if stored_max is None:
        return None
    try:
        d = stored_max if isinstance(stored_max, _dt.date) else _dt.date.fromisoformat(str(stored_max)[:10])
    except Exception:
        return None
    if (d - _dt.date.today()).days > max_future_days:
        return None
    return stored_max


class Tally:
    """Counts the outcome of each sub-unit (endpoint / dataset / series / day) a fetcher attempts."""

    def __init__(self):
        self.attempted = 0
        self.added = 0          # total NEW rows merged in
        self.empty = 0          # sub-units that succeeded but had no new data
        self.transient = 0      # sub-units that transient-failed (retry next run)
        self.structural = 0     # sub-units that returned 200 but were unparseable/empty-from-real-body

    def added_unit(self, n: int):
        self.attempted += 1
        if n and n > 0:
            self.added += n
        else:
            self.empty += 1

    def empty_unit(self):
        self.attempted += 1
        self.empty += 1

    def transient_unit(self):
        self.attempted += 1
        self.transient += 1

    def structural_unit(self):
        self.attempted += 1
        self.structural += 1


def finalize(tally: Tally, total_rows, last_obs, *, source, series_cursors=None,
             empty_window_floor=10):
    """Turn a Tally into an honest Result (or raise DefinitiveError). See module docstring."""
    if tally.structural:
        raise DefinitiveError(
            f"{source}: {tally.structural}/{tally.attempted} sub-unit(s) returned 200 but parsed 0 "
            f"rows from a non-trivial body (schema/structural break); existing data kept")
    if tally.added == 0 and tally.empty == tally.attempted and tally.attempted > empty_window_floor:
        raise DefinitiveError(
            f"{source}: all {tally.attempted} attempted sub-units returned empty/404 over a large "
            f"window — likely a structural break, not a quiet period; existing data kept")
    if tally.transient:
        return Result(status="partial", obs=total_rows, last_obs_date=last_obs,
                      new_vintage="date-tail", series_cursors=series_cursors,
                      error=f"{tally.transient}/{tally.attempted} sub-unit(s) transient-failed; will retry")
    status = "ok" if tally.added > 0 else "no_change"
    return Result(status=status, obs=total_rows, last_obs_date=last_obs, new_vintage="date-tail",
                  series_cursors=series_cursors,
                  error=(f"+{tally.added} new rows" if tally.added else "no new rows"))
