"""S2 — extend_by_date.

For API sources that accept a server-side date filter. Change-detection is cheap
("always fetch the small tail"), and the per-source fetcher pulls only
observations newer than the stored last_obs_date, merging them into the existing
series. This is the strategy that fixes the skip-if-series-exists freeze: existing
series get EXTENDED with new dates rather than skipped.
"""
from __future__ import annotations

from . import register
from .base import Strategy, Unit, Result
from .fetchers import get as get_fetcher


@register("extend_by_date")
class ExtendByDate(Strategy):
    def detect_change(self, unit: Unit, unit_state: dict | None) -> str | None:
        # Date-tail sources are cheap to poll; always attempt the tail. The fetcher
        # itself returns no_change (0 new rows) when upstream hasn't moved.
        return "date-tail"

    def run(self, unit: Unit, since: str | None) -> Result:
        return get_fetcher(unit.source_id).update(unit, since)
