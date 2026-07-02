"""Strategy contract + Unit/Result types shared by all adapters.

A Strategy answers three questions about a refresh Unit:
  is_due()        -> has enough time elapsed (cadence) to bother checking?
  detect_change() -> did upstream actually move? (returns a new vintage token or None)
  run()           -> fetch the delta/refresh, publish atomically, return a Result.

The orchestrator only ever runs a unit when is_due() AND (detect_change() or forced).
A unit is marked `ok` only after run() reports a successful atomic publish.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone

# nominal days between upstream updates per cadence (0.9 slack applied in cadence_due)
CADENCE_DAYS = {
    "daily": 1, "weekly": 7, "monthly": 28, "quarterly": 90,
    "annual": 365, "irregular": 7, "static": 10 ** 6,
}


@dataclass
class Unit:
    """An atomic refresh chunk: a flow, cube, dataset, endpoint, survey, or whole source."""
    source_id: str
    unit_id: str
    strategy: str
    cadence: str = "monthly"
    out_paths: list[str] = field(default_factory=list)   # parquet path(s) this unit owns
    config: dict = field(default_factory=dict)           # strategy-specific: url, since_param, rate, key_env...

    @property
    def key(self) -> str:
        return f"{self.source_id}/{self.unit_id}"


@dataclass
class Result:
    status: str                       # ok | partial | transient_fail | definitive_fail | no_change
    obs: int = 0
    new_vintage: str | None = None
    last_obs_date: str | None = None
    error: str | None = None
    # optional per-series {series_key: last_obs_date} so the orchestrator can persist
    # per-series freshness (a frozen series can't hide behind a unit-level max).
    series_cursors: dict | None = None


def cadence_due(cadence: str, last_success_utc: str | None, now: datetime | None = None) -> bool:
    if not last_success_utc:
        return True
    now = now or datetime.now(timezone.utc)
    try:
        last = datetime.fromisoformat(last_success_utc)
    except Exception:
        return True
    days = CADENCE_DAYS.get(cadence, 7)
    return (now - last).total_seconds() >= days * 86400 * 0.9


class Strategy(ABC):
    name: str = "base"

    def is_due(self, unit: Unit, unit_state: dict | None, now: datetime | None = None) -> bool:
        ls = (unit_state or {}).get("last_success_utc")
        return cadence_due(unit.cadence, ls, now)

    @abstractmethod
    def detect_change(self, unit: Unit, unit_state: dict | None) -> str | None:
        """Return a new upstream vintage token if changed (or an always-fetch sentinel
        for cheap date-tail sources), else None to skip."""

    @abstractmethod
    def run(self, unit: Unit, since: str | None) -> Result:
        """Fetch the delta/refresh, publish atomically via merge.merge_and_write,
        and return a Result. Must obey the Transient/Definitive failure contract."""
