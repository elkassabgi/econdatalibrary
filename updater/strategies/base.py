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

# How long a unit that ended PARTIAL waits before the whole pass is attempted again.
# See Strategy.is_due for why a partial has to count for scheduling at all. Capped rather
# than using the unit's own cadence so a source whose sub-units failed TRANSIENTLY is not
# stranded for up to a month, while a source whose failures are permanent still stops
# re-running its full cost every single day.
PARTIAL_RETRY_DAYS = 7


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
    # OPTIONAL merge-measured changed set: {series_key: max changed obs_date or None}
    # from merge_and_write(report_changed_keys=True). When present (not None), §5.7
    # derives CSVs from THIS instead of series_cursors.keys() — the two answer
    # different questions and the audit of 2026-08-31 found series_cursors serving
    # three contradictory contracts at once (health frontier / changed set / cursor
    # state): the seeding that health NEEDS (every on-disk flow, every run) makes the
    # changed-set reading over-report (ecb changed==attempted 25/25), while max-date
    # cursors under-report same-period revisions. The merge is the only place that
    # knows what actually changed. None (the default) = the fetcher has not migrated;
    # behaviour is exactly as before. An EMPTY dict is a real statement ("nothing
    # changed") and is honoured. Keys are STORE grain; for a source whose catalogue is
    # series-grain they map exactly (norgesbank, the pilot).
    changed_keys: dict | None = None


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


def _due_after(days: float, stamp: str | None, now: datetime | None = None) -> bool:
    """Has `days` elapsed since `stamp`? Same 0.9 slack as cadence_due.

    Split out so the partial-retry path can use an explicit number of days rather than a
    cadence name; an unparseable or missing stamp means due, never silently never-due.
    """
    if not stamp:
        return True
    now = now or datetime.now(timezone.utc)
    try:
        last = datetime.fromisoformat(stamp)
    except Exception:                                        # noqa: BLE001
        return True
    return (now - last).total_seconds() >= days * 86400 * 0.9


class Strategy(ABC):
    name: str = "base"

    def is_due(self, unit: Unit, unit_state: dict | None, now: datetime | None = None) -> bool:
        """Has enough time elapsed to bother checking this unit again?

        A PARTIAL PASS STILL COUNTS AS A PASS, FOR SCHEDULING ONLY (2026-07-31).

        This used to key solely on last_success_utc, and a `partial` deliberately never sets
        that — correctly, because a partial is not a success and the SLA/health gate must
        keep saying so. But scheduling drew the wrong conclusion from it: a source whose
        partial is PERMANENT (upstream retired some tables, a subset can never parse) has
        last_success NULL forever, so cadence_due returns True forever, so it re-runs its
        ENTIRE cost on every single run no matter what cadence it declares.

        Measured on production state 2026-07-31: 32 of 103 units had never recorded a
        success, and their typical durations summed to 1,303 minutes against a 240-minute
        budget — unsdg 351 min, ssb 231, statfin 138, insee_bdm 105, stat_estonia 103,
        hagstofa 64. The run therefore spent its whole budget on whoever sorted first and
        never reached the rest, every day. hagstofa declares cadence `monthly` and was being
        re-crawled — 1,906 tables, ~55 min — on every run it was offered.

        So: when a unit has never succeeded but its last attempt was a PARTIAL, the cadence
        is measured from that attempt. A partial means the pass RAN and some sub-units
        failed; repeating it tomorrow cannot help if those failures are permanent.

        `transient_fail` is deliberately NOT included: that means the pass could not run at
        all (timeout, 5xx, network), so it must stay immediately retryable.

        Nothing here changes what is REPORTED. last_success stays unset, the source still
        reads as stale to the health gate and to /v1/last-updates, and `--force` still
        overrides. This is only about how often we pay the cost of trying again.
        """
        st = unit_state or {}
        ls = st.get("last_success_utc")
        if ls:
            return cadence_due(unit.cadence, ls, now)
        if st.get("status") == "partial" and st.get("last_attempt_utc"):
            # CAPPED AT PARTIAL_RETRY_DAYS, not the full cadence. A partial can be caused by
            # PERMANENT sub-unit failures (upstream retired a table -> re-running sooner
            # cannot help) or by TRANSIENT ones (insee_bdm's last pass had 201/201 sub-units
            # transient-fail -> re-running sooner is exactly what should happen). The state
            # does not distinguish them structurally, so waiting a source's full cadence
            # would strand a recoverable source for up to a month, while waiting a day is
            # what caused the pathology. The cap splits the difference: far cheaper than
            # daily, far more responsive than monthly.
            return _due_after(min(CADENCE_DAYS.get(unit.cadence, 7), PARTIAL_RETRY_DAYS),
                              st.get("last_attempt_utc"), now)
        return True

    @abstractmethod
    def detect_change(self, unit: Unit, unit_state: dict | None) -> str | None:
        """Return a new upstream vintage token if changed (or an always-fetch sentinel
        for cheap date-tail sources), else None to skip."""

    @abstractmethod
    def run(self, unit: Unit, since: str | None) -> Result:
        """Fetch the delta/refresh, publish atomically via merge.merge_and_write,
        and return a Result. Must obey the Transient/Definitive failure contract."""
