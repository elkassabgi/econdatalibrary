"""S4 — giant_changed_units.

For the named GIANTS (eurostat, oecd; sdmx_nso, statcan share the shape): a source
whose data is THOUSANDS of per-flow parquet files in one directory (~1,400+ files,
~6B obs each). A blind re-crawl is many hours, so the refresh is a catalogue
CHANGE-FEED diff that re-pulls ONLY changed flows.

The whole engine lives in the per-source fetcher
(updater/strategies/fetchers/<eurostat|oecd>.py) on top of fetchers/_giant.py, so
this adapter is thin and reuses the exact extend_by_date contract:

    detect_change(unit, state) -> cheap catalogue token (None iff catalogue unmoved)
    run(unit, since)           -> fetcher.update(unit, since) -> honest Result

How the fetcher decides what to fetch (see fetchers/_giant.run_giant):
  * re-downloads the source catalogue / TOC,
  * DIFFs each flow's upstream last-update/version vs a stored per-flow snapshot
    (sidecar <source_dir>/_giant_state.json),
  * selects flows that CHANGED, are NEW, or whose last run was
    partial/failed/empty/absent (so a once-broken flow never freezes),
  * materializes ONE unit per changed flow (registry.flow_unit) and fetches it
    incrementally via server-side startPeriod, merging per-flow under
    dedup-on-(series_key,obs_date) + never-shrink,
  * returns an HONEST status (429/timeout -> partial & reselect; 200-with-0-rows
    from a real body -> structural; cap overflow -> partial).

DUPLICATION GUARD — the emitted series_key MUST be STABLE:
  oecd: already stable (dim columns between DATAFLOW and TIME_PERIOD joined '.';
        LAST_UPDATE is a post-TIME_PERIOD attribute, never in the key).
  eurostat: the fetcher BUILDS the key from dimension columns ONLY, explicitly
        dropping the `LAST UPDATE` column (and OBS_FLAG/CONF_STATUS). The legacy
        ingest left `LAST UPDATE=DD/MM/YY HH:MM:SS` IN the key, which changes every
        release and duplicated the whole file on each publish. With the stable key,
        re-publishing the same series on a new release dedups to the SAME rows.

  ┌────────────────────────────────────────────────────────────────────────────┐
  │ ONE-TIME RE-KEY OF EXISTING EUROSTAT DATA — DATA OP, DESIGNED, *NOT* RUN     │
  │                                                                              │
  │ The ~7,750 existing clean_full/eurostat/*.parquet rows carry the OLD         │
  │ unstable key (`LAST UPDATE=...:freq=...:...:OBS_FLAG=e`). On first run the    │
  │ stable-key tail would NOT dedup against those old rows (different key) and    │
  │ the file would grow a parallel stable-keyed copy of the latest periods —      │
  │ not a duplicate of history, but a key split. The clean fix is a one-time      │
  │ re-key migration BEFORE the first S4 run:                                     │
  │   for each clean_full/eurostat/<CODE>.parquet:                               │
  │     read; for each series_key strip the leading `LAST UPDATE=<...>:` prefix   │
  │     AND any trailing `:OBS_FLAG=<x>` / `:CONF_STATUS=<x>` token; re-dedup on  │
  │     (new_key, obs_date) keeping the last value; write atomically; record the  │
  │     dropped lastUpdate per file in a footer/metadata sidecar.                 │
  │ This is NON-destructive to obs VALUES (only the key string changes) and is    │
  │ idempotent. It is a DATA migration (touches production parquet) so it is      │
  │ FLAGGED here and must be run as its own gated, backed-up job — this strategy  │
  │ NEVER performs it implicitly.                                                 │
  └────────────────────────────────────────────────────────────────────────────┘
"""
from __future__ import annotations

from . import register
from .base import Strategy, Unit, Result
from .fetchers import get as get_fetcher


@register("giant_changed_units")
class GiantChangedUnits(Strategy):
    def detect_change(self, unit: Unit, unit_state: dict | None) -> str | None:
        """Cheap catalogue probe. The fetcher's current_vintage() hashes every flow's
        upstream last-update/version into one token; if it equals the stored one the
        whole catalogue is unmoved and we skip the (expensive) per-flow sweep. If the
        probe can't be determined (None) we fetch anyway (cadence-gated) — safe because
        the fetcher's per-flow merge dedups + never shrinks."""
        f = get_fetcher(unit.source_id)
        cur = f.current_vintage(unit) if hasattr(f, "current_vintage") else None
        self._cur = cur
        stored = (unit_state or {}).get("upstream_vintage")
        if cur is not None and stored is not None and cur == stored:
            return None
        return cur or "force"

    def run(self, unit: Unit, since: str | None) -> Result:
        r = get_fetcher(unit.source_id).update(unit, since)
        if r.new_vintage is None and getattr(self, "_cur", None):
            r.new_vintage = self._cur
        return r
