"""S5 — bulk_snapshot_if_changed.

For sources delivered as a whole bulk file / dump (e.g. faostat domains, cepii
trade & gravity panels). There is NO server-side date filter, so a row-level
delta is impossible: the only way to learn what changed is to download the whole
bulk again. What makes this affordable — and correct — is a cheap VINTAGE PROBE:

    detect_change(): read a cheap upstream signal (HTTP Last-Modified/ETag/
                     Content-Length, a manifest DateUpdate/FileRows/FileSize, a
                     published-date, or a content-hash) and compare it to the
                     stored vintage. Unchanged -> return None (skip the whole
                     expensive re-download). Changed / first run / undeterminable
                     -> return the new vintage token (re-fetch, cadence-gated).

    run(): re-download the bulk, parse it to LONG format
           (series_key, obs_date, value, [flag]), and publish via
           merge.merge_and_write in MERGE mode. Because merge dedups on
           (series_key, obs_date) (new row wins on revision) and never shrinks,
           a re-snapshot of the same data is an idempotent no-op, and a re-snapshot
           of a revised vintage UPDATES existing rows and EXTENDS with new ones —
           it can never DUPLICATE rows nor SHRINK the published file.

Identical fetcher contract to S1 overwrite_if_changed (each bulk source supplies
`updater/strategies/fetchers/<source_id>.py` exposing):

    current_vintage(unit) -> str | None      # cheap probe (None = undeterminable)
    update(unit, since)    -> Result          # full re-download + parse + merge

The difference from S1 is semantic and documented, not mechanical: S1 is for
small/medium whole-tables published as one artifact (overwrite mode), whereas S5
is for large multi-part bulk dumps where the honest publish is a MERGE (so a
vintage that restates history revises rather than replaces, and a partially
fetched multi-part bulk extends what it got without clobbering the rest).

THE DUPLICATION INVARIANT (the one rule a bulk strategy must never break): the
emitted `series_key` MUST be STABLE across snapshots. The vintage signal
(Last-Modified, DateUpdate, FileRows, a hash, ...) is used ONLY to GATE the
re-fetch and is stored in unit/sidecar state — it must NEVER be embedded in the
series_key. If it were, every new snapshot would mint brand-new keys and
merge_and_write would append instead of dedup, silently doubling the data. The
faostat/cepii/eurostat parsers already build vintage-free keys; any new bulk
fetcher must do the same.

The same strategy instance is reused for run(), so the vintage probed in
detect_change() is carried through to the Result without a second probe (and the
orchestrator only advances the stored vintage on a clean success).
"""
from __future__ import annotations

from . import register
from .base import Strategy, Unit, Result
from .fetchers import get as get_fetcher


@register("bulk_snapshot_if_changed")
class BulkSnapshotIfChanged(Strategy):
    def detect_change(self, unit: Unit, unit_state: dict | None) -> str | None:
        f = get_fetcher(unit.source_id)
        cur = f.current_vintage(unit) if hasattr(f, "current_vintage") else None
        self._cur = cur
        stored = (unit_state or {}).get("upstream_vintage")
        if cur is not None and stored is not None and cur == stored:
            return None             # upstream bulk unchanged -> skip the re-download entirely
        return cur or "force"       # changed / first run / undeterminable -> fetch (cadence-gated)

    def run(self, unit: Unit, since: str | None) -> Result:
        r = get_fetcher(unit.source_id).update(unit, since)
        # Carry the cheaply-probed vintage through so the orchestrator can persist
        # it (and skip next time) when the fetcher itself didn't return one.
        if r.new_vintage is None and getattr(self, "_cur", None):
            r.new_vintage = self._cur
        return r
