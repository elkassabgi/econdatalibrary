"""S1 — overwrite_if_changed.

For whole-table sources (small/medium CSV/XLSX/zip/SDMX that are re-published or
re-estimated each release). Change-detection is a cheap vintage probe
(ETag/Last-Modified/commit-SHA/version/catalog-timestamp); the re-fetch + atomic
publish happens ONLY when upstream actually moved. Each S1 fetcher exposes:

    current_vintage(unit) -> str | None      # cheap probe (None = undeterminable)
    update(unit, since)    -> Result          # full re-fetch + merge.merge_and_write

The same detect_change instance is reused for run(), so the probed vintage is
carried through to the Result without a second probe.
"""
from __future__ import annotations

from . import register
from .base import Strategy, Unit, Result
from .fetchers import get as get_fetcher


@register("overwrite_if_changed")
class OverwriteIfChanged(Strategy):
    def detect_change(self, unit: Unit, unit_state: dict | None) -> str | None:
        f = get_fetcher(unit.source_id)
        cur = f.current_vintage(unit) if hasattr(f, "current_vintage") else None
        self._cur = cur
        stored = (unit_state or {}).get("upstream_vintage")
        if cur is not None and stored is not None and cur == stored:
            return None             # upstream unchanged -> skip the re-fetch entirely
        return cur or "force"       # changed / first run / undeterminable -> fetch (cadence-gated)

    def run(self, unit: Unit, since: str | None) -> Result:
        r = get_fetcher(unit.source_id).update(unit, since)
        if r.new_vintage is None and getattr(self, "_cur", None):
            r.new_vintage = self._cur
        return r
