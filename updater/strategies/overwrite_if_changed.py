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
from .base import Strategy, Unit, Result, probe_token
from .fetchers import get as get_fetcher


@register("overwrite_if_changed")
class OverwriteIfChanged(Strategy):
    def detect_change(self, unit: Unit, unit_state: dict | None) -> str | None:
        f = get_fetcher(unit.source_id)
        cur = f.current_vintage(unit) if hasattr(f, "current_vintage") else None
        cur = probe_token(cur)      # finalize's "date-tail" placeholder is no token (base.DATE_TAIL)
        self._cur = cur
        # equal to the stored vintage, after a clean pass, within an admitted stamp's max age (base)
        if self._probe_matches(f, cur, unit_state):
            return None             # upstream unchanged -> skip the re-fetch entirely
        return cur or "force"       # changed / first run / undeterminable -> fetch (cadence-gated)

    def run(self, unit: Unit, since: str | None) -> Result:
        f = get_fetcher(unit.source_id)
        # The PRE-RUN probe, when the fetcher returned none - never over NO_STAMP (Strategy._carry_probe).
        return self._carry_probe(f.update(unit, since), f)
