"""S3 - sdmx_delta.

SDMX 2.1 and PxWeb sources, refreshed by a per-dataflow date tail: each fetcher
pulls only observations newer than the stored last_obs (SDMX `?startPeriod=` /
`?updatedAfter=`, or a PxWeb time-dimension value selection) and merges them in.

Functionally identical to extend_by_date (same fetcher contract: update(unit, since)
-> Result, honest Tally/finalize status, per-series cursors, atomic never-shrink
merge). Registered as its own strategy name so the registry's sdmx_delta sources
resolve, and so the giants' S4 engine can later specialize it per-flow.
"""
from __future__ import annotations

from . import register
from .extend_by_date import ExtendByDate


@register("sdmx_delta")
class SdmxDelta(ExtendByDate):
    pass
