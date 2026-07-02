"""The connector contract -- the generalization of HF Data Library's single
pull-and-format script to many sources.

Adding a new data source = adding one folder under connectors/ that subclasses
Connector and implements discover() + fetch(). Nothing else in the system changes.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Iterable, Optional


@dataclass
class SeriesMeta:
    series_id: str                 # e.g. "worldbank:NY.GDP.MKTP.CD:USA"
    title: str
    frequency: str                 # 'D','W','M','Q','A','irregular'
    unit: Optional[str]
    geography: Optional[str]       # ISO code / region
    category: Optional[str]
    license_id: str                # may override the source default (3rd-party carve-outs)
    metadata: dict = field(default_factory=dict)


@dataclass
class Observation:
    series_id: str
    obs_date: date
    value: Optional[float]
    version: str = "raw"           # 'raw' | 'clean' (mirrors HF Raw/Clean tiers)
    flags: tuple = ()              # data-quality flags
    vintage_date: Optional[date] = None   # point-in-time / as-first-released (ALFRED-style)


class Connector(ABC):
    source_id: str                 # 'worldbank'
    license_id: str                # source-level license class (see core/licenses.py)
    schedule: str                  # cron string, per-source cadence
    attribution: str               # rendered credit line

    @abstractmethod
    def discover(self) -> list[SeriesMeta]:
        """List the series this source offers (feeds the catalog + search)."""

    @abstractmethod
    def fetch(self, since: Optional[date]) -> Iterable[tuple[SeriesMeta, list[Observation]]]:
        """Pull data (incrementally when `since` is given); yield (series, raw observations)."""
