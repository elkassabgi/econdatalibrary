"""Strategy adapters (S1-S6). Each implements the Strategy contract in base.py.

Registry of strategy name -> class is populated as adapters are implemented:
  S1 overwrite_if_changed, S2 extend_by_date, S3 sdmx_delta,
  S4 giant_changed_units, S5 bulk_snapshot_if_changed, S6 manual_vintage.
"""
from .base import Strategy, Unit, Result, cadence_due  # noqa: F401

REGISTRY: dict[str, type] = {}


def register(name):
    def deco(cls):
        cls.name = name
        REGISTRY[name] = cls
        return cls
    return deco


def get(name) -> Strategy:
    if name not in REGISTRY:
        raise KeyError(f"strategy {name!r} not implemented (have: {sorted(REGISTRY)})")
    return REGISTRY[name]()


# Import implemented strategy modules so their @register decorators run.
# (Add new strategies here as their adapters land.)
from . import extend_by_date       # noqa: E402,F401  (S2)
from . import overwrite_if_changed  # noqa: E402,F401  (S1)
from . import sdmx_delta            # noqa: E402,F401  (S3)
from . import giant_changed_units   # noqa: E402,F401  (S4)
from . import bulk_snapshot_if_changed  # noqa: E402,F401  (S5)
from . import manual_vintage        # noqa: E402,F401  (S6)
