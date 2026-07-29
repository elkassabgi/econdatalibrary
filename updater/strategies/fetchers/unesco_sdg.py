"""UNESCO UIS SDG 4 indicators — direct from api.uis.unesco.org.

734,662 observations across 100,997 series in the same state as unesco_natmon:
present in the local store, absent from the catalog, absent from R2, denylisted, and
unregistered — hosted nowhere. Data stops in 2020.

Same UIS terms (CC BY-SA 4.0, CLEARED) and the same key grammar; see _uis.py.
"""
from ._uis import current_vintage, update as _update            # noqa: F401

SOURCE = "unesco_sdg"


def update(unit, since):
    return _update(SOURCE, unit, since)
