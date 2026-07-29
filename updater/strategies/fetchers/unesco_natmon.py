"""UNESCO UIS National Monitoring (education) — direct from api.uis.unesco.org.

1,876,322 observations across 98,664 series that were held in the local store and
served to NOBODY: no catalog rows, nothing on R2, a denylist entry, and no registry
unit — so no freshness check could even see them. Data stops in 2020; UIS is still
publishing (every theme lastUpdate 02/09/2026).

Licence settled from our own canonical audit rather than re-derived: UIS terms at
https://databrowser.uis.unesco.org/terms-and-conditions, CC BY-SA 4.0, verified
word-for-word and recorded CLEARED for re-hosting with attribution. The terms are
publisher-wide ("The work of the UIS is licensed under..."), so they cover this
dataset as well as the five UIS databases enumerated in the audit.

Key grammar and its proof live in _uis.py.
"""
from ._uis import current_vintage, update as _update            # noqa: F401

SOURCE = "unesco_natmon"


def update(unit, since):
    return _update(SOURCE, unit, since)
