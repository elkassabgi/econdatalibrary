"""IMF Historical Public Debt — DIRECT from api.imf.org (flow HPD, agency IMF.FAD).

Repairs a source that was SERVED but had no updater entry at all: 191 series in the
catalog and in the worker's supported list, downloadable, and never once attempted.
The flow was not discontinued, it was RENAMED (HPDD -> HPD); reading the exact-id
miss as "gone" is what kept it unwired (ledger R75).

All behaviour lives in _imf_mapped.py. The code map that turns IMF's current
vocabulary back into our published ids (AFG->AF, G63G_S13_POFYGDP->GGXWDG_GDP) is
derived from value agreement by tools/prove_direct_repair.py and read from
_imf_maps/imf_hpdd.json — never hand-typed. Proven: 191 of 191 ids preserved.
"""
from . import _imf_mapped as _m

SOURCE = "imf_hpdd"


def current_vintage(unit):
    return _m.vintage(SOURCE)


def update(unit, us):
    return _m.run(SOURCE)
