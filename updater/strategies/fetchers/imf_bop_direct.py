"""IMF Balance of Payments — DIRECT from api.imf.org (flow BOP, agency IMF.STA).

Thin wrapper: the registry resolves fetchers/<source_id>.py, so each IMF dataset needs its own
module. All behaviour lives in _imf_direct.py — including why these are NEW source ids rather
than replacements for the DBnomics-era ones.

WHY THIS ONE EXISTS. `imf_bop` holds 99,636 relay-era series and has no fetcher at all, so it has
never auto-updated: it is the single largest frozen block in the library after the imf_* family's
re-key question. IMF publishes it as dataflow BOP, agency IMF.STA, v21.0.0 — an EXACT id match,
read from IMF's own /dataflow catalogue rather than guessed. The agency matters: agency ids are
NOT uniform across IMF datasets (FDI is IMF.MCM, WEO is IMF.RES, FM/WORLD IMF.FAD), and assuming
IMF.STA produced four spurious 404s in an earlier pass.

COVERAGE IS MEASURED BEFORE THIS IS TREATED AS A SUCCESSOR. `jobs/ingest_imf_direct.py`'s own
header records that direct is SMALLER than the relay copy for some flows (MCDREO 57%, FM 9%),
which is why those two are excluded from this batch — switching a source to a feed serving less
is a reserved decision. Adding `imf_bop_direct` takes nothing away from `imf_bop`; what the new
source does or does not supersede is settled by the ingest's own series count, recorded when it
runs, not by assuming parity.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "BOP", "IMF.STA", "imf_bop_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
