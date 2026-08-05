"""IMF International Trade in Goods by partner country — DIRECT from api.imf.org
(flow IMTS, agency IMF.STA). Successor to Direction of Trade Statistics (DOTS).

Thin wrapper: the registry resolves fetchers/<source_id>.py, so each IMF dataset needs its own
module. All behaviour lives in _imf_direct.py — including why these are NEW source ids rather
than replacements for the DBnomics-era ones.

WHY THIS ONE EXISTS. `imf_dot` holds 101,000 relay-era series (catalogued, SERVED, frozen — no
fetcher and no registry entry), the largest frozen block after imf_ifs. IMF RENAMED the dataset:
its /dataflow catalogue has NO flow whose id contains DOT, but carries IMTS, agency IMF.STA,
v1.0.0, whose own description reads "The International trade in goods by partner country
dataset (formerly Direction of Trade Statistics (DOTS))". Read from the catalogue on
2026-08-05, not guessed — the rename class (PSBSFAD→PSBS, PCTOT→CTOT, GS_*, HPD) is ledger
R74/R75, and the first probe "no DOT flow exists" was ITSELF wrong until a known-present
control (BOP) exposed an attribute-order bug in my regex, twice (R338/R329).

MEASURED SHAPE, from the API's own responses (never assumed): 4 dims, all codelisted —
COUNTRY.INDICATOR.COUNTERPART_COUNTRY.FREQUENCY (that key order returns data; FREQUENCY-first
returns an empty body) — 359 country codes, e.g. `IMTS:USA.MG_CIF_USD.ABW.A`. USA alone answers
2,061 series keys for startPeriod=2023, so the full flow is plausibly several hundred thousand
series: this belongs in the updater-heavy matrix beside the other _direct giants, NOT in the
daily run's budget.

Adding `imf_imts_direct` takes nothing away from `imf_dot`; whether it supersedes it is the
reserved re-key question (#46), settled by the ingest's measured series count when it runs.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "IMTS", "IMF.STA", "imf_imts_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
