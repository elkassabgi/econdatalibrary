"""IMF Portfolio Investment Positions by Counterpart Economy — DIRECT from api.imf.org
(flow PIP, agency IMF.STA). Successor to the Coordinated Portfolio Investment Survey (CPIS).

Thin wrapper: the registry resolves fetchers/<source_id>.py; all behaviour lives in
_imf_direct.py.

WHY THIS ONE EXISTS. `imf_cpis` holds 100,783 relay-era series — catalogued, SERVED, frozen:
no fetcher, no registry entry. IMF RENAMED the dataset: the /dataflow catalogue carries no
CPIS flow; it carries PIP (IMF.STA, v5.0.0) whose own description reads "formerly Coordinated
Portfolio Investment Survey, or CPIS" — read from the catalogue 2026-08-05, the same R74/R75
rename class as DOTS→IMTS and PSBSFAD→PSBS.

NAME COLLISION, DISARMED IN WRITING: the library ALREADY serves a source id `pip` — the World
Bank's Poverty and Inequality Platform. IMF's flow id and the World Bank's product share four
letters and NOTHING else. This source id carries the imf_ prefix precisely so the two can
never be conflated (the R171 bea/BEA provider-collision class); never abbreviate this source
to "pip" in code, tasks, or messages.

MEASURED SHAPE: 7 dims, ALL codelisted (ACCOUNTING_ENTRY, COUNTERPART_COUNTRY,
COUNTERPART_SECTOR, COUNTRY, FREQUENCY, INDICATOR, SECTOR) — titles resolve fully. Sizing is
measured at the proof run, not assumed; with two counterpart-style dims the flow may exceed
IMTS's 472,234 series, and the serving grain is decided by the #45 D1 arithmetic when the
count lands.

Adding `imf_pip_direct` takes nothing from `imf_cpis`; supersession is the reserved re-key
question (#46).
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "PIP", "IMF.STA", "imf_pip_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
