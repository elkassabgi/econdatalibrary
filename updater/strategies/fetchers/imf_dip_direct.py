"""IMF Direct Investment Positions by Counterpart Economy — DIRECT from api.imf.org
(flow DIP, agency IMF.STA). Successor to the Coordinated Direct Investment Survey (CDIS).

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_cdis` holds 97,723 relay-era series — catalogued, SERVED, frozen:
no fetcher, no registry entry. IMF RENAMED the dataset: the /dataflow catalogue carries no
CDIS flow; it carries DIP (IMF.STA, v12.0.1) whose own name reads "Direct Investment
Positions by Counterpart Economy (formerly CDIS)" — probe-confirmed 2026-08-05, the fourth
confirmed rename in this family (DOTS→IMTS, CPIS→PIP, PSBSFAD→PSBS, CDIS→DIP).

MEASURED SHAPE: 5 dims, ALL codelisted — COUNTERPART_COUNTRY, COUNTRY, DV_TYPE, FREQUENCY,
INDICATOR. Alphabetical key order puts the counterpart FIRST and the table dims mid-key, the
PIP class: the serving resolver anchors by POSITION, never substring. Size measured at the
proof run; grain by the #45 D1 arithmetic when the count lands.

Adding `imf_dip_direct` takes nothing from `imf_cdis`; supersession is #46, reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "DIP", "IMF.STA", "imf_dip_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
