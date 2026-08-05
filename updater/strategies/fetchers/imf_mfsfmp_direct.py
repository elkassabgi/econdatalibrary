"""IMF Monetary and Financial Statistics — Financial Markets and Positions (MFS_FMP) — DIRECT from
api.imf.org (agency IMF.STA). One of the four flows IMF SPLIT the former MFS dataset into.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_mfs` holds 88,271 relay-era series — catalogued, SERVED, frozen:
no fetcher, no registry entry. IMF did not rename MFS, it SPLIT it: the /dataflow catalogue
carries no MFS flow; it carries the MFS_* family — MFS_DC, MFS_MA, MFS_OFC, MFS_FMP (all
IMF.STA; the dated *_VINTAGE snapshots of each are deliberately ignored). Probe-confirmed
2026-08-05, and current_vintage() returned MFS_FMP:3.0.0 live the same day. One source id per
flow, the FSI-trio precedent (imf_fsibsis/fsic/fsicdm).

Size and key shape are measured at the proof run, never assumed; serving grain is decided by
the #45 D1 arithmetic when the counts land (cycle 5 of the econ-updater loop).

Adding `imf_mfsfmp_direct` takes nothing from `imf_mfs`; supersession is #46, reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "MFS_FMP", "IMF.STA", "imf_mfsfmp_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
