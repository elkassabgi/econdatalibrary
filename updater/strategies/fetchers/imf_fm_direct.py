"""IMF Fiscal Monitor — DIRECT from api.imf.org (flow FM, agency IMF.FAD).

Thin wrapper: the registry resolves fetchers/<source_id>.py, so each IMF dataset
needs its own module. All behaviour lives in _imf_direct.py — see that file for why
these are NEW source ids rather than replacements for the DBnomics-era imf_fm.

The direct feed is the thinnest of the family — the relay-era imf_fm holds 1,356
series and the publisher's current FM scope carries ~9% of them. That gap is why
this build sat RESERVED as a "switching to a feed that serves LESS" decision;
Ahmed's 2026-08-06 ruling ("refresh to match publisher... I need a clean database")
makes the publisher's current scope the target, exactly as it did for MCDREO (57%).
Nothing is switched here regardless: this is a NEW parallel id and the legacy
imf_fm stays served-frozen until the Class A retirement pipeline reaches it.

Agency is IMF.FAD, not IMF.STA — read from the live dataflow catalogue 2026-08-06
(FM v5.0.0), the same agency as PSBS. Ignore FM_2025_OCT_VINTAGE: dated *_VINTAGE
flows are point-in-time snapshots, not the maintained dataset.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "FM", "IMF.FAD", "imf_fm_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
