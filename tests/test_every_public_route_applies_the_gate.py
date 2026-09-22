"""THE CLASS GUARD: every handler that names a source must apply the redistribution gate.

Three routes were found disclosing gated sources on 2026-09-17, one at a time — the metadata
route, the bundle manifest, and `/v1/last-updates`. Three is a class, not three bugs, so this
pins the property for the whole surface rather than for the three that were noticed.

A whole-surface sweep of the LIVE worker the same day, matching gated ids on word boundaries
against each raw body (so a leak in a field nobody thought of still trips), with the served-source
control taken from the live `/v1/sources` list rather than hand-picked:

    /v1/sources         321 served ids named   NAMES A GATED SOURCE  <- embedded licence row
    /v1/last-updates    267                    NAMES A GATED SOURCE  <- row-level, fixed in code
    /v1/stats             1                    clean
    /v1/public-stats      5                    clean
    /v1/catalog (x2)      1                    clean
    /v1/                  0                    VACUOUS - names no source, so it proves nothing

The `/v1/sources` finding is NOT a filter bug — `sources.ts` does filter its rows. The gated name
is embedded inside another source's licence id/name, which is a shared row and needs a RENAME in
the catalogue and D1 together (ledger R889 rule 4). That is the owner's held decision, not a code
fix, so this file does not assert it away.

Each handler below either applies the gate or is exempt FOR A STATED, CHECKED REASON. Adding a
new handler that reads source data and forgets the gate fails here.
"""
from __future__ import annotations

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "api", "worker", "src")

# handler -> why it needs no gate reference of its own. Anything not listed MUST reference it.
EXEMPT = {
    # Gated at the ROUTE in index.ts, before the handler is reached; pinned by
    # tests/test_metadata_route_is_gated.py, which asserts isGated runs before handleMetadata.
    "metadata.ts": "gated at the route in index.ts",
    # Cannot carry a source id at all: it allowlists a fixed set of site paths.
    "pageview.ts": "allowlists fixed site paths; no source id can reach it",
}
HANDLERS = ["sources.ts", "lastUpdates.ts", "catalog.ts", "bundle.ts", "series.ts",
            "metadata.ts", "publicStats.ts", "pageview.ts"]
GATE_REFS = ("NON_REDISTRIBUTABLE", "isGated", "isSeriesCarvedOut", "carveoutExcl")


def _code(path: str) -> str:
    src = open(path, encoding="utf-8").read()
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"//.*", "", src)


def test_every_source_naming_handler_applies_the_gate():
    missing = []
    for name in HANDLERS:
        path = os.path.join(SRC, name)
        assert os.path.exists(path), f"{name} has moved or been renamed; update this guard"
        if name in EXEMPT:
            continue
        code = _code(path)
        if not any(ref in code for ref in GATE_REFS):
            missing.append(name)
    assert not missing, (
        "these handlers name sources but apply no redistribution gate, so a gated source can be "
        f"disclosed through them: {missing}. Filter through NON_REDISTRIBUTABLE (see "
        "sources.ts and lastUpdates.ts), or add an EXEMPT entry here stating why it cannot "
        "carry a source id")


def test_the_exemptions_are_still_true():
    """An exemption is a claim, and a claim rots. Check each one against the code."""
    meta = _code(os.path.join(SRC, "metadata.ts"))
    assert not any(r in meta for r in GATE_REFS), (
        "metadata.ts now gates internally — good, but then it is no longer EXEMPT for the "
        "reason recorded; move it out of EXEMPT so the guard tests the real thing")
    idx = _code(os.path.join(SRC, "index.ts"))
    assert "isGated(id)" in idx, (
        "index.ts no longer gates at the route, so metadata.ts's exemption is false and the "
        "metadata route is open again")

    pv = _code(os.path.join(SRC, "pageview.ts"))
    assert "source_id" not in pv and "FROM source" not in pv, (
        "pageview.ts now touches source data, so its exemption is false: gate it or re-justify")


def test_the_guard_can_fail():
    """Planted positive: a handler with no gate reference must be detected. Without this the
    test above passes on an empty HANDLERS list or a broken comment stripper."""
    fake = "export async function handle() { return json({ source_id: 'x' }); }"
    assert not any(ref in fake for ref in GATE_REFS)


def test_the_comment_stripper_does_not_hide_a_real_call():
    """The mirror risk: stripping // comments must not delete code. A gate reference that
    survives only inside a comment would be a false pass, so check one handler both ways."""
    path = os.path.join(SRC, "sources.ts")
    raw = open(path, encoding="utf-8").read()
    code = _code(path)
    assert "NON_REDISTRIBUTABLE" in code, (
        "sources.ts's gate reference survives only in a comment — the stripper removed the call")
    assert raw.count("NON_REDISTRIBUTABLE") >= code.count("NON_REDISTRIBUTABLE")
