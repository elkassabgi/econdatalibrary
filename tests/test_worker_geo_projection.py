"""The per-geo projection (worker geoProjection.ts) must stay licence-safe and gated.

WHY THIS EXISTS. 2026-08-26: a user request for `worldbank:DT.DOD.DECT.CD:LMY` 404'd
while every byte it asked for was already served inside the CLEARED grouped
`worldbank_wdi:DT.DOD.DECT.CD` object. The fix is a worker-side projection (alias
3-part ids + `?geo=` resolve to the grouped object filtered to one economy) — chosen
over minting ~293k per-geo catalog ids because D1 sits near its 10 GB ceiling (#45 is
Ahmed's reserved capacity call) and `worldbank` is a DISPUTED licence whose
per-indicator third-party carve review exists only for its 3 legacy indicators.

That design is safe ONLY while three properties hold, so they are pinned here
mechanically (the test_licence_gate_matches_docs pattern — committed artifacts only,
no network, no store):

1. Every projection TARGET source's licence verdict is CLEARED in
   DATABASE_LICENSES_VERBATIM.md — the projection serves a subset of an
   already-served object, so the target's clearance is what carries it.
2. series.ts gates the CANONICAL spelling before serving the projection (R32:
   carve-outs must cover sibling ids — index.ts only gated the alias spelling).
3. Alias keys cover BOTH worldbank spellings (the R32 leak class: a carve keyed on
   `worldbank` alone once leaked ILO/IMF data through `worldbank_wdi`).

Plus a behavioral leg: the pure functions run under `node --experimental-strip-types`
(Node >= 22.6) against a fixture shaped like the real object (row ids WDI:<CODE>:<GEO>
— shape verified against production bytes 2026-08-26: 6,444 rows, LMY = 55, exactly
matching the World Bank API upstream). Skips when node is absent or too old.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEO_TS = os.path.join(ROOT, "api", "worker", "src", "geoProjection.ts")
SERIES_TS = os.path.join(ROOT, "api", "worker", "src", "series.ts")
LICENCES = os.path.join(ROOT, "DATABASE_LICENSES_VERBATIM.md")


def _strip_ts_comments(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"^\s*//.*$", "", src, flags=re.M)


def _projection_map() -> dict[str, str]:
    src = _strip_ts_comments(open(GEO_TS, encoding="utf-8").read())
    m = re.search(r"GEO_PROJECTION_SOURCES[^=]*=\s*\{(.*?)\}", src, flags=re.S)
    assert m, "GEO_PROJECTION_SOURCES not found in geoProjection.ts"
    body = m.group(1)
    pairs = re.findall(r"([A-Za-z0-9_]+)\s*:\s*\"([a-z0-9_]+)\"", body)
    # The parser must see EVERY entry — a quoted key or uppercase value would
    # silently evade the licence pin (reviewer finding 2, 2026-08-26).
    n_entries = len(re.findall(r":", body)) - body.count("://")  # ':' per entry, minus URL colons
    assert pairs and len(pairs) == n_entries, (
        f"parsed {len(pairs)} pairs but the map body has {n_entries} entries — "
        f"an entry is spelled in a way this parser cannot see: {body!r}")
    return dict(pairs)


def test_every_projection_target_is_licence_cleared():
    """Property 1: projection targets must be CLEARED, never DISPUTED/RESTRICTED."""
    doc = open(LICENCES, encoding="utf-8").read()
    for target in set(_projection_map().values()):
        # The summary table row for the source id: | `<id>` | ... verdict cells ...
        row = re.search(r"^\|\s*`" + re.escape(target) + r"`\s*\|.*$", doc, flags=re.M)
        assert row, f"no summary-table row for projection target '{target}' in the verbatim audit"
        cells = row.group(0)
        assert re.search(r"CLEARED", cells, flags=re.I), (
            f"projection target '{target}' is not CLEARED in DATABASE_LICENSES_VERBATIM.md: "
            f"{cells!r} — a projection of a non-cleared source serves gated bytes")
        assert not re.search(r"DISPUTED|RESTRICTED|NEEDS HUMAN REVIEW", cells, flags=re.I), (
            f"projection target '{target}' carries a blocking verdict: {cells!r}")


def test_series_ts_gates_the_canonical_spelling():
    """Property 2: the alias path must isGated() the canonical id before serving."""
    src = _strip_ts_comments(open(SERIES_TS, encoding="utf-8").read())
    assert re.search(r"isGated\(\s*alias\.canonical\s*\)", src), (
        "series.ts no longer gates alias.canonical — the R32 sibling-leak door is open")
    # And the projection serve happens only inside that guard's branch: the gate call
    # must appear BEFORE the canonical SELECT that enables the projection.
    gate_pos = src.find("isGated(alias.canonical")
    select_pos = src.find("bind(alias.canonical")
    assert 0 <= gate_pos < select_pos, (
        "the canonical gate check must precede the canonical catalog lookup")


def test_alias_map_covers_both_worldbank_spellings():
    """Property 3: R32 — sibling ids resolve identically."""
    m = _projection_map()
    assert m.get("worldbank") == "worldbank_wdi", m
    assert m.get("worldbank_wdi") == "worldbank_wdi", m
    for target in m.values():
        assert m.get(target) == target, (
            f"target '{target}' must map to itself so ?geo= works on the canonical id too")


FIXTURE = (
    "series_id,obs_date,value\n"
    "WDI:DT.DOD.DECT.CD:AFG,2006-12-31,979344507.8\n"
    "WDI:DT.DOD.DECT.CD:LMY,1970-12-31,64269166756.8\n"
    "WDI:DT.DOD.DECT.CD:LMY,1971-12-31,74872554416.8\n"
    "WDI:DT.DOD.DECT.CD:ZWE,2020-12-31,1.5\n"
)

HARNESS = """
import {{ geoAlias, filterGeoRows, normalizeGeoParam, GEO_CODE_ALIASES }} from {geo_url};
const assert = (c, m) => {{ if (!c) {{ console.error("FAIL: " + m); process.exit(1); }} }};
const a = geoAlias("worldbank:DT.DOD.DECT.CD:LMY");
assert(a && a.canonical === "worldbank_wdi:DT.DOD.DECT.CD" && a.geo === "LMY", "alias");
assert(geoAlias("worldbank:NY.GDP.MKTP.CD") === null, "2-part not alias");
assert(geoAlias("imf_weo:NGDP_RPCH:OEMDC") === null, "non-projection source");
assert(geoAlias("worldbank:X:TOOLONG") === null, "bad geo");
assert(normalizeGeoParam(" usa ") === "USA" && normalizeGeoParam("!") === null, "param");
// Legacy income-group codes resolve to the form the grouped object actually holds.
// Both entry points, because a user who gets the 404 will retry with ?geo=.
const xd = geoAlias("worldbank:SP.POP.TOTL:XD");
assert(xd && xd.geo === "HIC", "XD -> HIC on the alias path");
// The caller's own code must survive to the error messages: refusing a request for XD
// with "no rows for HIC" names a code the user never typed.
assert(xd && xd.requested === "XD", "alias must carry the REQUESTED code");
const usa = geoAlias("worldbank:SP.POP.TOTL:usa");
assert(usa && usa.geo === "USA" && usa.requested === "USA", "unmapped: requested == geo");
assert(normalizeGeoParam("xm") === "LIC", "XM -> LIC on the ?geo= path");
assert(normalizeGeoParam("XN") === "LMC" && normalizeGeoParam("XT") === "UMC", "XN/XT");
// A code that is NOT in the alias map must pass through untouched, or the map becomes a
// filter — the control that proves the translation is targeted, not blanket.
assert(normalizeGeoParam("XK") === "XK", "unmapped 2-char passes through");
assert(normalizeGeoParam("USA") === "USA", "3-char passes through");
// Every map VALUE must itself be a legal geo. Nothing else checks the right-hand
// side, so a bad entry would reach the row filter unvalidated.
for (const [k, v] of Object.entries(GEO_CODE_ALIASES)) {{
  assert(/^[A-Z0-9]{{2,3}}$/.test(k), "map key not a legal geo: " + k);
  assert(normalizeGeoParam(v) === v, "map value not a legal geo: " + v);
}}
const fx = {fixture};
const r = filterGeoRows(fx, "LMY");
assert(r.rows === 2 && r.text.trim().split(String.fromCharCode(10)).length === 3, "filter rows");
assert(r.text.startsWith("series_id,obs_date,value"), "header kept");
const miss = filterGeoRows(fx, "ZZ");
assert(miss.rows === 0 && miss.geos.join(",") === "AFG,LMY,ZWE", "miss lists real geos");
console.log("OK");
"""


def _node_supports_strip_types() -> bool:
    node = shutil.which("node")
    if not node:
        return False
    try:
        v = subprocess.run([node, "--version"], capture_output=True, text=True,
                           timeout=30).stdout.strip().lstrip("v")
        major, minor = (int(x) for x in v.split(".")[:2])
    except Exception:
        return False
    return (major, minor) >= (22, 6)


@pytest.mark.skipif(not _node_supports_strip_types(),
                    reason="node >= 22.6 (type stripping) not available")
def test_projection_functions_behave(tmp_path):
    geo_url = json.dumps("file:///" + GEO_TS.replace(os.sep, "/"))
    harness = tmp_path / "harness.ts"
    harness.write_text(HARNESS.format(geo_url=geo_url, fixture=json.dumps(FIXTURE)),
                       encoding="utf-8")
    p = subprocess.run(["node", "--experimental-strip-types", str(harness)],
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 0 and "OK" in p.stdout, f"stdout={p.stdout!r} stderr={p.stderr!r}"
