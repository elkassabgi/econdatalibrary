"""gen_denylist.py — regenerate api/worker/src/denylist.ts from catalog.db.

Single source of truth for the redistribution gate. Historically the worker's
NON_REDISTRIBUTABLE set was hand-maintained and drifted from the site, which
gates on license.reservable — so a source could read "metadata only" on the
page yet still serve its .csv (observed 2026-07-14: transparency_ti and 141
other reservable=0 sources were downloadable). This script makes the gate
DB-derived so the two can never disagree again:

    NON_REDISTRIBUTABLE = { every source_id with license.reservable = 0 }
                          ∪ LEGACY_KEEP            (never silently un-gate)
                          − RELEASED               (deliberate, human-authorised un-gates)
                          − GRANTED_EXCEPTIONS     (written permission on file)

THE FLOOR IS READ, NOT TYPED. LEGACY_KEEP is every id the COMMITTED denylist.ts already
gates — real and phantom alike — parsed from that file at import time. Until 2026-09-08 it
was a hand-typed list of ids with a paragraph of history behind each pin. The ids are no
longer named anywhere in this repository (owner's order), and the mechanism never needed
them: the property the floor exists for is that a regeneration can never gate LESS than the
file it replaces unless a human removes an id on purpose. That property is exactly
"new set ⊇ old set − RELEASED − GRANTED_EXCEPTIONS", which needs the old file, not a list.

Why a floor at all — the two lessons behind it, kept because they ARE the mechanism:

  * A source purged from the catalogue has no `source` row, so the reservable=0 scan cannot
    see it. Verified on the first regeneration after the 2026-07-23 purge: the purged ids DID
    leak out of the gate. The floor pins them so a later re-ingest can never land un-gated.
  * A licence row can be SHARED (R117). Setting reservable=0 on `cc-by-4.0` to gate one
    disputed source would gate the 36 others on that row. The floor is the per-SOURCE gate
    for a source whose verdict is disputed while its licence row is not.

Un-gating is a decision, never a side effect. Add the id to RELEASED in the same commit as
the evidence (the written grant, or the CLEARED verdict in DATABASE_LICENSES_VERBATIM.md),
and only AFTER the served data has been rebuilt from the publisher and verified complete —
the barro_lee / norgesbank / unsdg / vdem pattern — so the R167 flag-first trap (451 -> 404)
is measured absent. A source that merely has a written grant belongs in GRANTED_EXCEPTIONS.

The SERIES_CARVEOUTS block and everything after the Set literal are carried over from the
committed file VERBATIM. A template regression once silently WIPED carve-outs that had been
added to denylist.ts by hand (commit be939627f dropped what 5fc56cea1 added), so the template
no longer owns that block. main() still refuses to write if any carve-out the committed file
protects — or the minimum set below — is missing from the output.

Run:  python -m core.gen_denylist        (from the econ repo root)
Then redeploy the worker.  Re-run whenever license flags change.
"""
from __future__ import annotations

import os
import re
import sqlite3

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DB = os.path.join(ROOT, "data", "catalog.db")
OUT = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")

# Sources that carry WRITTEN redistribution permission — kept downloadable even
# though their license row is still the conservative NEEDS-REVIEW default.
# Provenance lives in the generated header below; keep the two in sync.
GRANTED_EXCEPTIONS = {
    "kof_globalization",  # Prof. Jan-Egbert Sturm (KOF director), 2026-07-06: NC academic re-hosting
    "comtrade",           # UN Comtrade, 2026-07-07: free branch, holdings must stay <= 100k records
    # wid — WID.world, 2026-07-06 GRANTED (educational), Alice (info@wid.world) 2026-07-27:
    # "Yes, you can use the data for educational purpose", plus the site's own
    # rel="license" declaring CC BY-NC-SA 4.0. Audit: CONFIRMED, "CLEARED - re-host OK
    # (non-commercial, attribution, SHARE-ALIKE)". It sat on the floor because the
    # licence was undeclared when that pin was written; it is declared now, and a
    # written grant is exactly what GRANTED_EXCEPTIONS is for.
    # Moved 2026-07-29 on Ahmed's decision, only after the derive COMPLETED and was
    # verified: catalog 2,465,197 == R2 CSVs 2,465,197, missing 0. Un-gating earlier
    # would have served 404s for whatever had not yet been derived.
    "wid",
}

# Ids a human has deliberately released from the floor (module docstring). Empty between
# decisions: an entry lives here for exactly one regeneration, because once the id is out of
# the committed file the floor no longer carries it and the entry is dead weight.
RELEASED: set[str] = set()

# The minimum carve-outs the template must always know, independent of the committed file.
# They protect SERVED sources: the World Bank's IMF-sourced CPI and ILO-sourced unemployment
# indicators, on BOTH ids that republish them (live leak confirmed 2026-07-22:
# worldbank_wdi:SL.UEM.TOTL.ZS served 401 while worldbank's copy was gated).
MINIMUM_CARVEOUTS = {
    "worldbank": ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
    "worldbank_wdi": ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
}

_SET_RX = re.compile(
    r"NON_REDISTRIBUTABLE[^=]*=\s*new\s+Set\s*(?:<[^>]*>)?\s*\(\s*\[(.*?)\]\s*\)", re.S)


def _strip_ts_comments(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"//[^\n]*", "", src)


def _read_out() -> str:
    if not os.path.exists(OUT):
        return ""
    with open(OUT, encoding="utf-8") as f:
        return f.read()


def committed_gate(src: str | None = None) -> set[str]:
    """Every id the committed denylist.ts gates — real and phantom alike."""
    src = _read_out() if src is None else src
    m = _SET_RX.search(src)
    if not m:
        return set()
    return set(re.findall(r'"([^"]+)"', _strip_ts_comments(m.group(1))))


def committed_carveouts(src: str | None = None) -> dict[str, list[str]]:
    """source id -> indicator codes, from the committed SERIES_CARVEOUTS block."""
    src = _read_out() if src is None else src
    m = re.search(r"SERIES_CARVEOUTS[^=]*=\s*\{(.*?)\n\};", src, re.S)
    if not m:
        return {}
    body = _strip_ts_comments(m.group(1))
    out: dict[str, list[str]] = {}
    for key in re.findall(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", body, re.M):
        block = re.search(re.escape(key) + r"\s*:\s*\[(.*?)\]", body, re.S)
        if block:
            out[key] = re.findall(r'"([^"]+)"', block.group(1))
    return out


def committed_tail(src: str | None = None) -> str:
    """Everything after the Set literal's closing `])` in the committed file, verbatim
    (the carve-out block and the helper exports). DEFAULT_TAIL when there is no file."""
    src = _read_out() if src is None else src
    m = _SET_RX.search(src)
    if not m:
        return DEFAULT_TAIL
    tail = src[m.end():]
    return tail if tail.strip() else DEFAULT_TAIL


# Ids that were explicitly gated and must never be dropped even if they are not (or no
# longer) reservable=0 in the DB — phantom/renamed/purged ids included. Unioning this floor
# guarantees a regeneration never UN-gates anything the committed file blocked. It is read
# from that file; there is no list to maintain and nothing here to name.
LEGACY_KEEP: set[str] = committed_gate() - RELEASED

HEADER = '''// ---------------------------------------------------------------------------
// src/denylist.ts  --  redistribution gate (HTTP 451 + hidden from catalog).
//
// GENERATED FILE -- do not edit by hand. Regenerate with:
//     python -m core.gen_denylist        (then redeploy the worker)
//
// The set below is DERIVED FROM catalog.db so it can never drift from the site,
// which gates on license.reservable. A source is blocked iff its license is not
// verified-redistributable (license.reservable = 0), minus the granted
// exceptions below, plus a legacy safety floor so a regeneration never silently
// un-gates a previously-blocked source.
//
// GRANTED EXCEPTIONS (written permission on file -> kept downloadable):
//   kof_globalization -- Prof. Jan-Egbert Sturm (KOF director, index co-author),
//     2026-07-06: non-commercial academic re-hosting. Honor: NC use only; cite
//     "KOF, ETH Zurich"; link back to the official KOF Globalisation Index page;
//     no commercial resale/sublicensing; KOF may request removal.
//   comtrade -- UN Comtrade, 2026-07-07: our holdings sit in the free branch
//     ("up to 100,000 records"). STANDING GUARD: comtrade holdings must STAY
//     <= 100,000 records; growing past that leaves the free branch and requires
//     re-gating. Cite "UN Comtrade" + link back.
//
// NonCommercial-but-free sources are governed by license.reservable in the DB;
// if any appear here it is because their license row is reservable=0
// (unverified) -- fix the license row and regenerate, don't special-case them.
// ---------------------------------------------------------------------------

'''

# Used only when no committed denylist.ts exists (a fresh checkout without the worker).
# Carries the minimum carve-outs and the helper exports the worker expects.
DEFAULT_TAIL = ''';

/** The source id is the part of a series_id before the first ':'. */
export function seriesSource(seriesId: string): string {
  const i = seriesId.indexOf(":");
  return i < 0 ? seriesId : seriesId.slice(0, i);
}

export function isNonRedistributable(seriesId: string): boolean {
  return NON_REDISTRIBUTABLE.has(seriesSource(seriesId));
}

/**
 * Series-level carve-outs. The SOURCE is redistributable, but specific indicators
 * within it embed third-party data the source's licence does not cover, so those
 * series are gated individually. Keyed by source id -> indicator codes (the part
 * of a series_id between the first and second ':'). Hand-maintained; keep in sync
 * with permission records (held privately).
 *
 * worldbank: GDP (NY.GDP.MKTP.CD) is World-Bank-compiled and served; CPI
 * (FP.CPI.TOTL.ZG) is IMF-sourced and unemployment (SL.UEM.TOTL.ZS) is ILO-sourced
 * -- WB terms bar redistributing third-party data, so those two are gated.
 */
export const SERIES_CARVEOUTS: Readonly<Record<string, readonly string[]>> = {
  worldbank: ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
  // worldbank_wdi carries the SAME third-party indicators as worldbank; the carve-out
  // was once keyed only on `worldbank`, so they were SERVED through worldbank_wdi
  // (confirmed LIVE 2026-07-22). Same WB terms apply whichever id carries the data.
  worldbank_wdi: ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
};

function seriesIndicator(seriesId: string): string {
  const p = seriesId.split(":");
  return p.length > 1 ? p[1] : "";
}

/** True if this specific series is a third-party carve-out of a served source. */
export function isSeriesCarvedOut(seriesId: string): boolean {
  const carved = SERIES_CARVEOUTS[seriesSource(seriesId)];
  return carved ? carved.includes(seriesIndicator(seriesId)) : false;
}

/** Combined data gate: the whole source is non-redistributable, OR this specific
 *  series is a third-party carve-out. Every DATA endpoint must use this. */
export function isGated(seriesId: string): boolean {
  return isNonRedistributable(seriesId) || isSeriesCarvedOut(seriesId);
}

/** Escape LIKE metacharacters (`_` matches ANY single character in SQL LIKE). Use with ESCAPE '\\'. */
export function likeEscape(s: string): string {
  return s.replace(/[\\\\%_]/g, (c) => "\\\\" + c);
}

/** LIKE prefixes (`<source>:<indicator>:`) for SQL exclusion of THREE-part carved ids. */
export const SERIES_CARVEOUT_LIKE: readonly string[] = Object.entries(SERIES_CARVEOUTS)
  .flatMap(([src, inds]) => inds.map((ind) => likeEscape(`${src}:${ind}:`)));

/** Exact ids for TWO-part carved series (`<source>:<indicator>`, no third segment). */
export const SERIES_CARVEOUT_EXACT: readonly string[] = Object.entries(SERIES_CARVEOUTS)
  .flatMap(([src, inds]) => inds.map((ind) => `${src}:${ind}`));
'''


def required_carveouts(committed: str) -> dict[str, list[str]]:
    """Everything the committed file protects, plus the template minimum."""
    req = {k: sorted(v) for k, v in MINIMUM_CARVEOUTS.items()}
    for k, v in committed_carveouts(committed).items():
        req[k] = sorted(set(req.get(k, [])) | set(v))
    return req


def main() -> None:
    committed = _read_out()
    floor = committed_gate(committed) - RELEASED
    carve_required = required_carveouts(committed)
    tail = committed_tail(committed)

    c = sqlite3.connect(DB)
    reservable0 = {
        r[0] for r in c.execute(
            "SELECT s.source_id FROM source s "
            "JOIN license l ON l.license_id = s.license_id "
            "WHERE l.reservable = 0"
        )
    }
    all_sources = {r[0] for r in c.execute("SELECT source_id FROM source")}
    c.close()

    gated = (reservable0 | floor) - GRANTED_EXCEPTIONS
    real = sorted(s for s in gated if s in all_sources)
    phantom = sorted(s for s in gated if s not in all_sources)  # kept but flagged

    lines = [HEADER, "export const NON_REDISTRIBUTABLE: ReadonlySet<string> = new Set(["]
    for sid in real:
        lines.append(f'  "{sid}",')
    if phantom:
        lines.append("  // legacy/phantom ids (not currently in the catalog; kept as a safety floor):")
        for sid in phantom:
            lines.append(f'  "{sid}",')
    lines.append("])")
    text = "\n".join(lines) + tail

    # REGENERATION GUARDS (fail closed BEFORE writing).
    # 1. Every carve-out the committed file protects, and the template minimum, must be
    #    present in the output — a template edit can never silently drop a written refusal.
    assert "export const SERIES_CARVEOUTS" in text, "REFUSING to write: no SERIES_CARVEOUTS block"
    block = text.split("export const SERIES_CARVEOUTS", 1)[1].split("};", 1)[0]
    for src, inds in carve_required.items():
        assert re.search(r"(?m)^\s*" + re.escape(src) + r"\s*:", block), (
            f"REFUSING to write: carve-out source '{src}' missing from the "
            f"generated SERIES_CARVEOUTS (template regression — see 5fc56cea1)")
        for ind in inds:
            assert f'"{ind}"' in block, (
                f"REFUSING to write: carve-out {src}:{ind} missing from the "
                f"generated SERIES_CARVEOUTS")
    # 2. The floor: nothing the committed file gated may drop out of the gate unless it was
    #    released on purpose (RELEASED) or carries a written grant (GRANTED_EXCEPTIONS).
    kept = set(real) | set(phantom)
    dropped = sorted(floor - kept - GRANTED_EXCEPTIONS)
    assert not dropped, (
        f"REFUSING to write: {len(dropped)} previously gated id(s) would fall out of the gate. "
        f"Un-gating is a decision — add the id to RELEASED with its evidence, never a side effect.")
    # 3. A release must actually take effect, or it is a stale entry that will confuse the next run.
    stale = sorted(RELEASED & kept)
    assert not stale, f"{len(stale)} RELEASED id(s) are still gated by the reservable=0 scan"
    # 4. The granted ones must NOT be gated.
    for must_serve in GRANTED_EXCEPTIONS:
        assert must_serve not in kept, must_serve

    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)

    print(f"reservable=0 sources:        {len(reservable0)}")
    print(f"floor (committed file):      {len(floor)}")
    print(f"released this run:           {len(RELEASED)}")
    print(f"granted exceptions (remove): {sorted(GRANTED_EXCEPTIONS)}")
    print(f"-> NON_REDISTRIBUTABLE size: {len(real)} real + {len(phantom)} phantom = {len(kept)}")
    print(f"wrote {OUT}")
    print("sanity OK: grants excluded; floor intact; carve-outs carried")


if __name__ == "__main__":
    main()
