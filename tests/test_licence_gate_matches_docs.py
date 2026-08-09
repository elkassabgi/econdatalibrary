"""The licence verdict and the serving surface must agree — mechanically, at commit time.

WHY THIS EXISTS. Ahmed, 2026-08-09: "why do we need to address this issue every week?" Because
every licence guard we had was PROSE. `DATABASE_LICENSES_VERBATIM.md` records a verdict per
database, `REDISTRIBUTION_EMAIL_TRAIL.md` records what each publisher granted or refused, and
both were applied to `denylist.ts` and `util.ts` BY HAND. Nothing failed when they drifted.

Two classes that stopped recurring the moment they became mechanical: the db.nomics ban (a
PreToolUse hook + tests/test_dbnomics_ban.py) and the registry count (R347 +
tests/test_registry_count_guard.py). Licence compliance never got the same treatment, so it came
back roughly weekly — R8 (WTO refused data still served through a phantom-id gate), R29
(metadata-only listings), R408 (the email trail said `ei_statreview` "stays gated pending ...
remove from denylist.ts" while it was absent from denylist.ts and downloadable all along).

These tests turn that class into a red check on the commit that causes it. They read only
COMMITTED artifacts, so they run anywhere — no 9 GB catalog.db, no 600 GB store.

They do NOT replace the live probe (R9: never assert live posture from code alone). They assert
that the code we ship agrees with the verdicts we recorded. Drift between the code and the
RUNNING system is a different check, and the audit workflow covers it.
"""
from __future__ import annotations

import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LICENCES = os.path.join(ROOT, "DATABASE_LICENSES_VERBATIM.md")
UTIL_TS = os.path.join(ROOT, "api", "worker", "src", "util.ts")
DENYLIST_TS = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")

# Tiers that mean "must NOT be publicly downloadable without a written grant".
BLOCKING_TIER = re.compile(r"RESTRICTED|NEEDS HUMAN REVIEW", re.I)
# A written permission overrides the public terms; the doc says so in the Tier cell.
GRANTED_TIER = re.compile(r"WRITTEN PERMISSION", re.I)


def _strip_ts_comments(src: str) -> str:
    """Remove // and /* */ comments BEFORE any quote matching.

    R0.4, learned the hard way: a quoted phrase inside a comment flips quote-pairing parity and
    silently drops real entries. util.ts is mostly comment by volume — every removed source has
    a paragraph explaining why — and those paragraphs are full of apostrophes and quoted terms.
    """
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"//[^\n]*", "", src)


def _string_array(src: str, decl: str) -> list[str]:
    """Extract the quoted strings of a single `decl` array/Set literal."""
    body = _strip_ts_comments(src)
    i = body.find(decl)
    if i < 0:
        pytest.fail(f"{decl} not found — this guard is parsing the wrong file or shape")
    # Skip the EMPTY `[]` of a type annotation: `SUPPORTED_SOURCES: readonly string[] = [`.
    # Taking the first '[' matched `string[]`, so the scanner closed immediately and returned
    # zero entries — and zero entries made every assertion below pass vacuously.
    start = i
    while True:
        start = body.find("[", start)
        if start < 0:
            pytest.fail(f"no array literal found for {decl}")
        if body[start + 1:].lstrip()[:1] != "]":
            break
        start += 1
    depth, end = 0, -1
    for j in range(start, len(body)):
        if body[j] == "[":
            depth += 1
        elif body[j] == "]":
            depth -= 1
            if depth == 0:
                end = j
                break
    assert end > start, f"unterminated array literal for {decl}"
    return re.findall(r'"([^"]+)"', body[start:end])


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


def _verdict_index() -> dict[str, str]:
    """source_id -> Tier cell, from the '## Per-database index' markdown table."""
    text = _read(LICENCES)
    start = text.find("## Per-database index")
    assert start > 0, "the per-database index heading moved; fix this guard"
    end = text.find("## Per-provider detail", start)
    table = text[start: end if end > 0 else len(text)]
    out: dict[str, str] = {}
    for line in table.splitlines():
        m = re.match(r"\|\s*`([^`]+)`\s*\|(.*)\|\s*$", line)
        if not m:
            continue
        cells = [c.strip() for c in m.group(2).split("|")]
        if cells:
            out[m.group(1).strip()] = cells[-1]
    assert len(out) > 50, f"parsed only {len(out)} verdict rows — the table shape changed"
    return out


def served_sources() -> list[str]:
    return _string_array(_read(UTIL_TS), "SUPPORTED_SOURCES")


def gated_sources() -> set[str]:
    return set(_string_array(_read(DENYLIST_TS), "NON_REDISTRIBUTABLE"))


# ---------------------------------------------------------------------------------------------
# KNOWN DRIFT BASELINE — a ratchet, not an excuse. Measured 2026-08-09 when this guard was first
# written. Each entry is real drift that predates the guard; the guard's job is to stop the set
# GROWING while these are worked off. Removing a name here is the fix; adding one requires a
# reason in the same commit. The suite fails if the set grows, and fails if a name that has been
# resolved is still listed (so a stale exemption cannot hide a regression).
#
# Why these are drift and not violations: the per-database index Tier column was never updated
# when written permissions landed. damodaran, bundesbank, defillama and idb all have grants
# quoted verbatim in REDISTRIBUTION_EMAIL_TRAIL.md; ei_statreview likewise (and its S&P Platts
# carve-out is satisfied by construction — all 127 of its metrics are physical energy quantities,
# zero price series). The fix for each is to record the grant in the index, not to gate the data.
SERVED_TIER_DRIFT: frozenset[str] = frozenset({
    "bundesbank",      # GRANTED 2026-07-15 (inquiry 2026/005812) — index still says NEEDS REVIEW
    "damodaran",       # GRANTED 2026-07-15 by Prof. Damodaran — index not updated
    "defillama",       # GRANTED 2026-07-16 (Rick) — index still says RESTRICTED
    "ei_statreview",   # GRANTED 2026-07-20 (Gemma, EI) with conditions — index not updated
    "idb",             # GRANTED 2026-07-15 (IDB Open Data, CC-BY 4.0) — index not updated
    "faostat",         # DISPUTED verdict; the 13 fao_* domains carry verified-nc licence rows
    "frankfurter",     # ECB-derived rates; verdict never finalised
    "worldbank",       # third-party-series carve-out IS implemented in SERIES_CARVEOUTS
    "worldbank_pink",  # same carve-out family as worldbank
})

# Denylist ids matching neither the served surface nor a verdict row. The wto_* entries are the
# R8 purge's residue: WTO data is gone from the store and the catalogue (verified 2026-08-09),
# so these block nothing — they are inert, but they are also how a phantom-id gate looked healthy
# while refused data served, so they are recorded rather than quietly deleted.
DENYLIST_UNMATCHED: frozenset[str] = frozenset({
    "central_banks", "fraser_efw", "fred", "fred_releases", "fsi", "gus", "ibge",
    "imf_dbnomics", "ine_spain", "pxweb_bfs", "qog", "sdmx_nso", "sipri_polity",
    "social_progress", "spi", "stat_austria", "unesco_sci", "unicef", "vdem",
    "who_gho", "wiid", "wto_bat_bv_m", "wto_bat_bv_x", "wto_hs_0010", "wto_hs_0015",
    "wto_hs_0020", "wto_hs_0025", "wto_hs_0030", "wto_hs_0040",
})


def test_the_parsers_actually_see_the_artifacts():
    """The control, welded in — because a broken parse makes every other test here pass.

    R0.4: "when a probe reports ABSENCE, run it against something known PRESENT — and a FAILED
    control VOIDS the run." The first version of this file returned ZERO served sources (it
    matched the `[]` of the `readonly string[]` annotation instead of the array), so the two
    substantive assertions below passed while checking nothing. That is R316/R338 for the third
    time. The floors and the named controls make that failure loud instead of green.
    """
    served, gated, verdicts = served_sources(), gated_sources(), _verdict_index()
    assert len(served) > 250, (
        f"parsed only {len(served)} entries from SUPPORTED_SOURCES; the live API serves ~312. "
        f"The parser is broken and every assertion in this file is vacuous.")
    assert len(gated) > 10, f"parsed only {len(gated)} denylist entries; parser is broken"
    assert len(verdicts) > 150, f"parsed only {len(verdicts)} verdict rows; parser is broken"
    for known_live in ("noaa", "eia", "abs", "census"):
        assert known_live in served, (
            f"control failed: {known_live} is verified live but absent from the parsed "
            f"SUPPORTED_SOURCES — the parse is wrong, so no result here is trustworthy.")


# Served sources with NO per-database row and NO provider-family coverage — the genuine
# "nobody adjudicated this" list, measured 2026-08-09 (169 before family coverage was taken into
# account, 20 after). Each needs a row in the per-database index. Ratchet: this set may shrink,
# never grow. Several already have their answer recorded elsewhere and only need transcribing —
# gpi/gti/ppi/etr are the IEP set cleared by the 2026-07-06 CC BY-NC-SA web-form grant, adb
# carries licence id cc-by-3.0-igo-adb, unsdg carries un-data-terms.
UNADJUDICATED: frozenset[str] = frozenset({
    "adb", "bfs", "cso", "dst", "etr", "gapminder", "gpi", "gti", "hagstofa", "harvard_atlas",
    "norgesbank", "ons_uk", "ppi", "scb", "ssb", "stat_estonia", "stat_latvia", "stat_slovenia",
    "statfin", "unsdg",
})


def _provider_headings() -> set[str]:
    """Lowercased '### <provider>' headings from the per-provider detail section.

    The index is PER-DATABASE, but whole families were added later under one provider grant:
    every `unctad_*` id rides UNCTAD's hub-wide terms, every `imf_*_direct` id rides the IMF
    terms. Those are genuinely adjudicated — just not row-by-row — so family coverage counts.
    """
    text = _read(LICENCES)
    start = text.find("## Per-provider detail")
    return {h.strip().lower() for h in re.findall(r"^### (.+)$", text[start:], re.M)}


def _is_covered(sid: str, verdicts: dict[str, str]) -> bool:
    if sid in verdicts:
        return True
    fam = sid.split("_")[0]
    if fam in verdicts:                      # e.g. fao_ga -> `fao`, unctad_x -> `unctad`
        return True
    heads = _provider_headings()
    return any(fam == h or h.startswith(fam + " ") or fam in h.split() for h in heads)


def test_every_served_source_has_a_recorded_licence_verdict():
    """A source nobody adjudicated must not be on the serving surface.

    This is the freedomhouse/NO_VERDICT class: data arrives, gets catalogued, gets served, and
    the licence question is never asked because no artifact demands an answer.
    """
    verdicts = _verdict_index()
    missing = sorted(s for s in served_sources()
                     if not _is_covered(s, verdicts) and s not in UNADJUDICATED)
    fixed = sorted(n for n in UNADJUDICATED if _is_covered(n, verdicts))
    assert not fixed, (
        f"these are listed in UNADJUDICATED but now HAVE a verdict: {fixed}. Remove them from "
        f"the baseline so the ratchet keeps its teeth.")
    assert not missing, (
        f"{len(missing)} source(s) are in SUPPORTED_SOURCES with NO row in the per-database "
        f"index of DATABASE_LICENSES_VERBATIM.md: {missing}. Adjudicate each one and add its "
        f"row, or remove it from the serving surface. Serving data whose licence nobody "
        f"recorded is exactly what R8 forbids."
    )


def test_restricted_sources_are_gated_not_merely_documented():
    """RESTRICTED / NEEDS HUMAN REVIEW + served + not in the denylist = a live exposure.

    R408: the email trail asserted `ei_statreview` was gated; it was absent from denylist.ts and
    downloadable. A sentence in a document is not a gate.
    """
    verdicts = _verdict_index()
    gated = gated_sources()
    leaking = []
    for s in served_sources():
        tier = verdicts.get(s, "")
        if BLOCKING_TIER.search(tier) and not GRANTED_TIER.search(tier) and s not in gated:
            if s in SERVED_TIER_DRIFT:
                continue
            leaking.append((s, tier))
    resolved = sorted(n for n in SERVED_TIER_DRIFT
                      if not BLOCKING_TIER.search(verdicts.get(n, "")) or n in gated)
    assert not resolved, (
        f"these names are exempted in SERVED_TIER_DRIFT but are no longer drifting: {resolved}. "
        f"Delete them from the baseline — a stale exemption is how a regression hides.")
    assert not leaking, (
        "these sources are SERVED, their recorded tier says do not distribute, and they are "
        "NOT in denylist.ts NON_REDISTRIBUTABLE — so they are downloadable right now:\n  "
        + "\n  ".join(f"{s}: {t}" for s, t in leaking)
        + "\nEither gate them (add to denylist.ts and redeploy) or record the written "
          "permission that clears them. 'Gate now, purge later' is not an end state (R8)."
    )


def test_denylist_has_no_entries_that_are_not_real_sources():
    """A gate on a phantom id protects nothing.

    R8's WTO incident: the deny-gate carried ids that did not match the served facets, so the
    refused data flowed while the gate looked healthy. An entry that matches neither the served
    surface nor a recorded verdict is almost certainly such a phantom.
    """
    verdicts = _verdict_index()
    served = set(served_sources())
    phantom = sorted(s for s in gated_sources()
                     if s not in served and s not in verdicts and s not in DENYLIST_UNMATCHED)
    gone = sorted(n for n in DENYLIST_UNMATCHED if n not in gated_sources())
    assert not gone, (
        f"these names are exempted in DENYLIST_UNMATCHED but are no longer in denylist.ts: "
        f"{gone}. Delete them from the baseline.")
    assert not phantom, (
        f"{len(phantom)} denylist entr(ies) match neither SUPPORTED_SOURCES nor any recorded "
        f"verdict: {phantom}. Either the id is stale (remove it, and say so) or it is misspelled "
        f"— in which case the data it was meant to block is NOT blocked."
    )
