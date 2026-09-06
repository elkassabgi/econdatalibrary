"""A source the licence record calls GATED must not be in SUPPORTED_SOURCES.

WHY THIS EXISTS. On 2026-09-06 four sources were listed as "stay gated" in the econ-updater
skill's licence summary while being SERVED: `damodaran` (24,687 rows), `fdic` (298,869),
`defillama` (24) and `frankfurter` (46). Three had a legitimate basis that the summary had simply
not caught up with — two written permissions and one recorded owner decision — and one
(frankfurter) is a real outstanding question. Nothing was wrong with the serving surface; what
was wrong is that the two records disagreed and only a human comparing them could tell.

That disagreement is expensive in BOTH directions and I hit both the same day: reading the stale
list, I twice concluded a live source was a licence breach and started to act on it; and had the
drift gone the other way, a genuinely restricted source could sit served with the summary
insisting it was gated. Prose does not hold the two in step. This does.

WHAT IT CHECKS. The canonical file `DATABASE_LICENSES_VERBATIM.md` carries a per-database summary
table whose last column is the decision tier. Any row whose tier says gated/restricted/needs
review, AND which carries no override marker (SERVE / permission / owner decision), must not be
in the deployed worker's `SUPPORTED_SOURCES`.

It deliberately reads the CANONICAL file, not the skill summary: the skill ages, the canonical
record is the one the rules say wins.
"""
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools.audit_schedule_coverage import supported_sources          # noqa: E402

CANON = os.path.join(ROOT, "DATABASE_LICENSES_VERBATIM.md")

# a tier that means "do not serve", unless the same cell also carries an override
_GATED = re.compile(r"keep gated|RESTRICTED|NEEDS HUMAN REVIEW|permission_required", re.I)
# an override recorded in the tier cell itself
_OVERRIDE = re.compile(r"\bSERVE\b|written permission|owner decision|un-?gated", re.I)
# The file holds FOUR pipe tables with different columns (a needs-attention table, the main
# per-database table, and two later ones). An earlier version of this parser matched rows in all
# of them and produced four false positives out of seven — reading a VERDICT cell as if it were a
# decision TIER, which flagged `faostat` and `worldbank` although the main table classifies both
# as redistributable-with-conditions. So anchor to the main table by its own header and stop at
# the first non-row line.
_MAIN_HEADER = "| Database | Provider | Final classification | Verdict | Tier |"
_ROW = re.compile(r"^\|\s*`([a-z0-9_]+)`\s*\|[^|]*\|[^|]*\|[^|]*\|([^|]*)\|")


def _main_table(src):
    i = src.find(_MAIN_HEADER)
    if i < 0:
        return []
    out = []
    for line in src[i:].split("\n")[2:]:          # skip header and its |---| separator
        if not line.startswith("|"):
            break
        out.append(line)
    return out


def gated_rows():
    src = open(CANON, encoding="utf-8").read()
    out = {}
    for line in _main_table(src):
        m = _ROW.match(line)
        if not m:
            continue
        name, tier = m.group(1), m.group(2).strip()
        if _GATED.search(tier) and not _OVERRIDE.search(tier):
            out[name] = tier
    return out


def test_the_parser_finds_the_main_table_at_all():
    """A parser that stopped matching would make this test vacuously green (R64)."""
    src = open(CANON, encoding="utf-8").read()
    rows = _main_table(src)
    assert len(rows) > 100, (
        f"the main per-database table yielded {len(rows)} rows — its header changed and this "
        f"check is no longer looking at anything"
    )
    parsed = [r for r in rows if _ROW.match(r)]
    assert len(parsed) > 100, "rows found but none parsed — the column shape changed"


def denylisted():
    """Ids the deployed worker gates with a 451. Comments stripped first (R137/R329).

    This is the ACTUAL gate. `SUPPORTED_SOURCES` only says the worker can resolve an id, so a
    source can be both supported and gated — `worldbank_pink` is exactly that, and reading only
    SUPPORTED_SOURCES reports it as a breach when it is correctly withheld.
    """
    src = open(os.path.join(ROOT, "api", "worker", "src", "denylist.ts"), encoding="utf-8").read()
    src = re.sub(r"//[^\n]*", "", src)
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return set(re.findall(r'"([a-z0-9_]+)"', src))


# OPEN QUESTIONS, named rather than hidden. Each is SERVED under a tier that says a human must
# decide, and each is small and specific. They are listed here so this test can be green on a
# known state while the list itself is the work item — adding to it must be a deliberate act,
# and it should only ever shrink.
OPEN_PENDING_OWNER = {
    "faostat": "47 rows. Classification is redistributable_attribution_noncommercial WITH a "
               "third-party-data carve-out, and the tier says NEEDS HUMAN REVIEW. Nothing in "
               "the email trail. The open question is whether the carve-out touches the 47.",
    "worldbank": "692 rows. redistributable_attribution_with_exceptions — CC BY 4.0 on the "
                 "Bank's own compiled data, but third-party-sourced indicators are excluded. "
                 "The open question is whether any of the 692 are third-party-sourced.",
    "frankfurter": "46 rows. Its own entry says the operative authority is the ECB, not "
                   "Frankfurter, and asks that the ECB policy be recorded in a dedicated "
                   "entry — that entry exists and is CLEARED. Needs a one-line owner call plus "
                   "attribution naming the European Central Bank.",
}


def test_no_gated_source_is_in_supported_sources():
    served = supported_sources() - denylisted() - set(OPEN_PENDING_OWNER)
    gated = gated_rows()
    assert gated, "no gated rows found at all — the tier vocabulary changed"
    bad = {s: t for s, t in gated.items() if s in served}
    assert not bad, (
        "these sources are SERVED but the canonical licence record still calls them gated, with "
        "no override recorded in the tier cell. Either the serving surface is wrong, or the "
        "record has not caught up with a permission or an owner decision — find out which and "
        "annotate the tier, do not silence this test:\n  "
        + "\n  ".join(f"{s}: {t}" for s, t in sorted(bad.items()))
    )


def test_the_open_list_is_still_accurate():
    """Every OPEN entry must still be served AND still gated in the record.

    Without this the list becomes a place things go to be forgotten: an entry that has since
    been decided, or gated, would sit here forever excusing a source that no longer needs it.
    """
    served, gated, deny = supported_sources(), gated_rows(), denylisted()
    stale = [s for s in OPEN_PENDING_OWNER
             if s not in served or s in deny or s not in gated]
    assert not stale, (
        "these are on the OPEN list but no longer match its premise — they have been decided, "
        "gated, or removed from serving. Delete them from OPEN_PENDING_OWNER: " + ", ".join(stale)
    )


def test_the_override_marker_is_recognised():
    """The known-good case must pass, or the check would just forbid every override."""
    assert not _GATED.search("CLEARED - re-host OK (attribution)")
    assert _GATED.search("RESTRICTED (keep gated)")
    assert _OVERRIDE.search("SERVE — written permission 2026-07-15 (was NEEDS HUMAN REVIEW)")
    assert _OVERRIDE.search("SERVE - owner decision (Ahmed, 2026-08-17)")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
