"""Every source that HOLDS a third-party indicator must gate it. (Ledger R32.)

R32 is "a carve-out keyed on one source id does not cover the others", and it names the family:
worldbank vs worldbank_wdi / _esg / _pip / _wgi. It has now been violated twice by the same
mechanism -- a human adds the indicator to one source id and the sibling that republishes the
identical upstream data keeps serving it:

  * 2026-07-22: `worldbank_wdi` found serving ILO unemployment and IMF CPI ungated. Fixed by
    adding one key to SERIES_CARVEOUTS.
  * 2026-08-30: `worldbank_esg` found serving 178 ILO unemployment series ungated, advertised
    cc-by-4.0 / reservable / commercial_ok. It was named in R32 and left out of the July fix.

Both times the gap was invisible because nothing enumerated from the DATA. A reviewer reading
SERIES_CARVEOUTS sees a tidy list; only asking the catalogue "who else holds this indicator?"
finds the sibling. So this test does exactly that, and it is the check that makes the rule
mechanical rather than remembered.

Deliberately NOT a list of expected sources: hardcoding "worldbank, worldbank_wdi,
worldbank_esg" would pass forever while a NEW sibling appears -- which is the very failure
being guarded. The population is derived from the catalogue each run.

Skips when catalog.db is absent (CI without the 11.9 GB catalogue), because a test that
silently passes on missing data is worse than one that says it did not run.
"""
import os
import re
import sqlite3

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DENYLIST = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")
CATALOG = os.path.join(ROOT, "data", "catalog.db")


def _strip(js):
    return re.sub(r"//[^\n]*", "", re.sub(r"/\*.*?\*/", "", js, flags=re.S))


def _denylisted():
    src = open(DENYLIST, encoding="utf-8").read()
    m = re.search(r"NON_REDISTRIBUTABLE[^=]*=\s*new\s+Set\s*\(\s*\[(.*?)\]\s*\)", src, re.S)
    return set(re.findall(r'"([^"]+)"', _strip(m.group(1)))) if m else set()


def _carveouts():
    src = open(DENYLIST, encoding="utf-8").read()
    m = re.search(r"SERIES_CARVEOUTS[^=]*=\s*\{(.*?)\n\};", src, re.S)
    assert m, "SERIES_CARVEOUTS not found in denylist.ts"
    body = _strip(m.group(1))
    out = {}
    for key in re.findall(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", body, re.M):
        block = re.search(re.escape(key) + r"\s*:\s*\[(.*?)\]", body, re.S)
        if block:
            out[key] = re.findall(r'"([^"]+)"', block.group(1))
    assert out, "SERIES_CARVEOUTS parsed to nothing"
    return out


def _count_holding(con, source, indicator):
    """Rows of `source` whose SECOND segment is exactly `indicator`. Anchored, and indexed.

    The obvious form -- `series_id LIKE '%:<ind>:%'` -- is WRONG and this test's first version
    used it, which is R129's unanchored match inside a test about unanchored matching. It
    matches a segment anywhere in the id, so worldbank_pink's generic commodity names
    (`gold`, `copper`, `zinc`, `nickel`) collided with unrelated sources and reported
    `ksh_stadat holds 0 series of copper`. The contradiction -- "holds" a count of zero -- is
    what exposed it: the finder and the counter disagreed, so one of them was broken.

    Anchoring on `<source>:<indicator>` and bounding by the source's PK range fixes both the
    correctness and the speed (it rides the primary key instead of scanning 13.5M rows).
    """
    return con.execute(
        "SELECT COUNT(*) FROM series WHERE series_id >= ? AND series_id < ? "
        "AND (series_id LIKE ? ESCAPE '\\' OR series_id = ?)",
        (source + ":", source + ";",
         _like_escape(source + ":" + indicator + ":") + "%",
         source + ":" + indicator),
    ).fetchone()[0]


def _like_escape(s):
    return re.sub(r"([\\%_])", r"\\\1", s)


@pytest.mark.skipif(not os.path.exists(CATALOG), reason="catalog.db not present")
def test_every_source_holding_a_carved_indicator_gates_it():
    carve = _carveouts()
    deny = _denylisted()
    indicators = sorted({i for inds in carve.values() for i in inds})

    con = sqlite3.connect("file:%s?mode=ro" % CATALOG.replace("\\", "/"), uri=True)
    try:
        sources = [r[0] for r in con.execute("SELECT DISTINCT source_id FROM series")]
        leaks = []
        for ind in indicators:
            for source in sorted(sources):
                if source in deny:
                    continue                       # whole source refused, covered
                if ind in carve.get(source, []):
                    continue                       # explicitly carved, covered
                n = _count_holding(con, source, ind)
                if n:
                    leaks.append((source, ind, n))

        # Positive control: the guard must be able to SEE a holding it is not told about.
        # Without this, "no leaks" is equally consistent with a probe that finds nothing --
        # which is precisely how the first version of this test passed on broken matching.
        control = _count_holding(con, "worldbank", "NY.GDP.MKTP.CD")
        assert control > 0, (
            "positive control found 0 rows for worldbank:NY.GDP.MKTP.CD — the probe cannot "
            "detect a holding, so its 'no leaks' result would be vacuous"
        )
    finally:
        con.close()

    assert not leaks, (
        "Third-party indicator served UNGATED — ledger R32, third occurrence.\n"
        + "\n".join(
            "  %s holds %d series of %s with no carve-out and no denylist entry"
            % (s, n, i) for s, i, n in leaks
        )
        + "\nAdd the source to SERIES_CARVEOUTS in api/worker/src/denylist.ts."
    )


def test_carveout_like_prefixes_cover_both_id_shapes():
    """The `<src>:<ind>:` prefix cannot match a two-part id; the exact form must exist.

    `worldbank_wdi:FP.CPI.TOTL.ZG` and `worldbank_pink:aluminum` have no third segment, so
    their SQL exclusion matched 0 rows for as long as it existed. The JS gate still covered
    them, so this was defence-in-depth rather than an open door — but worldbank_pink's seven
    metals are REFUSED-in-writing and its own note anticipates the source being un-gated.
    """
    src = open(DENYLIST, encoding="utf-8").read()
    assert "SERIES_CARVEOUT_EXACT" in src, (
        "denylist.ts must export SERIES_CARVEOUT_EXACT: the LIKE prefix ends in ':' and so "
        "cannot match a two-part carved id"
    )
    assert "ESCAPE" in open(
        os.path.join(ROOT, "api", "worker", "src", "sql.ts"), encoding="utf-8"
    ).read(), "carve-out LIKE terms need ESCAPE — '_' is a wildcard and source ids contain it"


def test_sources_endpoint_hides_denylisted_sources():
    """A source we refuse to serve must not be advertised in /v1/sources.

    `worldbank_pink` was listed there with a full licence block while every path to its data
    answered 451 — a browsable entry nobody can obtain, which is the "metadata-only" listing
    Ahmed's standing rule forbids (host it fully, or do not list it). Measured live
    2026-08-30: /v1/sources total 322, of which one is denylisted; both
    /v1/catalog?source=worldbank_pink and /v1/series/worldbank_pink:aluminum.csv returned 451.

    Comments in catalog.ts and bundle.ts already ASSERTED this filtering happened, so the code
    contradicted its own documentation (R125). This pins the code, not the prose.
    """
    src = open(os.path.join(ROOT, "api", "worker", "src", "sources.ts"), encoding="utf-8").read()
    assert "NON_REDISTRIBUTABLE" in src, (
        "sources.ts must filter NON_REDISTRIBUTABLE — otherwise /v1/sources advertises "
        "sources whose every data path returns 451"
    )
    # The closing brace is INDENTED (`  });`), so an anchored `\n\}\);` never matches and the
    # assertion below would fail for the wrong reason — passing or failing on the locator
    # rather than on the thing under test (R488). Allow leading whitespace, and prove the
    # locator works before trusting what it finds.
    body = re.search(r"return json\(\{(.*?)\n\s*\}\);", src, re.S)
    assert body, "could not locate the /v1/sources response body"
    assert "sources:" in body.group(1), (
        "located a block that is not the response body — the locator is wrong, so any verdict "
        "it produces is meaningless"
    )
    assert "rows.length" not in body.group(1) and "rows.map" not in body.group(1), (
        "the response still emits the UNFILTERED `rows`; the denylist filter is computed but "
        "not used — a variable assigned and ignored is the same defect as no filter at all"
    )


def test_worldbank_esg_specifically_is_carved():
    """Regression pin for the 2026-08-30 leak: 178 ILO series served ungated.

    Live evidence at the time, using the contrast that isolates the gate (isGated runs before
    requireDownloadAuth, so a gated id 451s pre-auth):
        worldbank:SL.UEM.TOTL.ZS:AGO      -> 451
        worldbank_esg:SL.UEM.TOTL.ZS:AGO  -> 401
    """
    assert "SL.UEM.TOTL.ZS" in _carveouts().get("worldbank_esg", []), (
        "worldbank_esg must carve SL.UEM.TOTL.ZS (ILO-sourced, 178 series)"
    )
