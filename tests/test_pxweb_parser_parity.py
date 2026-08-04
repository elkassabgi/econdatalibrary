"""Every PxWeb source that keeps TWO date parsers must keep them identical.

WHY THIS EXISTS. Each source has an ingester (`jobs/ingest_X.py`, used for backfills and for
anything run by hand) and a fetcher (`updater/strategies/fetchers/X.py`, which is what the 06:00
UTC tick actually calls). Most fetchers delegate to the ingester's `parse_date`, so they cannot
drift. Four keep their own copy: scb, statfin, ssb, stat_slovenia.

On 2026-08-04 scb's two copies disagreed, and the consequence was not a cosmetic mismatch:

  * `Tid` values like '2011-2012' (multi-year window) and '2025V01' (week; V = Swedish vecka)
    parsed in neither copy at first, so the real time axis scored a ZERO parse-rate and the
    resolver fell through to `Region` — municipality codes 0114..2584 — producing 87,358 rows
    dated to years 114..2026.
  * The grammars were then added to the INGESTER only. That is the path which does not run
    nightly, so scb kept producing 0 rows for those tables every night while a live hand-run
    looked like proof that it was fixed (R333).

A divergence here is worse than a plain bug because the two paths write DIFFERENT (series_key,
obs_date) rows for the same upstream observation. Dedup is on that pair, so the two grains never
collide, both survive, and merge's never-shrink guard cannot see the duplication — which is how
ons_uk reached 20,198,302 rows for 10,099,151 observations (R22, R331).

This test is a real gate, not decoration: it FAILED against scb before the fix, on '2011-2012',
'1998-2002' and '2025V01'.
"""
from __future__ import annotations
import importlib

import pytest

# (source, ingester module, fetcher module). Only sources whose FETCHER defines its own parser;
# the delegating ones (hagstofa, bfs, dst, stat_latvia) cannot drift by construction. If you add
# a `def parse_date` to one of those fetchers, add it here too — or better, delegate instead.
PAIRS = [
    ("scb", "jobs.ingest_scb", "updater.strategies.fetchers.scb"),
    ("statfin", "jobs.ingest_statfin", "updater.strategies.fetchers.statfin"),
    ("ssb", "jobs.ingest_ssb", "updater.strategies.fetchers.ssb"),
    ("stat_slovenia", "jobs.ingest_stat_slovenia",
     "updater.strategies.fetchers.stat_slovenia"),
]

# Real PxWeb time values plus the codes that MUST NOT be treated as one. '0114' and '2584' are
# Swedish municipality codes and are in here deliberately: both parsers may accept them as bare
# years (the callers apply the sanity bound), but they must at least AGREE, because a
# disagreement there is what silently swaps the time axis between the two paths.
CASES = [
    "2023", "1968", "2023M01", "2009M01", "2023-01", "2023K1", "2023K4", "2023Q1",
    "2023H2", "2023W01", "2025V01", "2011-2012", "1998-2002", "2023-01-15",
    "0114", "2584", "9999", "2023T1", "", "garbage",
]


def _parser(mod_name: str):
    mod = importlib.import_module(mod_name)
    for attr in ("parse_date", "_parse_date"):
        fn = getattr(mod, attr, None)
        if fn is not None:
            return fn, attr
    raise AssertionError(f"{mod_name} exposes neither parse_date nor _parse_date")


def _call(fn, value):
    try:
        return fn(value)
    except Exception as exc:                                  # noqa: BLE001
        return f"RAISE({type(exc).__name__})"


@pytest.mark.parametrize("source,ing_mod,fet_mod", PAIRS, ids=[p[0] for p in PAIRS])
def test_ingester_and_fetcher_parse_dates_identically(source, ing_mod, fet_mod):
    ing, ing_attr = _parser(ing_mod)
    fet, fet_attr = _parser(fet_mod)

    disagreements = []
    for value in CASES:
        a, b = _call(ing, value), _call(fet, value)
        if a != b:
            disagreements.append(f"  {value!r}: {ing_mod}.{ing_attr}={a!r} "
                                 f"but {fet_mod}.{fet_attr}={b!r}")

    assert not disagreements, (
        f"{source}: the backfill parser and the NIGHTLY parser disagree on "
        f"{len(disagreements)} of {len(CASES)} values:\n" + "\n".join(disagreements) +
        "\n\nThis is not cosmetic. The two paths will write different (series_key, obs_date) "
        "rows for the same observation; they will not collide under dedup, so both survive and "
        "never-shrink cannot see the duplication. Fix BOTH copies, or make the fetcher import "
        "the ingester's parse_date. See R331/R333."
    )


def test_scb_parses_the_grammars_that_cost_87358_rows():
    """A regression guard on the two specific grammars, in both copies.

    Not folded into the parity test above: parity would also pass if BOTH copies lost these
    grammars together, and that is precisely the state that dated 87,358 rows to years
    114..2026. Parity says "they agree"; this says "they are right".
    """
    for mod_name in ("jobs.ingest_scb", "updater.strategies.fetchers.scb"):
        fn, _ = _parser(mod_name)
        # multi-year window -> the year it OPENS
        assert fn("2011-2012") is not None and fn("2011-2012").year == 2011, mod_name
        assert fn("1998-2002") is not None and fn("1998-2002").year == 1998, mod_name
        # week, Swedish 'V' for vecka. ISO week 1 of 2025 legitimately starts 2024-12-30.
        assert fn("2025V01") is not None, mod_name
        assert fn("2025V01").isoformat() == "2024-12-30", mod_name
        # and the monthly form must still win over the window form
        assert fn("2023-01") is not None and fn("2023-01").month == 1, mod_name
