"""Every fetcher that carries a COPY of its ingester's period parser must agree with it.

THE CLASS (two members found 2026-08-08, same night):
  - insee_bdm: the ingester grew semester/bimester support; the fetcher's copy did not —
    the two recovered flows would have merged 0 rows on every daily tick, forever.
  - eurostat: the fetcher's ISO-week branch tested s[6] == "W" but W sits at index 5
    ("2024-W05"), so the branch was DEAD and every weekly period parsed to None while
    the ingester parsed it fine.

A dropped period is silent: the merge sees "no new rows", the run books an honest-looking
no_change/ok, and the store freezes at its ingest snapshot. Nothing reddens. So the pairing
is pinned here mechanically: walk both parsers over a battery of every period format any of
these publishers emit; any divergence is a test failure naming the exact string.

If a fetcher legitimately must differ from its ingester (e.g. a deliberate annual Dec-31 vs
Jan-1 convention change), encode that in the battery expectations here WITH the reason —
do not just delete the pair.
"""
from __future__ import annotations

import importlib
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# (fetcher module, fn) <-> (ingester module, fn). Only pairs where the fetcher duplicates
# the ingester's parser with the same signature/semantics belong here.
PAIRS = [
    (("updater.strategies.fetchers.insee_bdm", "_parse_period"),
     ("jobs.ingest_insee_bdm", "parse_period")),
    (("updater.strategies.fetchers.ecb", "_parse_period"),
     ("jobs.ingest_ecb", "parse_period")),
    (("updater.strategies.fetchers.eurostat", "_parse_period"),
     ("jobs.ingest_eurostat", "parse_period")),
]

# Every period shape any of these publishers emit, plus malformed controls.
BATTERY = [
    "2024", "1999",                                  # annual
    "2024-01", "2024-12",                            # monthly YYYY-MM
    "2024-Q1", "2024-Q4",                            # quarterly
    "2024-S1", "2024-S2",                            # semester
    "2024-B1", "2024-B3", "2024-B6",                 # bimester (INSEE BDM)
    "2024-W01", "2024-W05", "2024-W53",              # ISO week (eurostat)
    "2024-07-15", "2024-02-29",                      # daily
    "2024-S3", "2024-B7", "2024-B0", "2024-W00",     # out-of-range ordinals
    "garbage", "", "20x4-01",                        # malformed
]


def _fn(mod, name):
    return getattr(importlib.import_module(mod), name)


def test_every_fetcher_parser_copy_agrees_with_its_ingester():
    failures = []
    for (fm, ff), (jm, jf) in PAIRS:
        fetcher, ingester = _fn(fm, ff), _fn(jm, jf)
        for s in BATTERY:
            got_f, got_j = fetcher(s), ingester(s)
            if got_f != got_j:
                failures.append(f"{fm.rsplit('.', 1)[-1]}: {s!r} -> "
                                f"ingester={got_j} fetcher={got_f}")
    assert not failures, (
        "fetcher parser copies have drifted from their ingesters (a dropped period "
        "silently freezes a store):\n  " + "\n  ".join(failures))


def test_the_pair_list_is_not_stale():
    """If someone deletes a fetcher's parser copy (e.g. converts it to a real import),
    this test should be UPDATED, not silently skipped: importing pins existence."""
    for (fm, ff), (jm, jf) in PAIRS:
        assert callable(_fn(fm, ff)), f"{fm}.{ff} vanished — update PAIRS"
        assert callable(_fn(jm, jf)), f"{jm}.{jf} vanished — update PAIRS"
