"""defillama.SERVED_CHAINS must BE the catalogued chain set, not a list that drifts from it.

The fetcher now spends 14 requests a run refreshing the per-chain series users can download.
That work list is a module constant, which is a staleness bomb with a scheduler attached unless
something checks it (R159: stats_nz's discovery was a hardcoded period list frozen at Dec-2024).
Two ways it can go wrong, and both are silent:

  * a chain is CATALOGUED and missing from the tuple  -> a downloadable series stays frozen, which
    is the exact defect this change was written to fix, re-introduced by omission;
  * a chain is in the tuple and NOT catalogued        -> requests spent every day, forever, on a
    series no user can reach.

The list must come from the CATALOGUE and never from the publisher's listing (R61, R769 rule 7):
six of defillama's eight catalogued protocols are absent from /protocols while live at their own
endpoint, so a listing-driven check would certify the very hole it is meant to catch.

Skips where catalog.db is absent (CI runners pull it only for the updater job), which is the
honest best available - a machine-local check that skips elsewhere beats a green assertion that
proves nothing.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import config                                            # noqa: E402
from updater.strategies.fetchers import defillama                     # noqa: E402

CATALOG = os.path.join(os.path.dirname(config.DATA_ROOT), "catalog.db")
PREFIX = "defillama:chain_tvl:"


def _catalogued_chains():
    con = sqlite3.connect(f"file:{CATALOG}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT series_id FROM series WHERE source_id='defillama' AND series_id LIKE ?",
            (PREFIX + "%",)).fetchall()
    finally:
        con.close()
    return {r[0][len(PREFIX):] for r in rows}


def _disagreement(catalogued, ours):
    """THE comparison, extracted so the test and its negative control run the SAME code (R66).

    R778 #5: my first control asserted `wrong != catalogued` — set inequality, which never invoked
    the missing/extra logic at all. Replacing the real test's body with `assert True` left it green.
    That is the exact R346 failure ("a check must be shown able to fail") in a test whose own
    docstring cited R346."""
    return sorted(catalogued - ours), sorted(ours - catalogued)


@pytest.mark.skipif(not os.path.exists(CATALOG),
                    reason=f"no local catalogue at {CATALOG} (CI pulls it only for the updater job)")
def test_served_chains_is_exactly_the_catalogued_set():
    catalogued = _catalogued_chains()
    assert catalogued, "no defillama chain_tvl rows in the catalogue - the control itself failed"
    missing, extra = _disagreement(catalogued, set(defillama.SERVED_CHAINS))
    assert not missing, (
        f"catalogued but NOT refreshed, so they stay frozen exactly as before this fix: {missing}")
    assert not extra, (
        f"refreshed every run but not catalogued, so nobody can download them: {extra}")


@pytest.mark.skipif(not os.path.exists(CATALOG), reason="no local catalogue")
def test_the_check_can_actually_fail():
    """Drive the REAL comparison with deliberately wrong inputs and require it to object, naming
    the right member each time."""
    catalogued = _catalogued_chains()
    one = sorted(catalogued)[0]

    missing, extra = _disagreement(catalogued, set(catalogued) - {one})
    assert missing == [one] and not extra, (missing, extra)      # a catalogued chain left frozen

    missing, extra = _disagreement(catalogued, set(catalogued) | {"ZZZ_Not_A_Chain"})
    assert extra == ["ZZZ_Not_A_Chain"] and not missing, (missing, extra)   # a chain nobody serves

    missing, extra = _disagreement(catalogued, set(catalogued))
    assert not missing and not extra, "the comparison objects to a CORRECT list"


@pytest.mark.skipif(not os.path.exists(CATALOG), reason="no local catalogue")
def test_the_runtime_resolver_prefers_the_catalogue(capsys):
    """R778 #6: the constant is only a fallback; updater-daily.yml pulls catalog.db, so the run that
    matters reads the list from it. Assert the resolver actually does that AND says so."""
    got = defillama._served_chains()
    assert set(got) == _catalogued_chains()
    assert "from the CATALOGUE" in capsys.readouterr().out


def test_the_resolver_falls_back_when_the_catalogue_is_absent(tmp_path, monkeypatch, capsys):
    """The fallback must work and must ANNOUNCE itself — a component with a silent fallback makes
    plausible output that proves nothing (R344)."""
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "nope.db"))
    monkeypatch.setattr(defillama.config, "ROOT", str(tmp_path))
    got = defillama._served_chains()
    assert got == defillama.SERVED_CHAINS
    assert "from the PINNED constant" in capsys.readouterr().out


def test_the_tuple_has_no_duplicates_and_no_blanks():
    """Cheap, runs everywhere, and catches the shape a merge conflict produces."""
    s = defillama.SERVED_CHAINS
    assert len(set(s)) == len(s), f"duplicate chain name in SERVED_CHAINS: {s}"
    assert all(isinstance(n, str) and n.strip() == n and n for n in s), s
