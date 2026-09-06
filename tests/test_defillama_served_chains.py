"""defillama.SERVED_CHAINS must BE the catalogued chain set, and the per-chain sweep's cost must be
bounded by ARITHMETIC rather than by a budget.

WHY THIS FILE IS SMALLER THAN IT WAS. Four consecutive adversarial reviews (R778, R780, R784, R788)
each found a HIGH here, and each round's fix created the next round's defect: a budget (R780 #8)
needed a rotation bookmark or it was a truncation (R190); the bookmark recorded successes instead of
attempts (R784 #1); moving the clock to fix its scope then let a slow-but-successful bulk call
starve 14 of 14 (R788 #1). All of that machinery existed to survive ONE number - the module
session's `Retry(total=5, backoff_factor=1.5)` against a 120 s timeout, i.e. 766.5 s per URL. The
number is bounded now instead of managed (a session that retries once, 20 s), so the budget, the
bookmark, the deferral class and the retirement classifier are gone, and so are the tests that
pinned them. Deleting a test whose subject no longer exists is not lost coverage; keeping it would
be a lie.

What remains is the set of properties the change actually has.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import config                                            # noqa: E402
from updater.strategies.fetchers import defillama                     # noqa: E402
from updater.strategies.fetchers._common import Tally                 # noqa: E402

# Same resolution as orchestrate.py:1067, so the test reads the catalogue the fetcher would.
CATALOG = os.environ.get("ECONDL_CATALOG") or os.path.join(config.ROOT, "data", "catalog.db")
PREFIX = "defillama:chain_tvl:"
TS = 1_700_000_000


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
    """THE comparison, extracted so the test and its control run the SAME code (R66).

    R778 #5: the first control asserted `wrong != catalogued` - set inequality, which never invoked
    the missing/extra logic at all. Replacing the real test's body with `assert True` left it
    green."""
    return sorted(catalogued - ours), sorted(ours - catalogued)


@pytest.mark.skipif(not os.path.exists(CATALOG),
                    reason=f"no local catalogue at {CATALOG} (CI pulls it only for the updater job)")
def test_served_chains_is_exactly_the_catalogued_set():
    """The work list is a curated constant, which is a staleness bomb with a scheduler attached
    unless something checks it (R159). Two silent failure modes: a catalogued chain missing from the
    tuple stays frozen - the very defect this change exists to fix, re-introduced by omission - and
    a chain in the tuple that is not catalogued spends a request a day on a series nobody can reach.

    HONEST LIMIT, written here rather than left for a reviewer to find: this SKIPS where catalog.db
    is absent, and CI never pulls it for the test job, so the pin holds on this machine and nowhere
    else. I tried the alternative - reading the work list from the catalogue at runtime (R778 #6) -
    and it produced R784 #5 and R788 #5 in consecutive rounds."""
    catalogued = _catalogued_chains()
    assert catalogued, "no defillama chain_tvl rows in the catalogue - the control itself failed"
    missing, extra = _disagreement(catalogued, set(defillama.SERVED_CHAINS))
    assert not missing, (
        f"catalogued but NOT refreshed, so they stay frozen exactly as before this fix: {missing}")
    assert not extra, (
        f"refreshed every run but not catalogued, so nobody can download them: {extra}")


@pytest.mark.skipif(not os.path.exists(CATALOG), reason="no local catalogue")
def test_the_check_can_actually_fail():
    """Drive the REAL comparison with deliberately wrong inputs and require it to object, naming the
    right member each time."""
    catalogued = _catalogued_chains()
    one = sorted(catalogued)[0]

    missing, extra = _disagreement(catalogued, set(catalogued) - {one})
    assert missing == [one] and not extra, (missing, extra)

    missing, extra = _disagreement(catalogued, set(catalogued) | {"ZZZ_Not_A_Chain"})
    assert extra == ["ZZZ_Not_A_Chain"] and not missing, (missing, extra)

    missing, extra = _disagreement(catalogued, set(catalogued))
    assert not missing and not extra, "the comparison objects to a CORRECT list"


def test_the_tuple_has_no_duplicates_and_no_blanks():
    s = defillama.SERVED_CHAINS
    assert len(set(s)) == len(s), f"duplicate chain name in SERVED_CHAINS: {s}"
    assert all(isinstance(n, str) and n.strip() == n and n for n in s), s


def test_the_chain_sweep_is_bounded_by_ARITHMETIC_not_by_a_budget():
    """R788 #1 and its three predecessors all lived in machinery that existed to survive the module
    session's retry stack. This pins the replacement: the chain session retries ONCE, and 14 chains
    x (2 attempts x CHAIN_TIMEOUT_S + backoff) must sit well inside orchestrate's 45-minute per-unit
    SIGALRM - with no bookmark, no deferral and no state, because every chain is attempted every run
    and there is no fixed-order prefix for a rotation to resume."""
    s = defillama._chain_session()
    retry = s.get_adapter("https://api.llama.fi").max_retries
    assert retry.total == 1, f"the chain session must retry once, got total={retry.total}"
    attempts = retry.total + 1
    worst = len(defillama.SERVED_CHAINS) * (attempts * defillama.CHAIN_TIMEOUT_S
                                            + retry.backoff_factor)
    assert worst < 15 * 60, f"worst case {worst:.0f}s is not comfortably inside the 45-min cap"

    # The module session is deliberately NOT this - the bulk and catalog calls are one request each
    # and keep the full retry stack. If the two ever converge, the arithmetic above is wrong.
    assert defillama._session().get_adapter("https://api.llama.fi").max_retries.total == 5


def _stub(monkeypatch, bulk, chain):
    monkeypatch.setattr(defillama, "_get",
                        lambda sess, url, timeout=120, _b=bulk, _c=chain:
                        _b if url.endswith("historicalChainTvl") else _c)


@pytest.mark.parametrize("label,resp", [
    ("404",        {"__http__": 404}),
    ("402",        {"__http__": 402}),
    ("error",      {"__err__": "boom"}),
    ("parses-0",   [{"date": TS, "value": 7.0}]),      # 200, a list, zero usable points
    ("dup date",   [{"date": TS, "tvl": 7.0}, {"date": TS, "tvl": 9.0}]),
    ("not a list", {"nope": 1}),
])
def test_every_per_chain_outcome_has_a_failure_class(monkeypatch, label, resp):
    """R780 #1 was a chain answering 200 with a list that parsed to zero points taking NO branch at
    all - not _is_err, not _is_http, it IS a list, `len(set([])) != len([])` is `0 != 0`, and
    `extend([])` a no-op - so the run booked ok with no tally entry and the series froze behind a
    file __ALL__ keeps fresh. Every disposition must name the chain, and none may empty the table."""
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha",))
    _stub(monkeypatch, [{"date": TS, "tvl": 1.0}], resp)
    t = Tally()
    tbl, _dk, _k, _d, _err = defillama._chains_tvl_aggregate(None, t)
    named = " ".join(t.transient_ids + t.empty_ids + t.structural_ids + t.deferred_ids)
    assert "Alpha" in named, f"{label}: the chain contributed nothing and was NOT named"
    assert tbl is not None and tbl.num_rows > 0, (
        f"{label}: the table went empty -> _merge_file books structural_unit -> whole-source veto")


@pytest.mark.parametrize("label,bulk", [
    ("empty",         []),
    ("renamed field", [{"date": TS, "value": 1.0}]),
    ("404",           {"__http__": 404}),
    ("error",         {"__err__": "x"}),
])
def test___ALL___has_its_own_failure_class(monkeypatch, label, bulk):
    """R778 #1: `structural_unit` is FILE-grained, so fifteen entities in one table turned it into
    'at least one of fifteen parsed something'. A bulk `[]` or a renamed field gave __ALL__ zero
    rows, status ok and the vintage ADVANCED, freezing defillama:tvl:total - the headline series -
    behind a file that looked fresh."""
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha",))
    _stub(monkeypatch, bulk, [{"date": TS, "tvl": 7.0}])
    t = Tally()
    tbl, _dk, _k, _d, _e = defillama._chains_tvl_aggregate(None, t)
    assert any("__ALL__" in s for s in t.transient_ids), (
        f"bulk {label}: __ALL__ contributed nothing and nothing flagged it - the run would book ok "
        f"and advance the vintage")
    assert tbl is not None and "Alpha" in tbl.column("series_key").to_pylist(), label


def test_a_total_outage_keeps_the_old_data_instead_of_vetoing(monkeypatch):
    """R778 #2: an empty TABLE is booked structural and raised as a whole-source DefinitiveError,
    discarding the other families' work. None is _merge_file's keep-old-data transient path, which
    is the honest reading of everything being down."""
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha", "Beta"))
    _stub(monkeypatch, {"__err__": "down"}, {"__err__": "down"})
    t = Tally()
    tbl, _dk, _k, _d, err = defillama._chains_tvl_aggregate(None, t)
    assert tbl is None and err, (tbl, err)
    assert t.transient >= 3, t.transient_ids          # __ALL__ and both chains, each named


def test_a_healthy_run_names_nothing_and_carries_every_chain(monkeypatch):
    """The positive control. Without it the assertions above are satisfied by a function that always
    fails everything (R346, and R783's lesson that a matrix of negative cases proves nothing until
    something has been shown to succeed)."""
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha", "Beta"))
    _stub(monkeypatch, [{"date": TS, "tvl": 1.0}],
          [{"date": TS, "tvl": 7.0}, {"date": TS + 86400, "tvl": 8.0}])
    t = Tally()
    tbl, _dk, _k, _d, err = defillama._chains_tvl_aggregate(None, t)
    assert err is None and t.transient == 0 and t.structural == 0, (err, t.transient_ids)
    keys = set(tbl.column("series_key").to_pylist())
    assert keys == {"__ALL__", "Alpha", "Beta"}, keys
    pairs = list(zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()))
    assert len(set(pairs)) == len(pairs), "the table must stay uniquely keyed on (series_key, date)"
