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
import datetime as _dt
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

# A RECENT timestamp, not a frozen 2023 one. The partial-parse guard (R794 #4) refuses a chain
# whose newest parsed point is more than CHAIN_MAX_OBS_AGE_D days old, so a fixed fixture epoch
# would make every one of these fixtures look like the defect. That the guard caught my own
# fixtures the moment it was added is the cheapest possible evidence that it fires.
TS = int((_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=1)).timestamp())


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


def test_the_chain_session_has_the_policy_the_bound_assumes():
    """R788 #1 and its three predecessors all lived in machinery that existed to survive the module
    session's retry stack. This pins the replacement's POLICY; the two tests at the bottom of this
    file pin that every call actually uses it, which is the half R792 #3 found missing.

    My first version of this test also computed the bound and got it wrong twice over (R792 #2): it
    added a `backoff_factor` term urllib3 does not apply before a first retry, and it omitted the
    bulk GET, which was then still on the module session at 765 s - larger than everything else
    combined. The arithmetic now lives where the calls are observed, not here."""
    # PROBE THE URL THE FETCHER ACTUALLY CALLS (R794 #8). requests resolves adapters by LONGEST
    # prefix, so probing "https://api.llama.fi" while the code calls
    # ".../v2/historicalChainTvl/<name>" let a mount on a longer prefix keep every assertion true
    # while every real call used total=5 and honoured Retry-After. Measured by the reviewer: 22
    # passed with the worst case back to 2,475 s + 5 x 21,600 s per call.
    s = defillama._chain_session()
    url = "https://api.llama.fi/v2/historicalChainTvl/Ethereum"
    retry = s.get_adapter(url).max_retries
    assert retry.total == 1, f"the chain session must retry once, got total={retry.total}"
    assert retry.respect_retry_after_header is False, "Retry-After must not be honoured here"
    # urllib3 honours Retry-After only for {413, 429, 503}, so keeping those OUT of the forcelist
    # is what actually makes the header unreachable - and it costs the publisher nothing, where
    # respect_retry_after_header=False alone sent the retry 0.00 s later (R794 #7).
    assert 429 not in retry.status_forcelist and 503 not in retry.status_forcelist, (
        f"429/503 must not be retried here: urllib3 honours Retry-After for exactly those and "
        f"caps the sleep at 21,600 s. forcelist={sorted(retry.status_forcelist)}")
    # Redirects are TRANSACTIONS and each gets its own retry budget; Session.max_redirects defaults
    # to 30, which the reviewer measured as 31 transactions and 1,240 s for ONE call (R794 #1).
    assert s.max_redirects <= 1, f"max_redirects={s.max_redirects} makes the per-call bound a lie"

    # NOT asserted here: what the MODULE session's retry policy is. R794 #2 showed the previous
    # version pinning it at total=5, which would have made hardening it FAIL this suite - a test
    # standing guard over the defect. The module session's 19 calls are 5.44 h and unbounded; that
    # is real, it is NOT fixed by this change, and it is recorded rather than pinned.


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


# ---------------------------------------------------------- the bound, pinned where it can BREAK
# R792 #3: the previous version asserted the CONSTANTS and nothing else, so mutating `csess` back
# to the module session, or `timeout=CHAIN_TIMEOUT_S` back to 120, left all 16 tests green while
# restoring a 2.98-hour worst case. A bound is a property of the CALLS, so these observe the calls.

def _record_calls(monkeypatch, chain_resp=None, bulk_resp=None):
    """Capture (session, url, timeout) for every _get the chains family makes."""
    seen = []
    bulk = bulk_resp if bulk_resp is not None else [{"date": TS, "tvl": 1.0}]
    chain = chain_resp if chain_resp is not None else [{"date": TS, "tvl": 7.0}]

    def _get(sess, url, timeout=120):
        seen.append((sess, url, timeout))
        return bulk if url.endswith("historicalChainTvl") else chain
    monkeypatch.setattr(defillama, "_get", _get)
    return seen


def test_EVERY_chains_call_uses_the_bounded_session_and_timeout(monkeypatch):
    """Both halves of the bound, observed rather than read. The module session is handed in and
    must be ignored; if it is ever used the 20 s cap and the retry-once policy do not apply."""
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha", "Beta"))
    seen = _record_calls(monkeypatch)
    handed_in = defillama._session()
    defillama._chains_tvl_aggregate(handed_in, Tally())

    assert len(seen) == 3, [u for _s, u, _t in seen]          # bulk + two chains
    for s, url, timeout in seen:
        assert s is not handed_in, f"{url} used the module session - the bound does not apply to it"
        r = s.get_adapter("https://api.llama.fi").max_retries
        assert r.total == 1, f"{url} used a session with total={r.total}"
        assert r.respect_retry_after_header is False, (
            f"{url} honours Retry-After; urllib3 caps that sleep at 21,600 s, so one 429 is up to "
            f"six hours on one call and nothing else clips it now the Deadline is gone")
        assert timeout == defillama.CHAIN_TIMEOUT_S, f"{url} was called with timeout={timeout}"


def test_the_stated_bound_covers_every_call_including_the_bulk(monkeypatch):
    """R792 #2: the published figure omitted the bulk GET, which was 765 s on the module session -
    larger than everything else combined - and added a backoff term urllib3 does not apply before a
    first retry. The arithmetic must be over the calls actually made."""
    monkeypatch.setattr(defillama, "SERVED_CHAINS", tuple(f"C{i}" for i in range(14)))
    seen = _record_calls(monkeypatch)
    defillama._chains_tvl_aggregate(defillama._session(), Tally())
    worst = sum(2 * t for _s, _u, t in seen)                  # 2 attempts, 0 backoff before retry 1
    assert len(seen) == 15, len(seen)
    assert worst == 600, worst
    assert worst < 45 * 60, f"{worst}s is not inside orchestrate's 45-minute per-unit SIGALRM"


@pytest.mark.parametrize("label,body", [
    ("null element",   [None, {"date": TS, "tvl": 7.0}]),
    ("string element", ["oops", {"date": TS, "tvl": 7.0}]),
    ("int element",    [123, {"date": TS, "tvl": 7.0}]),
])
def test_a_non_dict_ELEMENT_does_not_crash_out_of_the_fetcher(monkeypatch, label, body):
    """R792 #4, the seventh disposition. Both loops guarded the CONTAINER and then called .get on
    whatever was inside it, so `[null, {...}]` raised an uncaught AttributeError out of update() -
    taking the chains merge, all of stablecoins_total, the obs total, the cursor map and finalize()
    with it. That is a worse blast radius than the whole-source veto the None path exists to avoid."""
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha",))
    _record_calls(monkeypatch, chain_resp=body, bulk_resp=body)
    tbl, _dk, _k, _d, _e = defillama._chains_tvl_aggregate(None, Tally())
    keys = set(tbl.column("series_key").to_pylist())
    assert keys == {"__ALL__", "Alpha"}, (label, keys)        # the good element still lands


def test___ALL___refuses_a_repeated_obs_date(monkeypatch):
    """R792 #5: the docstring claimed this refusal for the endpoint and only the per-chain loop had
    it, so a duplicated bulk date reached merge and was deduped silently on the HEADLINE series -
    R773's pathology, on tvl:total."""
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha",))
    _record_calls(monkeypatch, bulk_resp=[{"date": TS, "tvl": 1.0}, {"date": TS, "tvl": 9.0}])
    t = Tally()
    tbl, _dk, _k, _d, _e = defillama._chains_tvl_aggregate(None, t)
    assert any("__ALL__" in s and "repeated obs_date" in s for s in t.transient_ids), t.transient_ids
    assert "__ALL__" not in set(tbl.column("series_key").to_pylist())


# ------------------------------------------------------- R794: partial parses and element shapes

@pytest.mark.parametrize("label,body", [
    ("null element",       [None, {"date": TS, "tvl": 7.0}]),
    ("string element",     ["oops", {"date": TS, "tvl": 7.0}]),
    ("nested-dict date",   [{"date": {"n": 1}, "tvl": 7.0}, {"date": TS, "tvl": 7.0}]),
    ("nested-list date",   [{"date": [1], "tvl": 7.0}, {"date": TS, "tvl": 7.0}]),
])
def test_no_response_SHAPE_escapes_as_an_exception(monkeypatch, label, body):
    """R794 #3. The element guard reached 2 of 7 loops, and `_to_date` caught ValueError but not
    TypeError - so `{"date": {...}}` raised straight THROUGH the guard it did have. An uncaught
    AttributeError/TypeError leaves update() entirely, taking the chains merge, stablecoins, the
    obs total, the cursor map and finalize() with it: a worse blast radius than the whole-source
    veto the None path exists to avoid."""
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha",))
    _record_calls(monkeypatch, chain_resp=body, bulk_resp=body)
    tbl, _dk, _k, _d, _e = defillama._chains_tvl_aggregate(None, Tally())
    assert set(tbl.column("series_key").to_pylist()) == {"__ALL__", "Alpha"}, label


def test__to_date_answers_None_for_any_shape_it_cannot_read():
    """A date parser's contract is to refuse, not to raise, whatever arrives."""
    for bad in ({"n": 1}, [1], (1,), object(), b"x", None, "", "not-a-date"):
        assert defillama._to_date(bad) is None, bad
    assert defillama._to_date(TS) is not None      # the positive control


def test_a_PARTIAL_parse_is_refused_rather_than_merged(monkeypatch):
    """R794 #4, and the sharpest of the class. Every other guard asks 'did we parse NOTHING'; a
    partial parse defeats all of them. Six points whose newest three carry `tvl` as a string parse
    to real rows with an old max date - tally empty, err None, status ok, vintage ADVANCED - which
    is R778 #1's freeze wearing a non-empty result. It is also the shape a type change upstream
    produces FIRST, because the newest rows are the ones a publisher reformats."""
    import datetime as _dt
    day = 86400
    now = int(_dt.datetime.now(_dt.timezone.utc).timestamp())
    old_ok = [{"date": now - (60 + i) * day, "tvl": 1.0 + i} for i in range(3)]
    new_broken = [{"date": now - i * day, "tvl": "1234.5"} for i in range(3)]   # strings
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha",))
    _record_calls(monkeypatch, chain_resp=old_ok + new_broken,
                  bulk_resp=[{"date": now, "tvl": 1.0}])
    t = Tally()
    tbl, _dk, _k, _d, _e = defillama._chains_tvl_aggregate(None, t)
    assert "Alpha" not in set(tbl.column("series_key").to_pylist()), (
        "a partial parse was merged over the top of fresher stored data")
    assert any("PARTIAL parse" in s for s in t.transient_ids), t.transient_ids


def test_a_genuinely_fresh_parse_is_NOT_refused(monkeypatch):
    """The positive control for the staleness guard: without it the test above is satisfied by a
    function that refuses everything (R783)."""
    import datetime as _dt
    day = 86400
    now = int(_dt.datetime.now(_dt.timezone.utc).timestamp())
    fresh = [{"date": now - i * day, "tvl": 1.0 + i} for i in range(5)]
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha",))
    _record_calls(monkeypatch, chain_resp=fresh, bulk_resp=fresh)
    t = Tally()
    tbl, _dk, _k, _d, _e = defillama._chains_tvl_aggregate(None, t)
    assert "Alpha" in set(tbl.column("series_key").to_pylist()), t.transient_ids
    assert t.transient == 0, t.transient_ids


# ------------------------------------------------------- R795: the sibling branch, and the class

def test_the_BULK_branch_gets_the_same_partial_parse_guard_as_the_chains(monkeypatch):
    """R795 #1, the blocker, and the ninth appearance of 'one example is a class' on this file. I
    put the partial-parse guard in the per-chain loop and NOT in the `__ALL__` branch thirty lines
    above it in the SAME function. __ALL__ is `defillama:tvl:total`, the headline series, and its
    silent freeze is invisible to health._recency_signal (a max over the whole unit)."""
    day = 86400
    now = int(_dt.datetime.now(_dt.timezone.utc).timestamp())
    old_ok = [{"date": now - (60 + i) * day, "tvl": 1.0 + i} for i in range(3)]
    new_broken = [{"date": now - i * day, "tvl": "1234.5"} for i in range(3)]
    monkeypatch.setattr(defillama, "SERVED_CHAINS", ("Alpha",))
    _record_calls(monkeypatch, bulk_resp=old_ok + new_broken,
                  chain_resp=[{"date": now, "tvl": 7.0}])
    t = Tally()
    tbl, _dk, _k, _d, _e = defillama._chains_tvl_aggregate(None, t)
    assert "__ALL__" not in set(tbl.column("series_key").to_pylist()), (
        "a partial parse of the BULK call was merged - tvl:total freezes behind a fresh file")
    assert any("__ALL__" in s and "PARTIAL parse" in s for s in t.transient_ids), t.transient_ids


def test_ONE_call_site_decides_partial_for_every_branch():
    """The structural half of R795 #1: a shared helper cannot be applied to one sibling and not the
    other. If this count ever drops, the branches have drifted apart again."""
    import inspect
    src = inspect.getsource(defillama._chains_tvl_aggregate)
    assert src.count("_partial_parse_reason(") == 2, (
        "the bulk branch and the per-chain loop must BOTH go through the shared helper")


def test_a_FUTURE_date_cannot_switch_the_staleness_guard_off():
    """R795 #4: the frontier was `max(dates)`, so one 2999-12-31 point turned the guard off
    entirely - and 2999-12-31 is not hypothetical, R320 records it already published across six
    sources of this store."""
    import datetime as d2
    today = d2.date.today()
    stale = today - d2.timedelta(days=60)
    assert defillama._partial_parse_reason([stale, d2.date(2999, 12, 31)]), (
        "a single future-dated point masked a 60-day-stale parse")
    assert defillama._partial_parse_reason([d2.date(2999, 12, 31)]), "all-future is itself a defect"
    assert defillama._partial_parse_reason([today, stale]) is None, "a sound parse was refused"


@pytest.mark.parametrize("fn,payload", [
    ("_catalog_protocols",   [None, {"name": "x", "chains": ["Ethereum"]}]),
    ("_catalog_chains",      [None, {"name": "x"}]),
    ("_catalog_stablecoins", {"peggedAssets": [None, {"id": 1, "name": "x"}]}),
    ("_catalog_yield_pools", {"data": [None, {"pool": "p", "project": "x"}]}),
])
def test_every_catalog_loop_survives_a_non_dict_element(monkeypatch, fn, payload):
    """R795 #2: `_catalog_stablecoins` was the one of five loops that did NOT get the guard, while
    my commit message said 'All five loops guarded' - true of the count, false of the set. It runs
    third of four catalog fetches, BEFORE the chains merge, so an AttributeError there takes the
    whole source to transient_fail: no derive, no last_success, no vintage.

    Enumerated with a parser rather than by eye, which is what found it."""
    monkeypatch.setattr(defillama, "_get", lambda sess, url, timeout=120: payload)
    tbl, _dk, _err = getattr(defillama, fn)(None)
    assert tbl is not None, fn


@pytest.mark.parametrize("value,expected", [
    (["Ethereum", "Base"], "Ethereum,Base"),
    ("Ethereum",           ""),        # a STRING would join character by character
    ([{"x": 1}, "Base"],   "Base"),    # a dict inside would raise TypeError
    (None,                 ""),
    (123,                  ""),
])
def test_join_names_refuses_everything_that_is_not_a_list_of_strings(value, expected):
    """R795 #3, same class, two call sites in two functions. The silent half is the string case:
    `",".join("Ethereum")` publishes 'E,t,h,e,r,e,u,m' into the catalogue snapshot."""
    assert defillama._join_names(value) == expected
