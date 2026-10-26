"""hagstofa: a pass ends inside its budget, with state consistent (2026-10-19).

10 of the 12 runs from 2026-10-02 to 10-16 were killed by the orchestrator's 45-minute limit although
the fetcher's budget is 30 minutes. The run logs show why: the budget was checked only at the top of
each DATABASE, Samfelag (736 tables) started at ~15 min and was still being fetched at 45 min, its rows
were never merged, and the rotation bookmark - saved only at the end of update() - was never written,
so every later run took the same path to the same kill.

These tests run the REAL update() and merge (only HTTP, or _fetch_table, is faked) and pin:
  - the budget is checked before every table; on a stop the fetched rows are merged, every table not
    reached is booked DEFERRED (partial, never no_change), and the bookmark names the last durable table;
  - the next pass starts with the first table the last one did not reach, and wraps;
  - a kill keeps the bookmark of the last merged segment;
  - a deferred table-tree search is never passed by the bookmark - unless the pass started on it;
  - every request timeout and every retry wait is cut to the budget left; 429 honours Retry-After;
  - the budget clock starts before the store reads, and the unit window can only lower the budget;
  - on a virtual clock, a pass whose requests hang near the end of the budget still ends inside it.
"""
from __future__ import annotations

import datetime as dt
import email.utils
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
requests = pytest.importorskip("requests")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import hagstofa as H  # noqa: E402
from updater.strategies.fetchers import _melodi_state as MS  # noqa: E402

B = dt.date(2025, 12, 31)


def _cat(spec):
    """spec: {db: n tables} -> catalogue entries in that order."""
    return [{"db": db, "path": f"f/{db}{k}.px", "id": f"{db}{k}.px", "text": "x"}
            for db, n in spec.items() for k in range(1, n + 1)]


class _DL:
    """A Deadline fake: spent after `after` checks of spent(). No budget/t0, so requests are unbounded."""
    def __init__(self, after=None):
        self.after, self.calls, self.budget_min = after, 0, 30.0

    def spent(self):
        self.calls += 1
        return self.after is not None and self.calls > self.after

    def elapsed_min(self):
        return 0.0


def _setup(tmp_path, monkeypatch, spec, outcome=lambda db, path: "data", dl=None, raise_at=None):
    """Real update() over a faked catalogue; returns (run(), fetched list)."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.delenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: _cat(spec))
    fetched = []

    def fake_fetch(sess, db, path, prefix, since, **k):
        fetched.append(f"{db}/{path}")
        if raise_at == f"{db}/{path}":
            raise RuntimeError("simulated kill")
        o = outcome(db, path)
        if isinstance(o, Exception):
            raise o
        if o == "data":
            return [(f"{prefix}:Dim=0", B, 1.0)], "data"
        return [], o

    monkeypatch.setattr(H, "_fetch_table", fake_fetch)
    box = {"dl": dl}
    monkeypatch.setattr(H, "Deadline", lambda minutes: box["dl"] or _DL())

    def run(new_dl=None):
        if new_dl is not None:
            box["dl"] = new_dl
        unit = types.SimpleNamespace(config={}, key="hagstofa/_all")
        try:
            return H.update(unit, None)
        except H.DefinitiveError as e:
            return types.SimpleNamespace(status="structural", error=str(e))
    return run, fetched


def _bookmark(tmp_path):
    p = tmp_path / "_rotation.json"
    return json.loads(p.read_text())["after"] if p.exists() else None


def _keys(tmp_path, db):
    p = tmp_path / f"{db}.parquet"
    return sorted(set(pq.read_table(str(p)).column("series_key").to_pylist())) if p.exists() else []


# ---- the per-table budget check, the merge on a stop, the deferrals, the bookmark -----------------

def test_a_budget_stop_inside_a_db_merges_what_was_fetched_and_defers_the_rest(tmp_path, monkeypatch):
    run, fetched = _setup(tmp_path, monkeypatch, {"A": 3, "B": 3}, dl=_DL(after=2))
    res = run()
    assert fetched == ["A/f/A1.px", "A/f/A2.px"], "no table is fetched after the budget is spent"
    assert res.status == "partial", res.error
    assert "4 deferred by budget" in res.error, res.error          # A3, B1, B2, B3: not reached, not failed
    assert "transient" not in res.error
    assert _keys(tmp_path, "A") == ["ICE:A:f:A1.px:Dim=0", "ICE:A:f:A2.px:Dim=0"], "the fetched rows merged"
    assert _bookmark(tmp_path) == "A/f/A2.px", "the bookmark names the last table whose rows merged"


def test_a_pass_that_only_defers_is_partial_never_no_change(tmp_path, monkeypatch):
    """R311: an empty tally makes finalize() stamp a vintage over tables the pass never reached."""
    run, _f = _setup(tmp_path, monkeypatch, {"A": 3}, outcome=lambda db, p: "quiet", dl=_DL(after=1))
    res = run()
    assert res.status == "partial" and "2 deferred by budget" in res.error, (res.status, res.error)


def test_the_next_pass_starts_at_the_first_table_not_reached_and_wraps(tmp_path, monkeypatch):
    run, fetched = _setup(tmp_path, monkeypatch, {"A": 3, "B": 2}, dl=_DL(after=2))
    run()
    fetched.clear()
    res = run(_DL())                                               # a pass with budget to spare
    assert fetched == ["A/f/A3.px", "B/f/B1.px", "B/f/B2.px", "A/f/A1.px", "A/f/A2.px"], fetched
    assert res.status == "ok", res.error
    assert _bookmark(tmp_path) == "A/f/A2.px", "a complete pass ends on the table before where it began"


def test_a_legacy_db_bookmark_starts_at_the_next_db(tmp_path, monkeypatch):
    """The code before 2026-10-19 saved a bare db name, meaning 'after that whole db'."""
    run, fetched = _setup(tmp_path, monkeypatch, {"A": 2, "B": 2, "C": 1})
    (tmp_path / "_rotation.json").write_text(json.dumps({"after": "A"}))
    run()
    assert fetched[0] == "B/f/B1.px" and fetched[-1] == "A/f/A2.px", fetched


def test_a_kill_inside_a_later_segment_keeps_the_bookmark_of_the_merged_one(tmp_path, monkeypatch):
    """R273: the bookmark is saved after each merge, not only at the end of update()."""
    run, _f = _setup(tmp_path, monkeypatch, {"A": 2, "B": 3}, raise_at="B/f/B2.px")
    with pytest.raises(RuntimeError):
        run()
    assert _bookmark(tmp_path) == "A/f/A2.px", "A merged, so the bookmark names A's last table"
    assert _keys(tmp_path, "A") == ["ICE:A:f:A1.px:Dim=0", "ICE:A:f:A2.px:Dim=0"]
    assert _keys(tmp_path, "B") == [], "B's segment was in flight: nothing of it is claimed"


def test_a_kill_keeps_the_sidecars_of_the_merged_segment(tmp_path, monkeypatch):
    """AR-352 #1: _labels.json and _withdrawn.json are written after each segment, so a kill in a
    later segment does not lose what the merged one learned (R273)."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: _cat({"A": 2, "B": 2}))
    monkeypatch.setattr(H, "Deadline", lambda minutes: _DL())

    def fetch(sess, db, path, prefix, since, **k):
        if f"{db}/{path}" == "B/f/B2.px":
            raise RuntimeError("simulated kill")
        sess._hagstofa_labels[prefix] = {"Dim": {"0": "Total"}}
        if db == "A":
            sess._hagstofa_withdrawn[f"{db}/f/gone.px"] = {"verdict": "withdrawn", "date": "2026-10-19"}
        return [(f"{prefix}:Dim=0", B, 1.0)], "data"
    monkeypatch.setattr(H, "_fetch_table", fetch)
    with pytest.raises(RuntimeError):
        H.update(types.SimpleNamespace(config={}, key="hagstofa/_all"), None)
    labels = json.loads((tmp_path / "_labels.json").read_text(encoding="utf-8"))
    assert "ICE:A:f:A1.px" in labels and "ICE:A:f:A2.px" in labels, labels
    assert json.loads((tmp_path / "_withdrawn.json").read_text())["A/f/gone.px"]["verdict"] == "withdrawn"


def test_a_transient_table_moves_the_bookmark(tmp_path, monkeypatch):
    """The design (AR-352 M21): a failed table has a final outcome for this pass and waits one rotation;
    only deferred tables hold the bookmark."""
    run, _f = _setup(tmp_path, monkeypatch, {"A": 3},
                     outcome=lambda db, p: H.TransientError("5xx") if p.endswith("A3.px") else "data")
    run()
    assert _bookmark(tmp_path) == "A/f/A3.px"


def test_a_budget_cut_books_the_cut_table_once(tmp_path, monkeypatch):
    """AR-352 M17: 3 tables, BudgetCut on the 2nd -> exactly 2 deferred (the cut table and the 3rd)."""
    run, _f = _setup(tmp_path, monkeypatch, {"A": 3},
                     outcome=lambda db, p: H.BudgetCut("cut") if p.endswith("A2.px") else "data")
    res = run()
    assert "2 deferred by budget" in res.error, res.error


def test_the_structural_note_also_names_transient_tables(tmp_path, monkeypatch):
    """AR-352 #2: finalize() names only the refused tables when it raises, so a 5xx / timeout / 429
    table was invisible in state while the 7 refusals stand."""
    def outcome(db, p):
        if p.endswith("A1.px"):
            return "structural"
        if p.endswith("A2.px"):
            return H.TransientError("HTTP 503")
        return "data"
    run, _f = _setup(tmp_path, monkeypatch, {"A": 3}, outcome=outcome)
    res = run()
    # A3 merged a new row, so since Route B (AR-378) the raise comes back as `partial` WITH the changed
    # set and the raise's own text; the note contract below is unchanged.
    assert res.status == "partial" and res.changed_keys == {"ICE:A:f:A3.px": B.isoformat()}, res
    assert "1 table(s) transient-failed" in res.error and "(first: f/A2.px)" in res.error, res.error
    assert res.error.index("transient-failed") < res.error.index(" ["), "before the offender list"


def test_negative_control_a_clean_structural_note_has_no_transient_clause(tmp_path, monkeypatch):
    run, _f = _setup(tmp_path, monkeypatch, {"A": 2},
                     outcome=lambda db, p: "structural" if p.endswith("A1.px") else "data")
    assert "transient" not in run().error


# ---- the table-tree search --------------------------------------------------------------------------

def test_a_deferred_tree_search_stops_the_pass_and_leads_the_next_one(tmp_path, monkeypatch):
    out = {"A/f/A2.px": "deferred"}
    run, fetched = _setup(tmp_path, monkeypatch, {"A": 3, "B": 1},
                          outcome=lambda db, p: out.get(f"{db}/{p}", "data"))
    res = run()
    assert fetched == ["A/f/A1.px", "A/f/A2.px"], "the pass stops at the deferred search"
    assert res.status == "partial" and "3 deferred by budget" in res.error, res.error
    assert "table-tree search" in res.error
    assert _bookmark(tmp_path) == "A/f/A1.px", "the bookmark does not pass the deferred table"
    fetched.clear()
    out.clear()
    run()
    assert fetched[0] == "A/f/A2.px", "the next pass begins with it, and its whole budget"


def test_a_tree_search_deferred_on_the_first_table_of_a_pass_does_not_pin_the_rotation(tmp_path, monkeypatch):
    """R190: if the search cannot finish even with a whole pass's budget, stopping again would freeze
    every later pass on this one table."""
    run, fetched = _setup(tmp_path, monkeypatch, {"A": 3},
                          outcome=lambda db, p: "deferred" if p.endswith("A2.px") else "data")
    (tmp_path / "_rotation.json").write_text(json.dumps({"after": "A/f/A1.px"}))
    res = run()
    assert fetched == ["A/f/A2.px", "A/f/A3.px", "A/f/A1.px"], fetched
    assert res.status == "partial" and "1 deferred by budget" in res.error, res.error
    assert _bookmark(tmp_path) == "A/f/A1.px"


def test_a_budget_cut_on_the_first_table_of_a_pass_does_not_pin_the_rotation(tmp_path, monkeypatch):
    """Review R1636 P1: a whole-budget tree search whose last listing is cut raises BudgetCut on the
    FIRST table of the pass. Without its own anti-pin arm, three passes each fetched only that table
    and the bookmark never moved."""
    run, fetched = _setup(tmp_path, monkeypatch, {"A": 3},
                          outcome=lambda db, p: H.BudgetCut("listing cut") if p.endswith("A1.px") else "data")
    seen = []
    for _ in range(3):
        fetched.clear()
        res = run(_DL())
        seen.append(list(fetched))
    assert seen[0] == ["A/f/A1.px", "A/f/A2.px", "A/f/A3.px"], seen
    assert res.status == "partial" and "deferred by budget" in res.error, res.error
    assert _bookmark(tmp_path) == "A/f/A3.px"


def test_negative_control_a_budget_cut_later_in_the_pass_still_stops_it_without_passing_the_table(tmp_path,
                                                                                                    monkeypatch):
    run, fetched = _setup(tmp_path, monkeypatch, {"A": 3},
                          outcome=lambda db, p: H.BudgetCut("cut") if p.endswith("A2.px") else "data")
    run()
    assert fetched == ["A/f/A1.px", "A/f/A2.px"] and _bookmark(tmp_path) == "A/f/A1.px"


# ---- a throttled sweep stops (ssb #177's rule) ------------------------------------------------------

def test_two_throttled_tables_in_a_row_stop_the_pass(tmp_path, monkeypatch):
    thr = {"A/f/A2.px", "A/f/A3.px"}
    run, fetched = _setup(tmp_path, monkeypatch, {"A": 5, "B": 2},
                          outcome=lambda db, p: H.Throttled("HTTP 429") if f"{db}/{p}" in thr else "data")
    res = run()
    assert fetched == ["A/f/A1.px", "A/f/A2.px", "A/f/A3.px"], fetched
    assert res.status == "partial" and "2/3 sub-unit(s) transient-failed" in res.error, res.error
    assert "4 table(s) deferred after 2 tables in a row were throttled" in res.error, res.error
    assert _keys(tmp_path, "A") == ["ICE:A:f:A1.px:Dim=0"], "the rows fetched before the stop merged"


def test_negative_control_throttled_tables_that_are_not_in_a_row_do_not_stop_the_pass(tmp_path, monkeypatch):
    thr = {"A/f/A1.px", "A/f/A3.px"}
    run, fetched = _setup(tmp_path, monkeypatch, {"A": 4},
                          outcome=lambda db, p: H.Throttled("HTTP 429") if f"{db}/{p}" in thr else "data")
    res = run()
    assert len(fetched) == 4 and "deferred" not in res.error, (fetched, res.error)


def test_a_request_429_on_every_try_raises_throttled(slept):
    s, _log = _sess([_Resp(429)] * H.MAX_ATTEMPTS)
    with pytest.raises(H.Throttled):
        H._get_meta(s, "u")


def test_the_tree_search_is_not_started_without_the_budget_it_needs(monkeypatch):
    monkeypatch.setattr(H.time, "sleep", lambda s: None)
    asked = []
    sess = types.SimpleNamespace(get=lambda url, timeout=None: asked.append(url))
    dl = H.Deadline(minutes=H.TREE_SEARCH_MIN - 1)
    assert H._missing_verdict(sess, "Samfelag", "x/T.px", dt.date(2024, 12, 31), dl) == ([], "deferred")
    assert asked == [], "no listing was requested"


def test_negative_control_with_enough_budget_the_tree_search_runs(monkeypatch):
    monkeypatch.setattr(H.time, "sleep", lambda s: None)
    asked = []

    def get(url, timeout=None):
        asked.append(url)
        return types.SimpleNamespace(status_code=500, json=lambda: None, headers={})
    sess = types.SimpleNamespace(get=get)
    dl = H.Deadline(minutes=H.TREE_SEARCH_MIN + 1)
    H._missing_verdict(sess, "Samfelag", "x/T.px", dt.date(2024, 12, 31), dl)
    assert asked, "the search started"


# ---- requests and waits are cut to the budget left -------------------------------------------------

class _Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._b, self.headers = status, body, headers or {}

    def json(self):
        return self._b


def _sess(responses, left_s=None, log=None):
    """A session whose get/post replay `responses` (a response or an exception each) and record the
    timeout they were given. left_s: a real Deadline with that many seconds of budget."""
    log = log if log is not None else []

    def call(url, json=None, timeout=None):
        log.append(timeout)
        r = responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r
    s = types.SimpleNamespace(get=lambda url, timeout=None: call(url, timeout=timeout), post=call,
                              _hagstofa_stats={})
    if left_s is not None:
        s._hagstofa_dl = H.Deadline(minutes=left_s / 60.0)
    return s, log


@pytest.fixture
def slept(monkeypatch):
    got = []
    monkeypatch.setattr(H.time, "sleep", lambda s: got.append(s))
    monkeypatch.delenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", raising=False)
    return got


def test_a_request_timeout_is_cut_to_the_budget_left(slept):
    s, log = _sess([_Resp(200, {"variables": []})], left_s=20)
    H._get_meta(s, "u")
    assert H.REQUEST_TIMEOUT_FLOOR_S <= log[0] <= 20, log


def test_a_cut_timeout_never_drops_below_the_floor(slept):
    s, log = _sess([_Resp(200, {"variables": []})], left_s=1)
    H._get_meta(s, "u")
    assert log == [H.REQUEST_TIMEOUT_FLOOR_S], log


def test_negative_control_with_budget_to_spare_the_timeout_is_not_cut(slept):
    s, log = _sess([_Resp(200, {"variables": []})], left_s=600)
    H._get_meta(s, "u")
    assert log == [H.TIMEOUT]


def test_no_request_starts_after_the_budget(slept):
    s, log = _sess([_Resp(200, {})], left_s=0)
    with pytest.raises(H.BudgetCut):
        H._get_meta(s, "u")
    assert log == [], "nothing was sent"


def test_a_timeout_the_budget_cut_short_is_a_budget_cut_not_a_failure(slept):
    s, _log = _sess([requests.Timeout("read timed out")], left_s=20)
    with pytest.raises(H.BudgetCut):
        H._post_data(s, "u", {})


@pytest.mark.parametrize("call", ["get", "post", "listing"])
def test_a_body_read_timeout_cut_by_the_budget_is_a_budget_cut(slept, call):
    """requests 2.32.5 raises ConnectionError, not Timeout, for a read timeout while the BODY downloads
    (review R1636). With the timeout cut to the budget left it is the budget, not the table."""
    s, _log = _sess([requests.ConnectionError("Read timed out.")], left_s=20)
    with pytest.raises(H.BudgetCut):
        {"get": lambda: H._get_meta(s, "u"), "post": lambda: H._post_data(s, "u", {}),
         "listing": lambda: H._listing(s, "u")}[call]()
    assert s._hagstofa_stats.get("timeouts") == 1


def test_negative_control_an_uncut_connection_error_is_still_a_failure(slept):
    s, _log = _sess([requests.ConnectionError("reset")] * H.MAX_ATTEMPTS, left_s=600)
    with pytest.raises(H.TransientError) as e:
        H._get_meta(s, "u")
    assert not isinstance(e.value, H.BudgetCut)


def test_negative_control_an_ordinary_timeout_is_still_a_transient_failure(slept):
    s, _log = _sess([requests.Timeout("t")] * H.MAX_ATTEMPTS)
    with pytest.raises(H.TransientError) as e:
        H._post_data(s, "u", {})
    assert not isinstance(e.value, H.BudgetCut)


def test_a_retry_wait_longer_than_the_budget_left_is_not_slept(slept):
    s, _log = _sess([_Resp(503), _Resp(200, {})], left_s=0.5)
    with pytest.raises(H.BudgetCut):
        H._get_meta(s, "u")
    assert slept == []


def test_a_listing_timeout_cut_by_the_budget_is_not_an_unreadable_tree(slept):
    """An unreadable listing voids the tree and reads 'structural'; a budget cut must not."""
    s, _log = _sess([requests.Timeout("t")], left_s=20)
    with pytest.raises(H.BudgetCut):
        H._listing(s, "u")
    s2, _ = _sess([requests.Timeout("t")])
    assert H._listing(s2, "u") is None, "negative control: an ordinary timeout is still unreadable"


# ---- HTTP 429 honours Retry-After -------------------------------------------------------------------

def test_429_waits_the_retry_after_seconds(slept):
    s, _log = _sess([_Resp(429, headers={"Retry-After": "7"}), _Resp(200, {"ok": 1})])
    assert H._get_meta(s, "u") == {"ok": 1}
    assert slept == [8], slept                                     # the shared helper adds 1 s


def test_429_waits_a_retry_after_http_date(slept):
    when = email.utils.format_datetime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=30),
                                       usegmt=True)
    s, _log = _sess([_Resp(429, headers={"Retry-After": when}), _Resp(200, {})])
    H._post_data(s, "u", {})
    assert 25 <= slept[0] <= 32, slept


def test_429_without_retry_after_waits_one_rate_window(slept):
    s, _log = _sess([_Resp(429), _Resp(200, {})])
    H._get_meta(s, "u")
    assert slept == [H.RATE_WINDOW_S + 1], slept


def test_429_on_a_listing_honours_retry_after(slept):
    s, _log = _sess([_Resp(429, headers={"Retry-After": "3"}), _Resp(200, [])])
    assert H._listing(s, "u") == []
    assert slept == [4]


def test_a_long_retry_after_stops_instead_of_a_clamped_wait(slept):
    """ssb #177 / AR-352 #4: the shared helper clamps to 121 s, which retries inside the server's window."""
    s, _log = _sess([_Resp(429, headers={"Retry-After": "3600"}), _Resp(200, {})], left_s=600)
    with pytest.raises(H.Throttled) as e:
        H._get_meta(s, "u")
    assert e.value.stop_now and slept == []


def test_negative_control_a_retry_after_at_the_limit_is_waited(slept):
    s, _log = _sess([_Resp(429, headers={"Retry-After": str(H.RETRY_AFTER_STOP_S)}), _Resp(200, {})], left_s=600)
    H._get_meta(s, "u")
    assert slept == [H.RETRY_AFTER_STOP_S + 1]


def test_a_long_retry_after_stops_the_pass_at_once(tmp_path, monkeypatch):
    run, fetched = _setup(tmp_path, monkeypatch, {"A": 4},
                          outcome=lambda db, p: H.Throttled("HTTP 429 with Retry-After 3600 s", stop_now=True)
                          if p.endswith("A2.px") else "data")
    res = run()
    assert fetched == ["A/f/A1.px", "A/f/A2.px"], fetched
    assert "2 table(s) deferred after an HTTP 429 asked to wait over 60 s" in res.error, res.error


def test_a_retry_after_longer_than_the_budget_left_stops_instead_of_waiting(slept):
    s, _log = _sess([_Resp(429, headers={"Retry-After": "60"}), _Resp(200, {})], left_s=20)
    with pytest.raises(H.BudgetCut):
        H._get_meta(s, "u")
    assert slept == []


# ---- the budget itself ------------------------------------------------------------------------------

@pytest.mark.parametrize("window_min,env,want", [(45, None, 30.0), (20, None, 10.0), (45, "12", 12.0),
                                                 (8, None, 2.0), (3, None, 1.5), (45, "nan", 30.0),
                                                 (45, "0", 30.0), (45, "-5", 30.0), (0, None, 30.0),
                                                 (0, "12", 12.0)])
def test_the_unit_window_can_only_lower_the_budget(monkeypatch, window_min, env, want):
    monkeypatch.setattr(MS, "unit_window_s", lambda: (window_min * 60.0, "armed alarm"))
    if env is None:
        monkeypatch.delenv("HAGSTOFA_BUDGET_MIN", raising=False)
    else:
        monkeypatch.setenv("HAGSTOFA_BUDGET_MIN", env)
    assert H._budget_minutes()[0] == want


def test_a_disabled_unit_timeout_does_not_become_a_zero_budget(monkeypatch):
    """Review R1636 P2: AQUEDUCT_UNIT_TIMEOUT_MIN=0 (no hard limit) read as a 0-min budget, so every
    table was deferred, silently. The real window reader runs here (no alarm armed in a test)."""
    monkeypatch.setenv("AQUEDUCT_UNIT_TIMEOUT_MIN", "0")
    monkeypatch.delenv("HAGSTOFA_BUDGET_MIN", raising=False)
    assert H._budget_minutes()[0] == H.BUDGET_MIN


def test_the_budget_clock_starts_before_the_store_is_read(tmp_path, monkeypatch):
    events = []
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "Deadline", lambda minutes: events.append("clock") or _DL())
    monkeypatch.setattr(H, "_load_catalog", lambda: events.append("catalogue") or _cat({"A": 1}))
    real = H._per_table_profile
    monkeypatch.setattr(H, "_per_table_profile", lambda *a, **k: events.append("profile") or real(*a, **k))
    monkeypatch.setattr(H, "_fetch_table", lambda *a, **k: ([], "quiet"))
    H.update(types.SimpleNamespace(config={}, key="hagstofa/_all"), None)
    assert events[:3] == ["clock", "catalogue", "profile"], events


def test_a_structural_pass_that_also_stopped_on_the_budget_says_so(tmp_path, monkeypatch):
    """finalize() names only the structural tables; the deferral clause goes before that list."""
    run, _f = _setup(tmp_path, monkeypatch, {"A": 3},
                     outcome=lambda db, p: "structural" if p.endswith("A1.px") else "data", dl=_DL(after=1))
    res = run()
    assert res.status == "structural"
    assert "2 table(s) deferred by budget" in res.error, res.error
    assert res.error.index("deferred by budget") < res.error.index("["), "before the offender list"


def test_a_transient_pass_that_also_stopped_on_the_budget_says_so(tmp_path, monkeypatch):
    run, _f = _setup(tmp_path, monkeypatch, {"A": 3},
                     outcome=lambda db, p: H.TransientError("x") if p.endswith("A1.px") else "data",
                     dl=_DL(after=1))
    res = run()
    assert res.status == "partial" and "transient" in res.error and "deferred by budget" in res.error, res.error


# ---- the whole pass on a virtual clock --------------------------------------------------------------

def test_over_several_passes_the_tree_table_gets_its_verdict_and_every_table_is_reached(tmp_path, monkeypatch):
    """Measured shape (2026-10): the 400/404 table sits ~23 min into a pass and its search needs ~13 min,
    so it was cut on every pass and nothing was ever cached. Here every table costs 2 s and the search
    13 min; the search runs only with TREE_SEARCH_MIN left, as _missing_verdict does."""
    clock = {"t": 0.0}
    monkeypatch.setattr(H.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(H.time, "sleep", lambda s: clock.__setitem__("t", clock["t"] + s))
    monkeypatch.setattr(MS, "unit_window_s", lambda: (45 * 60.0, "armed alarm"))
    for v in ("HAGSTOFA_BUDGET_MIN", "AQUEDUCT_BUDGET_MIN_OVERRIDE", "AQUEDUCT_BACKEND"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: _cat({"A": 700, "B": 700}))
    TREE = "A/f/A690.px"                                         # reached at ~23 min of a pass from the top
    seen, verdict = set(), []

    def fetch(sess, db, path, prefix, since, dl=None, **k):
        key = f"{db}/{path}"
        if key == TREE and not verdict:
            if H._dl_left_s(dl) < H.TREE_SEARCH_MIN * 60:
                return [], "deferred"
            clock["t"] += 13 * 60
            verdict.append(clock["t"])
            seen.add(key)
            return [], "quiet"
        clock["t"] += 2.0
        seen.add(key)
        return [(f"{prefix}:Dim=0", B, 1.0)], "data"
    monkeypatch.setattr(H, "_fetch_table", fetch)
    ends = []
    for _pass in range(4):
        t0 = clock["t"]
        H.update(types.SimpleNamespace(config={}, key="hagstofa/_all"), None)
        ends.append((clock["t"] - t0) / 60)
        if len(seen) == 1400:
            break
    assert verdict, "the search finished and its verdict could be cached"
    assert len(seen) == 1400, f"{len(seen)} of 1400 tables reached in {len(ends)} passes"
    assert max(ends) <= 30.1, ends

def test_a_pass_whose_requests_hang_near_the_end_still_ends_inside_its_budget(tmp_path, monkeypatch):
    """Every table costs 2 s (a GET and a POST) until 29 min; from then on every request hangs until
    its timeout. The old 90/120 s x 4 attempts would run ~8 min per table past the budget."""
    clock = {"t": 0.0}
    monkeypatch.setattr(H.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(H.time, "sleep", lambda s: clock.__setitem__("t", clock["t"] + s))
    monkeypatch.setattr(MS, "unit_window_s", lambda: (45 * 60.0, "armed alarm"))
    monkeypatch.delenv("HAGSTOFA_BUDGET_MIN", raising=False)
    monkeypatch.delenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", raising=False)
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: _cat({"A": 2000}))

    class Sess:
        def __init__(self):
            self.headers = {}

        def _req(self, timeout):
            if clock["t"] >= 29 * 60:
                clock["t"] += timeout
                raise requests.Timeout("hung")
            clock["t"] += 1.0
            return _Resp(200, {"variables": []})

        def get(self, url, timeout=None):
            return self._req(timeout)

        def post(self, url, json=None, timeout=None):
            return self._req(timeout)

    monkeypatch.setattr(H, "_session", lambda: Sess())

    def fetch(sess, db, path, prefix, since, **k):
        H._get_meta(sess, f"{H.BASE}/{db}/{path}/")
        H._post_data(sess, f"{H.BASE}/{db}/{path}/", {})
        return [(f"{prefix}:Dim=0", B, 1.0)], "data"
    monkeypatch.setattr(H, "_fetch_table", fetch)
    res = H.update(types.SimpleNamespace(config={}, key="hagstofa/_all"), None)
    assert res.status == "partial" and "deferred by budget" in res.error, res.error
    assert "transient" not in res.error, "the hung table near the end is deferred, not failed"
    assert _bookmark(tmp_path) and len(_keys(tmp_path, "A")) == int(_bookmark(tmp_path)[len("A/f/A"):-3])
    assert clock["t"] <= 30 * 60 + H.REQUEST_TIMEOUT_FLOOR_S, f"ended at {clock['t'] / 60:.2f} min"
    assert clock["t"] < (45 - H.HARD_MARGIN_MIN) * 60
