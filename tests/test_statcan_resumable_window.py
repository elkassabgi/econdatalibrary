"""statcan must be able to finish a change window across several passes.

Why this exists. The desktop heavy pass admits statcan late and kills it at the hard stop: it was
`killed_external` on 2026-09-02 (10,653 s), 09-14 (6,111 s), 09-15 (9,337 s) and 09-17 (8,907 s),
and a kill leaves no `unit_state` stamp, so the next pass re-ranked it the same way. The fetcher had
no budget of its own, so every pass restarted the same window from the first cube and the tail of the
window was never reached - work without progress.

R190's rule is that a budget without rotation or a resume set is a truncation, not a fix. So the
budget here comes with a persisted resume set: the cubes finished in a pass are remembered against
the window's `feed_since`, and the watermark moves only when the window is finished - and then only
to the OLDEST date any cube in it was fetched through, because a cube finished on an earlier pass
was not fetched through today.

These tests drive the real `update()` with fakes for the network, the store and the clock's budget,
rather than re-stating its logic in the test - a model of the code cannot catch the code drifting.
"""
from __future__ import annotations

import datetime as dt
import inspect
import json
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.errors import TransientError  # noqa: E402
from updater.strategies.fetchers import statcan as sc  # noqa: E402


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #
class FakeBlob:
    """The store and the state file. Every cube asked for is held unless named absent."""

    def __init__(self, state, absent=()):
        self.state = json.dumps(state).encode("utf-8") if state is not None else None
        self.absent = {str(p) for p in absent}
        self.saves = 0

    # state ---------------------------------------------------------------- #
    def read_bytes(self, path):
        return self.state

    def write_bytes_atomic(self, path, data):
        self.state = data
        self.saves += 1

    # store ---------------------------------------------------------------- #
    def exists(self, path):
        return os.path.basename(path).split(".")[0] not in self.absent

    def row_count(self, path):
        return 10

    def read_table(self, path, columns=None):
        return object()

    def list_parquets(self, d):
        return [os.path.join(d, "10000001.parquet")]

    def saved_state(self):
        return json.loads(self.state.decode("utf-8"))


class FakeMerge:
    def __init__(self):
        self.merged = []

    def merge_and_write(self, path, tbl, mode=None, dedup_keys=None, report_changed_keys=False):
        self.merged.append(os.path.basename(path).split(".")[0])
        return (30, "2026-09-15", {}) if report_changed_keys else (30, "2026-09-15")

    def _max_obs_date(self, tbl):
        return "2026-09-15"


class FakeDeadline:
    """Lets exactly `allow` cubes be STARTED, then reports the budget spent."""

    def __init__(self, allow):
        self.allow = allow
        self.asked = 0
        self.budget_min = 45.0

    def spent(self):
        self.asked += 1
        return self.asked > self.allow


def _wire(monkeypatch, *, state, changed, allow=99, absent=(), tail_raises_for=()):
    blob, merge = FakeBlob(state, absent=absent), FakeMerge()
    fetched: list = []

    def _tail(vmap, start, end):
        pid = vmap["pid"]
        fetched.append(pid)
        if pid in {str(p) for p in tail_raises_for}:
            raise TransientError(f"{pid}: pretend the tail fetch failed")
        return types.SimpleNamespace(num_rows=5)

    monkeypatch.setattr(sc, "blob", blob)
    monkeypatch.setattr(sc, "merge", merge)
    monkeypatch.setattr(sc, "_changed_pids", lambda since: set(changed))
    monkeypatch.setattr(sc, "_disk_vector_map",
                        lambda path: {"pid": os.path.basename(path).split(".")[0], "v1": "1.1"})
    monkeypatch.setattr(sc, "_fetch_cube_tail", _tail)
    monkeypatch.setattr(sc, "Deadline", lambda minutes=None: FakeDeadline(allow))
    monkeypatch.setattr(sc.os, "makedirs", lambda *a, **k: None)
    return blob, merge, fetched


def _feed_since(watermark):
    return (dt.date.fromisoformat(watermark) - dt.timedelta(days=sc.FEED_SLACK_DAYS)).isoformat()


TODAY = dt.date.today().isoformat()
WM = "2026-09-10"


# --------------------------------------------------------------------------- #
# the budget stops work without losing it
# --------------------------------------------------------------------------- #
def test_the_budget_stops_starting_cubes_and_persists_what_was_done(monkeypatch):
    blob, merge, fetched = _wire(monkeypatch, state={"last_release_date": WM, "window": {}},
                                 changed=[1, 2, 3, 4, 5], allow=2)
    res = sc.update(None, None)

    assert len(merge.merged) == 2, f"the budget allowed 2 cubes, {len(merge.merged)} were merged"
    st = blob.saved_state()
    assert st["last_release_date"] == WM, (
        "the watermark must NOT advance over a window that still owes work - that is the silent "
        "skip this window exists to prevent")
    assert st["window"]["feed_since"] == _feed_since(WM)
    assert set(st["window"]["done"]) == set(merge.merged)
    assert res.new_vintage is None, (
        "a capped run must not stamp a vintage that says 'fully current', or the strategy skips "
        "the backlog at the next tick")


def test_the_next_pass_resumes_and_refetches_nothing(monkeypatch):
    """The resume set is only worth having if it actually saves the work."""
    done = {"1": "2026-09-16", "2": "2026-09-16"}
    blob, merge, fetched = _wire(
        monkeypatch,
        state={"last_release_date": WM, "window": {"feed_since": _feed_since(WM), "done": done}},
        changed=[1, 2, 3, 4, 5], allow=99)
    sc.update(None, None)

    assert sorted(fetched) == ["3", "4", "5"], (
        f"cubes finished in the earlier pass must not be fetched again; fetched {sorted(fetched)}")
    st = blob.saved_state()
    assert st["window"] == {}, "a finished window must be cleared"


def test_the_watermark_lands_on_the_oldest_through_date_not_today(monkeypatch):
    """A window spread over passes was fetched through different dates. Taking `today` would jump
    over every release between the earlier pass and this one."""
    done = {"1": "2026-09-16", "2": "2026-09-16"}
    blob, merge, fetched = _wire(
        monkeypatch,
        state={"last_release_date": WM, "window": {"feed_since": _feed_since(WM), "done": done}},
        changed=[1, 2, 3], allow=99)
    sc.update(None, None)

    st = blob.saved_state()
    assert st["last_release_date"] == "2026-09-16", (
        f"expected the oldest through-date of the window, got {st['last_release_date']!r} "
        f"(today is {TODAY})")
    assert st["last_release_date"] != TODAY


def test_a_window_for_another_feed_since_is_discarded(monkeypatch):
    """The resume set is identified by the window it belongs to. A stale one must not suppress
    fetches in a new window."""
    stale = {"feed_since": "1999-01-01", "done": {"1": "1999-01-02", "2": "1999-01-02"}}
    blob, merge, fetched = _wire(
        monkeypatch, state={"last_release_date": WM, "window": stale},
        changed=[1, 2], allow=99)
    sc.update(None, None)

    assert sorted(fetched) == ["1", "2"], (
        f"a resume set from another window must not skip anything; fetched {sorted(fetched)}")


def test_a_cube_the_feed_adds_later_is_not_covered_by_a_stale_resume_set(monkeypatch):
    """`done` can hold cubes the feed no longer lists while a newly listed cube has never been
    fetched. Completion is set containment, so the new cube is fetched and only then does the
    window close."""
    done = {"1": "2026-09-16", "2": "2026-09-16", "3": "2026-09-16"}
    blob, merge, fetched = _wire(
        monkeypatch,
        state={"last_release_date": WM, "window": {"feed_since": _feed_since(WM), "done": done}},
        changed=[1, 2, 9], allow=99)
    sc.update(None, None)

    assert fetched == ["9"], f"the newly listed cube must be fetched; fetched {fetched}"
    st = blob.saved_state()
    assert st["window"] == {}
    assert st["last_release_date"] == "2026-09-16"


def test_a_capped_pass_leaves_the_newly_listed_cube_owed(monkeypatch):
    """Same set-up, no budget left: the count of done (3) already equals the count of changed (3),
    so a completion test that counted would close the window with cube 9 never fetched."""
    done = {"1": "2026-09-16", "2": "2026-09-16", "3": "2026-09-16"}
    blob, merge, fetched = _wire(
        monkeypatch,
        state={"last_release_date": WM, "window": {"feed_since": _feed_since(WM), "done": done}},
        changed=[1, 2, 9], allow=0)
    res = sc.update(None, None)

    assert fetched == [], "nothing may be started once the budget is spent"
    st = blob.saved_state()
    assert st["last_release_date"] == WM, "the watermark must not move over an unfetched cube"
    assert "9" not in st["window"]["done"]
    assert res.new_vintage is None


def test_a_transient_sub_fault_keeps_the_window_and_the_watermark(monkeypatch):
    """A cube whose tail fetch failed is not remembered, so the window stays open and the
    watermark stays put - the pre-existing contract, pinned against the new code path."""
    blob, merge, fetched = _wire(monkeypatch, state={"last_release_date": WM, "window": {}},
                                 changed=[1, 2, 3], allow=99, tail_raises_for=[2])
    sc.update(None, None)

    st = blob.saved_state()
    assert st["last_release_date"] == WM
    assert "2" not in st["window"]["done"], "a failed cube must not be recorded as done"
    assert set(st["window"]["done"]) == {"1", "3"}


def test_the_completion_test_is_containment_not_a_count():
    """The regression guard with teeth. `len(done) >= len(changed)` is true whenever the resume set
    holds as many cubes as the feed lists, even if they are not the same cubes."""
    # Match the ASSIGNMENT, not any mention: the code comments quote the rejected expression on
    # purpose, and a test that cannot tell an explanation from the thing it explains is noise.
    code = [ln.split("#")[0] for ln in inspect.getsource(sc).splitlines()]
    assigns = [ln.strip() for ln in code if ln.strip().startswith("everything =")]
    assert len(assigns) == 1, f"expected one completion test, found {assigns}"
    assert "len(" not in assigns[0], (
        f"completion by count can close a window over a cube that was never fetched: {assigns[0]}")
    assert "<= set(done)" in assigns[0], f"completion must be set containment: {assigns[0]}"


def test_the_budget_is_documented_and_finite():
    assert 0 < sc.BUDGET_MIN <= 120, sc.BUDGET_MIN
    assert sc.SAVE_EVERY >= 1
    src = inspect.getsource(sc)
    assert "feed_since" in src
    # The resumption mechanism must stay NAMED: tests/test_budget_needs_resumption.py recognises
    # this constant, and a budget whose resumption is anonymous is one rename away from reading as
    # "budgeted, no resumption" - the truncation R190 is about.
    assert sc.RESUME_WINDOW_KEY, "the resume-window state key must be a named constant"
    assert "state[RESUME_WINDOW_KEY]" in src


@pytest.mark.parametrize("allow", [0, 1, 3, 99])
def test_no_budget_setting_ever_advances_the_watermark_over_owed_work(monkeypatch, allow):
    """The property that matters, swept across budgets: either the window is finished, or the
    watermark has not moved."""
    blob, merge, fetched = _wire(monkeypatch, state={"last_release_date": WM, "window": {}},
                                 changed=[1, 2, 3, 4, 5], allow=allow)
    sc.update(None, None)
    st = blob.saved_state()
    finished = st.get("window") == {}
    assert finished or st["last_release_date"] == WM, (
        f"allow={allow}: watermark moved to {st['last_release_date']!r} with work still owed")
    if finished:
        assert len(merge.merged) == 5
