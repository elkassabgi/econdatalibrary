"""insee_melodi's budget-bounded sweep must resume, or its tail is never reached.

Measured 2026-08-07 on a real run: `for flow in flows:` under Deadline(BUDGET_MIN=25) with no
bookmark deferred 26 flows, and SIX of the deferred held ZERO rows — DS_SIDE_CREA_COM,
DS_SIDE_CREA_ENT_COM, DS_SIDE_CREA_ETAB_COM, DS_SIDE_STOCKS_COM, DS_SOCIAL_ECONOMY,
DS_TOUR_CAP — sitting at positions 123-143 of the publisher's 145. Starting at the head every
run, they would never have been fetched. This is the R190 class (stat_slovenia, task #41).
"""
from __future__ import annotations

import inspect

from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers._common import rotate_after


def _flows(*codes):
    return [{"code": c} for c in codes]


def _key(f):
    return f.get("code", "")


class TestRotationSemantics:
    def test_resumes_just_past_the_bookmark_and_wraps(self):
        out = rotate_after(_flows("A", "B", "C", "D", "E"), "C", key=_key)
        assert [f["code"] for f in out] == ["D", "E", "A", "B", "C"]

    def test_the_tail_reaches_the_front_on_the_next_run(self):
        """The property that matters: a flow at the END is at the FRONT once the sweep has
        stopped just before it."""
        flows = _flows(*[f"F{i:03d}" for i in range(145)])
        out = rotate_after(flows, "F122", key=_key)
        assert out[0]["code"] == "F123"          # the first deferred flow leads next run
        assert [f["code"] for f in out[:6]] == [f"F{i}" for i in range(123, 129)]

    def test_unknown_or_empty_bookmark_starts_at_the_top(self):
        """A first run, a renamed flow or a corrupt bookmark must skip NOTHING."""
        flows = _flows("A", "B", "C")
        assert rotate_after(flows, "", key=_key) == flows
        assert rotate_after(flows, "RENAMED_AWAY", key=_key) == flows


class TestBookmarkIsWiredCorrectly:
    def test_update_rotates_its_flow_list(self):
        src = inspect.getsource(M.update)
        assert "rotate_after(flows, load_rotation(out_dir)" in src, \
            "the flow list must be rotated, or the sweep restarts at the head every run"

    def test_bookmark_is_saved_AFTER_the_deferral_check(self):
        """Stamped before the check it would record a flow that was DEFERRED, and the next
        run — starting just past it — would skip the very flow the deferral promised to
        return to."""
        src = inspect.getsource(M.update)
        defer = src.index("tally.deferred_unit(code)")
        save = src.index("save_rotation(out_dir, code)")
        assert save > defer, "save_rotation must come after the deferral branch"

    def test_bookmark_is_saved_per_flow_not_once_at_the_end(self):
        """The orchestrator's per-source cap KILLS a source rather than breaking its loop, so
        an end-of-function save is lost entirely on a kill (R273)."""
        src = inspect.getsource(M.update)
        after_save = src[src.index("save_rotation(out_dir, code)"):]
        assert "for " not in after_save.split("save_rotation", 1)[0], \
            "save_rotation must sit INSIDE the per-flow loop"
