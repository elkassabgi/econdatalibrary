"""A `partial` run must re-derive the CSVs of the series it DID merge.

THE BUG THIS PINS (2026-08-07). The orchestrator's contract step 5 re-derives changed
series' CSVs in the same run, but the gate read `if status == "ok"`. The reasoning was that
a partial leaves the vintage un-bumped, so "the next run re-checks + re-derives" — which
assumes a source EVENTUALLY returns ok.

Chronically partial sources never do. One flaky sub-unit out of eighty is enough, every run,
forever, and the run reports `partial` with the parquet published perfectly well. Measured
across the whole fleet that day: 136 of 173 sources with run history had NEVER returned ok,
~56 of them live AND served.

The consequence was silent and user-visible. worldbank_esg has returned `partial` on 4 of
the 4 runs it has ever had, so the pipeline never re-derived its CSVs once:

    14 of 40 sampled R2 objects disagreed with the store, and not merely by a missing tail —
    SH.DYN.MORT:PAK served 58.5 for 2023 where the publisher had since revised it to 57.8,
    and SH.DYN.MORT:CAN served 5.1 where the store held 5.4. Users downloading a "current"
    series got last year's numbers AND superseded revisions.

Independently reproduced the same day on two more chronically-partial sources: hagstofa
(2 of 25 sampled objects stale) and stat_slovenia (1 of 25).

WHY `partial` IS THE RIGHT CALL and not a papering-over: a partial's succeeded sub-units DID
merge rows, and `res.series_cursors` names exactly those series. run_once ALREADY writes
their freshness cursors on a partial, with the comment "the parquet holding these
observations DID publish". Recording a series as fresh while refusing to re-derive the bytes
a user downloads is a contradiction, not a safety margin.

`transient_fail` and `no_change` stay excluded: nothing merged, so there is nothing to
re-derive. That exclusion is the negative control below — a fix that simply derived on every
status would pass the positive test and be wrong.
"""
from __future__ import annotations

import inspect
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def test_partial_derives_because_its_subunits_merged():
    from updater.orchestrate import _should_derive_csvs
    assert _should_derive_csvs("partial") is True, (
        "a partial's succeeded sub-units merged rows and reported series_cursors; not "
        "re-deriving them freezes live downloads for every source that never returns ok")


def test_ok_still_derives():
    from updater.orchestrate import _should_derive_csvs
    assert _should_derive_csvs("ok") is True


def test_nothing_merged_derives_nothing():
    """THE NEGATIVE CONTROL. Deriving on every status would pass the test above while
    burning a derive pass on runs that published nothing."""
    from updater.orchestrate import _should_derive_csvs
    assert _should_derive_csvs("transient_fail") is False
    assert _should_derive_csvs("no_change") is False
    assert _should_derive_csvs("timeout") is False


def test_the_run_loop_actually_uses_the_predicate():
    """The predicate is only worth testing if the call site calls it — an inline
    `status == "ok"` alongside a correct helper is exactly the drift this catches."""
    from updater import orchestrate as O
    src = inspect.getsource(O.run_once)
    assert "_should_derive_csvs(status)" in src, \
        "run_once must gate the CSV derive on _should_derive_csvs, not an inline comparison"
    assert 'if status == "ok" and not dry:' not in src, \
        "the old ok-only gate is back; chronically-partial sources will freeze again"
