"""A fetcher's thread pool must CANCEL its backlog when the unit timeout fires, not drain it.

THE OUTAGE THIS PINS (2026-08-07 daily updater, run 31155835244). The orchestrator's per-unit
hard limit is a SIGALRM that raises `UnitTimeout` in the main thread. It armed correctly at 45
minutes — the log says so. It still could not stop anything:

    10:02  owid starts and submits all 150 slugs to a 6-worker pool
    10:47  SIGALRM fires, UnitTimeout raised inside the as_completed loop
    10:47  `with ThreadPoolExecutor(...) as ex` begins shutdown(wait=True) on the way out,
           which waits for EVERY future already submitted
    12:32  GitHub kills the step at its 250-minute cap

owid printed nothing for 150 minutes and no timeout message ever appeared, because the exception
could not escape the context manager. The whole daily run died with it — and the four runs before
it that day failed too. A cap you cannot escape is not a cap.

`_common.cancellable_pool` shuts down with cancel_futures=True, so the queued backlog is dropped
and only the at most max_workers tasks already running are joined.
"""
from __future__ import annotations

import os
import re
import sys
import time
from concurrent.futures import as_completed

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

FETCHERS = ("boe", "ksh_stadat", "ons_uk", "owid")


def test_an_exception_cancels_the_backlog_instead_of_draining_it():
    from updater.strategies.fetchers._common import cancellable_pool
    t0 = time.time()
    try:
        with cancellable_pool(2) as ex:
            futs = [ex.submit(time.sleep, 2) for _ in range(20)]
            for _ in as_completed(futs):
                raise RuntimeError("stand-in for UnitTimeout")
    except RuntimeError:
        pass
    elapsed = time.time() - t0
    # Draining 20 two-second tasks on 2 workers takes ~20s. Cancelling joins only the 2 running.
    assert elapsed < 8, (
        f"shutdown took {elapsed:.1f}s — the backlog is still being drained, which is exactly "
        f"how a 45-minute cap turned into a 150-minute hang")


def test_no_fetcher_reintroduces_a_draining_pool():
    """`with ThreadPoolExecutor(...) as ex` is the shape that cannot be interrupted. Any fetcher
    using it is one slow publisher away from eating a whole run."""
    bad = []
    d = os.path.join(ROOT, "updater", "strategies", "fetchers")
    for fn in sorted(os.listdir(d)):
        # _common.py is the helper: it QUOTES the broken shape in its docstring to explain the
        # outage, so scanning its prose finds a match that is documentation, not code.
        if not fn.endswith(".py") or fn == "_common.py":
            continue
        src = open(os.path.join(d, fn), encoding="utf-8").read()
        code = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
        if re.search(r"with\s+ThreadPoolExecutor\s*\(", code):
            bad.append(fn)
    assert not bad, (
        f"{bad} use `with ThreadPoolExecutor(...)`, whose implicit shutdown(wait=True) waits for "
        f"every submitted future while an exception propagates. Use "
        f"_common.cancellable_pool(max_workers) instead.")


def test_the_four_known_fetchers_use_the_cancellable_pool():
    """A negative check alone would pass if someone deleted the pools entirely."""
    d = os.path.join(ROOT, "updater", "strategies", "fetchers")
    for fn in FETCHERS:
        src = open(os.path.join(d, f"{fn}.py"), encoding="utf-8").read()
        assert "cancellable_pool(MAX_WORKERS)" in src, f"{fn} no longer uses cancellable_pool"


def test_the_helper_still_joins_running_work():
    """cancel_futures must not become wait=False: threads still writing parquet files when the
    orchestrator moves to the next source is a worse failure than the hang."""
    import inspect
    from updater.strategies.fetchers import _common
    src = inspect.getsource(_common.cancellable_pool)
    assert "shutdown(wait=True, cancel_futures=True)" in src


def test_detect_change_runs_inside_the_unit_deadline():
    """The second outage (run 31224822131, WITH the pool fix on board). owid entered at 23:23
    and produced nothing until GitHub killed the step at 02:55 — 212 minutes inside
    `strat.detect_change`, which sat 85 lines BEFORE the `_unit_deadline` block, so no cap
    covered it. requests' timeout=180 is per-socket-op: a slow-drip response resets it on every
    byte, making one GET effectively unbounded. The probe must sit under the same ceiling as the
    fetch."""
    import inspect
    from updater import orchestrate as O
    src = inspect.getsource(O.run_once)
    i_detect = src.index("strat.detect_change(unit, us)")
    # the nearest _unit_deadline entry BEFORE the call must exist (probe is wrapped)
    head = src[:i_detect]
    assert "_unit_deadline(" in head.rsplit("try:", 1)[-1] or            "_unit_deadline(" in head[-600:], (
        "strat.detect_change is no longer wrapped in _unit_deadline — a hung vintage probe "
        "eats the entire run again (212 minutes of owid, twice)")
    assert "except UnitTimeout" in src[i_detect:i_detect + 900], (
        "UnitTimeout from the probe is not booked as transient_fail at the detect call site")
