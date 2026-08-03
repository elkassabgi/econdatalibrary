"""CALL every fetcher's current_vintage(). Reading it is not testing it.

WHY THIS EXISTS. bls.current_vintage() unpacked two values from a function that returns four, so it
raised ValueError on every call where any survey existed on disk — which is always. It never
returned a token and never returned None either; its own docstring promised "returns None ... so
the strategy fetches anyway" and that fallback was unreachable code. The break was introduced when
_survey_vintage's return widened: one of its two callers was updated, the other was not (R292).

Nothing caught it for the life of that commit, and the reasons generalise to every fetcher here:

  - The exception surfaces inside BulkSnapshotIfChanged.detect_change, which does not catch
    ValueError, so it looks like a source failure rather than a probe defect.
  - The health gate reports each source from STATE, not by probing. bls printed
    `RED-DATA bls weekly succ_age 2d obs_age 63d`, which reads as "runs fine, upstream is quiet" —
    the most dismissable line in the table.
  - updater/strategies/_bls_selftest.py calls current_vintage() and would have failed instantly.
    It is not wired into anything.
  - Two ledger entries already recorded bls as fixed. Both were true about what they measured.
    Neither called this function.

So the instrument that was missing is the cheapest one imaginable: call it and see. That is all
this does.

WHAT COUNTS AS PASSING. current_vintage's contract is two-valued, and BOTH values are fine:
  a real token  -> upstream vintage is determinable, the strategy can skip an unchanged bulk
  None          -> undeterminable, the strategy must fetch anyway (cadence-gated)
Only an EXCEPTION is a failure, because an exception is neither, and every caller in
updater/strategies/ is written against those two outcomes.

TransientError is NOT a failure here. Several fetchers raise it by design when the probe itself
cannot reach the network, and manual_vintage.py explicitly lets it propagate ("current_vintage()
raises TransientError on a probe network/5xx failure; let it"). Counting a publisher's bad minute
as a code defect would make this audit red for reasons nobody can fix, and an audit that is always
red is one nobody reads (the lesson audit_untouched_files.py already carries).

This DOES hit the network — one cheap HEAD/listing per source. That is the point: a mocked probe
would have passed the bls bug, because the bug is in how the real return value is unpacked.

    python tools/audit_current_vintage.py                 # every fetcher
    python tools/audit_current_vintage.py bls census      # just these
    python tools/audit_current_vintage.py --live          # only registry live:true sources
"""
from __future__ import annotations
import argparse
import glob
import importlib
import os
import sys
import traceback

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

os.environ.setdefault("AQUEDUCT_BACKEND", "r2")

from updater import registry                                       # noqa: E402
from updater.errors import TransientError                          # noqa: E402

FETCHER_DIR = os.path.join(ROOT, "updater", "strategies", "fetchers")


def all_fetchers() -> list:
    out = []
    for p in sorted(glob.glob(os.path.join(FETCHER_DIR, "*.py"))):
        b = os.path.basename(p)[:-3]
        if b.startswith("_") or b == "base":
            continue
        out.append(b)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="*", help="fetcher module names (default: all)")
    ap.add_argument("--live", action="store_true", help="only registry live:true sources")
    a = ap.parse_args()

    names = a.sources or all_fetchers()
    if a.live:
        live = {e["source_id"] for e in registry.load()["sources"] if e.get("live")}
        names = [n for n in names if n in live]

    tok = none = absent = transient = 0
    broken = []
    for m in names:
        try:
            f = importlib.import_module(f"updater.strategies.fetchers.{m}")
        except Exception as e:                                      # noqa: BLE001
            broken.append((m, f"IMPORT {type(e).__name__}: {e}"))
            print(f"{m:<26} IMPORT-FAIL {type(e).__name__}: {e}", flush=True)
            continue
        if not hasattr(f, "current_vintage"):
            absent += 1
            continue
        try:
            v = f.current_vintage("_all")
        except TransientError as e:
            # By design for several fetchers. Publisher weather, not a defect.
            transient += 1
            print(f"{m:<26} transient (probe could not reach upstream): {e}", flush=True)
            continue
        except Exception as e:                                     # noqa: BLE001
            broken.append((m, f"{type(e).__name__}: {e}"))
            print(f"{m:<26} RAISES {type(e).__name__}: {e}", flush=True)
            if os.environ.get("AUDIT_CV_TRACE"):
                traceback.print_exc()
            continue
        if v is None:
            none += 1
            print(f"{m:<26} None  (undeterminable — valid, strategy fetches anyway)", flush=True)
        else:
            tok += 1
            print(f"{m:<26} OK    {v!r}", flush=True)

    print(f"\n{len(names)} fetcher(s): {tok} token, {none} None, {transient} transient, "
          f"{absent} without current_vintage, {len(broken)} BROKEN")
    if broken:
        print("\nBROKEN — these raise something the strategy layer does not handle, so the source's "
              "change-probe cannot work at all:")
        for m, e in broken:
            print(f"    {m:<26} {e}")
        return 1
    print("no fetcher's current_vintage raises — the probe path is intact for all of them")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
