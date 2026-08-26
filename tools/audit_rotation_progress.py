"""Rotation-progress audit — the R377/R285 instrument the ROTATING class names as owed.

ROTATING (updater/health.py, 2026-08-26) certifies that nothing FAILED and the budget
stopped the sweep; it deliberately does NOT certify the rotation is advancing — no state
snapshot can. This tool answers that question from the only place that can: the STORE's
own write times. R285's behavioural test, mechanized: a rotating source leaves SCATTERED
write times across its files; a stuck rotation (the R190 fixed-prefix re-walk) leaves ONE
contiguous stale tail. R379/R377's lesson is the method's foundation: run notes tell you
a sweep was truncated; only the store tells you what was never reached — and a
name-based "did the first-deferred unit change?" check gives a FALSE all-clear (ecb's
first-deferred name differed in all seven stuck runs while the sweep never passed
position 297 of 540).

Per live source with >= MIN_FILES store parquets, ONE R2 listing (R140: never poll —
this runs on demand or weekly, one pass):

  stale_share = files older than max(21 days, 3 x cadence_days)  /  files

Verdict STUCK when stale_share > 0.5 — a live rotation should have touched half its
store within three cadences. Calibrated against the recorded episodes, not invented:
ecb's stuck era was 280/540 = 52% (fires) and its healthy state 15/540 = 3% (quiet);
ssb's stuck era 103/186 >= 21d = 55% (fires). A fresh-write-day count is printed as
context, never judged: bulk sources legitimately rewrite everything in one day.

NOT a gate (exit 0 always, unless --fail-on-stuck): the honest response to a STUCK
verdict is reading that source's rotation bookmark and run history, not a red run.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MIN_FILES = 20
STUCK_SHARE = 0.5
FLOOR_DAYS = 21.0


def assess_listing(age_days: "list[float]", cadence_days: float) -> "tuple[float, str]":
    """(stale_share, verdict) for one source's per-file ages. Pure — the tested core."""
    if not age_days:
        return 0.0, "EMPTY"
    horizon = max(FLOOR_DAYS, 3.0 * cadence_days)
    stale = sum(1 for a in age_days if a > horizon)
    share = stale / len(age_days)
    return share, ("STUCK" if share > STUCK_SHARE else "OK")


def budgeted_sources() -> "set[str]":
    """Sources whose fetcher constructs a Deadline — the ONLY population this test is
    valid for. A bulk snapshot source legitimately writes NOTHING for months when its
    upstream is static; write-time dispersion means nothing there. First cut judged all
    live sources and 'found' wid/eia/ons_uk stuck — quiet snapshot/local-route stores,
    not stuck rotations (R267: a threshold finds candidates, reading a record decides;
    the scoping IS part of the instrument)."""
    fdir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "updater", "strategies", "fetchers")
    out = set()
    for f in os.listdir(fdir):
        if not f.endswith(".py") or f.startswith("_"):
            continue
        try:
            src = open(os.path.join(fdir, f), encoding="utf-8", errors="replace").read()
        except OSError:
            continue
        if "Deadline(" in src:
            out.add(f[:-3])
    return out


def main() -> int:
    from updater import registry
    from updater.strategies.base import CADENCE_DAYS
    from core import r2_util

    c = r2_util.client()
    now = dt.datetime.now(dt.timezone.utc)
    reg = {e["source_id"]: e for e in registry.load().get("sources", [])}
    budgeted = budgeted_sources()
    live = [sid for sid, e in sorted(reg.items()) if e.get("live") and sid in budgeted]
    stuck = []
    print(f"rotation-progress audit over {len(live)} live BUDGETED sources "
          f"(>= {MIN_FILES} store files each; horizon = max({FLOOR_DAYS:.0f}d, 3x data cadence))")
    for sid in live:
        # the DATA clock, not the check clock: dst polls daily but its data moves ~84d;
        # judging its store by the polling cadence is the R327-family confusion.
        cad_key = reg[sid].get("data_cadence") or reg[sid].get("cadence", "monthly")
        cadence = CADENCE_DAYS.get(cad_key, 28) if isinstance(cad_key, str) else float(cad_key)
        ages = []
        token = None
        while True:
            kw = {"Bucket": "econ-data", "Prefix": f"clean_full/{sid}/"}
            if token:
                kw["ContinuationToken"] = token
            resp = c.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                if o["Key"].endswith(".parquet") and "/_" not in o["Key"].rsplit("/", 1)[-1][:1]:
                    ages.append((now - o["LastModified"]).total_seconds() / 86400.0)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        if len(ages) < MIN_FILES:
            continue
        share, verdict = assess_listing(ages, float(cadence))
        fresh_days = len({int(a) for a in ages if a <= 14})
        line = (f"  {verdict:5s} {sid:28s} files={len(ages):5d} stale_share={share:5.1%} "
                f"fresh-write-days(14d)={fresh_days}")
        if verdict == "STUCK":
            stuck.append(sid)
            print(line)
        elif "--verbose" in sys.argv:
            print(line)
    print(f"\nverdicts: {len(stuck)} STUCK of the sources large enough to judge")
    for s in stuck:
        print(f"  STUCK: {s} — read its rotation bookmark (load_rotation / sidecar) and "
              f"whether consecutive runs' deferred sets repeat the same suffix (R377)")
    if "--fail-on-stuck" in sys.argv and stuck:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
