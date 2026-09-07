"""Pay a csv_desktop_owed debt: clear the rows of flow-grain ids whose served CSV has been
rewritten SINCE the debt was noted, then push the state store.

    python tools/clear_csv_desktop_owed.py --source eurostat                 # report only
    python tools/clear_csv_desktop_owed.py --source eurostat --ids FILE --apply

WHAT THE ROW MEANS. The cloud derive could not take the id (its whole-file CSV is over the
runner's row ceiling — see updater/derive.py FLOW_DERIVE_MAX_ROWS), so its served CSV sits at
the previous vintage until the desktop derives it:

    python -m core.derive_csv --bucket econ-data --source <src> --only <ids file>

and reads every served object back against R2's parquet (never the local mirror — R385).

WHAT THIS TOOL CHECKS BEFORE CLEARING, per id, from the SERVED side (R345: the running system
is the evidence): the object `series/<id>.csv` exists on R2 and its LastModified is LATER than
the row's `noted_utc`. A row whose served object is older than the debt is NOT cleared and is
named. This is a necessary condition, not the byte read-back — do the read-back first.

STATE LIFECYCLE (R529/R340): pull -> clear -> push, like every durable write to the store;
refused while `logs/local_heavy.lock` is present, because a pull would wholesale-replace what a
running pass is writing. Only the printed "cleared ... and pushed" line means the debt is paid.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import os
import subprocess
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

BUCKET = "econ-data"


def served_after(s3, series_id: str, noted_utc: str, bucket: str = BUCKET):
    """(ok, detail): does the served object exist and postdate the debt?"""
    key = f"series/{urllib.parse.quote(series_id, safe='')}.csv"
    try:
        h = s3.head_object(Bucket=bucket, Key=key)
    except Exception as e:                                             # noqa: BLE001
        return False, f"no served object ({type(e).__name__})"
    lm = h["LastModified"]
    if lm.tzinfo is None:
        lm = lm.replace(tzinfo=dt.timezone.utc)
    try:
        noted = dt.datetime.fromisoformat(str(noted_utc).replace("Z", "+00:00"))
        if noted.tzinfo is None:
            noted = noted.replace(tzinfo=dt.timezone.utc)
    except Exception:                                                  # noqa: BLE001
        return False, f"unparseable noted_utc {noted_utc!r}"
    if lm <= noted:
        return False, f"served {lm:%Y-%m-%dT%H:%M:%SZ} is not after the debt {noted:%Y-%m-%dT%H:%M:%SZ}"
    return True, f"served {lm:%Y-%m-%dT%H:%M:%SZ} > debt {noted:%Y-%m-%dT%H:%M:%SZ}"


def _run_state(*args) -> int:
    p = subprocess.run([sys.executable, "-m", "updater.run", *args], cwd=ROOT,
                       capture_output=True, text=True, encoding="utf-8", errors="replace",
                       timeout=1800)
    for ln in (p.stdout or "").strip().splitlines()[-2:]:
        print("   ", ln, flush=True)
    return p.returncode


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True)
    ap.add_argument("--ids", help="file of series ids, one per line; default: every owed row of --source")
    ap.add_argument("--apply", action="store_true", help="pull state, clear the verified rows, push state")
    a = ap.parse_args(argv)

    lock = os.path.join(ROOT, "logs", "local_heavy.lock")
    if a.apply and os.path.exists(lock):
        print(f"REFUSED: {lock} is present — a heavy pass may be mid-run and a pull now would "
              f"wholesale-replace what it is writing (R340/R529). Re-run after the pass.")
        return 2
    if a.apply and _run_state("--pull-state") != 0:
        print("REFUSED: pull-state failed; a clear applied to a stale copy dies at the next pull.")
        return 2

    from updater.state import StateStore                              # noqa: PLC0415
    from core import r2_util                                          # noqa: PLC0415
    st = StateStore()
    rows = st.csv_desktop_owed(a.source)
    want = None
    if a.ids:
        want = {ln.strip() for ln in io.open(a.ids, encoding="utf-8") if ln.strip()
                and not ln.lstrip().startswith("#")}
        rows = [r for r in rows if r["series_id"] in want]
        missing = sorted(want - {r["series_id"] for r in rows})
        if missing:
            print(f"{len(missing)} requested id(s) have NO owed row (nothing to clear): {missing[:5]}")
    print(f"{a.source}: {len(rows)} owed row(s) considered")
    if not rows:
        return 0

    s3 = r2_util.client(write=False)
    ok, bad = [], []
    for r in rows:
        good, detail = served_after(s3, r["series_id"], r["noted_utc"])
        (ok if good else bad).append((r["series_id"], detail))
        print(f"  {'CLEARABLE' if good else 'STANDS   '} {r['series_id']}: {detail}")
    print(f"clearable {len(ok)}, standing {len(bad)}  — CAVEAT: 'served postdates the debt' proves a "
          f"REWRITE after the debt was noted, not CONTENT; a desktop derive from a mirror behind R2 "
          f"(R383/R530) would clear here too. Do the byte read-back against R2's parquet first.")
    if not a.apply:
        print("(report only — pass --apply to pull, clear the clearable rows, and push)")
        return 0 if not bad else 1
    if not ok:
        print("nothing clearable; state untouched, no push")
        return 1
    st.clear_csv_desktop_owed([sid for sid, _ in ok])
    if os.path.exists(lock):
        print(f"cleared LOCALLY but {lock} appeared mid-envelope — NOT pushing over a running "
              f"pass; the clear will not survive its pull. Re-run after the pass.")
        return 2
    if _run_state("--push-state") != 0:
        print("cleared LOCALLY but push-state failed (a writer likely raced us) — this clear "
              "will NOT survive the next pull. Re-run.")
        return 2
    print(f"cleared {len(ok)} csv_desktop_owed row(s) for {a.source} and pushed to the "
          f"authoritative store; {len(bad)} still stand")
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
