"""Release a lease whose holder is dead, so the unit is not locked out for its full TTL.

WHY THIS IS NEEDED. claim_lease is the cross-writer guard and its TTL is deliberately long -
_TTL_BY_COST gives refresh_cost 'large' 43200s (12 h) and 'giant' 172800s (48 h). A run that
DIES or is killed never releases, so the unit stays locked for the remainder of that window
even though nothing holds it. Measured: a local run killed at 19:04 on 2026-07-30 kept
abs/_all locked until 06:58 the next morning, and the 20:10 cloud run skipped abs entirely -
its log says `locked abs/_all` and the fetcher never executed. That is the same shape as
R193, where a stale lease made a "verification" run measure nothing.

SAFE BY CONSTRUCTION:
  * pulls state FIRST so the compare-and-swap at push time can succeed and we never clobber
    a newer remote state;
  * refuses to touch a lease that has not expired UNLESS --force, and prints the holder;
  * names every lease it removes.

Usage:
    python tools/release_stale_lease.py --list
    python tools/release_stale_lease.py --key abs/_all --force
"""
from __future__ import annotations
import argparse
import datetime as dt
import os
import sqlite3
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", action="append", help="lease key, e.g. abs/_all")
    ap.add_argument("--list", action="store_true", help="show leases and exit")
    ap.add_argument("--force", action="store_true",
                    help="release even if the lease has NOT expired (holder assumed dead)")
    ap.add_argument("--no-pull", action="store_true",
                    help="skip --pull-state (only when state was just pulled)")
    args = ap.parse_args()

    from updater import config

    if not args.no_pull:
        print("pulling state so the push-time compare-and-swap can succeed ...", flush=True)
        rc = subprocess.call([sys.executable, "-m", "updater.run", "--pull-state"])
        if rc != 0:
            print(f"pull-state failed ({rc}); refusing to touch leases", file=sys.stderr)
            return 1

    db = sqlite3.connect(config.STATE_DB)
    now = dt.datetime.now(dt.timezone.utc)
    rows = list(db.execute("SELECT key, owner, expires_utc FROM leases ORDER BY expires_utc"))
    print(f"\n{len(rows)} lease row(s):")
    for k, o, e in rows:
        try:
            exp = dt.datetime.fromisoformat(e)
            live = exp > now
            mins = (exp - now).total_seconds() / 60
        except Exception:                                    # noqa: BLE001
            live, mins = True, float("nan")
        print(f"   {k:26s} owner={o:18s} expires {e}  "
              f"{'HELD, ' + format(mins, '.0f') + ' min left' if live else 'expired'}")
    if args.list or not args.key:
        return 0

    removed = []
    for key in args.key:
        cur = [r for r in rows if r[0] == key]
        if not cur:
            print(f"\n{key}: no such lease")
            continue
        _k, owner, e = cur[0]
        try:
            held = dt.datetime.fromisoformat(e) > now
        except Exception:                                    # noqa: BLE001
            held = True
        if held and not args.force:
            print(f"\n{key}: still held by {owner} until {e} - pass --force if that holder "
                  f"is known dead")
            continue
        db.execute("DELETE FROM leases WHERE key=?", (key,))
        removed.append(f"{key} (was {owner}, until {e})")
    db.commit()
    db.close()

    if not removed:
        print("\nnothing released")
        return 0
    print("\nreleased:")
    for r in removed:
        print(f"   {r}")
    print("\npushing state ...", flush=True)
    rc = subprocess.call([sys.executable, "-m", "updater.run", "--push-state"])
    print(f"push-state rc={rc}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
