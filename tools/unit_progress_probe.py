"""Did a local pass ADVANCE anything? Snapshot before, diff after.

Three rounds of this guard counted `runs` rows, and all three were wrong for the same reason:
a row in `runs` records that a unit was ATTEMPTED, not that it got anywhere. The motivating
pass (2026-09-01 23:26Z) wrote exactly two rows - `istat partial 40.7s`, whose own error says
"every ISTAT host unusable this run" and whose last_success_utc is still 2026-07-14, and the
`killed_external` row for the giant that ate the budget. Counting rows said "1 productive unit"
and stamped the 20-hour clock; nothing had advanced (R629, R630).

`partial` is a BUCKET, not a status: it covers eia merging 235,050,106 rows and istat reaching
no host at all. No partition of it can carry this decision. The signals that cannot be faked
are in `unit_state`:

    last_success_utc   ADVANCED  -> that unit reached a state the updater calls success
    upstream_vintage   changed   -> we hold a different vintage than we did before the pass
    last_obs_date      ADVANCED  -> the data reaches further than it did

The third is there because the first two are ONE signal, not two: orchestrate.py writes both in
the same upsert gated on the same `ok` boolean, so no `partial` run can advance either, BY
DESIGN - and `partial` is 412 of 1,359 runs and 33 of 283 units right now, including abs
(977,441,166 observations), eia (235,050,106) and ecb (150,547,002). `last_obs_date` is NOT
gated on `ok` and the lines above it enforce monotonicity, so a productive partial that extends
the tail is visible where the other two are blind (R634).

ADVANCED, not merely changed - but only where "advanced" has a meaning.

`last_success_utc` is the field the direction test EARNS its place on: it has no clamp above it,
and R340 records pull-state replacing local state wholesale with four CI writers doing
pull-then-push, so a backward move there is a real event.

`last_obs_date` is clamped by the writer three lines above its own upsert (`if new_last <
old_last: new_last = old_last`), so the store never regresses it and the direction test here is
belt-and-braces. It is kept deliberately, not redundantly: the clamp is upstream code that could
change, and this is the guard at the point of use.

Ordering is applied ONLY to values that have the ISO shape on BOTH sides. 257 of the 258
populated last_obs_date values are YYYY-MM-DD and one is not - sec_edgar holds
'01mar2026-31may2026', a Stata-style range - and sec_edgar is `partial` with a NULL
last_success_utc, so last_obs_date is the only field that can ever make it count. A string
comparison there is blind forwards (conservative) and PERMISSIVE backwards: if that value ever
became an ordinary ISO date, '2026-05-01' > '01mar2026-31may2026' compares '2' against '0' and
stamps the clock on a format change (R638). Anything not ISO on both sides falls back to
inequality, which is the honest answer for an unordered value.

`upstream_vintage` is unordered by nature and stays an inequality.

    python tools/unit_progress_probe.py --snapshot <file>   -> writes the before-state, prints
                                                               the number of units captured
    python tools/unit_progress_probe.py --diff <file>       -> prints how many units advanced

KNOWN BLIND SPOT, stated rather than hidden and now smaller than it was: a `partial` unit whose
`last_obs_date` is NULL (eia, eurostat and hagstofa hold NULL today) or whose work does not
extend the tail - a backfill of older observations - advances none of the three fields, and
this probe will call that pass empty. That is the conservative direction - it costs a
re-run, not a wait - but it is a blind spot and it belongs in the open.

Any failure prints -1, which the caller must treat as "unknown, do not stamp". The caller must
ALSO treat empty output as unknown: in PowerShell 5.1 `[int]$null` is 0 and does not throw, so
a sentinel that only this script can print does not cover the case where this script never runs.
"""
import argparse
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.environ.get("AQUEDUCT_STATE_DB") or os.path.join(ROOT, "data", "_aqueduct", "state.db")


sys.path.insert(0, ROOT)
from updater.obs_date import advanced as _advanced          # noqa: E402


def _read():
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT source_id, unit_id, last_success_utc, upstream_vintage, last_obs_date "
            "FROM unit_state"
        ).fetchall()
    finally:
        con.close()
    return {f"{s}/{u}": [ls, uv, lo] for s, u, ls, uv, lo in rows}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot")
    ap.add_argument("--diff")
    a = ap.parse_args(argv)
    try:
        if not os.path.exists(DB):
            print(-1)
            return 0
        if a.snapshot:
            state = _read()
            with open(a.snapshot, "w", encoding="utf-8") as fh:
                json.dump(state, fh)
            print(len(state))
            return 0
        if a.diff:
            with open(a.diff, encoding="utf-8") as fh:
                before = json.load(fh)
            now = _read()
            advanced = 0
            for key, vals in now.items():
                ls, uv, lo = (list(vals) + [None, None, None])[:3]
                old = before.get(key)
                if old is None:
                    if ls or uv or lo:
                        advanced += 1          # a unit that did not exist before and now has state
                    continue
                o_ls, o_uv, o_lo = (list(old) + [None, None, None])[:3]
                moved = (
                    _advanced(ls, o_ls)          # a LATER success, never an earlier one
                    or (uv and uv != o_uv)       # a different vintage; unordered by nature
                    or _advanced(lo, o_lo)       # the data reaches further
                )
                if moved:
                    advanced += 1
            print(advanced)
            return 0
        print(-1)
    except Exception:                                    # noqa: BLE001
        print(-1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
