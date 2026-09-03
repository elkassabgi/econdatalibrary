"""Which sources are stale AGAINST THEIR OWN CADENCE - not against a flat 14 days?

My first attempt aggregated the `runs` table and reported "104 sources stale, 33 never
succeeded". That instrument was wrong and the conclusion would have been alarmist: `runs` holds
1,376 rows spanning 2026-06-23 to 2026-09-03, about 4 rows per source, because sources are NOT
run daily - they carry cadences, several are irregular, and a source with no row is one that has
not been DUE, not one that has failed.

`source_state` is the right table: one row per source with `cadence`, `enabled`, `status`,
`last_success_utc` and `last_attempt_utc`. Staleness is then judged against what each source is
meant to do.

Local and free.
"""
import datetime as dt
import os
import sqlite3
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATE = os.path.join(ROOT, "data", "_aqueduct", "state.db")

# generous multiples: a source is only called stale when it is well past its own cadence
BUDGET_DAYS = {
    "daily": 7, "weekly": 21, "biweekly": 35, "monthly": 75,
    "quarterly": 200, "annual": 500, "irregular": 180,
}
DEFAULT_DAYS = 90


def main() -> int:
    con = sqlite3.connect("file:%s?mode=ro" % STATE, uri=True)
    now = dt.datetime.now(dt.timezone.utc)

    def age(ts):
        if not ts:
            return None
        try:
            t = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if t.tzinfo is None:
                t = t.replace(tzinfo=dt.timezone.utc)
            return (now - t).days
        except Exception:                                              # noqa: BLE001
            return None

    rows = con.execute("SELECT source_id, cadence, status, enabled, last_success_utc, "
                       "last_attempt_utc FROM source_state").fetchall()

    # THE REGISTRY IS THE SCHEDULE. A source in source_state but absent from registry.yaml
    # cannot be run at all - it was retired, and its row is a leftover. Ten sources were removed
    # on 2026-07-22/23 as material we are not permitted to re-host (updater/config.py), and
    # without this they show up as "stale" on every run of this tool, forever.
    reg = yaml.safe_load(open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8"))
    scheduled = {e.get("source_id") for e in (reg.get("sources") or []) if isinstance(e, dict)}

    retired = [r for r in rows if r[0] not in scheduled]
    live = [r for r in rows if r[0] in scheduled]
    enabled = [r for r in live if (r[3] in (1, "1", True, None))]
    disabled = [r for r in live if r not in enabled]

    stale, never, ok = [], [], []
    for sid, cad, status, en, succ, att in enabled:
        a = age(succ)
        budget = BUDGET_DAYS.get(str(cad or "").lower(), DEFAULT_DAYS)
        if a is None:
            never.append((sid, cad, status, age(att)))
        elif a > budget:
            stale.append((sid, cad, status, a, budget))
        else:
            ok.append(sid)

    print(f"source_state: {len(rows)} rows | {len(scheduled)} in registry.yaml | "
          f"{len(enabled)} enabled and scheduled, {len(disabled)} not enabled, "
          f"{len(retired)} RETIRED\n")
    print(f"  current for their cadence      {len(ok):>4}")
    print(f"  PAST their cadence budget      {len(stale):>4}")
    print(f"  no success ever recorded       {len(never):>4}\n")

    if stale:
        print(f"{'source':<22}{'cadence':<12}{'status':<16}{'days':>6}{'budget':>8}")
        for sid, cad, status, a, b in sorted(stale, key=lambda r: -(r[3] / max(r[4], 1)))[:20]:
            print(f"{sid[:20]:<22}{str(cad)[:10]:<12}{str(status)[:14]:<16}{a:>6}{b:>8}")
        if len(stale) > 20:
            print(f"   ... and {len(stale) - 20} more")

    if never:
        print(f"\nno success ever recorded ({len(never)}):")
        for sid, cad, status, att in never[:14]:
            print(f"   {sid[:24]:<26} cadence={str(cad)[:10]:<10} status={str(status)[:12]:<14}"
                  f" last attempt {'never' if att is None else str(att) + 'd ago'}")
    if retired:
        print(f"\nretired - in source_state but not in registry.yaml, so never scheduled "
              f"({len(retired)}). These are NOT stale; they were removed deliberately, several "
              f"on 2026-07-22/23 as material we may not re-host:")
        for sid, cad, status, en, succ, att in sorted(retired)[:14]:
            print(f"   {sid[:24]:<26} last success {str(succ)[:10] or 'never'}")
        if len(retired) > 14:
            print(f"   ... and {len(retired) - 14} more")

    print()
    print("Cadence budgets are deliberately generous (daily->7d, monthly->75d, irregular->180d):")
    print("this answers 'which sources have clearly stopped', not 'which ran late once'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
