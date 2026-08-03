"""Which recorded failures were already fixed? Date every state error against the fix history.

WHY THIS IS A TOOL AND NOT A HABIT. The rule is one sentence — "a state row records the LAST
ATTEMPT, not the current code, so date the error before treating it as work" — and it has now been
broken twice by the person who wrote it, on the same day (R297, R301).

    bea        TypeError from a mid-run commit; the exact call worked when re-run
    boc        AttributeError fixed in a1c42881, FIVE HOURS after the failure it still reports
    insee_bdm  201/201 sub-units "transient-failed"; INSEE answers HTTP 200 today
    hagstofa   "26/1906 returned 200 but parsed 0 rows" — fixed in 1188fb62 two days before I
               looked, and the commit message names that exact count. I probed the publisher,
               filed a task and wrote a ledger entry before running one `git log`.

Four of four were stale. The rule is right and remembering it is not reliable, because the artefact
often arrives disguised as CORROBORATION for a pattern rather than as a bug report — and supporting
evidence gets audited less than a primary claim.

WHAT IT COMPARES. For every source with a recorded error: `last_attempt_utc` from state.db against
the newest commit touching that source's own code (its fetcher module, its ingester job, and the
shared PxWeb/merge helpers it is most likely to depend on). A fix that POSTDATES the last attempt
means the error describes code that no longer exists.

WHAT IT DOES NOT CLAIM. "Superseded" is not "fixed" — the commit may be unrelated to this error.
It is a claim about WHICH CODE the message describes, and therefore about whether the message is
evidence at all. The right next step for a superseded row is to let the source RUN, not to debug it
and not to trust it.

    python tools/audit_stale_errors.py
    python tools/audit_stale_errors.py --live      # only registry live:true sources
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

from updater import config, registry                              # noqa: E402

# DELIBERATELY NOT INCLUDING SHARED CODE, having tried it and watched it fail.
#
# The first version compared against core/pxweb.py, merge.py, blob.py and _common.py as well. One
# commit to core/pxweb.py that afternoon made 112 of 140 rows "superseded", nearly all citing that
# same commit — including bcb, boe and bea, which are not PxWeb sources and never import it. The
# docstring of that very version warned that a broad list "would make every row look superseded on
# any commit, which is the same uselessness as an audit that is always red", and it did.
#
# A shared-code change CAN invalidate an error message, but it cannot be ATTRIBUTED without knowing
# whether this source's failing path actually runs through it — and guessing that from a filename
# produces noise at exactly the rate that makes the tool ignorable. So the signal here is narrow and
# honest: did THIS SOURCE'S OWN code change since the attempt. False negatives (a shared fix this
# misses) leave a row marked CURRENT, which costs one wasted look; false positives would poison
# every row at once.
def own_paths(src: str) -> list:
    return [f"updater/strategies/fetchers/{src}.py", f"jobs/ingest_{src}.py"]


def newest_commit(paths) -> "tuple[dt.datetime | None, str]":
    """(committer datetime, 'hash subject') of the newest commit touching any of `paths`."""
    real = [p for p in paths if os.path.exists(os.path.join(ROOT, p))]
    if not real:
        return None, ""
    try:
        out = subprocess.run(
            ["git", "log", "-1", "--format=%cI\t%h %s", "--"] + real,
            cwd=ROOT, capture_output=True, text=True, timeout=60)
    except Exception:                                              # noqa: BLE001
        return None, ""
    line = (out.stdout or "").strip()
    if not line or "\t" not in line:
        return None, ""
    iso, subject = line.split("\t", 1)
    try:
        return dt.datetime.fromisoformat(iso), subject
    except ValueError:
        return None, subject


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="only registry live:true sources")
    a = ap.parse_args()

    live = {e["source_id"] for e in registry.load()["sources"] if e.get("live")}
    con = sqlite3.connect(f"file:{config.STATE_DB}?mode=ro", uri=True)
    rows = con.execute(
        "SELECT source_id, unit_id, status, last_attempt_utc, last_error "
        "FROM unit_state WHERE last_error IS NOT NULL AND last_error != ''").fetchall()
    con.close()

    superseded, current = [], []
    for src, unit, status, attempt, err in rows:
        if a.live and src not in live:
            continue
        if not attempt:
            continue
        try:
            att = dt.datetime.fromisoformat(attempt)
        except ValueError:
            continue
        when, subject = newest_commit(own_paths(src))
        # BOTH SIDES IN UTC BEFORE COMPARING *AND* BEFORE PRINTING. git's %cI carries the
        # committer's local offset (-05:00 here) while state.db stores UTC, so the comparison was
        # already correct but the OUTPUT read as nonsense: bcrp printed "attempt 08:01Z -> fix
        # 07:11Z" and was still filed superseded, because 07:11-05:00 is 12:11Z. A line that looks
        # self-contradictory gets the whole tool disbelieved.
        if when is not None:
            when = when.astimezone(dt.timezone.utc)
        if att.tzinfo is None:
            att = att.replace(tzinfo=dt.timezone.utc)
        if when is not None and when > att:
            superseded.append((src, unit, status, att, when, subject, err))
        else:
            current.append((src, unit, status, att, err))

    print(f"{len(rows)} recorded error(s); {len(superseded)} describe code that has since changed\n")
    if superseded:
        print("SUPERSEDED — the message predates a change to this source's code. Do NOT debug "
              "these from the message; let the source RUN and re-read it:")
        for src, unit, status, att, when, subject, err in sorted(superseded, key=lambda r: r[0]):
            print(f"\n  {src}/{unit}  status={status}")
            print(f"      attempt {att:%Y-%m-%d %H:%M}Z  ->  fix {when:%Y-%m-%d %H:%M}Z  {subject}")
            print(f"      says: {(err or '')[:110]}")
    if current:
        print(f"\n\nCURRENT — no code change since the attempt, so the message still describes "
              f"what runs today ({len(current)}):")
        for src, unit, status, att, err in sorted(current, key=lambda r: r[0]):
            print(f"  {src:<22} {status:<16} {att:%Y-%m-%d}  {(err or '')[:70]}")
    # Informational: a superseded row is a question about evidence, not a defect to gate on.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
