"""Daily operator digest for the Aqueduct updater — the econ twin of the
hfdatalibrary morning email.

Runs as the LAST step of updater-daily.yml (even when earlier steps failed:
`if: always()`), reads the run's state.db off the runner, and emails a short
honest summary via Resend: per-source status, data frontiers, and anything
needing attention. Sender reuses the Resend-verified hfdatalibrary.com domain.

Honesty rules: the digest reports what the ledger says — counts and dates come
from unit_state rows written only after verified publishes; a red overall run
status is stated in the subject line, never softened. If RESEND_API_KEY is
absent the step prints a loud notice and exits 0 (email is an add-on, not a
gate — the health gate step is what turns runs red).
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import urllib.request

FROM = "Econ Data Library <noreply@hfdatalibrary.com>"
TO = "elkassabgi@yahoo.com"
STATE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "data", "_aqueduct", "state.db")


def main() -> None:
    key = os.environ.get("RESEND_API_KEY", "").strip()
    run_status = os.environ.get("RUN_STATUS", "unknown")   # ${{ job.status }} from the yml
    run_id = os.environ.get("GITHUB_RUN_ID", "local")

    con = sqlite3.connect(f"file:{STATE}?mode=ro", uri=True)
    rows = con.execute(
        "SELECT source_id, status, last_obs_date, last_success_utc, last_error "
        "FROM unit_state ORDER BY source_id").fetchall()
    con.close()

    ok = [r for r in rows if r[1] in ("ok", "no_change")]
    warn = [r for r in rows if r[1] in ("partial", "transient_fail")]
    bad = [r for r in rows if r[1] not in ("ok", "no_change", "partial", "transient_fail")]

    healthy = run_status.lower() == "success" and not bad
    subject = (f"econdatalibrary daily: OK — {len(ok)} sources current"
               if healthy else
               f"econdatalibrary daily: ATTENTION — run={run_status}, "
               f"{len(bad)} failed, {len(warn)} retrying")

    lines = [f"Run {run_id}: {run_status}",
             f"{len(ok)} ok/no_change · {len(warn)} partial/transient · {len(bad)} failed",
             ""]
    for r in sorted(warn + bad, key=lambda x: x[0]):
        lines.append(f"  !! {r[0]:20} {r[1]:15} last_obs={r[2] or '—'}  err={str(r[4] or '')[:90]}")
    if warn or bad:
        lines.append("")
    for r in ok:
        lines.append(f"     {r[0]:20} {r[1]:15} data through {r[2] or '—'}")
    lines += ["", "Status page: https://econdatalibrary.com/status.html",
              f"Run log: https://github.com/elkassabgi/econdatalibrary/actions/runs/{run_id}"]
    body = "\n".join(lines)

    print(f"[digest] {subject}\n{body}\n", flush=True)
    if not key:
        print("[digest] RESEND_API_KEY not set — email SKIPPED (add the GitHub secret "
              "to enable the morning email; the run's red/green state still notifies "
              "via GitHub Actions).", flush=True)
        return

    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=json.dumps({"from": FROM, "to": [TO], "subject": subject,
                         "text": body}).encode(),
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(f"[digest] sent: HTTP {resp.status}", flush=True)
    except urllib.error.HTTPError as e:
        # Print Resend's exact error body (safe: their error JSON carries no
        # secrets) so a misconfigured key/domain is diagnosable from the log.
        try:
            detail = e.read().decode()[:300]
        except Exception:
            detail = "(no body)"
        print(f"[digest] SEND FAILED: HTTP {e.code} — {detail} (run status is "
              f"still governed by the health gate, not the email)", flush=True)
    except Exception as e:  # noqa: BLE001 — email failure must not flip a green run red
        print(f"[digest] SEND FAILED: {e!r} (run status is still governed by the "
              f"health gate, not the email)", flush=True)


if __name__ == "__main__":
    sys.exit(main())
