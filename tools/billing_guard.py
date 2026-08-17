"""Daily D1 billing guard — the durable replacement for dashboard alerts (Ahmed 2026-08-17).

WHY. The 2026-08-13..15 incident billed ~$175 of D1 reads over three days and the
first alarm was the INVOICE: Cloudflare's dashboard notifications do not usefully
alert on D1 rows-read, and Ahmed's existing alert never fired ("i already had one
but its ineffective"). This guard measures the actual meter daily from CI, where
it survives workstation sessions and restarts.

WHAT. `wrangler d1 insights <db> --timePeriod 1d` for every database in the
account, summed. Under WARN_ROWS: green, numbers in the log. Over WARN_ROWS:
alert email via the digest's own Resend path + numbers. Over ALERT_ROWS: same,
and exit 1 so the workflow reddens (GitHub emails failed-workflow notices too —
two independent delivery paths).

Baselines (measured): incident peak 130B/day; healthy steady state ~200-400M/day
econ + ~600M/day hf download_log aggregates (task #134) => WARN 2B, ALERT 5B
chosen ABOVE today's known-benign load and ~50x below the incident, so it fires
on real anomalies, not noise. $1 per billion rows read.
"""
import json
import os
import subprocess
import sys
import urllib.request

DBS = ("econ-catalog", "econ-catalog-climate", "hfdatalibrary-db")
WARN_ROWS = 2_000_000_000
ALERT_ROWS = 5_000_000_000

# Same sender identity + secret contract as updater/send_digest.py (that module
# has no importable sender — its Resend POST is inline in main(), so the proven
# request pattern is replicated here rather than imported).
FROM = "Econ Data Library <noreply@hfdatalibrary.com>"
TO = os.environ.get("DIGEST_TO") or "admin@hfdatalibrary.com"


def insights(db: str) -> int:
    out = subprocess.run(
        ["npx", "wrangler", "d1", "insights", db, "--timePeriod", "1d",
         "--sort-by", "reads", "--count", "100", "--json"],
        capture_output=True, text=True, timeout=300, shell=(os.name == "nt"))
    if out.returncode != 0:
        print(f"  {db}: wrangler failed: {out.stderr[-300:]}")
        return -1
    # wrangler may prefix banner lines; the JSON array starts at the first '['.
    txt = out.stdout
    data = json.loads(txt[txt.index("["):])
    return sum(q.get("totalRowsRead", 0) for q in data)


def send_alert(subject: str, body: str) -> None:
    key = os.environ.get("RESEND_API_KEY", "").strip()
    if not key:
        print("  RESEND_API_KEY not set — email skipped; the red workflow is the delivery path")
        return
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=json.dumps({"from": FROM, "to": [TO], "subject": subject,
                         "text": body}).encode(),
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json",
                 # api.resend.com sits behind Cloudflare bot protection, which
                 # 1010-blocks urllib's default signature — identify honestly.
                 "User-Agent": "econdatalibrary-billing-guard/1.0"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(f"  alert email sent: HTTP {resp.status}")
    except Exception as e:  # noqa: BLE001 — the red workflow is the second delivery path
        print(f"  alert email failed ({e}) — relying on the red-workflow notification")


def main() -> int:
    total = 0
    failed = []
    lines = []
    for db in DBS:
        n = insights(db)
        lines.append(f"{db}: {n:,} rows read (24h)" if n >= 0 else f"{db}: MEASUREMENT FAILED")
        if n < 0:
            failed.append(db)
        elif n > 0:
            total += n
    report = "\n".join(lines) + f"\nTOTAL: {total:,} rows/day (~${total/1e9:.2f}/day)"
    print(report)
    if failed:
        # A guard that cannot see the meter must not look green — that is exactly
        # how the previous alert was "ineffective". Red the run until coverage is
        # restored (e.g. the API token loses reach to a database's account).
        print(f"MEASUREMENT FAILED for {failed} — reddening the workflow (guard coverage gap)")
        return 1
    if total > ALERT_ROWS:
        send_alert("D1 BILLING ALERT: reads exceed emergency threshold",
                   report + f"\n\nThreshold: {ALERT_ROWS:,}. Investigate query shapes with "
                            "`wrangler d1 insights` immediately (see ledger R430).")
        print(f"ALERT: total {total:,} > {ALERT_ROWS:,} — reddening the workflow")
        return 1
    if total > WARN_ROWS:
        send_alert("D1 billing warning: reads above baseline",
                   report + f"\n\nWarn threshold: {WARN_ROWS:,}. Not an emergency; check shapes.")
        print(f"WARN: total {total:,} > {WARN_ROWS:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
