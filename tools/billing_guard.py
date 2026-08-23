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

# WRITES are 1000x the price of reads ($1.00/M vs $0.001/M) — a modest-looking
# campaign is a real invoice line. Measured 2026-08-17: the one-day serve+drain
# campaign metered ~14M rows written (~$14) via catalog upserts x FTS index
# amplification (~3 internal rows per series row); steady state is ~140k/day
# (~$0.14, hf download bookkeeping). Thresholds sit ~35x above steady state and
# below a repeat of the campaign day.
WARN_WRITES = 5_000_000     # ~$5/day
ALERT_WRITES = 15_000_000   # ~$15/day

# R2 storage baseline for the email's context line (measured 2026-08-18:
# econ-data 2.37 TB + ipdatalibrary 599 GB + hfdatalibrary-data 282 GB
# = 3.25 TB ~= $49/mo at $0.015/GB-mo). Not alerted on — it moves slowly and
# deliberately (new sources) — but reported so the number is never a surprise.
BUCKETS = ("econ-data", "hfdatalibrary-data", "ipdatalibrary")

# Same sender identity + secret contract as updater/send_digest.py (that module
# has no importable sender — its Resend POST is inline in main(), so the proven
# request pattern is replicated here rather than imported).
FROM = "Econ Data Library <noreply@hfdatalibrary.com>"
TO = os.environ.get("DIGEST_TO") or "admin@hfdatalibrary.com"


def _insights_sorted(db: str, sort_by: str):
    """Top-100 query shapes over 24h sorted by one dimension, or None on failure."""
    # encoding pinned per R363: wrangler prints emoji; Windows' cp1252 reader
    # thread dies mid-capture and stdout comes back None.
    out = subprocess.run(
        ["npx", "wrangler", "d1", "insights", db, "--timePeriod", "1d",
         "--sort-by", sort_by, "--count", "100", "--json"],
        capture_output=True, text=True, timeout=300, shell=(os.name == "nt"),
        encoding="utf-8", errors="replace")
    if out.returncode != 0:
        # NON-THROWING BY CONSTRUCTION (R415). The subprocess capture is already
        # utf-8 (R363 above), but this line then hands wrangler's emoji-bearing
        # stderr to print(), and on a cp1252 console THAT raises
        # UnicodeEncodeError — so the branch that REPORTS the failure becomes the
        # failure, and a billing run dies with a traceback instead of a number.
        # Measured 2026-08-23: '🪵' from wrangler killed the whole run.
        # An error handler that can raise is worse than no error handler.
        detail = (out.stderr or "")[-300:]
        enc = getattr(sys.stdout, "encoding", None) or "utf-8"
        print("  %s: wrangler failed (%s): %s"
              % (db, sort_by, detail.encode(enc, "replace").decode(enc, "replace")))
        return None
    # wrangler may prefix banner lines; the JSON array starts at the first '['.
    txt = out.stdout
    return json.loads(txt[txt.index("["):])


def insights(db: str) -> tuple:
    """(rows_read, rows_written) over 24h, or (-1, -1) on measurement failure.

    Each dimension is summed from its OWN sorted top-100: a write-heavy shape
    (catalog sync upserts) does virtually no reads and never surfaces in the
    reads-sorted list — the first version summed writes from that list and
    reported 0 against a measured 137k/day.
    """
    reads_data = _insights_sorted(db, "reads")
    writes_data = _insights_sorted(db, "writes")
    if reads_data is None or writes_data is None:
        return -1, -1
    return (sum(q.get("totalRowsRead", 0) for q in reads_data),
            sum(q.get("totalRowsWritten", 0) for q in writes_data))


def d1_storage_gb() -> float:
    """Account-wide D1 file size in GB (context for the monthly projection).
    Best-effort: -1.0 on failure — never a gate, the read/write measurement
    is the guarded surface."""
    out = subprocess.run(["npx", "wrangler", "d1", "list", "--json"],
                         capture_output=True, text=True, timeout=300,
                         shell=(os.name == "nt"),
                         encoding="utf-8", errors="replace")
    try:
        return sum((row.get("file_size") or 0) for row in json.loads(out.stdout)) / 1e9
    except Exception:  # noqa: BLE001
        return -1.0


def bucket_sizes() -> tuple:
    """Context lines per bucket + the summed GB: (text, total_gb). Best-effort."""
    lines = []
    total_gb = 0.0
    for b in BUCKETS:
        out = subprocess.run(["npx", "wrangler", "r2", "bucket", "info", b],
                             capture_output=True, text=True, timeout=300,
                             shell=(os.name == "nt"),
                             encoding="utf-8", errors="replace")
        size = count = "?"
        for ln in (out.stdout or "").splitlines():
            if "bucket_size" in ln:
                size = ln.split(":", 1)[1].strip()
            if "object_count" in ln:
                count = ln.split(":", 1)[1].strip()
        try:
            val, unit = size.split()
            total_gb += float(val) * (1024 if unit == "TB" else 1 if unit == "GB" else 0.001)
        except Exception:  # noqa: BLE001 — context line only, never a gate
            pass
        lines.append(f"{b}: {size} ({count} objects)")
    lines.append(f"R2 storage ~= ${total_gb * 0.015:,.0f}/mo at $0.015/GB-mo")
    return "\n".join(lines), total_gb


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
    reads = writes = 0
    failed = []
    lines = []
    for db in DBS:
        r, w = insights(db)
        if r < 0:
            failed.append(db)
            lines.append(f"{db}: MEASUREMENT FAILED")
        else:
            reads += r
            writes += w
            lines.append(f"{db}: {r:,} read / {w:,} written (24h)")
    bucket_text, r2_gb = bucket_sizes()
    # THE month-to-month number (Ahmed 2026-08-18: "follow month to month").
    # Every daily email carries the same projected-invoice line so the trend is
    # readable from any two dated emails: base plan + R2 storage + 30x today's
    # D1 read/write spend + D1 storage overage ($0.75/GB-mo past the 5 GB free).
    d1_gb = d1_storage_gb()
    d1_over = max(0.0, d1_gb - 5.0) * 0.75 if d1_gb >= 0 else 0.0
    d1_gb_txt = f"{d1_gb:.1f} GB" if d1_gb >= 0 else "unmeasured"
    projected = 5.0 + r2_gb * 0.015 + (reads / 1e9) * 30 + (writes / 1e6) * 30 + d1_over
    month_line = (f"PROJECTED MONTH ~= ${projected:,.0f}/mo "
                  f"(base $5 + R2 ${r2_gb * 0.015:,.0f} + D1 reads ${reads / 1e9 * 30:,.0f} "
                  f"+ D1 writes ${writes / 1e6 * 30:,.0f} + D1 storage ${d1_over:,.0f} "
                  f"[{d1_gb_txt}])")
    report = ("\n".join(lines)
              + f"\nREADS:  {reads:,}/day (~${reads/1e9:.2f}/day at $0.001/M)"
              + f"\nWRITES: {writes:,}/day (~${writes/1e6:.2f}/day at $1.00/M)"
              + "\n\n" + bucket_text
              + "\n\n" + month_line)
    print(report)
    if failed:
        # A guard that cannot see the meter must not look green — that is exactly
        # how the previous alert was "ineffective". Red the run until coverage is
        # restored (e.g. the API token loses reach to a database's account).
        print(f"MEASUREMENT FAILED for {failed} — reddening the workflow (guard coverage gap)")
        return 1
    if reads > ALERT_ROWS or writes > ALERT_WRITES:
        send_alert("D1 BILLING ALERT: usage exceeds emergency threshold",
                   report + f"\n\nThresholds: reads {ALERT_ROWS:,}, writes {ALERT_WRITES:,}. "
                            "Investigate query shapes with `wrangler d1 insights` "
                            "immediately (see ledger R430).")
        print("ALERT — reddening the workflow")
        return 1
    if reads > WARN_ROWS or writes > WARN_WRITES:
        send_alert("D1 billing warning: usage above baseline",
                   report + f"\n\nWarn thresholds: reads {WARN_ROWS:,}, writes "
                            f"{WARN_WRITES:,}. Not an emergency; check shapes.")
        print("WARN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
