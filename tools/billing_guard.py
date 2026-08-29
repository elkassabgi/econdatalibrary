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
import time
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


_TRIES = 3            # wrangler transients are common; see R222


def _insights_sorted(db: str, sort_by: str, attempt: int = 1):
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
        safe = detail.encode(enc, "replace").decode(enc, "replace")
        # RETRY BEFORE DECLARING FAILURE (R222). On 2026-08-23 econ-catalog - the LARGEST
        # database - came back "Authentication error [code: 10000]" while the other two
        # measured fine on the same OAuth session, and an immediate manual retry returned
        # 109,615,101 reads / 224,465 writes. A transient wrangler hiccup therefore dropped
        # the biggest contributor out of the bill: the run printed $28/mo when the measured
        # figure was nearer $37. An identical call succeeding moments later was never a
        # permission wall.
        if attempt < _TRIES:
            wait = 5 * attempt
            print("  %s: wrangler failed (%s), retrying in %ds: %s"
                  % (db, sort_by, wait, safe.splitlines()[0][:110] if safe.strip() else "no stderr"))
            time.sleep(wait)
            return _insights_sorted(db, sort_by, attempt + 1)
        print("  %s: wrangler failed (%s) after %d attempts: %s"
              % (db, sort_by, _TRIES, safe))
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


def _load_env_token() -> str:
    """CF_ANALYTICS_TOKEN from env or the repo .env (never printed)."""
    tok = os.environ.get("CF_ANALYTICS_TOKEN", "").strip()
    if tok:
        return tok
    try:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for line in open(os.path.join(root, ".env"), encoding="utf-8"):
            line = line.strip()
            if line.startswith("CF_ANALYTICS_TOKEN=") :
                return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return ""


def _account_id() -> str:
    """Account tag, derivable from the R2 endpoint host (no secret involved)."""
    ep = os.environ.get("R2_WRITE_ENDPOINT", "")
    if not ep:
        try:
            root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            for line in open(os.path.join(root, ".env"), encoding="utf-8"):
                if line.startswith("R2_WRITE_ENDPOINT="):
                    ep = line.split("=", 1)[1].strip()
                    break
        except OSError:
            pass
    return ep.split("//")[1].split(".")[0] if "//" in ep else ""


# R2 operation classes per Cloudflare's pricing page (Class A $4.50/M past 1M/mo
# free, Class B $0.36/M past 10M/mo free). Anything unknown is counted CLASS A —
# the guard must fail toward the expensive reading, never the cheap one.
_R2_CLASS_B = {"GetObject", "HeadObject", "HeadBucket", "UsageSummary"}

# MEASURED-BUT-UNPRICED WAS THE DEFECT. account_analytics() printed R2 operations — the
# largest variable line on this account (~230k Class A/day measured 2026-08-29) — while
# the "PROJECTED MONTH" total omitted them entirely, so the headline understated the bill
# by roughly the size of everything else combined. It now publishes the newest COMPLETE
# day here (today is partial and would understate) and the projection prices it.
_MEASURED = {"r2_class_a_day": None, "r2_class_b_day": None, "workers_day": None,
             "d1_reads_day": None, "d1_writes_day": None}


def _graphql(token: str, query: str, variables: dict):
    """One GraphQL call; None on any failure (never raises — this is a meter,
    not a gate, and its absence must degrade to a LOUD 'unmetered' line)."""
    try:
        body = json.dumps({"query": query, "variables": variables}).encode()
        req = urllib.request.Request(
            "https://api.cloudflare.com/client/v4/graphql", data=body,
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/json"})
        d = json.load(urllib.request.urlopen(req, timeout=60))
        if d.get("errors"):
            enc = getattr(sys.stdout, "encoding", None) or "utf-8"
            msg = json.dumps(d["errors"])[:200]
            print("  account analytics: GraphQL error: "
                  + msg.encode(enc, "replace").decode(enc, "replace"))
            return None
        return d["data"]["viewer"]["accounts"][0]
    except Exception as e:  # noqa: BLE001 — meter, not gate
        print(f"  account analytics: fetch failed ({type(e).__name__})")
        return None


def account_analytics() -> str:
    """R2 operations + Workers requests + TRUE D1 totals for the last 2 complete
    days — the lines D1 insights cannot see. THE 2026-08-26 GAP: Ahmed saw $7+/day
    on the invoice while every instrument here summed to ~$3; R2 Class A ops and
    Workers were simply unmetered (R430 rule 2: metered infrastructure needs a
    meter-watcher — the invoice must never be the first alarm). Also detects the
    insights top-100 truncation: GraphQL's rowsRead is the true total, insights
    sums only 100 query shapes.

    Requires CF_ANALYTICS_TOKEN (read-only 'Account Analytics: Read' token).
    Without it, returns the loud UNMETERED line — visibility of the blind spot is
    the point; silence here is how the $7/day surprise happened."""
    import datetime as _dt
    tok = _load_env_token()
    acct = _account_id()
    if not tok or not acct:
        return ("ACCOUNT ANALYTICS: UNMETERED — R2 operations and Workers usage are "
                "invisible to this guard. Set CF_ANALYTICS_TOKEN (read-only "
                "Analytics token) in .env / CI secrets to close the gap.")
    end = _dt.date.today().isoformat()
    start = (_dt.date.today() - _dt.timedelta(days=2)).isoformat()
    out = []
    a = _graphql(tok, """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2OperationsAdaptiveGroups(limit: 2000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date actionType } sum { requests } } } } }""",
                 {"acct": acct, "start": start, "end": end})
    if a is not None:
        per = {}
        for r in a.get("r2OperationsAdaptiveGroups") or []:
            dd = r["dimensions"]
            cls = "B" if dd["actionType"] in _R2_CLASS_B else "A"
            per.setdefault(dd["date"], {"A": 0, "B": 0})
            per[dd["date"]][cls] += r["sum"]["requests"]
        dates = sorted(per)
        for date in dates:
            ca, cb = per[date]["A"], per[date]["B"]
            out.append(f"R2 ops {date}: ClassA {ca:,} (~${ca / 1e6 * 4.50:.2f}) "
                       f"ClassB {cb:,} (~${cb / 1e6 * 0.36:.2f})")
        if len(dates) >= 2:                      # newest COMPLETE day, never today
            _MEASURED["r2_class_a_day"] = per[dates[-2]]["A"]
            _MEASURED["r2_class_b_day"] = per[dates[-2]]["B"]
    w = _graphql(tok, """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    workersInvocationsAdaptive(limit: 100, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date } sum { requests } } } } }""",
                 {"acct": acct, "start": start, "end": end})
    if w is not None:
        per = {}
        for r in w.get("workersInvocationsAdaptive") or []:
            per[r["dimensions"]["date"]] = per.get(r["dimensions"]["date"], 0) + r["sum"]["requests"]
        for date in sorted(per):
            n = per[date]
            out.append(f"Workers requests {date}: {n:,} (~${n / 1e6 * 0.30:.2f} past free tier)")
    d1 = _graphql(tok, """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1AnalyticsAdaptiveGroups(limit: 500, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date } sum { rowsRead rowsWritten } } } } }""",
                 {"acct": acct, "start": start, "end": end})
    if d1 is not None:
        per = {}
        for r in d1.get("d1AnalyticsAdaptiveGroups") or []:
            p = per.setdefault(r["dimensions"]["date"], [0, 0])
            p[0] += r["sum"]["rowsRead"]
            p[1] += r["sum"]["rowsWritten"]
        dates = sorted(per)
        for date in dates:
            rr, rw = per[date]
            out.append(f"D1 TRUE totals {date}: {rr:,} read (~${rr / 1e9:.2f}) / "
                       f"{rw:,} written (~${rw / 1e6:.2f}) — GraphQL, not top-100")
        if len(dates) >= 2:
            _MEASURED["d1_reads_day"], _MEASURED["d1_writes_day"] = per[dates[-2]]
    return "ACCOUNT ANALYTICS (last 2 complete days + today):\n  " + "\n  ".join(out) \
        if out else "ACCOUNT ANALYTICS: token present but every query failed — see lines above"


def hf_registrations_24h() -> int:
    """New-user registrations in the last 24h (hfdatalibrary-db users table, ~1.3k rows —
    a trivial read). 2026-08-26: registrations dipped ~85% for seven hours and AHMED was
    the alarm — the endpoint was healthy the whole time, so only a rate meter could have
    caught it. Zero-in-24h triggers a WARN email below (baseline is ~20-30/day; a true
    zero day has never occurred in the recorded series). -1 = unmeasured, printed as such
    and never alarmed on (a meter that cannot see must say so, not cry wolf)."""
    out = subprocess.run(
        ["npx", "wrangler", "d1", "execute", "hfdatalibrary-db", "--remote", "--json",
         "--command", "SELECT COUNT(*) n FROM users WHERE created_at >= datetime('now','-1 day')"],
        capture_output=True, text=True, timeout=300, shell=(os.name == "nt"),
        encoding="utf-8", errors="replace")
    try:
        txt = out.stdout
        return json.loads(txt[txt.index("["):])[0]["results"][0]["n"]
    except Exception:  # noqa: BLE001 — a meter, never a gate
        return -1


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
    # ORDER MATTERS: account_analytics() is what POPULATES _MEASURED, so it must run
    # BEFORE the projection prices anything. Called later (inside the report string) the
    # projection always saw an empty carrier and fell back to the truncated insights
    # figure and an UNMETERED R2 line — measured-but-unpriced, again.
    analytics_txt = account_analytics()
    # THE month-to-month number (Ahmed 2026-08-18: "follow month to month").
    # Every daily email carries the same projected-invoice line so the trend is
    # readable from any two dated emails: base plan + R2 storage + 30x today's
    # D1 read/write spend + D1 storage overage ($0.75/GB-mo past the 5 GB free).
    d1_gb = d1_storage_gb()
    d1_over = max(0.0, d1_gb - 5.0) * 0.75 if d1_gb >= 0 else 0.0
    d1_gb_txt = f"{d1_gb:.1f} GB" if d1_gb >= 0 else "unmeasured"
    # INCLUDED MONTHLY ALLOWANCES — the plan's own terms, which this model omitted until
    # 2026-08-29 (R502). Workers Paid includes 25e9 D1 rows read and 50e6 rows written per
    # month before ANY per-row charge (developers.cloudflare.com/workers/platform/pricing,
    # read 2026-08-29; allowances reset on the SUBSCRIPTION RENEWAL date, not the 1st).
    # Billing from row zero overstated every D1 figure this tool has ever printed — and
    # those figures went to the owner while he was worried about the bill.
    D1_READS_INCLUDED = 25_000_000_000
    D1_WRITES_INCLUDED = 50_000_000
    R2_CLASS_A_INCLUDED = 1_000_000        # $4.50/M past this
    R2_CLASS_B_INCLUDED = 10_000_000       # $0.36/M past this
    # PREFER THE GraphQL TRUE TOTALS over `wrangler d1 insights`, which sums only the top
    # 100 query SHAPES: measured 2026-08-29, insights reported 36.1M reads/day for the
    # fleet while GraphQL reported 374.3M for the same account — a 10x truncation. The
    # tool was already FETCHING the true number and still projecting from the small one.
    d1_r_day = _MEASURED["d1_reads_day"] if _MEASURED["d1_reads_day"] is not None else reads
    d1_w_day = _MEASURED["d1_writes_day"] if _MEASURED["d1_writes_day"] is not None else writes
    src_note = "GraphQL" if _MEASURED["d1_reads_day"] is not None else "insights (TRUNCATED)"
    mo_reads, mo_writes = d1_r_day * 30.0, d1_w_day * 30.0
    d1_read_cost = max(0.0, mo_reads - D1_READS_INCLUDED) / 1e6 * 0.001
    d1_write_cost = max(0.0, mo_writes - D1_WRITES_INCLUDED) / 1e6 * 1.00
    ca_day, cb_day = _MEASURED["r2_class_a_day"], _MEASURED["r2_class_b_day"]
    r2_op_cost = 0.0
    r2_op_txt = "UNMETERED"
    if ca_day is not None:
        mo_a, mo_b = ca_day * 30.0, (cb_day or 0) * 30.0
        r2_op_cost = (max(0.0, mo_a - R2_CLASS_A_INCLUDED) / 1e6 * 4.50
                      + max(0.0, mo_b - R2_CLASS_B_INCLUDED) / 1e6 * 0.36)
        r2_op_txt = (f"${r2_op_cost:,.0f} [{mo_a/1e6:.1f}M class-A vs 1M included]")
    projected = (5.0 + r2_gb * 0.015 + d1_read_cost + d1_write_cost + d1_over + r2_op_cost)
    # A total that silently omits a database is not a projection, it is a floor, and it
    # must not be readable as the bill. On 2026-08-23 econ-catalog failed to measure and
    # the run printed "PROJECTED MONTH ~= $28/mo"; econ-catalog alone carries 109.6M
    # reads and 224k writes a day, which puts the real figure nearer $37. The number was
    # wrong by a third and nothing in that line said so - the coverage gap was disclosed
    # four lines earlier and in the exit code, both easy to skim past when a dollar figure
    # is sitting right there.
    label = "PROJECTED MONTH" if not failed else "PARTIAL MONTH FLOOR"
    caveat = "" if not failed else (
        f" -- EXCLUDES {', '.join(failed)}, which did not measure; the real bill is HIGHER")
    month_line = (f"{label} ~= ${projected:,.0f}/mo "
                  f"(base $5 + R2 ${r2_gb * 0.015:,.0f} + D1 reads ${d1_read_cost:,.0f} "
                  f"[{mo_reads/D1_READS_INCLUDED*100:.0f}% of the 25B included] "
                  f"+ D1 writes ${d1_write_cost:,.0f} "
                  f"[{mo_writes/D1_WRITES_INCLUDED*100:.0f}% of the 50M included] "
                  f"+ D1 storage ${d1_over:,.0f} [{d1_gb_txt}] "
                  f"+ R2 operations {r2_op_txt}) [D1 source: {src_note}]{caveat}")
    n_reg = hf_registrations_24h()
    reg_line = ("hf registrations (24h): " + (f"{n_reg:,}" if n_reg >= 0 else "UNMEASURED"))
    report = ("\n".join(lines)
              + f"\nREADS:  {reads:,}/day (~${reads/1e9:.2f}/day at $0.001/M)"
              + f"\nWRITES: {writes:,}/day (~${writes/1e6:.2f}/day at $1.00/M)"
              + "\n" + reg_line
              + "\n\n" + bucket_text
              + "\n\n" + analytics_txt
              + "\n\n" + month_line)
    print(report)
    if n_reg == 0:
        # Not a workflow-red (billing is this guard's gated surface); a WARN email is the
        # meter Ahmed asked for after being the alarm himself on 2026-08-26.
        send_alert("hf registrations: ZERO in the last 24h",
                   report + "\n\nZero new users in a full day against a ~20-30/day baseline. "
                            "Check the register funnel: the Turnstile widget on "
                            "hfdatalibrary.com/pages/download, then POST /v1/auth/register "
                            "with a garbage token (a 400 'CAPTCHA verification failed' means "
                            "the endpoint is healthy and the break is client-side).")
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
