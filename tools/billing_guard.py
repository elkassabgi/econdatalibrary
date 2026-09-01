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
import math
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

# R2 CLASS-A OPERATIONS HAD NO ALARM AT ALL, and on 2026-08-31 they were the single largest
# line of the day: 2,880,378 ops (~$12.96) against 84,876 (~$0.38) on 08-30 — more than the
# D1 writes that same day. This guard measured them, printed them, and alarmed on neither.
# $4.50 per million, 1M included per month, so a single day over ~1M has spent the whole
# month's allowance. Steady state on this account is ~85k/day.
WARN_R2_A = 400_000         # ~$1.80/day
ALERT_R2_A = 1_000_000      # ~$4.50/day, and the entire monthly included allowance in a day

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
# The plan renews on the 9th — invoice IN-74622130 covers Jul 9..Aug 8, 2026. This matters
# twice: the INCLUDED allowances below reset on that date rather than the 1st, and the R2
# storage charge is a GB-MONTH average over exactly that window.
PERIOD_START_DAY = 9

_MEASURED = {"r2_class_a_day": None, "r2_class_b_day": None, "workers_day": None,
             "d1_reads_day": None, "d1_writes_day": None,
             # Storage is billed as an AVERAGE over the period, not a snapshot — see
             # r2_storage_period() for the measurement and why the snapshot was wrong.
             "r2_gb_avg": None, "r2_gb_days": 0, "r2_gb_latest": None,
             "period_days": 30, "period_elapsed": 0,
             # PERIOD-TO-DATE, because the included allowances are cumulative and reset only
             # on renewal. A daily rate cannot say how much of a monthly allowance is left.
             "d1_reads_ptd": None, "d1_writes_ptd": None, "r2_a_ptd": None,
             "r2_b_ptd": None, "workers_ptd": None, "days_elapsed": 0,
             "r2_elapsed": 0, "wk_elapsed": 0, "d1_gb_avg": None,
             # MEDIAN daily rates, not yesterday's: see _median() for the backtest that
             # shows a single day swinging the forecast by an order of magnitude.
             "d1_reads_med": None, "d1_writes_med": None, "r2_a_med": None,
             "r2_b_med": None, "workers_med": None,
             # COVERAGE, not just values. A GraphQL failure used to leave every value
             # None, whereupon the projection quietly fell back to the 10x-truncated
             # `wrangler d1 insights` AND added an R2 line it had priced at $0 —
             # while still printing the confident label "PROJECTED MONTH". That is
             # R503 (a guard failing open) and R505 (a total that omits a line it
             # printed) rebuilt inside the very tool written to fix them. This flag
             # makes the absence a FACT the total must react to.
             "graphql_ok": False}


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
WORKERS_INCLUDED = 10_000_000          # $0.30/M past this
# Texas data-processing services: 80% of the amount is taxable at 8.25% = a 6.60% uplift.
# Verified against IN-74622130: 154.96 x 0.80 x 0.0825 = $10.22 vs the billed $10.23.
TAX_UPLIFT = 1.066


def gb_months(mean_gb: float, n_days: float) -> float:
    """Convert a daily-mean level into the GB-MONTHS Cloudflare actually bills.

    A "GB-month" is 730 GB-hours, not one calendar month of whatever length. Pricing a
    31-day period as if its mean WERE the monthly quantity drops 31*24/730 = 1.9% of the
    charge, and always downward. Measured against invoice IN-74622130: the plain daily
    mean gives $21.72 against a billed $22.46 (-3.3%); this conversion gives $22.29
    (-0.75%), and the residual is sub-daily variation this dataset cannot resolve.
    """
    return mean_gb * n_days * 24.0 / 730.0


def units(n: float, included: float, price: float) -> float:
    """Cloudflare bills WHOLE units, always rounded up.

    Confirmed twice on invoice IN-74622130: 32,284,689 billable writes -> 33 units ->
    $33.00, and 20,325,560 billable Class A -> 21 units -> $94.50. Exact division
    understated that invoice by $3.75 (2.4%) and did so in the cheap direction, which is
    the dangerous one for a cost meter — an overstatement gets investigated within the
    hour, an understatement gets believed.

    THE TRAP: round the MONTHLY quantity only. Ceiling a DAILY figure and multiplying by
    30 turns 0.2M rows/day into 30 whole units, a 150x overstatement.
    """
    return math.ceil(max(0.0, n - included) / 1e6) * price


_DEGRADED = []          # every reason this run cannot see the whole meter


def _degrade(reason: str) -> None:
    """Record a coverage failure. THE POINT: the projection reads this list, so a blind
    spot cannot be printed and then omitted from the total (R505)."""
    if reason not in _DEGRADED:
        _DEGRADED.append(reason)
    print("  account analytics DEGRADED: " + reason)


def _graphql(token: str, query: str, variables: dict, attempt: int = 1):
    """One GraphQL call; None on any failure, and every failure recorded in _DEGRADED.

    RETRIED, matching `_insights_sorted`'s `_TRIES` (R222). Both paths talk to the same
    vendor with the same transience, and only one of them was hardened: a single blip on
    this path used to swap the whole account meter for `wrangler d1 insights` — a source
    the file's own comments call 10x low — without a word in the output.

    THE STATUS CODE IS PART OF THE REPORT. Printing `type(e).__name__` alone collapses a
    dead token (400), a throttle (429) and an outage (500) into one indistinguishable
    "fetch failed (HTTPError)". That is R504 exactly: I diagnosed a WORKING USPTO key as
    an entitlement problem from a probe that discarded the code that would have told me
    otherwise, and Ahmed was sent to buy a key he already had.
    """
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
            _degrade("GraphQL error: "
                     + msg.encode(enc, "replace").decode(enc, "replace"))
            return None
        acct = (((d.get("data") or {}).get("viewer") or {}).get("accounts") or [None])[0]
        if acct is None:
            # HTTP 200 with data.viewer.accounts == null is what "token not authorized for
            # this account" looks like. Indexing [0] on it used to raise and land in the
            # generic handler as an unexplained TypeError.
            _degrade("GraphQL returned no account (token not authorised for this account?)")
        return acct
    except Exception as e:  # noqa: BLE001 — meter, not gate
        code = getattr(e, "code", None)
        detail = f"{type(e).__name__}" + (f" HTTP {code}" if code else "")
        if attempt < _TRIES and code not in (400, 401, 403):   # auth errors will not heal
            time.sleep(5 * attempt)
            return _graphql(token, query, variables, attempt + 1)
        _degrade(f"GraphQL fetch failed after {attempt} attempt(s): {detail}")
        return None


def _rows(acct, field: str, limit: int):
    """Rows for one dataset, REFUSING a response that may have been truncated by `limit`.

    Measured 2026-08-30 on the live API, same query, limit 1 vs 2000:
        limit=1     errors absent, 1 row,  total requests         9
        limit=2000  errors absent, 24 rows, total requests 1,038,162
    A 115,000x undercount, HTTP 200, no error key, nothing to catch. That is the top-100
    truncation of `wrangler d1 insights` reborn one layer up, inside the query written to
    escape it — and the only way to see it is to compare the row count against the cap you
    asked for. A full page is not proof of completeness, so it is treated as suspect.
    """
    rows = (acct or {}).get(field) or []
    if len(rows) >= limit:
        _degrade(f"{field}: {len(rows)} rows == the limit of {limit} — the response may be "
                 f"TRUNCATED and every total from it is a floor")
    return rows


def _median(xs) -> float:
    """Median of the period's daily values.

    THE RATE MUST NOT BE ONE DAY. Backtested across Aug 9..30, what this guard WOULD have
    printed for D1 reads using yesterday's rate x remaining days: $0 on the 14th, $2,598 on
    the 15th, $2,459 on the 16th, $181 on the 20th, $315 on the 26th, $195 on the 30th. An
    order-of-magnitude swing that PEAKS the morning after an incident — precisely when Ahmed
    opens the email and precisely when a wrong number does the most damage. R2 Class A daily
    ops over the same period range 12,512 to 1,196,333, a 96x spread. The median is stable
    against exactly the one-day spikes that make the mean useless here.
    """
    s = sorted(xs)
    if not s:
        return 0.0
    mid = len(s) // 2
    return float(s[mid]) if len(s) % 2 else (s[mid - 1] + s[mid]) / 2.0


def _check_days(name: str, dates: list, want: int, yesterday: str) -> bool:
    """True only if this series can be billed from. Records WHY not, otherwise.

    THE FAILURE THIS EXISTS FOR, and it is the one my own first fix missed. Every
    dangerous path here returns HTTP 200 with no error key: a short day list, a day
    missing from the middle, an empty list read as "zero operations", or fewer than two
    days so `dates[-2]` cannot exist. On all of them `_graphql()` succeeded, so a relabel
    triggered by a None fetch NEVER FIRES — and the R2-operations term goes into the total
    as $0.00 under the word "PROJECTED". That is R505 rebuilt inside the fix for R505.
    Coverage is therefore asserted on the SUCCESS path, not inferred from the failure one.
    """
    if len(dates) < 2:
        _degrade(f"{name}: {len(dates)} day(s) returned — cannot price (needs a complete day)")
        return False
    ok = True
    if len(dates) < want:
        _degrade(f"{name}: {len(dates)} of {want} days returned ({dates[0]}..{dates[-1]}) "
                 f"— period totals are FLOORS")
        ok = False
    if dates[-2] != yesterday:
        # The rate must come from YESTERDAY. If the newest complete day is older, the rate
        # is stale and the remaining-days extrapolation silently uses it anyway.
        _degrade(f"{name}: newest complete day is {dates[-2]}, not {yesterday} — the daily "
                 f"rate is STALE")
        ok = False
    return ok


def r2_storage_period(tok: str, acct: str, start: str, end: str):
    """Mean stored GB per day across the billing period, or None.

    WHY NOT THE SNAPSHOT. R2 storage is billed in GB-MONTHS — the average over the period —
    and this tool priced it from `wrangler r2 bucket info` read at the moment of the run.
    Measured against invoice IN-74622130 (Jul 9..Aug 8, 2026): billed $22.46, snapshot
    estimate $13.87, a 62% understatement. The snapshot was not a lower bound and not an
    upper bound; it was the wrong quantity. The IP raw corpus (599 GB) was deleted on
    2026-08-18 mid-period, so the account carried storage for days the snapshot could not
    see. A number that only equals the bill when nothing changes is not a meter.

    THE AGGREGATION TRAP, measured 2026-08-29 and the reason bucketName AND storageClass are
    in `dimensions`: this dataset offers only `max`, so grouping by date alone returns the
    largest SINGLE bucket rather than the account total — 641.9 GB against a true 924.6 GB
    on the day, and 758.3 vs 1,486.1 GB-months across the invoice period, a 49% shortfall
    that carries no error and looks like a rigorous fix. Summing per-(bucket, class) maxima
    per day IS the account total; `max` rather than `avg` is right because each row is a
    running level, not a flow. storageClass is in the grouping because Standard and
    Infrequent Access price differently ($0.015 vs $0.010) and are separate rows — both are
    Standard here today, and pricing the sum at the Standard rate errs expensive, which is
    the only safe direction for a cost meter.

    Reconciled by the parallel review against invoice IN-74622130: 1,486.1 GB-months
    measured against the invoice's implied 1,497, i.e. 0.75% low. The snapshot it replaces
    was 38% low.
    """
    d = _graphql(tok, """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2StorageAdaptiveGroups(limit: 3000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date bucketName storageClass } max { payloadSize metadataSize } } } } }""",
                  {"acct": acct, "start": start, "end": end})
    if d is None:
        return None
    per = {}
    for r in _rows(d, "r2StorageAdaptiveGroups", 3000):
        m = r["max"]
        per[r["dimensions"]["date"]] = per.get(r["dimensions"]["date"], 0) + \
            (m["payloadSize"] or 0) + (m["metadataSize"] or 0)
    if len(per) < 2:
        # SAY SO. Returning a bare None here sent the projection back to the `wrangler r2
        # bucket info` snapshot — $13.72 against the blend's $23.44 — with nothing in the
        # label to show the instrument had changed underneath the number.
        _degrade(f"R2 storage: {len(per)} day(s) returned — falling back to the SNAPSHOT, "
                 f"which measured 38% below the invoice on the one period we can check")
        return None
    # DROP TODAY. Buckets report into this dataset at different times of day, so the current
    # UTC date is a PARTIAL account: measured 2026-08-30T00:20Z it carried econ-data alone
    # (642 GB) while the account held 925 GB, because hfdatalibrary-data had not yet landed.
    # Left in, that missing-bucket day reads as a 31% overnight deletion and drags the mean.
    # Same "newest COMPLETE day, never today" rule the R2-ops and D1 blocks already use.
    days = sorted(per)[:-1]
    return (sum(per[x] for x in days) / len(days) / 1e9, len(days), per[days[-1]] / 1e9)


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
        # A MISSING TOKEN IS A RED RUN, not a footnote. Printing the blind spot was supposed
        # to be enough; it was not. Every scheduled run took this branch, priced R2
        # operations, Workers and the storage MEAN at zero, fell back to a D1 source it knows
        # is ~10x low, and exited 0 — so the workflow was GREEN through the 2026-08-31 spike
        # and reported a "PARTIAL MONTH FLOOR ~= $13/mo" against a real $328. Measured the
        # same minute on the same commit, with and without the token.
        #
        # This is distinct from _DEGRADED below, which stays green on purpose: a failed query
        # is a vendor blip, an absent token is a permanent configuration hole that nothing
        # else will ever report.
        _MEASURED["unmetered"] = True
        return ("ACCOUNT ANALYTICS: UNMETERED — R2 operations and Workers usage are "
                "invisible to this guard, D1 falls back to a ~10x-low source, and the "
                "month figure below is a FLOOR that will read far under the real bill. "
                "Set CF_ANALYTICS_TOKEN (read-only 'Account Analytics: Read' token) in "
                ".env and in the repo secrets to close the gap.")
    # UTC, NOT LOCAL. GraphQL's `date` dimension is UTC (proven 2026-08-29: the API's own
    # error carried 2026-08-30T00:13Z while the local clock read 08-29 19:13, UTC-5). With
    # date.today() this window ended a UTC day EARLY for five hours every evening, so the
    # "newest complete day" was wrong exactly when someone was most likely to be looking.
    _utc_today = _dt.datetime.now(_dt.timezone.utc).date()
    end = _utc_today.isoformat()
    # THE WINDOW IS THE BILLING PERIOD, not the last three days. The included allowances are
    # consumed CUMULATIVELY and reset only on renewal, so a daily rate x30 cannot say how much
    # of them is left. Measured 2026-08-30: this tool printed "14% of the 25B included" from
    # a quiet day's 117M reads x30, while the period since Aug 9 had ALREADY spent ~218B —
    # the allowance was exhausted nine times over and the guard was reporting headroom.
    # Rate answers "is today anomalous"; period-to-date answers "what will the invoice say".
    # Both are needed and they are different questions.
    #
    # Clamped to 31 days because this API rejects any span wider than 32 — the rejection that
    # used to fail open into a cheap answer, so the clamp is correctness, not tidiness.
    #
    # PERIOD_START_DAY must be <= 28 or `replace(day=...)` raises ValueError in February —
    # inside account_analytics(), called bare from main(), so the whole billing run would
    # die with a traceback rather than print a number. R415: the error path IS the failure.
    _pd = min(PERIOD_START_DAY, 28)
    pstart = _utc_today.replace(day=_pd)
    if _utc_today.day < _pd:                              # still inside last month's period
        prev = _utc_today.replace(day=1) - _dt.timedelta(days=1)
        pstart = prev.replace(day=_pd)
    pstart = max(pstart, _utc_today - _dt.timedelta(days=31))
    start = pstart.isoformat()
    # A PURE CALENDAR FACT, computed here and never gated on a network call. It used to live
    # inside the storage branch, so a storage fetch failure left it at its default 30 while
    # `days_elapsed` came from D1 — quietly shortening the remaining-days term for EVERY
    # series. Nothing about the length of a billing period depends on R2 answering.
    _MEASURED["period_days"] = (
        (pstart.replace(day=1) + _dt.timedelta(days=32)).replace(day=_pd) - pstart).days
    _SHOW = 3                     # print the newest few days; measure the whole period
    _WANT = (_utc_today - pstart).days + 1
    _YDAY = (_utc_today - _dt.timedelta(days=1)).isoformat()
    out = []
    a = _graphql(tok, """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2OperationsAdaptiveGroups(limit: 2000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date actionType } sum { requests } } } } }""",
                 {"acct": acct, "start": start, "end": end})
    if a is not None:
        per = {}
        for r in _rows(a, "r2OperationsAdaptiveGroups", 2000):
            dd = r["dimensions"]
            cls = "B" if dd["actionType"] in _R2_CLASS_B else "A"
            per.setdefault(dd["date"], {"A": 0, "B": 0})
            per[dd["date"]][cls] += r["sum"]["requests"]
        dates = sorted(per)
        for date in dates[-_SHOW:]:
            ca, cb = per[date]["A"], per[date]["B"]
            out.append(f"R2 ops {date}: ClassA {ca:,} (~${ca / 1e6 * 4.50:.2f}) "
                       f"ClassB {cb:,} (~${cb / 1e6 * 0.36:.2f})")
        # TODAY, PARTIAL, ON PURPOSE. The complete-day figure is the right basis for BILLING
        # arithmetic, and the wrong basis for an ALARM: a leak that starts at 09:00 is not
        # visible in a complete day until the next one, so the guard would report it up to
        # ~24 h late no matter how often it runs. On 2026-08-31 class-A ops went 84,876 ->
        # 2,880,378 in a day (~$0.38 -> ~$12.96); at a 30-minute cadence that is catchable
        # within the hour, but only if today counts.
        if dates:
            _MEASURED["r2_class_a_today"] = per[dates[-1]]["A"]
            _MEASURED["r2_class_b_today"] = per[dates[-1]]["B"]
        if _check_days("R2 operations", dates, _WANT, _YDAY):
            _MEASURED["r2_class_a_day"] = per[dates[-2]]["A"]   # newest COMPLETE day
            _MEASURED["r2_class_b_day"] = per[dates[-2]]["B"]
            _MEASURED["r2_a_ptd"] = sum(per[d]["A"] for d in dates[:-1])
            _MEASURED["r2_b_ptd"] = sum(per[d]["B"] for d in dates[:-1])
            # PER-SERIES elapsed. Borrowing D1's day count for this series' remaining-days
            # term double-undercounts whenever the two datasets return different windows:
            # the period total is short AND the forecast is computed from someone else's
            # longer elapsed. All four agree today; nothing enforced it.
            _MEASURED["r2_elapsed"] = len(dates) - 1
            _MEASURED["r2_a_med"] = _median([per[d]["A"] for d in dates[:-1]])
            _MEASURED["r2_b_med"] = _median([per[d]["B"] for d in dates[:-1]])
            out.append(f"R2 ops SINCE {dates[0]}: ClassA {_MEASURED['r2_a_ptd']:,} "
                       f"over {len(dates)-1} complete days")
    w = _graphql(tok, """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    workersInvocationsAdaptive(limit: 500, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date } sum { requests } } } } }""",
                 {"acct": acct, "start": start, "end": end})
    if w is not None:
        per = {}
        for r in _rows(w, "workersInvocationsAdaptive", 500):
            per[r["dimensions"]["date"]] = per.get(r["dimensions"]["date"], 0) + r["sum"]["requests"]
        dates = sorted(per)
        for date in dates[-_SHOW:]:
            n = per[date]
            out.append(f"Workers requests {date}: {n:,} (~${n / 1e6 * 0.30:.2f} past free tier)")
        # FETCHED, PRINTED, AND NOW SUMMED. `workers_day` was initialised in _MEASURED and
        # never assigned, so this line has been visible-but-unpriced since the carrier was
        # added — the same measured-but-unpriced shape as R505's R2 operations, sitting four
        # lines above a total that ignored it. The parallel review measured it over the whole
        # invoice period: 712,600 requests, 7.1% of the 10M included, and the invoice indeed
        # carries no Workers line. So it is genuinely $0 — but "genuinely $0" is now a
        # MEASUREMENT the projection makes, not an assumption it inherits.
        if _check_days("Workers requests", dates, _WANT, _YDAY):
            _MEASURED["workers_ptd"] = sum(per[d] for d in dates[:-1])
            _MEASURED["workers_day"] = per[dates[-2]]
            _MEASURED["wk_elapsed"] = len(dates) - 1
            _MEASURED["workers_med"] = _median([per[d] for d in dates[:-1]])
    d1 = _graphql(tok, """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1AnalyticsAdaptiveGroups(limit: 500, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date } sum { rowsRead rowsWritten } } } } }""",
                 {"acct": acct, "start": start, "end": end})
    if d1 is not None:
        per = {}
        for r in _rows(d1, "d1AnalyticsAdaptiveGroups", 500):
            p = per.setdefault(r["dimensions"]["date"], [0, 0])
            p[0] += r["sum"]["rowsRead"]
            p[1] += r["sum"]["rowsWritten"]
        dates = sorted(per)
        for date in dates[-_SHOW:]:
            rr, rw = per[date]
            out.append(f"D1 TRUE totals {date}: {rr:,} read (~${rr / 1e9:.2f}) / "
                       f"{rw:,} written (~${rw / 1e6:.2f}) — GraphQL, not top-100")
        if _check_days("D1 analytics", dates, _WANT, _YDAY):
            _MEASURED["d1_reads_day"], _MEASURED["d1_writes_day"] = per[dates[-2]]
            # Same reasoning as the R2 block: today is partial and that is exactly why the
            # alarm needs it. 2026-08-31 wrote 11,412,906 rows (~$11.41) against 678,127 the
            # day before, and the guard's only same-day view was a complete-day figure that
            # would not include it until 2026-09-01.
            _MEASURED["d1_reads_today"] = per[dates[-1]][0]
            _MEASURED["d1_writes_today"] = per[dates[-1]][1]
            _MEASURED["d1_reads_ptd"] = sum(per[d][0] for d in dates[:-1])
            _MEASURED["d1_writes_ptd"] = sum(per[d][1] for d in dates[:-1])
            _MEASURED["days_elapsed"] = len(dates) - 1
            _MEASURED["d1_reads_med"] = _median([per[d][0] for d in dates[:-1]])
            _MEASURED["d1_writes_med"] = _median([per[d][1] for d in dates[:-1]])
            _MEASURED["graphql_ok"] = True
            out.append(f"D1 SINCE {dates[0]}: {_MEASURED['d1_reads_ptd']:,} read / "
                       f"{_MEASURED['d1_writes_ptd']:,} written over "
                       f"{_MEASURED['days_elapsed']} complete days — this is what the "
                       f"included allowance is measured against")
    # D1 STORAGE HAS THE SAME SNAPSHOT DEFECT, four lines from the R2 one. `d1_storage_gb()`
    # reads `wrangler d1 list --json` file_size — an instantaneous level — and prices it
    # against a GB-MONTH charge, which is exactly the bug being fixed for R2. Measured by the
    # parallel review over 32 days: min 1.60, max 9.44, mean 7.26 GB; the snapshot overstates
    # the overage by 48%. Ahmed's standing rule is that a reported example is one instance of
    # a class (feedback_example_means_class) — fixing the R2 snapshot and leaving its twin is
    # how the same entry gets written twice.
    ds = _graphql(tok, """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1StorageAdaptiveGroups(limit: 500, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date } max { databaseSizeBytes } } } } }""",
                  {"acct": acct, "start": start, "end": end})
    if ds is not None:
        per = {}
        for r in _rows(ds, "d1StorageAdaptiveGroups", 500):
            per[r["dimensions"]["date"]] = max(per.get(r["dimensions"]["date"], 0),
                                               r["max"]["databaseSizeBytes"] or 0)
        days = sorted(per)[:-1]                  # drop today, same partial-day rule
        if days:
            _MEASURED["d1_gb_avg"] = sum(per[d] for d in days) / len(days) / 1e9
            out.append(f"D1 stored: {_MEASURED['d1_gb_avg']:,.1f} GB mean over {len(days)} "
                       f"complete days (billed as GB-months, not today's size)")
    # Storage over the same billing-period window: the charge is an average, and the period
    # is the averaging window.
    st = r2_storage_period(tok, acct, start, end)
    if st is not None:
        avg_gb, n_days, last_gb = st
        _MEASURED["r2_gb_avg"], _MEASURED["r2_gb_days"] = avg_gb, n_days
        _MEASURED["r2_gb_latest"] = last_gb
        _MEASURED["period_elapsed"] = n_days
        out.append(f"R2 stored: {avg_gb:,.0f} GB mean over {n_days} complete days since "
                   f"{pstart} (latest complete day {last_gb:,.0f} GB) — the invoice charges "
                   f"GB-MONTHS, i.e. this mean, not the current size")
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
    # PREFER THE GraphQL TRUE TOTALS over `wrangler d1 insights`, which sums only the top
    # 100 query SHAPES: measured 2026-08-29, insights reported 36.1M reads/day for the
    # fleet while GraphQL reported 374.3M for the same account — a 10x truncation. The
    # tool was already FETCHING the true number and still projecting from the small one.
    d1_r_day = _MEASURED["d1_reads_day"] if _MEASURED["d1_reads_day"] is not None else reads
    d1_w_day = _MEASURED["d1_writes_day"] if _MEASURED["d1_writes_day"] is not None else writes
    src_note = "GraphQL" if _MEASURED["d1_reads_day"] is not None else "insights (TRUNCATED)"
    # A GraphQL failure is a COVERAGE failure, and coverage failures must reach the label.
    # Without this the tool degraded silently: the D1 line reverted to the 10x-truncated
    # insights figure, the R2-operations line was priced at $0 while printing "UNMETERED",
    # and the header still read "PROJECTED MONTH" — a confident total assembled from a
    # broken instrument. Measured trigger: any window wider than 32 days errors, so the
    # very act of measuring a full billing period the wrong way produced a cheap answer.
    graphql_failed = not _MEASURED["graphql_ok"]
    # SPENT SO FAR + WHAT THE CURRENT RATE ADDS BEFORE RENEWAL. Never rate x30: the
    # allowances are cumulative and this period has already burned through the D1 read
    # allowance nine times over on two incident days (Aug 14: 100.6B, Aug 15: 95.1B),
    # which a quiet Tuesday x30 reports as "14% of the 25B included".
    p_days = _MEASURED["period_days"]
    elapsed = _MEASURED["days_elapsed"]

    def _month(ptd, med_rate, fallback_rate, series_elapsed):
        """(period total, period-to-date, measured?) for one series.

        EACH SERIES USES ITS OWN elapsed — see the R2 block. And the forecast half uses the
        MEDIAN daily rate, not yesterday's, because yesterday is what makes this number
        swing 13x between two consecutive mornings (_median's backtest).
        """
        if ptd is None or not series_elapsed:
            return (fallback_rate or 0) * 30.0, 0.0, False
        left = max(0, p_days - series_elapsed)
        return ptd + (med_rate or 0) * left, float(ptd), True

    mo_reads, acc_reads, ptd_ok = _month(_MEASURED["d1_reads_ptd"],
                                         _MEASURED["d1_reads_med"], d1_r_day, elapsed)
    mo_writes, acc_writes, _ = _month(_MEASURED["d1_writes_ptd"],
                                      _MEASURED["d1_writes_med"], d1_w_day, elapsed)
    basis = (f"{elapsed}/{p_days}d SPENT + {max(0, p_days-elapsed)}d forecast at the "
             f"period median" if ptd_ok else "NO PERIOD DATA — daily rate x30")

    d1_read_cost = units(mo_reads, D1_READS_INCLUDED, 0.001)
    d1_write_cost = units(mo_writes, D1_WRITES_INCLUDED, 1.00)
    acc_read_cost = units(acc_reads, D1_READS_INCLUDED, 0.001)
    acc_write_cost = units(acc_writes, D1_WRITES_INCLUDED, 1.00)
    # D1 storage from the period mean, snapshot only as a labelled fallback.
    if _MEASURED["d1_gb_avg"] is not None:
        d1_sgb, d1_ssrc = _MEASURED["d1_gb_avg"], f"{elapsed}d mean"
    else:
        d1_sgb, d1_ssrc = (d1_gb, "SNAPSHOT") if d1_gb >= 0 else (0.0, "unmeasured")
    d1_over = max(0.0, gb_months(d1_sgb, p_days) - 5.0) * 0.75
    d1_gb_txt = f"{d1_sgb:.1f} GB, {d1_ssrc}" if d1_ssrc != "unmeasured" else "unmeasured"
    ca_day, cb_day = _MEASURED["r2_class_a_day"], _MEASURED["r2_class_b_day"]
    r2_op_cost = 0.0
    r2_op_txt = "UNMETERED"
    acc_op_cost = 0.0
    if ca_day is not None:
        re_ = _MEASURED["r2_elapsed"]
        mo_a, acc_a, _ = _month(_MEASURED["r2_a_ptd"], _MEASURED["r2_a_med"], ca_day, re_)
        mo_b, acc_b, _ = _month(_MEASURED["r2_b_ptd"], _MEASURED["r2_b_med"], cb_day, re_)
        r2_op_cost = (units(mo_a, R2_CLASS_A_INCLUDED, 4.50)
                      + units(mo_b, R2_CLASS_B_INCLUDED, 0.36))
        acc_op_cost = (units(acc_a, R2_CLASS_A_INCLUDED, 4.50)
                       + units(acc_b, R2_CLASS_B_INCLUDED, 0.36))
        r2_op_txt = (f"${r2_op_cost:,.0f} [{mo_a/1e6:.1f}M class-A vs 1M included]")
    # Workers: 10M requests included, $0.30/M after. Priced, not just printed (see the
    # comment at the assignment site).
    w_day = _MEASURED["workers_day"]
    wk_cost = wk_mo = 0.0
    if w_day is not None:
        wk_mo, _, _ = _month(_MEASURED["workers_ptd"], _MEASURED["workers_med"], w_day,
                             _MEASURED["wk_elapsed"])
        wk_cost = units(wk_mo, WORKERS_INCLUDED, 0.30)
    wk_txt = (f"${wk_cost:,.0f} [{wk_mo/1e6:.1f}M vs 10M included]"
              if w_day is not None else "UNMETERED")
    # Storage: the period MEAN when GraphQL measured it, the snapshot only as a fallback —
    # and the fallback says so, because on the one period we can check against an invoice
    # the snapshot was 62% low.
    # NO 10 GB DEDUCTION. It is in R2's published free tier, but deducting it moves the fit
    # AWAY from the one invoice we can check (-0.75% becomes -1.42%), so on this plan the
    # allowance is evidently not applied. An unverified allowance that biases the number
    # cheap is exactly R502 — do not re-add it without an invoice that shows it.
    if _MEASURED["r2_gb_avg"] is not None:
        # Elapsed days at their MEASURED level + remaining days at the CURRENT level. Neither
        # half alone is the bill: over Aug 9..29 the mean-to-date is 1,881 GB (it still bills
        # the 2.4 TB deleted on the 18th) while the current size is 925 GB (it pretends those
        # ten days never happened). The blend is ~1,573 GB. Full-period figures only converge
        # on the last day, which is when this line becomes a measurement rather than a guess.
        pd_, pe = _MEASURED["period_days"], _MEASURED["period_elapsed"]
        st_gb = ((_MEASURED["r2_gb_avg"] * pe
                  + (_MEASURED["r2_gb_latest"] or _MEASURED["r2_gb_avg"]) * max(0, pd_ - pe))
                 / pd_) if pd_ else _MEASURED["r2_gb_avg"]
        st_src = (f"{pe}/{pd_}d measured at {_MEASURED['r2_gb_avg']:,.0f} GB mean, rest at "
                  f"today's {_MEASURED['r2_gb_latest']:,.0f} GB")
    else:
        st_gb, st_src = r2_gb, "SNAPSHOT, not the billed mean — expect an UNDERSTATEMENT"
    # GB-MONTHS, not a GB mean. A Cloudflare month is 730 hours, so a 31-day period's mean
    # must be scaled by 31*24/730 = 1.019. Pricing the bare mean dropped 1.9% of the charge,
    # always downward: measured on invoice IN-74622130, $21.72 against a billed $22.46
    # (-3.3%); with this conversion, $22.29 (-0.75%).
    r2_store_cost = gb_months(st_gb, p_days) * 0.015
    acc_store_cost = (gb_months(_MEASURED["r2_gb_avg"], _MEASURED["period_elapsed"]) * 0.015
                      if _MEASURED["r2_gb_avg"] is not None else r2_store_cost)
    projected = (5.0 + r2_store_cost + d1_read_cost + d1_write_cost + d1_over
                 + r2_op_cost + wk_cost)
    # THE ACCRUED FLOOR: what the period has ALREADY cost, with no forecast in it. This is
    # the number to lead with. The reviewer's backtest is the argument — on 2026-08-16 the
    # forecast half of the projection would have read $2,459 for D1 reads and on 2026-08-20
    # it would have read $181, from the same period and the same true spend. The floor
    # moved monotonically from $0 to $194 across those same days. One of those two numbers
    # is an instrument; the other is a rumour about the future.
    accrued = (5.0 + acc_store_cost + acc_read_cost + acc_write_cost + acc_op_cost)
    # A total that silently omits a database is not a projection, it is a floor, and it
    # must not be readable as the bill. On 2026-08-23 econ-catalog failed to measure and
    # the run printed "PROJECTED MONTH ~= $28/mo"; econ-catalog alone carries 109.6M
    # reads and 224k writes a day, which puts the real figure nearer $37. The number was
    # wrong by a third and nothing in that line said so - the coverage gap was disclosed
    # four lines earlier and in the exit code, both easy to skim past when a dollar figure
    # is sitting right there.
    label = ("PROJECTED MONTH" if not (failed or graphql_failed or _DEGRADED)
             else "PARTIAL MONTH FLOOR")
    notes = []
    if failed:
        notes.append(f"EXCLUDES {', '.join(failed)}, which did not measure")
    if graphql_failed:
        # The loudest possible statement of the degradation, ON the dollar figure. Everything
        # below the D1 line either fell back to a 10x-truncated source or was priced at zero.
        notes.append("ACCOUNT ANALYTICS DID NOT MEASURE: D1 fell back to top-100 insights "
                     "(~10x low) and R2 operations, Workers and storage-mean are priced at "
                     "ZERO here")
    # EVERY recorded blind spot reaches the dollar figure. _DEGRADED is appended to by the
    # fetch layer AND by the per-series coverage checks, so a truncated page or a short day
    # list — both HTTP 200, both invisible to a fetch-failure test — degrade the label too.
    notes.extend(_DEGRADED)
    caveat = "" if not notes else (" -- " + "; ".join(notes)
                                   + "; the real bill is HIGHER")
    # AN ALLOWANCE PERCENTAGE FROM A BLIND SOURCE IS NOT A MEASUREMENT, AND IT INVERTS THE
    # DECISION. Without CF_ANALYTICS_TOKEN this printed "D1 reads $0 [71% of the 25B
    # included]" — which reads as 29% of headroom left — on a day the period had actually
    # spent 887.6% of it, i.e. the allowance was exhausted nine times over and every read
    # was already billable. The caveat below said "the real bill is HIGHER", and a reader
    # looking at a headroom percentage still concludes there is headroom. A percentage
    # computed from a top-100-truncated, non-period source cannot see the allowance at all,
    # so it must say so rather than produce a reassuring number (R502's class: pricing
    # against an allowance the instrument has not measured).
    _blind = bool(graphql_failed)
    _reads_pos = ("ALLOWANCE POSITION UNMEASURED" if _blind
                  else f"{mo_reads/D1_READS_INCLUDED*100:.0f}% of the 25B included")
    _writes_pos = ("ALLOWANCE POSITION UNMEASURED" if _blind
                   else f"{mo_writes/D1_WRITES_INCLUDED*100:.0f}% of the 50M included")
    month_line = (f"{label} ~= ${projected:,.0f}/mo "
                  f"(base $5 + R2 storage ${r2_store_cost:,.0f} [{st_gb:,.0f} GB, {st_src}] "
                  f"+ Workers {wk_txt} + D1 reads ${d1_read_cost:,.0f} "
                  f"[{_reads_pos}] "
                  f"+ D1 writes ${d1_write_cost:,.0f} "
                  f"[{_writes_pos}] "
                  f"+ D1 storage ${d1_over:,.0f} [{d1_gb_txt}] "
                  f"+ R2 operations {r2_op_txt}) [D1 source: {src_note}; basis: {basis}]"
                  f"{caveat}")
    # WHAT AHMED ACTUALLY PAYS. Invoice IN-74622130 charged $10.23 tax on $154.96, which is
    # not 8.25% — it is Texas's data-processing rule, 80% of the amount taxable at 8.25%, an
    # effective 6.60% uplift (154.96 x 0.80 x 0.0825 = $10.22, against the billed $10.23).
    # Every figure this tool has ever printed was pre-tax and read as the bill.
    total_line = (f"ACCRUED SO FAR ${accrued:,.0f} ({elapsed}/{p_days} days, already spent, "
                  f"no forecast) -> WITH TAX ${accrued * TAX_UPLIFT:,.0f}"
                  f"\nFULL PERIOD IF THE MEDIAN DAY REPEATS ${projected:,.0f} "
                  f"-> WITH TAX ${projected * TAX_UPLIFT:,.0f}   "
                  f"(TX data-processing: 80% of the subtotal taxed at 8.25% = +6.6%)")
    n_reg = hf_registrations_24h()
    reg_line = ("hf registrations (24h): " + (f"{n_reg:,}" if n_reg >= 0 else "UNMEASURED"))
    report = ("\n".join(lines)
              + f"\nREADS:  {reads:,}/day (~${reads/1e9:.2f}/day at $0.001/M)"
              + f"\nWRITES: {writes:,}/day (~${writes/1e6:.2f}/day at $1.00/M)"
              + "\n" + reg_line
              + "\n\n" + bucket_text
              + "\n\n" + analytics_txt
              + "\n\n" + month_line
              + "\n" + total_line)
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
    # ALARM ON THE TRUE METER, NOT THE TRUNCATED ONE. Until now the thresholds compared
    # `wrangler d1 insights` — which sums only the top 100 query SHAPES and measures ~10x
    # low — while the report beside them printed the GraphQL total. The alarm was therefore
    # running an order of magnitude below the number it displayed. Backtested against this
    # period: 2026-08-10 truly read 5.4B and 2026-08-25 read 8.9B, both past ALERT_ROWS,
    # and both would have arrived at insights as ~0.5B and ~0.9B — under even the WARN
    # threshold. The two biggest days were loud enough to trip it anyway; the shoulder days
    # were not, and they are the ones a guard exists to catch early.
    alarm_r = d1_r_day if _MEASURED["d1_reads_day"] is not None else reads
    alarm_w = d1_w_day if _MEASURED["d1_writes_day"] is not None else writes
    alarm_src = "GraphQL true total" if _MEASURED["d1_reads_day"] is not None else \
                "insights (TRUNCATED ~10x low — thresholds are effectively 10x higher)"
    thresholds = (f"\n\nAlarm read {alarm_r:,} / wrote {alarm_w:,} on the newest complete "
                  f"day, source: {alarm_src}.")
    # EVERY BREACH REDDENS THE WORKFLOW. Until 2026-09-01 only ALERT did, and WARN's only
    # delivery was a Resend email — which has never been configured on this account, so the
    # guard prints "RESEND_API_KEY not set - email skipped" and exits 0. On 2026-08-31 D1
    # read 2,805,188,474 rows: comfortably over WARN_ROWS, under ALERT_ROWS, so the run went
    # GREEN and nothing reached anyone. Ahmed found that day on his invoice.
    #
    # A red workflow is the one delivery path that needs no secret, because GitHub emails the
    # repo owner on failure. So WARN reddens too. If that proves noisy the answer is to move
    # the threshold to where the noise stops, never to make a breach silent again.
    #
    # TODAY'S PARTIAL COUNTS. A complete-day alarm is up to ~24 h late by construction, which
    # defeats a 30-minute cadence.
    if _MEASURED.get("unmetered"):
        # Cannot measure => cannot reassure. Exit before the threshold arithmetic, which
        # would otherwise compare zeros against limits and pass.
        send_alert("BILLING GUARD IS BLIND: CF_ANALYTICS_TOKEN is not set",
                   report + "\n\nThis run measured NOTHING that matters: R2 operations, "
                            "Workers and the storage mean were priced at zero and D1 fell "
                            "back to a source that reads ~10x low. Set CF_ANALYTICS_TOKEN "
                            "(read-only 'Account Analytics: Read') in the repo secrets.")
        print("BILLING GUARD BLIND - reddening the workflow: CF_ANALYTICS_TOKEN is not set, "
              "so R2 operations are unmetered and D1 is measured ~10x low. The month figure "
              "printed above is a FLOOR, not the bill.")
        return 1

    r2a_day = _MEASURED.get("r2_class_a_day") or 0
    r2a_today = _MEASURED.get("r2_class_a_today") or 0
    d1r_today = _MEASURED.get("d1_reads_today") or 0
    d1w_today = _MEASURED.get("d1_writes_today") or 0
    breaches = []
    if alarm_r > ALERT_ROWS:
        breaches.append(f"D1 reads {alarm_r:,} > ALERT {ALERT_ROWS:,} (complete day)")
    elif alarm_r > WARN_ROWS:
        breaches.append(f"D1 reads {alarm_r:,} > WARN {WARN_ROWS:,} (complete day)")
    if alarm_w > ALERT_WRITES:
        breaches.append(f"D1 writes {alarm_w:,} > ALERT {ALERT_WRITES:,} (complete day)")
    elif alarm_w > WARN_WRITES:
        breaches.append(f"D1 writes {alarm_w:,} > WARN {WARN_WRITES:,} (complete day)")
    if r2a_day > ALERT_R2_A:
        breaches.append(f"R2 class-A {r2a_day:,} > ALERT {ALERT_R2_A:,} (complete day, "
                        f"~${r2a_day / 1e6 * 4.50:.2f})")
    elif r2a_day > WARN_R2_A:
        breaches.append(f"R2 class-A {r2a_day:,} > WARN {WARN_R2_A:,} (complete day, "
                        f"~${r2a_day / 1e6 * 4.50:.2f})")
    if r2a_today > ALERT_R2_A:
        breaches.append(f"R2 class-A {r2a_today:,} TODAY SO FAR > ALERT {ALERT_R2_A:,} "
                        f"(~${r2a_today / 1e6 * 4.50:.2f} and the day is not over)")
    if d1r_today > ALERT_ROWS:
        breaches.append(f"D1 reads {d1r_today:,} TODAY SO FAR > ALERT {ALERT_ROWS:,}")
    if d1w_today > ALERT_WRITES:
        breaches.append(f"D1 writes {d1w_today:,} TODAY SO FAR > ALERT {ALERT_WRITES:,}")

    if breaches:
        body = (report + thresholds + "\n\nBREACHES:\n- " + "\n- ".join(breaches)
                + "\n\nThe catalogue sync is the usual cause of a D1 spike: `series_fts` is "
                  "fts5(series_id UNINDEXED), so every id-scoped statement full-scans "
                  "~23.8M rows. Both call sites are gated behind CATALOG_SYNC_ENABLED; if "
                  "that variable is set, unset it. Then check `wrangler d1 insights` for "
                  "query shapes (R430).")
        send_alert("BILLING ALERT: " + breaches[0], body)
        print("BILLING BREACH - reddening the workflow:")
        for b in breaches:
            print("  " + b)
        return 1
    if _DEGRADED:
        # Deliberately NOT a red run. A guard that reddens on a vendor blip becomes noise,
        # and noise is how Ahmed's original alert became "ineffective" (this file's own
        # docstring). But a blind spot must still be delivered, because while the analytics
        # are down the alarm above silently reverts to the 10x-low source.
        send_alert("Billing guard: measuring with a blind spot",
                   report + "\n\nDEGRADED:\n- " + "\n- ".join(_DEGRADED)
                   + "\n\nThe totals above are FLOORS. While this persists the D1 alarm "
                     "falls back to `wrangler d1 insights`, which measures ~10x low.")
        print("DEGRADED (not reddening): " + "; ".join(_DEGRADED))
    return 0


if __name__ == "__main__":
    sys.exit(main())
