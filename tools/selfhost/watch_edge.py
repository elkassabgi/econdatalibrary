"""Off-machine daily check of the self-hosted econ API (docs/ECON_SELF_HOSTING_PLAN.md, code change 6).

Runs in GitHub Actions (.github/workflows/selfhost-watch.yml), never on the workstation it watches: a
check that runs on the machine it watches goes silent exactly when that machine is down.

Three checks, each a FAILURE when it cannot measure (a blind check is louder than a passing one):

  1. UP AND FRESH - the public edge answers /v1/sources and /v1/last-updates with 200 JSON, and the
     newest `last_updated` in /v1/last-updates is under FRESH_HOURS old (an edge serving cached answers
     while nothing updates would otherwise pass).
  2. THE DEPLOYED EDGE IS THE EXPECTED ONE - /v1/edge-status must report the same effective state AND the
     same raw FORWARD / EDGE_STATE values that api/worker/wrangler.toml in this checkout commits. Whenever
     the committed state is not the default (FORWARD on, or EDGE_STATE users), a missing route (404) or a
     missing commit id is a FAILURE: the old worker has no route, and "no route" must never read as fine
     (review R1174 blocker - a stale deploy passed).
  3. NO WRITES TO THE RETIRED CLOUD COPY - from T0 (--no-writes-since, an ISO UTC moment such as
     2026-10-05T14:00:00Z) any R2 write on the econ bucket, and any D1 row or write query on the two econ
     catalogue databases, read from Cloudflare's GraphQL analytics, is a failure. The window is the last
     WINDOW_DAYS days clipped at T0 (the datasets refuse ranges over 32 days, R1174). The SAME queries
     carry positive controls - buckets and the users database that are active every day (measured 8 of
     8 days, NUMBERS) - and no activity for a control means the check is BLIND, never "zero writes".

Exit 1 on any failure, with an email through Resend when RESEND_API_KEY is set; the red workflow run is
the second delivery path. Needs CF_ANALYTICS_TOKEN (Account Analytics: Read) for check 3 only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import tomllib
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WRANGLER = os.path.join(ROOT, "api", "worker", "wrangler.toml")
EDGE = "https://econdl-api.elkassabgi.workers.dev"
BUCKET = "econ-data"
CONTROL_BUCKETS = ("hfdatalibrary-data", "ipdatalibrary")      # active every day, measured 2026-09-24
WINDOW_DAYS = 30
FRESH_HOURS = 48
UA = "econdatalibrary-selfhost-watch/1.0"
# R2 operations that do not write. Everything else (incl. a FAILED write attempt) counts as a write.
R2_READ_ACTIONS = frozenset({"GetObject", "HeadObject", "HeadBucket", "ListBuckets", "GetBucket",
                             "ListObjects", "ListObjectsV2", "ListMultipartUploads", "ListParts",
                             "UsageSummary", "GetBucketEncryption", "GetBucketLocation",
                             "GetBucketCors", "GetBucketLifecycleConfiguration"})


def committed_config(path: str = WRANGLER) -> dict:
    """What this checkout says the deployed edge must be. account_id sits inside [limits] in today's
    wrangler.toml (misplaced; plan section 2), so both places are read."""
    with open(path, "rb") as fh:
        cfg = tomllib.load(fh)
    v = cfg.get("vars", {})
    ids = {d["binding"]: d["database_id"] for d in cfg.get("d1_databases", [])}
    missing = [b for b in ("CATALOG", "CATALOG_CLIMATE", "USERS") if b not in ids]
    if missing:
        raise SystemExit(f"{path}: D1 bindings {missing} not found (have {sorted(ids)})")
    forward_raw, state_raw = v.get("FORWARD", ""), v.get("EDGE_STATE", "")
    return {"forward": forward_raw == "on",
            "edge_state": "users" if (forward_raw == "on" or state_raw == "users") else "econ",
            "forward_raw": forward_raw, "edge_state_raw": state_raw,
            "econ_d1_ids": [ids["CATALOG"], ids["CATALOG_CLIMATE"]], "users_d1_id": ids["USERS"],
            "account_id": cfg.get("account_id") or cfg.get("limits", {}).get("account_id", "")}


def get_json(url: str, timeout: int = 60) -> tuple[int, object]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:  # noqa: BLE001 - reported as a failure with its type
        return 0, f"{type(e).__name__}: {e}"


def check_up(edge: str, now: dt.datetime) -> list[str]:
    bad = []
    code, _ = get_json(edge + "/v1/sources")
    if code != 200:
        bad.append(f"/v1/sources: HTTP {code}")
    code, body = get_json(edge + "/v1/last-updates")
    if code != 200 or not isinstance(body, dict) or not isinstance(body.get("datasets"), list):
        return bad + [f"/v1/last-updates: HTTP {code}, no datasets list"]
    stamps = [d["last_updated"] for d in body["datasets"] if isinstance(d, dict) and d.get("last_updated")]
    if not stamps:
        return bad + ["/v1/last-updates: no dataset has a last_updated time"]
    newest = max(dt.datetime.fromisoformat(s.replace("Z", "+00:00")) for s in stamps)
    age = (now - newest).total_seconds() / 3600
    if age > FRESH_HOURS:
        bad.append(f"STALE: the newest last_updated is {newest.isoformat()} ({age:.0f} h old > {FRESH_HOURS} h)")
    return bad


def check_status(edge: str, want: dict) -> list[str]:
    required = want["forward"] or want["edge_state"] != "econ"
    code, body = get_json(edge + "/v1/edge-status")
    if code == 404:
        return (["/v1/edge-status is missing, but this checkout commits "
                 f"forward={want['forward']} edge_state={want['edge_state']}: the deployed edge is older"]
                if required else [])
    if code != 200 or not isinstance(body, dict):
        return [f"/v1/edge-status: HTTP {code} {str(body)[:120] if body else ''}".rstrip()]
    bad = []
    for k in ("forward", "edge_state", "forward_raw", "edge_state_raw"):
        if body.get(k) != want[k]:
            bad.append(f"deployed edge has {k}={body.get(k)!r}, this checkout commits {want[k]!r}"
                       f" (deployed commit {body.get('commit')!r})")
    if required and not body.get("commit"):
        bad.append("the deployed edge carries no commit id (deploy through tools/selfhost/deploy_edge.sh)")
    return bad


def undeployed_worker_changes(edge: str) -> str | None:
    """A WARNING (not a failure: deploys are Ahmed's and follow merges with a lag) when this checkout's
    api/worker differs from the deployed commit's - main has worker changes that are not live (AR-151
    finding 7). None when they match, or when nothing can be compared (reported as such)."""
    import subprocess                                                        # noqa: PLC0415
    code, body = get_json(edge + "/v1/edge-status")
    commit = body.get("commit") if code == 200 and isinstance(body, dict) else None
    if not commit:
        return None
    try:
        r = subprocess.run(["git", "diff", "--quiet", commit, "HEAD", "--", "api/worker"], cwd=ROOT,
                           capture_output=True, timeout=60)
    except Exception as e:  # noqa: BLE001
        return f"could not compare the deployed commit {commit} with this checkout ({type(e).__name__})"
    if r.returncode == 0:
        return None
    if r.returncode == 1:
        return f"main has api/worker changes after the deployed commit {commit[:12]}: they are not live"
    return f"the deployed commit {commit[:12]} is not in this checkout's history (git exit {r.returncode})"


def graphql(token: str, query: str, variables: dict) -> dict:
    req = urllib.request.Request(
        "https://api.cloudflare.com/client/v4/graphql",
        data=json.dumps({"query": query, "variables": variables}).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json", "User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        d = json.load(r)
    if d.get("errors"):
        raise RuntimeError("GraphQL errors: " + json.dumps(d["errors"])[:300])
    accts = ((d.get("data") or {}).get("viewer") or {}).get("accounts") or []
    if not accts:
        raise RuntimeError("GraphQL returned no account (token not authorised for this account?)")
    return accts[0]


def rows_of(acct: dict, field: str, limit: int) -> list[dict]:
    rows = acct.get(field)
    if rows is None:
        raise RuntimeError(f"GraphQL answer has no {field}")
    if len(rows) >= limit:
        raise RuntimeError(f"{field} returned {len(rows)} rows = the limit; the answer may be truncated")
    return rows


def window(t0: dt.datetime, now: dt.datetime) -> tuple[str, str]:
    start = max(t0, now - dt.timedelta(days=WINDOW_DAYS))
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return start.strftime(fmt), now.strftime(fmt)


def check_writes(token: str, acct_id: str, t0: dt.datetime, now: dt.datetime, want: dict) -> list[str]:
    """Every write to the retired cloud copy since max(T0, now - WINDOW_DAYS). Raises when blind."""
    start, end = window(t0, now)
    v = {"acct": acct_id, "start": start, "end": end}
    buckets = [BUCKET, *CONTROL_BUCKETS]
    r2 = graphql(token, """
query($acct: String!, $start: Time!, $end: Time!, $buckets: [string!]) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2OperationsAdaptiveGroups(limit: 5000, filter: {datetime_geq: $start, datetime_leq: $end, bucketName_in: $buckets}) {
      dimensions { date actionType bucketName } sum { requests } } } } }""", {**v, "buckets": buckets})
    bad, control = [], {b: 0 for b in CONTROL_BUCKETS}
    for r in rows_of(r2, "r2OperationsAdaptiveGroups", 5000):
        d = r["dimensions"]
        b = d.get("bucketName")
        if b not in buckets:                       # the filter must have held; if not, the check is void
            raise RuntimeError(f"R2 analytics returned bucket {b!r} outside the filter")
        if b in control:
            control[b] += r["sum"]["requests"]
        elif d["actionType"] not in R2_READ_ACTIONS and r["sum"]["requests"] > 0:
            bad.append(f"R2 {BUCKET} {d['date']}: {r['sum']['requests']:,} x {d['actionType']}")
    if not any(control.values()):
        raise RuntimeError(f"no R2 activity at all for the control buckets {list(CONTROL_BUCKETS)} "
                           f"in {start}..{end}: the analytics answer cannot be trusted")
    ids = [*want["econ_d1_ids"], want["users_d1_id"]]
    d1 = graphql(token, """
query($acct: String!, $start: Time!, $end: Time!, $ids: [string!]) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1AnalyticsAdaptiveGroups(limit: 5000, filter: {datetime_geq: $start, datetime_leq: $end, databaseId_in: $ids}) {
      dimensions { date databaseId } sum { rowsWritten writeQueries readQueries } } } } }""", {**v, "ids": ids})
    users_activity = 0
    for r in rows_of(d1, "d1AnalyticsAdaptiveGroups", 5000):
        d, s = r["dimensions"], r["sum"]
        if d.get("databaseId") not in ids:
            raise RuntimeError(f"D1 analytics returned database {d.get('databaseId')!r} outside the filter")
        if d["databaseId"] == want["users_d1_id"]:
            users_activity += s["readQueries"] + s["writeQueries"]
        elif s["rowsWritten"] > 0 or s["writeQueries"] > 0:
            bad.append(f"D1 {d['databaseId']} {d['date']}: {s['rowsWritten']:,} rows written, "
                       f"{s['writeQueries']:,} write queries")
    if not users_activity:
        raise RuntimeError(f"no D1 activity at all for the users database (control) in {start}..{end}: "
                           "the analytics answer cannot be trusted")
    return bad


def parse_t0(s: str) -> dt.datetime:
    """An ISO UTC moment, e.g. 2026-10-05T14:00:00Z. A bare date is refused: T0 is a moment, and a date
    either fails on the writes made before the freeze that day or leaves the rest of the day unwatched."""
    if "T" not in s or not s.endswith("Z"):
        raise ValueError(f"T0 {s!r} must be an ISO UTC moment like 2026-10-05T14:00:00Z")
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def send_alert(subject: str, body: str) -> None:
    key = os.environ.get("RESEND_API_KEY", "").strip()
    if not key:
        print("RESEND_API_KEY not set - email skipped; the red workflow is the delivery path")
        return
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=json.dumps({"from": "Econ Data Library <noreply@hfdatalibrary.com>",
                         "to": [os.environ.get("DIGEST_TO") or "admin@hfdatalibrary.com"],
                         "subject": subject, "text": body}).encode(),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json", "User-Agent": UA},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            print(f"alert email sent: HTTP {r.status}")
    except Exception as e:  # noqa: BLE001 - the red workflow is the second delivery path
        print(f"alert email failed ({type(e).__name__}) - relying on the red workflow")


def main(argv: list[str] | None = None, now: dt.datetime | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--edge", default=os.environ.get("SELFHOST_EDGE") or EDGE)
    ap.add_argument("--no-writes-since", default=os.environ.get("SELFHOST_NO_WRITES_SINCE", ""),
                    help="T0 as an ISO UTC moment; unset = check 3 is off (before T0 the cloud copy is written)")
    a = ap.parse_args(argv)
    now = now or dt.datetime.now(dt.timezone.utc)
    want = committed_config()
    failures: list[str] = []
    failures += [f"UP: {m}" for m in check_up(a.edge, now)]
    failures += [f"STATUS: {m}" for m in check_status(a.edge, want)]
    if a.no_writes_since:
        try:
            t0 = parse_t0(a.no_writes_since)
        except ValueError as e:
            failures.append(f"WRITES: {e}")
        else:
            token = os.environ.get("CF_ANALYTICS_TOKEN", "").strip()
            acct = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "").strip() or want["account_id"]
            if not token or not acct:
                failures.append("WRITES: BLIND - CF_ANALYTICS_TOKEN or the account id is not set")
            else:
                try:
                    failures += [f"WRITES: {m}" for m in check_writes(token, acct, t0, now, want)]
                except Exception as e:  # noqa: BLE001 - a check that could not measure is a failure
                    failures.append(f"WRITES: BLIND - {type(e).__name__}: {str(e)[:300]}")
    print(f"edge {a.edge}; committed forward={want['forward']} edge_state={want['edge_state']}; "
          f"writes check {'from ' + a.no_writes_since if a.no_writes_since else 'off'}")
    warn = undeployed_worker_changes(a.edge)
    if warn:
        print("WARN " + warn)
    if failures:
        body = "\n".join(failures)
        print("FAIL\n" + body)
        send_alert(f"[econ self-host] {len(failures)} check(s) failed", body)
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
