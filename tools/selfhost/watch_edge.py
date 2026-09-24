"""Off-machine daily check of the self-hosted econ API (docs/ECON_SELF_HOSTING_PLAN.md, code change 6).

Runs in GitHub Actions (.github/workflows/selfhost-watch.yml), never on the workstation it watches: a
check that runs on the machine it watches goes silent exactly when that machine is down.

Three checks, each a FAILURE when it cannot measure (a blind check is louder than a passing one):

  1. UP AND FRESH - the public edge answers /v1/sources and /v1/last-updates with 200 JSON.
  2. THE DEPLOYED EDGE IS THE EXPECTED ONE - /v1/edge-status reports FORWARD and EDGE_STATE; they must
     equal what api/worker/wrangler.toml in this checkout commits. An old copy of the config (a stale
     worktree, the _OLD folder) redeployed by accident would serve the frozen cloud copy while every
     other check stays green (plan section 3, change 6; R1165). Before the status route is deployed the
     check reports "not deployed" and fails only with --require-status.
  3. NO WRITES TO THE RETIRED CLOUD COPY - from T0 (--no-writes-since DATE) until decommission, any R2
     write operation on the econ bucket or any D1 row written to the two econ catalogue databases,
     read from Cloudflare's GraphQL analytics, is a failure. That is the only proof the freeze holds:
     hooks cannot see writes made inside scripts.

Exit 1 on any failure, with an email through Resend when RESEND_API_KEY is set; the red workflow run is
the second delivery path. Needs CF_ANALYTICS_TOKEN (Account Analytics: Read) and CLOUDFLARE_ACCOUNT_ID
for check 3 only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import tomllib
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WRANGLER = os.path.join(ROOT, "api", "worker", "wrangler.toml")
EDGE = "https://econdl-api.elkassabgi.workers.dev"
BUCKET = "econ-data"
UA = "econdatalibrary-selfhost-watch/1.0"
# R2 operations that do not write (class B and listing). Everything else counts as a write.
R2_READ_ACTIONS = frozenset({"GetObject", "HeadObject", "HeadBucket", "ListBuckets", "GetBucket",
                             "ListObjects", "ListObjectsV2", "ListMultipartUploads", "ListParts",
                             "UsageSummary", "GetBucketEncryption", "GetBucketLocation",
                             "GetBucketCors", "GetBucketLifecycleConfiguration"})


def committed_config(path: str = WRANGLER) -> dict:
    """What this checkout says the deployed edge must be: FORWARD, EDGE_STATE and the econ D1 ids.
    account_id sits inside [limits] in today's wrangler.toml (misplaced; plan section 2), so both are read."""
    with open(path, "rb") as fh:
        cfg = tomllib.load(fh)
    v = cfg.get("vars", {})
    ids = {d["binding"]: d["database_id"] for d in cfg.get("d1_databases", [])}
    econ = [ids[b] for b in ("CATALOG", "CATALOG_CLIMATE") if b in ids]
    if len(econ) != 2:
        raise SystemExit(f"{path}: expected CATALOG and CATALOG_CLIMATE bindings, found {sorted(ids)}")
    return {"forward": v.get("FORWARD") == "on",
            "edge_state": "users" if (v.get("FORWARD") == "on" or v.get("EDGE_STATE") == "users") else "econ",
            "econ_d1_ids": econ, "account_id": cfg.get("account_id") or cfg.get("limits", {}).get("account_id", "")}


def get_json(url: str, timeout: int = 60) -> tuple[int, object]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:  # noqa: BLE001 - reported as a failure with its type
        return 0, f"{type(e).__name__}: {e}"


def check_up(edge: str) -> list[str]:
    bad = []
    for path in ("/v1/sources", "/v1/last-updates"):
        code, body = get_json(edge + path)
        if code != 200 or not isinstance(body, (dict, list)):
            bad.append(f"{path}: HTTP {code} {str(body)[:120] if body else ''}".rstrip())
    return bad


def check_status(edge: str, want: dict, require: bool) -> list[str]:
    code, body = get_json(edge + "/v1/edge-status")
    if code == 404:
        return ["/v1/edge-status is not deployed yet"] if require else []
    if code != 200 or not isinstance(body, dict):
        return [f"/v1/edge-status: HTTP {code} {str(body)[:120] if body else ''}".rstrip()]
    bad = []
    for k in ("forward", "edge_state"):
        if body.get(k) != want[k]:
            bad.append(f"deployed edge has {k}={body.get(k)!r}, this checkout commits {want[k]!r}"
                       f" (deployed commit {body.get('commit')!r})")
    return bad


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


def check_writes(token: str, acct_id: str, since: str, today: str, d1_ids: list[str]) -> list[str]:
    """Every R2 write on the econ bucket and every D1 row written to the econ databases since `since`."""
    v = {"acct": acct_id, "start": since, "end": today}
    r2 = graphql(token, """
query($acct: String!, $start: Date!, $end: Date!, $bucket: String!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2OperationsAdaptiveGroups(limit: 5000, filter: {date_geq: $start, date_leq: $end, bucketName: $bucket}) {
      dimensions { date actionType bucketName } sum { requests } } } } }""", {**v, "bucket": BUCKET})
    bad = []
    for r in rows_of(r2, "r2OperationsAdaptiveGroups", 5000):
        d = r["dimensions"]
        if d.get("bucketName") != BUCKET:          # the filter must have held; if not, the check is void
            raise RuntimeError(f"R2 analytics returned bucket {d.get('bucketName')!r} under a {BUCKET} filter")
        if d["actionType"] not in R2_READ_ACTIONS and r["sum"]["requests"] > 0:
            bad.append(f"R2 {BUCKET} {d['date']}: {r['sum']['requests']:,} x {d['actionType']}")
    d1 = graphql(token, """
query($acct: String!, $start: Date!, $end: Date!, $ids: [string!]) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1AnalyticsAdaptiveGroups(limit: 5000, filter: {date_geq: $start, date_leq: $end, databaseId_in: $ids}) {
      dimensions { date databaseId } sum { rowsWritten } } } } }""", {**v, "ids": d1_ids})
    for r in rows_of(d1, "d1AnalyticsAdaptiveGroups", 5000):
        d = r["dimensions"]
        if d.get("databaseId") not in d1_ids:
            raise RuntimeError(f"D1 analytics returned database {d.get('databaseId')!r} outside the filter")
        if r["sum"]["rowsWritten"] > 0:
            bad.append(f"D1 {d['databaseId']} {d['date']}: {r['sum']['rowsWritten']:,} rows written")
    return bad


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


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--edge", default=os.environ.get("SELFHOST_EDGE") or EDGE)
    ap.add_argument("--require-status", action="store_true",
                    default=os.environ.get("SELFHOST_REQUIRE_STATUS", "") == "1")
    ap.add_argument("--no-writes-since", default=os.environ.get("SELFHOST_NO_WRITES_SINCE", ""),
                    help="YYYY-MM-DD (T0); unset = check 3 is off (before T0 the cloud copy is still written)")
    a = ap.parse_args(argv)
    want = committed_config()
    failures: list[str] = []
    failures += [f"UP: {m}" for m in check_up(a.edge)]
    failures += [f"STATUS: {m}" for m in check_status(a.edge, want, a.require_status)]
    if a.no_writes_since:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", a.no_writes_since):
            failures.append(f"WRITES: --no-writes-since {a.no_writes_since!r} is not YYYY-MM-DD")
        else:
            token = os.environ.get("CF_ANALYTICS_TOKEN", "").strip()
            acct = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "").strip() or want["account_id"]
            if not token or not acct:
                failures.append("WRITES: BLIND - CF_ANALYTICS_TOKEN or CLOUDFLARE_ACCOUNT_ID is not set")
            else:
                today = dt.datetime.now(dt.timezone.utc).date().isoformat()
                try:
                    failures += [f"WRITES: {m}" for m in
                                 check_writes(token, acct, a.no_writes_since, today, want["econ_d1_ids"])]
                except Exception as e:  # noqa: BLE001 - a check that could not measure is a failure
                    failures.append(f"WRITES: BLIND - {type(e).__name__}: {str(e)[:300]}")
    print(f"edge {a.edge}; committed forward={want['forward']} edge_state={want['edge_state']}; "
          f"writes check {'from ' + a.no_writes_since if a.no_writes_since else 'off'}")
    if failures:
        body = "\n".join(failures)
        print("FAIL\n" + body)
        send_alert(f"[econ self-host] {len(failures)} check(s) failed", body)
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
