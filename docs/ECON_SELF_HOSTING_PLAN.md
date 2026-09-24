# Econ self-hosting plan (draft 9, 2026-09-24) - answers reviews R1158, R1160, R1161, R1163-R1167

Build status: code change 2 (local mode + wrangler.origin.toml, effc6784f) and code change 3 (blob store,
sidecar, R2 import tool, LocalBucket adapter, d18698a2d) are on branch feat/econ-selfhost-origin, verified
locally (NUMBERS rows 1103, 1105: a real series served byte-identical to its R2 object). Changes 1, 3 and 6 may be built now; changes 4 and 5
follow the rules in section 3.4a-c (R1167 A-C).

Owner decision (Ahmed, 2026-09-23 ~22:00Z): host ALL of econ on his UCA workstation; UCA approved and
offered funding; minimise cost because more databases are coming. hf, ip and the portal are out of scope
(the portal's page-view beacon calls econ's /v1/pv and is handled at the edge).

## 1. Facts (measured; NUMBERS.md 1088-1101; reviews R1158-R1164)

- API host everywhere: `econdl-api.elkassabgi.workers.dev` (origin/main 8fcca2e08: 379 occurrences in
  356 files, R1165). It stays on an edge Worker for good.
- Worker `econdl-api`: D1 CATALOG, CATALOG_CLIMATE (noaa), USERS (hfdatalibrary-db: users, sessions,
  rate_limits, econ_download_log), R2 SERIES_BUCKET; */30 cron = the ACCOUNT-WIDE cost guard (needs a
  status sink + CF_ANALYTICS_TOKEN + CF_ACCOUNT_ID; nothing reads cost_status.json). /v1/public-stats
  reads USERS and CATALOG `source`. /v1/pv (the portal's beacon, portal/index.html:508; it needs only a
  1x1 GIF back) writes CATALOG `pageview`; /v1/pv/report reads it with GROUP BY (pageview.ts:84-94).
  JSON and CSV answers carry `public, max-age=300`; the worker also uses the Cache API (s-maxage 21600).
- R2 econ-data 798 GB / 14,047,709 objects: stores 341.56 GB (1,376 multipart), `_aqueduct/` 9.12,
  `_backup/` 0.79, `archive/` 1.12, series/ ~445 GB. Local parquet 345.6 GB. Free disk E: 2.95 TB, F:
  9.89 TB. Multipart ETags are MD5-of-part-MD5s (18 of 18 local files reproduced); three uploaders use
  64 MiB parts (statcan matches only at 64 MiB), the rest boto3's 8 MiB.
- catalog.db is in rollback-journal mode (header bytes 18/19 = 1/1).
- Writers: CI (updater-daily, updater-heavy, sec-edgar-daily; billing-guard only reads); the
  workstation's EconGuard loop, crawlers and run_local_heavy.ps1 (AQUEDUCT_BACKEND=r2); the worker's cron
  (R2) and /v1/pv (D1); 40 R2-writing modules (32 PUT, 8 DELETE/COPY only - incl. purge_unpermitted_r2,
  retire_source, delist_source_rows, delist_timeless_tables); at least 7 of them PUT through a bare
  `r2_util.client()`, which on the desktop prefers the R2_READ_* key (r2_util.py:97-101); 4 call
  boto3.client directly; 24 files call `wrangler d1 ... --remote`, and core/load_d1_rest.py writes D1 over
  the REST API. The desktop's wrangler uses OAuth (shared with hf/ip/portal); D1 tokens cannot be scoped
  to one database. The econ R2 write key is distinct from hf's. No other worker binds econ-catalog or
  econ-data; no scheduled task runs econ work; the only R2 lifecycle rule is the default multipart abort.
- Local probe: the unchanged worker in Miniflare (wrangler 3.114.17 accepts -c and --persist-to) answers
  catalogue routes in 9-24 ms; one LIKE count over 13,952,906 rows 11.0 s; workerd holds a D1 file only
  while busy (+~2 s), ~81-88 MB RAM per process.
- Traffic: ~66.4k econ requests/day.

## 2. Design

```
client -> econdl-api.elkassabgi.workers.dev   EDGE worker (same name, forever)
            itself: OPTIONS; /v1/public-stats (USERS + an edge-cached sources.json from the origin, stale
                    fallback when the origin is down); /v1/pv and /v1/pv/report on a `pageview` table
                    MOVED TO USERS (same upsert, same GROUP BY report) - both unauthenticated, so /v1/pv is
                    rate-limited with the Workers rate-limit binding (keys: a constant per-location cap
                    plus per-IP; declared for the pinned wrangler 3.114.17 as [[unsafe.bindings]]
                    type="ratelimit"; the code REFUSES the write when the binding is missing) and
                    /v1/pv/report is edge-cached. The binding counts per Cloudflare location and is
                    eventually consistent (docs), so it REDUCES a flood's load on the family login DB; it
                    does not stop a flood spread over many locations. The */30 cost guard with its status
                    in a KV key (48 writes/day)
            data requests: licence gate (451 before auth, as today) -> auth (USERS) -> rate limit ->
                    strip X-API-Key / Authorization / ?api_key= -> overwrite identity + secret headers ->
                    forward -> download log: content-length when present; when the ORIGIN SETS
                    `x-econ-count: 1` (answers it builds without a length) the edge pipes through a counting
                    stream and writes the row on completion AND on abort (pipe promise; waitUntil runs up to
                    30 s after a disconnect), so no download goes unlogged (R593/R599); gzip passthrough
                    bodies are never read
          PRIVATE ORIGIN: Workers VPC binding to the tunnel (beta, free); fallback Access service token +
            Cache Rule bypass on the tunnel hostname. The origin sends private, no-store on EVERY answer.
          -> workstation, 127.0.0.1 only:
            local router -> blue/green pair of workerd/Miniflare instances, pinned wrangler version, own
            config `api/worker/wrangler.origin.toml` - name not `econdl-api`, FORWARD absent, LOCAL=1,
            TOP-LEVEL workers_dev = false (the existing wrangler.toml wrongly puts workers_dev and
            account_id inside [limits], where they have no effect - fixed too), no routes, placeholder
            database ids, preview_urls = false (wrangler 3.114.17 defaults it to true whatever
            workers_dev says); a tomllib CI test checks those top-level keys in both files
              - LOCAL MODE: refuses EVERY request when the secret is unset; requires the secret
                (constant-time); skips requireDownloadAuth + logDownload; keeps licence gate + denylist;
                sets x-econ-count on length-less answers; no-store everywhere; the Cache API is off (or a
                fresh persist dir per swap), so a swap is never hidden for 6 h
              - CATALOG / CATALOG_CLIMATE: per-swap DISPOSABLE COPIES of the build, made with the SQLite
                backup API (or under the single-writer lock - a plain copy of a rollback-journal file
                during a write can be torn), then PRAGMA quick_check, total = primary + climate, and
                COUNT(series_fts) = COUNT(series) per file, before the flip; noaa only in CLIMATE; the
                service account is read-only on the build and the store
              - SERIES_BUCKET: adapter over the blob sidecar (content-addressed files + SQLite index
                key -> hash, R2 etag, size, content-encoding, custom metadata; get / range with full size /
                onlyIf returning a bodyless object; stored gzip sent WITHOUT Content-Encoding on the hop)
            swap = build N+1, start the idle instance on its copy, health-check, flip the router, stop
            the old one (a Windows service stop must kill its workerd children - tested in the soak)
          writers: local, one lock, chokepoints in section 3.5
```

## 3. Code changes

1. Edge worker: today's worker + FORWARD (committed in wrangler.toml) + /v1/pv and /v1/pv/report on USERS
   + cost guard to KV + the counting stream on `x-econ-count` + sources.json for public-stats; a CI test
   fails if FORWARD is off after the committed cutover date. At STEP 7 (decommission, never earlier) it is
   redeployed without CATALOG, CLIMATE and R2 bindings.
2. `wrangler.origin.toml` (undeployable, as above) + LOCAL mode in the same codebase.
3. SERIES_BUCKET adapter + blob sidecar + local router.
4. Updater: LocalBlob fixed root + gzip at rest; local state with a single-writer lock replacing
   --pull/--push-state and ci_writer_gate - active only behind the CUTOVER flag, because CI keeps running
   from main until T0. The lock, the state and the catalogue build live at FIXED MACHINE-WIDE paths
   (not under the checkout: STATE_DIR follows AQUEDUCT_STATE_DIR and 159 modules open
   ROOT/data/catalog.db), and after the flag any write whose resolved catalogue, store or state dir is
   not the build is refused - so a run from any of the 67 worktrees cannot become a second writer;
   4a. ONE CATALOGUE RESOLVER (R1167 A): 144 non-test modules name catalog.db without ECONDL_CATALOG and
       128 of them connect, so every catalogue open goes through one function (core/catalog_path.py),
       with a CI test failing on any other `catalog.db` open. It opens the build with a mode=rw or mode=ro
       URI, which never creates a file (a missing build is an error, never an empty catalogue). Fixed
       paths: the build at E:\econ_live\catalog\catalog.db, state at E:\econ_live\state\, the lock at
       E:\econ_live\state\writer.lock (NOT under C:\ProgramData\econ, where Users can only read).
   4b. THE FLAG PATH IS A MODULE CONSTANT (R1167 B) with no environment or config override (an override
       would let any process point it at a missing file = "not cut over"). Tests monkeypatch the constant;
       a CI test fails on any environment read in the flag module.
   4c. D1 READ VERSUS WRITE (R1167 C): step 6b reads both D1 databases after the flag. After the flag,
       every D1 access goes over the REST API with a D1 READ-ONLY token, so the server refuses writes;
       d1_remote() also allows only ONE statement that is SELECT or WITH ... SELECT, with no semicolon,
       no RETURNING, no PRAGMA or ATTACH, and no --file - anything else is refused (fail closed). run_local_heavy.ps1:253; the freshness / source_counts / data_through writers
   target the local build; FTS rebuilt locally from `series`.
5. Chokepoints on the OPERATION, not on a flag a caller may forget:
   - The flag is ONE machine-wide file (`C:\ProgramData\econ\CUTOVER`), not per checkout. It is checked
     with os.stat (never os.path.exists, which turns a PermissionError into "absent"):
     FileNotFoundError or NotADirectoryError = NOT cut over (so the ubuntu CI runners, where the path
     never exists, keep writing until T0); any other exception = cut over (fail closed). Tested for all
     three outcomes on ubuntu and Windows, with an injected temp path - tests never create the real
     folder. Ahmed creates `C:\ProgramData\econ` ELEVATED with an explicit ACL (SYSTEM and Administrators
     full, Users read only, no CREATOR OWNER) and its OWNER set to BUILTIN\Administrators (an owner can
     always change permissions, and the owner of files an elevated admin makes is not predictable on this
     machine) - or a High integrity label; if the folder already exists it is re-owned or re-created. A
     non-elevated delete AND a non-elevated `icacls` grant are both shown to fail. The flag file itself is
     created by Ahmed, elevated, at T0 (Users can only read the folder).
   - R2: every S3 client r2_util builds gets a botocore before-call hook that ALLOWS ONLY reads
     (GetObject, HeadObject, ListObjects, ListObjectsV2, HeadBucket) and refuses every other operation
     (PutObject, CopyObject, UploadPart, UploadPartCopy, DeleteObject(s), multipart, bucket lifecycle,
     DeleteBucket, ...) once the flag is set - measured: the hook sees s3transfer's multipart calls too.
     So the bare-client() writers
     (refresh_sec_edgar, probe_csv_freshness, sec_edgar_union_repair, _upload_clean_full_parquet,
     upload_statcan_store, _upload_biotrademerch_store and any other) are refused by the hook, not by
     their callers; the 4 direct boto3.client users are routed through r2_util.
   - THE DESKTOP HAS NO REAL READ KEY (NUMBERS row 1104): the R2_READ_* entries in the E: .env are
     placeholders, so every desktop R2 read uses the WRITE key today. Before T0 Ahmed creates an
     Object-Read-only R2 token scoped to econ-data; it goes into R2_READ_*, and step 6b's final sync uses
     it - revoking the write key at T0 would otherwise stop every desktop R2 read too.
   - D1: a shared `d1_remote()` helper refuses the same way; the 24 `wrangler d1 ... --remote` callers,
     core/load_d1_rest.py and core/load_d1_chunked.py move into it; a CI test fails on any `--remote` or
     `/d1/database/` call outside it.
   - Hand and agent writes: a PreToolUse deny in the USER-GLOBAL ~/.claude/settings.json (hooks are per
     project; the R251 ban was once hf-only). Its flag-path rule (refuse ANY command that names the flag
     path) is active from install; its write rules are active once the flag exists, matching the econ
     binding names, the database NAMES (econ-catalog, econ-catalog-climate) and ids, the bucket,
     `wrangler r2 object put/delete`, `wrangler d1 execute/migrations apply ... --remote` and the D1 REST
     path. The hook script and settings are editable by the same user, so it stops mistakes, not intent;
     it cannot see writes made inside scripts; the daily analytics check (code change 6) is the proof.
     The settings change is shown to Ahmed before it is made.
   - The VERIFIERS move in step 6d, with the served store: ledger_check.py (its D1 and head_object
     checks), tools/audit_d1_*, audit_r2_vs_catalog, audit_serving_coherence, verify_source_served,
     probe_csv_freshness and every other tool that reads D1/R2 to prove "served" (the full list is
     enumerated by grep in step 1 and attached to the step-6d change) are re-pointed to the EDGE (the
     address users get), or refuse after the flag; CLAUDE.md, DESKTOP_FIRST.md, the econ-updater skill
     and the runbooks change in the same 6d commit.
   - The licence tools (purge_unpermitted_r2, retire_source, delist_source_rows, delist_timeless_tables)
     get their local backend FIRST, so licence enforcement never has a gap.
6. Health gate and digest local; an off-machine scheduled GitHub Action checks freshness/uptime through the
   edge AND, from T0 until step 7, reads R2 and D1 write analytics (billing-guard's analytics token) for
   econ-data, econ-catalog and econ-catalog-climate every day and emails on any write.

## 4. Steps (each reviewed before it runs)

1. Build and test code changes 1-6. The edge with FORWARD off is byte-diffed against today's worker.
2. Bulk copy while the cloud runs: stores via footer_diff.py --all + mirror_sync.py (AHEAD files are a
   merge queue); `_aqueduct/`, `_backup/`, `archive/`; series/ into the blob store. Verification:
   single-part objects by their MD5 etag; multipart objects by recomputing the ETag from the copied bytes
   with 8 MiB and 64 MiB parts, then the MiB interval the part count allows - no match is a copy failure;
   stores also byte-compared with the independent local mirror copy.
3. Measure locally: the route mix, /v1/catalog and search/browse counts under concurrency at the origin
   (the edge no longer caches them) against the 125 s origin timeout.
4. Soak behind a SECOND workers.dev name (its cron disabled) for several days. Tests: keyed GET via edge
   ok; unkeyed GET to the origin fails; same URL unkeyed via edge -> 401; headers across the tunnel for
   all three download shapes (whole object, range, filtered/inflated); edge CPU on the largest filtered
   answer; a length-less download logs on completion and on abort; a service stop kills the workerd
   children; the origin refuses when its secret is unset; /v1/pv answers 429 past its limit and refuses
   when its binding is missing.
5. Before T0: create `pageview` in hfdatalibrary-db with CREATE TABLE (no IF NOT EXISTS - a schema write
   on hf's production DB, stated and reviewed); deploy the edge with FORWARD off but /v1/pv(+report) on
   USERS and the cost guard on KV, so the production worker stops writing econ-data and econ D1; THEN copy
   the old rows with an upsert that adds (hits = hits + excluded.hits).
6. Cutover:
   a. Freeze at T0: `gh workflow disable` updater-daily, updater-heavy and sec-edgar-daily (their
      workflow_dispatch otherwise survives, on every branch that has the file) and prove it with
      `gh workflow list --all`; Ahmed replaces CLOUDFLARE_API_TOKEN with a token that has no D1 Edit and
      no R2 write but keeps what deploy-site.yml needs (Pages Edit, once granted); if billing-guard's d1
      insights needs more than D1 Read, its D1 check moves to the GraphQL analytics under
      CF_ANALYTICS_TOKEN first (any workflow file pushed on any branch can use the repo's token); remove their schedules on main; delete the econ repo's R2_WRITE_* secrets
      except the endpoint/account id billing-guard needs - Ahmed; stop EconGuard and the crawlers; create
      the machine-wide CUTOVER flag; REVOKE the econ R2 write key (not rotate - a new key with no home is a
      live write path) - Ahmed. Prove the freeze with R2 and D1 GraphQL analytics by bucket/database and
      action type (deletes included): zero writes for one hour, and then checked daily until step 7 by the
      off-machine Action of code change 6, which emails on any write - a write fails the fallback and is
      investigated before a rollback could serve it.
   b. Final delta sync (store, series/, state.db). Catalogue reconcile by paged primary-key SELECTs from
      both D1 databases (one page measured first; the d1_cost_guard hook is told the driver and its
      measured total, ~14M rows read, inside the allowance); FTS is rebuilt locally, not paged; the diff
      covers all columns and all tables with the denylist applied, and search queries are compared on
      both sides.
   c. Owner gate: Ahmed sees the diff summarised by source and change class (incl. every change staged
      during the D1 freeze and any licence row change) and approves.
   d. Commit FORWARD on, together with the verifier re-pointing and the docs change; deploy the edge
      (Ahmed); start the local writers.
   Fallback: the frozen R2/D1 copy for at most 14 days. A rollback re-serves T0 data; anything purged
   locally after T0 goes on the edge denylist before any rollback.
7. Decommission (Ahmed confirms): redeploy the edge without CATALOG, CLIMATE and R2 bindings, then delete
   econ R2 objects and econ D1 databases - only after an off-machine, versioned backup and a restore test.
   The write key is revoked and the hook refuses deletes, so the deletion runs from the Cloudflare
   dashboard or with a short-lived token that is revoked afterwards (Ahmed). The backup continues after.

## 5. Adding a database later

fetcher -> local store -> catalogue rows in the local build -> series CSVs into the blob store -> source
counts -> blue/green swap -> site page -> Pages deploy (if the site stays on Pages). SUPPORTED_SOURCES
(util.ts) needs a local origin redeploy (ours); a new GATED item needs the edge denylist too (an edge
deploy, Ahmed). No cloud storage cost.

## 6. Cost

After step 7: R2 econ-data (~$12/mo) and econ's D1 storage go. The edge worker (inside the account's
Workers plan and included requests), KV (free tier), the tunnel and DNS add nothing; the rate-limit
binding's price is NOT yet confirmed (no price line found in the docs - checked in step 1 before it is
used; if it is billed, /v1/pv falls back to a per-IP check on USERS); USERS reads/writes by primary key (incl. the moved pageview table) add nothing at today's volume
(80 pageview rows so far) - a traffic change would be seen by the billing guard. During the fallback period R2 storage continues. Caveats:
Workers VPC is free only in beta; an off-machine backup in R2 would bring ~$12/mo back. New databases cost
local disk only.

## 7. Reliability and security

Windows services at boot (router, workerd pair, blob sidecar, cloudflared, updater schedule); a UPS;
Windows Update restarts in a fixed night window; the off-machine check; nightly versioned backups of
store, catalogue build, blob store and state to F: and off the machine. Services run as a low-privilege
user: read-only on the store, the build and the blob store; write only on its disposable catalogue copies
and its own logs. When the workstation is down the edge answers "temporarily unavailable" and serves its
cached public answers.

## 8. Decisions and actions for Ahmed

- Off-machine backup location: UCA storage, rotated external drives, or R2 (~$12/mo).
- Static site econdatalibrary.com: stay on Pages ($0) or move to the tunnel.
- Fallback period (proposed 14 days).
- His actions: creating C:\ProgramData\econ elevated with its ACL and owner; creating the flag file,
  elevated, at T0; the Workers VPC service or Access
  setup; the edge `wrangler deploy`s (steps 5, 6d, 7); creating an Object-Read-only R2 token for the
  desktop before T0 (today's read-key entries are placeholders);
  approving the user-global deny hook; at T0 deleting the econ repo's R2_WRITE_* secrets, REVOKING the
  econ R2 write key and replacing CLOUDFLARE_API_TOKEN with a narrower one; a bucket-scoped token for an
  R2 backup if he chooses R2; approving the 6c diff; the step-7 deletion.
