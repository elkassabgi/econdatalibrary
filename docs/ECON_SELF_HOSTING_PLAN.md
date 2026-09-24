# Econ self-hosting plan (draft 14, 2026-09-24) - answers reviews R1158, R1160, R1161, R1163-R1167, R1169, R1171,
# R1172, R1174, R1176-R1193, AR-151-AR-153

Build status (branch feat/econ-selfhost-origin):
- Code change 2 (local mode + wrangler.origin.toml, effc6784f) and code change 3 (blob store, sidecar, R2
  import tool, LocalBucket adapter, d18698a2d, f4b338dbc) are built. They were verified locally: NUMBERS
  rows 1103 and 1105 show a real series served byte-identical to its R2 object.
- Code change 1 (the forwarding edge) is built: e2b2855e1, then 472611585 (page views and cost-guard status
  on USERS). Three reviews failed:
  - R1169: the secret could be sent to a host the client chose;
  - R1171: the origin tests did not run from the CI root;
  - R1172: a missing table hid a cost breach.
- All three are fixed in a897ecd11 and 8f4ba04c4, and a verification review is running.
- Built since, each reviewed (R1174 FAIL -> fixed; AR-151 PASS-WITH-CHANGES -> its required changes in
  7e950c02b): EDGE_STATE and /v1/edge-status; the report cache; the off-machine check and its workflow
  (change 6); tools/selfhost/deploy_edge.sh; core/cutover.py (4b); the R2 write guard on every client
  in the repo, with a ratchet (5); core/d1_remote.py with a ratchet (4c, 5); core/catalog_path.py, the
  one resolver and the single-writer lock, with a ratchet of files still to move (4a; 137 today - see
  below); SelfhostBlob
  (AQUEDUCT_BACKEND=selfhost, 4); the updater's post-T0 preflight (4); tools/selfhost/cutover_hook.py
  (5, written and tested, NOT installed - Ahmed approves it first).
- Built after that: core/licence_targets.py with retire_source.py and delist_source_rows.py on it
  (licence enforcement has its local backend before anything else moves); the R1176 fixes (one
  catalogue, every override refused, hot-journal recovery).
- Built on 2026-09-24, each with a parallel review: the R1178 fixes (88384b74b: a behaviour pin that the
  edge-state paths never touch a catalogue binding, a 60 s back-off after a failed source-names refresh,
  more hook spellings, one shared ratchet walk); the blue/green swap, tools/selfhost/swap.py (a089e76be);
  PERSIST-mode journals are not hot and the preflight checks what econdl itself resolves (9d8d91bcb); the
  T0 readiness check, tools/selfhost/t0_ready.py (48d03d264). The swap ran twice for real on the full
  probe catalogue (13,952,906 series): the copies and checks took 5 min 34 s; since the copies REBUILD
  their search index (R1183) the copy step took 10 min 11 s (an upper bound - measured with the whole test
  suite running alongside; NUMBERS). The writer lock is held only while the catalogue is READ (R1185).
- Step 1 has started (ab04a41e0): core/d1_remote.py gained the wrangler roads (run_json / rows /
  execute_file - wrangler as today before T0; after T0 one plain read over REST with the read-only token,
  writes refused) and six D1 readers moved onto them; the ratchets now read Python through the parser
  (comments, docstrings and bare strings dropped, implicit concatenation joined).
- COUNTS: the file lists in the tests are the numbers to use - tests/catalog_db_legacy.txt (137 on
  2026-09-24; 62 at 2d49d8a40 on branch feat/econ-selfhost-catpath, not yet merged) and LEGACY_REMOTE_D1 in
  tests/test_d1_remote.py (25, then 14 after ab04a41e0, then 13 after b7e768dd2, then 6 after 594410504). The other counts in this plan (144 and
  154 catalogue files, 24 remote-D1 files) were taken earlier with other rules and are superseded.
- THE FRESHNESS PROJECTION (R1186, measured on the production file): E:\...\data\catalog.db holds only
  license, series, series_fts and source. unit_state, source_state and source_data_through - what
  /v1/last-updates and the freshness blocks read - existed only in D1, built by core/sync_state_d1.py
  from state.db and the catalogue. So tools/selfhost/origin_copies.py builds them into the primary copy
  with that same emitter (the same licence gate), from the live state.db, inside the locked read; a copy
  without them fails its check, and t0_ready checks the live state.db has rows (282 unit_state and 256
  source_state on 2026-09-24). Since 01938a010 only state.db is read inside the lock; data_through is
  computed from the primary copy afterwards (the GROUP BY took 1,833 s cold on production - R1191).
- sec_edgar IS A T0 PREREQUISITE (R1191, 01938a010). Its catalogue rows, its source_state row and its
  data_through live only in D1: its CI refresher writes D1 alone (R726), and tools/stamp_source_data_through.py
  stamps data_through from D1. A statistic over the catalogue copy is not a substitute - R737 measured
  MAX(end_date <= today) over the copy giving a forward date that creeps with the calendar. So
  tools/selfhost/t0_ready.py has a check, d1-only-sources: READY is refused while any source in
  sync_state_d1.DATA_THROUGH_FROM_D1 has no entry in sync_state_d1.LOCAL_FRESHNESS_WRITERS (empty today).
  The work: a local sec_edgar refresher that writes the catalogue, state.db and data_through (step 1).
  DESIGN (draft, for review before it is built; mapped from the code on 2026-09-24):
  * NAMES. Catalogue source_id `sec_edgar` is the XBRL company-facts product (17,467 D1 series); its
    registry entry is `sec_edgar_xbrl` (no fetcher: "refreshed_elsewhere" = sec-edgar-daily.yml ->
    tools/refresh_sec_edgar.py --days 4 --apply --d1). Registry entry `sec_edgar` is the 13F/insider
    product (edgar_13f, not catalogued). R275: one id, two products.
  * WHAT THE CI RUN WRITES TODAY: store parquet clean_grouped/sec_edgar/<safe>.parquet and CSV
    series/sec_edgar%3A<id>.csv in R2 (and the desktop mirror); D1 series (span UPDATE / INSERT OR IGNORE
    new) + series_fts for new ids + source_counts + source_state('sec_edgar', edgar_delta, daily,
    ok|partial) + source_data_through (stamp_source_data_through: MAX(end_date <= today) over D1). No
    unit_state.
  DRAFT 1 OF THE REST FAILED REVIEW R1193 (2026-09-24); DRAFT 2 BELOW TAKES ITS CORRECTIONS.
  * ONE STORE, AND IT MUST BE WHOLE FIRST. After T0 the parquet store is LOCAL: under
    AQUEDUCT_BACKEND=selfhost updater.blob reads and writes files under the live checkout
    (blob._r2_routed() is None); only series/ CSVs go to the SelfhostBlob store. Draft 1 read "prior facts"
    from SelfhostBlob, which holds no clean_grouped/ - every company would look new and merge_facts would
    REPLACE its history (R386's XOM case, 20,903 rows -> 274). So: prior facts and the parquet write go
    through the updater.blob path functions (the local store after T0); the CSV through SelfhostBlob.
    Measured 2026-09-24 (R1193): the local store holds 17,467 sec_edgar parquets, newest written 2026-09-05
    - about 19 days behind CI's daily R2 writes. So the refresher is enabled only after step 6b's
    footer_diff for sec_edgar shows 0 behind and 0 R2-only, with its AHEAD queue merged.
  * NEVER "NEW" BY ACCIDENT. A company whose ident the catalogue holds but whose store file is missing, or
    whose read fails for any reason but not-found, is REFUSED, never treated as new. The written row count
    is asserted >= the stored file's (never shrink, R386).
  * THE LOCK. core.catalog_path.writer_lock is taken BEFORE the first store write (a bounded wait, as
    swap._waiting_writer_lock does - the updater holds it for its whole run) and held through the catalogue
    and state.db writes. Any catalogue failure is fatal (today it prints "continuing to D1"; after T0
    there is no D1). The skip rule ("merged facts equal the store") also compares the catalogue span, so a
    run that died after its store write is caught up on the next run (R730).
  * CATALOGUE: span UPDATE and new-row INSERT (+ series_fts for a new id) through
    catalog_path.connect(write=True); series.last_updated set on refreshed rows. No per-id DELETE on
    series_fts for a rename (a full scan of the fts5 table inside the lock): the origin copies rebuild
    series_fts from series (origin_copies._rebuild_fts).
  * THE STATE COLLISION IS REAL TODAY. Read-only on the live state.db (R1193): source_state('sec_edgar') =
    giant_changed_units / quarterly / ok (the 13F product) and unit_state('sec_edgar','_all') = 13F. So
    the fix is the 13F entry's own key (e.g. sec_edgar_13f): a registry change (EXPECTED_SOURCE_COUNT
    unchanged by a rename, but checked in the same commit - R347), which also closes R275; its state rows
    move with it. The XBRL refresher then writes the WHOLE source_state('sec_edgar') row (strategy
    included), and no 13F unit shows under sec_edgar in /v1/last-updates or metadata.ts's last_updated.
    Built on branch feat/econ-13f-own-key: updater/state_migrations.py moves the rows on every StateStore
    open until source_state('sec_edgar') carries another strategy, then never again; upsert_source
    refuses a sec_edgar row without the XBRL product's own strategy, so the refresher's FIRST write must
    be that row (R1201).
  * THE 13F D1 CLEAN-UP (R1201 finding 5, corrected by R1202). The local move does not heal D1:
    sync_state_d1 upserts and never deletes, so unit_state('sec_edgar','_all') stays in D1 until removed,
    and any run of OLD code re-creates it locally and the next sync re-upserts it.
    ONE key only: DELETE FROM unit_state WHERE source_id='sec_edgar' AND unit_id='_all' AND
    strategy='giant_changed_units' - a PK delete, costed first as the same statement's SELECT.
    NOT source_state('sec_edgar'): in D1 that row has two owners. refresh_sec_edgar's stamp_freshness_d1
    writes it every day (sec-edgar-daily.yml, 08:00 UTC) and /v1/sources reads it (sql.ts SELECT_SOURCES,
    LEFT JOIN source_state), so deleting it would blank the XBRL product's status, cadence and
    last_updated until the next fully-ok stamp. After the move the sync stops pushing it and the stamp
    overwrites every served column; only `strategy` stays stale, and the worker does not read it.
    It runs only when ALL hold, each read at the time:
      (1) OLD CODE CANNOT RUN (R1205 made this checkable). RENAME = the SQUASH commit of the rename PR ON
          main (the repo squash-merges, so the branch commit is never an ancestor of main). Then, for each
          of updater-daily.yml and updater-heavy.yml:
              git fetch origin
              gh run list --workflow <file> --limit 200 --json databaseId,headSha,status
          every run whose status is NOT completed - queued, in_progress, pending (waiting on the shared
          concurrency group aqueduct-updater), waiting, requested - must have a head SHA with
              git merge-base --is-ancestor RENAME <headSha>
          true. By SHA, not by creation time: workflow_dispatch runs any branch;
      (2) the E: checkout that runs the desktop passes is at or after RENAME (git merge-base --is-ancestor
          RENAME HEAD in that checkout);
      (3) one sync from new code has pushed the sec_edgar_13f rows.
    THE SAME CONDITIONS (1)-(2) GATE THE XBRL WRITER'S FIRST LOCAL RUN, and T0 (R1205 finding 2): old code
    that writes source_state('sec_edgar') after the XBRL product owns the id merges the 13F strategy into
    the XBRL row, and the next open then moves the XBRL row, runs and cursors to sec_edgar_13f. StateStore
    refuses an XBRL write over a still-unmoved 13F row, but it cannot stop old code, which does not have
    that guard; only the conditions can.
    Then re-read the key from D1 twice: right after the delete, and again after the NEXT state sync -
    that is when an old-code writer would show (R1201 rule 4).
    Expected after the rename: /v1/last-updates lists the 13F unit as sec_edgar_13f/_all - the same row
    it listed as sec_edgar/_all before, under its own id.
  * data_through: computed by the origin copy from the catalogue with this source's rule, MAX(end_date)
    over end_date <= today UTC (the stamp tool's rule). It equals D1's stamp while the rows are equal.
    But the clamp hides a forward row instead of failing (R737), and the refresher can write one
    (coverage_span falls back to max(obs_date) when no fact has ended; EDGAR dates filings after 17:30 ET
    to the next business day). So the origin copy's check REFUSES any sec_edgar row with end_date after
    today UTC.
  * THE PROOF, AND WHEN. After sec-edgar-daily.yml is disabled AND no run of it is queued or in progress
    (gh run list), sync sec_edgar's rows D1 -> local and compare ALL columns (tools/
    sync_source_rows_d1_to_local.py --update-differing compares only start/end/title - it is extended),
    listing local-only ids too; D1 cost: one primary-key range read (~17,468 rows_read, R1193). The proof
    writes a receipt; t0_ready's d1-only-sources check reads that receipt and checks that the desktop
    scheduled task exists, not only that LOCAL_FRESHNESS_WRITERS names sec_edgar.
  * THE SCAN WINDOW is a watermark (the last successful scan's date), not a fixed --days 4: a gap longer
    than four days (the 6c owner gate, the machine off) would miss a filer until its next filing.
  * ALSO LOCAL: CI's read-back proof (parquet rows == CSV rows, plus a control company), --respan and
    --audit (both read R2 today), receipts, the gate check, and a time of day away from the updater run and
    the swap.
  * enrich_sec_edgar_tickers.py and the remaining D1 roads of refresh_sec_edgar.py leave
    LEGACY_REMOTE_D1 (a local mode; the D1 half refused after T0).
  * NOT ONLY sec_edgar: R1193 found sec_edgar is the only D1-only FRESHNESS writer, but other sources may
    have catalogue ROWS only in D1 (updater-daily.yml notes "D1 +285 rows vs local"; sec_edgar was 161 of
    them). Step 6b's reconcile is their road; it is measured before T0.
- Also built on 2026-09-24, each answering a parallel review: R1191 (01938a010) - a D1 file that a retry
  re-applies is safe twice (sync_catalog_d1 keeps each id-list DELETE in one file with its INSERTs; a
  file that is not safe twice runs once; migrate_noaa_shard never retries or resumes after a failure),
  nothing escapes as a traceback after the router flip (exit 3), the retry reason is wrangler's [ERROR]
  line. On the catalogue branch: R1189, R1190 and R1192 - the four fetchers that read the catalogue
  refuse an unreadable one (TransientError, never an empty set that skips their id self-check), with
  real hot-journal and run() tests; a ratchet that every resolver writer takes the writer lock, and one
  against a plain sqlite3.connect of a resolver path.
- Still to do in step 1: the local sec_edgar refresher (above); move the remaining catalogue callers and remote-D1 callers onto the
  chokepoints (the ratchets list them and may only shrink; the catalogue callers go through
  core.catalog_path.connect / connect_path, which keeps a tool's --db argument before T0); the direct put_series_csv writers onto
  the CSV store (updater.blob.csv_store(bucket): SelfhostBlob once the cutover flag is set or
  AQUEDUCT_BACKEND=selfhost, R2Blob otherwise, never LocalBlob - R1200; writes by
  updater.derive._put_with_retry -> put_atomic, which gzips a series CSV through
  series_csv_put_args in both stores). Progress is the shrink-only list
  tests/test_object_writers_ratchet.py LEGACY_OBJECT_WRITERS (added with batch 2): 24 left after
  batches 1 (statcan, csv_bulk) and 2 (pxweb_flowgrain, the four IMF table tools, usda, census,
  ilostat, istat); batch 3 moved unsdg, noaa_missing, insee_melodi, ons_uk and bea (19 left, plus the
  three that write through core.derive_csv's PUT helpers - derive_one, derive_eia_tables,
  derive_usda_bulk - which the ratchet now sees and which move with core/derive_csv.py).
  ENCODING IS KEPT (R1206): the 12 moved tools that always stored their CSVs plain keep doing so
  (put_atomic(plain=True)). Gzip is not neutral for them - the worker refuses a filter on a gzipped object
  above its decompression-ratio limit (4 flow-grain CSVs measured at 45-66x) and serves it without the
  citation in its body. Whether to gzip them is a separate decision, with its own measurement.
  THE BLOB STORE NEEDS THE CATALOGUE'S SINGLE-WRITER RULE (R1203 finding 2): after T0 SelfhostBlob writes
  the served store with no lock and from any checkout, so a worktree's derive can publish CSVs built from
  the worktree's parquets. After catpath merges into origin: a SelfhostBlob write requires the writer lock
  (core.catalog_path) and the live checkout, as a catalogue write does.
  delist_timeless_tables.py onto licence_targets (purge_unpermitted_r2.py stays defused
  until re-armed, then on licence_targets); a self-hosted path for tools/run_local_heavy.ps1 and
  make_servable.py (after T0 both fail closed today: the heavy run asks for --pull-state and the R2
  backend, make_servable forces R2); the verifier re-pointing (6d).
- Changes 4 and 5 follow the rules in section 3.4a-c (R1167 A-C).

Measured 2026-09-24T01:33:51Z: the live worker's Cache API works on workers.dev (CF-Cache-Status HIT,
Age 10766 on /v1/stats). The docs name only custom domains, so this was checked.

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
            itself: OPTIONS; /v1/public-stats (USERS + the origin's /v1/sources, with a 30-day last-good
                    copy at the edge; with neither, it answers without a top-sources list, never 500);
                    /v1/pv and /v1/pv/report on table `econ_pageview` IN USERS (same columns, same upsert,
                    same report; the table is created on first use if the migration was missed); the */30
                    cost guard with its status in USERS table `econ_ops_status` (48 upserts/day; no KV
                    namespace to create). Both move when EDGE_STATE = "users" (step 5) or FORWARD = "on".
                    A failed status write never replaces the guard's verdict (R1172).
                    Rate limit on /v1/pv: NOT built. The rate-limit binding needs wrangler >= 4.36 (the
                    worker pins 3.114.17), and its docs page gives no price. /v1/pv today writes econ D1
                    with no limit, so the move keeps today's exposure but puts it on the family login DB.
                    Decision open (section 8).
            forwarding rules (R1169): only the origin's own routes are forwarded, anything else is the
                    edge's 404; the upstream URL is ORIGIN_URL with the client's path SET on it and must
                    keep ORIGIN_URL's origin; redirects are never followed; any 3xx, or any answer without
                    the origin's x-econ-origin mark (a tunnel error page, an Access login page), is a 502
                    that is never cached or logged; an allowlist of request headers; a 30 s wait for
                    response headers (504); edge cache per route (catalog and stats 6 h as today, sources
                    and last-updates 5 min, metadata 1 h, bundle and data never), keyed without api_key
            data requests: licence gate (451 before auth, as today) -> auth (USERS) -> rate limit ->
                    strip X-API-Key / Authorization / ?api_key= -> overwrite identity + secret headers ->
                    forward -> download log: content-length when present; a 200 WITHOUT a length is
                    counted through a stream whatever the origin's x-econ-count says (a hop can drop the
                    length, R1169 M2), and the row is written on completion AND on abort (pipe promise;
                    waitUntil runs up to 30 s after a disconnect), so no download goes unlogged
                    (R593/R599); gzip passthrough bodies carry a length and are never read
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
                backup API ONLY (no plain file copy, even under the lock: a writer killed mid-transaction
                leaves a hot journal that makes any plain copy torn - R1176; the lock's holder rolls it
                back first), SPLIT as the D1 sync splits (tools/selfhost/origin_copies.py: the shard
                sources' series, series_fts and source_counts rows only in the climate copy, their
                source/license parents in both; source_counts recomputed from series - measured
                2026-09-24: the probe's primary copy held all 13,952,906 series incl. 3,138,159 noaa and
                its climate copy was empty, so noaa series were unreachable through the worker's
                routing), then PRAGMA quick_check, total = primary + climate, and
                COUNT(series_fts) = COUNT(series) per file, before the flip; noaa only in CLIMATE; the
                service account is read-only on the build and the store
              - SERIES_BUCKET: adapter over the blob sidecar (content-addressed files + SQLite index
                key -> hash, R2 etag, size, content-encoding, custom metadata; get / range with full size /
                onlyIf returning a bodyless object; stored gzip sent WITHOUT Content-Encoding on the hop)
            swap = build N+1, start the idle instance on its copy, health-check, flip the router, stop
            the old one (a Windows service stop must kill its workerd children - tested in the soak).
            Built as tools/selfhost/swap.py. Each swap is a GENERATION folder: the catalogue copies, a
            FROZEN worker (git archive of the committed HEAD plus a copy of node_modules - wrangler dev
            hot-reloads whatever folder it runs from, so it never runs from a checkout; R1180) and the
            log. One swap at a time (a lock); the D1 slot files are discovered (d1_slots.mjs); after T0
            only THE build is copied, under the writer lock (waited for, bounded); every answer the
            health check accepts carries the instance id the swap gave that process (another process
            on the port is refused) and the metadata TITLE stored in the copy (the id is only echoed);
            the router flipped must be the router of the state file; the old instance is stopped with
            its process tree after two 0-in-flight readings, and only if its pid still belongs to the
            process that was started; a failed check leaves the router alone
          writers: local, one lock (core.catalog_path.writer_lock), chokepoints in section 3.5. The lock
            binds only code that goes through core.catalog_path: the legacy openers still in
            tests/catalog_db_legacy.txt do not take it, so tools/selfhost/t0_ready.py refuses T0 while
            that list is not empty
```

## 3. Code changes

1. Edge worker: today's worker + FORWARD (committed in wrangler.toml) + EDGE_STATE (page views and the
   cost-guard status on USERS, deployable at step 5 with FORWARD still off) + the counting stream + the
   origin's /v1/sources for public-stats; migrations/users_selfhost.sql (additive, IF NOT EXISTS); a CI
   test fails if FORWARD is off after the committed cutover date. The tests run the FORWARD-on edge with
   the CATALOG, CLIMATE and R2 bindings REMOVED from its config, so no path can still depend on them. At
   STEP 7 (decommission, never earlier) it is redeployed without those bindings.
2. `wrangler.origin.toml` (undeployable, as above) + LOCAL mode in the same codebase.
3. SERIES_BUCKET adapter + blob sidecar + local router.
4. Updater: LocalBlob fixed root + gzip at rest; local state with a single-writer lock replacing
   --pull/--push-state and ci_writer_gate - active only behind the CUTOVER flag, because CI keeps running
   from main until T0. The catalogue build and the state are the PRODUCTION CHECKOUT'S own files
   (E:\research\econfindatalibrary\data\catalog.db and data\_aqueduct\ - review R1176: a separate build
   path left the updater and ~150 modules that open ROOT/data/catalog.db writing a different file), and
   after the flag updater/run.py refuses a run unless the code, config.ROOT, STATE_DIR, DATA_ROOT,
   REGISTRY, ECONDL_CATALOG and ECONDL_DATA all resolve to that checkout - so a run from any of the 67
   worktrees cannot become a second writer. Only the lock lives outside the checkout;
   4a. ONE CATALOGUE RESOLVER (R1167 A): 144 non-test modules name catalog.db without ECONDL_CATALOG and
       128 of them connect, so every catalogue open goes through one function (core/catalog_path.py),
       with a CI test failing on any other `catalog.db` open. It opens the build with a mode=rw or mode=ro
       URI, which never creates a file (a missing build is an error, never an empty catalogue). Fixed
       paths: the build and the state as above; the lock at E:\econ_live\state\writer.lock (NOT under
       C:\ProgramData\econ, where Users can only read). Taking the lock rolls back a hot journal left by
       a killed writer before anything reads or copies the catalogue (R1176).
   4b. THE FLAG PATH IS A MODULE CONSTANT (R1167 B) with no environment or config override (an override
       would let any process point it at a missing file = "not cut over"). Tests monkeypatch the constant;
       a CI test fails on any environment read in the flag module.
   4c. D1 READ VERSUS WRITE (R1167 C): step 6b reads both D1 databases after the flag. After the flag,
       every D1 access goes over the REST API with a D1 READ-ONLY token, so the server refuses writes;
       d1_remote() also allows only ONE statement that is SELECT or WITH ... SELECT, with no semicolon,
       no RETURNING, no PRAGMA or ATTACH, and no --file - anything else is refused (fail closed).
   4d. LOCAL WRITERS: tools/run_local_heavy.ps1 (line 253 sets AQUEDUCT_BACKEND=r2 today, and it runs
       --pull-state / --push-state) moves to AQUEDUCT_BACKEND=selfhost with neither; the freshness /
       source_counts / data_through writers target the local build. FTS: the writers do NOT keep
       series_fts in step (R1183 measured 31 files that change `series` - new series, retitles, re-keyed
       ids - without touching it; only core.catalog.rebuild_fts callers repair it), and a count check
       cannot see a retitle. So tools/selfhost/origin_copies.py REBUILDS series_fts from `series` in
       every copy it builds: what users search is what `series` holds, whatever the build's own index
       says. The build's index is rebuilt once in step 6b too (the reconcile does not page FTS).
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
     NOT covered by the hook: tools/series_census.py hands R2Blob's credentials - the WRITE key - to
     DuckDB httpfs, which makes its own S3 calls. It only reads today; it moves to local files in step 1,
     and the write key is revoked at T0 in any case.
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
   econ-data, econ-catalog and econ-catalog-climate every day and emails on any write. It also checks that
   the DEPLOYED edge version is the expected one (the edge answers its commit id and FORWARD state on a
   status route) and emails otherwise: old copies of the config (D:\research\econfindatalibrary_OLD, the
   E:\research\econ_wt_* worktrees and every git worktree with api/worker) still say `econdl-api` with
   no FORWARD, and a routine `wrangler deploy` from any of them would silently put back a worker that
   serves the frozen R2/D1 copy (R1165 late finding). At T0 those stale configs are renamed or removed.

## 4. Steps (each reviewed before it runs)

1. Build and test code changes 1-6. The edge with FORWARD off is byte-diffed against today's worker.
2. Bulk copy while the cloud runs: stores via footer_diff.py --all + mirror_sync.py (AHEAD files are a
   merge queue); `_aqueduct/`, `_backup/`, `archive/`; series/ into the blob store. Verification:
   single-part objects by their MD5 etag; multipart objects by recomputing the ETag from the copied bytes
   with 8 MiB and 64 MiB parts, then the MiB interval the part count allows - no match is a copy failure;
   stores also byte-compared with the independent local mirror copy.
3. Measure locally: the route mix, /v1/catalog and search/browse counts under concurrency at the origin
   (the edge caches each distinct URL 6 h, so every NEW query still reaches the origin) against the
   edge's 30 s wait for response headers (ORIGIN_TIMEOUT_MS; the tunnel's own limit is 125 s), including
   the time a large streamed download takes to prime before its headers leave the origin.
4. Soak behind a SECOND workers.dev name (its cron disabled) for several days. Tests: keyed GET via edge
   ok; unkeyed GET to the origin fails; same URL unkeyed via edge -> 401; headers across the tunnel for
   all three download shapes (whole object, range, filtered/inflated); edge CPU on the largest filtered
   answer; a length-less download logs on completion and on abort; a service stop kills the workerd
   children; the origin refuses when its secret is unset; an Access login page and a tunnel error page
   reach the client as 502, not as data.
5. Before T0: apply migrations/users_selfhost.sql to hfdatalibrary-db (two new tables, IF NOT EXISTS, no
   hf table touched - a schema write on hf's production DB, stated and reviewed); deploy the edge with
   FORWARD off and EDGE_STATE = "users", so the production worker stops writing econ-data and econ D1
   (otherwise the 6a freeze proof sees the cron's R2 write every 30 min and every page view in D1); at
   least 10 minutes later, merge the old page-view rows ONCE through a staging table under a marker row
   (the recipe is in the migration header; a second run is refused - R1172).
6. Cutover:
   a. Freeze at T0: `gh workflow disable` updater-daily, updater-heavy and sec-edgar-daily (their
      workflow_dispatch otherwise survives, on every branch that has the file) and prove it with
      `gh workflow list --all`; Ahmed replaces CLOUDFLARE_API_TOKEN with a token that has no D1 Edit and
      no R2 write but keeps what deploy-site.yml needs (Pages Edit, once granted); if billing-guard's d1
      insights needs more than D1 Read, its D1 check moves to the GraphQL analytics under
      CF_ANALYTICS_TOKEN first (any workflow file pushed on any branch can use the repo's token); remove their schedules on main; delete the econ repo's R2_WRITE_* secrets
      except the endpoint/account id billing-guard needs - Ahmed; stop EconGuard and the crawlers; create
      the machine-wide CUTOVER flag ONLY after `python tools/selfhost/t0_ready.py`, run from the
      production checkout, prints READY (legacy lists empty, ratchets pass, the launcher self-hosted, the
      updater preflight passes, the three CI writers disabled, the edge on EDGE_STATE users, the live
      state.db has rows, every D1-only source has a local freshness writer); REVOKE the
      econ R2 write key (not rotate - a new key with no home is a
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
   locally after T0 goes on the edge denylist before any rollback. A rollback turns FORWARD off and KEEPS
   EDGE_STATE = "users": a config from before step 5 would send page views and the */30 cost-guard write
   back to econ D1/R2 and break the freeze (R1174 B3); the soak worker of step 4 keeps it too if it
   outlives T0. Every flip of FORWARD or EDGE_STATE is recorded with its UTC time and the page-view merge
   marker key it used (R1174 B8), so a later merge exports exactly the days after the first flip.
   T0 for the off-machine check is that recorded MOMENT (repository variable SELFHOST_NO_WRITES_SINCE,
   e.g. 2026-10-05T14:00:00Z), not a date.
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
Workers plan and included requests), the tunnel and DNS add nothing; no KV namespace is used; the
rate-limit binding is not used (price not published on its docs page, checked 2026-09-24); USERS
reads/writes by primary key (the moved page-view table, 48 status upserts/day) add nothing at today's volume
(80 pageview rows so far) - a traffic change would be seen by the billing guard. During the fallback period R2 storage continues. Caveats:
Workers VPC is free only in beta; an off-machine backup in R2 would bring ~$12/mo back. New databases cost
local disk only.

## 7. Reliability and security

Windows services at boot (router, workerd pair, blob sidecar, cloudflared, updater schedule); a UPS;
Windows Update restarts in a fixed night window; the off-machine check; nightly versioned backups of
store, catalogue build, blob store and state to F: and off the machine. The SQLite files among them (the
catalogue build, state.db, the blob store's index) are backed up with the SQLite backup API under the
writer lock, never as a plain file copy (a hot journal makes a plain copy torn - R1176); the blob store's
content files are immutable (content-addressed) and are copied as files. The blob store lives at
E:\econ_live\blobs (updater/blob.py SELFHOST_BLOB_ROOT). Services run as a low-privilege
user: read-only on the store, the build and the blob store; write only on its disposable catalogue copies
and its own logs. The swap itself runs as the WRITER (the updater's account), not as that user: after T0 it
takes the writer lock, whose hot-journal recovery opens the build read-write (R1180 finding 7). When the workstation is down the edge answers "temporarily unavailable" and serves its
cached public answers.

The off-machine check (tools/selfhost/watch_edge.py, workflow selfhost-watch.yml), what it can and
cannot do (review R1174):
- It runs only from the default branch, so it starts working when this branch is merged, and it compares
  the deployed edge with MAIN's wrangler.toml. Deploys go through tools/selfhost/deploy_edge.sh, which
  sets the commit id the check reads.
- GitHub disables a public repository's scheduled workflows after 60 days without a commit. If the
  self-hosted updater stops committing to this repository, a monthly commit (or a re-enable) keeps it
  alive.
- Scheduled runs here arrive 4 to 6.5 h late but are not dropped (15 of 15 measured); the write window is
  cumulative, so a late run still sees every write. The alert goes to DIGEST_TO, which is not set, so it
  goes to admin@hfdatalibrary.com (the same as billing-guard).
- It cannot see a deleted database or bucket, a D1 time-travel restore, or an R2 lifecycle expiry; those
  are covered only by the write key being revoked and the D1 token narrowed at T0.

Risks still open, to measure in the soak (step 4): whether an instance started by swap.py survives the
console or scheduled task that started it being closed (it is started with CREATE_NEW_PROCESS_GROUP, not
DETACHED_PROCESS - R1180 could not check); a real drain with a download in flight; a hop that drops content-length on a gzip passthrough
makes the edge inflate and re-compress it (CPU per GB, and the logged bytes are the inflated size);
cache-busting query parameters on browse routes reach the origin, and browse routes have no rate limit
(R1169 m1); /v1/bundle must send its headers within the edge's 30 s wait.

## 8. Decisions and actions for Ahmed

- Off-machine backup location: UCA storage, rotated external drives, or R2 (~$12/mo).
- Static site econdatalibrary.com: stay on Pages ($0) or move to the tunnel.
- Fallback period (proposed 14 days).
- /v1/pv flood protection: keep it as today (no limit; the family login DB takes the writes), or upgrade
  wrangler to 4.x for the rate-limit binding once its price is confirmed.
- His actions: creating C:\ProgramData\econ elevated with its ACL and owner; creating the flag file,
  elevated, at T0; the Workers VPC service or Access
  setup; the edge `wrangler deploy`s (steps 5, 6d, 7); creating an Object-Read-only R2 token for the
  desktop before T0 (today's read-key entries are placeholders);
  approving the user-global deny hook (tools/selfhost/cutover_hook.py; its settings snippet is in its
  header); creating a D1-READ-only API token for core/d1_remote.py (D1_READ_TOKEN) and, before T0,
  proving on a scratch database that the server refuses a write made with it (AR-151 finding 5 - plan
  4c relies on the server refusing, not only on the SQL check); at T0 deleting the econ repo's R2_WRITE_* secrets, REVOKING the
  econ R2 write key and replacing CLOUDFLARE_API_TOKEN with a narrower one; a bucket-scoped token for an
  R2 backup if he chooses R2; approving the 6c diff; the step-7 deletion.
