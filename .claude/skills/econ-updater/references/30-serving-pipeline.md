# Serving pipeline — exact commands, mined from each tool's own argparse 2026-08-04

> Every command was read from the tool's source, not remembered. If a tool's CLI changes,
> this file is stale — trust the tool's --help and fix this file in the same commit.

# econdatalibrary — exact operational command sequences

All commands run from the repo root `E:\research\econfindatalibrary` unless noted. Every fact below was read from the named file, never from memory.

**Standing environment**
- `AQUEDUCT_BACKEND=r2` for anything that must see the *served* store — the local `data/clean_full/` is only a scratch mirror of the last run under r2 (R296/R36, cited at tools/gen_runbook.py:522-524; CI sets it at .github/workflows/updater-daily.yml:116). The catalog tools set it themselves (tools/catalog_complete.py:18).
- `PYTHONIOENCODING=utf-8` on Windows for anything that wraps wrangler — cp1252 consoles crash on wrangler's emoji/box-drawing exactly on the retry/failure paths (core/sync_state_d1.py:58-72, subprocess decode pinned at core/sync_state_d1.py:226-233).
- R2 credentials come from the gitignored `.env` at repo root (`R2_WRITE_ENDPOINT` / `R2_WRITE_ACCESS_KEY_ID` / `R2_WRITE_SECRET_ACCESS_KEY`, read fallback `R2_READ_*`); real env vars win over `.env`, which is how CI runs headless (core/r2_util.py:18-59). Placeholder values (`...`, `changeme`) are treated as absent (core/r2_util.py:35-41).
- Live API host: `https://econdl-api.elkassabgi.workers.dev`, overridable via `ECONDL_API` (tools/verify_source_served.py:27). D1 database name: `econ-catalog` (core/sync_state_d1.py:53). Bucket: `econ-data`; per-series CSV key layout: `series/<urlencode("<source>:<series_key>")>.csv` — the single definition is `csv_key()` at tools/derive_csv_bulk.py:43-51; never re-derive it by hand.
- Ledger: `D:\research\hfdatalibrary\.claude\MISTAKES.md` (path hardcoded at tools/gen_runbook.py:44). Read entries a runbook page names before changing anything.

---

## Checklist A — build & prove a fetcher for a new source

1. **Record the licence FIRST.** Quote it verbatim in `DATABASE_LICENSES_VERBATIM.md` and create the `license` + `source` rows in `data/catalog.db` before any cataloguing. `tools/catalog_complete.py` *refuses* to insert rows for a source with no licence anywhere ("Record the licence first", tools/catalog_complete.py:39-45), and it *copies* whatever licence it finds — copying a wrong `commercial_ok=1` once relicensed 211,924 FAO series as commercially usable (R117, tools/catalog_complete.py:47-77).

2. **Add the registry entry AND bump the count guard in the same change.** Edit `updater/registry.yaml` (`source_id`, `strategy`, `cadence`, `live: false` until proven, optional `out_dir`, `run_location: local` for cloud-infeasible sources) and bump `EXPECTED_SOURCE_COUNT` at updater/config.py:160. The validator refuses the entire run — not just your source — on any mismatch: `"registry invalid (fix before running)"` (updater/orchestrate.py:614-617; `expected N sources, found M` from updater/registry.py:55-56). Updating the guard is part of adding a source, not an afterthought (R347, updater/config.py:157-159). Validator also rejects: missing/duplicate `source_id`, strategy not in `VALID_STRATEGIES`, missing `cadence`, non-boolean `live` (updater/registry.py:34-57).

3. **Write the fetcher module** at `updater/strategies/fetchers/<source_id>.py` exposing `def update(unit, since) -> Result`. Contract: read existing parquet(s) to learn each series' last obs_date (or use `since`), request only newer observations via the source's native date filter, publish via `merge.merge_and_write` (atomic, dedup, never-shrink), return `Result(status, obs=<rows>, last_obs_date=...)`, raise `TransientError` / `DefinitiveError` per the failure contract (updater/strategies/fetchers/__init__.py:1-13). The fetcher-backed strategies are `extend_by_date, overwrite_if_changed, sdmx_delta, manual_vintage, bulk_snapshot_if_changed` — this list must match `orchestrate._has_adapter` and `health._adapter_ready` exactly; the two once drifted and mislabelled ~35 sources (R10 global, tools/audit_schedule_coverage.py:63-67; updater/health.py:50-59). A fetcher-backed source with **no** module is filed `PENDING — no adapter built` and skipped forever, however it is scheduled (tools/audit_schedule_coverage.py:70-84).

4. **Prove the module imports:**
   ```
   python -c "from updater.strategies.fetchers import implemented; print(implemented('<sid>'))"
   ```
   Proof: prints `True`. A module that exists but crashes on import prints a loud `FAILED TO IMPORT` line and counts as not implemented (updater/strategies/fetchers/__init__.py:30-57).

5. **Dry plan:**
   ```
   python -m updater.run --source <sid> --dry
   ```
   Proof: the registry validates, and the source materialises units. A misspelled/unitless source aborts loudly: `"N requested source(s) have no unit in the registry"` (updater/orchestrate.py:626-633). `--dry` reports what's due and changes nothing (updater/run.py:189-190).

6. **Real local proof — `--force` is mandatory:**
   ```
   AQUEDUCT_BACKEND=r2 python -u -m updater.run --source <sid> --force
   ```
   Without `--force` a not-due source reports "0 unit(s) processed" and the run goes green having exercised no fetcher code — a green light that means nothing (R35 "configured is not running", .github/workflows/updater-daily.yml:221-225; flags at updater/run.py:185-196: `--source/--strategy/--cadence` repeatable, `--force` = ignore cadence + change-detection).

7. **CI proof (same rule):**
   ```
   gh workflow run updater-daily.yml -f source=<sid> -f force=true
   ```
   The `force` input description says it plainly: "REQUIRED to prove a fetcher" (.github/workflows/updater-daily.yml:36-40). `source` accepts a comma/space-separated list; tokens are validated to `[a-z0-9_]` (updater-daily.yml:206-218). A manual dispatch with explicit `source` runs a non-live source despite `AQUEDUCT_LIVE_ONLY=1` — that is the designed pre-go-live delta proof (updater-daily.yml:117-122).

8. **Verify in the DATA, never the exit code** (R1 global ledger — 45/61 "green" once meant 4 ingested):
   ```
   python -c "import sys,sqlite3;sys.path.insert(0,'.');from updater import config;con=sqlite3.connect(f'file:{config.STATE_DB}?mode=ro',uri=True);print(*con.execute(\"SELECT unit_id,status,last_success_utc,obs_count,last_obs_date,last_error FROM unit_state WHERE source_id='<sid>'\"),sep=chr(10))"
   ```
   and count the store (the canonical §4 step-5 probe, tools/gen_runbook.py:437-440):
   ```
   AQUEDUCT_BACKEND=r2 python -c "import sys,os;sys.path.insert(0,'.');from updater import config,blob;d=config.source_dir('<sid>');fs=[f for f in blob.list_parquets(d) if not os.path.basename(f).startswith('_')];print(len(fs),'files',sum(blob.row_count(os.path.join(d,os.path.basename(f))) for f in fs),'rows')"
   ```
   Reading the state, remember: a `partial` NEVER sets `last_success_utc` (R231) and `obs_count` is not comparable across runs (R326) (tools/gen_runbook.py:286-292).

9. **Flip `live: true`**, push, and confirm coverage moved: `python tools/audit_schedule_coverage.py` (see Checklist C step 9 for how to read it).

---

## Checklist B — serve a source end-to-end

Order matters: catalogue → derive → R2-catalog refresh → D1 → util.ts → **deploy** → live check. A series is reachable only if it is in D1 **and** its source is in the deployed `SUPPORTED_SOURCES` **and** its object is in R2 — local artefacts agreeing with each other proves nothing (R224, tools/verify_source_served.py:181-187: noaa once passed catalogue↔R2 with 3,135,873 rows while D1 held TEN).

1. **Catalogue the series** (pick ONE path):
   - General non-IMF path: `python core/broaden_catalog.py --dry-run` then without the flag. Per-series grain, `series_id = <source>:<native_key>`, honest titles (= native key), real min/max dates, licence from the registry source row; idempotent per source (delete+reinsert). Defers sources over `SERIES_CAP=50_000` series or `FILE_CAP=2_000` files (giants get flow-grain), and refuses purged/unhostable sources via the denylist floor from `core/gen_denylist.LEGACY_KEEP` — re-cataloguing them would silently undo the 2026-07-22/23 purge (core/broaden_catalog.py:1-47).
   - Incremental completion of an already-catalogued source (new keys with no row): `python tools/catalog_complete.py <sid> [...]` — `INSERT OR IGNORE`, only for named sources; refuses without a licence; prints the licence flags it is about to apply — **stop if `commercial_ok=1` and DATABASE_LICENSES_VERBATIM.md disagrees** (R117, tools/catalog_complete.py:1-77). If it prints "NO parquet files under ... (backend=...)", the data is not on the backend being read — upload first (tools/catalog_complete.py:80-92).
   - Giants / special grains use their per-family tools: `tools/catalog_fed_board.py`, `catalog_fhfa.py`, `catalog_noaa.py`, `catalog_istat_flows.py`, `catalog_census_tables.py`, `catalog_usda_tables.py`, `catalog_statcan_tables.py`, `catalog_ilostat_indicators.py`, `catalog_imf_direct.py`, `catalog_pxweb_flowgrain.py`, `catalog_whr.py`, `flowgrain_ons_uk.py`, `flowgrain_insee_melodi.py` (each INSERTs into `series`; found by grepping `INSERT INTO series` across tools/ and core/).
   - Proof: `SELECT COUNT(*) FROM series WHERE source_id='<sid>'` > 0. Note `ix_series_source_id` is declared in the schema — without it every source-scoped query full-scans 8.5 GB and looks like a hang (R308, core/catalog.py:24-39).

2. **Derive the per-series CSVs — verify BEFORE writing:**
   ```
   python tools/derive_csv_bulk.py --source <sid> --verify 300 --dry-run
   ```
   Proof: `verify: 300/300 byte-identical` — a **random** sample across the full key range compared byte-for-byte against `core.derive_csv._series_csv_bytes` (the resolver contract: header `series_id,obs_date,value`, source prefix stripped from the id column, `lineterminator='\n'`; core/derive_csv.py:35-52, tools/derive_csv_bulk.py:80-88). A prefix sample once nearly certified a 13%-complete derive (R167, tools/derive_csv_bulk.py:13-17). Any mismatch → the tool refuses to run (exit 1).
   Then the real run:
   ```
   python tools/derive_csv_bulk.py --source <sid> --bucket econ-data --skip-existing --workers 24
   ```
   Proof: `done: N series streamed, put N, skipped 0, errors 0` (exit 1 if any errors, tools/derive_csv_bulk.py:319-321).
   - Add `--qualify-with-shard` for sources whose **catalogue ids carry the store shard** (fed_board, fhfa) — deriving bare keys writes every CSV to the wrong R2 object and the catalogue lists series whose downloads all 404 (tools/derive_csv_bulk.py:119-124,159-161).
   - For non-qualified sharded sources the tool checks shards share no `series_key` and stops loudly if they do — the second shard would silently overwrite the first with a partial history (tools/derive_csv_bulk.py:126-147).
   - Incremental / re-derive path: `python core/derive_csv.py --source <sid> --bucket econ-data` with `--skip-newer-than <ISO8601Z>` to resume a re-derive campaign (`--skip-existing` skips *everything* on a re-derive because every key already exists; core/derive_csv.py:93-97,149-178).

3. **Refresh the R2 catalog copy** — Checklist D. Without it the CI coherence step cannot map the new series and the source demotes to "csv coherence unmet" every run, forever (tools/refresh_r2_catalog.py:3-10).

4. **Sync the catalogue rows to D1:**
   ```
   set PYTHONIOENCODING=utf-8
   python core/sync_catalog_d1.py --source <sid> --dry-run
   python core/sync_catalog_d1.py --source <sid>
   ```
   (default with no flags consumes `data/_aqueduct/pending_catalog_sync.txt`, the orchestrator's queue of derived ids, and truncates it on success; `--keep-pending` preserves it; core/sync_catalog_d1.py:53-58,194-251). This now emits the parent `source` **and** `license` rows FIRST — without them the series are fetchable and invisible: the worker's `/v1/sources` requires BOTH a `source` row and ≥1 series (27 imf_* sources were exactly this gap, 196 listed vs 223 catalogued; core/sync_catalog_d1.py:74-115). Emitted SQL is replayed into in-memory SQLite before any wrangler call. Proof lines: `verified: N series rows replay cleanly (K file(s))` then `catalog sync OK: N series row(s) upserted to D1`.
   D1 rules honored everywhere (core/sync_state_d1.py:13-18): no BEGIN/COMMIT/PRAGMA, ~20-row multi-VALUES statements, files < 900 KB, executed via the version-pinned local wrangler in `api/worker/` (refuses to run if `node_modules/wrangler` is absent; core/sync_state_d1.py:202-205).

5. **Add the source to `SUPPORTED_SOURCES`** in `api/worker/src/util.ts` (the array at api/worker/src/util.ts:19; regenerate from the single source of truth `econdl._resolve.supported_sources()` — util.ts:13-18). A source absent from it 501s `not_migrated` even with perfect D1 + R2.

6. **DEPLOY — editing util.ts changes nothing until this runs.** The constant takes effect only on deploy, and nothing in `.github/workflows` deploys the worker; treating the edited text file as "served" once mislabelled 425,462 series live while unreachable (R345, tools/verify_source_served.py:33-42).
   ```
   cd api/worker
   npm install
   npm run typecheck      # tsc --noEmit, must be clean
   npx wrangler deploy    # = npm run deploy
   ```
   (api/worker/README.md:110-117.)

7. **Live check against the deployed API** (not the repo — staged ≠ deployed, global ledger M-20260715-02):
   ```
   curl -s https://econdl-api.elkassabgi.workers.dev/v1/sources | grep '"<sid>"'
   ```
   `/v1/sources` is unauthenticated and actually discriminates; a `.csv` probe does NOT — auth runs before the migration gate, so a fabricated id returns the same 401 as a real one (tools/verify_source_served.py:44-51). In Python, set a real User-Agent: urllib's default is 403'd by the edge (tools/verify_source_served.py:59-63).

8. **Full three-leg verification:**
   ```
   python tools/verify_source_served.py --source <sid> --sample 40
   ```
   What each output line proves (tools/verify_source_served.py:110-220):
   - `catalogue rows : N` / `R2 objects : N` — local catalogue vs actual R2 listing.
   - `MISSING (catalogued, no object): 0` — no series whose download 404s.
   - `ORPHANED ...: 0 unreachable` — every stray object still resolves (retained legacy ids are reported separately, not as defects — fed_board keeps 21 on purpose; lines 138-156).
   - `byte-compare : 40/40 identical` — what is actually *served* matches the resolver, checked after upload (presence counts alone pass while every object holds the wrong series; lines 5-9).
   - `D1 : N row(s) — matches the catalogue` — via `wrangler d1 execute econ-catalog --remote`; a gap prints `CATALOGUED BUT NOT IN D1: those ids 404 at the API` (R224; lines 75-96, 187-196).
   - `LIVE /v1/sources : listed — discoverable on the deployed API` — the deployed worker, not the local util.ts (R345).
   - Final proof: `<sid>: SERVED — MISSING 0, 0 unreachable objects, sample byte-identical, D1 in step, source supported` and **exit 0**. `STORE COHERENT BUT NOT REACHABLE` means D1 is behind and/or the source is not supported — users cannot fetch it yet (lines 201-220).

9. **Confirm the coverage audit sees it:** `python tools/audit_schedule_coverage.py` — the source must leave `CATALOGUED BUT NOT RESOLVABLE` and, once scheduled, the `WORK QUEUE`.

---

## Checklist C — daily-run triage

The workflow is `updater-daily.yml`, cron 06:00 UTC, concurrency group `aqueduct-updater` (cancel-in-progress: false) (.github/workflows/updater-daily.yml:20-44). Step order: Pull state from R2 → Pull catalog.db → **Run updater** (250-min step timeout inside a 300-min job) → Push state to R2 (CAS + dated backup, `always()` gated on the pull) → Sync freshness to D1 → Sync new catalog series to D1 (`continue-on-error`) → Health gate → Workstation watchdog heartbeat → Daily digest → Heartbeat commit (cron-death guard).

1. **Find and open the red run** (from the repo root, so no `-R` needed):
   ```
   gh run list --workflow updater-daily.yml --limit 10
   gh run view <run-id>                 # which step failed
   gh run view <run-id> --log-failed    # just the failing step's log
   ```

2. **Registry-invalid failure** — the very first thing the "Run updater" step does is validate; the line reads `registry invalid (fix before running):` followed by the problems (updater/orchestrate.py:614-617), most commonly `expected 144 sources, found N` (updater/registry.py:55-56, count at updater/config.py:160). Fix = the registry entry or the `EXPECTED_SOURCE_COUNT` bump (R347). This kills EVERY run, not just one source — top priority.

3. **Killed step, empty-looking log** — the step prints `updater exit code: N` at the end; `137`/`143` means the runner OOM/SIGTERM-killed it ("::error::updater was KILLED ... not a source failure"), and the `[mem] used=..MB avail=..MB` lines sampled every 15 s prove it rather than leave you guessing between OOM, hang, and rate-limit (three ons_uk investigations ran blind before this; updater-daily.yml:60-66, 226-247). `PYTHONUNBUFFERED=1` is what makes the partial log exist at all.

4. **Push-state exit 2** — the compare-and-swap lost: another writer's ETag landed first. Never blind-overwrite; the CAS is the cross-writer guard and a lost race is a loud red, not corruption (updater/run.py:11-22, updater-daily.yml:249-269).

5. **Health-gate red** — step "Health gate (fail past 2x SLA)" runs `python -m updater.health --fail-past-2x-sla` (updater-daily.yml:325-328). Classes, worst-first: `RED-SLA` (job hasn't succeeded within 2× cadence), `RED-DATA` (job "succeeds" but newest observation is stale), `RED-UNRUN` (adapter built, never succeeded), `ATTENTION` (partial/failed/running), `PENDING` (live, no adapter) (updater/health.py:24-28, 235-248, 330, 340-343). Interactive views: `python -m updater.health` (table + health.json), `--red`, `--json` (updater/health.py:11-13). Before "fixing" anything, apply the standing misreads (tools/gen_runbook.py:286-307, 447-455):
   - `last SUCCESS: NEVER` on a healthy source: a `partial` never sets `last_success_utc` (R231).
   - `obs_count` swings by 168M: it means "rows this run" on a productive run and "whole store" on a quiet one (R326) — count the store instead.
   - `deferred (budget N min)` / `budget spent`: nothing failed; the slice ran out and the rest is taken next tick (R303). Do not "fix" it.
   - A future `last_obs_date` is usually a real projection (CSO to 2057, UN WPP to 2101); a defect is a sentinel (9999-12-31) or a counter-as-year — tell them apart by the key and sequence, never the size (R320/R322/R327). `python tools/audit_impossible_dates.py --r2 --source <sid>`.
   - From the 2026-08-04 all-source audit: the real causes were budget_deferral / code_bug / rate_limited / gated_by_design — **zero** were expired credentials or dead endpoints, though that's the usual first guess (tools/gen_runbook.py:447-451).

6. **Workstation-watchdog red** (`python tools/guard_heartbeat.py --check`) — the local machine stopped stamping its 5-minute beat to R2; a different fact from "a source is past SLA", deliberately a separate step so the log names which claim failed (updater-daily.yml:330-346). The workstation is the ONLY updater for the ~17 `run_location: local` sources.

7. **Re-dispatch one source:**
   ```
   gh workflow run updater-daily.yml -f source=<sid> -f force=true
   gh run watch    # or: gh run list --workflow updater-daily.yml --limit 1
   ```
   `-f dry_run=true` prints the plan and writes nothing — no R2, no D1, no heartbeat (updater-daily.yml:31-35). `force` required to actually exercise a not-due source (updater-daily.yml:36-40, R35). Multiple sources: comma-separate the value (updater-daily.yml:206-218).

8. **Per-source deep dive** — `docs/runbook/<sid>.md`, regenerated from ground truth (never hand-edit):
   ```
   python tools/gen_runbook.py --source <sid>     # one source, to stdout
   python tools/gen_runbook.py                    # all pages + index
   python tools/gen_runbook.py --with-store       # + store-vs-state cross-check (slow)
   ```
   (tools/gen_runbook.py:23-25, 539-553.) Each page's §4 is the diagnose-in-order sequence; §5 lists every ledger entry mentioning the source.

9. **The coverage number** — `python tools/audit_schedule_coverage.py [--verbose]`. Definitions (tools/audit_schedule_coverage.py:13-27, 87-145): **SERVED** = catalogued in `catalog.db` AND present in util.ts `SUPPORTED_SOURCES` (either alone is a defect: 501s or invisibility). **SCHEDULED** = registry `live: true` ∪ the updater-heavy.yml matrix literal ∪ sec-edgar-daily.yml's sources ∪ the workstation route (`run_location: local`, which runs regardless of `live` — omitting it once under-counted by 17 sources) — each mechanism MINUS entries with a fetcher-backed strategy and no fetcher module ("scheduled on paper, cannot run"; measured: cepii_gravity and eia sat in the matrix printing `PENDING — no adapter built`, lines 70-84). The headline is `covered/served`. It is a registry fact only — a scheduled source can still have most of its store frozen (worldbank_esg 32/71 indicators, adb 44/54 flows, both honest `partial` for months; R190); for sub-unit coverage run `tools/audit_untouched_files.py --live` (lines 186-204). History of why it's a tool: R143 (unfiltered GROUP BY believed), R157 (cadence filter hid 10 ready sources), R142 (gap-check that passed on 10 missing), R159 (no hardcoded second copies), R137 (comment-stripping before harvesting util.ts).

---

## Checklist D — R2 catalog refresh

**When:** any source demoting to "csv coherence unmet" / logging "no catalog mapping" every run — CI pulls `_aqueduct/catalog.db.zst` from R2 read-only to map changed store keys to catalogue ids, and a lagging copy fails the map *silently and forever*, because a `partial` never sets `last_success_utc` and so never trips RED-SLA either (tools/refresh_r2_catalog.py:1-10; measured 2026-08-02: R2 copy 57.6% short, 28 sources failing to map). Also required after ANY cataloguing (tools/catalog_complete.py:8).

**Command:**
```
python tools/refresh_r2_catalog.py 2026-08-04 --dry-run     # every check, no write
python tools/refresh_r2_catalog.py 2026-08-04               # real upload
```
- `stamp` (positional, default `manual`) — the date suffix for the backup key; supply the real date, "no Date.now in scripts" (tools/refresh_r2_catalog.py:58-59).
- `--against <path>` — an already-decompressed copy of the current R2 catalog, to skip the re-download for the superset check (line 60-61).
- `--allow-shrink src1,src2` — names sources whose series loss is DELIBERATE. Without it, **any** per-source shrink aborts the upload with exit 2 and prints the exact re-run line; losing a source here is silent and total (lines 18-22, 99-110).

**What it does, in order** (each with its proof line):
1. `PRAGMA quick_check` on local `catalog.db` — a torn sqlite uploads happily and fails later on the runner as an unexplained mapping miss (lines 71-79). Proof: `local catalog.db quick_check: ok`.
2. Per-source superset guard against the current R2 copy. Proof: `SHRINK : none — clean superset` (or the abort).
3. **`.bak` backup BEFORE any write** — server-side copy of the live object to `_aqueduct/catalog.db.zst.bak-<stamp>` (no download, no memory; lines 118-121).
4. Streamed zstd compress → upload (everything chunked through disk; the old in-memory path would need ~17 GB RSS at today's 8.5 GB catalogue; lines 11-16, 123-132).
5. Re-download and `quick_check` **the object that is now live** (another process can write catalog.db mid-stream), plus count round-trip and spot-checks including purged sources staying gone (`cow`, `sipri`, `polity` must read 0; lines 134-158). Proof: `uploaded object quick_check: ok` … `DONE`.

**Rollback:** copy the printed `.bak` key back over `_aqueduct/catalog.db.zst` (the script's own final line: `Rollback: copy <bak> back over <KEY>`; line 159).

**Related but distinct syncs, do not confuse:**
- `python core/sync_state_d1.py [--dry-run] [--state-db PATH]` — freshness projection only (`unit_state` + `source_state`, upsert-ALL every run, idempotent, replay-verified twice; explicitly never touches the catalog; core/sync_state_d1.py:1-31, 165-193).
- `python core/sync_catalog_d1.py` — new catalogue rows + parent source/license rows to D1 (Checklist B step 4). Both run automatically in updater-daily after the state push (updater-daily.yml:271-303).

---

COVERAGE: read lines 1-326 of E:/research/econfindatalibrary/tools/derive_csv_bulk.py, last line read: `    sys.exit(main())`
COVERAGE: read lines 1-225 of E:/research/econfindatalibrary/tools/verify_source_served.py, last line read: `    sys.exit(main())`
COVERAGE: read lines 1-256 of E:/research/econfindatalibrary/core/sync_catalog_d1.py, last line read: `    sys.exit(main())`
COVERAGE: read lines 1-267 of E:/research/econfindatalibrary/core/derive_csv.py, last line read: `    main()`
COVERAGE: read lines 1-294 of E:/research/econfindatalibrary/core/sync_state_d1.py, last line read: `    main()`
COVERAGE: read lines 1-165 of E:/research/econfindatalibrary/tools/refresh_r2_catalog.py, last line read: `    raise SystemExit(main())`
COVERAGE: read lines 1-604 of E:/research/econfindatalibrary/tools/gen_runbook.py, last line read: `    raise SystemExit(main())`
COVERAGE: read lines 1-245 of E:/research/econfindatalibrary/tools/audit_schedule_coverage.py, last line read: `    sys.exit(main())`
COVERAGE: read lines 1-415 of E:/research/econfindatalibrary/.github/workflows/updater-daily.yml, last line read: `          exit 1`
COVERAGE: read lines 1-105 (whole file) of E:/research/econfindatalibrary/core/catalog.py, last line read: `    return conn.execute("SELECT * FROM series WHERE series_id=?", (series_id,)).fetchone()`
COVERAGE: read lines 1-100 (header/docstring + licence logic only) of E:/research/econfindatalibrary/tools/catalog_complete.py, last line read: `                print(f"  {source}: NO series_key/idbank column ({cols}) — skip"); return 0`
COVERAGE: read lines 1-80 (header + caps only) of E:/research/econfindatalibrary/core/broaden_catalog.py, last line read: `    return agg`
COVERAGE: read lines 1-60 + grep of add_argument (185-197) of E:/research/econfindatalibrary/updater/run.py, last line read: `    a = ap.parse_args()`
COVERAGE: read lines 1-80 + targeted grep of E:/research/econfindatalibrary/updater/health.py, last line read (grep): `if "--fail-past-2x-sla" in sys.argv:` (line 505)
COVERAGE: read lines 595-634 (validation + source reconciliation only) of E:/research/econfindatalibrary/updater/orchestrate.py, last line read: `                f"registry entry. Refusing to run a partial set silently.")`
COVERAGE: read lines 1-60 (whole file) of E:/research/econfindatalibrary/core/r2_util.py, last line read: `    return None`
COVERAGE: read lines 1-60 (whole file, 57 lines) of E:/research/econfindatalibrary/updater/strategies/fetchers/__init__.py, last line read: `        return False`
COVERAGE: read lines 80-149 (deploy + smoke-test sections) of E:/research/econfindatalibrary/api/worker/README.md, last line read: `at-rest resolver — a series whose source is not in it returns **501`
COVERAGE: read lines 1-449 (whole file) of C:/Users/aelkassabgi/.claude/skills/mistake-ledger/references/global-ledger.md, last line read: `"latest date matches" proves recency, not correctness — diff the values.`