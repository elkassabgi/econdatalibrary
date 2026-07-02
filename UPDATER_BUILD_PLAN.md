# UPDATER_BUILD_PLAN.md — Continuous-Update Backend, Build Plan

**Date:** 2026-07-02
**Directive:** auto-update all econ data, no whack-a-mole, build it without flaws; all-cloud daily updates (never local desktop).
**Supersedes for operations:** `PLAN.md` §5 (connector-matrix model), `docs/DAILY_UPDATE_STATUS.md`, `STATUS.md` update sections, and the legacy `jobs/daily_update.py` + `.github/workflows/daily.yml` path. `CONTINUOUS_UPDATE_DESIGN.md` (Aqueduct) remains the architecture of record; this plan is the build/rollout plan that closes the gaps between that design and what is actually on disk.

---

## 0. Where we actually are (verified 2026-07-02)

What exists and works locally:

- `updater/` Aqueduct package is real: `state.py` (SQLite StateStore: `source_state`, `unit_state`, `series_cursor`, `runs`, `leases` — schema ports verbatim to D1), `orchestrate.py` (leases, TTL-by-cost due-check, transient/definitive failure contract, first-pass protection via `FIRSTPASS_DIRS`), `merge.py` (never-shrink `merge_and_write`, atomic `.tmp` + `os.replace`, refuses 0-row/`min_ratio=0.97` shrink/column drops), `run.py` CLI, `health.py`, `registry.yaml` (130 sources), 6 strategy modules, ~71 source fetchers.
- It has run against production data: `data/_aqueduct/state.db` has 39 `source_state` rows, 48 `unit_state` rows, 1,964,592 `series_cursor` rows, 55 runs spanning 2026-06-23 → 2026-06-24.
- The Worker serves freshness: `api/worker/src/sql.ts` (`LAST_UPDATES`, `UNIT_STATE_FOR_SOURCE`), `lastUpdates.ts` (`/v1/last-updates`), `sources.ts` — all reading `unit_state`/`source_state` from D1 `econ-catalog`.
- R2 publishing exists as manual one-shots: `core/upload_r2.py` (bulk parquet), `core/derive_csv.py` (per-series CSV, byte-identical to the Worker's `/v1/series/{id}.csv` contract).

What is broken or missing (each is a work item below):

| # | Gap | Evidence |
|---|-----|----------|
| G1 | `D:/research/econfindatalibrary` is **not a git repository** — no CI can exist | `git status` → fatal, exit 128 |
| G2 | Nothing schedules `python -m updater.run` anywhere; last updater run 2026-06-24 | grep hits only docs; `runs` MAX(ts_utc)=2026-06-24T10:20:15Z |
| G3 | `.github/workflows/daily.yml` is a trap: legacy pipeline, wrong secret names (`R2_ACCESS_KEY` vs `R2_WRITE_ACCESS_KEY_ID`), wrong bucket (`econdatalibrary-data` vs `econ-data`), nonexistent `--incremental`/`--delta` flags, hardcoded `ROOT="D:/research/econfindatalibrary"` in the script it calls | daily.yml:38-43,72,84; jobs/daily_update.py:27-28 |
| G4 | `updater/blob.py` is filesystem-only; no R2 backend; `AQUEDUCT_BACKEND=cloud` is an unimplemented flag | blob.py:1-7; config.py:10 |
| G5 | `core/r2_util.py` reads creds ONLY from `.env`, never `os.environ` — GH secrets invisible | r2_util.py:18-26,38-53 |
| G6 | Source-count gate not enforced: design says 133, matrix profiles dict has 129, registry has 130; `orchestrate.py:64` calls `registry.validate()` **without** `expected_count` | validate() signature in registry.py:29-48 |
| G7 | Registry has ZERO `unit_id:` entries — every source collapses to a single `_all` unit despite design §"source ≠ directory" | grep -c 'unit_id:' registry.yaml = 0 |
| G8 | D1 freshness refresh is a manual 945 MB full re-dump (`core/export_d1.py`); no incremental state→D1 sync; `/v1/last-updates` frozen at the June-24 snapshot | export_d1.py:56-62; dist/d1/econ_catalog.sql |
| G9 | Local store is ~300.5 GB (clean_full 281.5 + clean_grouped 19.0) vs ~14 GB usable disk on ubuntu-latest — the store can NEVER exist in CI | robocopy byte scan; ARCHITECTURE.md:41 says ~130 GB (stale, 2.3x under) |
| G10 | No `CLOUDFLARE_API_TOKEN` exists anywhere; local wrangler auth is machine-local OAuth that cannot run headless | .env var grep; AppData wrangler config = oauth_token/refresh_token |
| G11 | 79 script-entries (incl. 8 of the 11 rollout starters) use skip-if-series-exists — a plain re-run succeeds but fetches nothing new | UPDATE_CAPABILITY_MATRIX.json `needs_force_or_clear` |
| G12 | Two pipelines can write the same parquet store (legacy `jobs/` vs `updater/`); no recorded decision to retire the legacy path | OVERSIGHT_REPORT_core_reaudit.md |

**Decision recorded here (D-1): the legacy path is retired.** `jobs/daily_update.py`, `jobs/run_connector.py` as an update mechanism, and `.github/workflows/daily.yml` are dead. They are kept in the repo history only. The Aqueduct `updater/` package is the sole update mechanism. Anything the legacy path did that we still need (edgar weekly, eurostat) gets an Aqueduct registry entry instead.

---

## 1. THE update contract (one contract, every adapter)

Every source adapter implements exactly this sequence. It deliberately mirrors the **working** hfdatalibrary daily pipeline (`D:/research/hfdatalibrary/pipeline/daily_update.py` + `.github/workflows/daily-update.yml`), adapted where econ differs.

### 1.1 The five steps

```
fetch_latest(state) → normalize → R2 parquet read-modify-write → state/D1 freshness → re-derive changed series CSVs
```

**Step 1 — `fetch_latest(unit_state) -> FetchResult`**
Input: the unit's stored `upstream_vintage` / `last_obs_date` / `series_cursor` rows from the StateStore. Output: `FetchResult(rows, upstream_vintage, no_change: bool, failure: Transient|Definitive|None)`. The six strategies (S1 overwrite_if_changed … S6 manual_vintage, `updater/strategies/`) are the six lawful implementations of this step. A cheap vintage probe (ETag/Last-Modified/API vintage field) that matches the stored vintage returns `no_change=True` and fetches nothing.

**Step 2 — Normalize.** Rows come out in the store schema (`series_key, obs_date, value, ...`) that `updater/merge.py` expects. No adapter writes files directly.

**Step 3 — R2 parquet read-modify-write (the hf pattern, per object).**
For each touched output object:
1. `GET` the existing parquet from R2 at the object's **full R2 key taken from the registry `out_paths`** — or start empty if the key is absent. The prefix is data, not a constant: most sources live under `clean_full/...`, but some served objects live under `clean_grouped/...` (e.g. sec_edgar's one-parquet-per-company files — `econdl/_resolve.py:163-169` falls back to `clean_grouped/sec_edgar/`). Hardcoding `clean_full/` would write those sources to the wrong prefix and the Worker/clients would keep serving the stale grouped object.
2. `pd.concat([existing, new])`, sort on the series key + `obs_date`, `drop_duplicates(keep="last")` — exactly hf's `daily_update.py:260-269` pattern.
3. Run the merged frame through `merge_and_write`'s invariants: refuse 0 rows, refuse shrink below `min_ratio=0.97`, refuse column drops (`updater/merge.py:52-96`).
4. `PUT` to a temp key, then copy over the final key (R2 has no rename; `CopyObject` + `Delete` of temp, or single `PUT` since R2 PUTs are atomic per key — see D-3 below).

*Where hf's pattern fits:* per-object read-modify-write, sort/dedup keep-last, catch-up self-healing, env-var creds with `.env` fallback, GH cron + `workflow_dispatch`.
*Where it doesn't:* (a) hf has ONE source and ~one schema; econ has 130 heterogeneous sources — hence the registry + strategy layer stays. (b) hf keeps its state ledger (`data/metadata.json`) in git via bot-commit; econ's state is a SQLite db with 1.96M cursor rows — too big and too hot for git. Econ's state round-trips via R2 instead (§1.2). (c) hf re-cleans with 100 context bars; econ's merge invariant replaces that role.

**Step 4 — State + D1 freshness, only after verified publish.**
Only after step 3 succeeds does the orchestrator write `unit_state` (`status=ok|no_change`, `last_obs_date` = max obs_date actually observed in fetched rows, `obs_count`, `upstream_vintage`, `checked_at_utc`). Then a **delta** sync pushes just the changed `unit_state`/`source_state` rows to D1 (new script `core/sync_state_d1.py`, §1.3). Note: the status enum officially includes `no_change` (dominant in practice, 35/48 rows); update `CONTINUOUS_UPDATE_DESIGN.md:46` to match (doc fix, D-2).

**Step 5 — Re-derive changed series CSVs.**
Collect the set of `series_id`s whose rows changed in step 3; for exactly that set, call the derive function factored out of `core/derive_csv.py` and `PUT series/<urlencoded id>.csv` (same bytes as the Worker contract). The updater never assumes the `series/` prefix is fully populated (the 1.37M backfill derive is a separate, still-open task) — it only guarantees: *any series the updater touches has a fresh CSV*.

**CI reality check (this step is NOT portable as-is).** The derive stack reads **local-only assets**: `clients/python/econdl/_catalog.py` opens `data/catalog.db` (measured 1.79 GB, 2026-07-02; overridable via `$ECONDL_CATALOG`) and `econdl/_resolve.py` reads native parquet from `data/clean_full` (overridable via `$ECONDL_DATA`, with a `clean_grouped/` fallback for sec_edgar). Neither exists on a runner, and the 300 GB store never can (G9). So `updater/derive.py` must, in CI: (a) keep the step-3 merged parquet objects on runner disk in a scratch dir laid out like the store and point `$ECONDL_DATA` at it — the bytes are already in hand from the merge, no extra download — deleting per source per §3.3; (b) point `$ECONDL_CATALOG` at a copy of `catalog.db` pulled from R2 (`_aqueduct/catalog.db`, re-uploaded whenever the catalog changes). That is a ~1.8 GB download per run until a slim metadata export exists — acceptable on 14 GB disk, tracked as O-9.

**New-series honesty.** Fetched rows can contain `series_key`s that are NOT in the catalog yet (new upstream series inside an existing unit). They are merged to parquet (never dropped), but they cannot get a CSV or appear in `/v1` until the catalog knows them. v1 rule: detect them (diff against the catalog slice for the unit), record them in a `new_series_pending` table in state, surface the count in health and the run summary — **never silently absorb them**. Catalog/D1 registration + their CSV derive rides the existing `core/export_d1_new_series.py`-style delta path, wired in Phase 3 (O-10).

### 1.2 State in the cloud without D1-StateStore (v1 decision D-3)

Implementing a D1-backed StateStore (`AQUEDUCT_BACKEND=cloud`) is real work and not needed for v1. Instead:

- `state.db` lives in R2 at `_aqueduct/state.db`. **Measured today (2026-07-02): 216,776,704 bytes (~207 MB)** — already past the 200 MB threshold O-8 treated as future, driven by the 1.96M `series_cursor` rows. So the per-run round-trip is ~207 MB down + ~207 MB up *from day one*, growing. Phase 1 must decide the split up front, not "if": either move `series_cursor` to its own db file synced only when cursors change, or keep one file but `VACUUM` + zstd-compress for transfer. Either is fine; deferring the decision is not (O-8).
- CI job start: download it to the runner **recording the object's ETag**; job end: upload it back **plus** a dated backup `_aqueduct/backups/state-YYYYMMDD-runid.db`. The upload is compare-and-swap: if the remote ETag no longer matches the one downloaded, DO NOT overwrite — fail the run loudly and re-run. Last-writer-wins silently losing another writer's state is the one corruption this design must never allow.
- Single-writer guarantee: GitHub Actions `concurrency: { group: aqueduct-updater, cancel-in-progress: false }` on every updater workflow, plus the existing `leases` table as a second belt. **The `leases` table cannot arbitrate across writers by itself** — it lives *inside* state.db, and two writers each operate on their own downloaded copy; the ETag compare-and-swap above is the actual cross-writer guard.
- The existing local `data/_aqueduct/state.db` is seeded to R2 once at Phase 1 cutover; after that, R2 is the source of truth and local runs are forbidden except for giants (§3.4) — and a giant run also does download-before/upload-after. Because a local drain is OUTSIDE the GH `concurrency` group, it must additionally either (a) run with the cron disabled for the drain window, or (b) create an R2 lock marker `_aqueduct/lock` that CI checks-and-refuses on; plus the ETag compare-and-swap as the backstop.

D1 gets only the *freshness projection* (`unit_state`, `source_state`), via idempotent full-upserts of those two tiny tables (§1.3 — no watermark to get wrong). `AQUEDUCT_BACKEND=cloud` (true D1 StateStore) is explicitly a **non-goal for v1** (§7).

### 1.3 New/changed code inventory for the contract

| File | Change |
|---|---|
| `core/r2_util.py` | Read `R2_WRITE_ENDPOINT` / `R2_WRITE_ACCESS_KEY_ID` / `R2_WRITE_SECRET_ACCESS_KEY` from `os.environ` **first**, `.env` fallback — copy the pattern from `D:/research/hfdatalibrary/pipeline/r2_client.py:20-56` |
| `updater/blob.py` | Add `R2Blob` backend (boto3, via `core/r2_util.py` client): `get(key) -> bytes|None`, `put_atomic(key, bytes)`, selected by `AQUEDUCT_BACKEND` env (`local` = current filesystem behavior, `r2` = new). Note this is a *narrower* meaning of "cloud" than the design's D1 switch — rename the value to `r2` to avoid implying D1 works |
| `updater/merge.py` | Accept a Blob handle instead of assuming local paths, so read-modify-write runs against R2 objects in CI and local files at home. Invariants unchanged |
| `updater/statesync.py` (new) or `core/sync_state_d1.py` (new) | After each run: upsert `unit_state`/`source_state` to D1 via `npx wrangler d1 execute econ-catalog --remote --file=<delta.sql>` (`INSERT ... ON CONFLICT ... DO UPDATE`). v1 simplification: these two tables are tiny (48 + 39 rows today; a few thousand at full rollout) — upsert ALL rows every run, no watermark to get wrong, idempotent by construction. Chunk ≤ ~1 MB per file (README already warns about payload limits, `api/worker/README.md:86`). Never full-dump the catalog in CI. Pin the wrangler version in `package.json`/requirements so `npx` doesn't float |
| `updater/derive.py` (new) | Factor the per-series CSV projection out of `core/derive_csv.py` into a callable `derive_and_put(series_ids: list[str])`; `core/derive_csv.py` becomes a thin bulk wrapper for the backlog. **Must be CI-portable per §1.1 step 5**: set `$ECONDL_DATA` to the runner scratch mirror of touched objects and `$ECONDL_CATALOG` to the R2-pulled `catalog.db` — the current local-path defaults (`econdl/_catalog.py:20`, `_resolve.py:45`) do not exist in CI |
| `updater/orchestrate.py` | (a) call `registry.validate(reg, expected_count=EXPECTED_SOURCE_COUNT)` with the pinned constant; (b) after-merge hook that records changed series_ids and calls `derive_and_put`; (c) demote `_has_adapter()` silent-skip: a registry source with no adapter is reported `PENDING` in the run summary AND fails the run if it's inside the live tier (no silent skips within the rollout perimeter). The live tier is a `live: true` flag on each source's registry entry — one source of truth in data, never a source list hardcoded in Python (that would be the whack-a-mole pattern reborn) |
| `updater/registry.py` | `validate()` gains nothing; callers must pass `expected_count`. Pin `EXPECTED_SOURCE_COUNT` in one place (`updater/config.py`) |
| `updater/config.py` | Remove/parameterize any absolute `D:/` paths; everything relative to `ECONDL_ROOT` env with local default |
| `jobs/daily_update.py`, `.github/workflows/daily.yml` | Delete `daily.yml`; add a deprecation header to `jobs/daily_update.py` pointing here (D-1) |

**Registry count reconciliation (fixes G6):** at Phase-1 time, re-measure — do not trust 129/130/133 from any document. Procedure: (1) `len(yaml.safe_load('updater/registry.yaml')['sources'])`; (2) `len(json.load('UPDATE_CAPABILITY_MATRIX.json')['profiles'])`; (3) diff the two sets; (4) for each diff member decide add-or-drop with a one-line reason committed to `updater/REGISTRY_RECONCILIATION.md`; (5) set `EXPECTED_SOURCE_COUNT` to the reconciled number and fix the matrix's false `profiled=133` metadata. Today's known diff: registry has `sec_edgar_xbrl`, matrix doesn't; matrix metadata claims 133 but contains 129. The "133" in `CONTINUOUS_UPDATE_DESIGN.md:66,112` matches nothing on disk — correct the doc.

**Per-unit decomposition (G7):** v1 keeps single `_all` units for all non-giant sources (that is what has actually run and it is adequate for fast/medium sources). Real `units:[{unit_id, out_paths}]` lists are added ONLY for the four giants when Phase 4 builds their change-feed refresh, starting with `central_banks` (design's own example: boc/snb/riksbank sub-units) as the dry run since it's small. Populating units for all 130 sources is a non-goal for v1.

---

## 2. Phases at a glance

| Phase | What | Exit gate |
|---|---|---|
| 0 | Repo + secrets + hygiene (unblocks everything) | Public repo exists, CI hello-world green, all secrets set |
| 1 | Contract hardening (code changes §1.3) | Full local dry-run + one real CI run of `frankfurter`/`cnb` (plus `tcmb` only if A5 is answered — its skip-set blocker must not deadlock the phase gate) writing to R2 |
| 2 | Tier-1 pilot: 11 daily/weekly-fast sources on cron | 14 consecutive green scheduled days, freshness visibly advancing on `/v1/last-updates` |
| 3 | Tier-2/3 expansion: remaining fast, then medium, then large | All non-giant registry sources inside SLA or explicitly stale-marked |
| 4 | Giants: change-detect in CI + capped unit refresh | Each giant has a freshness row that honestly advances or honestly says stale |
| 5 | Steady state: SLA gate, doc cleanup, backlog derive | "Working updater" definition (§6.3) met for 30 days |

---

## 3. Rollout tiers

### 3.1 Tier 1 — the 11 starters (Phase 2)

From the capability matrix (daily/weekly × fast, cross-checked against `rerun_safe_now` / `incremental_ready`):

| Order | Source | Why / mechanism | Caveat |
|---|---|---|---|
| 1 | `tcmb` | ONLY true incremental (append-only, incr=yes) | **OPEN:** ADAPTER_NOTES lists a "tcmb skip-set" hard blocker needing human input — resolve with Ahmed before go-live; if unresolved, start with frankfurter |
| 2 | `frankfurter` | daily full overwrite-single-file = always current, in `rerun_safe_now` | none |
| 3 | `cnb` | rerun-safe overwrite-single-file | none |
| 4 | `riksbank` | checkpoint-resume, incr=partial | needs_force_or_clear — adapter must drive from cursor, not file presence |
| 5 | `bcrp` | incr=partial | same |
| 6 | `nyfed` | daily fast | requires `FRED_API_KEY` (SystemExit without it) — GH secret |
| 7 | `ofr` | daily fast | skip-if-exists — adapter must bypass |
| 8 | `cboe` | daily fast | same |
| 9 | `central_banks` | keyless multi-CB | same |
| 10 | `nbp` | daily fast | same |
| 11 | `cftc` | weekly fast | same; weekly cadence |

Skip `worldbank_esg` despite its weekly/fast label — the matrix notes the data actually refreshes ~annually; give it monthly cadence in Tier 2.

**The G11 trap, addressed head-on:** 8 of these 11 are in `needs_force_or_clear` — their legacy ingest scripts skip when a series file exists, so a naive CI wiring produces runs that *succeed and add nothing*. Rule: Tier-1 go-live for a source requires a **delta proof** — one CI run must demonstrably add ≥1 new observation to ≥1 series (compare `obs_count`/`last_obs_date` before/after in `unit_state`), OR record an honest `no_change` backed by a vintage probe. A source that can only "succeed" vacuously does not ship; its Aqueduct fetcher gets fixed first. ~71 fetchers exist in `updater/strategies/fetchers/` — whether each actually implements Aqueduct semantics (vs. wrapping legacy skip-if-exists logic) is UNVERIFIED until its delta proof passes (O-6); each of the 11 gets this proof individually — no assumptions.

### 3.2 Tier 2 — remaining fast + weekly/monthly (Phase 3a)

The rest of the 65-source fast tier plus weekly/monthly medium sources. Batches of ~10, each batch needing: adapter present, delta proof or honest-no-change proof, key present if keyed (`bea`, `census`, `fred_releases`, `insee_sirene`, `usda` are in `blocked_on_keys` — add GH secrets as each is reached). Sources with known upstream breakage (`wiid` all-403, `gpi` all-404, `spi` 404s, `cow` version-bump filenames, `whr` 403/404s — the 5 hard blockers in `ADAPTER_NOTES.md`) are marked `definitive_fail`/stale in state, visible in health, and parked for Ahmed input; they do NOT block the tier.

### 3.3 Tier 3 — medium (39) and large (25) cost sources (Phase 3b)

Runner constraint (G9) becomes binding: ubuntu-latest has ~14 GB usable disk and several singles approach it (`insee_sirene` ~7 GB download/~4.6 GB parquet, SEC EDGAR ~2.9 GB zips, Bundesbank BBEX3 ~1.3 GB XML fully buffered in RAM). Rules:

- Per-source lifecycle inside one job: stream-download → process → upload to R2 → **delete local artifacts** before the next source.
- Large-cost sources get their own workflow (`updater-large.yml`) with at most 2 sources per job, serialized.
- Any source measured >10 GB peak footprint is escalated to the giants treatment (§3.4) regardless of the matrix label. **OPEN:** per-source peak-disk numbers exist only for a few sources in the matrix; measure each large source once during its onboarding run and record in the registry entry.

### 3.4 Giants — `oecd`, `eurostat`, `sdmx_nso`, `statcan`: explicitly OUT of CI

Full sweeps are multi-hour/multi-GB (~57 GB oecd, ~6.1B-obs eurostat, statcan sandbox-kill hazard on concurrent >8MB streams) and can never fit a runner. How they update instead:

1. **Change-detect IS in CI** (cheap): a weekly job (`updater-giants-detect.yml`, cron `0 2 * * 2`) probes each giant's change-feed/metadata endpoints only (S4 `giant_changed_units` strategy), writes the changed-unit queue into `unit_state` (`status=pending_refresh` or a `refresh_queue` table), and updates D1 so `/v1/last-updates` honestly shows detection time.
2. **Capped refresh in CI where units fit**: a follow-up job refreshes at most N changed units per run within a hard budget (≤5 GB disk, ≤120 min), sequential downloads only for statcan (watchdog hazard). Most changed units per week should be small; the budget prevents blowups.
3. **Oversize spillover runs locally, rarely, and honestly**: units exceeding the CI budget are left queued and surface in health as `pending_refresh`; they are drained by a manually launched local run (`python -m updater.run --source oecd --queued-only`) that round-trips state via R2 exactly like CI (§1.2). This is the ONE sanctioned local role — a spillover drain, not a schedule. The owner directive says never local desktop for the *dailies*; a monthly manual drain of oversize giant units is disclosed here as the exception until a paid always-on runner exists. **OPEN:** if Ahmed prefers zero local involvement, the alternative is a self-hosted/paid cloud VM — his call, costed separately.
4. Giants never run the skip-if-exists legacy scripts; refresh = whole changed unit re-pull through `merge_and_write`.

This is also where registry `units:[]` lists get real (G7): Phase 4 populates unit lists for the four giants (and `central_banks` as the pilot) from their catalog endpoints via `updater/gen_registry.py`.

### 3.5 First-pass trio — untouched

`cbs_nl`, `gus_dbw`, `dbnomics`(-ISTAT) stay protected by `FIRSTPASS_DIRS` in `orchestrate.py:23-32`. The design wanted `owner=firstpass` seeding; the hardcoded skip is functionally equivalent — keep it, add a comment, update the design doc (D-2). When a first-pass job completes, removing it from `FIRSTPASS_DIRS` + adding its registry cadence is a deliberate, single-line PR.

---

## 4. Infrastructure prerequisites — WHO does WHAT

### 4.1 Ahmed (nobody else can)

| # | Action | Detail |
|---|---|---|
| A1 | Create the **public GitHub repo** (suggested: `elkassabgi/econdatalibrary`) and grant push access | Public per the econ-infra design decision. Name choice is his |
| A2 | Mint `CLOUDFLARE_API_TOKEN` for CI | Scopes exactly per `api/DEPLOY.md:13-14`: Account → **D1 Edit, Workers Scripts Edit, Workers R2 Storage Edit**. The local wrangler OAuth (`AppData/.../.wrangler/config/default.toml`) is machine-local and cannot run headless — there is no workaround. Add as GH secret `CLOUDFLARE_API_TOKEN`, plus `CLOUDFLARE_ACCOUNT_ID=ce51d5c7fe3859098751b89bbebeab7a` |
| A3 | Add R2 secrets to GH | `R2_WRITE_ENDPOINT`, `R2_WRITE_ACCESS_KEY_ID`, `R2_WRITE_SECRET_ACCESS_KEY` — **same names as `.env`** so `core/r2_util.py` (post-fix) reads them identically local and CI. Values copied from the local `.env` |
| A4 | Add `FRED_API_KEY` GH secret | Required by `nyfed` (Tier 1); also unblocks `fred_releases` later |
| A5 | Answer the 5 hard-blocker questions in `updater/ADAPTER_NOTES.md` | cow version-bump filenames, gpi 404s, spi 404s + duplicate script, **tcmb skip-set (blocks Tier-1 #1)**, whr 403/404s |
| A6 | (Optional) `RESEND_API_KEY` GH secret | Enables hf-style failure email; without it, failures are GH-Actions-red + health.json only |
| A7 | (Later, Tier 2) `BEA_API_KEY`, `CENSUS_API_KEY`, `INSEE_SIRENE_KEY`, EIA/others as their sources onboard | From `blocked_on_keys` in the matrix |
| A8 | Confirm the Cloudflare plan tier (Workers Paid) covers daily D1 writes + the D1 catalog size | Project notes record the custom-domain cutover as already pending Workers Paid activation; the catalog SQL dump is ~1.6 GB and daily `wrangler d1 execute` upserts add ongoing writes — verify plan limits BEFORE Phase 1's first `sync_state_d1.py` run, or the D1 sync fails on quota mid-rollout |

### 4.2 Assistant (all code and repo work)

**Phase 0 — repo creation, done surgically (G1, and the "300 GB tree" risk):**

- `git init` with the existing `.gitignore` (already excludes `data/` line 2, `.env*` line 14) **extended first** to also exclude: `dist/`, `api/worker/node_modules/`, `_raw_*/`, `*.log`, `*.db`, `data_playground/`, any `_aqueduct/` local copies, and top-level scratch JSON artifacts.
- **Never `git add .`.** Curated allowlist add: `updater/`, `core/`, `api/` (minus node_modules/dist), `jobs/` (deprecated but historical), `connectors/`, `docs/`, `econdl/` (resolver), top-level `*.md`, `.github/`, `.gitignore`, `requirements*`. Then `git status --porcelain` audit: any file >5 MB or matching secret-ish patterns (`*key*`, `*token*`, `*.env*`, `*_vars_*.log`) is individually justified or excluded.
- Pre-push scan of the staged tree for the **literal secret VALUES** read out of `.env` (every BLS/BEA/CENSUS/EIA/NOAA/FRED/INSEE/USDA/R2_*/HFDL/GUS value grepped verbatim against every staged blob) — grepping for variable *names* is not enough: a key hardcoded without its name (exactly the hf `local_backfill` incident) sails past a name scan. Plus a regex for 20+-char hex/base64 blobs as the catch-all, each hit individually justified.
- Delete `.github/workflows/daily.yml` **in the very first commit** so the stale trap can never fire on push (G3).
- Push to Ahmed's repo (A1); verify a trivial `workflow_dispatch` hello-world action runs green.

**Phase 1 — the §1.3 code changes**, plus workflows:

### 4.3 Actions layout (concrete)

One orchestrated entrypoint; the Aqueduct TTL-by-cost due-check already decides what's due, so we do NOT need one workflow per cadence — but we split giants and large for disk/time isolation:

```
.github/workflows/
  updater-daily.yml          # cron: '0 6 * * *'   (every day 06:00 UTC — FX/CB dailies publish weekdays,
                             #                      but daily cron + no_change probes is simpler and honest)
  updater-weekly-large.yml   # cron: '0 3 * * 3'   (Wed 03:00 UTC — large-cost sources, ≤2 per job)
  updater-giants-detect.yml  # cron: '0 2 * * 2'   (Tue 02:00 UTC — change-feed probe only, Phase 4)
  updater-giants-refresh.yml # workflow_run after detect; capped unit refresh, Phase 4
  monthly is handled by TTLs inside updater-daily.yml — a monthly-cadence source
  simply isn't due 29 days out of 30.
```

`updater-daily.yml` core (mirrors hf's `daily-update.yml` shape):

```yaml
on:
  schedule: [{cron: '0 6 * * *'}]
  workflow_dispatch:
    inputs: {source: {required: false}, dry_run: {type: boolean, default: false}}
concurrency: {group: aqueduct-updater, cancel-in-progress: false}
jobs:
  update:
    runs-on: ubuntu-latest
    timeout-minutes: 300
    env:
      R2_WRITE_ENDPOINT: ${{ secrets.R2_WRITE_ENDPOINT }}
      R2_WRITE_ACCESS_KEY_ID: ${{ secrets.R2_WRITE_ACCESS_KEY_ID }}
      R2_WRITE_SECRET_ACCESS_KEY: ${{ secrets.R2_WRITE_SECRET_ACCESS_KEY }}
      CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}
      CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
      FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
      AQUEDUCT_BACKEND: r2
    steps:
      - checkout; setup-python 3.11; pip install -r requirements-updater.txt
      - run: python -m updater.run --pull-state      # download _aqueduct/state.db from R2
      - run: python -m updater.run ${{ inputs.source && format('--source {0}', inputs.source) }} ${{ inputs.dry_run && '--dry-run' }}
      - run: python -m updater.run --push-state       # upload state.db + dated backup
      - run: python core/sync_state_d1.py             # delta upsert unit_state/source_state to D1
      - run: python -m updater.health --fail-past-2x-sla   # exit 1 → red run (§5)
      - run: |                                        # heartbeat commit — see cron-death guard below
          date -u +'%Y-%m-%dT%H:%M:%SZ run=${{ github.run_id }}' > ops/last_run.txt
          git config user.name github-actions; git config user.email actions@github.com
          git add ops/last_run.txt && git commit -m "heartbeat" && git push
```

(Exact flags to be implemented in `updater/run.py`; `--pull-state/--push-state` may fold into `run_once()` — implementation detail, the contract is what's fixed.)

**Cron-death guard (mandatory, not optional):** GitHub automatically **disables scheduled workflows after 60 days without repository activity**. Econ's state round-trips via R2 and nothing else ever commits — so at steady state (Phase 5, no code churn) the cron would silently switch off and freshness would rot with zero red runs, the exact failure mode §5 exists to prevent. hf survives this only because it bot-commits `metadata.json` every run. Econ does the same: every scheduled run commits a one-line `ops/last_run.txt` heartbeat (doubles as an in-repo audit trail of run ids). If the heartbeat push fails, the run is red.

The Worker itself needs no cron (`wrangler.toml` correctly has none) — freshness arrives by D1 writes.

**OPEN (deploy verification):** memory says the Worker is live on workers.dev, but nothing inside this repo proves deployment state. During Phase 1, verify with `npx wrangler deployments list` (after A2) or a plain HTTPS GET to the workers.dev `/v1/last-updates`; record the URL in `api/DEPLOY.md`.

---

## 5. Honesty rules (baked into code, not into promises)

1. **Freshness rows advance only on verified fetch.** `unit_state.last_obs_date` is set from the max `obs_date` present in *rows actually fetched and merged this run* — never from `datetime.now()`, never from a schedule expectation. `checked_at_utc` (when we looked) is stored separately from `last_obs_date` (what the data says); the Worker already serves honest nulls — keep it that way.
2. **`no_change` must be earned.** A `no_change` status requires a recorded vintage probe result (ETag/hash/vintage field equal to stored). "The fetch returned nothing and we shrugged" is a `transient_fail`, not a `no_change`.
3. **Failed source → loud + stale-marked, never silently skipped.** Transient/definitive contract stands (`orchestrate.py`). Every registry source inside the live tier gets a status EVERY run; the `_has_adapter()` silent skip is demoted to an explicit `PENDING` line in the run summary and is a **run failure** if the source is in the live tier (§1.3). `updater/health.py --fail-past-2x-sla` exits nonzero when any live source exceeds 2× its SLA → the Actions run goes red → GH notification (+ Resend email if A6). D1 keeps serving the true (stale) date — the public endpoint never lies to hide our failure.
4. **Never-shrink is enforced at publish, not audited after.** `merge_and_write` invariants (0-row refusal, `min_ratio=0.97`, column-drop refusal) apply to every R2 write. A refused merge is a `definitive_fail` with the refusal reason in `runs`.
5. **Idempotent re-runs.** Running the same day twice must be a no-op: dedup keep-last makes double-merges harmless; state uploads are serialized by the concurrency group AND guarded by the ETag compare-and-swap (§1.2) — never blind last-writer-wins; `sync_state_d1.py` upserts are idempotent by primary key; CSV re-derives are byte-identical re-PUTs. Phase-1 test T-4 proves this.
6. **No fabricated counts anywhere.** `EXPECTED_SOURCE_COUNT` is measured at reconciliation time (§1.3), not copied from a doc. health.json snapshots carry their generation timestamp. Docs that state numbers (`ARCHITECTURE.md` ~130 GB vs measured 300.5 GB; matrix `profiled=133` vs actual 129) get corrected in Phase 1's doc pass.
7. **CSV/parquet coherence.** Any series whose parquet changed gets its CSV re-derived in the same run (step 5 of the contract). If the CSV PUT fails after the parquet succeeded, the run is `partial` and the series_id goes into a retry queue table — never silently dropped.

---

## 6. Testing & verification

### 6.1 Per-phase gates

**Phase 0:** hello-world `workflow_dispatch` green; `git ls-files` audit shows no file >5 MB, no secrets, no `data/`; secret-scan of staged tree clean.

**Phase 1 (local first, then CI):**
- T-1 unit: `merge_and_write` invariant suite (0-row, shrink, column-drop, dedup keep-last) against the R2Blob backend using a scratch prefix `_aqueduct_test/`.
- T-2 creds: `core/r2_util.py` resolves creds from env-only (unset `.env`), from `.env`-only, and env-over-`.env` precedence.
- T-3 end-to-end dry-run: `python -m updater.run --source frankfurter --dry-run` in CI prints the full plan, writes nothing (verify scratch prefix untouched).
- T-4 idempotency: run `frankfurter` twice in one hour; second run must be `no_change` or byte-identical parquet (compare R2 ETags before/after).
- T-5 delta proof for `frankfurter`/`cnb` (+`tcmb` if A5 resolved): `unit_state.last_obs_date` advances or honest `no_change`; new obs visible via `GET /v1/series/{known_id}.csv` (CSV re-derive proven end-to-end).
- T-6 D1 sync: after the CI run, `GET /v1/last-updates` shows `checked_at` within the last hour for the pilot sources.
- T-7 failure honesty: point a scratch registry entry at a 404 URL and another at a connection-refused endpoint; verify the failure contract classifies each (persistent 404 → `definitive_fail` per the contract's retry policy; refused connection → `transient_fail`), run summary red, D1 date NOT advanced, no parquet touched.

**Phase 2:** every Tier-1 source passes T-4/T-5 individually before joining the cron; then 14 consecutive scheduled days green.

**Phase 3:** each batch of ~10 repeats T-4/T-5; large sources additionally record measured peak disk in the registry; runner never exceeds 80% disk (log `df` per source).

**Phase 4:** giants — a seeded fake "changed unit" flows detect → queue → refresh → merge → D1 within budget; statcan refresh proven sequential-only.

### 6.2 Standing weekly audit (cheap, automated)

A `verify` step in the daily workflow, Sundays only: sample 20 random series across live sources; for each, download parquet from R2 and CSV from the Worker; assert CSV bytes == fresh derive of the parquet, and `last_obs_date` in D1 == max obs_date in parquet. Any mismatch → red run.

### 6.3 Definition of "working updater" (measurable, binary)

The updater is WORKING when, over a rolling 30-day window, all of:
1. ≥ 28 of 30 scheduled `updater-daily.yml` runs completed (green or honest-red-with-cause; zero runs lost to infra we control).
2. Every live-tier source is within 2× its SLA **or** is explicitly `stale`-marked with a recorded failure cause — zero sources in silent limbo.
3. `GET /v1/last-updates` `checked_at` is < 26 h old for every daily-cadence live source.
4. Zero never-shrink violations and zero rows lost (spot-audit 6.2 all green).
5. At least one real new observation ingested for ≥ 80% of daily-cadence live sources over the window (proves we're not accumulating vacuous `no_change`s — FX/CB dailies publish most weekdays).
6. Zero manual local interventions required for non-giant sources.

Until all six hold, the updater is "in rollout", and we say so.

---

## 7. Explicit non-goals for v1

1. **D1-native StateStore** (`AQUEDUCT_BACKEND=cloud` as designed) — v1 uses SQLite-via-R2 round-trip (D-3). Revisit only if state.db R2 round-trip proves fragile.
2. **Giants full re-pulls in CI** — never. Change-detect + capped unit refresh only (§3.4).
3. **Per-unit registry decomposition for all 130 sources** — units only for the 4 giants + `central_banks` pilot.
4. **Finishing the 1.37M-series CSV derive backlog** — separate task; the updater only guarantees freshness for series it touches.
5. **Fixing the 5 hard-blocked sources** (cow/gpi/spi/whr, tcmb pending A5) — they surface honestly as stale until Ahmed's input.
6. **First-pass trio migration** (`cbs_nl`, `gus_dbw`, `dbnomics`) — protected, untouched, until their backfills finish.
7. **Worker feature work, Pages, i18n, custom-domain cutover** — separate tracks; this plan only feeds them fresh data.
8. **Legacy connector framework revival** — retired (D-1). No effort goes into `connectors/base.py` `fetch(since)`, `sources.yaml` cadences, or `data/_last_run.json`.
9. **Cost re-estimation / R2 class changes** — flag only: measured 300.5 GB vs the ~130 GB in `ARCHITECTURE.md` and 130–240 GB in `PLAN.md:73` means storage-cost docs are stale; re-do the numbers in the Phase-1 doc pass, but no infra change in v1.

---

## 8. OPEN items register (nothing here is assumed resolved)

| ID | Open question | How it gets resolved |
|---|---|---|
| O-1 | Reconciled source count (129 vs 130 vs 133) | §1.3 procedure at Phase-1 time; commit `updater/REGISTRY_RECONCILIATION.md` |
| O-2 | tcmb skip-set hard blocker | Ahmed (A5) before Tier-1 slot #1; frankfurter leads meanwhile |
| O-3 | Worker deployment state (memory says live on workers.dev; repo doesn't prove it) | Phase-1 `wrangler deployments list` / HTTPS probe; record in `api/DEPLOY.md` |
| O-4 | Per-source peak disk for the 25 large-cost sources | Measure during each source's onboarding run; record in registry |
| O-5 | Giant spillover: manual local drain vs paid always-on runner | Ahmed decides when Phase 4 starts; plan defaults to disclosed manual drain |
| O-6 | Do any Tier-1 Aqueduct fetchers still inherit skip-if-exists semantics? | Per-source delta proof (T-5) — no source ships without it |
| O-7 | Repo name and public/private | Ahmed (A1) |
| O-8 | `series_cursor` growth — state.db is **already 216,776,704 bytes (~207 MB, measured 2026-07-02)**, past the old ">200 MB" trigger | Decided at Phase 1 (not deferred): split `series_cursor` into its own db file synced only when cursors change, or single file with VACUUM + compression for transfer; log db size per run either way |
| O-9 | `catalog.db` (1.79 GB) needed in CI for CSV derive (step 5) — full pull per run vs slim metadata export | v1: pull full `_aqueduct/catalog.db` from R2 per run (fits 14 GB disk); build the slim series-metadata export when run time hurts |
| O-10 | New upstream series inside existing units — catalog/D1 registration + CSV derive path | v1: merged to parquet + `new_series_pending` in state + health surface (never dropped, never silent); auto-registration via `export_d1_new_series.py`-style delta wired in Phase 3 |

---

## 9. Sequence summary

```
Phase 0  (blocked on A1)          : curated git init → push → hello-world CI → secrets A2-A4
Phase 1  (assistant, ~code only)  : §1.3 changes → T-1..T-7 → pilot sources (frankfurter/cnb, +tcmb if A5) live in CI manually
Phase 2  (cron on)                : Tier-1 11 sources → 14 green days
Phase 3  (batches)                : fast remainder → medium → large (disk rules)
Phase 4  (giants)                 : detect cron → capped refresh → unit lists real
Phase 5  (steady state)           : SLA gate standing, §6.3 met 30 days, docs corrected
```

The single biggest schedule risk is Phase 0: every downstream step is blocked until the repo exists (A1) and the Cloudflare token is minted (A2), because without them there is no CI substrate and no headless path to D1 at all.
