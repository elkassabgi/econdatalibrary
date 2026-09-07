# econdatalibrary — Continuous-Update System ("Aqueduct")

**Status:** design locked 2026-06-23 (Phase 2 of the update-system build).
**Why this exists:** "free, *continuously updated* data in one place" is the backbone of the
project. Today there is no working updater wired to the production data (`data/clean_full/`),
so sources go stale and fixing them one at a time is whack-a-mole. This system makes staleness
*structurally impossible* — every source has exactly one declared update strategy, "did upstream
change?" and "what have I already stored?" are persisted facts (not inferred from file presence),
and the runner is a thin shell over a portable core so **local-D: today and R2/D1 tomorrow is a
config swap, not a rewrite**.

Derived from a 3-architecture design bake-off + critic panel (winner: "Aqueduct", avg 6.67).
The three critic-flagged gaps are fixed here (see **Fixes** in each relevant section).

---

## Core principle

The current failure is that **file-presence IS the state** (67 skip-if-exists scripts), so existing
series freeze — a re-run adds only new series, never extends existing ones with new dates. Aqueduct
replaces *"file exists → skip"* with *"compare stored vintage/last_obs vs upstream → fetch only the
delta, then publish atomically."*

New package `updater/` (importable, no hard dependency on D:). Everything env-specific hides behind
two interfaces — a **`StateStore`** and a **`Blob`** (object accessor). Local impls use the
filesystem + SQLite; cloud impls use Cloudflare D1 + R2. The orchestrator, registry, and strategy
adapters never touch a path or a boto call directly.

---

## (1) State store — `updater/state.py`

Small KV + table API: `get/put_source`, `get/put_unit_state`, `series_cursors(source)`,
`get/put_series_cursor`, `claim_lease/release_lease` (no double-runs), append-only `run_log`.

- **Local now:** SQLite at `data/_aqueduct/state.db` (WAL). Tables: `source_state`, `unit_state`,
  `series_cursor`, `runs`, `leases`. SQLite is the local stand-in for D1 (D1 *is* SQLite).
- **Cloud later:** D1 for `source_state`/`unit_state`/`runs`/`leases`; per-source `series_cursor`
  as an R2 Parquet sidecar (cursors can be millions of rows for statcan). Switch = `AQUEDUCT_BACKEND=local|cloud`.

**`unit_state` row (the heart of change-detection):**
```
source_id, unit_id, strategy,
upstream_vintage,     # etag | last-modified | TOC lastUpdate | cube releaseTime | FileRows/FileSize | release tag/SHA | content-hash
last_success_utc, last_attempt_utc,
status,               # ok | partial | transient_fail | definitive_fail | running
last_obs_date,        # max obs_date written for this unit (for since= extension)
obs_count, attempt_count, last_error
```
A **unit** is the atomic refresh chunk: a flow, cube, dataset, endpoint, survey, *or* the whole source.
A unit is "due" when cadence says so OR `upstream_vintage` changed. A unit is "done" only when
`status=ok AND fully written` — never because a file exists. This row is exactly what the 79
skip/checkpoint/append sources are missing.

> **Fix — source ≠ directory** (critic gap, design [0]): a source maps to **one or more units**, and
> each unit declares its own output path(s). `GATED` → units `boc`,`snb`,`riksbank`,… each
> with its own dir and its own atomic publish + state row. Coverage accounting and atomic publish are
> **per unit**, never per source-dir. The registry carries `units: [{unit_id, out_paths:[...]}]`.

---

## (2) Strategy taxonomy — 6 adapters, all 133 mapped

Each strategy implements `is_due(unit,state,now)` / `detect_change(unit) -> new_vintage|None` /
`run(unit, since) -> Result`. Registry assigns exactly one strategy per source (per unit where they
differ). A registry validator **fails CI** if `count != 133` or any unit lacks a strategy.

- **S1 `overwrite_if_changed`** (~70) — whole-table refresh gated by an upstream vintage signal
  (ETag/Last-Modified, GitHub commit SHA, OWID CSV, faostat `datasets_E.json` FileRows/FileSize, bls
  sizes, bis HEAD). Re-pull + atomic overwrite **only when upstream moved**. Covers all overwrite-on-rerun
  + tiny static/annual full tables.
  > **Fix — lying vintage** (critic gap, design [2]): vintage signals can fail to bump on silent
  > revisions. Mitigations: (a) a **periodic forced full refresh** per S1 source (`max_vintage_age`,
  > default 90d) re-pulls regardless of signal; (b) cheap **content-hash** corroboration where the
  > payload is small; (c) the health monitor re-probes vintage and flags "we think current but upstream moved."
- **S2 `extend_by_date`** (~18–22) — true `since=last_obs` delta within existing series. Read
  `last_obs_date` (per series/unit), fetch only `>last_obs` via the native filter (`startPeriod`,
  `mindate`, `dataInicial`, `record_date:gt:`, `observation_start`), dedup `(series_key,obs_date)`,
  merge, advance cursor. Covers GATED, treasury, ofr, bcb, bcrp, GATED (also fixes the
  0-obs-marked-done bug), eia, fed_board, defillama, riksbank, ecb, GATED, the 8 partials.
- **S3 `sdmx_delta`** (~25–30) — SDMX specialization of S2: `?updatedAfter=<last_success>` to detect,
  `?startPeriod=<last_obs+1>` to pull, merge per flow. Covers abs, adb, bis_full, bundesbank, ilostat,
  insee_bdm/melodi, and the PxWeb/SIDRA NSOs (dst, scb, ssb, statfin, GATED, stat_*, hagstofa, cso,
  GATED, ipea, idb, gus_bdl).
- **S4 `giant_changed_units`** (the 4 giants) — catalog change-feed → selective **whole-unit** re-pull
  (no brute re-pull, no per-vector watermark):
  - **statcan:** `getChangedCubeList(last_success)` → re-pull only changed `productId`s in full.
    > **Fix — mixed-frequency vectors** (critic gap, design [1]): refresh the whole changed *cube*
    > (not a per-cube `since=`), so heterogeneous vector frequencies are handled correctly by construction.
  - **eurostat:** TOC `lastUpdate` per code → reparse only changed `.tsv.gz`.
  - **oecd:** dataflow `last-updated` → re-pull changed flows via `startPeriod` under the 4s token bucket;
    partial flows → `status=partial` (retried, never silently locked).
  - **GATED:** per-provider `?updatedAfter=` + 800MB-XML guard → gaps become `status=partial`, retried.
- **S5 `bulk_snapshot_if_changed`** (~12–15) — whole-file zip/CSV sources: HEAD `Last-Modified`/
  `Content-Length`/manifest gate, rebuild only changed files. Covers bis_cbs_lbs, sec_edgar, bfs,
  cepii_baci, cbs_nl, gus_dbw, insee_sirene_bulk, worldbank_wdi/extra, faostat per-domain, noaa.
- **S6 `manual_vintage`** (~10–12) — publish-rarely / hardcoded-URL / credential-or-WAF-blocked sources
  (stats_nz, ksh WAF, GATED 403, insee_sirene offset-ceiling, barro_lee, several GATED sources, edgar_jrc,
  GATED, yale_epi, harvard_atlas, wid, fsi_fundforpeace, nasa_giss). **Never silently succeeds** —
  `detect_change` polls a cheap signal (release page / GitHub tag / OWID mirror / calendar roll) and,
  when it sees a likely new vintage it can't auto-fetch, opens a **"needs attention" alert**. Cadence
  still tracks "last verified" age.

Registry is **generated from `UPDATE_CAPABILITY_MATRIX.json`** (default strategy per a (mechanism,
incremental, cost) decision table), then human-pinned overrides. No source ships without a strategy.

---

## (3) Orchestrator — `updater/orchestrate.py`

```
load registry (YAML) -> validate (133, all units have a strategy) ->
for each unit: load unit_state ->
  if strategy.is_due(unit,state,now):                 # cadence elapsed OR --force
      v = strategy.detect_change(unit)                # cheap HEAD/feed/manifest
      if v or cadence_forces: enqueue(unit, since=unit.last_obs_date)
dispatch with a concurrency governor:
  per-source rate limits from registry (rate_per_sec, max_workers, cooldown_on_429);
  global semaphore caps total network jobs; cost-tier lanes (fast/medium parallel,
  large/giant get a dedicated low-concurrency lane — statcan large-exclusive preserved).
each job: strategy.run(unit, since) -> Result(status, obs, new_vintage, last_obs_date)
          -> StateStore.put_unit_state(...) AFTER atomic data write -> run_log append.
```
**Resume:** `unit_state` is truth; a crash leaves a unit `running` with an expired lease, re-claimed
next run. `ok` units are skipped. This generalizes the proven `_dbnomics_pull.py`
finalize-only-when-complete + resumable + skip-completed pattern to all 133.

**In-flight protection:** cbs_nl, gus_dbw, GATED-ISTAT are seeded `status=running, owner=firstpass`;
the lease check skips them until first-pass reports done. Aqueduct runs *update* passes only and never
touches their checkpoints or the watchdog.

---

## (4) Extension invariant (the core fix)

A shared `merge_and_write()` helper enforces: **a write either advances `last_obs_date`/`obs_count`
for a unit, or it is a no-op; it NEVER replaces good data with fewer/zero rows.** A 0-row fetch with
unchanged vintage = legitimate no-op; a 0-row fetch from a transient error leaves the unit
`transient_fail` and the existing parquet untouched. This kills the "silently write a 0-row group then
mark done" class (bea, GATED) and the "skip series if key present" freeze (67 sources).
Extension modes: date-tail append (S2/S3), whole-unit overwrite-if-changed (S1/S4/S5), manual-alert (S6).

---

## (5) Failure model (generalized `_dbnomics_pull.py` contract)

- **TransientError** (timeout/5xx/429/network): discard partial rows, unit → `transient_fail`, **don't
  touch existing parquet**, retry next run with backoff.
- **DefinitiveError** (4xx≠404/429, hard caps like insee_sirene offset 10000 / 100k-series cap, BadZip):
  keep max obtainable slice, mark `partial` + reason, surface to monitoring, re-attemptable on demand —
  never silently frozen (fixes oecd/sdmx partial-locked-forever).
- **404 / non-trading-day:** definitive-empty for that key/date, recorded in a skip-set (fixes GATED
  re-probing holidays every run).
- **Atomicity:** every write `.tmp`+`os.replace` (local) / staged-then-PUT (R2); state written *after*
  data. Retrofit atomic-write into the ~dozen non-atomic scripts the matrix flags (abs, bfs,
  several GATED sources checkpoint, bcb/bcrp).

---

## (6) Scheduling / deploy — local-now, cloud-later

`python -m updater.run [--source X] [--cadence daily|weekly|monthly] [--force] [--dry]`.

- **Local now:** Windows Task Scheduler (or a `run_in_background` loop): daily 06:00 (`--cadence daily`
  S2/S3 tails), weekly Tue 02:00 (`--cadence weekly` + giant change-detect passes — only changed units),
  monthly 1st 02:00. `AQUEDUCT_BACKEND=local`, `DATA_ROOT=.../clean_full`. Leaves the 3 first-pass jobs
  + watchdog alone.
- **Cloud later:** identical `updater/` on GitHub Actions / Cloudflare cron; `Blob`→R2 (swap `os.replace`
  for `put_object`, already anticipated in `core/storage.py`), `StateStore`→D1, secrets `.env`→CI Secrets
  (same key names via `core/config.require()`). No strategy/registry/orchestrator changes.

---

## (7) Monitoring / self-check — `updater/health.py`

Reads purely from StateStore; powers a one-page dashboard (HTML local / Worker route cloud):
- **Staleness SLA per source:** `now - last_success` vs `cadence * tolerance` → RED if past. *This is
  what makes whack-a-mole impossible — a frozen source surfaces automatically.*
- **Per-series freshness drift:** newest `last_obs_date` vs expected cadence (a daily source 10 days
  stale is RED even if the job "succeeded").
- **Vintage re-probe** (S1/S4/S5): stored vs upstream → "think current but upstream moved."
- **partial/definitive_fail + S6 alerts** listed with reason + age (the human-attention queue).
- **No-shrink assertion:** any unit whose `obs_count` dropped without a revision flag → alert.
- CI fails the scheduled run if any source is past 2× SLA. Optional push notification on RED.

---

## (8) Rollout order

1. **Scaffold (no behavior change):** `updater/` with `StateStore`(SQLite), `Blob`(fs), registry
   loader+validator, registry generated from the matrix (133 units, default strategy). Backfill
   `unit_state.last_obs_date` by scanning each parquet's max(obs_date) once. Run nothing yet.
2. **Shared helpers:** `merge_and_write()` (atomic, dedup, never-shrink), Transient/Definitive contract,
   rate governor. Unit-test against bcb/GATED fixtures.
3. **S2 first** (highest currency, lowest risk): GATED, treasury, ofr, bcb, bcrp, GATED (+fix
   0-obs bug). Ship the daily cron for just these.
4. **S1 broad sweep:** vintage gates on ~70 overwrite/static sources; retrofit atomic-write to the flagged dozen.
5. **S3 SDMX delta:** ~25–30 medium SDMX/PxWeb NSOs.
6. **S5 bulk-snapshot gates:** faostat, bis_cbs_lbs, sec_edgar, noaa, wdi.
7. **S4 GIANTS one at a time behind change-feeds:** statcan → eurostat → oecd → GATED; each validated
   to touch only changed units before enabling its weekly cron.
8. **S6 manual-vintage alerts** for hardcoded/blocked sources.
9. **Monitoring dashboard + SLA gates** (developed throughout, formalized here).
10. **Cloud cutover:** `StateStore(D1)` + `Blob(R2)`, flip `AQUEDUCT_BACKEND=cloud`, move secrets, run
    the identical orchestrator on CI cron.

---

## Locked decisions
- 6 strategies (S1–S6); registry-as-data generated from the matrix; one strategy per unit, CI-validated.
- State = SQLite→D1; cursors = parquet sidecar; unit = atomic refresh chunk; **source maps to many units**.
- Giants refresh whole *changed units* via change-feeds (no brute re-pull, no per-vector watermark).
- Extension invariant + Transient/Definitive contract + atomic publish are non-negotiable and shared.
- Portable core; local-now/cloud-later is a config swap. First-pass jobs untouched.
