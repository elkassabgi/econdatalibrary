# Data-updater map — for independent review

**Repo:** https://github.com/elkassabgi/econdatalibrary  ·  **Branch:** `main`
**Purpose of this doc:** a complete map of the code that keeps the hosted economic data
up to date, so a second reviewer (human or AI) can independently verify the diagnosis
below. GitHub file links are `.../blob/main/<path>`; where a specific function matters
the line number is given (line numbers drift as the files change — search the function
name if they don't match).

---

## 0. The question being reviewed

"Why do the databases not auto-update, and what makes a source actually update in
production (GitHub Actions) rather than only on a local machine?"

**My diagnosis (please confirm or refute):**

1. **The rollout perimeter.** The scheduled job runs **only** sources flagged `live: true`
   in the registry. Exactly **2 of ~105** are flagged (`cnb`, `frankfurter`), so every
   daily run processes 2 sources and skips the rest — by design, but the rollout was never
   advanced. Everything else is fetched-capable but never executed.

2. **The local-vs-CI trap.** A source's fetcher learns "what's new" by reading the
   existing data. Some fetchers read it through the R2-aware `blob` layer (works in CI);
   others do a **raw local `pq.read_table(path)`** which only works on a machine that has
   the files on disk. In CI (`AQUEDUCT_BACKEND=r2`) the raw read hits a path that doesn't
   exist on the runner, so the source would silently ingest nothing. This is **latent**
   (the 2 live sources are both clean) but it is why "works on my machine ≠ updates in
   GitHub," and it blocks promoting the raw-read sources.

Both are verifiable from the files below.

---

## 1. The data-update flow (top to bottom)

```
GitHub Actions (cron 06:00 UTC)
  .github/workflows/updater-daily.yml
      └─ python -m updater.run            [env: AQUEDUCT_BACKEND=r2, AQUEDUCT_LIVE_ONLY=1]
           └─ updater/run.py              (CLI: --pull-state / run / --push-state)
                └─ updater/orchestrate.py : run_once()
                     ├─ registry.load()   updater/registry.yaml  (which sources exist; live:true)
                     ├─ perimeter filter  _is_live(unit)  ← ONLY live:true run when LIVE_ONLY=1
                     ├─ is_due(unit)       per-strategy cadence gate
                     └─ strat.run(unit)   updater/strategies/<strategy>.py
                          └─ fetcher.update(unit, since)   updater/strategies/fetchers/<src>.py
                               ├─ read existing data to find the frontier  ← blob vs raw read
                               ├─ fetch only newer observations from the provider API
                               └─ merge.merge_and_write(...)   updater/merge.py
                                    └─ updater/blob.py  ← routes read/write to R2 or local disk
           └─ updater/health.py  : the "RED-SLA / OK / PENDING" report + SLA gate
           └─ updater/send_digest.py : the morning email
```

---

## 2. Files, with GitHub links and role

### The scheduler
- **[.github/workflows/updater-daily.yml](https://github.com/elkassabgi/econdatalibrary/blob/main/.github/workflows/updater-daily.yml)**
  — the cron job. Key facts: `cron: '0 6 * * *'`; `workflow_dispatch` (manual, accepts a
  `source` input); env **`AQUEDUCT_BACKEND: r2`** and **`AQUEDUCT_LIVE_ONLY: '1'`**. Steps:
  pull-state → pull catalog.db → **Run updater** (`python -m updater.run --source X`) →
  push-state → sync freshness to D1 → health gate → digest email.

### The engine
- **[updater/run.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/run.py)**
  — entrypoint / CLI (`--pull-state`, `--push-state`, `--source`, `--dry-run`).
- **[updater/orchestrate.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/orchestrate.py)**
  — the heart. Key functions:
  - `run_once(...)` (~line 170) — selects units, applies the perimeter, runs due sources.
  - `_is_live(unit)` (~line 74) — returns `unit.config.get("live")`; **this is the 2-of-105 gate**.
  - the `live_only` filter (~line 191–200): `if live_only and not _is_live(unit): skip`.
  - `_has_adapter(unit)` (~line 61), `_derive_changed_csvs(...)` (~line 88), `_record(...)` (~line 357).
- **[updater/registry.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/registry.py)**
  — `load()` and `validate(reg, expected_count)` (asserts the registry size against config).
- **[updater/registry.yaml](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/registry.yaml)**
  — the source list. Each entry has `source_id`, `strategy`, `cadence`, and optionally
  **`live: true`**. Grep `live: true` → only `cnb`, `frankfurter`.
- **[updater/config.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/config.py)**
  — `DATA_ROOT`, `source_dir(source_id)`, `EXPECTED_SOURCE_COUNT` (a hard assert coupled to
  the registry size).

### The R2/local backend — the CI-capability choke point
- **[updater/blob.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/blob.py)**
  — `AQUEDUCT_BACKEND=r2` routes `exists/read_table/read_schema/write_table_atomic/row_count`
  to the R2 bucket; `=local` uses the filesystem. **A fetcher that reads the store through
  these functions is CI-safe; one that calls raw `pq.read_table(path)` is not.**
- **[updater/merge.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/merge.py)**
  — `merge_and_write(out_path, new_table, mode, dedup_keys)` — atomic dedup merge with a
  **never-shrink** guard (refuses a write that would drop >3% of rows). Routes through blob.
- **[updater/state.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/state.py)**
  — per-source run state (last success, vintage cursors); single-writer, compare-and-swap
  against R2 so a local run and a CI run must not overlap.

### Reporting
- **[updater/health.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/health.py)**
  — the `RED-SLA / RED-DATA / RED-UNRUN / OK / PENDING` summary and the "fail past 2×SLA"
  gate. **This is where a stale source shows up** (e.g. a daily source with `succ_age 29d`).
- **[updater/send_digest.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/send_digest.py)**
  — the morning digest email.
- **[updater/derive.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/derive.py)**
  — turns updated parquet into the per-series CSVs the public API serves.

### Strategies (how a source decides what to fetch)
- **[updater/strategies/base.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/base.py)**
  — `Strategy`, `Unit`, `Result` types + the `is_due` cadence gate.
- **[.../__init__.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/__init__.py)**
  — strategy registry (`@register`).
- The 6 strategies, each delegating to a per-source fetcher:
  [overwrite_if_changed](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/overwrite_if_changed.py) ·
  [extend_by_date](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/extend_by_date.py) ·
  [sdmx_delta](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/sdmx_delta.py) ·
  [bulk_snapshot_if_changed](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/bulk_snapshot_if_changed.py) ·
  [giant_changed_units](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/giant_changed_units.py) ·
  [manual_vintage](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/manual_vintage.py)

### Per-source fetchers — the actual API code
- **Directory:** [updater/strategies/fetchers/](https://github.com/elkassabgi/econdatalibrary/tree/main/updater/strategies/fetchers)
- **[fetchers/__init__.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/fetchers/__init__.py)**
  — the loader: `get(source_id)` imports `fetchers/<source_id>.py`; `implemented(source_id)`
  is how the orchestrator knows a source has an adapter.
- Each fetcher exposes **`update(unit, since) -> Result`** (and for S1 sources
  `current_vintage(unit)`). Clean reference examples:
  [frankfurter.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/fetchers/frankfurter.py)
  (live, CI-safe) and
  [cnb.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/strategies/fetchers/cnb.py) (live).
- Shared helpers: `_common.py`, `_vintage.py`, `_iep.py`, `_giant.py`.

---

## 3. Source inventory (as of HEAD `0a927bf`)

- **Registry size:** 105 sources (`EXPECTED_SOURCE_COUNT`, [config.py](https://github.com/elkassabgi/econdatalibrary/blob/main/updater/config.py)).
- **`live: true` (run daily in CI):** **2** — `cnb`, `frankfurter`.
- **Have a fetcher but NOT live (~40):** abs, adb, bcb, bcrp, bfs, bls, bundesbank, cso,
  defillama, dst, ecb, epu, eurostat, faostat, gapminder, ggdc, hagstofa, imf_commodity,
  imf_weo, insee_bdm, insee_melodi, nasa_giss, oecd, ofr, pip, scb, sec_edgar, ssb,
  stat_estonia, stat_latvia, stat_slovenia, statcan, statfin, treasury, wikidata,
  worldbank_wdi, yale_epi (+ the IEP/PxWeb set).
- **No fetcher yet (~40):** bea, bis, boe, census, eia, ilostat, imf, owid, un_wpp, etc. —
  these have a legacy `jobs/ingest_*.py` bulk loader but no incremental `fetchers/<id>.py`.
- **Raw-local-read fetchers (CI-unsafe until fixed — my item #2):** abs, adb, bls, ecb,
  eurostat, insee_bdm, insee_melodi, istat, scb, stat_estonia, treasury (grep
  `pq.read_table(` / `pq.ParquetFile(` / `open(path` inside those files; `ecb` uses `blob`
  nowhere at all).

---

## 4. How to prove it in production (the only real test)

A source "auto-updates in GitHub" **only** if a real `AQUEDUCT_BACKEND=r2` run ingests
rows. Trigger one for a single source without waiting for the cron:

- GitHub → Actions → **updater-daily** → **Run workflow** → set **source** = `<id>` →
  read the run log for `=== N unit(s) processed ===` and the per-unit `ok/added/…` lines.
- Equivalent CLI: `python -m updater.run --source <id>` with `AQUEDUCT_BACKEND=r2` and the
  R2 credentials set (this is what the CI step runs).

A local run with `AQUEDUCT_BACKEND=local` proves the fetcher executes, but **cannot** prove
CI-capability, because locally every store path exists.

---

## 5. Suggested questions for the reviewer

1. Confirm from `orchestrate.py` + `registry.yaml` that only `live: true` sources run under
   `AQUEDUCT_LIVE_ONLY=1`, and that only 2 are flagged.
2. Confirm from `blob.py` that raw `pq.read_table(path)` in a fetcher is not R2-routed, and
   list which fetchers do it (my list in §3).
3. Is the right rollout to (a) flip `live: true` per source after a green `workflow_dispatch`
   proof, and (b) convert the raw-read fetchers to `blob.read_table` first? Or is there a
   safer/faster sequence?
4. Anything in `merge.py`'s never-shrink guard, `state.py`'s compare-and-swap, or the 6-hour
   Actions job ceiling that would block promoting the large sources (e.g. `abs`)?
