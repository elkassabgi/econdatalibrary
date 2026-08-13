# Giant #14 pre-launch card — US.TradeFoodCatByProc (measured 2026-08-13)

Prepared while giant #13's M0100 campaign holds the one-pull-at-a-time slot, so the
pull can launch the moment that campaign completes.

## Measured facts (live API, not assumed)

- **Dataset**: `US.TradeFoodCatByProc` → source id `unctad_tradefoodcatbyproc`
  (source_id_for verified; no collision with `unctad_tradefoodprocbycat`, which is a
  DIFFERENT dataset also in the unfetched set)
- **Version token**: `10003|2025-03-18T13:57:58` via report_metadata — current_vintage()
  will be non-None (R347 launch precondition)
- **Layout** (dataset_layout): key dims `ProcessFoodCategory.Economy.Partner.Flow`,
  time axis `Year` (is_year=True, not period-coded)
- **Measures**: FOUR magnitude-1 codes — `4023`, `0100`, `5066`, `5058`
  (biotrademerch had two; the measure-aware spill-cache hash (R424 fix) and the
  500-retry ladder (15da5d5a) are both already in the ingest)
- **Size probe** (facts_csv full select for M4023, size-cap 400 response):
  cap 62,500, **estimated obs 673,115,520** — same order as biotrademerch's
  1.06B; expect a multi-day descent + spill campaign per measure

## Launch (verbatim, when M0100 completes — NOT before, one pull at a time)

    cd E:\research\econfindatalibrary
    $env:AQUEDUCT_BACKEND='r2'
    Start-Process -WindowStyle Hidden cmd -ArgumentList '/c', 'python jobs\ingest_unctad_ds.py US.TradeFoodCatByProc >> _ingest_tradefoodcatbyproc.log 2>&1'

Full ingest walks all four measures sequentially through the shared spill cache at
`data/_unctad_spill/unctad_tradefoodcatbyproc/`. If it dies partway, the same command
resumes (leaf pulls cache; intermediate 400 probes re-walk — hours of quiet log +
low CPU is the documented resume pattern, R423).

## Then the standard giant chain (biotrademerch precedent)

grain depth-1/2/3 over the FULL store (duckdb, no sampling) → catalogue at whichever
depth fits D1 headroom → sync_catalog_d1 immediately (R401) → _DOT_TABLE_GRAIN →
core.derive_csv (NEVER derive_csv_bulk, R364) with --skip-existing on any relaunch
(R427) → fetcher + registry + EXPECTED_SOURCE_COUNT same commit (R347) → util.ts →
deploy from main (R405) → verify_source_served exit 0 + R2 count == catalogue count.
