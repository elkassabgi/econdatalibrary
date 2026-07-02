# Econ Data Library — Storage → Download Architecture

*Decision record. Approved 2026-06-25. Supersedes ad-hoc per-source storage.*

## Thesis
Store every source as **one canonical long table** under a **global `PROVIDER/DATASET/SERIES` key**, with a **central metadata registry** as the single source of truth. A multi-source download is then a **pure projection** over the store (`filter(series_id IN …) UNION`), emitted with Croissant + Frictionless manifests. The heterogeneity problem is solved once, at rest — not re-solved per download.

## 1. Canonical at-rest model (econ time series)
One long schema for all ~299 econ sources — append-only, mixed frequencies coexist, new series never change the schema.

| column | type | role |
|---|---|---|
| `series_id` | string | global key (the join surface) |
| `source` | dict | provider (1st key segment; partition prune) |
| `obs_date` | date32 | **period-START** (one date axis across all frequencies) |
| `value` | double | the measure |
| `freq` `unit` `geo` `adjustment` `obs_status` | dict | the *universal* dimensions (dictionary-encoded ≈ free) |
| `vintage_date` | date32, optional | ALFRED-lite; populate only for revising macro + SEC `filed` |

Source-specific dimensions (age, sex, sector, …) stay encoded in the `series_id` tail, or in a single `dims` JSON/struct column if a source needs them queryable — **never** promoted to per-source columns. **Relational sources stay as-is** (EDGAR 13F/insider = wide per-table parquet under `period=`; intraday equities = wide OHLCV). The global `series_id` namespace *bridges* them into one bundle/manifest without forcing one physical table.

## 2. Global key namespace — `PROVIDER / DATASET / SERIES`
DBnomics grammar (all leaders converge on it). `/` separates the three namespace levels only; `.` separates ordered dimension codes inside the SERIES segment only. PROVIDER is uppercase and **centrally registered** (the registry *is* the collision-free authority). **Migration is additive — no observation value is ever re-keyed:** keep the existing ad-hoc key verbatim as the SERIES tail, prepend a registered PROVIDER + DATASET, swap abused separators. A `key_alias(legacy_key → canonical_id)` table lets the catalog/export resolve either key during transition.

Worked mappings of our real key families:
- `GUSDBW:574:318:…` → `GUS/DBW_574/318.<dim>.<dim>`
- `BCRP:USDPEN_mid` → `BCRP/FX/USDPEN.MID`
- `SI:tableid:dim=code` → `SI/<tableid>/<code>`

**Citation identity is separate from the runtime id.** Never encode version in `series_id`. (See §5.)

## 3. Metadata registry (single source of truth)
Lives in the existing catalog (D1 in prod / SQLite locally) + `catalog.json`. Three tiers:
1. **Central registry** (`core/catalog.py`): `license`, `source` (provider: name/homepage/license_id/attribution/terms_url), `series` (title/freq/unit/geo/category/dates).
2. **Per-source sidecars** on R2: `_provider.json`, `_series.json`.
3. **Parquet footer** KV metadata (so a stray file is still traceable).

License is a **re-serve gate** checked at ingest (`reservable`, `attribution_required`, `commercial_ok`, `no_modify`). Internal model borrows SDMX concept discipline + OWID's indicator/`origins[]` field set; **export emits Croissant (primary, schema.org JSON-LD → Google Dataset Search) + Frictionless `datapackage.json`**. DCAT/PROV-O are the conceptual model, not artifacts we maintain.

## 4. Physical layout — plain partitioned parquet (NOT a lakehouse)
- Parquet on **Cloudflare R2** + the **D1/JSON catalog**. Explicit **NO** to Iceberg/Delta/Postgres (they need a compute engine; we are Workers+R2+D1, ~130 GB, one daily append-mostly batch — they solve problems we don't have).
- Hive layout: `clean_full/<provider>/<dataset>/freq=<f>/data.parquet`; partition only the few giant sources (Eurostat/OECD/ILOSTAT). Most sources = **one file** (avoid the small-file problem). The directory tree *is* the namespace, so a `series_id` resolves to a path → bundling is path resolution + predicate pushdown, not a DB scan.
- Markets + EDGAR keep their current working layouts.

## 5. Versioning
- **Revisions/vintages:** optional `vintage_date` column (SCD-2 by extra rows, never mutate). Populate only where it matters AND the source gives the date free (macro aggregates, SEC `filed`). Skip for never-revising series.
- **Snapshot/citation:** stamp `snapshot_date` + `schema_version` in footer + manifest. **DOI grain = per-dataset Zenodo concept-DOI (cites the evolving dataset) + version-DOI (cites a snapshot).** Not per-series, not one library-wide DOI. (Approved 2026-06-25.)

## 6. Export = projection (ties storage to the bundle)
A multi-source bundle is one zip: per-source subfolders of native long parquet **copied straight from R2** + `datapackage.json` + `croissant.json` + `README.md` + `CITATION.cff`/`ATTRIBUTION.txt` + per-source `codebook`/`provenance`/`LICENSE`. Self-describing, citable, clean — *because the store is clean*. The bundle's `datapackage.json` **is a re-runnable spec** (the reproducible/updatable headline feature): `econdl pull bundle.json` rebuilds the series **pinned to the bundle's `snapshot_date`/version-DOI by default** (exact reproduction); `--latest` is an explicit opt-in. The exporter must never silently substitute latest data for a published snapshot.

## 7. Migration — staged, additive, lowest-risk first (no big bang, no value rewrites)
- **Stage 0 — registry foundation** (catalog only, zero data touch): populate provider/license/series registry, extend coverage 129 → 299, emit sidecars. ← *in progress*
- **Stage 1 — global id as alias** (`key_alias` table; resolve either key).
- **Stage 2 — footer metadata** (one-time pass rewriting parquet footers, not rows).
- **Stage 3 — canonical columns in the ingesters** (forward-only; old files self-heal on next re-pull). *(The continuous-update strategy/wiring work folds in here.)*
- **Stage 4 — `vintage_date`** (opt-in: macro + SEC).
- **Stage 5 — projection bundler + sidecars** (replaces the bespoke single-resource export).

## 9. Adversarial hardening (red-team 2026-06-25 — applied)
Design-level fixes the red-team surfaced; these amend the sections above.
- **§2 key-alias MUST be bijective + collision-checked [w13].** The old_key→canonical_id transform swaps the very separators it parses on, so it can collide. Rule: the migration MUST verify the mapping is injective over ALL real keys before cutover — build `key_alias`, then assert `COUNT(DISTINCT legacy_key)==COUNT(DISTINCT canonical_id)` and that no canonical_id is produced by two legacy keys; on any collision, fall back to a reserved-escaped encoding of the original key (don't lossily swap). No Stage-1 cutover until the alias passes a 100%-bijectivity test on the live catalog.
- **§6 zero-install REST surface [w6].** The Worker exposes, per series, a stable `/v1/{provider}/{dataset}/{series}.csv?format=full|filtered&geo=&from=&to=` + matching `.metadata.json` — curl/R/Stata/browser, no SDK. `econdl` becomes a convenience over these URLs, not the gate. (Copy OWID's `.csv?csvType=full` + `.metadata.json`.)
- **§6 freshness feed [w8].** Public `/v1/last-updates` (per dataset: `last_updated`, `next_update_expected`, `source_date_accessed`, `source_version`) projected from the Aqueduct `unit_state` table (already has `last_success_utc`+`upstream_vintage`). (Copy DBnomics `/last-updates`.)
- **§3 per-series human context [w7].** Add to the series tier: `description_key` (bulleted caveats — for HF equities the **survivorship-bias disclosure is the first bullet**), `description_processing` (what we did to the raw source), and producer-first `citation_short`/`citation_long` (credit BLS/UN/SEC first, "compiled by Elkassabgi Data Library" second). (Copy OWID.)
- **§1 per-segment provenance for spliced series [w11].** For stitched series (equities pre-2022 vs IEX post-2022; multi-feed HF), keep `source` PER-ROW (dictionary-encoded ≈ free) OR ship a per-series date-range→source table, so each observation carries its true origin. The IEX-2022 cutover is the reference case.
- **§6 proxy / pull-through mode for non-redistributable sources [w5].** For `reservable=0` sources, `econdl`/Worker fetch from upstream at runtime and emit **manifest + provenance only** — never a re-hosted copy or our DOI. (Copy DBnomics' license-passthrough posture.) This is what makes "+ a proxy to the rest" real.
- **§6 cross-section query [w12].** `econdl.fetch(provider, dataset, freq='M', geo=['DE','FR'])` resolves a dimension mask to the series_id set server-side via the catalog (freq/unit/geo/adjustment are already columns). (Copy DBnomics SDMX-style masks.)
- **§4/serving free-tier cliffs [w10].** Serve via a Workers **custom domain** (NOT `r2.dev`, which is dev-only/rate-limited); budget the $5/mo Workers Paid plan as a known cost; assemble bundles as **client-side fan-out** (presigned R2 URLs / a manifest the client fetches) so a >50-object bundle doesn't hit the 50-subrequest limit.
- **Durability / citations [w9].** R2 is the unambiguous canonical store; **HF is a regenerable cache** (script full reconstruction from R2); anchor citations on **Zenodo version-DOIs** (sharded under the 50 GB/100-file limit) that resolve back to our landing pages.

## 8. Scope guardrails (do NOT build)
No Iceberg/Delta/Postgres; no full SDMX stack (borrow the DSD *discipline*, not the machinery); no full ALFRED bitemporal history; no harmonizing 299 vocabularies (share codelists only for the universal dims); no re-keying values / per-series files; no wide-at-rest; no Solr; no triple-maintained DCAT+PROV+Croissant (Croissant primary); no CSV/SAS/Stata at rest (export shapes only); no bespoke per-source export code.
