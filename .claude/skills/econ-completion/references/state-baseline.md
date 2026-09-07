# State baseline — condensed current state of the system

All figures dated 2026-08-30, instruments named. **Re-measure before acting** (R509). Full detail: `ECONDATALIBRARY_COMPLETE.md` and `ECONLIB_COMPLETION_PLAN.md` Part 1.

## Headline numbers

| Fact | Value | Instrument |
|---|---|---|
| Served sources | 322 | `py tools/audit_schedule_coverage.py` |
| Catalogued series | 13,486,342 | audit + PK-range sweep (agree exactly) |
| Served observations | 33,908,707,379 | `logs/stats-2026-08-26.json` |
| Scheduled | 270 of 322 sources; 13,148,499 of 13,486,342 series (97.5%) | audit |
| Unscheduled = archival | 52 sources / 337,843 series; **actionable: 0** | audit |
| Local catalogue | 349 source rows (322 with series + 27 empty); 71 licence rows; 11.91 GB | `catalog.db` |
| Local store | 345 GB; 98,785 files; 430 dirs | os.walk |
| State store | 11.30 GB; 28.77M `series_cursor` rows (50x the code's assumed size) | `data/_aqueduct/state.db` |
| Denylist | 49 gated ids (21 live + 28 legacy floor); carve-outs on worldbank/wdi/pink | `denylist.ts` (generated) |

## The five places a series lives (no foreign keys between them)

1. R2 CSV (`series/<urlencoded id>.csv`, gzipped) — what the user downloads
2. D1 `series` — what the catalogue API lists
3. D1 `series_fts` — what search matches (`fts5(series_id UNINDEXED, title, geography)`)
4. local `catalog.db` — what the site generator and local tools read
5. `source_counts` (D1) — the per-source `total`. **NOT single-writer, whatever the older wording here said (corrected 2026-09-07).** `core/sync_catalog_d1.py` refreshes it for the sources ITS OWN push touched; every other tool that writes `series` directly must refresh it too, and one that did not left `sec_edgar` advertising 17,437 against 17,467 rows for two days. `tools/refresh_sec_edgar.py` now does (unconditionally, via `--command`); `tools/delist_source_rows.py`, `tools/retire_source.py` and `tools/delist_timeless_tables.py` already did. **STILL UNHANDLED, so check before trusting a total after they run:** `tools/migrate_noaa_shard.py`, `core/export_d1_sources.py`, and the `core/export_d1.py` / `core/load_d1_chunked.py` re-dump path. Written by `core/sync_catalog_d1.py`

Deleting from four leaves a 404 or an advertising-catalogue (R481, R489). Drift among them has billed money (R489: missing cache row → live `COUNT(*)` per page view).

## Cost model (one paragraph)

D1 reads are ~free ($1/B row, 25B included); **D1 writes are $1/M**; **R2 Class A (PUT/LIST/DELETE) is $4.50/M — the dominant line**; Class B $0.36/M; egress $0. Forward run-rate ~$30/mo pre-tax after the noaa fix (7 post-fix hours of evidence — thin). The billing guard (`tools/billing_guard.py`) reconciles to invoice IN-74622130 at −0.11%. Period runs the 9th–8th; Texas tax ×1.066. The lesson (R430): a bad query shape is a **billing defect**, and the detector used to be Ahmed reading the bill.

## Serving/operational facts that keep causing "it's live" lies

- Worker deploys **manually**: `npx wrangler deploy` from `api/worker/`; no workflow deploys it. Site publishes **manually**: `workflow_dispatch` of `deploy-site.yml`. Pushing publishes nothing.
- API host: `https://econdl-api.elkassabgi.workers.dev`. `api.econdatalibrary.com` is NXDOMAIN; `econdatalibrary.com/v1` returns the site's index.html — never verify API behaviour against those.
- `noaa` is sharded into `econ-catalog-climate`; global counts must merge both DBs or silently drop 3.1M rows.
- `/v1/stats` serves the **July census** (79.8B obs / 7.73B series) from `_aqueduct/stats.json`; the honest measured store is 33.9B / 3.90B. Publication is RESERVED.
- Scheduler count: FOUR paths (registry `live:true` 229 via updater-daily; updater-heavy matrix 34; sec-edgar-daily; `run_local_heavy.ps1` on `run_location: local` 29). The union is 272 of 282 registry entries; 10 are unscheduled; `EXPECTED_SOURCE_COUNT=282` must move with any registry edit or every run refuses.
- `upstream_vintage` advances only on clean success; a `partial` never sets `last_success_utc`; `series_cursors` drive the CSV derive; `CURSOR_CAP=50,000` — an exact-50,000 count is a cap until proven otherwise.

## Open-work register (the plan's W1–W7, one line each)

- **W1 key collisions** (re-key = RESERVED): eia (52.9% of ids; 566 MB single "series"), idb, unctad ×2 (Flow; publisher-confirmed), damodaran (721 wrong values; serves India's tax rate as its default spread), bea/defillama/istat/GATED minor, several GATED sources gated. Five giants unswept: statcan, eurostat, cbs_nl, oecd, ilostat.
- **W2 delivery gap**: 73,125 unmapped changed keys (eia's 50,000 is a cap); 231,782 in `csv_retry_queue` (183,735 UnitTimeout crashes, all attempts=1); ~56 live sources never returned `ok`; 11 fetchers compute "changed" from disk.
- **W3 freshness** *(re-measured 2026-08-30)*: the 26 untouched-file sources are now **ATTRIBUTED** — 13 ROTATING by design (R285's scattered-write-block test), 3 wrote today, 1 pre-attributed (`bea`, R282 ingester-owned tree), **3 that looked stuck are refuted by their own cadence** (`fdic`/`stats_nz` quarterly, `scb` monthly), **1 genuine fault: `idb`** (10 of 40 sub-units refused by the never-shrink guard on every run since 2026-08-06, `obs` frozen at 15,066,444 — it is the key-collision defect, since 39.83% conflicting rows make a clean pull smaller than the store). The audit's fixed 14-day window is **cadence-blind** and over-reports; eurostat 440 flows serve nothing / 540 files vanished; oecd 60 cross-sectional flows (RESERVED); gate has no tolerance for bounded broken minorities (RESERVED); norgesbank provenance (RESERVED); GATED 26 rows (RESERVED).
- **W4 catalogue integrity**: series_fts 2.00x (boc 8x); source_counts drift (vdem missing, ilo advertising 1,157 with 0 rows); stale strings (catalog_coverage "33", SUPPORTED_SOURCES "191", updater-daily comments); /v1/sources cost unmeasured.
- **W5 reliability system** *(re-measured 2026-08-30, superseding this file's first draft)*: `ledger_check --digest` sees only `^## (R\d+) — ` = **112** ids. **155 distinct ids are invisible**, not "100". Missing a digest line: **147 distinct / 174 occurrences** — quote both (R341). **The population is bigger than the R-form:** 171 entries are headed `### M-YYYYMMDD-NN:` with **no R id at all** (42–45 uncited), so a widened `^#{2,3} (R\d+)\b` still covers only **294 of 470 headings = 62.6%** — R501's shape. **27 ids each carry TWO different entries**, so coverage must be keyed on `(id, heading line)`, never id membership, plus a no-id-reuse check. The `RULE_FROM=475` cutoff and any date-based replacement are open-ended and EXEMPT what they cannot parse (`## R443 … (2026-08-22/23)` is a date *range*) — use an **enumerated allowlist** that fails if a member is missing and fails if it would need to grow. CLAUDE.md's "150+ entries" is fixed: **483 distinct rule ids, R1–R520 with 37 gaps** (R520 is the highest id, not the count). See ledger **R521**.
- **W6 honesty**: /v1/stats publication (RESERVED); R-client claim (no econ R client); ~~GATED licence drift~~ — **premise withdrawn 2026-08-30**: `GATED` has **0 series rows locally and does not exist in live D1 at all** (0 series, 0 `source_counts`, 0 `source` rows), so there are not two records to reconcile. It is one stale local `source` row naming ISTAT, which is separately live as `istat`. Local debris, not a licence question.
- **W7 running** *(2026-08-30)*: **statcan derive COMPLETE** — `[8207/8207] units 466,341 · put 252,425 · errors 0`, 5 oversized tables named as refused; this completed Ahmed's 2026-08-18 order to delete the 1,548.7 GB parquet and re-derive compressed, so **the plan's Phase-5 'then the parquet re-upload' is CANCELLED** — it would reverse his own decision (ledger **R520**). Fleet sweep **421/430**, 5.47B rows. cbs_nl/gus_dbw crawlers (judge by artefacts only).

## Reserved list (complete, as of this baseline)

Public-series-id changes (every Phase-4 re-key) · deleting non-re-crawlable data · un-gating a DISPUTED licence · auth & billing · sending email as Ahmed · `/v1/stats` publication · gate policy for bounded minorities · cross-sectional serving (oecd/gleif) · norgesbank un-gating provenance · GATED 26 rows · sec_edgar/sec_edgar_xbrl crossing · building an econ R client.