# PxWeb Flow-Grain Publish — Flip Plan

_Companion to `PXWEB_TIMEAXIS` work (2026-07-22). The DATA fix is done + live; this file is
the remaining **go-live flip** for the 9 PxWeb sources (ssb, stat_slovenia, stat_latvia, dst,
scb, statfin, hagstofa, stat_estonia, bfs). Everything below is production / needs the
diverged-main reconciliation, so it waits for Ahmed._

## Status
- **DATA** — all 9 sources fixed (name-first time-axis defect) + re-uploaded to R2, verified
  CORRUPT=0 and out-of-range=0 (28.8M wave-2 rows + core sources). **DONE + LIVE.**
- **CATALOG (local)** — **22,787 flow-grain tables** (one per PxWeb table) committed to
  `data/catalog.db`: `series_id="<source>:<prefix>"`, **real titles** from each source's own
  metadata (title-match 100% for 7 sources, 99% dst, ~96% bfs; the rest fall back to the real
  table id — never fabricated), real min/max obs_date. **DONE this session (local only).**
- **DERIVE tool** — `tools/derive_pxweb_flowgrain.py` built + verified: CSV header
  `series_id,obs_date,value` (contract-shape), sorted; byte-parity checked vs parquet
  (34,622 == 34,622 rows for `stat_slovenia:SI:0214809S`). **Built + verified; not yet run to R2.**

## Flip checklist (production)
Run from a shell where `econfindatalibrary/.env` has the R2 write creds + wrangler is logged in.

1. **Derive → R2** (inert until step 2 — the Worker 404s on an uncataloged id):
   `python tools/derive_pxweb_flowgrain.py --bucket econ-data --skip-existing`  (~22.7k CSVs).
   _Claude can run this now if desired — it touches R2 but changes nothing user-visible until D1._
2. **D1 export** (makes them discoverable + served): emit `dist/titles/<src>.json` for the 9
   from catalog.db, run `core/export_d1_new_series.py <the 9>`, then apply
   `dist/d1/newseries/part_*.sql` then `_fts_rebuild.sql` via
   `wrangler d1 execute econ-catalog --remote --file=...`. (Exports ONLY `series` + FTS — does
   NOT touch the license table, so deployed reservable flags are undisturbed.)
3. **supportedSources** (else the Worker returns 501 not_migrated): add the 9 to
   `SUPPORTED_SOURCES` in `api/worker/wrangler.toml`, then `(cd api/worker && wrangler deploy)`.
4. **reservable flags** (LOCAL catalog.db — needed for site gen + `gen_denylist`, NOT for the D1
   series export). Collateral-checked:
   - SAFE (single-source licenses): `UPDATE license SET reservable=1 WHERE license_id IN
     ('nlod-2.0','surs-terms','opendata-swiss-by-ask');`  (used only by ssb / stat_slovenia / bfs)
   - stat_latvia: `UPDATE source SET license_id='cc-by-4.0' WHERE source_id='stat_latvia';`
     — its current `NEEDS-REVIEW` license is **shared by 8 restricted sources** (several GATED sources,
     wid, whr, several GATED sources); do **NOT** flip NEEDS-REVIEW.
   - **stat_estonia — AHMED DECISION.** Its `cc-by-sa-4.0` is shared with **8 `unesco_*`**
     sources (5 of which are NOT in the denylist). If those unesco_* are redistributable, flip
     `cc-by-sa-4.0` globally; if not, give stat_estonia its own row (e.g. `cc-by-sa-4.0-ee`,
     reservable=1) and repoint. Verified audit says stat_estonia itself IS redistributable.
5. **source attribution** — confirm the 9 `source` rows carry attribution/homepage/terms so the
   CC-BY citation header (series.ts) is populated (attribution is a binding licence condition).
6. **site** — `python catalog/gen_site.py && wrangler pages deploy catalog/site --project-name=econdatalibrary`.
7. **git push** — `pipeline-robustness` → `origin/main`. Local `main` is DIVERGED (48 ahead / 7
   behind) — Ahmed reconciles (this is the one true blocker).
8. **daily wiring** — confirm/wire the 9 into the daily updater (the "updated" half of the goal);
   the new resolver already makes future pulls correct.

## Reservable collateral (why step 4 needs care)
| license_id | reservable(local) | used by | flip safe? |
|---|---|---|---|
| nlod-2.0 | 0 | ssb only | YES |
| surs-terms | 0 | stat_slovenia only | YES |
| opendata-swiss-by-ask | 0 | bfs only | YES |
| NEEDS-REVIEW | 0 | 9 (8 restricted) | NO — repoint stat_latvia to cc-by-4.0 |
| cc-by-sa-4.0 | 0 | stat_estonia + 8 unesco_* | AHMED DECISION |

Deployed production already has denylist.ts un-gating all 9 + D1 reservable=1 (2026-07-21); the
LOCAL catalog.db just lags — see memory `project_redistributability`.
