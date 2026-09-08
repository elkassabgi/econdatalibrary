# GUS DBW — completeness repair (2026-06-18)

## The bug (silent data loss)
`jobs/ingest_gus_dbw.py` could silently drop data on slow/large variables:

1. In `fetch_year`, a page that timed out made `api_get` return `None` (same as a
   genuine "no data" response). The loop then `break`-ed and kept only the
   partial rows it had already fetched — the rest of that (year, period) was lost.
2. In `main`, an area was **finalized and marked done even if a section failed**,
   so the truncated section was merged into `area_<id>.parquet`, its parts were
   deleted, and it was **never retried** → permanent hole.

Observed trigger: variable **581** (area 15, ≈343k obs/yr) timing out on the old
120 s read timeout. 109 timeout events across Jun 10–17.

## The fix (code)
- `api_get` now **raises `TransientError`** when retries are exhausted on a
  transient failure (timeout / 5xx / network / persistent 429), and returns
  `None` ONLY for a definitive empty response (HTTP 400/404/422). A timeout can
  no longer be mistaken for "no more pages."
- Read timeout 120 s → **300 s**; retries 4 → **6**; page cap 300 → **2000**.
- `main` finalizes an area **only if every section succeeded** (`area_complete`).
  Any transient failure leaves the section parts in place and does NOT mark the
  area done, so the next run retries it. Completed parts are skipped on restart.
- Writes `logs/gus_dbw.DONE` only when **all** areas complete (so the watchdog
  stops relaunching only when the crawl is genuinely finished).

## The repair (data)
Audited every timed-out variable → mapped to its area → flagged finalized areas.

- **Clean, kept:** areas 7, 9, 11, 13 (no timeouts during their processing).
- **Re-fetching (had timeouts under buggy code):** areas **3, 5, 6, 8, 10, 12**.
  Their `area_*.parquet` finals were deleted and their sections removed from the
  checkpoint (`done_sections` 136→35, `done_areas` 10→4) so they re-crawl clean.
  Backup: `data/clean_full/gus_dbw/_checkpoint.json.bak_20260618_065041`.
- Area 15 (var 581) was never finalized (parts only) → no truncation baked in;
  continues normally.

## Run state
- GUS relaunched on the fixed code at the **registered (X-ClientId) tier**
  (9 s spacing), re-fetching area 3 first. Log: `logs/gus_dbw_fixed_0618_0651.log`.
- Watchdog: one instance (RELAUNCH_GUARD_LOOP, PID was 15924) — relaunches on the
  fixed code if the process dies; stops when `gus_dbw.DONE` appears.
- CBS NL and DBnomics ISTAT untouched, still running.

## To verify when GUS finishes
`gus_dbw.DONE` will exist and the log ends with "all areas complete". Then run a
full Parquet recount before the grand-total update.
