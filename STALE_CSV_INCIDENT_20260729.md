# Served CSVs are stale for 1,471 tables/series — csv_derive is failing silently

*Found 2026-07-29 from the scheduled run 30434871628 (06:00 UTC job, started 08:16Z).*

## What the run reported

The daily digest, not the health gate:

    !! owid          partial  last_obs=2100-12-31  err=+14583 new rows; csv_derive failed 56/64 series
    !! stat_estonia  partial  last_obs=2026-12-31  err=+231757 new rows; csv_derive failed 1415/3437 series

So both sources FETCHED and MERGED new data into their parquets, then failed to rewrite the
per-series CSVs that are what users actually download. **1,415 of stat_estonia's 3,437 tables
(41%) and 56 of owid's 64 series are now serving values older than the store holds.**

## Why this is the dangerous class, not a cosmetic one

This is the `fao_oa` failure recorded in `tools/make_servable.py`: there, all 1,388 CSVs were
PRESENT, so every existence check passed and the verify printed OK — while 69.6% of served
observations differed from the published parquet by up to 460%, because the CSVs predated a
republish by 26 days. Presence is not currency. A stale CSV carries plausible dates and
plausible values; nothing date-based can see it.

**Neither source was escalated by the health gate.** It failed the run on `bcrp` (RED-DATA),
`wid` (RED-UNRUN) and `riksbank` (ATTENTION), while `owid` and `stat_estonia` passed as
`partial`. A source that fetched new rows and then failed to publish 41% of them is arguably
worse than a source that simply had nothing new, and the gate ranks it lower.

## The other three, correctly diagnosed

| source | gate | what is actually true |
|---|---|---|
| `wid` | RED-UNRUN | `PROTECTED, not attempted this run — in-flight backfill (FIRSTPASS_DIRS)`. Now unpinned, and the resume fix + 60-min cap land on the next cron. Expected to clear. |
| `bcrp` | RED-DATA | Runs clean in 5s, 0d since success, but latest obs is 2026-07-22 — **7 days stale on a DAILY cadence**. The fetcher succeeds and returns nothing new; either BCRP has not published or the date-tail logic is missing observations. |
| `riksbank` | ATTENTION | Fetched +1,698 new rows in 999s, then `csv coherence partial: 28 changed keys unmapped`. Smaller version of the same publish-side gap. |

## Suggested order of work (not started)

1. **stat_estonia** — 1,415 tables serving stale data is the largest user-visible defect here.
   Get the actual derive error; the likely cause is the one `make_servable` documents, where
   the CSV resolver reads the LOCAL store and under the r2 backend that holds only what the
   current process wrote, so every unsynced series fails with "zero rows matched".
2. **owid** — same failure, 56/64, much smaller blast radius.
3. **Make the health gate rank this class properly.** `csv_derive failed N/M` after a
   successful merge should not pass as `partial` while a no-new-data source goes RED. Failing
   to publish data you just fetched is a serving defect, not a soft warning.
4. **bcrp** — establish whether 7 days without an observation is upstream or ours.
