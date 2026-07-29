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

---

## UPDATE, same day: the diagnosis is worse and the obvious fix is UNSAFE

Before republishing stat_estonia's CSVs I checked which store is authoritative. Do not skip
this step — the flow-grain derive reads the LOCAL parquets, so deriving from a stale local copy
would republish stale CSVs and report success.

**RETRACTED — Finding 1 below is WRONG.** There is NO `[orchestrator] >>> stat_estonia` line in
run 30434871628: the source was never processed by that run. The daily digest reports each
source's LAST RECORDED STATE from the state db, not the current run's activity — so
"+231,757 new rows; csv_derive failed 1415/3437" describes stat_estonia's last ACTUAL run,
around 2026-07-26, which is precisely why R2's parquets carry `LastModified 2026-07-26`. The
rows DID reach R2. Nothing is inconsistent, and the original framing at the top of this file is
the correct one: **1,415 CSVs are stale behind the 2026-07-26 parquets.** R2 is the published
truth for serving; the local divergence is local-only noise and is not served. Left in place
rather than deleted, per the same rule as R145/R146 — a retraction is evidence, a silent edit is
not.

**Finding 1 (WRONG, retained for the record) — the new rows may never have reached R2.** The newest `clean_full/stat_estonia/`
parquet on R2 has `LastModified = 2026-07-26 04:40:34Z`. The run that reported **+231,757 new
rows** ran on **2026-07-29**. A PUT updates LastModified, so on this evidence the merge did not
land in R2 at all. If that holds, the framing "CSVs are stale behind fresh parquets" is wrong:
the PARQUETS are stale too, and the csv_derive failure may be a symptom of the same failed
write rather than a separate publish bug.

**Finding 2 — local and R2 diverge in BOTH directions**, so neither is simply ahead:

| parquet | R2 bytes | local bytes |
|---|---:|---:|
| `majandus.parquet` | 8,686,962 | 8,639,723 (local SMALLER) |
| `Lepetatud_tabelid.parquet` | 23,952,534 | 23,972,427 (local LARGER) |

A local file LARGER than the published one is exactly the condition `make_servable`'s
never-shrink guard refuses to overwrite, and it is refusing for the right reason: something
wrote to one store and not the other.

**SUPERSEDED (see retraction above): the derive IS the right fix, sourced from R2.**
~~Therefore: DO NOT run the flow-grain derive for stat_estonia yet.~~ Either store could be the
stale one, and republishing from the wrong one would overwrite good data with old data while
every presence check passes — the same silent-corruption shape this document is about.

### What has to be established first (in order)

1. **Did the 2026-07-29 merge write anything?** Read the run's own log around
   `[orchestrator] >>> stat_estonia` for the merge/publish lines, and compare its reported row
   counts against `pq.read_table(...).num_rows` on the R2 object. The digest's "+231,757 new
   rows" is a count the fetcher produced; it is not evidence of a durable write.
2. **Reconcile the two divergent files per-key**, not by size. Byte size cannot say which store
   holds more observations — compare row counts and max(obs_date) per series_key.
3. Only then republish, from whichever store is proven complete, and verify both directions.

The wider lesson is the one this file already carries: a source can report new rows, pass the
health gate as `partial`, and leave BOTH its parquets and its CSVs behind — with nothing in the
pipeline comparing what was fetched to what was durably stored.
