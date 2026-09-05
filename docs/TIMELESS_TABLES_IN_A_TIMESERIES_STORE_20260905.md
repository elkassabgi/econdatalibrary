# Cross-tabulations catalogued as time series (found 2026-09-04/05)

**Status: DELISTED AND VERIFIED 2026-09-05 ~02:05** (Ahmed: "ok delist"), by
`tools/delist_timeless_tables.py --apply` after adversarial review of the tool: D1 `series` 27 → 0,
`series_fts` 0 matching (MATCH-title predicate), R2 31 objects deleted (27 served CSVs, content-verified
to carry impossible dates before deletion, + the 4 cbs_nl store parquets that also lived on R2),
local `catalog.db` 27 → 0, `source_counts` refreshed, the 4 cbs_nl parquets archived to
`data/archive/timeless_20260905/`, live `metadata.json` 404 on 27/27. Edge-cached browse pages and
`/v1/stats` may list the 27 for up to 6 h after. Original status line: *DIAGNOSED across two
sources, 27 tables; the only remedy is a delisting.*

*Adversarially reviewed 2026-09-05 (subagent, told to find the flaw). Two claims FAILED and are
corrected below in place: the delist mechanism I first named (`broaden_catalog`) would never reach
these rows, and "no third option" was asserted without checking the publishers' metadata. Numbers
that had no instrument are now either instrumented or marked as such.*

## The class, in one sentence

An older reader, given a publisher table with **no time dimension**, invented one — a positional
counter over the table's rows, which is why the fabricated dates start at year **0001** — and the
resulting rows were catalogued, derived and served. The store has since been rebuilt correctly
*without* those tables, but the catalogue rows and the served CSVs were never removed.

## Evidence — stat_slovenia, 23 tables (`05W0101S` … `05W0609S`)

| layer | state |
|---|---|
| catalogue (2026-08-16 snapshot, PK-range query — reviewer) | exactly 23 rows `stat_slovenia:SI:05W0101S … 05W0609S`, all `start_date = 0001-12-31`; end dates **6152-12-31 ×4, 4481 ×11, 4441 ×1, 2735 ×7, 2456 ×1**; every title *"… census 2002"*. `05W0101S`'s `NASELJA` (settlements) dimension has **n = 6152** — the end date is the positional counter, exactly |
| local store `clean_full/stat_slovenia/05W.parquet` (pyarrow — reviewer) | 10,031 bytes, mtime 2026-08-07 18:27 UTC; **1,463 rows**, `1953-12-31 .. 2002-12-31`, 630 series keys over 10 tables (`05W1301S 1401S 1501S 1601S 1605S 1606S 1607S 1701S 1801S 1802S`) — **0 rows for any of the 23** |
| R2 parquet mirror of the same file | read via boto3 on 2026-09-04: same row count, date range and zero out-of-range dates. The reviewer verified the LOCAL file only; re-read R2 before acting |
| served CSVs | **23 of 23 broken** on 2026-09-04 (e.g. `05W0101S`: 5,863 rows across years `0001, 0002, 0003…`). Measured by fetching each served CSV; that script was not retained — **re-measure before any delete** |
| timing | served CSVs written 2026-08-07 16:16 UTC (R2 object metadata); clean store written 18:27 UTC the same day, from a rebuild that dropped these tables |

*Six sibling tables, `05W0701S–05W0903S`, are also census-2002 cross-tabs without time and are absent
from BOTH the store and the catalogue — correct state, no action (reviewer's consistency check).*

**The publisher settles what they are.** `https://pxweb.stat.si/SiStatData/api/v1/en/Data/{id}/`,
read 2026-09-05 for **every table whose id starts `05W0`** in SURS's own listing (`_catalog.json`,
4,696 tables) — 29 tables, a superset of the 23 catalogued orphans — plus one control:

```
SUMMARY over 29 '05W0*' tables: no-time=29  has-time=0  errors=0      (D:\temp\claude\_surs05W0.log)
```

| table | title | variable flagged `"time": true` |
|---|---|---|
| `05W0101S` | Families, census 2002 by SETTLEMENT and FAMILIES | **none** (2 vars) |
| `05W0201S` | Households, census 2002 by SETTLEMENT and HOUSEHOLDS | **none** (2 vars) |
| `05W0609S` | Buildings with dwellings, census 2002 by MUNICIPALITY … | **none** — its *"LETO ZGRADITVE STAVBE"* is **year of construction of the building**, an attribute |
| … all 29 `05W0*` | census 2002 cross-tabulations | **none** |
| `05W1301S` *(control — IS in the clean store)* | Families, censuses 1991, 2002 by MEASURES … | **none either** — but `LETO` = [1991, 2002] |

The control corrected my first reading. SURS does **not** set the `time` flag on `05W1301S`, yet it
is in the store; so the store's inclusion rule is not the publisher's flag. It is the ingest's own
resolver, quoted from `jobs/ingest_stat_slovenia.py:160-166` and `:201-202`:

> `meta_time_code` is the AUTHORITATIVE time dimension id from the PxWeb metadata (the variable
> flagged `time: true`). When provided the shared resolver locks onto that dimension instead of
> guessing. Falls back to the value-first selection (highest date-parse-rate, literal name only as
> a last resort) when the flag is absent
>
> Pick the time dimension via the shared value-first resolver (core/pxweb.py): authoritative
> `time: true` / role.time, else highest date-parse-rate, else name.

with `is_time_dim` (`:136-147`) anchoring "parses as a date" on a **sane year range** so that
category codes like `1000/2000/…/6000` no longer read as years. Under that rule `05W1301S`'s LETO
values 1991 and 2002 parse at 100 % and win; the 23 orphans offer only settlement codes and category
codes, which parse at 0 %, so they yield no rows. The old reader had no sane-year guard, which is
exactly how a positional code became year 0001. The store is right. The catalogue and served files
are the residue of the reader that was wrong.

`05W0609S` is the R334/R703 trap in miniature: a dimension literally named "year" that is not
*when* but *what*. A reader that matches on the word "year" would fabricate a time series from it;
the current resolver tries the literal name only *last*, after the flag and the parse-rate test,
and this table's construction-year buckets do not out-parse its other dimensions.

## Evidence — cbs_nl, 4 tables (`70169NED`, `70170NED`, `70167NED`, `81823NED`)

Same class, found earlier the same day. Their forced re-pull returned, verbatim:

> `SKIP 70169NED: no period column in 9 columns (CBS declares no TimeDimension for it — it is a
> cross-tabulation, not a time series, so it has no observations to contribute)`

and recorded them in `_repull_refused.json` as `undatable`. The crawler is right to refuse: it will
not manufacture dates. But refusing means the fabricated rows they already carry (years 4549, 6064,
8589, 9597 were observed; **the row count has not been measured** — measure it before acting) are
frozen in place. A fifth table, `37471`, *did* have a real time axis
and was fixed cleanly — store, catalogue, D1 and served now all read 1991–2009.

## Why re-deriving cannot fix these

`core/derive_csv.py` resolves a catalogue id against the store. For all 27 the store has **no
rows**, so a re-derive reports them unresolvable and writes nothing — it cannot replace the broken
served file with a correct one, because there is no correct one to produce. **A time-series store
cannot truthfully represent a table that has no time.**

## The decision (Ahmed's — it is a deletion)

For all 27, the choices:

1. **Delist**: remove the catalogue rows (local + D1) and the served CSVs on R2. This is the honest
   state: the store already does not contain them.

   *Mechanism — corrected by the review.* NOT `core/broaden_catalog.py`: its line 110 skips every
   source that is already catalogued (`if … d in cataloged or d in PROTECTED: continue`), so it never
   reaches its `DELETE` for stat_slovenia, and cbs_nl is on its `PROTECTED` list (line 28). The
   cataloguer that owns stat_slovenia is **`tools/catalog_pxweb_flowgrain.py`** (SOURCES, line 34;
   store-derived `DELETE`+reinsert, lines 150–154). Running it drops the 23 orphans — at two costs
   that must be accepted knowingly: (a) it rebuilds **all 4,134** stat_slovenia rows with titles
   from `_catalog.json`, **reverting** whatever `tools/apply_series_names.py` applied on 2026-08-16;
   (b) lines 160–162 do a whole-catalogue FTS `DELETE`+rebuild on the live `catalog.db` while two
   crawlers write to it. A targeted `DELETE … WHERE series_id IN (23 ids)` avoids both costs and is
   the better instrument. Either way **D1 needs its own step**: `core/sync_catalog_d1.py:119` treats
   a row absent locally as "nothing to advertise; skip quietly" — a local delete does not propagate.
   Served CSVs on R2 are a third, separate delete. The 4 cbs_nl tables need the same three steps.

2. **Keep** serving fabricated dates on 27 tables.

3. **Single-dated representation** — named because the review showed it is conceivable, and
   rejected: CBS's `TableInfos` carries a `Period` field (`70169NED` → `'1998'`; `81823NED` →
   `'2000 - 2011'`), so a one-observation series dated to the reference period could be built —
   but for `81823NED` the period is a **range**, so a single date would itself be a fabrication;
   SURS's metadata has only `title` and `variables`, so its reference date exists only as the words
   "census 2002" in a title — the R334/R703 string-parsing class; and no code path does this today
   (`ingest_stat_slovenia.py:215-216` returns empty when no time dimension resolves; `core/pxweb.py:314`
   returns `None`; `ingest_cbs_nl.py:1210-1219` SKIPs). A one-point "series" in a time-series
   library adds no research value that the publisher's own table does not already offer. Not
   recommended.

No re-pull, re-derive or re-catalogue repairs these, because every one of those needs a time axis
the publisher does not supply.

## What the fleet-wide catalogue audit will add

A full scan of the catalogue for `start_date < 1500-01-01 OR end_date > 2200-01-01` was still
running when this was written. Its per-source breakdown is the census of this class: any source
it lists beyond stat_slovenia and cbs_nl is another candidate for the same diagnosis — check the
publisher's time dimension before assuming a repair is possible.

## A gap this exposed in the tooling

`tools/audit_impossible_dates.py` audits **stores**. It reported stat_slovenia as *"1 file, 3 rows
at 1000-12-31"* — true of the store — and was structurally blind to the 23 **catalogue** rows and
23 **served files** carrying year-0001 dates. The catalogue is what `metadata.json` and browse show
users; the served CSV is what they download. A store-only audit can pass while both user-facing
surfaces are wrong. The check that found this was a direct catalogue query, and it belongs in the
tool.
