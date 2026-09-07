# Catalog broadening — wave 1 (2026-06-26)

Made the uncataloged uniform-long sources discoverable + bundleable via one **generic
resolver** (`<source>:<series_key|series_id>`) — no per-source resolver code needed.

## Result
- **Catalog: 33 → 191 sources, 34,368 → 1,271,879 series.** 99.9% carry a real
  min/max obs_date range (from the data, not fabricated). Frequency taken from a
  freq/frequency column where the source has one, else null. Title = the native key
  (no fabricated titles — see the enrichment follow-up).
- **158 sources cataloged** this wave (1,237,511 new series).
- Every newly-cataloged series is bundleable (`econdl.bundle`), cross-section
  queryable (`econdl.fetch`), and served by the dev shim / Worker once cutover ships.

## Deferred — 47 giants (per-series grain is wrong; flow-grain in a later wave)
Each exceeds 50,000 distinct series; per-series cataloging would bloat D1 with
millions of near-structural rows. They remain **generic-resolvable** and source-level
discoverable, just not series-level catalogued yet. Notable: insee_melodi (14.6M),
GATED (5.8M), istat, imf_ifs/mfs/irfcl/GATED/dot/cpis/cdis/bop/gfsr, GATED, vdem,
wid, ilo, GATED, ons_uk, norgesbank, ssb, statfin, ksh_stadat, dst, unsdg, scb, GATED,
stat_{estonia,latvia,slovenia}, un_wpp, unesco_{sci,sdg,natmon}, harvard_atlas,
gapminder, global_findex, cso, ecb_sdmx, adb, bfs, GATED, hagstofa, cepii_gravity,
fao_tp, wto_bat_bv_{m,x}.

## Skipped — 10 relational/wide (need explicit resolvers, not the generic one)
edgar_13f, edgar_insider, edgar_pointers, cepii_baci, cftc, fdic, gleif, insee_bdm,
insee_sirene, worldbank_extra. These have no canonical (key, obs_date, value) shape;
they need bespoke resolvers like the existing relational set (wikidata/fhfa/census/…).

## Flagged data-op — GATED (store schema inconsistency)
`data/clean_full/GATED/` mixes two schemas across its 165 files (some `series_key`,
some `series_id` for what should be one uniform source). The cataloger errored
honestly rather than emit partial rows. Fix = re-ingest GATED to a single uniform
schema, then catalog it (it's high-value — FRED).

## Follow-ups (later waves)
1. **Title enrichment** — extract real human titles from sidecar `<flow>__series.parquet`
   files / source metadata where available, so keyword search beats the current
   series_id-LIKE match. (Search still works today via series_id substring.)
2. **Flow-grain cataloging** for the 47 deferred giants (catalog at indicator/flow
   level, not full per-series).
3. **Explicit resolvers** for the 10 relational sources.
4. **GATED re-ingest** (uniform schema), then catalog.

## Cutover implication
At 1.27M series, do NOT pre-derive 1.27M per-series CSV objects to R2. The Worker
`.csv` endpoint should read the source parquet from R2 and filter at request time
(or derive lazily + cache) for the broadened sources; pre-derived objects remain fine
for the original 33. D1 at ~510 MB needs the paid plan (already in api/DEPLOY.md).
The D1 export + per-series metadata are regenerated at cutover with final scope.
