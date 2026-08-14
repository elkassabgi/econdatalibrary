# Giants #15–#17 pre-launch cards (measured live 2026-08-14, ~15:10Z)

Measured with the same probes as #14's card (report_metadata + dataset_layout + the
size-cap 400's own "Estimated size" for the FIRST measure). One pull at a time —
launch order after #14: #15, #16, #17.

## #15 US.TradeFoodProcByCat → unctad_tradefoodprocbycat

- Vintage: `10003|2025-03-18T13:57:58`
- Dims: `ProcessFoodCategory.Economy.Partner.Flow` + Year (is_year, not period-coded)
- Measures: 4 — `4023, 0100, 5066, 5058` (same family shape as #14)
- M4023 estimated obs: **581,327,040**
- Launch: `python jobs\ingest_unctad_ds.py US.TradeFoodProcByCat >> _ingest_tradefoodprocbycat.log 2>&1`

## #16 US.TradeMatrix → unctad_tradematrix

- Vintage: `10003|2026-06-29T18:08:17` (freshest of the three — updated June 2026)
- Dims: `Product.Economy.Flow.Partner` + Year
- Measures: 3 — `5019, 0100, 5020`
- M5019 estimated obs: **2,335,763,200** — 2.2x biotrademerch's value measure;
  largest single measure in the family so far
- Launch: `python jobs\ingest_unctad_ds.py US.TradeMatrix >> _ingest_tradematrix.log 2>&1`

## #17 US.TransportCosts → unctad_transportcosts — DECISION NEEDED BEFORE PULL

- Vintage: `10001|2024-04-29T18:25:23` (older vintage series — publisher last updated
  April 2024; annual data, low refresh urgency)
- Dims: `Destination.TransportMode.Origin.Product` + Year
- Measures: **TEN** — `1970, 1960, 1980, 1985, 1986, 1991, 1992, 2100, 5070, 7120`
- M1970 alone estimated obs: **4,983,398,316** (~4.7x biotrademerch's value measure).
  If the other nine measures are similarly dense the FULL dataset approaches
  ~50 BILLION observations — an order of magnitude beyond anything served, with a
  pull measured in WEEKS at observed descent rates and a store in the hundreds of GB.
- FLAG FOR AHMED before launching: options are (a) full pull of all 10 measures
  (weeks), (b) pull a chosen subset of measures (e.g. the headline cost measures),
  (c) defer. Measure-code decode needed from reportMetadata before that choice —
  do NOT start this pull on autopilot.

## Standing chain (unchanged)

grain depth-1/2/3 over the FULL store → catalogue at the depth D1 affords →
sync_catalog_d1 (R401) → _DOT_TABLE_GRAIN → core.derive_csv --skip-existing on any
relaunch (R427) → fetcher + registry + EXPECTED_SOURCE_COUNT same commit (R347) →
util.ts → deploy from main (R405) → verify_source_served exit 0.
