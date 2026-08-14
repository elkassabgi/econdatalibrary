# Giants #15–#17 pre-launch cards (measured live 2026-08-14, ~15:10Z)

Measured with the same probes as #14's card (report_metadata + dataset_layout + the
size-cap 400's own "Estimated size" for the FIRST measure). One pull at a time —
launch order after #14: #15, #16, #17.

## #15 US.TradeFoodProcByCat → unctad_tradefoodprocbycat

- Vintage: `10003|2025-03-18T13:57:58`
- Dims: `ProcessFoodCategory.Economy.Partner.Flow` + Year (is_year, not period-coded)
- Measures: 4, ALL sized (decoded 2026-08-14 ~21:15Z; every measure's grid estimate is
  identical — the API sizes the cross-product, not the density):
  - M4023 "Growth rate, year-on-year" — est **581,327,040**
  - M0100 "US$ at current prices" (the VALUE measure — NOTE: in this family M0100 is
    value, M4023 is a growth rate) — est **581,327,040**
  - M5066 "Percentage of total food" — est **581,327,040**
  - M5058 "Percentage of total merchandise trade" — est **581,327,040**
  - Full-dataset ceiling: **~2.33B** grid cells (4 x 581M); real density lower.
- Launch: `python jobs\ingest_unctad_ds.py US.TradeFoodProcByCat >> _ingest_tradefoodprocbycat.log 2>&1`

## #16 US.TradeMatrix → unctad_tradematrix

- Vintage: `10003|2026-06-29T18:08:17` (freshest of the three — updated June 2026)
- Dims: `Product.Economy.Flow.Partner` + Year
- Measures: 3, ALL sized:
  - M5019 "Percentage by destination" — est **2,335,763,200**
  - M0100 "US$ at current prices" (value) — est **2,335,763,200**
  - M5020 "Percentage by product or group of product" — est **2,335,763,200**
  - Full-dataset ceiling: **~7.0B** grid cells — largest measures in the family so far
    (each 2.2x biotrademerch's biggest).
- Launch: `python jobs\ingest_unctad_ds.py US.TradeMatrix >> _ingest_tradematrix.log 2>&1`

## #17 US.TransportCosts → unctad_transportcosts — DECISION NEEDED BEFORE PULL

- Vintage: `10001|2024-04-29T18:25:23` (older vintage series — publisher last updated
  April 2024; annual data, low refresh urgency)
- Dims: `Destination.TransportMode.Origin.Product` + Year
- Measures: **TEN**, decoded + ALL sized at est **4,983,398,316 each**:
  - M1970 "Transport expenditure (US$)"
  - M1960 "FOB value (US$)"
  - M1980 "Per-unit freight rate (US$/kg)"
  - M1985 "Transport work in ton-km"
  - M1986 "Transport work in 1000 $-km"
  - M1991 "Transport cost intensity in US$ per ton-km"
  - M1992 "Transport cost intensity in US$ per 1000 $-km"
  - M2100 "Kilograms"
  - M5070 "Ad-valorem freight rate"
  - M7120 "Unit value (US$/kg)"
- Full-dataset ceiling: **~49.8B** grid cells (10 x 4.98B). Scaling from
  biotrademerch's observed pull rate (~1.06B obs in ~2 days), a FULL pull is on the
  order of **~3 MONTHS**; a 2-3 headline-measure subset (e.g. M1970 expenditure +
  M5070 ad-valorem rate + M1980 per-unit rate) is **~3-4 weeks**. Store in the
  hundreds of GB either way.
- FLAG FOR AHMED before launching: (a) full pull of all 10 measures (~3 months),
  (b) headline subset (~3-4 weeks; ingest supports --measures per ae9fbd183 and the
  spill cache is measure-keyed, so later measures can be added incrementally without
  re-pulling), (c) defer. Do NOT start this pull on autopilot.

## Standing chain (unchanged)

grain depth-1/2/3 over the FULL store → catalogue at the depth D1 affords →
sync_catalog_d1 (R401) → _DOT_TABLE_GRAIN → core.derive_csv --skip-existing on any
relaunch (R427) → fetcher + registry + EXPECTED_SOURCE_COUNT same commit (R347) →
util.ts → deploy from main (R405) → verify_source_served exit 0.
