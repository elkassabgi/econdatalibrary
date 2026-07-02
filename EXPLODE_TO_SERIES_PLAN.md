# Plan — Explode the catalog to series grain (apples-to-apples with DBnomics)

**Goal:** make econdatalibrary's public series count reflect the individual series we
actually hold (~1 billion, pending the census), the same way DBnomics counts its 1.72B —
because the data is already on disk; today's 1.37M is a *cataloging* artifact, not a data gap.

## The hard constraint that shapes everything
**Cloudflare D1 cannot hold ~1B catalog rows.** D1's max database size is ~10 GB; our current
1.37M-row catalog is already ~1.6 GB. A full per-series explosion (~1B rows + titles/metadata)
would be ~100× over the ceiling. So "just insert every series into D1" is infeasible.

Fortunately, **DBnomics doesn't statically materialize 1.72B pages either** — it *resolves*
series on demand from per-dataset structures. We should do the same: keep the catalog at
dataset grain, but (a) *report* the true series count, and (b) *resolve* individual series on
demand. That's both honest and architecturally sound.

## Phased plan

### Phase 0 — Truthful headline number ✅ SHIPPED 2026-07-02
Census measured **7,730,440,157 individual series / 79,782,631,887 observations**
(`_series_census_hll.py` → `_series_census_hll.json`; eurostat reconciled: store has
15,390 datasets vs 7,637 cataloged). Live on worker `/v1/stats`; site hero + gen_site.py
updated ("7.7B+ / 79.8B / 309 sources" + methodology line).
- We already store `n_series` per grouped dataset (verified exact: `aact_ali01` → 111 = 111),
  and the running census counts distinct `series_key` for the rest. Sum = the real total.
- Surface it on the site: "**N individual series across M sources**" with a one-line
  methodology note ("series = every fully-specified individual time series, counted from the
  data; datasets are indexed at group grain"). This is the apples-to-apples number, defensible
  because the observations back it (e.g. eurostat: 8.58B obs physically stored).
- Effort: tiny — a number + a stats endpoint field. **This alone closes the optics gap honestly.**

### Phase 1 — Series addressing / drill-down (no D1 explosion)
- On a dataset's page, let users expand to its individual series. The worker resolves the
  dataset → its series list on demand from either:
  - the parquet's distinct `series_key` + dimension columns (the resolver already reads these), or
  - a small per-dataset **series manifest** in R2 (`series-index/<dataset>.json`: series_key,
    dimension labels, start/end, n_obs), generated once by a batch job.
- This gives real per-series browse/addressing at ~1B scale with only ~50k manifest objects
  (one per dataset), not 1B D1 rows.

### Phase 2 — Series-level search at scale (only if needed)
- Do NOT force 1B rows into a search index. Options, cheapest first:
  1. Keep the existing dataset-level FTS + add **dimension faceting** (filter by geo/unit/etc.)
     so a user narrows dataset → series without a 1B index. (Recommended first.)
  2. If true free-text search over individual series is required, build the index over
     **dimension-label tuples** (bounded, far < 1B distinct strings) rather than every series.
- Measure demand before building; dataset+facet search likely suffices.

### Phase 3 — Per-series download (already unblocked)
- `core/derive_csv.py` (bug fixed this session; proven on `abs`) produces `series/{id}.csv`.
- Serve on demand (derive-on-first-request + cache to R2) for the long tail; precompute only
  popular/curated series to bound R2 object count. Do NOT precompute 1B objects up front.

## Data model / honesty
- Per-series titles must come from **official dimension member labels** (in the parquet/DSD),
  never machine-invented — same rule as the catalog titles. Where labels are missing, show the
  raw dimension codes rather than fabricating prose.
- Keep provenance + license per series (inherited from the dataset).

## Effort & risks
- **Phase 0:** hours. **Phase 1:** days (batch manifest job + worker drill-down + UI).
  **Phase 2/3:** weeks, demand-driven.
- Risks: D1 ceiling (designed around); title honesty (use official labels only); compute to
  enumerate ~1B series once for manifests (bounded, one pass over parquet — same as the census);
  R2 object growth (mitigated by manifests-per-dataset + derive-on-demand, not 1B static CSVs).

## Recommendation
Ship **Phase 0 now** (honest ~1B headline — closes the optics-and-substance gap the moment the
census confirms the number), then **Phase 1** for real drill-down. Treat 2/3 as demand-driven.
This matches how DBnomics actually operates (resolve, don't statically materialize) while
staying within Cloudflare's limits and our honesty rules.
