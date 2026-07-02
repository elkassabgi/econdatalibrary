# Title enrichment — wave 3 plan (the long tail)

**Status:** PLAN, data-backed by a full-catalog scan on 2026-06-27. Execution is
gated on official-label fetches + adversarial audit (see §4) — NOT rushed overnight,
because composing wrong official-looking titles is worse than honest raw keys (the
wave-2 `sipri` rejection is the cautionary case). Companion to
[BROADENING.md](BROADENING.md) follow-up #1 and [TITLE enrichment waves 1–2].

## 1. The gap (measured, full catalog — not sampled)
**130 sources, 699,314 series still carry a raw key as their title** (title == the
series_id with the `<source>:` prefix stripped). These are not keyword-searchable by
title — search falls back to series_id-LIKE. Waves 1–2 already titled the high-value
core (33 sources incl. imf_gfse, gppd, barro_lee, pip, damodaran, who_sdg, fao_qcl,
imf_weo, unctad_rfia — all rich), so this is the *long tail*, but it's a big one.

## 2. Clusters (by where the OFFICIAL labels live — all external; verified NOT in-store)
`_provider.json` holds provider metadata only (name/license/citation), NOT codelists.
The good wave-1/2 IMF titles were composed from SDMX DSD codelists fetched from
api.imf.org. So every cluster below needs an external official-label fetch:

| cluster | sources (raw) | series | official label source |
|---|---|---|---|
| **IMF SDMX** | ~30 `imf_*` (gfsssuc 36.9k, gfsfalcs 20.2k, fsire 18.6k, psbsfad 14k, fas 14k, pgi 8.9k, bopagg 7.8k, …) | ~190k | api.imf.org SDMX 2.1 DSD/codelists (`dataflow/IMF.STA/<CODE>?references=all`). **Proven**: 4 GFS siblings already done this way. |
| **UNCTAD** | ~40 `unctad_*` (tabbapotta 29.4k, gdpgbtoevbkoeatasa 21.2k, sbtisvsaga 7.9k, …) | ~150k | UNCTADstat SDMX / data API dimension labels |
| **FAO** | ~25 `fao_*` (fo 16.7k, ga 15k, ge 11.8k, gt 10.5k, …) | ~120k | FAOSTAT definitions API (element/item/area codelists) |
| **WTO** | `wto_hs_a_00{10..40}` (6×~21.8k), `wto_its_mtv_*` | ~140k | WTO HS product-code descriptions + WTO API |
| **UNESCO** | unesco_inno 18.9k, film 8.5k, dem 7.1k, cltt 6.2k | ~41k | UIS SDMX codelists |
| **research / indices** | cow 20k, polity 5.7k, sipri 1.9k, freedomhouse, fsi_fundforpeace, idb, oxcgrt, ggdc, ipea, yale/epi-adjacent | ~60k | each dataset's own codebook (heterogeneous) |
| **central banks** | boc 12.9k, bundesbank 6.9k, rba 3.8k, snb, nbp, tcmb, riksbank, cnb, bcb, bcrp, nyfed, cboe, ofr | ~40k | each CB's series-name API/dictionary |
| **other** | ei_statreview 18.5k, irena 10.8k, edgar_jrc 3.7k, comtrade, stats_nz, insee_sdmx | ~40k | per-source metadata |

(Counts from the full scan; cluster subtotals are the sum of the per-source raw counts.)

## 3. Method (reuse the proven wave-1/2 pattern)
For each source: read its series_ids from `data/catalog.db` (cheap — they're already
cataloged), fetch the source's official codelists, compose `{series_id: title}` by
mapping each series_key dimension to its official label in the established sibling
style (e.g. `imf_gfse:…A.AD.S13.XDC.1A_S1_G26` → "Grants expense to int orgs, General
government (Domestic currency) - Andorra"), write `dist/titles/<source>.json`. The
honest fallback when a code has no official label is to leave that series raw — never
invent a label.

## 4. Adversarial audit (REQUIRED gate before any apply)
Independent verifier per source: re-checks a sample of composed titles against the
fetched codelists — every dimension code maps to its claimed label, **units are
correct** (the sipri lesson: SIPRI milex is millions, not "US$ billions"; check base
year), country/area names match. A source is applied ONLY with zero audit defects;
flagged sources are fixed or left raw. This is why wave-2 correctly did NOT ship sipri.

## 5. Execution shape (ultracode workflow, when run)
`pipeline(rawSources, extract→adversarialAudit)`; apply only confirmed → catalog.db →
`core/export_d1_i18n_delta.py`-style UPDATE delta (retarget to `title`) → chunked
`wrangler d1 execute` → rebuild `series_fts` → verify live. Start with the **IMF SDMX
cluster** (largest, proven pattern, codelists confirmed fetchable). Multilingual bonus:
SDMX codelist names carry `xml:lang`, so titled IMF/UNCTAD/UNESCO series can gain
ar/es/fr/ru/zh in the same pass (extends [[project_i18n_coverage]]).

## 6. Recommendation
Run cluster-by-cluster with the audit gate, IMF first, with Ahmed able to spot-check the
first applied batch before the rest. ~700k series is real searchability value but the
wrong-label risk is real — the audit gate, not speed, is the priority.
