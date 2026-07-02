# data.overheid.nl — additive source scan (2026-06-18)

Multi-agent discovery + adversarial verification of the Dutch national open-data
portal (CKAN, ~20,730 datasets). Goal: structured, redistributable econ/finance
**time-series** additive to our direct CBS ingest (`cbs_nl`). The portal is
**catalog-only** — ingest from each publisher's own backend, never the portal.

## ✅ Confirmed wins (live fetch + redistribution license + additivity all verified)

### 1. CBS DataDerden — ~800 OData tables (HIGH; near-zero marginal cost)
- **What:** StatLine-style tables CBS publishes *on behalf of others* (Politie, RIVM)
  + CBS sub-programs (Monitor Langdurige Zorg, AZW, Jeugdmonitor), on a table-ID
  space **disjoint** from main CBS StatLine. Strong econ-relevant: police registered
  crime / Prio-1 response times (e.g. `47008NED`, monthly), long-term-care socio-econ
  (MLZ, e.g. `40060NED`), RIVM public-health (e.g. `50141NED`).
- **Access:** OData v3 — `https://dataderden.cbs.nl/ODataApi/OData/<tableId>`.
  Verified: `47008NED` returns 404 on opendata.cbs.nl but **resolves with data** on
  dataderden.cbs.nl → genuinely disjoint from `cbs_nl`. Ignore CKAN front-end URLs
  (e.g. data.politie.nl); use the dataderden OData backend.
- **License:** CC-BY 4.0 (800/800 datasets).
- **Ingest:** add sibling job `cbs_derden` = the existing `cbs_nl` OData ingester with
  base URL `opendata.cbs.nl` → `dataderden.cbs.nl`, driven by a table-ID list
  enumerated from CKAN org `centraal-bureau-voor-de-statistiek-derden`. Filter active
  tables via TableInfos Period/Modified (some Jeugdmonitor tables discontinued).

### 2. DUO school-funding family — persbek + mibek (HIGH)
- **What:** per-school (BRINNUMMER) annual euro funding. `persbek` = personnel
  funding (verified `persbek_bo_03.csv` = 826,254 rows, 2010–2020, 6,930 schools);
  `mibek` = material-maintenance funding (verified `mibek_bo_03.csv` = 231,639 rows,
  2010–2021). Per-BRIN euro panel — CBS only has national aggregates → additive.
- **Access:** direct CSV at `onderwijsdata.duo.nl` (verified HTTP 200, text/csv).
  6 CSVs each (bo_01/02/03, sbo_01/02/03); euro `BEDRAG` lives in the `_03` files.
  Enumerate via `package_show?id=persbek_bo_en_sbo` / `mibek_bo_en_sbo`.
- **License:** CC-BY 4.0.
- **Caveat:** only `_03` files actually downloaded in verify — spot-check the other 5
  + re-confirm year spans at ingest (row counts have grown since metadata).

### 3. Rijksbegroting — Ministerie van Financiën (MEDIUM)
- **What:** article-level central-government budget. `Begrotingsstaten 2013–2026`
  (commitments/expenditures/receipts per chapter+article across 4 budget moments +
  Realisatie) + companions (Apparaat kostensoorten, Financiële instrumenten,
  ICT-uitgaven, Agentschappen, ZBOs). Fiscal appropriations ≠ CBS national accounts.
- **Access:** bulk CSV+JSON at `rijksfinancien.nl`. **Catalog static URLs are STALE
  (404)** — resolve current filenames from
  `https://www.rijksfinancien.nl/open-data/overzicht-datasets` (filenames rotate per
  budget cycle). MinFin has no org slug; harvested under `data-overheid-nl`.
- **License:** CC0 1.0 / Publiek domein.

## 🔎 Investigate further (high value, access NOT yet confirmed)

- **DNB (De Nederlandsche Bank)** — ~120 central-bank/monetary/supervisory series
  (MFI, pension funds, insurance/Solvency II, balance of payments, daily indices
  since 1990). **Highest content value.** License CC-BY 4.0 confirmed. **BLOCKED:**
  catalog host `statistiek.api.dnb.nl` is **NXDOMAIN** (dead in public DNS); the live
  DNB Statistics API requires an **eHerkenning-based "Public" subscription key** — no
  anonymous access. → needs credentials (like the GUS key) before any ingest.
- **Kadaster Vastgoed Dashboard** — monthly NL **house-price index (PBK)**, avg sale
  price, homes sold, mortgage counts/avg/total, building-plot & farmland prices,
  forced auctions. CC0. Excellent finance fit. Resource URLs were landing pages, not
  direct CSVs → confirm the real download URL on the current vastgoeddashboard.
- **CPB Ramingsdata** — macroeconomic **forecasts** (CEP + MEV: GDP, consumption,
  inflation, unemployment, govt balance, 4×/yr). Different class than CBS realized
  data. CSV link was a landing page → confirm direct file.

## ⛔ Checked and skipped (not silently dropped)
- Municipal publishers (Rotterdam/Amsterdam/Den Haag/Groningen/Utrecht/Eindhoven,
  ~2,735): geospatial/local-operational; the few finance datasets duplicate CBS Iv3.
- Utrecht "Financiële detaildata": = standard CBS Iv3, not additive.
- CBS DataDerden `45006NED` (municipal Iv3): valuable but it's a CBS product.
- RDW (69): operational per-license-plate vehicle data, not econ time-series.
- ProRail (13): railway GIS layers.
- nationaalgeoregister-nl (8,026): purely geospatial (WMS/WFS/SHP).
- cbs-microdata (1,313): restricted, "Geen open licentie", NON_PUBLIC.

## Verification method
6 parallel discovery agents (per publisher/cluster) → adversarial verify (each
candidate's API/bulk URL independently fetched + license read verbatim) → synthesis.
14 candidates assessed; only live-confirmed ones recommended.
