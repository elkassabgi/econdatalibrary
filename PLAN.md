# Comprehensive Build Plan — Econ-Fin Data Library

*Working name. Prepared 2026-06-01. The serverless, Cloudflare-native build of the
combined free economic & financial data platform — an expansion of HF Data Library.
Supersedes the Postgres/Timescale/FastAPI stack assumed in the earlier planning docs
(`expansion-plan/03-Free-Only-Pipeline-Design.md`, `04-Storage…md`).*

---

## 0. Design goals → how each is met

You asked for **sophisticated, easy to use, flexible — just like HF Data Library.**
Each goal maps to a concrete mechanism (detailed below):

| Goal | How we deliver it |
|---|---|
| **Sophisticated** | Unified cross-source schema · point-in-time vintages (ALFRED-style) · Raw/Clean tiers + quality flags · computed EDGAR ratios · entity graph (CIK↔ticker↔ISIN) · cross-source overlay · per-series DOI/citation |
| **Easy to use** | One clean REST API (JSON/CSV/Parquet) mirroring `api.hfdatalibrary.com/v1` · universal search · copy-as-code (Python/R) · "Cite this" everywhere · bulk bundles · modern charting UI |
| **Flexible** | Connector framework: one folder per source, config-driven (`sources.yaml`) · license-class data plane · schema absorbs any source shape · phased & modular · BYO-key as a future bolt-on |
| **Autonomous** (like HF) | GitHub Actions cron *matrix* → R2 + D1 → Workers + Pages, self-deploying · freshness monitor + status email · no always-on server |

---

## 1. The locked source set

Full machine-readable registry in **`configs/sources.yaml`**. In short:

- **Host (cache + re-serve):** SEC EDGAR, BLS, BEA, Census, Treasury, Federal Reserve
  Board, EIA, USDA, NOAA, FHFA · World Bank (WDI + ESG + Pink Sheet), OECD, Eurostat,
  IMF, ILOSTAT, FAOSTAT, Penn World Table, Statistics Canada, ABS, Bank of England,
  INSEE, Our World in Data, Ember, BIS\* · Wikidata · ECB/Frankfurter, Zillow,
  DeFiLlama · DBnomics\* (accelerator) · **your HF intraday equities**.
- **Drop:** CoinGecko, Alternative.me.
- **Carve-out:** Eurostat (non-EU/some trade), OWID (upstream third-party), FRED
  (discovery-only; exclude "Copyright" series; re-pull PD series from origin).
- \* BIS = **non-commercial only**; DBnomics = **per-series license passthrough**.

**The one rule, enforced in code:** `core/licenses.py` refuses to publish anything
whose license class isn't `reservable: true`. That single gate keeps "one location" legal.

---

## 2. Architecture (serverless, Cloudflare-native)

```
  Free sources ──▶  GitHub Actions (cron, matrix over connectors)
                      for each source:  fetch ▶ normalize ▶ Raw/Clean
                      ▶ validate ▶ license-gate ▶ write Parquet ▶ upsert catalog
                                   │                         │
                                   ▼                         ▼
                          R2 (Parquet:               D1 (SQLite:
                          per-series raw+clean,       series catalog +
                          + bulk bundles)             license registry,
                                   │                  FTS5 search index)
                                   └───────────┬──────────────┘
                                               ▼
                                  Cloudflare Worker  /v1/...      ◀── CDN cache
                                  JSON / CSV / Parquet + bulk
                                  + auto attribution + "Cite this"
                                               ▼
                                  Cloudflare Pages (SPA + SSR series pages)
```

| Layer | Service | Mirrors HF Data Library? |
|---|---|---|
| Scheduler | GitHub Actions cron + matrix | ✅ same model, generalized to N sources |
| Observation store | **R2** Parquet, partitioned `source/series_id/version` | ✅ how you serve 1.5B bars today |
| Catalog + license registry | **D1** (SQLite) + **FTS5** full-text search | ✅ D1 already in use |
| API | **Cloudflare Worker** (`/v1`) | ✅ extend your existing Worker conventions |
| Frontend | **Cloudflare Pages** | ✅ same |
| Email/monitor | Worker cron + Resend | ✅ reuse digest machinery |

**Cost:** dominated by R2 storage of ~130–240 GB → single-to-low-double-digit $/month
at research traffic. No VPS, no managed DB.

---

## 3. Data model (canonical schema, in D1)

```sql
license(license_id PK, name, reservable, commercial_ok, attribution_required, no_modify, url)
source (source_id PK, name, homepage, license_id FK, attribution, terms_url)
series (series_id PK, source_id FK, title, frequency, unit, geography, category,
        license_id FK, start_date, end_date, last_updated, metadata JSON)
series_fts  -- FTS5 virtual table over series.title/category  (universal search)
```
Observations are **NOT** in D1 — they live in R2 as per-series Parquet
(`source/series_id/{raw,clean}.parquet`), looked up by deterministic key from the
catalog. Schema per row: `obs_date, value, version(raw|clean), flags[], vintage_date`.

- **Raw/Clean** = your two-tier model (as-fetched vs validated/normalized + quality flags).
- **vintage_date** = point-in-time / as-first-released (uses SEC `filed` date, release
  dates) — a research-grade differentiator, near-free to capture.
- **EDGAR fundamentals are series too** (`sec_edgar:AAPL:Revenues:USD`), so one API
  serves macro and company financials uniformly.

---

## 4. Connector framework (the flexibility)

Contract in `connectors/base.py`: every source implements `discover()` (list its
series for the catalog) + `fetch(since)` (pull data, incrementally). Adding a source =
adding one folder under `connectors/`. The runner (`jobs/run_connector.py`) does:

```
load connector → assert_reservable(source license) → for each series:
  assert_reservable(series license) → clean → validate → write Parquet to R2
  → upsert catalog row in D1 → record freshness
```

- **SDMX adapter** covers most macro at once (World Bank, OECD, Eurostat, IMF, ECB,
  BIS, ILO, ABS) — one client + per-provider quirks.
- **Per-source adapters** for the rest (EDGAR, EIA, Treasury, BLS, BEA, Census, FHFA,
  Zillow CSV, OWID GitHub CSV, Ember, DeFiLlama, Frankfurter, Wikidata subset).
- **DBnomics** as a breadth accelerator; dedupe vs direct connectors; store per-series license.
- **Incremental ingest is first-class** — initial backfill, then small daily deltas
  (smart ≈ 0.7 GB/day total; naive re-pull would be ~100× more).

---

## 5. Ingestion & autonomy (GitHub Actions)

`.github/workflows/daily.yml` — one cron, a **matrix** over connectors,
`fail-fast: false` so a dead source never blocks the rest. Per-source cadence lives in
`sources.yaml` (daily macro-US, weekly World Bank, monthly Pink Sheet, etc.). The job
writes Parquet to R2 and upserts the D1 catalog (`wrangler d1 execute --remote` or a
Worker admin endpoint), then self-deploys — exactly like your daily IEX pipeline.
Free API keys (BLS/BEA/Census/EIA/USDA/NOAA/INSEE) live in GitHub Secrets, used
server-side; **visitors never need a key.**

**Heavy connectors** (full EDGAR backfill, big Eurostat pulls) can graduate to a
self-hosted runner or a small cron VM if they exceed Actions' 6-hour job cap — but the
day-to-day deltas fit Actions comfortably.

---

## 6. The API & developer UX (easy to use)

Mirror your existing conventions so it feels like a sibling of HF Data Library:

```
GET /v1/search?q=us+inflation                      → matching series
GET /v1/series/{series_id}                          → metadata + license + "cite this"
GET /v1/observations?series={id}&start=&end=&version=clean&format=json|csv|parquet
GET /v1/sources                                     → public source + license registry
GET /v1/bulk/{bundle}.parquet                       → pre-built country/topic bundles
```

- API keys + rate limit (echo your 100/min download limit), JSON/CSV/Parquet, OpenAPI spec.
- **Copy-as-code** snippets (Python/R) on every series — your audience lives in pandas.
- **"Cite this"** auto-generated citation + permalink + (for curated derived sets) a Zenodo DOI.
- **Attribution** auto-rendered from the registry on every response.

---

## 7. The web UI (easy + sophisticated)

Modern SPA on Pages, SSR for series/country/company pages (SEO is the main acquisition
channel). Signature features:

- **Universal search** across every series (D1 FTS5).
- **Series / country / company pages** with charts, provenance panel (source, license,
  last-updated, "free at <source>" where required), and "Cite this".
- **Cross-source overlay** — plot any series against any other (CPI vs gold vs BTC),
  done client-side over the unified schema (no server DB needed).
- **Computed fundamentals** — ratios (margins, ROE, leverage) derived from EDGAR XBRL.
- **Embeddable charts** — drive journalist/analyst backlinks + SEO.
- Keep your clean, minimalist aesthetic.

---

## 8. Licensing & compliance system (product infrastructure)

1. **License registry** (`sources.yaml` → D1) — machine-readable per source/series.
2. **Code gate** (`core/licenses.py`) — server cache refuses non-green classes.
3. **Attribution rendering** — every chart/response auto-emits the required credit.
4. **FRED "Copyright" filter** — exclude any series whose notes contain "Copyright".
5. **Carve-outs honored** — Eurostat non-EU, OWID upstream, per `sources.yaml`.
6. **Public "Data Sources & Licensing" page** — trust + SEO (your methodology habit).
7. **Lawyer review before monetizing** — esp. EU database right + BIS non-commercial.

---

## 9. Brand & relationship to HF Data Library

Keep the **"Data Library"** family. Parent brand (e.g. *Economic & Financial Data
Library*) with **HF Data Library as the flagship intraday dataset inside it**, on a
sibling domain/subdomain, cross-linked. Separate repo + Worker + Pages + D1 + R2 —
same Cloudflare account, same playbook. Don't fold into hfdatalibrary.com (HF = high-
frequency equities specifically).

---

## 10. Phased roadmap

- **Phase 0 — Foundation (NOW, in progress):** scaffold ✅ · license registry ✅ ·
  **SEC EDGAR bulk backfill downloading** ✅ · next: World Bank connector end-to-end
  (fetch→Parquet→R2→D1→`/v1`→a Pages page) to prove the autonomous loop.
- **Phase 1 — Macro beachhead:** World Bank, OECD, Eurostat, IMF + US-origin
  (BLS/BEA/Census/Treasury) + unified search + series pages + API v1 + Python client.
- **Phase 2 — Markets:** EDGAR (fundamentals + filing pointers + 13F + insider) +
  computed ratios; feature HF intraday. *The seat no free aggregator holds.*
- **Phase 3 — Breadth:** rates/FX (Treasury/ECB/NY-Fed/BIS/Frankfurter), commodities/
  energy (EIA/Pink Sheet/USDA), crypto (DeFiLlama), housing (FHFA/Zillow/Census),
  climate (NOAA/OWID/Ember), bulk bundles, embeddable charts.
- **Phase 4 — Depth & monetization:** point-in-time vintages, official-RSS news,
  freemium API + grants/sponsorship (core stays free/CC-BY; no ads).

---

## 11. What's running now / what I need from you

- **Running:** SEC EDGAR bulk download (`companyfacts.zip` 1.39 GB + `submissions.zip`
  1.54 GB) → `data/raw/sec_edgar/`. Resumable, polite, logged.
- **Need from you (when back at a desk, non-blocking):**
  1. Free API keys: **BLS, BEA, Census, EIA** (+ USDA/NOAA/INSEE) — registration links to follow.
  2. **R2 bucket + brand/domain** decision when ready (new bucket vs prefix; new apex vs subdomain).
  3. Confirm the working name `econfindatalibrary` (trivial to rename).
- **Then:** stand up the World Bank connector end-to-end as the Phase-0 proof, and
  start processing the EDGAR bulk into the canonical schema.
