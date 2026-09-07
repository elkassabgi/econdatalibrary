# Flow-grain cataloging for the 47 deferred giants — proposal

**Status:** DESIGN PROPOSAL for Ahmed's sign-off. Needs three decisions (§6) before
build. No live changes made. Companion to [BROADENING.md](BROADENING.md) follow-up #2.

## 1. The problem
The 47 giants each exceed 50,000 distinct series; together they hold the bulk of the
~20M+ series in `data/clean_full` (e.g. vdem 77.4M rows, imf_ifs 13.5M, GATED 8.2M,
insee_melodi 14.6M series). Per-series cataloging would push D1 from ~0.65 GB to many
GB of near-structural rows — most of which are one country/age/sex cell of the *same*
indicator. They are already **generic-resolvable** (`<source>:<series_key>`) and
source-level discoverable; what's missing is a *browsable, searchable middle grain*
between "the whole source" and "1.3M individual keys".

## 2. The grain: one catalog row per (source, flow)
A **flow** = an indicator / dataflow / dataset — the SDMX "dataflow" or a national
statistics table. One catalog row per flow, NOT per series. Each flow row carries:

| field | value |
|---|---|
| `series_id` | `<source>:<flow_id>` — a *browse* id (e.g. `istat:DCIS_POPRES1`, `GATED:HCF_REL_ELECTRICITY`) |
| `grain` | **NEW column** = `"flow"` (existing rows are implicitly `"series"`) |
| `title` | the flow's OFFICIAL name (SDMX DSD / codelist label; see §4) — never fabricated |
| `n_series` | distinct `series_key` within the flow (measured at build) |
| `start_date`/`end_date` | min/max `obs_date` over the flow (from data, not faked) |
| `frequency` | the flow's freq if uniform, else null |
| `geography` | null at flow grain (it's the varying dimension) |
| category, license_id | inherited from the source |

A flow row is the unit a human searches and cites; its member series are reached by
`econdl.fetch`/cross-section or a new flow-expansion endpoint (§5). The per-series
generic resolver is unchanged for power users who already know an exact key.

## 3. Three archetypes (how `flow_id` is derived)
Verified by inspecting the data layout + parquet schemas:

- **A — `flow` column present** (e.g. insee_melodi: cols `flow, series_key, obs_date,
  value`). `flow_id = DISTINCT flow`. insee_melodi ≈ 71 flows.
- **B — multi-file, file = dataflow** (ecb_sdmx 101 files, istat 755 files,
  wto_bat_*). `flow_id = file stem`; one row per data parquet. No row scan needed for
  the flow list (only for n_series/date-range).
- **C — single-file, flow encoded in `series_key`** (imf_ifs `IMF_IFS:A.1C_355.<IND>`,
  GATED `<IND>:<COUNTRY>`, vdem `VDEM:<var>:<unit>`). `flow_id` = the indicator
  segment of the key. The segment position differs per source, so each needs a
  one-line **flow extractor** rule (derivable from `_provider.json`'s SDMX DSD, which
  names the dimensions). This is the only source-specific code.

Archetype B is the common SDMX case and is trivial; A is trivial; C needs a small
per-source rule (≈ 8–12 of the 47).

## 4. Titles — official, and a free i18n win
Flow names come from each provider's own metadata: the SDMX **dataflow/codelist name**
(`_provider.json` DSD, or a `references=all` fetch), the insee_melodi `*_SERIES`
sidecars, or the file/dataflow id as the honest fallback when no name is published
(same rule as wave-1 titles: **never fabricate**). Because SDMX names carry `xml:lang`,
flow titles can be captured in ar/es/fr/ru/zh *in the same pass* — the flow grain would
be **multilingual from day one** (ties directly into the live `?lang=` work; see
[../api/CONTRACT.md](../api/CONTRACT.md) i18n section and [[project_i18n_coverage]]).

## 5. Search / resolve / UX
- **Catalog search**: flow rows have real titles → keyword search finally beats
  series_id-LIKE for the giants. Results can be filtered by `grain=flow|series`.
- **A flow is browsable, not a single download.** Decision needed on its `.csv`
  (§6.2): either stream the flow's whole long table (it *is* one parquet for B/C, a
  GROUP for A) or return an honest pointer to `econdl.fetch`/`/v1/bundle`.
- **Flow expansion**: add `GET /v1/catalog?flow=<source>:<flow_id>` (or
  `/v1/flow/{id}/series`) listing member series — pure D1/parquet, no new storage.

## 6. Decisions needed from Ahmed
1. **Add the `grain` column** (`flow`/`series`) to the catalog + API responses? It's
   the cleanest way to keep flow rows from being mistaken for downloadable series.
   (Recommended: yes.)
2. **Flow `.csv` behavior** — stream the entire flow table, or refuse with a 400-style
   pointer to bundle/fetch? (Recommended: stream for archetypes B/C where the flow is
   one bounded parquet; pointer for the largest, e.g. vdem 77M rows.)
3. **Fetch official flow NAMES now** (adds a bounded SDMX-DSD metadata pass per source,
   multilingual) vs. ship flow-id titles first and enrich names in a follow-up?
   (Recommended: fetch names — it's the difference between `istat:DCIS_POPRES1` and
   "Resident population" and it's where the real value is.)

## 7. D1 size impact (estimate — exact counts measured at build)
Flow rows replace millions of series rows with **order 10k–60k rows total** across the
47 giants (anchors: istat 755, ecb_sdmx 101, insee_melodi 71 measured today; imf_ifs
~2.5k indicators, GATED ~2.3k, vdem ~0.5k — *estimates to confirm at build*). This is
a rounding error on the current 0.65 GB D1 and keeps search fast.

## 8. Build plan (resume-safe, no creds, doesn't touch backfills)
1. `core/catalog_flows.py` — per-archetype flow extractor (A: distinct `flow`; B: file
   stem; C: per-source key rule from DSD), emit flow rows with measured n_series +
   date range. Resume-safe per-source commit, mirroring `core/broaden_catalog.py`.
2. Optional names pass (decision §6.3): SDMX DSD/codelist names incl. `xml:lang`.
3. Regenerate the D1 export delta + push (reuse the chunked `wrangler d1 execute`
   path; flow rows are tiny). Add `grain` to FTS/`series` shape behind the existing
   byte-identical-contract discipline (new column, default `series`, so current
   responses are unaffected).
4. Conformance test: a flow row is searchable, carries a title, and its `.csv`/expansion
   behaves per §6.2.

The 10 relational sources (edgar_*, cepii_baci, cftc, fdic, gleif, insee_bdm,
insee_sirene, worldbank_extra) are OUT of scope here — they need bespoke resolvers, not
flow-grain (BROADENING.md follow-up #3).
