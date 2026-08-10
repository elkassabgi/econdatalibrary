# ESG Data Library + Finance Data Library — create plan

Decided with Ahmed 2026-08-09/10. This file is the durable version of that conversation;
update it here, not in chat.

## Decisions already made (Ahmed)

- **Three-way split**: econdatalibrary keeps official economic statistics; a new **ESG Data
  Library** (his name choice — not "Climate": ClimateBERT-derived sustainability measures are
  coming, so ESG is the umbrella and NOAA is its physical-risk pillar); a new **Finance Data
  Library** built on public filings. **HF stays its own library** — it is derived from purchased
  data with a survivorship caveat; mixing provenances muddies both licensing stories.
- **EDGAR moves out of econ into Finance** (sec_edgar + the 13F/insider backfills).
- Tadawul's terms **permit redistribution** per Ahmed's reading — the verbatim clause + URL
  still needs to land in DATABASE_LICENSES_VERBATIM.md (owed; ledger R416 records why no
  exchange verdict may ever again be asserted without a quote).

## Measured foundations (2026-08-10)

- 57.3% of econ's catalogue rows are ESG-adjacent (6,543,983 of 11,421,296; 51 sources).
  noaa alone = 3,137,871 rows (27.5% of the catalogue) but only 0.69% of the library's
  79.8B observations — row-expensive, observation-cheap, which is why it shards first.
- Finance-adjacent inside econ is only 6.3% (25 sources / 721,564 rows) and is mostly IMF
  BOP/FSI/central-bank data that IS economics — Finance is therefore built on filings we
  already hold, not carved out of econ's macro-financial series.
- D1 primary measured 9.35/10 GB, 819 B/row. The shard (step 0) drops it to ~6.78 GB and
  unblocks bea (912,990 rows) and fdic (298,869 rows).

## ESG Data Library

**Step 0 — data backbone (EXECUTING).** Second D1 `econ-catalog-climate`
(e34114f2-c0be-43d9-bcb5-798a3952414c) on the same account/worker. noaa's 3,137,871 catalogue
rows migrating (tools/migrate_noaa_shard.py: emit EXACT -> resumable push -> verify -> worker
routing -> live smoke -> only then delete from primary). No new domain, no tokens, no billing.

**Step 1 — public face (needs Ahmed: domain purchase + Pages attach, a dashboard click).**
Site scaffolded from econ's frontend; same worker API with an ESG-filtered catalogue view.
A presentation layer, not new infrastructure. SSO: bind the existing USERS db + auth.ts;
Ahmed's one console step is registering the new redirect URIs on Google/ORCID.

**Step 2 — content.** Immediately: noaa, eia, fao_* environment/agriculture, unctad
environmental-goods, wid (inequality = the S), WHO/UNESCO/SDG sets — all already served.
Then the differentiator: **ClimateBERT-derived sustainability measures** over SEC filings
(public domain, safe) and papers (ship SCORES, never the corpus — papers are copyrighted).
Derived measures get their own provenance lane from measure #1: model checkpoint, version,
config, and the exact input document set ship with every number, or it is not reproducible
and not citable. Do NOT retrofit the fetched-source _provider.json shape.

## Finance Data Library

**Step 1 — seed with what we already hold.**
- sec_edgar moves over (Ahmed's decision) + edgar_13f_backfill / edgar_insider_backfill
  outputs (already on disk in hfdatalibrary/pipeline).
- fdic: 19,918,427 obs / 298,869 series of US bank call reports, fully downloaded, 0
  catalogued — serves immediately after the shard frees D1 headroom (task #129).
- ofr; gleif's 3,383,323-row LEI entity registry once the "what is a lookup table here"
  design question is answered (it has no obs_date/value — do not force series shape).

**Step 2 — expansion.** More SEC document types; then exchanges ONE at a time, each with its
licence quoted verbatim in DATABASE_LICENSES_VERBATIM.md BEFORE any crawler is written —
Tadawul first (permissive per Ahmed; clause pending). Never assume restrictive OR permissive:
R416 was a warning asserted backwards.

**Same pattern as ESG**: domain + Pages attach when ready; SSO reuse; no API tokens needed —
deploys ride the existing wrangler OAuth.

## Sequencing logic

Shard first (it is the backbone of both libraries AND the D1 fix); domains whenever Ahmed is
ready — nothing blocks on them, they are a presentation decision. The stats identity across
the family is unchanged: observations move between labels, never disappear (noaa's 553M obs
count under ESG instead of econ).

## What needs Ahmed, exhaustively

1. Buy domain(s) + attach to Pages (dashboard click) — when he wants the public faces.
2. Register new-site redirect URIs on Google/ORCID (family SSO console step).
3. Paste the Tadawul permissive clause + URL for DATABASE_LICENSES_VERBATIM.md.
Nothing else: no API tokens, no billing changes for step 0-2 infrastructure.
