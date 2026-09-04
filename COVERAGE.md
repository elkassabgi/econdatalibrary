# econdatalibrary — Coverage scoreboard (measured)

**Directive:** FULL coverage of EVERY source. No trimming, no skipping, however long it takes.
**Numbers measured from Parquet footers (not agent claims).** Last count: 2026-06-04 (session-2 final).

Legend: ✅FULL · 🟡PARTIAL/growing · 🔗REFERENCED · ⛔BLOCKED(keys)

| Source | Status | Measured obs | Note |
|---|---|---|---|
| statcan | ✅ | 56,845,453,642 | ALL 8,207/8,207 cubes done (5 with no data = fail markers). 56.8 billion observations. |
| eurostat | ✅ | 6,145,322,377 | all 7,637 datasets |
| oecd | 🟡 | 6,004,142,903 | 1,406/1,509 files. Fresh pass running (102 remain, mostly 404 empties). |
| hf_equities | 🔗 | 1,498,837,188 | 1-min clean referenced in R2 (other tiers not bridged) |
| abs | ✅ | 975,186,637 | all SDMX dataflows |
| noaa | ✅ | 549,412,914 | GSOM 127,905 stations + GSOY 86,018 stations (100% both). Re-measured 2026-08-01: 549,412,914 OBSERVATIONS over 3,135,873 series. The old 552,553,147 counted the __series.parquet sidecars alongside the observation shards — obs + 3,135,873 sidecar rows = 552,548,787, which is what a whole-directory row count returns. |
| bls | ✅ | 482,052,185 | 63/69 surveys (6 legitimate empties: compressed, sdmx, yy, esbr, nc, pb) |
| ilostat | ✅ | 388,164,886 | = full TOC (1,947 indicators) |
| eia | ✅ | 316,437,940 | 26/26 bulk datasets |
| ecb | ✅ | 215,943,140 | all SDMX dataflows |
| faostat | ✅ | 169,509,698 | 68/68 domains |
| sec_edgar (fund.) | ✅ | 123,345,899 | all 19,814 companies/taxonomies |
| imf | ✅ | 120,623,146 | 101/102 flows (102nd has no published data) |
| edgar_13f | ✅ | 118,176,190 | full 2013q2–2026 |
| owid | ✅ | 72,492,801 | 95.7% of what OWID permits redistribution (554 charts non_redistributable by OWID's own license, 18 source-503 resource limits, 151 empty/categorical) |
| ofr  | ✅ | 425,070 | OFR fnyr (30 series incl. BGCR) + repo (164 series) + mmf (42) + nypd (194). BGCR captured. US public domain. |
| bea | ✅ | 67,490,269 | all BEA datasets |
| usda | ✅ | 57,631,852 | = API published total |
| edgar_insider | ✅ | 37,169,724 | full 2003–2026 |
| bis | ✅ | 88,421,620 | 30/32 flows; CBS (11.2M) + LBS (36.4M) added via data.bis.org bulk. WS_NA_SEC_C3 = 404 everywhere (not publicly served). LBSN/LBSR are dimensions within LBS (not separate flows) |
| census | ✅ | 44,939,061 | 89/93 datasets + QWI fully completed (no truncation); 4 intltrade HS/port paths not API-extractable per Census |
| defillama | 🟡 | 31,223,909 | 82%; yields per-pool history still downloading |
| edgar_pointers | ✅ | 26,862,039 | rebuilt clean (256 shards, full history) |
| fed_board | ✅ | 13,982,879 | all DDP datasets |
| dbnomics | 🟡 | 13,248,246 | aggregator ~60% dup; unique slice pulled |
| worldbank_wdi | ✅ | 8,894,931 | all 1,486 indicators |
| ember | ✅ | 8,107,323 | all datasets |
| treasury | ✅ | 18,555,857 | 181/181 endpoints, 53 datasets — full FiscalData catalog |
| boe | ✅ | 4,000,326 | all IADB series |
| fhfa | ✅ | 3,331,951 | all HPI datasets |
| worldbank_esg | ✅ | 599,099 | full ESG set |
| frankfurter | ✅ | 527,315 | all currencies, full history |
| penn_world_table | ✅ | 422,767 | full PWT 11.0 |
| worldbank_pink | ✅ | 93,495 | all commodities (monthly + annual) |
| wikidata | ✅ | 22,843 | full econ/finance entity set |
| insee_bdm | ✅ | 10,799,719 | BDM 201/243 active dataflows (42 empty/discontinued). Full French macro: national accounts, employment, prices, trade, industry. Keyless. |
| insee_sirene | ✅ | 247,461,408 | 6 stock files from data.gouv.fr: 29.7M legal units + 43.5M establishments + historical periods + succession links. Full registry. |
| insee_melodi | 🔄 | 12,239,892 | 39/109 flows done. DS_BPE (2.3M obs) completed. DS_BPE_SPORT_CULTURE downloading. |

## Measured totals (2026-06-08 post-outage update)
- **Local Parquet: 37,367,792,282 obs** (measured; OECD 6.0B + Melodi +2.3M since last count)
- **HF referenced (R2): 1,498,837,188 obs**
- **GRAND TOTAL: ~38.9 billion obs** (OECD final 103 flows + Melodi 70 flows + WB extra 7 dbs + DeFiLlama yields pending)

## Still running (2026-06-08)
- **OECD_Crawl** schtask: 1,406/1,509 files; fresh pass launched 17:03 after stuck crawler killed; 102 remaining flows mostly 404 empties
- **Melodi**: 39/109 flows; downloading BPE variants (large geographic datasets)
- **WorldBank extra**: 2/9 done (hnp+ids); working EdStats (8,450 indicators, slow)
- **DeFiLlama yields**: 2,867/15,961 pools staged; ~10 hrs remaining

## New sources added (session 3)
| gleif | ✅ | 3,330,161 | Full GLEIF golden copy (CC0) — join key for EDGAR/13F ownership |
| cftc | ✅ | 625,856 | Full CoT history 1986–2026, all report types (public domain) |
| famafrench | ✅ | 238,797 | 3-factor/5-factor/momentum daily+monthly (educational use) |
| treasury | ✅ | 18,555,857 | 181/181 FiscalData endpoints (was 29 datasets / 6.6M) |
| cepii_baci | ✅ | 308,561,322 | BACI bilateral trade HS17 (65.6M) + HS96 (242.9M); Etalab 2.0 |
| worldbank_extra | 🔄 | 8,817,027 | 2/9 done (hnp 8.5M, ids 268K). gfdd/gender/gem returned 0 obs. EdStats (8,450 ind) running. |
| nyfed | ✅ | 16,656 | SOFR/OBFR/SOFR-averages/SOFR-index/TGCR/TGCR-volume via FRED API; BGCR → see OFR fnyr above |
| fred | ✅ | 48,048,976 | 325/325 releases, copyright filter = 0 copyrighted series stored; top: State employment 10.5M, Z.1 flow-of-funds 8.2M, state unemployment 6.1M |
| bis_cbs_lbs | 🔄 | — | CBS+LBS bulk downloading (resumable) |

## Still needed before catalog ingest + cloud build
1. StatCan finish (wait for .done count = 8,207)
2. OECD_Crawl finish (wait for 1,509 files)
3. BIS verify — confirm all 32 flows covered (run `python jobs/ingest_bis_full.py --list` + check file count)
4. Census — 13 missing datasets
5. Treasury — verify 29 datasets = full FiscalData catalog
6. HF — bridge other tiers (5m/15m/30m/hourly/daily/weekly) from R2
7. INSEE — needs keys from Ahmed
8. **Then: ingest full set into catalog (catalog.db / D1) + cloud build (R2 econdatalibrary-data + D1 + Worker + Pages + GitHub Actions)**
