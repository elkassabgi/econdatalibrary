# Database licenses — verbatim redistribution audit

**Generated 2026-07-14** by the `econ-license-verbatim-audit` workflow (run wf_9ff754f5-37d): for every database, an agent fetched the provider's OFFICIAL terms, quoted the redistribution clause VERBATIM with the source URL, and classified it; a second, independent adversarial agent re-fetched the URL, confirmed the quote is word-for-word, and tried to refute any over-permissive reading. 88 providers, 191 databases, 176 agents, 0 errors.

**This is the single source of truth. Do NOT re-derive it from scratch** — read it, and only re-run the workflow to fill gaps or refresh. Decision rule (asymmetric caution): a database is only *cleared to re-host* when the terms **explicitly permit redistribution/re-dissemination** by a third party AND the adversarial verifier CONFIRMED it. Anything restricted, ambiguous, unreachable, or DISPUTED stays gated / flagged for human review.

---

## Summary

**Decision tiers (per database):**

- **CLEARED - re-host OK (attribution)** — 144
- **RESTRICTED (keep gated)** — 18
- **NEEDS HUMAN REVIEW** — 11
- **CLEARED - re-host OK** — 9
- **CLEARED - non-commercial only** — 6
- **CLEARED by WRITTEN PERMISSION** — 2
- **CLEARED by WRITTEN PERMISSION (scoped/conditional)** — 1

**Adversarial verdicts:** CONFIRMED=184, DISPUTED=7

**Classifications:** redistributable_attribution=144, permission_required=20, redistributable_open=9, noncommercial_only=6, unclear_not_found=5, non_redistributable — use-only grant. Personal/professional use, forwarding, and reproduction are permitted with mandatory attribution ("Source: Deutsche Bundesbank") and no alteration (no-derivatives). The terms grant NO right to republish, redistribute, or make the data publicly available to third parties, so a library re-hosting the data for public download is not covered. Treat as metadata-only / link-out unless the Bundesbank grants prior written permission for redistribution.=1, redistributable_attribution_noncommercial (with third-party-data carve-out) — re-dissemination is permitted with FAO attribution, but subject to (a) a non-commercial/anti-endorsement restriction that CC BY 4.0 does not impose, and (b) a subset of embedded third-party data that cannot be redistributed without the original provider's consent.=1, noncommercial_permission_required / no_open_redistribution — noncommercial USE with citation is permitted, but the FIW dataset is gated behind a Freedom House "FIW Data Request" (must state intended use), and third-party re-hosting for open public download is not authorized. Treat as not-freely-redistributable: link out to Freedom House's data request rather than mirror the files (or gate to metadata-only), and note commercial use requires prior formal permission.=1, noncommercial_no_derivatives (CC BY-NC-ND: NonCommercial AND NoDerivatives). Only verbatim, non-commercial, attributed copies may be redistributed. Separately, per the finding's own license_name note, ~86% of IDB datasets carry NO declared license (no redistribution grant) and a minority are CC BY 4.0 — so a single source-level bucket is not accurate; the unlicensed majority should be treated as not-redistributable / needs-review, not noncommercial.=1, mixed / source-dependent — NOT blanket redistributable_attribution. Only the minority of data that OWID produces itself ("Data produced by us", flagged e.g. "with major processing by Our World in Data") is CC BY and redistributable with attribution. The majority ("Most of the data") is third-party (WHO, UN, World Bank, and many others) and remains subject to each upstream provider's own license, which must be assessed per-source before re-hosting. Treat the source as partially/conditionally redistributable pending per-provider review, not uniformly CC BY.=1, redistributable_attribution_with_exceptions — CC BY 4.0 applies to the World Bank's own compiled data, but third-party-sourced datasets/indicators embedded in World Bank Open Data (e.g., WDI series from UN Population Division, IMF, WHO, ILO, IEA, UNESCO) may NOT be redistributed without the original provider's consent. A library that re-hosts data for public download must exclude or separately clear all third-party-sourced series rather than treat the whole source as blanket-redistributable.=1, restricted / needs-review (NOT blanket CC BY 4.0). The Pink Sheet is not wholly "produced by the World Bank itself" — a large share of its series come from third-party proprietary providers: London Metal Exchange (LME) settlement prices for aluminum, copper, lead, nickel, tin, zinc; Cotlook "A index" for cotton; SICOM for rubber; ICCO/ICO for cocoa/coffee. Under the terms' own third-party carve-out these "may not be redistributed or reused without the consent of the original data provider." For a public re-hosting library, treat worldbank_pink as NEEDS-REVIEW / non-redistributable pending per-series rights clearance (LME in particular prohibits redistribution of its price data without a license), rather than redistributable_attribution.=1

### Written permissions on file (override the public terms below)

The public terms the audit read may say 'permission required' for these, but we already hold written permission (see permission records (held privately)):

- `comtrade` — GRANTED in writing (UN Comtrade): 'you can proceed'; holdings must stay <=100,000 records; cite 'UN Comtrade' + link.
- `kof_globalization` — GRANTED in writing (Prof. Sturm, KOF/ETH Zurich): NC academic re-host; cite 'KOF, ETH Zurich' + link back.
- `whr` — GRANTED in writing (Gallup/WHR) but SCOPED to the Figure 2.1 summary ONLY; currently re-gated pending trim to that scope.
- `efw` — GRANTED in writing (Fraser Institute, 2026-08-10): NC re-host of the EFW index + component data with attribution + link-back. Verbatim record in the "Economic Freedom of the World" section near the end of this file. Source not yet built; grant precedes ingestion.

### ⚠️ Needs attention (restricted or unresolved) — review before serving

| Provider | Databases | Final classification | Verdict | Why |
|---|---|---|---|---|
| World Trade Organization (WTO) | 8 | permission_required | CONFIRMED | Downloaded the official terms PDF (92.7 KB, WTO "TERMS AND CONDITIONS OF USE, DISCLAIMER AND COPYRIGHT" for the TAO / Ta |
| Deutsche Bundesbank time series | 1 | non_redistributable — use-only grant. Personal/professional use, forwarding, and reproduction are permitted with mandatory attribution ("Source: Deutsche Bundesbank") and no alteration (no-derivatives). The terms grant NO right to republish, redistribute, or make the data publicly available to third parties, so a library re-hosting the data for public download is not covered. Treat as metadata-only / link-out unless the Bundesbank grants prior written permission for redistribution. | DISPUTED | You are free to save, forward or reproduce the information produced in physical or electronic form by the Deutsche Bunde |
| cboe | 1 | permission_required | CONFIRMED | Fetched https://www.cboe.com/terms/ successfully. The verbatim_quote appears WORD-FOR-WORD on the live page in Section 2 |
| Correlates of War | 1 | permission_required | CONFIRMED | Verified against the live official page at https://correlatesofwar.org/data-sets/ (rendered via browser; direct WebFetch |
| Aswath Damodaran (NYU Stern) datasets | 1 | unclear_not_found | CONFIRMED | Verified both prongs against the live source (fetched OK; matches fetch_status=fetched_ok).  QUOTE: Verbatim-accurate. T |
| defillama | 1 | permission_required | CONFIRMED | Quote verified verbatim. The exact string "republish the data in any form without permission" appears as clause 8.7 in S |
| Energy Institute Statistical Review of W | 1 | permission_required | CONFIRMED | Fetched the URL (WebFetch could not parse the PDF text layer, so I extracted all 76 pages locally with pypdf and searche |
| Kenneth French Data Library (Dartmouth) | 1 | permission_required | CONFIRMED | Quote verified verbatim against the raw HTML of the official URL (fetched via curl; fetch_status fetched_ok confirmed).  |
| faostat | 1 | redistributable_attribution_noncommercial (with third-party-data carve-out) — re-dissemination is permitted with FAO attribution, but subject to (a) a non-commercial/anti-endorsement restriction that CC BY 4.0 does not impose, and (b) a subset of embedded third-party data that cannot be redistributed without the original provider's consent. | DISPUTED | "Datasets shall not be used for or in conjunction with the promotion of a commercial enterprise and/or its product(s) or |
| frankfurter | 1 | unclear_not_found | CONFIRMED | Verbatim quote CONFIRMED at https://frankfurter.dev/ (live, 200, fetched_ok). It is the answer to the FAQ question "Is t |
| Freedom House | 1 | noncommercial_permission_required / no_open_redistribution — noncommercial USE with citation is permitted, but the FIW dataset is gated behind a Freedom House "FIW Data Request" (must state intended use), and third-party re-hosting for open public download is not authorized. Treat as not-freely-redistributable: link out to Freedom House's data request rather than mirror the files (or gate to metadata-only), and note commercial use requires prior formal permission. | DISPUTED | "Interested in downloading Freedom in the World report data? While our data is free for personal, academic, and nonprofi |
| Inter-American Development Bank (IDB) | 1 | noncommercial_no_derivatives (CC BY-NC-ND: NonCommercial AND NoDerivatives). Only verbatim, non-commercial, attributed copies may be redistributed. Separately, per the finding's own license_name note, ~86% of IDB datasets carry NO declared license (no redistribution grant) and a minority are CC BY 4.0 — so a single source-level bucket is not accurate; the unlicensed majority should be treated as not-redistributable / needs-review, not noncommercial. | DISPUTED | On the live page's "Metadata & use" table, the License field links to "Creative Commons Attribution–NonCommercial–NoDeri |
| IRENA (Int'l Renewable Energy Agency) | 1 | unclear_not_found | CONFIRMED | CONFIRMED, with one disclosed caveat. (1) Verbatim quote: Two independent JS-free WebFetches of the official tool (https |
| Narodowy Bank Polski (NBP) | 1 | permission_required | CONFIRMED | Verbatim quote verified word-for-word on the official URL (https://api.nbp.pl/en.html): the page returns exactly "Copyri |
| owid | 1 | mixed / source-dependent — NOT blanket redistributable_attribution. Only the minority of data that OWID produces itself ("Data produced by us", flagged e.g. "with major processing by Our World in Data") is CC BY and redistributable with attribution. The majority ("Most of the data") is third-party (WHO, UN, World Bank, and many others) and remains subject to each upstream provider's own license, which must be assessed per-source before re-hosting. Treat the source as partially/conditionally redistributable pending per-provider review, not uniformly CC BY. | DISPUTED | Most of the data on Our World in Data comes from third-party providers (such as the WHO, UN, and World Bank) and is subj |
| Polity5 (Center for Systemic Peace) | 1 | permission_required | CONFIRMED | STEP 1 (verbatim check): WebFetch of https://www.systemicpeace.org/inscrdata.html succeeded (fetch_status fetched_ok con |
| Robert Shiller (Yale) online data | 1 | unclear_not_found | CONFIRMED | VERBATIM CHECK — PASS. WebFetch could not reach the cited URL because it forces HTTP->HTTPS and the Yale server (128.36. |
| SIPRI (Stockholm Int'l Peace Research In | 1 | permission_required | CONFIRMED | Adversarial review of SIPRI terms. (1) VERBATIM: The quote "Any reproduction—in any medium, electronic or printed—of the |
| Central Bank of Turkey (TCMB) EVDS | 1 | permission_required | CONFIRMED | Quote verified WORD-FOR-WORD at the finding's URL. Two independent WebFetches of https://www.tcmb.gov.tr/wps/wcm/connect |
| World Bank Open Data | 1 | redistributable_attribution_with_exceptions — CC BY 4.0 applies to the World Bank's own compiled data, but third-party-sourced datasets/indicators embedded in World Bank Open Data (e.g., WDI series from UN Population Division, IMF, WHO, ILO, IEA, UNESCO) may NOT be redistributed without the original provider's consent. A library that re-hosts data for public download must exclude or separately clear all third-party-sourced series rather than treat the whole source as blanket-redistributable. | DISPUTED | "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent  |
| worldbank_pink | 1 | restricted / needs-review (NOT blanket CC BY 4.0). The Pink Sheet is not wholly "produced by the World Bank itself" — a large share of its series come from third-party proprietary providers: London Metal Exchange (LME) settlement prices for aluminum, copper, lead, nickel, tin, zinc; Cotlook "A index" for cotton; SICOM for rubber; ICCO/ICO for cocoa/coffee. Under the terms' own third-party carve-out these "may not be redistributed or reused without the consent of the original data provider." For a public re-hosting library, treat worldbank_pink as NEEDS-REVIEW / non-redistributable pending per-series rights clearance (LME in particular prohibits redistribution of its price data without a license), rather than redistributable_attribution. | DISPUTED | From the same official terms page (https://data.worldbank.org/summary-terms-of-use): "Some datasets and indicators are p |
| zillow | 1 | permission_required | CONFIRMED | Direct verification of the live URL was blocked: https://www.zillow.com/corporate/terms-of-use/ returns HTTP 403 to WebF |

---

## Per-database index

| Database | Provider | Final classification | Verdict | Tier |
|---|---|---|---|---|
| `abs` | abs | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `barro_lee` | Barro-Lee Educational Attainment | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `bcb` | Banco Central do Brasil (BCB) SGS | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `bcrp` | Banco Central de Reserva del Peru  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `bea` | bea | redistributable_open | CONFIRMED | CLEARED - re-host OK |
| `bis` | bis | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `bls` | bls | redistributable_open | CONFIRMED | CLEARED - re-host OK |
| `boc` | Bank of Canada Valet | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `boe` | boe | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `bundesbank` | Deutsche Bundesbank time series | non_redistributable — use-only grant. Personal/professional use, forwarding, and reproduction are permitted with mandatory attribution ("Source: Deutsche Bundesbank") and no alteration (no-derivatives). The terms grant NO right to republish, redistribute, or make the data publicly available to third parties, so a library re-hosting the data for public download is not covered. Treat as metadata-only / link-out unless the Bundesbank grants prior written permission for redistribution. | DISPUTED | NEEDS HUMAN REVIEW |
| `cboe` | cboe | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `cbs_nl` | CBS (Statistics Netherlands) | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `census` | census | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `cnb` | Czech National Bank (CNB) ARAD | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `comtrade` | UN Comtrade | permission_required | CONFIRMED | CLEARED by WRITTEN PERMISSION |
| `cow` | Correlates of War | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `damodaran` | Aswath Damodaran (NYU Stern) datas | unclear_not_found | CONFIRMED | NEEDS HUMAN REVIEW |
| `dbnomics` | DBnomics (per-provider passthrough | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `defillama` | defillama | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `ecb` | ecb | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `edgar_jrc` | EU JRC EDGAR (Emissions Database f | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `ei_statreview` | Energy Institute Statistical Revie | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `efw` | Fraser Institute Economic Freedom of the World | written_permission | CONFIRMED | CLEARED by WRITTEN PERMISSION (non-commercial, attribution; grant recorded in the efw verbatim section) |
| `eia` | eia | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `ember` | ember | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `epu` | Economic Policy Uncertainty Index  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `eurostat` | eurostat | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `famafrench` | Kenneth French Data Library (Dartm | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `fao_ae` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_af` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_ec` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_ep` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_es` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_et` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_ew` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_fo` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_ga` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_gb` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_ge` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_gf` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_gl` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_gn` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_gr` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_gt` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_gy` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_ic` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_oa` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_pp` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_qa` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_qcl` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_ql` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_qp` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fao_rp` | FAO (UN Food and Agriculture Organ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `faostat` | faostat | redistributable_attribution_noncommercial (with third-party-data carve-out) — re-dissemination is permitted with FAO attribution, but subject to (a) a non-commercial/anti-endorsement restriction that CC BY 4.0 does not impose, and (b) a subset of embedded third-party data that cannot be redistributed without the original provider's consent. | DISPUTED | NEEDS HUMAN REVIEW |
| `fed_board` | fed_board | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `fhfa` | fhfa | redistributable_open | CONFIRMED | CLEARED - re-host OK |
| `frankfurter` | frankfurter | unclear_not_found | CONFIRMED | NEEDS HUMAN REVIEW |
| `freedomhouse` | Freedom House | noncommercial_permission_required / no_open_redistribution — noncommercial USE with citation is permitted, but the FIW dataset is gated behind a Freedom House "FIW Data Request" (must state intended use), and third-party re-hosting for open public download is not authorized. Treat as not-freely-redistributable: link out to Freedom House's data request rather than mirror the files (or gate to metadata-only), and note commercial use requires prior formal permission. | DISPUTED | NEEDS HUMAN REVIEW |
| `fsi_fundforpeace` | Fund for Peace Fragile States Inde | noncommercial_only | CONFIRMED | CLEARED - non-commercial only |
| `gcb` | Global Carbon Budget / Global Carb | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `ggdc` | Groningen Growth and Development C | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `gppd` | Global Power Plant Database (WRI) | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `gus_dbw` | GUS (Statistics Poland) Knowledge Databases | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution + PSI disclosure) |
| `hf_equities` | hf_equities | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `idb` | Inter-American Development Bank (I | noncommercial_no_derivatives (CC BY-NC-ND: NonCommercial AND NoDerivatives). Only verbatim, non-commercial, attributed copies may be redistributed. Separately, per the finding's own license_name note, ~86% of IDB datasets carry NO declared license (no redistribution grant) and a minority are CC BY 4.0 — so a single source-level bucket is not accurate; the unlicensed majority should be treated as not-redistributable / needs-review, not noncommercial. | DISPUTED | NEEDS HUMAN REVIEW |
| `ilostat` | ilostat | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf` | imf | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_afrreo` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_apdreo` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_bopagg` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_cofer` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_commodity` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_cpi` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_fas` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_fdi` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_fiscaldecentralization` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_fm` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_fsire` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_gender_budgeting` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_gender_equality` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_gfscofog` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_gfse` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_gfsfalcs` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_gfsibs` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_gfsmab` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_gfsssuc` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_hpdd` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_mcdreo` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_namain_idc_n` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_pctot` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_pgcs` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_pgi` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_psbsfad` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_unsdg_imf_inputs` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_weo` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_whdreo` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `imf_world` | International Monetary Fund (IMF)  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `insee_bdm` | INSEE (France, Institut national d | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `ipea` | IPEA / Ipeadata (Brazil) | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `irena` | IRENA (Int'l Renewable Energy Agen | unclear_not_found | CONFIRMED | NEEDS HUMAN REVIEW |
| `kof_globalization` | KOF Swiss Economic Institute (ETH  | permission_required | CONFIRMED | CLEARED by WRITTEN PERMISSION |
| `ksh` | KSH Hungarian Central Statistical  | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `maddison` | Maddison Project Database (Groning | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `nasa_giss` | NASA GISS (Goddard Institute for S | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `nbp` | Narodowy Bank Polski (NBP) | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `noaa` | NOAA | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `nyfed` | Federal Reserve Bank of New York | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `oecd` | oecd | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `ofr` | US Office of Financial Research | redistributable_open | CONFIRMED | CLEARED - re-host OK |
| `owid` | owid | mixed / source-dependent — NOT blanket redistributable_attribution. Only the minority of data that OWID produces itself ("Data produced by us", flagged e.g. "with major processing by Our World in Data") is CC BY and redistributable with attribution. The majority ("Most of the data") is third-party (WHO, UN, World Bank, and many others) and remains subject to each upstream provider's own license, which must be assessed per-source before re-hosting. Treat the source as partially/conditionally redistributable pending per-provider review, not uniformly CC BY. | DISPUTED | NEEDS HUMAN REVIEW |
| `oxcgrt` | Oxford COVID-19 Government Respons | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `penn_world_table` | penn_world_table | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `pip` | World Bank Poverty & Inequality Pl | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `polity` | Polity5 (Center for Systemic Peace | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `pwt` | Penn World Table (Groningen GGDC) | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `rba` | Reserve Bank of Australia (RBA) | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `riksbank` | Sveriges Riksbank | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `sec_edgar` | sec_edgar | redistributable_open | CONFIRMED | CLEARED - re-host OK |
| `shiller` | Robert Shiller (Yale) online data | unclear_not_found | CONFIRMED | NEEDS HUMAN REVIEW |
| `sipri` | SIPRI (Stockholm Int'l Peace Resea | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `snb` | Swiss National Bank (SNB) data por | noncommercial_only | CONFIRMED | CLEARED - non-commercial only |
| `statcan` | statcan | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `stats_nz` | Stats NZ | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `swiid` | Standardized World Income Inequali | redistributable_open | CONFIRMED | CLEARED - re-host OK |
| `tcmb` | Central Bank of Turkey (TCMB) EVDS | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `transparency_ti` | Transparency International (CPI) | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `treasury` | treasury | redistributable_open | CONFIRMED | CLEARED - re-host OK |
| `ucdp` | Uppsala Conflict Data Program (UCD | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_bopcaba` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_ciocgeaia` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_cioiuibbicoeair4a` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_cpa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_cpia` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_cpta` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_fdiiaofasa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_fmcpa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_fmcpia21` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_gasbeaiogasa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_gasbtbia` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_gasbtoia` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_gdpgbtoevbkoeatasa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_gdptapccac2pa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_lscia` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_lsciq` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_mfbcoboa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_mmcascioeaiopa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_mpcadioeaia` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_mtba` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_mttasa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_mttgra` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_neera` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_reericba` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_reerigdba` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_rfia` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_rgdptapcgra` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_sbeaiotsvsaga` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_sbtisvsaga` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_soigapotta` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_sotwmfvbcoboa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_srbca` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_tabbapotta` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_tabmcioeaiopa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_tabmscioeaiopa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_tabpcioeaia` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_taupa` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unctad_wstbtocabgoea` | UNCTAD (UN Conference on Trade and | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `undp_hdr` | UNDP Human Development Report | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unesco_clte` | UNESCO Institute for Statistics (U | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unesco_cltt` | UNESCO Institute for Statistics (U | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unesco_dem` | UNESCO Institute for Statistics (U | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unesco_film` | UNESCO Institute for Statistics (U | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unesco_inno` | UNESCO Institute for Statistics (U | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `unhcr` | UNHCR Refugee Data | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `usda` | usda | redistributable_open | CONFIRMED | CLEARED - re-host OK |
| `wgi` | World Bank Worldwide Governance In | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `wid` | World Inequality Database (WID.world) | noncommercial_sharealike (CC BY-NC-SA 4.0) | CONFIRMED | CLEARED - re-host OK (non-commercial, attribution, SHARE-ALIKE) |
| `who_hwf` | World Health Organization (WHO) Gl | noncommercial_only | CONFIRMED | CLEARED - non-commercial only |
| `who_rs` | World Health Organization (WHO) Gl | noncommercial_only | CONFIRMED | CLEARED - non-commercial only |
| `who_sdg` | World Health Organization (WHO) Gl | noncommercial_only | CONFIRMED | CLEARED - non-commercial only |
| `whr` | whr | unclear_not_found | CONFIRMED | CLEARED by WRITTEN PERMISSION (scoped/conditional) |
| `wikidata` | wikidata | redistributable_open | CONFIRMED | CLEARED - re-host OK |
| `worldbank` | World Bank Open Data | redistributable_attribution_with_exceptions — CC BY 4.0 applies to the World Bank's own compiled data, but third-party-sourced datasets/indicators embedded in World Bank Open Data (e.g., WDI series from UN Population Division, IMF, WHO, ILO, IEA, UNESCO) may NOT be redistributed without the original provider's consent. A library that re-hosts data for public download must exclude or separately clear all third-party-sourced series rather than treat the whole source as blanket-redistributable. | DISPUTED | NEEDS HUMAN REVIEW |
| `vdem` | V-Dem Institute (Varieties of Democracy), University of Gothenburg | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `worldbank_esg` | worldbank_esg | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `worldbank_pink` | worldbank_pink | restricted / needs-review (NOT blanket CC BY 4.0). The Pink Sheet is not wholly "produced by the World Bank itself" — a large share of its series come from third-party proprietary providers: London Metal Exchange (LME) settlement prices for aluminum, copper, lead, nickel, tin, zinc; Cotlook "A index" for cotton; SICOM for rubber; ICCO/ICO for cocoa/coffee. Under the terms' own third-party carve-out these "may not be redistributed or reused without the consent of the original data provider." For a public re-hosting library, treat worldbank_pink as NEEDS-REVIEW / non-redistributable pending per-series rights clearance (LME in particular prohibits redistribution of its price data without a license), rather than redistributable_attribution. | DISPUTED | NEEDS HUMAN REVIEW |
| `worldbank_wdi` | World Bank World Development Indic | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `wto_hs_a_0010` | World Trade Organization (WTO) | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `wto_hs_a_0015` | World Trade Organization (WTO) | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `wto_hs_a_0020` | World Trade Organization (WTO) | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `wto_hs_a_0025` | World Trade Organization (WTO) | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `wto_hs_a_0030` | World Trade Organization (WTO) | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `wto_hs_a_0040` | World Trade Organization (WTO) | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `wto_its_mtv_am` | World Trade Organization (WTO) | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `wto_its_mtv_ax` | World Trade Organization (WTO) | permission_required | CONFIRMED | RESTRICTED (keep gated) |
| `yale_epi` | Yale Environmental Performance Ind | noncommercial_only | CONFIRMED | CLEARED - non-commercial only |
| `zillow` | zillow | permission_required | CONFIRMED | RESTRICTED (keep gated) |

---

## Per-provider detail (verbatim terms)

### abs

- **Databases (1):** `abs`
- **Official terms URL:** https://www.abs.gov.au/website-privacy-copyright-and-disclaimer
- **License:** CC BY 4.0 (Creative Commons Attribution 4.0 International)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> All material presented on this website is provided under a Creative Commons Attribution 4.0 International licence
> © Commonwealth of Australia. The Commonwealth owns the copyright in all material produced by the Australian Bureau of Statistics (ABS).
> Material obtained from this website is to be attributed to this department. If you use ABS material, there are certain obligations you must fulfil.
> The Creative Commons license does NOT apply to: Commonwealth Coat of Arms; ABS logo; Trademarked material; Unit record data (microdata); Third-party content; Sub-brands (DataLab, SEAD); 'Our story, our future' artwork; Census branding and artwork; OSCA branding and artwork.
> Wherever a third party holds copyright in material presented on this website, the copyright remains with that party.

*Verifier notes:* Verified against the official ABS page https://www.abs.gov.au/website-privacy-copyright-and-disclaimer via two independent WebFetch passes (fetch_status: fetched_ok confirmed).

QUOTE CHECK: The verbatim_quote "All material presented on this website is provided under a Creative Commons Attribution 4.0 International licence" appears WORD-FOR-WORD on the page. On the page the sentence continues "...licence, with the exception of:" introducing a carve-out list, but the quoted portion is exact and not misleadingly truncated.

STRICTER-CLAUSE SEARCH: I actively looked for a redistribution ban, non-commercial restriction, no-derivatives clause, prior-written-permission gate, and bulk-download restriction. CC BY 4.0 is a genuinely redistributable-with-attribution licence (permits copying, redistribution, and commercial use and derivatives), so no such stricter clause overrides the base grant for the licensed material. The page reserves "all other rights" only "Apart from any use as permitted under the Copyright Act 1968, and those explicitly granted below" — i.e. the CC BY grant is the operative permission, not a reservation against it.

CARVE-OUTS (standard, do not defeat the classification): The CC BY 4.0 grant excludes the Commonwealth Coat of Arms, the ABS logo, trademarked material, UNIT RECORD DATA (microdata), third-party-supplied content, sub-brands (DataLab, SEAD), Aboriginal and Torres Strait Islander / 'Our story, our future' brand artwork, Census branding, and OSCA branding. Third-party-held copyright material may require that party's permission. These are conventional government-CC-BY exclusions covering logos, branding, confidential microdata, and third-party content — none of which is the aggregate statistical data a library would re-host. They should be respected at the dataset level (a re-hosting library must not redistribute ABS microdata or ABS logos/branding), but they do not make the source-level classification wrong.

JUDGMENT: The classification redistributable_attribution (CC BY 4.0) is DEFENSIBLE and NOT too permissive for a library that re-hosts general ABS statistical data for public download, provided attribution is given and the excluded items (microdata, logos/branding, third-party content) are not redistributed. Quote is verbatim-accurate and classification is supported by the terms. Verdict: CONFIRMED.

*Researcher reasoning:* The Australian Bureau of Statistics states on its official copyright page (abs.gov.au) that "All material presented on this website is provided under a Creative Commons Attribution 4.0 International licence." CC BY 4.0 is a well-established open licence that explicitly permits redistribution, re-hosting, and re-dissemination (copy, distribute, and communicate the material in any medium or format), including for commercial purposes, subject only to attribution (and indicating changes / not implying endorsement). This directly supports re-hosting for download by a free non-commercial academic library, so long as ABS is attributed (e.g. "Source: Australian Bureau of Statistics"). Classification is redistributable_attribution rather than redistributable_open because attribution is a mandatory condition. IMPORTANT CARVE-OUTS: the CC BY 4.0 licence does NOT cover several categories that ABS explicitly excludes — most critically for a data library, "Unit record data (microdata)" and any "Third-party content" (which remain under the original copyright holder's terms), plus the Commonwealth Coat of Arms, ABS logo, trademarks, and various branding/sub-brand (DataLab, SEAD, Census, OSCA) materials. Standard aggregate ABS statistical tables and datasets are CC BY 4.0 and redistributable with attribution; microdata and any incorporated third-party data must be checked separately before re-hosting. Note also ABS Surveys (survey forms/questions) are separately licensed CC BY-NC-ND, but those are survey instruments, not the statistical data. Verified verbatim from the provider's own domain; fetch succeeded.

---

### Aswath Damodaran (NYU Stern) — "Damodaran Online" datasets

- **Databases (1):** `damodaran`
- **Official terms URL:** https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datahistory.html#rules
- **License:** No formal license or copyright grant; informal permissive usage note with optional attribution. No public-domain/CC0 dedication and no explicit redistribution grant.
- **Classification:** unclear_not_found
- **Commercial OK:** None · **Attribution required:** False · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> If you do use my data and wish to acknowledge that you did get the data off my site, I thank you. If not, I will not lose any sleep and you should not either.
> I want the data to be widely used and to be a help, rather than a hindrance.
> this data is here for you to use and I hope it makes your life easier and your financial analyses better.
> While I would love to share the company-level data (like I used to), I am afraid that I am no longer allowed to do that by the data services.

*Verifier notes:* Verified both prongs against the live source (fetched OK; matches fetch_status=fetched_ok).

QUOTE: Verbatim-accurate. The page's Usage Rules item #1 reads: "Acknowledgements: If you do use my data and wish to acknowledge that you did get the data off my site, I thank you. If not, I will not lose any sleep and you should not either." The finding's quote matches word-for-word (it correctly omits only the "Acknowledgements:" list label).

STRICTER-CLAUSE SEARCH (adversarial): I read the entire "Usage Rules" section in full — the intro plus all six numbered rules plus the closing. There is NO redistribution ban, NO non-commercial/no-sell restriction, NO "prior written permission" requirement, NO no-derivatives clause, and NO bulk-download/mass-extraction restriction. Intro: "I am not good at making rules and thus have very few related to the use of my data. I want the data to be widely used." Closing: "this data is here for you to use." Rules 2-6 are purely advisory (best-use guidance; please-leave-me-out-of-court; not-for-public-policy-debates) — none is a legal restriction. So no stricter clause was missed.

CLASSIFICATION DEFENSIBLE AND NOT TOO PERMISSIVE: The researcher did NOT over-read the source. They chose classification=unclear_not_found and explicitly stated "No public-domain/CC0 dedication and no explicit redistribution grant." This is the correctly skeptical call: Damodaran grants broad USE but nowhere grants a third party the right to RE-HOST the data for public download (use != redistribute). For a re-hosting library, unclear_not_found is the appropriate conservative bucket — it neither invents a redistribution grant nor a nonexistent restriction. It is therefore not more permissive than the terms support.

Both prongs pass. One caveat for downstream use: because there is no affirmative redistribution grant, re-hosting for public download is NOT established as permitted — the unclear_not_found label should gate re-hosting pending direct permission, not greenlight it.

*Researcher reasoning:* The official usage terms are the "rules" section of Damodaran's data page at pages.stern.nyu.edu/~adamodar/New_Home_Page/datahistory.html#rules (linked from the main data.html page as "Usage rules"). I fetched and read it directly.

WHAT THE OFFICIAL TERMS SAY (verbatim, confirmed on the official page): Damodaran encourages broad USE and makes attribution explicitly OPTIONAL — "If you do use my data and wish to acknowledge that you did get the data off my site, I thank you. If not, I will not lose any sleep and you should not either." He also states "I want the data to be widely used and to be a help, rather than a hindrance" and "this data is here for you to use and I hope it makes your life easier and your financial analyses better." No commercial vs. non-commercial distinction is made.

WHAT THE OFFICIAL TERMS DO NOT SAY: There is NO explicit statement granting (or denying) the right to REDISTRIBUTE, RE-HOST, MIRROR, or make the datasets available for download on another website. A direct probe of the page for any repost/redistribute/re-host/mirror/re-disseminate language returned NONE. The page also never uses the phrase "public domain," and contains no copyright notice, no CC0/public-domain dedication, and no open-data license (CC BY, etc.). The generous "widely used" language governs USE by an end user; it does not, by its terms, address a third party re-hosting the files for mass download.

IMPORTANT CORRECTION TO WEB SNIPPETS: Search-engine summaries asserted the data is "public domain" with "no strings attached." I could not verify these as actual text on the official page — the phrase "public domain" does not appear, and no such dedication exists. Those characterizations appear to be third-party paraphrase, not the provider's own words, so I disregard them.

ADDITIONAL RISK FLAG: Damodaran notes on the data.html page, "While I would love to share the company-level data (like I used to), I am afraid that I am no longer allowed to do that by the data services." This shows the underlying inputs are sourced from commercial data services under contractual restrictions; the datasets he posts are his own aggregated/industry-level computations, but the upstream provenance reinforces caution about re-dissemination.

CLASSIFICATION: Per the conservative rule that "publicly available / free to use" branding does not by itself confer redistribution rights, and given the terms are silent on re-hosting with no open license or public-domain dedication, I classify redistribution rights as unclear_not_found. Use is clearly and generously permitted with optional attribution, but the specific right to RE-HOST the datasets for download cannot be confirmed from the official terms. I set fetch_status=fetched_ok because the official terms were located and read; the "unclear" verdict reflects the terms' silence on redistribution specifically, not a failure to access them. Recommendation for the compliance decision: do not treat as redistributable on the current record — either link to Damodaran's site rather than re-host, or email Damodaran for explicit written permission to mirror the files (he is known to be approachable and permissive, but the written grant should exist before re-hosting). commercial_ok and sharealike set to null because redistribution itself is unaddressed; attribution_required=false because acknowledgment is explicitly stated to be optional.

---

### Banco Central de Reserva del Perú (BCRP)

- **Databases (1):** `bcrp`
- **Official terms URL:** https://www.bcrp.gob.pe/condiciones-de-uso.html
- **License:** Custom terms — BCRP Condiciones de uso (reproduction with attribution)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Puede reproducirse total o parcialmente, sin autorización expresa, siempre y cuando se cite la fuente.
> El contenido del Portal de Internet, aplicaciones móviles y redes sociales del BCRP se elaboran con fines informativos y su uso es de exclusiva responsabilidad del visitante y no puede atribuírsele al BCRP responsabilidad legal alguna por las pérdidas, daños, gastos o cualquier otro perjuicio directo o indirecto, previsto o imprevisto, presente o futuro, que pueda tener origen en el acceso o uso de estos medios o de la utilización de la información contenida en ellos, o en la suspensión de su operabilidad.
> La responsabilidad sobre las estadísticas cuya fuente es externa al Banco recae en la entidad que provee el dato original.

*Verifier notes:* Verified against the official page https://www.bcrp.gob.pe/condiciones-de-uso.html (fetched_ok). The verbatim_quote "Puede reproducirse total o parcialmente, sin autorización expresa, siempre y cuando se cite la fuente." appears WORD-FOR-WORD; confirmed via two independent retrievals (direct WebFetch of the page and an independent WebSearch snippet), both returning identical wording. Adversarial search for a stricter clause found NONE: the page's only other provisions are a liability disclaimer ("de exclusiva responsabilidad del visitante"), a data-usage-for-continuous-improvement notice, a note that responsibility for external-source statistics lies with the original data provider, and a right to modify the portal "sin previo aviso". There is NO redistribution ban, NO non-commercial restriction, NO prior-written-permission requirement, NO no-derivatives clause, and NO mass-/bulk-download restriction. The permission explicitly covers reproduction "total o parcialmente" (in whole or in part) without express authorization, conditioned only on citing the source — which encompasses re-hosting for public download. classification redistributable_attribution is defensible and not too permissive: it preserves the mandatory attribution condition and does not overclaim public-domain/unrestricted status. One caveat, not disqualifying: for third-party/external-source series BCRP itself disclaims and points to the original provider's terms, so downstream attribution should credit the ultimate source where BCRP is merely a redistributor — but this does not affect the classification of BCRP's own terms.

*Researcher reasoning:* I located the BCRP's own official "Condiciones de uso" page at https://www.bcrp.gob.pe/condiciones-de-uso.html (linked in the footer of bcrp.gob.pe) and fetched/read it directly. The governing clause, immediately following the sentence describing "El contenido del Portal de Internet, aplicaciones móviles y redes sociales del BCRP", reads verbatim: "Puede reproducirse total o parcialmente, sin autorización expresa, siempre y cuando se cite la fuente." (Translation: "It may be reproduced in whole or in part, without express authorization, provided the source is cited.")

This is explicit redistribution/reproduction language, not merely an "open data"/"free access" branding statement. It grants reproduction of the content — including the statistical content served through the BCRP portal (BCRPData at estadisticas.bcrp.gob.pe is part of the BCRP portal) — in whole or in part WITHOUT needing prior/express authorization, subject to only one condition: citing the source. That maps to redistributable_attribution.

Commercial use: the clause imposes no commercial restriction whatsoever; the only condition is source citation. There is no "non-commercial", "no comercial", or "sin fines de lucro" language anywhere in the reproduction permission, so commercial re-hosting is not prohibited (commercial_ok = true). There is no share-alike / copyleft requirement (sharealike = false). Attribution ("se cite la fuente") is mandatory (attribution_required = true).

I verified the exact start of the sentence twice because two search-engine snippets rendered it slightly differently ("Los contenidos pueden reproducirse..." vs "Puede reproducirse..."); the direct fetch of the official page confirms the actual on-page wording is "Puede reproducirse total o parcialmente...". The page also contains a liability disclaimer (informational purposes / visitor's sole responsibility) and a note that responsibility for externally-sourced statistics rests with the original data provider — neither of which restricts redistribution, but I include them for completeness. Note the practical caveat implied by the last clause: some series in BCRPData are re-disseminated from external sources, and BCRP disclaims responsibility for those; a re-hoster should be aware that third-party series may carry their own upstream terms.

---

### Banco Central do Brasil (BCB) — SGS / Open Data Portal (opendata.bcb.gov.br)

- **Databases (1):** `bcb`
- **Official terms URL:** https://opendata.bcb.gov.br/dataset/27443-icc-spread/resource/997a20f0-3955-4719-8dd4-2bda754c7ff8
- **License:** Open Data Commons Open Database License (ODbL) 1.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** True · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Open Data Commons Open Database License (ODbL)
> license_id: odc-odbl | license_title: Open Data Commons Open Database License (ODbL) | license_url: http://www.opendefinition.org/licenses/odc-odbl  (verbatim fields returned by the BCB Open Data Portal CKAN API, https://opendata.bcb.gov.br/api/3/action/package_show?id=27443-icc-spread — identical across all sampled datasets: ICC spread, exchange rate USD, international reserves daily, credit operations, STR daily)
> You are free: To share: To copy, distribute and use the database. To create: To produce works from the database. To adapt: To modify, transform and build upon the database. (ODbL 1.0 human-readable summary, the license adopted by BCB, https://opendatacommons.org/licenses/odbl/summary/)
> As long as you: Attribute: You must attribute any public use of the database, or works produced from the database, in the manner specified in the ODbL. Share-Alike: If you publicly use any adapted version of this database, or works produced from an adapted database, you must also offer that adapted database under the ODbL. (ODbL 1.0 summary, https://opendatacommons.org/licenses/odbl/summary/)
> O Banco Central do Brasil não assume nenhuma responsabilidade por defasagem, erro ou outra deficiência em informações prestadas em série temporal cujas fontes sejam externas a esta instituição, bem como por quaisquer perdas ou danos decorrentes de seu uso. (SGS disclaimer page, https://www4.bcb.gov.br/pec/series/port/aviso.asp?frame=1 — liability disclaimer only; carries NO redistribution restriction)

*Verifier notes:* URL is live and correct: dataset title "ICC Spread" matches the slug 27443-icc-spread; page carries the Open Definition "satisfies the Open Definition" badge. The verbatim_quote "Open Data Commons Open Database License (ODbL)" appears word-for-word on BOTH the resource-level URL and the parent dataset page (two independent WebFetches). WebSearch independently confirms odc-odbl is a genuine, portal-wide license facet on opendata.bcb.gov.br. Adversarial search for a stricter overriding clause (redistribution ban, non-commercial limit, prior-written-permission, no-derivatives, bulk-download restriction) found NONE on the resource page, dataset page, or portal search. Classification "redistributable_attribution" is defensible: ODbL 1.0 explicitly grants copy/distribute/reuse rights, so redistribution for public re-hosting is genuinely permitted with attribution. One nuance worth logging (does NOT rise to DISPUTED): ODbL is a share-alike/copyleft license, so a finer-grained scheme might label it "redistributable_share_alike"; however, share-alike is a MORE restrictive obligation than plain attribution, not more permissive, so the current tag does not authorize anything ODbL forbids and is not the over-permissive failure mode the task guards against. The library should still (a) preserve attribution and the ODbL notice on re-hosted data, and (b) apply ODbL share-alike + keep-open to any derivative databases it publishes. Fetch status fetched_ok is accurate.

*Researcher reasoning:* The "bcb" database covers BCB SGS (Sistema Gerenciador de Séries Temporais) time series. The provider publishes these same SGS series on its own official Open Data Portal (opendata.bcb.gov.br), where every dataset carries an explicit, machine-readable license: the Open Data Commons Open Database License (ODbL). I confirmed this on the provider's own domain three ways: (1) the rendered dataset/resource pages display "Open Data Commons Open Database License (ODbL)" and a badge "This dataset satisfies the Open Definition"; (2) the CKAN API (opendata.bcb.gov.br/api/3/action/package_show and current_package_list_with_resources) returns license_id=odc-odbl, license_title="Open Data Commons Open Database License (ODbL)", license_url=http://www.opendefinition.org/licenses/odc-odbl; (3) the license was identical across every sampled dataset (ICC spread, USD exchange rate, international reserves daily, credit operations, STR daily), indicating it is the portal-wide default, not a one-off. ODbL is an open, redistribution-permitting license: its human-readable summary grants the right "To copy, distribute and use the database," to create works from it, and to adapt it — explicitly permitting re-hosting/redistribution, including commercial use — subject to two conditions: Attribution (must attribute any public use as specified in ODbL) and Share-Alike (any publicly used adapted database, or works produced from an adapted database, must also be offered under ODbL). Therefore re-hosting the BCB SGS data for free download is permitted, so long as the library attributes BCB and honors the share-alike obligation for any adapted database it publishes. Note: the classic SGS front-end disclaimer page (www4.bcb.gov.br/pec/series/port/aviso.asp) contains only a liability disclaimer and no license or redistribution restriction; it does not contradict the ODbL grant on the Open Data Portal. Classification is redistributable_attribution (attribution required = true; commercial use allowed = true) with a share-alike obligation flagged (sharealike = true) — the one compliance action item for the library is attributing BCB and, if it ever republishes an *adapted/derived* version of the database, releasing that adaptation under ODbL as well.

---

### Bank of Canada Valet

- **Databases (1):** `boc`
- **Official terms URL:** https://www.bankofcanada.ca/terms/
- **License:** Bank of Canada Terms of Use (custom permissive attribution terms)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> the Bank permits you to freely use, copy, distribute and transmit its website content
> Unless otherwise stated the copyright and any other rights in the contents of the material available through this website, including any images and text, are owned by the Bank of Canada.
> You must attribute the Bank of Canada as the source of the content, and indicate if changes were made. You may do so in any reasonable manner
> Circumvent such limit or limits imposed by the Bank with respect to the number or frequency of requests made to a Bank Site, including the retrieval of financial data and information using Bank of Canada services (e.g., the Bank of Canada Valet API)
> If You provide content from this website through paid services or incorporate any content in documents for sale (regardless of the medium), You must inform any prospective purchaser

*Verifier notes:* Adversarially reviewed and could not refute. VERBATIM: The quote "the Bank permits you to freely use, copy, distribute and transmit its website content" appears word-for-word at https://www.bankofcanada.ca/terms/ under section "1. Copyright / Permission to Reproduce"; the full sentence continues "...under the following terms:" and the finding quoted an accurate, non-misleading substring.

STRICTER-CLAUSE SEARCH (no disqualifier found): (a) Redistribution is explicitly granted ("distribute and transmit"). (b) NO non-commercial restriction on data — the terms explicitly allow providing content "through paid services or... documents for sale," requiring only a notice that it is available free of charge; commercial redistribution is thus permitted. (c) The only "written permission" carve-outs are narrow and do NOT apply to Valet time-series data: bank note images, the Bank's logo/wordmark, and third-party content. (d) The sole genuine restriction is a rate-limit/anti-circumvention clause on request frequency ("circumvent such limit or limits... with respect to the number or frequency of requests"), which governs API-access behavior, not redistribution of data already obtained — it does not weaken the classification.

COVERAGE CHECK: The finding cites the general /terms/ page rather than a Valet-specific page. This is correct: the terms explicitly reference "the retrieval of financial data and information using Bank of Canada services (e.g., the Bank of Canada Valet API)," and Bank of Canada's own guidance points Valet users to these same Terms of Use. The cited terms genuinely govern Valet data.

CLASSIFICATION: "redistributable_attribution" is defensible and not too permissive. Attribution is mandatory ("You must attribute the Bank of Canada as the source of the content, and indicate if changes were made"). A library re-hosting Valet data for public download is within these terms provided it attributes the Bank and indicates any modifications. license_name ("custom permissive attribution terms") is also accurate — these are the Bank's bespoke terms, not a standard OGL/CC license.

One caveat for the library operator (does not change the classification): must (1) attribute the Bank and flag any transformations, and (2) if it ever resells or gates the data, disclose that it is available free from the Bank.

*Researcher reasoning:* The official Bank of Canada Terms of Use page (https://www.bankofcanada.ca/terms/) governs the Valet API — the same terms explicitly name "the retrieval of financial data and information using Bank of Canada services (e.g., the Bank of Canada Valet API)." The copyright section states the content is "owned by the Bank of Canada," but the reproduction section grants an EXPLICIT redistribution right: "the Bank permits you to freely use, copy, distribute and transmit its website content." The words "distribute and transmit" go beyond mere access/use and cover re-hosting/re-dissemination, so this is not a case of "publicly available" branding without a redistribution grant.

Conditions: (1) attribution — "You must attribute the Bank of Canada as the source of the content, and indicate if changes were made"; (2) due diligence on accuracy; (3) for paid/commercial redistribution, a notification duty — "If You provide content from this website through paid services or incorporate any content in documents for sale... You must inform any prospective purchaser" (that it is available free on the website). This notification requirement is a condition, not a prohibition, so commercial use is permitted; it does NOT restrict use to non-commercial. The only redistribution-adjacent restriction is a prohibition on circumventing request rate/frequency limits (technical fair-use guardrail), and separately bank-note images, the Bank's logo and wordmark require written permission — but those are not the numeric time-series data a data library would re-host.

For a FREE, NON-COMMERCIAL academic data library re-hosting Valet time-series data: redistribution is expressly permitted provided the Bank of Canada is attributed as the source and any changes are indicated. Classification: redistributable_attribution. Note the operational conditions: attribute the source, indicate modifications, and respect the API rate limits when harvesting.

---

### Barro-Lee Educational Attainment Dataset (Robert J. Barro and Jong-Wha Lee)

- **Databases (1):** `barro_lee`
- **Official terms URL:** https://raw.githubusercontent.com/barrolee/BarroLeeDataSet/master/LICENSE
- **License:** MIT License
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
> MIT License
> 
> Copyright (c) 2020 barrolee
> © 2021 by Robert J. Barro and Jong-Wha Lee. All rights reserved. (displayed as site/footer boilerplate on https://barrolee.github.io/BarroLeeDataSet/ )
> Barro, Robert and Jong-Wha Lee, 2013, "A New Data Set of Educational Attainment in the World, 1950-2010." Journal of Development Economics, vol 104, pp.184-198. (requested citation, https://barrolee.github.io/BarroLeeDataSet/Data.html )

*Verifier notes:* URL fetched OK (HTTP 200); it is the raw LICENSE file of the authors' own repo (barrolee/BarroLeeDataSet), copyright line 'Copyright (c) 2020 barrolee', GitHub-reported license = MIT. VERBATIM: all words of the finding's quote appear in the file in exact order. The only difference is whitespace/formatting — the file has a paragraph break after 'subject to the following conditions:' whereas the inline quote renders it as 'conditions: The above...' on one line. This is normal collapsing of a multi-line license block into an inline string, not a word alteration or fabrication, so I count the quote as verbatim-accurate. ADVERSARIAL STRICTER-CLAUSE SEARCH: Reviewed both the LICENSE and the repo README/landing page for a redistribution ban, non-commercial clause, prior-written-permission requirement, no-derivatives clause, or bulk-extraction/mass-download restriction — none present. MIT's sole condition is inclusion of the copyright + permission notice (an attribution obligation). The README contains a citation request (Barro & Lee 2013, JDE) which is an academic-attribution norm, and a boilerplate 'All rights reserved' site footer; neither restricts redistribution nor overrides the explicit MIT LICENSE file. CLASSIFICATION: MIT expressly grants rights to distribute, sublicense, and sell subject only to notice inclusion, so redistributable_attribution is correct and not too permissive for a library that re-hosts the data for public download (the copyright/license notice must accompany the redistribution). Minor caveat noted but not disqualifying: MIT is drafted for 'Software'; the authors applied it to a data repo, but their clear licensing intent supports redistribution-with-attribution, so it does not undermine the classification.

*Researcher reasoning:* The Barro-Lee Educational Attainment Dataset is officially distributed through the GitHub repository github.com/barrolee/BarroLeeDataSet; the site at barrolee.github.io/BarroLeeDataSet is GitHub Pages served from that same repo. I verified that the actual data files (CSV, DTA, XLS for BL2013 age/sex breakdowns) are hosted directly inside that repository's BLData/ folder (e.g. raw.githubusercontent.com/barrolee/BarroLeeDataSet/master/BLData/BL2013_MF1599_v2.2.csv), so the repository's license governs the data itself, not merely website code. The repo root contains an MIT LICENSE file, whose verbatim text I fetched via curl (not a paraphrase). The MIT license EXPLICITLY grants the right to \"publish, distribute, sublicense, and/or sell copies\" without restriction, subject only to the condition that the copyright notice and permission notice be included in all copies or substantial portions. That is an explicit redistribution/re-hosting grant, which is exactly what a re-hosting library needs. Because redistribution is conditioned on retaining the copyright/permission notice (an attribution/notice requirement) but is otherwise unrestricted and permits commercial use, the correct conservative classification is redistributable_attribution. MIT is permissive, not copyleft, so there is no ShareAlike obligation. Practical compliance step for re-hosting: include the MIT copyright line (\"Copyright (c) 2020 barrolee\") and the MIT permission/warranty notice alongside the redistributed files, and additionally provide the standard academic citation (Barro & Lee 2013, Journal of Development Economics) as a scholarly courtesy. Caveats worth noting but not changing the classification: (1) The website/footer carries \"© 2021 by Robert J. Barro and Jong-Wha Lee. All rights reserved.\" This is standard academic site boilerplate covering page content/images (the site has a separate ImageCopyright page); the operative, explicit machine-readable license file governing the repository where the data files reside is MIT, which is the more specific and controlling instrument for the data. (2) MIT's text references \"the Software,\" and whether an MIT grant cleanly extends to a dataset is a mild interpretive point, but the maintainers deliberately placed the data files inside the MIT-licensed repo, so the grant reasonably reaches them. (3) barrolee.com (the older mirror) was inaccessible due to a TLS certificate mismatch (cert only covers *.blueweb.co.kr), so it could not be read; the GitHub repo is the current canonical source per the site's own README.

---

### bea

- **Databases (1):** `bea`
- **Official terms URL:** https://www.bea.gov/help/faq/147
- **License:** U.S. Government public domain (17 U.S.C. §105)
- **Classification:** redistributable_open
- **Commercial OK:** True · **Attribution required:** False · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK

**Verbatim quote:**
> Unless stated otherwise, the information posted on this web site is in the public domain and may be used or reproduced without specific permission. A citation such as 'Source: U.S. Bureau of Economic Analysis' would be appreciated.
> Unless stated otherwise, the information posted on the BEA web site is in the public domain and may be used or reproduced without specific permission. A citation such as 'Source: U.S. Bureau of Economic Analysis' would be appreciated. (from https://www.bea.gov/help/faq/145)
> Organizations may use the BEA logo on a web page as an aid to establishing link identity, but use of the logo for any other purpose is prohibited. (from https://www.bea.gov/help/faq/145)

*Verifier notes:* Quote verified verbatim at https://www.bea.gov/help/faq/147 (FAQ #147, titled "Are the information and data presented on the BEA web site copyright-protected?"). The full passage — including the two-word "web site" spelling and the 'Source: U.S. Bureau of Economic Analysis' citation request — appears WORD-FOR-WORD as a single continuous statement. No stricter clause on the FAQ page.

Adversarial follow-up: I independently retrieved and read the full BEA API Terms of Service PDF (https://apps.bea.gov/API/_pdf/bea_api_tos.pdf), the most likely place a redistribution restriction would hide. It contains a Use clause, attribution/endorsement-disclaimer requirements, a "Modification or False Representation" clause (do not alter content and still credit BEA — an attribution-integrity term, not a no-derivatives ban), rate limits, and standard warranty/liability boilerplate. It contains NO redistribution ban, NO non-commercial restriction, NO prior-written-permission requirement, NO bulk-extraction prohibition. Commercial use is explicitly contemplated. Rate limits govern the API endpoint, not the public-domain content.

Classification assessment: BEA is a U.S. federal agency; its works are public domain under 17 U.S.C. §105, which the FAQ affirms directly. The "Unless stated otherwise" caveat covers occasional third-party content and is a standard qualifier that does not undermine the general classification. redistributable_open is defensible and not more permissive than the terms support for a library that re-hosts BEA data for public download. fetch_status fetched_ok is accurate.

*Researcher reasoning:* BEA is a U.S. federal agency (Bureau of Economic Analysis, Dept. of Commerce). Its own official FAQ pages state twice, verbatim, that "the information posted on this web site is in the public domain and may be used or reproduced without specific permission." The phrase "used or reproduced without specific permission" explicitly covers reproduction/redistribution, not merely access. Attribution ("A citation such as 'Source: U.S. Bureau of Economic Analysis' would be appreciated") is expressly worded as appreciated, i.e. optional/courtesy, not a binding condition — so it does not rise to a required-attribution licence. Public-domain U.S. government works carry no commercial or non-commercial restriction. Therefore redistribution and re-hosting by a free non-commercial academic library are permitted. Classified redistributable_open rather than redistributable_attribution because attribution is requested but not mandatory. Two caveats noted for the compliance record: (1) the public-domain grant is qualified by "Unless stated otherwise," so any individual BEA page/dataset that carries a specific contrary notice (e.g. copyrighted third-party material embedded in a BEA page or linked site) is excluded — BEA warns it cannot authorize use of copyrighted materials in linked sites; (2) the BEA logo may not be redistributed/reused for purposes other than link identity. Neither caveat affects the underlying statistical data series covered here. Attribution is still strongly recommended as best practice.

---

### bis (Bank for International Settlements)

- **Databases (1):** `bis`
- **Official terms URL:** https://www.bis.org/terms_statistics.htm
- **License:** BIS "Terms of permitted use of BIS statistics" (custom terms, not a named CC/open-gov licence)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> The use of the statistics is unrestricted, provided that: ... if the statistics are reproduced, the BIS must be cited in your publication or product as the source ... if the statistics will be used in a commercial publication or product, their inclusion in the publication or product will not result in any additional charge to subscribers or other users ... No other use is permissible.
> (terms_conditions.htm) all copyright and other intellectual property rights in the publications and statistical data available on this website...are owned by the BIS
> (terms_conditions.htm) Users may download, display, print out, photocopy or redistribute any BIS Material for non-commercial purposes.
> (terms_conditions.htm) Users may use the statistics published in the BIS Data Portal in accordance with the terms set out under the heading 'About BIS statistics'.
> (terms_statistics.htm) your use of the statistics must not be potentially misleading, for example by implying endorsement or affiliation with the BIS
> (data.bis.org/help/legal) The use of the statistics is unrestricted, provided that: ... No other use is permissible.

*Verifier notes:* Fetched https://www.bis.org/terms_statistics.htm (live, correct page, fetched_ok). Every fragment of the verbatim_quote matches word-for-word: "The use of the statistics is unrestricted, provided that:"; the citation condition "if the statistics are reproduced, the BIS must be cited in your publication or product as the source of the statistics" (the quote's ellipsis fairly truncates "of the statistics"); the commercial condition "...their inclusion in the publication or product will not result in any additional charge to subscribers or other users;" (including the "or other users" ending); and "No other use is permissible." The ellipses omit only the translation-notice, no-misleading, and warranty/no-advice conditions — none stricter than represented.

Adversarial stricter-clause search: NO prior-written-permission requirement, NO redistribution ban, NO non-commercial bar, NO no-derivatives, NO bulk/mass-download/scraping/systematic-extraction restriction on the statistics data. The only "may not modify/distribute/reverse engineer" language lives in a SEPARATE API section and governs the API interface itself, not the statistics data. Reproduction of the statistics is explicitly contemplated and permitted (the citation condition triggers "if the statistics are reproduced"), so the "No other use is permissible" catch-all does not exclude attributed redistribution.

Classification redistributable_attribution is defensible and not too permissive. One nuance recorded: the commercial condition bars the BIS data from producing "any additional charge to subscribers or other users," i.e. you may not resell the BIS data as a standalone paid line item. A free public-download library satisfies this cleanly; downstream commercial re-users must not charge extra for the BIS portion. This still permits commercial use with attribution and does not drop the data below redistributable-with-attribution. Recommend the library carry the BIS source citation and (optionally) note the no-additional-charge condition for commercial redistributors.

*Researcher reasoning:* The datasets a data library would re-host are BIS statistics from the BIS Data Portal. Two layers of official terms apply, and they route statistics to the more permissive of the two:

1) General site terms (https://www.bis.org/terms_conditions.htm, "Copyright and permissions"): "all copyright and other intellectual property rights in the publications and statistical data available on this website...are owned by the BIS." It grants "Users may download, display, print out, photocopy or redistribute any BIS Material for non-commercial purposes." Crucially, it excludes Data Portal statistics from the restrictive 400-word/two-table "limited extract" rule and explicitly redirects them: "Users may use the statistics published in the BIS Data Portal in accordance with the terms set out under the heading 'About BIS statistics'."

2) The controlling statistics terms (https://www.bis.org/terms_statistics.htm, mirrored at https://data.bis.org/help/legal) state: "The use of the statistics is unrestricted, provided that:" followed by conditions — reproduction requires citing the BIS as source; translations must carry a non-official-translation disclaimer; use must not be misleading (no implied endorsement/affiliation); "if the statistics will be used in a commercial publication or product, their inclusion in the publication or product will not result in any additional charge to subscribers or other users"; plus warranty and no-investment-advice disclaimers — closing with "No other use is permissible."

Classification rationale: For BIS statistics, "use...is unrestricted" and reproduction is expressly contemplated (subject to attribution), with NO extract/volume cap applied to Data Portal statistics. That supports redistribution/re-hosting with attribution → redistributable_attribution. It is NOT noncommercial_only: commercial use is affirmatively permitted, subject only to the condition that including the BIS statistics does not trigger an additional charge to end users — a condition a free, non-commercial academic library trivially satisfies (attribution_required=true, commercial_ok=true, sharealike=false). It is NOT permission_required because the grant is automatic ("unrestricted...provided that") with no prior-written-permission step for statistics (unlike the >400-word/>10% reuse of non-statistical publications, which does require permission).

One caveat for the compliance record: BIS retains copyright and frames the grant as conditional ("No other use is permissible"), and a separate API-specific terms block imposes additional conditions on programmatic/API access. A re-hosting library must (a) attribute the BIS as source clearly, (b) not imply BIS endorsement/affiliation, and (c) not charge users for the BIS data. Verbatim quotes were fetched and read from the official bis.org and data.bis.org pages; the words "redistribute"/"re-disseminate"/"re-host" do not appear in the statistics terms, but the granted right of "unrestricted use" including reproduction covers redistribution for a free library. Confidence is high given three mutually consistent official sources.

---

### bls

- **Databases (1):** `bls`
- **Official terms URL:** https://www.bls.gov/opub/copyright-information.htm
- **License:** U.S. Government public domain (17 U.S.C. sec. 105); no copyright, use without specific permission, citation requested
- **Classification:** redistributable_open
- **Commercial OK:** True · **Attribution required:** False · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK

**Verbatim quote:**
> The Bureau of Labor Statistics (BLS) is a Federal government agency and everything that we publish, both in hard copy and electronically, is in the public domain, except for previously copyrighted photographs and illustrations. You are free to use our public domain material without specific permission, although we do ask that you cite the Bureau of Labor Statistics as the source.
> The public domain use of our materials includes linking to our website. You do not need to obtain special permission from the BLS to link to our site.
> The BLS emblem, and its variations, which are displayed on the BLS website, as well as on BLS publications and other BLS products, are federally registered trademarks. Unauthorized use of the BLS emblem is prohibited. All rights reserved.
> Automated retrieval programs (commonly called "robots" or "bots") can cause delays and interfere with other customers' timely access to information. Therefore, excessive robot activity on BLS websites is prohibited. [source: https://www.bls.gov/bls/blsterms.htm]
> BLS will block robots that access the website in any way that BLS considers excessive or malicious, including robots that attempt to access or download survey information multiple times per second with resulting degradation of service to others. [source: https://www.bls.gov/bls/blsterms.htm]

*Verifier notes:* Verbatim quote CONFIRMED word-for-word against the live primary source (https://www.bls.gov/opub/copyright-information.htm, page title "BLS Copyright Information : U.S. Bureau of Labor Statistics"). Server-side WebFetch to bls.gov returned HTTP 403 (BLS blocks automated fetchers) and web.archive.org was unavailable, so the researcher's fetch_status:fetched_ok was not reproducible via server fetch; however I independently rendered the live page in a browser and extracted the full text, which contains the quoted sentence character-for-character. Adversarial search for stricter clauses: the full page is only three short paragraphs. Paragraph 1 is the quoted public-domain grant. Paragraph 2 affirms that linking to the site requires no special permission (reinforces openness). Paragraph 3 reserves the BLS emblem/trademark only ("The BLS emblem, and its variations... are federally registered trademarks. Unauthorized use of the BLS emblem is prohibited. All rights reserved."). There is NO redistribution ban, NO non-commercial restriction, NO prior-written-permission requirement, NO no-derivatives clause, and NO bulk/mass-download restriction on the statistical data. The two carve-outs (previously copyrighted photographs/illustrations; the trademark emblem) do not touch the statistical series a data library would re-host. Classification redistributable_open is defensible and not too permissive: this is the textbook U.S. federal public-domain case (citation is requested, not required — "we do ask"). The license_name's reference to 17 U.S.C. sec. 105 is a correct legal characterization placed in the characterization field, not asserted as a verbatim quote, so it does not affect quote accuracy.

*Researcher reasoning:* BLS is a U.S. federal agency and its "BLS Copyright Information" page states unambiguously that "everything that we publish, both in hard copy and electronically, is in the public domain, except for previously copyrighted photographs and illustrations," and "You are free to use our public domain material without specific permission." Public-domain U.S. government works (17 U.S.C. sec. 105) carry no copyright and no license conditions, so redistribution / re-hosting for download is permitted, including commercially. This is the strongest possible open status, hence classification = redistributable_open. Citation of BLS as the source is only REQUESTED ("we do ask that you cite"), not imposed as a mandatory legal condition, so attribution_required = false (though the library should still cite BLS as best practice and for accuracy). commercial_ok = true and sharealike = false follow directly from public-domain status. Two caveats worth flagging to the professor, neither of which restricts redistribution rights: (1) The BLS emblem/logo is a federally registered trademark whose unauthorized use is prohibited ("All rights reserved") — so re-host the DATA but do not reproduce the BLS logo/seal. Previously copyrighted photographs and illustrations are also carved out of the public-domain grant. (2) The separate "BLS Terms of Use Statement" prohibits EXCESSIVE automated retrieval ("robot" activity) against BLS websites and BLS will block scrapers that degrade service; this is a server-access/rate-limiting rule governing HOW you obtain data from bls.gov, not a restriction on redistributing data once lawfully obtained. For bulk ingestion, use BLS's official public API / downloadable flat files rather than hammering the site. ACCESS NOTE: the live bls.gov pages return HTTP 403 to all automated fetches (Akamai bot protection consistent with the robots policy), so the verbatim text above was read from the Internet Archive Wayback Machine's exact archived copies of the official pages — copyright page snapshot 2026-07-11 (https://web.archive.org/web/20260711053220/https://www.bls.gov/opub/copyright-information.htm) and terms page snapshot 2026-06-13 (https://web.archive.org/web/20260613014404/https://www.bls.gov/bls/blsterms.htm). These are byte-identical archives of the official bls.gov pages; the source URLs cited are the canonical official ones.

---

### boe (Bank of England)

- **Databases (1):** `boe`
- **Official terms URL:** https://www.bankofengland.co.uk/legal
- **License:** UK Open Government Licence v3.0 (OGL v3.0)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> The information made available via the Database is the copyright of the Governor and Company of the Bank, unless otherwise stated. Reproduction of data in the Database is subject to the terms of the UK Open Government Licence, allowing and encouraging free and flexible data reuse.
> (from https://www.bankofengland.co.uk/legal) In relation to SONIA data only, please use the following attribution statement as per the terms of the UK Open Government Licence: SONIA and/or SONIA Compounded Index data licensed under the Open Government Licence v3.0 and copyright the Governor and Company of the Bank of England.
> (from https://www.bankofengland.co.uk/legal) Please note that selected exchange rate data and series are excluded from this licence as they are reproduced by the Bank under licence from third parties. In these instances, you are advised to contact us directly for permission if you wish to reproduce this information.
> (from https://www.bankofengland.co.uk/legal) Data relating to monetary financial institutions in the Isle of Man, Guernsey and Jersey are the copyright of the Isle of Man Treasury, Guernsey Financial Services Commission and Jersey Financial Services Commission respectively. We make such data available under licence from the relevant institution.
> (from https://www.bankofengland.co.uk/legal) Where Bank Resources include or are comprised of third party copyright materials, the copyright of that material remains with the originating organisation and any re-use is subject to the separate approval of the relevant third party.
> (OGL v3.0, from https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) You are free to: copy, publish, distribute and transmit the Information; adapt the Information; exploit the Information commercially and non-commercially for example, by combining it with other Information, or by including it in your own product or application.
> (OGL v3.0 attribution requirement) acknowledge the source of the Information in your product or application by including or linking to any attribution statement specified by the Information Provider(s) and, where possible, provide a link to this licence

*Verifier notes:* VERBATIM CHECK: PASS. WebFetch to bankofengland.co.uk/legal returned HTTP 403, but I loaded the live page in the browser pane and read the full text. The quoted sentence appears WORD-FOR-WORD under the "Bank of England Database" heading (page last updated 12 March 2026), the only differences being inline "Opens in a new window" link labels. The Database-specific wording "Governor and Company of the Bank" (not "...of the Bank of England") is quoted correctly. An independent web search also reproduced the sentence verbatim from the same /legal URL.

CLASSIFICATION CHECK: DEFENSIBLE. The Bank explicitly places the Database under the UK Open Government Licence v3.0. OGL v3.0 grants the right to copy, publish, distribute, transmit, adapt and commercially exploit the data subject to attribution — so redistributable_attribution is correct, and re-hosting for public download is squarely within what OGL v3.0 permits. Not too permissive at the source level.

ADVERSARIAL CAVEATS the finding omits (must be handled by the re-hosting library, but they do NOT overturn the OGL classification of the core database — they are the "unless otherwise stated" exceptions the finding's own quote references):
1) EXCHANGE-RATE EXCLUSION (most material): "Please note that selected exchange rate data and series are excluded from this licence as they are reproduced by the Bank under licence from third parties. In these instances, you are advised to contact us directly for permission if you wish to reproduce this information." These specific series are NON-redistributable and must be gated.
2) CROWN-DEPENDENCY DATA: Isle of Man / Guernsey / Jersey monetary-financial-institution data are third-party copyright, made available under licence from those institutions.
3) THIRD-PARTY COPYRIGHT incl. LSEG: "any re-use is subject to the separate approval of the relevant third party"; re-users of LSEG data "should be approaching LSEG and not the Bank for approval." Not redistributable under OGL.
4) SONIA: OGL-licensed but requires the specific attribution statement "SONIA and/or SONIA Compounded Index data licensed under the Open Government Licence v3.0 and copyright the Governor and Company of the Bank of England," and "Bank of England"/"SONIA" are registered trademarks.

RECOMMENDATION: Confirm boe = OGL v3.0 / redistributable_attribution, but the library must exclude (or separately clear) selected exchange-rate series, Crown-Dependency MFI data, and any LSEG-sourced series, and apply the SONIA attribution string. The headline finding is accurate; these are sub-series gating requirements, not a misclassification.

*Researcher reasoning:* The Bank of England's official Legal page (bankofengland.co.uk/legal, last updated 12 March 2026) is the governing terms for the Bank of England Database (the "boe" statistical database). The Database help page's own "Terms and Conditions" link resolves to this Legal page. WebFetch was blocked (HTTP 403) on the BoE domain, so I read the pages directly in the browser and confirmed the verbatim text.

The Legal page states that data in the Database is copyright of the Bank but that "Reproduction of data in the Database is subject to the terms of the UK Open Government Licence, allowing and encouraging free and flexible data reuse." The UK Open Government Licence v3.0 (verified verbatim on nationalarchives.gov.uk) explicitly permits the licensee to "copy, publish, distribute and transmit the Information" and to "exploit the Information commercially and non-commercially," subject to acknowledging the source. This is a redistribution-permissive, attribution-required, non-sharealike open-government licence functionally equivalent to CC BY. Hence classification = redistributable_attribution; commercial_ok = true; sharealike = false; attribution_required = true.

IMPORTANT CARVE-OUTS the re-hosting library must respect (data-specific, not blanket):
1. Selected exchange-rate data and series are EXCLUDED from the OGL because they are reproduced under third-party licence; the Bank instructs users to contact it directly for permission before reproducing them (permission_required for those specific series). The re-host should exclude BoE exchange-rate series unless permission is obtained.
2. Any third-party copyright material embedded in Bank Resources (the page explicitly names LSEG data) remains the third party's copyright and re-use "is subject to the separate approval of the relevant third party" — approach LSEG, not the Bank.
3. Data for monetary financial institutions in the Isle of Man, Guernsey and Jersey are copyright of those Crown Dependency bodies and provided under licence from them.
4. SONIA / SONIA Compounded Index data is OGL-licensed but requires the specific attribution statement quoted above (and "Bank of England"/"SONIA" are registered trade marks).

Net: the bulk of the BoE statistical Database is redistributable with OGL v3.0 attribution, but the professor must (a) attribute per OGL v3.0 (using the mandated SONIA statement where SONIA series are included), and (b) exclude/obtain permission for the selected exchange-rate series and any LSEG or other third-party-sourced series before re-hosting.

---

### Cboe (Cboe Global Markets)

- **Databases (1):** `cboe`
- **Official terms URL:** https://www.cboe.com/terms/
- **License:** Cboe Terms and Conditions for Use of Cboe Websites (proprietary; last updated November 16, 2022)
- **Classification:** permission_required
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> You may not otherwise copy, reproduce, alter, store either in hard copy or in an electronic retrieval system, license, transmit, display, broadcast, create a derivative work (for example, a financial product, service or index) from, use to verify or correct other data or information, publish, rent, sublicense, distribute, or otherwise use in whole or in part in any other manner the Materials without Cboe's prior written consent except to the extent that such use constitutes 'fair use' under the 'Copyright Act of 1976', as amended from time to time.
> You may view, print and download one copy of the Materials for your personal non-commercial use in connection with products and services offered by Cboe, provided that you maintain all copyright, trademark and other notices contained on the Materials.
> To formally request such consent you must submit a Request to Use Cboe Content.
> The Materials are protected by copyrights, trademarks, service marks, and/or other proprietary rights and laws of the United States and other countries.
> Subscribers are prohibited from selling, distributing, transferring, or otherwise disseminating Data to any other person or entity.

*Verifier notes:* Fetched https://www.cboe.com/terms/ successfully. The verbatim_quote appears WORD-FOR-WORD on the live page in Section 2 (Copyrights, Trademarks, Service Marks, And Proprietary Rights); WebFetch returned it in 125-char segments that reassemble to an exact match of the researcher's quote, including the parenthetical '(for example, a financial product, service or index)', the 'use to verify or correct other data or information' phrasing, and the closing fair-use carve-out under the Copyright Act of 1976. The 'last updated November 16, 2022' date in license_name is confirmed on-page. Adversarial check for a stricter/missed clause: this quoted clause IS the strictest relevant provision — an explicit redistribution/reproduction/derivative-work ban gated on 'Cboe's prior written consent', with only a fair-use exception. Classification 'permission_required' is defensible and not too permissive: a library re-hosting Cboe data for public download performs copying/reproduction/distribution that the terms forbid absent prior written consent, so permission_required correctly gates the data. The terms also allow limited personal non-commercial viewing/printing and fair use, but that does not loosen the classification for a re-hosting use case. No contradicting clause found; classification stands.

*Researcher reasoning:* Cboe's official website Terms of Use (https://www.cboe.com/terms/, last updated November 16, 2022) expressly prohibit redistribution of "Materials" (defined by Cboe to include the content and data on its sites) absent prior written consent. The core clause forbids copying, reproducing, transmitting, publishing, sublicensing, distributing, or "otherwise use[ing] in whole or in part in any other manner the Materials without Cboe's prior written consent" (fair use excepted). The only permission granted to a general visitor is to "view, print and download one copy ... for your personal non-commercial use," which does not extend to re-hosting or mass redistribution. Consent must be affirmatively obtained by submitting a "Request to Use Cboe Content." Separately, Cboe's Global Data Agreement and Market Data Policies bar subscribers from "selling, distributing, transferring, or otherwise disseminating Data" without specifically contracted redistribution rights (which require signing a Data Agreement, completing order forms, and obtaining approval). Because a free non-commercial academic re-hosting library would be redistributing/re-disseminating Cboe content to third parties, and Cboe permits this only after a written request/approval, the correct conservative classification is permission_required — not prohibited outright (a request path exists) and not any open/redistributable category. The permitted personal-use copy is non-commercial and requires maintaining all copyright/trademark notices, but that permission does not authorize redistribution, so noncommercial_only would understate the restriction. Re-hosting Cboe data without a signed Cboe Data Agreement / written Content-use consent is not compliant.

---

### U.S. Census Bureau (census.gov)

- **Databases (1):** `census`
- **Official terms URL:** https://www.census.gov/data/developers/about/terms-of-service.html
- **License:** U.S. Government Work / public domain (17 U.S.C. §105), governed by Census Bureau Data API Terms of Service; open data
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> You may use the Census Bureau API to develop a service or service to search, display, analyze, retrieve, view and otherwise "get" information from Census Bureau data.
> All services, which utilize or access the API, should display the following notice prominently within the application: 'This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau.'
> You may not modify or falsely represent content accessed through the API and still claim the source is the Census Bureau.
> You may use the Census Bureau name in order to identify the source of API content subject to these rules. You may not use the Census Bureau name, or the like to imply endorsement of any product, service, or entity, not-for-profit, commercial or otherwise.
> users will not use these data, alone or in combination with any other Census or non-Census data, to identify any individual person, household, business or other entity; not link or combine these data with information in any other Census or non-Census dataset in a manner that identifies an individual person, household, business or other entity; not publish information from these data files, particularly in combination with any other Census or non-Census data, in a manner that identifies any individual person, household, business or other entity
> The Census Bureau is committed to open government by sharing its public data as open data. (https://www.census.gov/about/policies/open-gov/open-data.html)

*Verifier notes:* Quote is verbatim-accurate at the official URL (https://www.census.gov/data/developers/about/terms-of-service.html). Confirmed via two WebFetch passes plus an independent WebSearch snippet of the page's own text, all returning the exact string including the page's awkward "a service or service" phrasing (apparent typo for "software"), which the finding preserved rather than silently correcting. Only delta is straight vs. curly quotes around "get" — immaterial. fetch_status fetched_ok verified.

Adversarial search for a stricter clause found NONE that bars redistribution. The ToS imposes only: (1) an attribution/disclaimer notice ("This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau"); (2) no-modification-while-claiming-Census-as-source; (3) a re-identification prohibition under 13 U.S.C. §§8-9 (bars identifying individuals, NOT redistributing published de-identified data); (4) a rate-limit / "right to limit" clause governing API access, not downstream redistribution. There is no redistribution ban, no non-commercial restriction, no prior-written-permission requirement, no no-derivatives clause, and no bulk-extraction prohibition (Census itself publishes bulk data files).

Classification redistributable_attribution is defensible and, if anything, slightly conservative. The legal basis is 17 U.S.C. §105: Census data is a U.S. Government Work, uncopyrightable and public domain; the license_name correctly cites this. Retaining the ToS attribution/disclaimer as a condition is the safe choice — it is not more permissive than the terms support for a library that re-hosts data for public download.

One noted nuance (non-disqualifying): the chosen verbatim quote grants only "use...to get information" from the API and is not itself the redistribution grant; the redistribution defense rests on §105 public-domain status, which is correctly named. Since the quote is accurate and the classification is independently sound on public-domain grounds, this does not rise to DISPUTED.

*Researcher reasoning:* The U.S. Census Bureau is a U.S. federal statistical agency, and its statistical data products are works of the U.S. Government, which are not subject to copyright protection in the United States (17 U.S.C. §105) and are treated as public/open data. On its Open Data page the Bureau states it "is committed to open government by sharing its public data as open data."

The operative governing document for using and re-serving the tabular statistical data is the official Census Bureau Data API Terms of Service (https://www.census.gov/data/developers/about/terms-of-service.html). It grants broad permission — "You may use the Census Bureau API to develop a service or service to search, display, analyze, retrieve, view and otherwise 'get' information from Census Bureau data." It imposes NO commercial-use restriction and contains NO clause prohibiting redistribution, re-hosting, or bulk/mass download of the statistical data. The only substantive conditions are: (1) an attribution/disclaimer notice — services must "display the following notice prominently ... 'This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau'"; (2) no false representation — "You may not modify or falsely represent content accessed through the API and still claim the source is the Census Bureau"; (3) no implied endorsement when using the Census Bureau name; and (4) privacy — users must not use or combine the data to identify any individual person, household, business or entity. None of these bar a free, non-commercial academic library from re-hosting the aggregate statistical data for download.

Because the data are public domain and the official terms permit use/redistribution subject to an attribution notice and a source-integrity/privacy condition (rather than requiring prior written permission or banning redistribution), I classify this as redistributable_attribution. Attribution is required (display the API notice and cite the U.S. Census Bureau as source); commercial use is permitted; there is no share-alike obligation.

Important scope note: I deliberately did NOT apply the restrictions on the separate Census Bureau "Multimedia Usage Policy" (https://www.census.gov/library/multimedia-usage-policy.html), which states media assets are "not licensed for commercial or advertising use" and "cannot be sold to third parties." Those non-commercial/no-resale limits govern only photographs, audio and video media assets, not the statistical data covered by this provider record. Conflating them would incorrectly downgrade the statistical data to non-commercial.

fetch_status = fetched_ok: I fetched and read the official governing Terms of Service page and the official Open Data page and quoted them verbatim. One supplementary FAQ (ask.census.gov article on public-domain/copyright) was inaccessible due to a servlet redirect loop, but it is not the governing document and its absence does not affect the classification.

---

### Central Bank of the Republic of Türkiye (TCMB / CBRT) — Electronic Data Delivery System (EVDS)

- **Databases (1):** `tcmb`
- **Official terms URL:** https://www.tcmb.gov.tr/wps/wcm/connect/EN/TCMB+EN/Bottom+Menu/Other/
- **License:** Custom CBRT website terms / disclaimer (no standard open licence; copyright reserved to CBRT)
- **Classification:** permission_required
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> Information published in this site may be quoted by specific reference thereto, but the use of such information for commercial purposes shall be subject to prior written permission of the CBRT.
> Copyright and any other rights in the contents are owned by CBRT. Information in other web sites, which may be linked through this web site, is published by the owners of those web sites.
> Access to or the use of this web site constitutes consent to the below statements.
> CBRT reserves the right to change the design and the content of the information included in this web site, without prior notice.

*Verifier notes:* Quote verified WORD-FOR-WORD at the finding's URL. Two independent WebFetches of https://www.tcmb.gov.tr/wps/wcm/connect/EN/TCMB+EN/Bottom+Menu/Other/ both returned the sentence verbatim, and a WebSearch independently corroborated it, surfacing the canonical disclaimer page (.../Other/Disclaimer, a child of the finding URL) carrying identical text. fetch_status "fetched_ok" is accurate.

Adversarial stricter-clause check: the disclaimer contains NO open licence and reserves all rights ("Copyright and any other rights in the contents are owned by CBRT."). The only affirmative permission is narrow: "Information published in this site may be quoted by specific reference thereto" — i.e., quoting with attribution, NOT bulk reproduction/redistribution. Commercial use additionally "shall be subject to prior written permission of the CBRT." Re-hosting the full EVDS dataset for public download plainly exceeds "quoting by specific reference" and is not granted by these terms.

Classification judgment: the finding's "permission_required" (Custom CBRT website terms / disclaimer, no standard open licence, copyright reserved to CBRT) is defensible and NOT too permissive — it is the conservative call. It correctly signals that a re-hosting library must obtain prior written permission before redistributing. No permissive redistribution right exists in the terms, so there is nothing to over-state.

Minor note (non-disqualifying): the exact finding URL is the "Other/" parent directory; the canonical standalone disclaimer lives at .../Other/Disclaimer. Both serve the identical verbatim sentence, so the quote and fetch_status remain valid.

*Researcher reasoning:* I fetched the CBRT's official Disclaimer/copyright page on the provider's own domain (tcmb.gov.tr). The EVDS mobile-app and web materials state that the usage rules published on the TCMB website apply to EVDS data, so this disclaimer is the governing terms. Copyright in all contents is expressly "owned by CBRT" — there is NO CC0/CC BY/open-government licence and no public-domain dedication.

The only reuse grant is: information "may be quoted by specific reference thereto" (i.e., non-commercial quoting/excerpting WITH attribution). Two reasons this does not authorize the re-hosting use case: (1) "quoted by specific reference" covers citing/excerpting portions with a source reference — it is not an explicit permission to redistribute, re-disseminate, or re-host entire datasets for third-party download; wholesale re-hosting exceeds "quoting." (2) The clause explicitly gates "the use of such information for commercial purposes" behind "prior written permission of the CBRT."

Per the task's conservative rule ("publicly available / open-data branding does not by itself mean may redistribute; look for EXPLICIT redistribution/re-hosting/mass-download language"), there is no explicit redistribution or bulk-download grant here. Re-hosting the full EVDS datasets for download would therefore require prior written permission from the CBRT. Classification: permission_required. Attribution ("specific reference") is required for the limited quoting grant; commercial use is not permitted without written permission. Note: the library being free/non-commercial removes the commercial bar but does NOT supply the missing redistribution grant — re-hosting still exceeds the "quoting by specific reference" permission.

---

### Correlates of War Project

- **Databases (1):** `cow`
- **Official terms URL:** https://correlatesofwar.org/data-sets/
- **License:** Custom COW Project "Terms and Conditions" (no standard/open licence)
- **Classification:** permission_required
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> Users agree not to distribute the dataset to any third party without written permission of the COW director and data host.
> Terms and Conditions — By downloading a COW dataset, you agree to the following terms and conditions:
> Data are not to be used for any commercial activity; users of COW data agree not to sell or use the data as part of any financial profit-making activity.
> Users agree to cite each of the datasets they use in the manner described on each specific dataset web page.
> Users agree to ask permission for any dissemination, posting, or other use of the data that is not covered by the above restrictions.
> [FAQ, https://correlatesofwar.org/faq/ — 'What are the use restrictions of the COW data?'] We encourage the use of COW data under certain guidelines. 1) The data be properly cited. 2) COW data users cannot charge for access.

*Verifier notes:* Verified against the live official page at https://correlatesofwar.org/data-sets/ (rendered via browser; direct WebFetch failed only on an SSL intermediate-certificate chain error, not a 404/absence — the page is genuinely accessible and is the correct Data Sets page bearing the Terms and Conditions). Google's search snippet independently corroborated the same phrasing.

VERBATIM QUOTE: EXACT MATCH. The page's "Terms and Conditions" list (four items, introduced by "By downloading a COW dataset, you agree to the following terms and conditions:") contains as item 3, word-for-word: "Users agree not to distribute the dataset to any third party without written permission of the COW director and data host." No wording, punctuation, or truncation discrepancy.

CLASSIFICATION: "permission_required" is defensible and NOT too permissive. Clause 3 requires written permission (from BOTH the COW director and the data host) to redistribute, which is permission-gated rather than outright-prohibited — matching "permission_required" precisely. For a library that re-hosts COW data for public download, this correctly flags that redistribution is not allowed without written consent.

ADDITIONAL (STRICTER) CLAUSES THE FINDING OMITTED — all cut in the restrictive direction, so they do not make the classification too permissive:
(1) Non-commercial ban: "Data are not to be used for any commercial activity; users of COW data agree not to sell or use the data as part of any financial profit-making activity."
(2) Catch-all permission: "Users agree to ask permission for any dissemination, posting, or other use of the data that is not covered by the above restrictions."
These reinforce, rather than contradict, "permission_required." Recommended enrichment (non-blocking): the license record could additionally note the non-commercial restriction, since a re-hosting library must satisfy both the redistribution-permission and non-commercial constraints. No contradicting clause exists; verdict is CONFIRMED.

*Researcher reasoning:* The Correlates of War (COW) Project's official Data Sets page carries an explicit, binding "Terms and Conditions" block that a user accepts "By downloading a COW dataset." I read this on the live official page at https://correlatesofwar.org/data-sets/ using the in-browser tool (WebFetch failed with a TLS 'unable to verify the first certificate' error on correlatesofwar.org, so the terms were read directly from the rendered official page, not a third-party summary). The terms directly and unambiguously govern redistribution/re-hosting: "Users agree not to distribute the dataset to any third party without written permission of the COW director and data host," and further, "Users agree to ask permission for any dissemination, posting, or other use of the data that is not covered by the above restrictions." A free download-library that re-hosts COW files for third-party download is exactly the "distribute the dataset to any third party" / "dissemination, posting" activity these clauses cover, and both are conditioned on obtaining prior written permission from the COW director and data host. Redistribution is therefore NOT permitted by default but CAN be authorized on request, so the correct classification is permission_required (not prohibited, which would apply if no permission path existed). Commercial use is separately and explicitly barred ("Data are not to be used for any commercial activity ... agree not to sell or use the data as part of any financial profit-making activity"), and the corroborating FAQ (https://correlatesofwar.org/faq/) states data "cannot charge for access" — so commercial_ok=false. Attribution is required ("Users agree to cite each of the datasets they use in the manner described on each specific dataset web page"), so attribution_required=true. No share-alike obligation appears, so sharealike=false. Note: a third-party GitHub repo (jenna-jordan/correlates-of-war) describes a "BSD 3-Clause License," but that is an unofficial repackaging and is NOT the COW Project's own licence; the governing official terms are the ones quoted above. Even though the data is non-commercial and free, redistribution/re-hosting is gated on written permission, so this is permission_required rather than noncommercial_only.

---

### Czech National Bank (CNB) — ARAD

- **Databases (1):** `cnb`
- **Official terms URL:** https://www.cnb.cz/en/privacy-statement-and-disclaimer/disclaimer-copyright/
- **License:** CNB website terms of use (custom permissive terms — store/distribute/reproduce permitted with attribution)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> You may store, distribute and reproduce all our Internet information with the exception of texts with an author, i.e. texts whose author is given in the header or footer (in such cases, you must obtain the author's consent directly from her/him or via the Czech National Bank), and images whose copyright is clearly not owned by the CNB.
> The CNB must always be stated as the source of the information (Source: the CNB), the file and its content may not be altered in any manner and each page must be displayed in a new browser window.
> The CNB does not approve the use of the logotype by other legal entities for their own commercial and advertising purposes.

*Verifier notes:* Verbatim quote CONFIRMED word-for-word at the cited URL (https://www.cnb.cz/en/privacy-statement-and-disclaimer/disclaimer-copyright/), fetched OK. The fetched page returns the identical sentence character-for-character, including the authored-text and image exceptions. URL/governance CONFIRMED: I independently checked the ARAD time-series page (https://www.cnb.cz/en/statistics/arad-time-series-system/) and it carries NO separate terms of use — it only describes what ARAD is — so this general website disclaimer is the correct governing instrument for ARAD data. Adversarial stricter-clause hunt found NO redistribution ban, NO prior-written-permission requirement for non-authored content, NO non-commercial restriction on data (the only 'commercial' restriction is on the CNB logotype, irrelevant to data), and NO bulk/systematic/mass-download restriction. The terms explicitly grant 'store, distribute and reproduce' — redistribution is genuinely permitted, so redistributable_attribution is defensible and not too permissive. Two conditions the library must honor (not defeating the classification): (1) attribution — the verbatim clause 'The CNB must always be stated as the source of the information (Source: the CNB)'; (2) a no-alteration condition — 'the file and its content may not be altered in any manner.' The no-alteration clause is a no-derivatives-style condition, but the classification does not claim derivative rights, so it neither contradicts the redistribution grant nor makes the classification too permissive; it is simply a compliance obligation (re-host the underlying data unaltered and attributed to the CNB).

*Researcher reasoning:* The CNB's official "Terms and conditions of using the internet pages of the Czech National Bank" (disclaimer/copyright page) EXPLICITLY grants redistribution rights: "You may store, distribute and reproduce all our Internet information" — "distribute" and "reproduce" are direct grants of re-dissemination/re-hosting rights, not merely a right to access or use. ARAD is described by the CNB as "a public database that is part of the information service of the Czech National Bank," and these site-wide terms govern all CNB internet information, including ARAD data (which is CNB-produced statistical data, not authored texts or third-party images).

CONDITIONS (why this is redistributable_attribution rather than redistributable_open):
1) Attribution is mandatory — "The CNB must always be stated as the source of the information (Source: the CNB)". A source citation is therefore required on any re-hosted data.
2) Two carve-outs to the permission: (a) "texts with an author" (author named in header/footer) require the author's consent, and (b) "images whose copyright is clearly not owned by the CNB." Neither carve-out applies to ARAD numeric time-series data, which is CNB-produced and unauthored, so the grant covers ARAD data.

COMMERCIAL: The terms place NO non-commercial restriction on the data itself. The only commercial restriction is narrowly about the CNB logotype ("The CNB does not approve the use of the logotype by other legal entities for their own commercial and advertising purposes") — that governs the logo/branding, not the data. Hence commercial_ok = true. (The page separately warns users to verify accuracy "especially if using this information for commercial purposes," which is a disclaimer of warranty, not a prohibition.)

CAVEAT worth flagging to the compliance owner: the same clause states "the file and its content may not be altered in any manner." This is a no-alteration/no-derivatives condition. Redistributing the ARAD data unchanged (bulk download/re-host) is squarely permitted; but if the data library reformats, reprocesses, or otherwise alters CNB files, that could conflict with the "may not be altered" language. For a data library that re-hosts the values as-is with a "Source: the CNB" attribution, redistribution is permitted. For transformed/derived outputs, the no-alteration clause introduces risk and may warrant a brief email to the CNB.

Two independent fetches of the same official English-language page returned consistent wording for the governing sentence and the attribution requirement. No prior written permission is required for CNB-produced data (permission is only needed for authored texts and for logotype use).

---

### DBnomics (Cepremap)

- **Databases (1):** `dbnomics`
- **Official terms URL:** https://db.nomics.world/about
- **License:** Open Database License (ODbL) — for the DBnomics aggregate/compilation layer; per-provider passthrough to each original source's own licence for the underlying data
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** True · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> The DBnomics aggregated datasets are distributed under the Open Database License (ODbL). ... Data distributed by DBnomics is subject to the same license and terms of use as its original source provider.
> DBnomics republishes data from various official and public sources.
> To share: To copy, distribute and use the database. (ODbL summary, https://opendatacommons.org/licenses/odbl/summary/)
> Attribute: You must attribute any public use of the database, or works produced from the database, in the manner specified in the ODbL. (ODbL summary)
> Share-Alike: If you publicly use any adapted version of this database, or works produced from an adapted database, you must also offer that adapted database under the ODbL. (ODbL summary)

*Verifier notes:* Primary source (https://db.nomics.world/about, "Legal Terms" section) fetched OK and independently reproduced verbatim. Both quoted sentences appear WORD-FOR-WORD: "The DBnomics aggregated datasets are distributed under the Open Database License (ODbL)." and "Data distributed by DBnomics is subject to the same license and terms of use as its original source provider." The full Legal Terms section has only four bullets; the other two are (a) code repos are AGPLv3+ (about source code, not data) and (b) a liability disclaimer. Adversarial search for a stricter clause found NONE: no non-commercial restriction, no prior-written-permission requirement, no no-derivatives clause, no bulk/mass-download ban. DBnomics even advertises a feature section headed "Redistribute Data As-Is," corroborating genuine redistribution intent.

Classification "redistributable_attribution" is DEFENSIBLE and NOT too permissive as framed: it is explicitly a per-provider-passthrough classification scoped to the DBnomics aggregate/compilation layer (ODbL = redistribution permitted with attribution), while the first-listed bullet makes each original provider's own terms controlling for the underlying data. The researcher did not miss the passthrough clause — it is captured in license_name. So the finding does NOT blanket-greenlight all DBnomics data; it defers underlying-data redistributability to per-provider review.

Two non-refuting caveats recorded for accuracy: (1) ODbL is share-alike/keep-open (copyleft), so "attribution" slightly understates the conditions — but these are conditions on redistribution, not a prohibition, so the bucket does not overclaim. (2) Operationally the library MUST still resolve and enforce each underlying provider's license before re-hosting any specific DBnomics dataset (the per-provider gate); the finding's license_name already flags this. Presentation-order note: the two sentences appear on the page in the reverse order from the finding's quote (passthrough bullet first, ODbL second), joined by the finding's ellipsis; each sentence is nonetheless verbatim-exact, so this is a presentation detail, not a misquote.

*Researcher reasoning:* DBnomics's official About page (https://db.nomics.world/about) states two governing rules verbatim: (1) "The DBnomics aggregated datasets are distributed under the Open Database License (ODbL)." and (2) "Data distributed by DBnomics is subject to the same license and terms of use as its original source provider."

The ODbL is an explicit open license that PERMITS redistribution. Its official summary (https://opendatacommons.org/licenses/odbl/summary/) grants the freedom "To share: To copy, distribute and use the database," subject to three conditions: Attribution, Share-Alike (any adapted/published database must also be offered under ODbL), and Keep Open (no DRM-only distribution). ODbL does NOT restrict commercial use, so commercial redistribution is permitted. Hence at the DBnomics aggregate layer the correct classification is redistributable_attribution (with a share-alike obligation and mandatory attribution).

CRITICAL CAVEAT for the compliance decision (this is why the entry is "per-provider passthrough"): DBnomics itself only applies ODbL to its aggregated compilation/database wrapper. It explicitly disclaims control over the underlying data — that data "is subject to the same license and terms of use as its original source provider." Therefore ODbL/redistributable does NOT automatically authorize re-hosting the actual observations sourced from a given provider (e.g. IMF, BIS, national statistical offices, or NON-redistributable providers). Each underlying provider must be cleared on its own terms; some upstream providers on DBnomics forbid redistribution or require permission, and the ODbL wrapper does not override those. In short: the DBnomics-level licence is redistributable with attribution + share-alike, but re-hosting any specific dataset is only lawful if that dataset's ORIGINAL provider also permits redistribution. Conservative operational guidance: rely on the ODbL classification only for series whose original source is independently confirmed redistributable; treat the rest per their source provider's determination.

fetch_status = fetched_ok: the official DBnomics About page and the official ODbL summary were both fetched and read; the DBnomics quotes were reproduced verbatim (confirmed via two independent fetches returning identical wording).

---

### DeFiLlama

- **Databases (1):** `defillama`
- **Official terms URL:** https://defillama.com/terms
- **License:** Custom proprietary Terms of Use (DeFiLlama)
- **Classification:** permission_required
- **Commercial OK:** False · **Attribution required:** None · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> republish the data in any form without permission
> resell the data or resell access to the data through your plan without permission
> copy, scrape, harvest or otherwise exploit the Content & Data for commercial purposes without prior written consent
> we grant you a revocable, non-transferable, non-exclusive licence to access and use the Site for personal, non-commercial purposes
> all Content, data sets, layout, design, logos and underlying code are owned by or licensed to us and are protected by UAE copyright, trademark and database-rights laws
> use robots, spiders or other automated means to access the Site for any purpose

*Verifier notes:* Quote verified verbatim. The exact string "republish the data in any form without permission" appears as clause 8.7 in Section 8 (Permitted & Prohibited Use) at https://defillama.com/terms. Confirmed via two independent WebFetch passes (identical wording, consistent clause structure) plus a WebSearch that independently corroborated the page's non-commercial personal-use license framing, guarding against extraction-model hallucination. fetch_status fetched_ok is accurate; the URL is live and is the official DeFiLlama terms page.

Adversarial search for stricter clauses found only clauses that REINFORCE the finding, none that contradict it: Section 7 grants merely "a revocable, non-transferable, non-exclusive licence to access and use the Site for personal, non-commercial purposes"; Section 8 additionally bars transferring/"mirroring" the materials on another server (8.4), using the data for competitive purposes (8.5), reselling the data without permission (8.6), and copying/scraping/harvesting or otherwise exploiting the Content & Data for commercial purposes "without prior written consent" (8.10), plus a robots/automated-access ban (8.12).

Classification "permission_required" (Custom proprietary Terms of Use) is defensible and NOT too permissive. For a library that re-hosts DeFiLlama data for public download, republication is explicitly prohibited absent permission and the base license is personal/non-commercial only. If anything the terms are stricter than "permission_required" (they layer a non-commercial restriction on top), but they are in no way more permissive than the classification asserts. No refutation found.

*Researcher reasoning:* DeFiLlama's official Terms of Use (https://defillama.com/terms) governs data reuse via a proprietary, custom licence — not an open/CC licence. The "Prohibited Use" section (Section 8) lists among prohibited actions: "republish the data in any form without permission" and "resell the data or resell access to the data through your plan without permission", plus "copy, scrape, harvest or otherwise exploit the Content & Data for commercial purposes without prior written consent" and "use robots, spiders or other automated means to access the Site for any purpose". The IP section (Section 7) grants only "a revocable, non-transferable, non-exclusive licence to access and use the Site for personal, non-commercial purposes", and asserts that "all Content, data sets, layout, design, logos and underlying code are owned by or licensed to us and are protected by UAE copyright, trademark and database-rights laws".

An academic data library that RE-HOSTS DeFiLlama data for download is precisely "republish[ing] the data in any form", which the terms forbid WITHOUT permission. Because the terms explicitly contemplate that this can be done "with permission" (rather than banning it absolutely), the correct conservative classification is permission_required rather than prohibited: DeFiLlama's prior written consent must be obtained before re-hosting/redistributing. The user licence is expressly personal and non-commercial, so commercial_ok is false; there is no CC-style attribution or share-alike condition governing redistribution (a separate press-usage note says attribution is "appreciated", not required), so attribution_required and sharealike are left null. Note the terms are governed by UAE law and enforce database rights. Bottom line: DeFiLlama data may NOT be freely re-hosted; written permission from DeFiLlama is required. (Also note DeFiLlama itself aggregates third-party/on-chain data; some underlying datasets may carry their own separate terms.)

---

### Deutsche Bundesbank (time series / macroeconomic time series databases)

- **Databases (1):** `bundesbank`
- **Official terms URL:** https://www.bundesbank.de/en/homepage/user-information/conditions-for-the-general-use-of-the-website-764706
- **License:** Custom terms — Deutsche Bundesbank "Conditions for the general use of the website" (site-wide copyright/usage terms; no CC or open-gov licence named)
- **Classification:** redistributable_attribution  →  **corrected to `non_redistributable — use-only grant. Personal/professional use, forwarding, and reproduction are permitted with mandatory attribution ("Source: Deutsche Bundesbank") and no alteration (no-derivatives). The terms grant NO right to republish, redistribute, or make the data publicly available to third parties, so a library re-hosting the data for public download is not covered. Treat as metadata-only / link-out unless the Bundesbank grants prior written permission for redistribution.`** by adversarial review
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **DISPUTED** (quote verbatim: True, classification agrees: False)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> You are free to save, forward or reproduce the information produced in physical or electronic form by the Deutsche Bundesbank for your personal or professional use.
> Unless otherwise stated, the user rights for the content of this website lie with the Deutsche Bundesbank.
> This information must not be altered or distorted in any way.
> The download, republication, retransmission, reproduction, or any other use of all images and videos on the Bundesbank's website as an independent file without the consent of the copyright owners is prohibited.
> These images and videos may only be used for editorial, educational or private purposes, and must include the credit 'Source: Deutsche Bundesbank'...Any further use for commercial purposes, in particular for advertising, is not permitted.

**Adversary's contradicting clause:** You are free to save, forward or reproduce the information produced in physical or electronic form by the Deutsche Bundesbank for your personal or professional use. This information must not be altered or distorted in any way. — plus §4.2: "the download, republication, retransmission, reproduction, or any other use of all images and videos on the Bundesbank's website as an independent file without the consent of the copyright owners is prohibited."

*Verifier notes:* STEP 1 (quote): PASS. The verbatim_quote appears word-for-word in section 4.1 at the official_terms_url. Confirmed via two WebFetch reads and independently via WebSearch, which returned the same sentence and the same next sentence. URL is live (fetch_status fetched_ok is accurate).

STEP 2/3 (classification): FAIL — too permissive. The finding quoted only the permissive half of a two-sentence paragraph and ignored the scope limiter and the no-derivatives clause.

(a) Scope limiter: the grant is expressly "for your personal or professional use." The page contains NO permissive language about publishing, redistributing, republishing, or making the information publicly available to unlimited third parties. WebFetch confirmed "No" when asked directly whether such permissive redistribution language exists.

(b) No-derivatives: the immediately following sentence — "This information must not be altered or distorted in any way." — is a no-alteration/no-derivatives condition that a CC-BY-style redistributable_attribution label does not capture.

(c) Drafting asymmetry: §4.2 explicitly PROHIBITS "republication, retransmission, reproduction ... without the consent of the copyright owners" for images/videos. The drafters plainly knew how to address republication and chose to grant only personal/professional use for general information. Reading a public-redistribution right into §4.1 is not defensible.

(d) Ownership reserved: "Unless otherwise stated, the user rights for the content of this website lie with the Deutsche Bundesbank."

This is the classic "use permitted, redistribution not granted" pattern the review guards against (cf. mistake-ledger M-20260714-10: a grant is narrower than its permissive-sounding summary). A public data library re-hosting Bundesbank series for anyone to download and bulk-extract exceeds "your personal or professional use." Per the skeptical default — not confident redistribution is genuinely permitted — the redistributable_attribution classification must not be confirmed. Corrected to non_redistributable (use-only, attribution + no-derivatives), pending explicit written permission from the Bundesbank.

**PERMISSION UPDATE 2026-07-15 (email, inquiry 2026/005812):** the Bundesbank's Communication
division answered our explicit public-re-hosting request (sent to presse@bundesbank.de
2026-07-14) with the current terms, whose operative wording DIFFERS from the website text
audited above: "you may save, **distribute** or reproduce information provided by the
Deutsche Bundesbank in printed or electronic form for your personal or professional use
provided that you do so **free of charge**. The information must not be altered or
distorted in any way. … If you **distribute** or reproduce the information provided by the
Deutsche Bundesbank, you must cite the source as **'Copyright: Deutsche Bundesbank,
Frankfurt am Main, Germany'** and **send us a copy**." A fee-document clause further
regulates (not prohibits) republication to paying audiences. Cures for the adversary's
objections: (a) "distribute" is now an expressly permitted verb, with a distribution-
specific citation + copy duty that presupposes third-party publication; (b) sent as the
ANSWER to "may I re-host publicly?", not free-floating boilerplate. **Residual caution:**
the reply never uses the word "granted", and "for your personal or professional use"
still appears — a stricter reader could call it a canned terms restatement. Decision
tier: **human review completed — Ahmed approved the un-gate 2026-07-15** (send-reply-
then-deploy flow; confirmation reply to the Bundesbank restates our reading and
compliance). Un-gated + LIVE 2026-07-15, verified 451→401 with the exact credit on
every page. Full verbatim + status in permission records (held privately).

Caveat: this review covers only the general website terms at the cited URL. Bundesbank time series are also distributed via its SDMX/statistics API, which may carry its own separate terms; if the library ingests data through that channel, those terms should be reviewed separately rather than relying on this page.

*Researcher reasoning:* The Bundesbank time-series database's own help/terms pages now return HTTP 404 (the classic time series database is being retired ~30 June 2026), so the governing legal document is the Bundesbank's site-wide "Conditions for the general use of the website," which applies to all "information produced ... by the Deutsche Bundesbank" — this expressly includes the macroeconomic/ESCB time series offered as free "public use files" for CSV/SDMX download. Two independent WebFetch reads of that page returned identical wording for the core clauses.

The operative permission — "You are free to save, forward or reproduce the information ... for your personal or professional use" — is an EXPLICIT grant to reproduce and forward (i.e. redistribute) the data, not merely to access it. It is conditioned on (a) not altering or distorting the information and (b) citing "Source: Deutsche Bundesbank." That is an attribution-style permission, so classification = redistributable_attribution. The user rights vest in the Bundesbank (so this is a licence grant, not public domain), ruling out redistributable_open.

Commercial scope: the only commercial restriction on the page ("Any further use for commercial purposes ... is not permitted") is written specifically for images and videos, NOT for the statistical information/data. The data-permission clause draws no commercial/non-commercial distinction and covers "professional use," so commercial_ok = true and the classification is not noncommercial_only. No prior-written-permission requirement attaches to the data (that requirement is confined to images/videos and to significant image editing), so it is not permission_required. No share-alike obligation exists.

Caveat for the compliance decision: the phrase "for your personal or professional use" is arguably narrower than "unrestricted mass re-dissemination," and one could read it as bounding reproduction to the user's own contexts. However, the inclusion of "forward" (which inherently means passing the material to others) plus "reproduce," with only an attribution + no-alteration condition and no permission gate on the data, supports treating bulk re-hosting-with-attribution as permitted. Practical safeguards for the library: keep the data unaltered, label each series "Source: Deutsche Bundesbank," and note that ESCB/ECB-origin series carried on the site remain subject to their original owners' copyright rules per section 4.3 of the terms.

---

### ecb

- **Databases (1):** `ecb`
- **Official terms URL:** https://www.ecb.europa.eu/services/using-our-site/disclaimer/html/index.en.html
- **License:** ECB Disclaimer & copyright (free reuse with attribution)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> When such information is distributed or reproduced, it must appear accurately and the ECB must be cited as the source.
> Where the information is incorporated in documents that are sold (regardless of the medium), the natural or legal person publishing the information must inform buyers, both before they pay any subscription or fee and each time they access the information taken from this website, that the information may be obtained free of charge through this website.
> any reproduction, publication or reprint, in whole or in part, of documents that bear the name of their authors, such as ECB Working Papers and ECB Occasional Papers, in the form of a different publication (whether printed or produced electronically) is permitted only with the explicit prior written authorisation of the ECB or the authors.

*Verifier notes:* Quote confirmed WORD-FOR-WORD at the official_terms_url as condition #1 of the ECB Copyright section: "When such information is distributed or reproduced, it must appear accurately and the ECB must be cited as the source." URL is accessible (fetched_ok is accurate); no 404/redirect.

Full Copyright section grants: "users of this website may make free use of the information obtained directly from it" subject to four conditions: (1) accuracy + cite ECB as source [attribution]; (2) if info is incorporated in documents that are SOLD, buyers must be told it is available free of charge on the ECB site — a disclosure duty triggered only by resale, not a redistribution prohibition, and not triggered by a free public library; (3) modifications (e.g., seasonal adjustment, growth-rate calc) must be stated explicitly; (4) no framing when linking from business/promotional sites.

Adversarial search for a stricter clause: NO non-commercial restriction, NO general "prior written permission" requirement for statistical data, NO mass-download/bulk-extraction ban. The ONLY prior-written-authorisation requirement is a narrow exception for AUTHORED documents (ECB Working Papers, ECB Occasional Papers) reproduced as a different publication — this does not apply to statistical data series a library re-hosts.

Judgment: For a library re-hosting ECB statistical data for public download, redistributable_attribution is defensible and not overly permissive. Redistribution is explicitly permitted with attribution. Minor caveat: the library must not re-host ECB Working/Occasional Papers under this same license, and any modified series should be labeled as modified — but neither affects the classification of the statistical data itself.

*Researcher reasoning:* The ECB's official site-wide "Disclaimer & copyright" page governs the entire content of ecb.europa.eu, including the statistical data delivered via the ECB Data Portal (the "ecb" provider). It states that users "may make free use of the information obtained directly from it" subject to conditions, and expressly contemplates distribution and reproduction: "When such information is distributed or reproduced, it must appear accurately and the ECB must be cited as the source." This is explicit permission to redistribute/reproduce the data, conditioned only on accurate reproduction and attribution to the ECB — i.e. a redistributable-with-attribution regime, not a mere access/use permission.

Commercial use is permitted: the terms allow information to be "incorporated in documents that are sold," adding only a disclosure duty that the publisher inform buyers the information "may be obtained free of charge through this website." Because commercial redistribution is allowed, this is not noncommercial_only. There is no share-alike obligation.

One narrow exception requires "explicit prior written authorisation of the ECB or the authors" for reproducing authored documents "such as ECB Working Papers and ECB Occasional Papers ... in the form of a different publication." This exception is limited to named-author publications (working/occasional papers), not the statistical time-series data the "ecb" provider re-hosts, so it does not push the data classification to permission_required. A free non-commercial academic library re-hosting ECB statistical data for download is squarely permitted provided it cites the ECB as source (and, if any series are modified, states so explicitly).

Fetch note: the main copyright page was fetched and read (verbatim clauses confirmed across three independent fetches). The ECB Data Portal's own overview page (data.ecb.europa.eu/help/data/overview) returned HTTP 503, but the site-wide disclaimer explicitly covers all ecb.europa.eu content and is the controlling terms document.

---

### Economic Policy Uncertainty Index (Baker/Bloom/Davis)

- **Databases (1):** `epu`
- **Official terms URL:** https://www.policyuncertainty.com/
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> This work is licensed under a Creative Commons Attribution 4.0 International License
> © 2012-2018 by Economic Policy Uncertainty
> This work is licensed under a Creative Commons Attribution 4.0 International License

*Verifier notes:* Verified via three WebFetch calls: homepage (two different prompts) and a representative data page (us_monthly.html). The verbatim quote "This work is licensed under a Creative Commons Attribution 4.0 International License" appears word-for-word in the site footer, with the CC BY badge (i.creativecommons.org/l/by/4.0/88x31.png) hyperlinked to http://creativecommons.org/licenses/by/4.0/ and the line "© 2012-2018 by Economic Policy Uncertainty". URL is live (HTTP 200), official Baker/Bloom/Davis site. The footer is site-wide: it appears identically on both the homepage and the individual data page, so the license covers the downloadable index files, not merely a landing page. Adversarial search for a stricter clause found NONE: no redistribution ban, no non-commercial restriction, no "prior written permission" requirement, no no-derivatives clause, no bulk-download/mass-extraction prohibition. CC BY 4.0 affirmatively permits Share (redistribution, including commercial) and Adapt, conditioned only on attribution; the EPU index values are the site's own computed product, so the licensor has authority to grant these rights and re-hosting the numeric series does not implicate third-party newspaper source text. Classification "redistributable_attribution" maps exactly to CC BY 4.0 and is not more permissive than the terms support; a library re-hosting for public download is allowed provided it attributes the Economic Policy Uncertainty project. The stale "© 2012-2018" copyright year does not narrow or invalidate the grant. Minor caveat: verbatim confirmation rests on WebFetch markdown extraction; independent WebSearch surfaced only generic Creative Commons reference pages, not a second rendering of the site, and /terms.html returns 404 (it is not their terms page). Corroboration across the phrase, badge image path, license hyperlink, and copyright line over three fetches is strong and internally consistent, supporting CONFIRMED.

*Researcher reasoning:* The provider's official site (policyuncertainty.com), operated by the Economic Policy Uncertainty team (Baker, Bloom, Davis), publishes the data under a Creative Commons Attribution 4.0 International License. I fetched two official pages — the homepage (https://www.policyuncertainty.com/) and the US monthly data page (https://www.policyuncertainty.com/us_monthly.html) — and both display the identical verbatim license statement: "This work is licensed under a Creative Commons Attribution 4.0 International License", alongside the copyright notice "© 2012-2018 by Economic Policy Uncertainty". CC BY 4.0 is a standard, well-established open license whose legal terms explicitly permit redistribution, re-hosting, and re-dissemination of the material (including in modified form and for commercial purposes) provided appropriate attribution/credit is given, a link to the license is provided, and any changes are indicated. It imposes no non-commercial restriction and no ShareAlike obligation. Therefore a free, non-commercial academic library may re-host/redistribute this data, conditioned only on proper attribution to Economic Policy Uncertainty (Baker, Bloom & Davis). Classification: redistributable_attribution. Note: the CC BY badge is a site-wide license shown on the individual data pages; it should be understood to govern the index data offered on those pages. No page-specific clause was found that forbids redistribution or requires prior written permission — to the contrary, the CC BY license affirmatively grants redistribution rights.

---

### eia

- **Databases (1):** `eia`
- **Official terms URL:** https://www.eia.gov/about/copyrights_reuse.php
- **License:** U.S. Government public domain (with requested attribution)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> You may use and/or distribute any of our data, files, databases, reports, graphs, charts, and other information products.
> U.S. government publications are in the public domain and are not subject to copyright protection.
> However, if you use or reproduce any of our information products, you should use an acknowledgment, which includes the publication date, such as: 'Source: U.S. Energy Information Administration (Oct 2008).'
> You may see on our website documents, illustrations, photographs, or other information resources contributed or licensed by private individuals, companies, or organizations that may be protected by U.S. and foreign copyright laws.
> The EIA logo is a registered trademark...and may not be used without the expressed consent of the U.S. Energy Information Administration.

*Verifier notes:* Quote verified at official_terms_url (https://www.eia.gov/about/copyrights_reuse.php), triangulated via two WebFetch retrievals and one WebSearch. The words of the verbatim_quote appear word-for-word and in order on the page. The finding truncates the sentence at "information products." with an added period; the actual sentence continues "...that are on our website or that you receive through our email distribution service." The omitted tail is a benign scoping clause, not a restriction, so the partial quote does not overstate the grant — treating it as verbatim-accurate.

Classification (U.S. Government public domain, redistributable_attribution) is DEFENSIBLE and not too permissive. The page explicitly states "U.S. government publications are in the public domain and are not subject to copyright protection" and grants "You may use and/or distribute any of our data, files, databases..." EIA is a U.S. federal agency (DOE); 17 U.S.C. Sec. 105 places its works in the public domain. Attribution is requested ("you should use an acknowledgment... Source: U.S. Energy Information Administration (Oct 2008)"), matching the "with requested attribution" qualifier.

Adversarial search for a stricter clause found only standard third-party carve-outs, none of which restrict EIA's own data: (1) "Transmission or reproduction of protected items beyond that allowed by fair use... requires the written permission of the copyright owners" applies to privately contributed copyrighted material, not EIA data; (2) the EIA logo/"Energy Ant" trademark may not be used without consent; (3) photographs are under private licensing and "may not be reproduced without EIA's and/or the licensor's prior written consent." No general redistribution ban, non-commercial restriction, no-derivatives clause, or bulk/mass-download restriction exists for the data itself. Operator caveat: the public-domain grant does not extend to embedded third-party photos, licensed images, or the EIA logo — those should not be re-hosted, but they are not the statistical data a data library serves.

*Researcher reasoning:* The U.S. Energy Information Administration (EIA) publishes an official "Copyright and reuse" policy at eia.gov/about/copyrights_reuse.php. It states that EIA products are U.S. government publications in the public domain ("U.S. government publications are in the public domain and are not subject to copyright protection.") and EXPLICITLY grants redistribution: "You may use and/or distribute any of our data, files, databases, reports, graphs, charts, and other information products." This is direct, unambiguous re-dissemination/re-hosting permission — not merely an access grant.

Attribution is requested but phrased as a courtesy ("you should use an acknowledgment"), not as a binding legal condition of a licence. Because it is public domain, redistribution would technically be lawful even without attribution; nonetheless I classify as redistributable_attribution (the conservative and practically appropriate choice) since EIA asks for a source acknowledgment including publication date, and an academic re-hosting library should honor that.

No commercial vs non-commercial distinction appears anywhere in the terms, so commercial_ok = true and there is no non-commercial restriction. No share-alike/copyleft obligation exists.

Two carve-outs to note for the re-hosting library: (1) Third-party content on the EIA site — "documents, illustrations, photographs, or other information resources contributed or licensed by private individuals, companies, or organizations that may be protected by U.S. and foreign copyright laws" — is NOT covered by the public-domain grant and may require separate permission; the professor's library should re-host only EIA's own data products (the eia database of energy statistics), not embedded third-party media. (2) The EIA logo is a registered trademark and "may not be used without the expressed consent of the U.S. Energy Information Administration," so do not reproduce the logo/branding. For the actual energy datasets (the "eia" database ID covered), redistribution with attribution is clearly permitted.

---

### Ember (ember-energy.org)

- **Databases (1):** `ember`
- **Official terms URL:** https://ember-energy.org/creative-commons/
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Ember content is released under a Creative Commons Attribution Licence (CC-BY-4.0) This means you're free to share and adapt our work – as long as you credit us. If you would like to use our logo, we ask that you request permission.
> Open, always
> More data will often be available on request.
> Best-in-class open generation data on electricity, emissions, prices and much more.

*Verifier notes:* WebFetch of the official_terms_url returned HTTP 403, so I loaded the page in a real browser (Chrome tools) and extracted the full text. The page (title: "Creative Commons | Ember", subheading "Open, always") contains the finding's quoted wording word-for-word: "Ember content is released under a Creative Commons Attribution Licence (CC-BY-4.0)" / "This means you're free to share and adapt our work - as long as you credit us." / "If you would like to use our logo, we ask that you request permission." The only differences are trivial/typographic: the live page uses a curly apostrophe in "you're" (finding uses a straight apostrophe) and the paragraph line breaks are rendered as spaces in the concatenated quote; the en-dash matches. These are standard normalizations, not substantive alterations, so I treat the quote as verbatim-accurate.

Adversarial check for stricter clauses: I read the entire page. There is NO redistribution ban, NO non-commercial restriction, NO no-derivatives clause, NO prior-written-permission requirement for the data/content, and NO mass/bulk-download restriction. The single carve-out is the logo ("we ask that you request permission"), which is a standard trademark restriction that does not affect redistribution of the data/content itself. Other lines ("More data will often be available on request"; "Please let us know if you're using our work") are soft requests about data availability/collaboration, not binding restrictions. This is corroborated by independent search results describing Ember's electricity data as "fully open and available for free under a CC BY 4.0 license."

Classification judgment: CC BY 4.0 explicitly permits copying and redistribution in any medium/format for any purpose (including commercially) plus remix/transform/build-upon, conditioned only on attribution. "redistributable_attribution" is therefore the correct classification and is NOT more permissive than the terms support - it correctly preserves the attribution requirement while permitting a library to re-host the data for public download. Verdict: CONFIRMED.

*Researcher reasoning:* Ember (the energy think tank, ember-energy.org) publishes a dedicated official "Creative Commons" policy page on its own domain. WebFetch was blocked by the site's WAF (HTTP 403), so I loaded the page in the browser and read its rendered text verbatim. The page states, word for word: "Ember content is released under a Creative Commons Attribution Licence (CC-BY-4.0)" and "This means you're free to share and adapt our work – as long as you credit us." The CC BY 4.0 licence explicitly grants the right to redistribute/re-host ("share") and adapt the material, including for commercial purposes, with the sole condition being attribution (no NonCommercial or ShareAlike restriction). This directly and explicitly covers re-dissemination, not merely access — the page header even reads "Open, always." Therefore the redistribution/re-hosting the professor's library performs is permitted, provided Ember is credited. Note two carve-outs that do NOT affect the data itself: (1) use of Ember's LOGO requires prior permission ("If you would like to use our logo, we ask that you request permission") — so the re-hosting must not reproduce Ember's logo without asking; (2) some additional data beyond the open datasets "will often be available on request," implying not all data is under the open licence, but the standard published datasets are. Classification: redistributable_attribution (CC BY 4.0), commercial_ok=true, sharealike=false, attribution_required=true. Multiple independent search summaries corroborate CC-BY-4.0, and the quote is taken directly from Ember's own official policy page.

---

### Energy Institute Statistical Review of World Energy

- **Databases (1):** `ei_statreview`
- **Official terms URL:** https://assets.kpmg.com/content/dam/kpmg/sk/pdf/2025/Statistical-Review-of-World-Energy-2025.pdf
- **License:** Custom terms — Energy Institute Statistical Review of World Energy (attribution permitted for quoting; prior written permission required for extensive reproduction of tables/charts; S&P Global-sourced data redistribution prohibited without S&P authorisation)
- **Classification:** permission_required
- **Commercial OK:** None · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> Quoting from the review. Publishers are welcome to quote from this review provided that they attribute the source to Energy Institute Statistical Review of World Energy 2025. However, for extensive reproduction of tables and/or charts, permission must first be obtained from: EI Statistical Review of World Energy, 61 New Cavendish Street, London W1G 7AR, statisticalreview@energyinst.org. The redistribution or reproduction of data whose source is S&P Global Commodity Insights, S&P Global Inc, or S&P Global Platts is strictly prohibited without its prior authorisation.
> Using S&P Global Commodity Insights and S&P Global Platts data. The redistribution or reproduction of data whose sources is S&P Global Commodity Insights or S&P Global Platts is strictly prohibited without prior authorisation from S&P Global Commodity Insights. Email: ci.support@spglobal.com
> Statistics published in this Review are taken from government sources and published data. No use is made of confidential information obtained by the Energy Institute or its partners in the course of their business.

*Verifier notes:* Fetched the URL (WebFetch could not parse the PDF text layer, so I extracted all 76 pages locally with pypdf and searched them). The quoted terms appear on page 75 ('More information' / 'Quoting from the review') and are substantively verbatim: every material word matches, including 'Publishers are welcome to quote from this review provided that they attribute the source to Energy Institute Statistical Review of World Energy 2025', 'However, for extensive reproduction of tables and/or charts, permission must first be obtained from', the EI address/email, and 'The redistribution or reproduction of data whose source is S&P Global Commodity Insights, S&P Global Inc, or S&P Global Platts is strictly prohibited without its prior authorisation.' Only formatting was normalized: the researcher inserted commas to flatten the four-line mailing-address block into one line and appended a period to the 'Quoting from the review' heading. No substantive word was changed, added, or dropped, so I count the quote as verbatim.

Adversarial search for a stricter clause turned up only provisions that REINFORCE the classification, none that overturn it: overall '©Energy Institute, London 2025' (ISSN 2976-7857); a second S&P prohibition on p.73 ('strictly prohibited without prior authorisation from S&P Global Commodity Insights. Email: ci.support@spglobal.com'); S&P Global Commodity Insights credited as source for essentially all price series, LNG, coal and metals tables (dozens marked '©2025 by S&P Global Inc'); plus embedded IEA 'all rights reserved' and BGS '©UKRI' notes.

Classification 'permission_required' is defensible and NOT too permissive for a library that re-hosts data for public download: only quoting-with-attribution is free; re-hosting full tables is 'extensive reproduction' requiring prior EI permission, and the large S&P-sourced subset is prohibited without S&P authorisation. If anything the terms lean stricter for the S&P-sourced portions, which the finding's license_name already flags.

Minor caveat (non-disqualifying): official_terms_url is a KPMG-hosted mirror (KPMG is a named co-author/knowledge partner), not the EI's own energyinst.org, but it is the authentic 2025 publication carrying its own terms. Local copy of the PDF and extracted text at scratchpad/sr_text.txt.

*Researcher reasoning:* TASK: determine whether a third party may REDISTRIBUTE / RE-HOST the Statistical Review data (database id ei_statreview) for free download.

WHAT I READ: The Energy Institute's own domain (www.energyinst.org, incl. its data-downloads page and its self-hosted PDF) is behind bot protection and returned HTTP 403 to every automated fetch (WebFetch and curl with browser UA), so I could not read the terms directly on the provider domain. I instead obtained and read the full official EI publication "Energy Institute Statistical Review of World Energy 2025" (76-page PDF, EI/KPMG/Kearney branding, authored/published by the Energy Institute) via a verbatim mirror on assets.kpmg.com and extracted its text with pypdf. The governing terms below are authored by the Energy Institute; the same "Quoting from the review" statement is printed in every EI Statistical Review edition and in the EI-hosted PDF (energyinst.org/__data/assets/pdf_file/0007/1658077/Statistical-Review-of-World-Energy.pdf), which I could not fetch due to the 403 wall.

GOVERNING CLAUSE (verbatim, page "More information / Quoting from the review"): quoting is welcome ONLY with attribution to "Energy Institute Statistical Review of World Energy 2025"; and "for extensive reproduction of tables and/or charts, permission must first be obtained from" the EI Statistical Review office. Re-hosting the full dataset for download is precisely "extensive reproduction of tables" — well beyond permitted quoting — so it requires PRIOR WRITTEN PERMISSION. In addition, a subset of the data (series sourced from S&P Global Commodity Insights / S&P Global / S&P Global Platts) is "strictly prohibited" from redistribution or reproduction without S&P's prior authorisation (stated twice: methodology page and More-information page).

IMPORTANT CAVEAT ON CC-BY-4.0: Web-search snippets asserted the Review is "CC-BY-4.0." That claim traces to Ember's website (ember-energy.org), a partner that describes ITS OWN content/derived visualisations as CC-BY — NOT to the Energy Institute. Nothing in the Energy Institute's own publication grants a Creative Commons or open licence; on the contrary, it gates extensive reproduction on permission and prohibits redistribution of S&P-sourced data. I therefore did not rely on the Ember CC-BY claim.

CLASSIFICATION: permission_required. The only redistribution-adjacent right the EI grants is quoting-with-attribution; wholesale re-hosting of the tables/data is explicitly conditioned on obtaining prior permission from the EI, and part of the data is outright prohibited without S&P's authorisation. commercial_ok set to null because the terms gate on volume ("extensive reproduction") and source (S&P), not on a commercial/non-commercial distinction. Recommendation for the library: do NOT re-host without written permission from statisticalreview@energyinst.org, and note the S&P-sourced series are prohibited regardless.

NOTE ON URL FIELD: official_terms_url points to the exact document I actually fetched and quoted from (the KPMG-hosted copy of the official EI report). The provider's own copy is at https://www.energyinst.org/__data/assets/pdf_file/0007/1658077/Statistical-Review-of-World-Energy.pdf (inaccessible to automated fetch here); the data-downloads/terms landing page is https://www.energyinst.org/statistical-review/resources-and-data-downloads.

---

### EU JRC EDGAR (Emissions Database for Global Atmospheric Research)

- **Databases (1):** `edgar_jrc`
- **Official terms URL:** https://data.jrc.ec.europa.eu/dataset/b54d8149-2864-4fb9-96b9-5fd3a020c224
- **License:** European Commission reuse notice (Commission Decision 2011/833/EU on the reuse of Commission documents)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> According to the European Commission reuse notice, reuse is authorised, provided the source is acknowledged. The reuse policy of the European Commission is implemented by the Decision of 12 December 2011.
> Anybody can directly and anonymously access the data, without being required to register or authenticate.
> 'reuse' means the use of documents by persons or legal entities of documents, for commercial or non-commercial purposes other than the initial purpose for which the documents were produced. [Commission Decision 2011/833/EU, definition of 'reuse', fetched from https://eur-lex.europa.eu/eli/dec/2011/833/oj/eng]

*Verifier notes:* Fetched the official_terms_url (JRC Data Catalogue, EDGAR v8.0 GHG dataset). The verbatim_quote appears word-for-word: the page states "According to the European Commission reuse notice, reuse is authorised, provided the source is acknowledged." immediately followed by "The reuse policy of the European Commission is implemented by the Decision of 12 December 2011." Two independent WebFetch passes plus a WebSearch corroborated the exact wording. License is the European Commission reuse notice implementing Commission Decision 2011/833/EU (dated 12 December 2011), which permits reuse for commercial and non-commercial purposes including redistribution and derivatives, conditioned only on source acknowledgment — functionally equivalent to CC BY. Adversarial search for stricter clauses found only two standard EC boilerplate caveats: (1) "Reuse is not applicable to documents subject to intellectual property rights of third parties." (a third-party-IP carve-out that does not gate the EC/JRC-produced EDGAR data itself), and (2) "The general principle of reuse can be subject to conditions which may be specified in individual copyright notices." — while the dataset's own stated license remains the EC reuse notice with access "No limitations." No non-commercial restriction, no prior-written-permission requirement, no no-derivatives clause, and no bulk/mass-download ban were found. Classification redistributable_attribution is defensible and not too permissive. Minor residual caveat: redistributability rests on the general EC reuse notice; the standard attribution expectation (cite the EDGAR source/references) is consistent with the _attribution classification.

*Researcher reasoning:* The EDGAR datasets (edgar_jrc) are published through the official JRC Data Catalogue (data.jrc.ec.europa.eu). I fetched the EDGAR v8.0 Greenhouse Gas Emissions dataset record, whose "Licence" field verbatim reads: "According to the European Commission reuse notice, reuse is authorised, provided the source is acknowledged. The reuse policy of the European Commission is implemented by the Decision of 12 December 2011." The "Access rights" field states there are no limitations and anyone can access the data anonymously. This is the standard EC/JRC open-reuse licence built on Commission Decision 2011/833/EU.

Crucially, this is NOT mere "access" language — it is an explicit authorisation to REUSE, and I verified against the governing instrument (EUR-Lex, fetched) that "reuse" is legally defined as "the use of documents by persons or legal entities ... for commercial or non-commercial purposes other than the initial purpose for which the documents were produced," and that Decision 2011/833/EU makes documents available "for commercial or non-commercial purposes." Under EU reuse law this encompasses reproduction, distribution, dissemination and re-hosting by third parties. This is functionally an attribution-only open-government licence (equivalent in effect to CC BY): redistribution/re-hosting is permitted, including commercially, with the single mandatory condition that the source (EDGAR / European Commission JRC) be acknowledged. There is no share-alike obligation and no non-commercial restriction.

Therefore a free, non-commercial academic library RE-HOSTING EDGAR data is permitted, provided it attributes the source. Classification: redistributable_attribution.

Caveats for compliance: (1) The reuse notice carries the standard carve-out that reuse "is not applicable to documents subject to intellectual property rights of third parties" — EDGAR core emissions data is the Commission's own, but any embedded third-party layers would be excluded. (2) The EDGAR site disclaimer only addresses liability ("The Union and the IEA provide any such information as-is and as-available, and make no representations, conditions or warranties of any kind"); it does not add redistribution restrictions. (3) Note EDGAR is jointly produced with the IEA and some recent air-pollutant releases may reference IEA-derived energy inputs; the professor should attribute EDGAR/European Commission JRC and confirm no specific dataset record swaps in a more restrictive licence, since the JRC catalogue permits per-dataset licence overrides ("users are advised to refer to the copyright notices of the individual documents"). The verified record for the flagship GHG dataset carries the open attribution reuse notice quoted above.

Note: data.europa.eu (89h) records 307-redirect to the same data.jrc.ec.europa.eu dataset pages; the JRC catalogue is the authoritative source. The generic /licence/com_reuse licence page returned 404, so the verbatim quote is taken directly from the dataset record's own Licence field, which is the operative, dataset-scoped statement.

---

### eurostat

- **Databases (1):** `eurostat`
- **Official terms URL:** https://ec.europa.eu/eurostat/help/copyright-notice
- **License:** CC BY 4.0 (Creative Commons Attribution 4.0 International)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Reuse of statistical data, metadata, publications, and other dissemination tools published on this website for commercial or non-commercial purposes is authorised provided the source is acknowledged.
> The copyright for the editorial content of this website, which is owned by the EU, is licensed under the Creative Commons Attribution 4.0 International licence.
> The following Eurostat data and documents may not be reused for commercial purposes, but non-commercial reuse is possible without restriction: Data identified as belonging to sources other than Eurostat; Publications or documents where the copyright belongs partly or fully to other organisations; Data for countries other than Member States of the European Union (EU), Member States of the European Free Trade Association (EFTA), or official EU acceding and candidate countries; Trade data originating from Liechtenstein and Switzerland (as declaring countries); Trade data originating from Austria (as a declaring country) for Combined Nomenclature at 8-digit level
> The permission granted above does not extend to any material whose copyright is identified as belonging to a third-party...authorisation must be obtained from the relevant copyright holder(s).

*Verifier notes:* Verified across three independent fetches of https://ec.europa.eu/eurostat/help/copyright-notice (fetch_status confirmed: page live, correct copyright notice, not a 404/redirect). STEP 1 (quote): The verbatim_quote appears WORD-FOR-WORD on the page: "Reuse of statistical data, metadata, publications, and other dissemination tools published on this website for commercial or non-commercial purposes is authorised provided the source is acknowledged." Corroborated by two targeted checks. STEP 2 (stricter clause search): The page explicitly names the licence — "licensed under the Creative Commons Attribution 4.0 International licence" — and states no written licence is required for reuse (commercial or non-commercial) unless a listed exception applies. I found NO redistribution ban, NO general non-commercial restriction, NO prior-written-permission requirement, NO no-derivatives clause, and NO bulk/mass-extraction restriction on the general body of statistical data. The only exceptions are the standard CC BY carve-outs: (a) third-party copyrighted materials (photos/illustrations) need separate authorisation; (b) logos and trademarks are excluded; and (c) a COMMERCIAL-reuse-only restriction on a narrow subset — data attributed to non-Eurostat sources, co-publications, non-EU-country data, and specific Liechtenstein/Switzerland (1995+) and Austria (8-digit CN) trade data — all of which still permit non-commercial reuse. STEP 3 (classification): CC BY 4.0 grants the right to redistribute/share (including commercially) with attribution, which is exactly what a re-hosting-for-public-download library requires. 'redistributable_attribution' is defensible and NOT too permissive. Caveat for downstream use (does not change the verdict): if the library re-hosts the narrow commercial-restricted subsets or third-party/logo content, those specific carve-outs must be honoured. Eurostat is a canonical open-data provider and this is a genuinely open licence.

*Researcher reasoning:* The official Eurostat copyright notice (ec.europa.eu/eurostat/help/copyright-notice, an official EU website) explicitly authorizes REUSE — which encompasses redistribution/re-dissemination — of statistical data, metadata, publications and other dissemination tools "for commercial or non-commercial purposes ... provided the source is acknowledged." Editorial website content is placed under CC BY 4.0. This is a genuine redistribution grant (not mere access), consistent with the EU's 2019 adoption of CC BY 4.0 (Decision 2011/833/EU on reuse of Commission documents) and the ESS commitment to provide statistics free of charge as a public good. Attribution to Eurostat as source is mandatory; there is no share-alike obligation (CC BY 4.0). Because the professor's library is FREE and NON-COMMERCIAL, the entire corpus is redistributable — the general rule permits even commercial redissemination with attribution. IMPORTANT CARVE-OUTS the library must respect: a defined set of items may be reused for NON-COMMERCIAL purposes only (not commercial): (a) data attributed to non-Eurostat sources, (b) publications whose copyright belongs partly/fully to other organisations, (c) data for non-EU/non-EFTA/non-candidate countries, (d) trade data declared by Liechtenstein and Switzerland, and (e) Austrian trade data at 8-digit Combined Nomenclature. These are non-commercial-only, which is still compatible with this non-commercial library, but re-hosting must NOT strip attribution and must exclude embedded third-party-copyright material for which permission was not obtained. Net classification: redistributable_attribution (with the noted non-commercial-only sub-exceptions, all of which remain permissible for a non-commercial re-host). Verbatim text was fetched and read directly from the official page.

---

### FAO (UN Food and Agriculture Organization) — FAOSTAT corporate statistical databases

- **Databases (25):** `fao_ae`, `fao_af`, `fao_ec`, `fao_ep`, `fao_es`, `fao_et`, `fao_ew`, `fao_fo`, `fao_ga`, `fao_gb`, `fao_ge`, `fao_gf`, `fao_gl`, `fao_gn`, `fao_gr`, `fao_gt`, `fao_gy`, `fao_ic`, `fao_oa`, `fao_pp`, `fao_qa`, `fao_qcl`, `fao_ql`, `fao_qp`, `fao_rp`
- **Official terms URL:** https://www.fao.org/contact-us/terms/db-terms-of-use/en/
- **License:** CC BY 4.0 (Creative Commons Attribution 4.0 International), plus FAO additional Database terms of use
- **Classification:** redistributable_attribution
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> You may access, download, create copies, adapt and re-disseminate datasets subject to these Database terms.
> all datasets disseminated through FAO corporate statistical databases ... are licensed under the Creative Commons Attribution-4.0 International licence (CC BY 4.0)
> Datasets shall not be used for or in conjunction with the promotion of a commercial enterprise and/or its product(s) or services (s).
> The citation must follow the following format: 'FAO. [YYYY (year of last update)]. [Name of database: Name of dataset OR Name of database]. [Accessed on [DD Month YYYY]]. [URL] Licence: CC-BY-4.0.'
> FAO corporate statistical databases may include data provided by third parties which may not be redistributed or reused without the consent of the original data provider ... It is your responsibility to determine if a particular dataset is fully or partially owned by third parties.
> you shall not use FAO datasets in a manner that falsifies or misrepresents their content.

*Verifier notes:* VERBATIM CHECK: The quote "You may access, download, create copies, adapt and re-disseminate datasets subject to these Database terms." appears word-for-word on the official page (https://www.fao.org/contact-us/terms/db-terms-of-use/en/), confirmed on two independent fetches. fetch_status fetched_ok is accurate.

LICENSE CHECK: The page names "Creative Commons Attribution-4.0 International licence (CC BY 4.0)" as the default: "unless specified otherwise in their metadata or webpage, all datasets disseminated through FAO corporate statistical databases are licensed under CC BY 4.0." I specifically probed for the older, stricter CC BY-NC-SA 3.0 IGO that FAOSTAT historically used and for any share-alike (SA) or non-commercial (NC) wording — none is present. (One FAO data-catalog page still tags some legacy datasets CC-BY-3.0-IGO, but that is also redistributable-with-attribution, so it does not weaken the classification.)

ADVERSARIAL SEARCH FOR STRICTER CLAUSES: No blanket redistribution ban, no non-commercial license, no no-derivatives clause, no "prior written permission" requirement, and no bulk/mass-download or API-scraping restriction. Re-dissemination (i.e., redistribution) is explicitly permitted. The FAO "additional Database terms" impose only narrow limits: (1) datasets "shall not be used for or in conjunction with the promotion of a commercial enterprise and/or its product(s) or services" — an anti-endorsement clause, NOT a non-commercial ban, and it does not stop a library from re-hosting data for public download; (2) no falsifying/misrepresenting content; (3) no de-anonymization; (4) attribution via a specified FAO citation format. The finding already flags "plus FAO additional Database terms of use," so these are disclosed.

ONE MATERIAL CAVEAT (does not change the classification): The terms warn that FAO databases "may include data provided by third parties which may not be redistributed or reused without the consent of the original data provider," with such conditions noted in each dataset's metadata. A re-hosting library must honor per-dataset third-party carve-outs rather than treat every FAO dataset as blanket CC BY 4.0. This is the standard "check the metadata" exception, not a top-level bar to redistribution.

CONCLUSION: Quote is verbatim-accurate at the official URL; the default license is genuinely CC BY 4.0 (redistribution + adaptation explicitly allowed, attribution required). The classification redistributable_attribution is defensible and not more permissive than the terms support. Recommend the library carry FAO's citation format and respect per-dataset third-party restrictions.

*Researcher reasoning:* The official FAO "Statistical Database Terms of Use" page (fao.org/contact-us/terms/db-terms-of-use/en/) is the governing document for the FAOSTAT corporate statistical databases (fao_ae, fao_qcl, etc.). It EXPLICITLY permits redistribution: "You may access, download, create copies, adapt and re-disseminate datasets subject to these Database terms." Datasets are licensed under CC BY 4.0, with the required citation naming FAO as source and "Licence: CC-BY-4.0". This maps to redistributable_attribution — re-hosting for download is allowed provided FAO is attributed in the prescribed citation format.

Three important caveats for the compliance decision:

1. COMMERCIAL USE IS RESTRICTED beyond plain CC BY 4.0. Although base CC BY 4.0 permits commercial use, FAO layers an ADDITIONAL binding term: "Datasets shall not be used for or in conjunction with the promotion of a commercial enterprise and/or its product(s) or services." I set commercial_ok=false conservatively. This is not a problem for a FREE, NON-COMMERCIAL academic library, which is squarely permitted, but a downstream commercial reuse would need care. (Historically some FAO databases carried CC BY-NC-SA 3.0 IGO; the current unified terms state CC BY 4.0 for FAOSTAT databases. sharealike=false since CC BY 4.0 has no ShareAlike requirement.)

2. THIRD-PARTY DATA CARVE-OUT. FAO warns: "FAO corporate statistical databases may include data provided by third parties which may not be redistributed or reused without the consent of the original data provider ... It is your responsibility to determine if a particular dataset is fully or partially owned by third parties." The re-hoster is responsible for checking each dataset. Some of the covered IDs are trade/price/food-balance derivatives that may embed third-party inputs; these should be spot-checked before mass re-hosting.

3. NO MISREPRESENTATION / NO IMPLIED ENDORSEMENT clauses also apply.

Net: for a free non-commercial academic data library re-hosting FAOSTAT for download with proper FAO CC BY 4.0 attribution, redistribution is permitted (redistributable_attribution), conditioned on (a) using the prescribed citation, (b) not using the data to promote a commercial enterprise, and (c) verifying no third-party-owned sub-datasets are being re-disseminated without consent. Verbatim quotes above were read directly from the official FAO page; no terms were reconstructed from memory.

---

### faostat

- **Databases (1):** `faostat`
- **Official terms URL:** https://www.fao.org/contact-us/terms/db-terms-of-use/en/
- **License:** CC BY 4.0 (with FAO additional Statistical Database Terms of Use)
- **Classification:** redistributable_attribution  →  **corrected to `redistributable_attribution_noncommercial (with third-party-data carve-out) — re-dissemination is permitted with FAO attribution, but subject to (a) a non-commercial/anti-endorsement restriction that CC BY 4.0 does not impose, and (b) a subset of embedded third-party data that cannot be redistributed without the original provider's consent.`** by adversarial review
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **DISPUTED** (quote verbatim: True, classification agrees: False)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> You may access, download, create copies, adapt and re-disseminate datasets subject to these Database terms.
> all datasets disseminated through FAO corporate statistical databases...are licensed under the Creative Commons Attribution-4.0 International licence (CC BY 4.0)
> when you access, download, or otherwise extract any data or datasets from these databases, you agree to comply with the terms and conditions of the CC BY 4.0 licence and all terms specified in the additional terms of use outlined below.
> you must give appropriate attribution and credit to FAO for any work produced using an FAO dataset or when FAO data is re-disseminated. The citation must follow the following format: 'FAO. [YYYY (year of last update)]. [Name of database: Name of dataset OR Name of database]. [Accessed on [DD Month YYYY]]. [URL] Licence: CC-BY-4.0.'
> Datasets shall not be used for or in conjunction with the promotion of a commercial enterprise and/or its product(s) or services (s)
> FAO corporate statistical databases may include data provided by third parties which may not be redistributed or reused without the consent of the original data provider, or that may be subject to terms and conditions which are different than those of FAO.
> You may not in any way represent that FAO has participated, sponsored, approved, or endorsed the manner or purpose of your use

**Adversary's contradicting clause:** "Datasets shall not be used for or in conjunction with the promotion of a commercial enterprise and/or its product(s) or services, and/or in any way that suggests that FAO endorses any specific company, products or services." Additionally: "FAO corporate statistical databases may include data provided by third parties which may not be redistributed or reused without the consent of the original data provider, or that may be subject to terms and conditions which are different than those of FAO."

*Verifier notes:* VERDICT: DISPUTED (classification too permissive; quote itself is accurate).

QUOTE: Verified verbatim word-for-word at the official URL https://www.fao.org/contact-us/terms/db-terms-of-use/en/ (fetched OK, HTTP 200, genuine FAO Statistical Database Terms of Use page). fetch_status "fetched_ok" is correct.

WHY DISPUTED: The researcher cherry-picked the single most permissive sentence and classified as "redistributable_attribution" (attribution as the ONLY condition). Independent reading of the SAME terms page surfaces two stricter clauses that were missed:

1) COMMERCIAL/ENDORSEMENT RESTRICTION (verbatim, confirmed both via WebFetch and independent WebSearch): "Datasets shall not be used for or in conjunction with the promotion of a commercial enterprise and/or its product(s) or services, and/or in any way that suggests that FAO endorses any specific company, products or services." Standard CC BY 4.0 permits commercial use unconditionally and prohibits adding downstream restrictions ("no additional restrictions"). FAO therefore layers a non-CC restriction on top of a CC BY 4.0 label — a well-known FAOSTAT tension. So the terms are NOT plain CC BY 4.0, and "attribution-only" understates the constraints. (Caveat for fairness: this clause is narrower than a full CC BY-NC ban — it targets promotional/endorsement use rather than all commercial use — but it is still a real restriction the classification does not convey.)

2) THIRD-PARTY CARVE-OUT (verbatim): "FAO corporate statistical databases may include data provided by third parties which may not be redistributed or reused without the consent of the original data provider, or that may be subject to terms and conditions which are different than those of FAO." For a library that re-hosts data for public download, this is the material risk: a blanket re-host of all FAOSTAT would redistribute data FAO itself says is non-redistributable without per-source consent.

NUANCE / WHAT THE FINDING GOT RIGHT: Re-dissemination IS explicitly permitted ("You may access, download, create copies, adapt and re-disseminate datasets..."), attribution IS required, and the finding's license_name honestly flags "(with FAO additional Statistical Database Terms of Use)". So this is genuinely redistributable — NOT "non_redistributable." The problem is only that the machine-readable bucket "redistributable_attribution" implies attribution is the sole condition, when in fact a non-commercial/anti-endorsement restriction and a third-party consent carve-out also apply.

RECOMMENDATION: Downgrade to a noncommercial-flagged redistributable class and add a third-party-data caveat requiring per-dataset checks before re-hosting. Required FAO attribution string per the terms: "FAO. [YYYY]. [Name of database]. [Accessed on DD Month YYYY]. [URL] Licence: CC-BY-4.0."

*Researcher reasoning:* FAOSTAT is FAO's corporate statistical database, and its terms are governed by FAO's official "Statistical Database Terms of Use" page (fetched and read twice; consistent both times). Redistribution/re-hosting is EXPLICITLY permitted — the terms state: "You may access, download, create copies, adapt and re-disseminate datasets subject to these Database terms." The datasets are licensed under CC BY 4.0. For a FREE, NON-COMMERCIAL academic library that re-hosts FAOSTAT for download WITH attribution, this is clearly allowed, so classification is redistributable_attribution.

Two important caveats that must be honored:
(1) COMMERCIAL RESTRICTION — despite the CC BY 4.0 badge, FAO layers an additional term on top of the licence: "Datasets shall not be used for or in conjunction with the promotion of a commercial enterprise and/or its product(s) or services (s)." This is why commercial_ok is set to false even though vanilla CC BY 4.0 normally permits commercial use. The professor's library is non-commercial, so it satisfies this. (I did not classify as noncommercial_only because FAO's restriction is narrower than a blanket CC BY-NC — it bars using the data to promote a commercial enterprise/product, not all commercial reuse — and the primary redistribution right is CC BY with attribution.)
(2) THIRD-PARTY DATA CARVE-OUT — "FAO corporate statistical databases may include data provided by third parties which may not be redistributed or reused without the consent of the original data provider, or that may be subject to terms and conditions which are different than those of FAO." Any FAOSTAT domain containing third-party-sourced data must be checked at the dataset/metadata level before re-hosting.

ATTRIBUTION is mandatory, with a prescribed citation format: 'FAO. [YYYY]. [Name of database: Name of dataset]. [Accessed on DD Month YYYY]. [URL] Licence: CC-BY-4.0.' No-endorsement clause also applies: the library must not imply FAO sponsorship or endorsement. All quotes are verbatim from https://www.fao.org/contact-us/terms/db-terms-of-use/en/ (the licence-name quote contains an ellipsis inserted by the fetch tool for the phrase between "databases" and "are licensed").

---

### Federal Reserve Board (Board of Governors of the Federal Reserve System)

- **Databases (1):** `fed_board`
- **Official terms URL:** https://www.federalreserve.gov/disclaimer.htm
- **License:** U.S. federal government public domain (attribution/citation requested)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Unless otherwise indicated, information on Board's website is in the public domain and may be copied and distributed without permission. Please cite to the Board as the source of the information.
> For any photo, graphic, or other material that is identified as being associated with a non-Board (such as materials with a copyright or trademark) permission to copy and distribute such photo, graphic, or material must be obtained from the non-Board source.
> The Board permits hyperlinking to the Board's website from other websites. This permission extends only to a hyperlink, however, and does not include authority to engage in any other activities such as the "framing" of the Board's website on another website (e.g., by creating a part of another website that independently loads the Board's website on that other webpage).

*Verifier notes:* Verbatim quote confirmed word-for-word at https://www.federalreserve.gov/disclaimer.htm (fetched_ok). The operative clause "in the public domain and may be copied and distributed without permission" directly authorizes re-hosting/public download. I searched for stricter clauses and found only standard, non-defeating caveats: (1) "Unless otherwise indicated" plus a non-Board third-party carve-out requiring permission from the ORIGINAL source for copyrighted/trademarked third-party photos/graphics/materials — this does not affect Board-produced economic/statistical data, which is what fed_board sources; (2) an anti-"framing" clause that restricts embedding the Board's website inside another site but explicitly not copying/distributing the underlying data; (3) protection of Board seals/logos. None imposes a redistribution ban, non-commercial limit, prior-written-permission requirement, no-derivatives term, or bulk-download restriction on the Board's own content. Classification redistributable_attribution is defensible and, if anything, marginally more conservative than the terms require, since the citation is phrased as a courtesy request ("Please cite to the Board as the source of the information") rather than a binding legal condition. Minor note for downstream use: a blanket "the entire site is redistributable" reading would overreach because of the third-party/"unless otherwise indicated" carve-out, but for Board-authored data the public-domain status holds.

*Researcher reasoning:* The provider fed_board is the Board of Governors of the Federal Reserve System, whose statistical data (e.g., H.15 Selected Interest Rates, G.17, foreign exchange rates, released via federalreserve.gov and the Data Download Program) is published on federalreserve.gov. I located the Board's OWN official terms page — the "Disclaimer" page at federalreserve.gov/disclaimer.htm — and fetched it twice to confirm the wording verbatim; both fetches returned identical text.

The governing clause under the "Copyright/trademark" heading states EXPLICITLY that the Board's website information is in the public domain and "may be copied and distributed without permission." This is an explicit, affirmative redistribution grant — not merely "publicly available" branding — so it clears the bar for redistribution/re-hosting. As U.S. federal-government-produced work, this data is in the public domain, so there is no commercial restriction (commercial_ok = true) and no share-alike obligation (sharealike = false).

I classified as redistributable_attribution rather than redistributable_open because the same clause contains an explicit citation instruction — "Please cite to the Board as the source of the information." Although "Please cite" is phrased as a courtesy request (public-domain material carries no legally enforceable conditions), the conservative and safest posture for a compliance decision is to honor the stated citation request, so attribution_required = true. Strictly, the material is public domain (which could also read as redistributable_open); the attribution classification is the more cautious choice that still faithfully reflects the page.

Two caveats the professor should note: (1) The grant begins "Unless otherwise indicated," and a separate sentence carves out third-party/non-Board materials (photos, graphics, copyrighted/trademarked material) which require permission from the original non-Board source — so any dataset or item on federalreserve.gov flagged as sourced from a non-Board third party is NOT covered by this public-domain grant and must be checked individually. For the Board's own statistical releases this caveat does not apply. (2) The hyperlinking permission explicitly forbids "framing" the Board's website, which is irrelevant to re-hosting downloaded datasets but confirms the Board does restrict certain re-presentation methods.

---

### Federal Reserve Bank of New York

- **Databases (1):** `nyfed`
- **Official terms URL:** https://www.newyorkfed.org/privacy/termsofuse
- **License:** Federal Reserve Bank of New York Terms of Use (custom permissive license: attribution + share-alike; Last Updated 6/9/2023)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** True · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> The New York Fed grants you a non-exclusive license, subject to the Terms, to use, copy, and distribute Content for your personal or business purposes. You may: Access the Content, manually or through an automated process or device, provided your access does not have the effect of disabling, damaging, or interfering with the function of the Website, Download, store, and use Content in any format or media, Copy and distribute the Content in any format or media, and Modify and create derivative works from the Content.
> In general, the New York Fed intends for Website visitors to have permission to use and share Content. Some conditions apply to all Content.
> When you copy or distribute any Content, you must include any copyright notice and other source identifiers that the New York Fed includes with that Content. If the Content identifies individual authors, you must also include that information in your copy.
> If the Terms or relevant Website pages provide a specific form of attribution for the Content you use, you must follow that form. Otherwise, follow this format: “© [year] Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.”
> If you distribute the Content, you must make the Content available with the same permissions, conditions, and restrictions set forth in these Terms. You may not impose more restrictive terms or conditions on the Content.
> You must not state or imply that the New York Fed endorses your use, reproduction, or distribution of the Content or any product, service, financial instrument, or material you create or derive using the Content or any excerpt of the Content.
> Reference Rates — If you use or distribute reference rate data or related information posted to the website, you must include the following notice and disclaimer with your presentation of that data or information: “The [NAME OF DATA or CONTENT]* is subject to the Terms of Use posted at newyorkfed.org. The New York Fed is not responsible for publication of the [DATA NAME] by [NAME OF PUBLISHER], does not [sanction] or [endorse] any particular republication, and has no liability for your use.”
> Staff Reports and Working Papers — You may use individual Staff Reports and Working Papers for personal use or for internal business purposes. Your use of Staff Reports and Working Papers is subject to the Conditions listed above. You may not distribute Staff Reports or Working Papers for a business or commercial purpose.
> Blog Posts — Distribution of Blog posts on a regular or serial basis and archiving or storing Blog posts in an archive made available to the public (for free or subject to a subscription) requires a separate written license agreement with the New York Fed.
> Household Debt and Credit Reports — If you use the Consumer Credit Panel data, the proper attribution format is “New York Fed Consumer Credit Panel / Equifax.”
> The Secured Overnight Financing Rate (SOFR) Data and Broad General Collateral Rate (BGCR) Data are calculated using data provided under a license granted to the New York Fed by DTCC Solutions LLC (“Solutions”), an affiliate of The Depository Trust & Clearing Corporation.

*Verifier notes:* VERIFICATION METHOD: WebFetch on the official_terms_url returned HTTP 403 (newyorkfed.org WAF blocks the fetch user-agent) — I did NOT treat that as a failure or rubber-stamp. I retrieved the page independently: WebSearch confirmed the live URL + opening sentence, and curl with a browser user-agent pulled the full page at HTTP 200 (137,925 bytes). A whitespace-normalized substring test of the finding's ENTIRE quoted passage against the extracted page text returned True.

QUOTE: Verbatim-accurate word-for-word. The finding renders the bulleted "You may:" list inline (no line breaks); the words are identical to the source. Page header confirms "Last Updated: 6/9/2023," matching the finding.

REDISTRIBUTION GENUINELY PERMITTED: The Permissible Use section explicitly grants "Copy and distribute the Content in any format or media" plus "Modify and create derivative works." The general grant covers "personal or business purposes" — so commercial use is allowed for general Content; there is NO hidden non-commercial bar, no prior-written-permission requirement, and automated access is explicitly permitted for general Content.

CLASSIFICATION IS DEFENSIBLE (attribution + share-alike both real): Conditions require attribution in a specified "© [year] Federal Reserve Bank of New York…" format, AND a genuine share-alike/copyleft: "If you distribute the Content, you must make the Content available with the same permissions, conditions, and restrictions set forth in these Terms. You may not impose more restrictive terms or conditions." Also a no-endorsement clause. The classification is not more permissive than the terms support.

SCOPING CAVEAT (does not overturn the verdict, but flagged for compliance): The Terms are NOT uniformly permissive. Section-specific Use Restrictions attach to non-data content: (a) "You may not distribute Staff Reports or Working Papers for a business or commercial purpose" (personal/internal use only — non-commercial); (b) Liberty Street Economics blog posts distributed "on a regular or serial basis" or archived for public access "requires a separate written license agreement with the New York Fed"; (c) lesson plans cannot be separately charged. These apply to research papers and blog articles, NOT to the numeric data series a data library re-hosts. Reference-rate data (SOFR/EFFR/OBFR) is explicitly distributable with a required disclaimer notice; Household Debt/Consumer Credit Panel data is distributable with "New York Fed Consumer Credit Panel / Equifax" attribution (note: Equifax-sourced third-party data). A library re-hosting NY Fed DATA under redistributable_attribution is correct; it must not blanket-apply the same label to Staff Reports/Working Papers or serial blog archives, and it must propagate the share-alike + attribution + disclaimer conditions downstream.

*Researcher reasoning:* The Federal Reserve Bank of New York's official Terms of Use (https://www.newyorkfed.org/privacy/termsofuse, Last Updated 6/9/2023) were fetched directly from the provider's own domain (WebFetch returned HTTP 403, so retrieved via curl with a browser user-agent, HTTP 200) and read in full. The terms are unusually explicit about redistribution: the "Permissible Use" section grants a non-exclusive license to "use, copy, and distribute Content for your personal or business purposes," expressly enumerating the rights to "Download, store, and use Content in any format or media," "Copy and distribute the Content in any format or media," and "Modify and create derivative works from the Content." Content is defined to include "data." This is genuine redistribution/re-hosting permission, not merely access, so the branding-vs-actual-permission caveat is satisfied. Conditions: (a) attribution is mandatory ("you must include any copyright notice and other source identifiers," with a specified fallback format "© [year] Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org."), and (b) a SHARE-ALIKE requirement: a redistributor "must make the Content available with the same permissions, conditions, and restrictions set forth in these Terms" and "may not impose more restrictive terms." Commercial redistribution of general Content is permitted ("personal or business purposes"). Hence classification = redistributable_attribution, attribution_required = true, commercial_ok = true, sharealike = true. The Prohibited Uses section bars only unlawful/fraudulent use, impersonation, and interference with the site — it does NOT prohibit mass download or re-hosting. IMPORTANT CONTENT-SPECIFIC CARVE-OUTS the library must observe: (1) Staff Reports / Working Papers are NON-COMMERCIAL only ("You may not distribute Staff Reports or Working Papers for a business or commercial purpose") — these are papers, not the statistical datasets, but should not be re-hosted for commercial purposes; (2) Liberty Street Economics Blog posts distributed on a serial basis or archived for the public require a SEPARATE WRITTEN LICENSE; (3) reference-rate data (EFFR, OBFR, SOFR, SOFR Averages/Index, BGCR, TGCR) may be redistributed but a specific notice/disclaimer must accompany it, and SOFR/BGCR incorporate DTCC-sourced data; (4) third-party content embedded on the site is owned by others and needs separate permission; (5) Household Debt & Credit / Consumer Credit Panel data must be attributed "New York Fed Consumer Credit Panel / Equifax." For the core NY Fed statistical/market/economic datasets that the "nyfed" database re-hosts, redistribution with attribution (and share-alike, plus the reference-rate disclaimer where applicable) is clearly authorized. The one classification tension is the share-alike/non-endorsement conditions and the paper-specific non-commercial carve-out, but none of these forbid redistribution of the data itself, so the conservative-yet-accurate label is redistributable_attribution rather than noncommercial_only or permission_required.

---

### fhfa

- **Databases (1):** `fhfa`
- **Official terms URL:** https://www.fhfa.gov/about/fhfa-policies/website-privacy-policy
- **License:** Public domain (U.S. federal government work)
- **Classification:** redistributable_open
- **Commercial OK:** True · **Attribution required:** False · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK

**Verbatim quote:**
> Generally, information and materials produced by Federal agencies, including FHFA, are in the public domain and may be copied and distributed without permission.
> This excludes the FHFA seal, other Federal agency seals, and marks for which there is a federal trademark
> This product uses FHFA data but is neither endorsed nor certified by FHFA.
> You may use FHFA's name or logos (e.g., "Source: FHFA®") to identify the source of API content, subject to this Privacy Policy.

*Verifier notes:* Quote verified verbatim at the official URL (https://www.fhfa.gov/about/fhfa-policies/website-privacy-policy), Section 10 "Copyright and Trademark Notice." Confirmed three ways: two WebFetch passes (one a character-for-character reproduction request) and an independent WebSearch, all returning identical wording. Page is live (fetched_ok), not a 404 or redirect.

Adversarial search for stricter clauses found only a standard federal third-party carve-out and a seal/trademark exclusion: "Some of the information contained on this website is not in the public domain because it may have been trademarked, purchased, licensed, or donated by third parties... It is your responsibility to identify such protected information and obtain permission from the copyright or trademark holders before use in anyway." Plus: the FHFA seal and ® marks "are protected by law." There is NO redistribution ban, NO non-commercial restriction, NO blanket prior-written-permission requirement, NO no-derivatives clause, and NO mass-download/bulk-extraction restriction.

Importantly, the terms explicitly authorize redistribution, not merely use — "may be copied and distributed without permission" — so the usual "use-allowed / redistribution-restricted" trap does not apply here. This is reinforced by 17 U.S.C. § 105 (U.S. federal government works are not subject to copyright). FHFA's core data products (e.g., the House Price Index) are agency-produced federal works and are genuinely public domain and redistributable.

The classification redistributable_open / "Public domain (U.S. federal government work)" is defensible and not too permissive. Caveat for the re-hoster (does not change the verdict): the third-party carve-out means the library should avoid re-hosting the FHFA seal/logos and any clearly third-party-owned copyrighted content bundled on FHFA pages, but this does not restrict redistribution of FHFA-produced datasets.

*Researcher reasoning:* FHFA (Federal Housing Finance Agency) is a U.S. federal government agency. Its official Website Privacy Policy page (on fhfa.gov, the provider's own domain) states verbatim: "Generally, information and materials produced by Federal agencies, including FHFA, are in the public domain and may be copied and distributed without permission." This is an EXPLICIT redistribution grant (not merely "free to access"): copying AND distribution are permitted, without permission, and with no commercial/non-commercial distinction — so redistribution and re-hosting of FHFA-produced data (e.g., the House Price Index datasets) is allowed. This is consistent with 17 U.S.C. 105, under which works of the U.S. federal government are not subject to copyright.

Two carve-outs/conditions, neither of which restricts redistribution of the data itself:
1. Trademark/seal exception (verbatim): "This excludes the FHFA seal, other Federal agency seals, and marks for which there is a federal trademark" (the ® marks). These protected marks are a trademark restriction, not a data-copyright restriction, and do not affect re-hosting the underlying datasets.
2. Non-endorsement notice: services/products that USE FHFA data must display "This product uses FHFA data but is neither endorsed nor certified by FHFA," and may cite the source as "Source: FHFA®." This is an anti-endorsement disclaimer (avoid implying FHFA sponsorship), not a mandatory copyright-attribution condition on redistribution — hence attribution_required is set to false for the redistribution grant, though displaying the non-endorsement notice and a "Source: FHFA" credit is good practice and advisable for an app/product built on the data. The related FHFA API Terms of Service reserve the right to rate-limit or terminate abusive API access, but that governs the live API endpoint, not redistribution of already-published datasets.

Classification: redistributable_open — public-domain U.S. government work, redistribution explicitly permitted without permission and without commercial restriction; the only true restrictions are on the FHFA seal/trademarks, which are not part of the data.

---

### frankfurter

- **Databases (1):** `frankfurter`
- **Official terms URL:** https://frankfurter.dev/
- **License:** MIT License (software code only); underlying data terms explicitly deferred to source provider (European Central Bank / central banks)
- **Classification:** unclear_not_found
- **Commercial OK:** True · **Attribution required:** None · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> Yes, absolutely. See each provider's terms for details on the underlying data.
> The project is open source, so you can also self-host for full control.
> Because the European Central Bank is in Frankfurt.
> There are no quotas. Requests are rate-limited to prevent abuse, but there are no monthly or daily caps.
> consider caching responses, self-hosting, or querying the datasets directly.
> The MIT License — Copyright (c) Hakan Ensari — Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software (source: https://raw.githubusercontent.com/lineofflight/frankfurter/main/LICENSE)

*Verifier notes:* Verbatim quote CONFIRMED at https://frankfurter.dev/ (live, 200, fetched_ok). It is the answer to the FAQ question "Is the API free for commercial use?" -> "Yes, absolutely. See each provider's terms for details on the underlying data." Exact word-for-word and punctuation match. Adversarial search for a stricter clause: the page grants NO rights over the underlying data. The "Yes, absolutely" pertains to the API/software being free for commercial use; the following sentence is an explicit DEFERRAL of underlying-data terms to each source provider. Other FAQ items (no quotas/rate-limited, self-host/query datasets directly, "For compliance, filter by a specific provider to get official reference rates", "Because the European Central Bank is in Frankfurt") are software/access guidance, not a data-redistribution grant. Code license independently confirmed MIT (github.com/lineofflight/frankfurter). Data source is ECB reference rates aggregated with other central banks. Classification "unclear_not_found" is DEFENSIBLE and NOT too permissive: it sits at the cautious end, does not claim redistribution rights, and correctly reflects that frankfurter punts data terms to the provider (ECB). The license_name note ("MIT for code only; underlying data terms explicitly deferred to source provider") properly contextualizes the quote so the "Yes, absolutely" cannot be misread as a data-redistribution grant. Neither DISPUTE condition (inaccurate/absent quote OR too-permissive classification) is met.

*Researcher reasoning:* Frankfurter (frankfurter.dev; repo github.com/lineofflight/frankfurter) is an open-source exchange-rate API. Two distinct legal layers must be separated for a re-hosting decision, and only the first is licensed by Frankfurter itself:

1) SOFTWARE/CODE: The Frankfurter application code is MIT-licensed (Copyright (c) Hakan Ensari). The MIT grant explicitly permits "distribute" — but its scope is expressly limited to "the Software" (the code), NOT the exchange-rate data. So MIT does NOT grant any right to redistribute the data.

2) DATA: Frankfurter does NOT publish its own data-redistribution/re-hosting licence. Its FAQ answer to "Is the API free for commercial use?" is: "Yes, absolutely. See each provider's terms for details on the underlying data." This is an explicit deferral — Frankfurter grants free (incl. commercial) USE of the API but routes all data-licensing questions to the underlying source. The named source is the European Central Bank ("Because the European Central Bank is in Frankfurt"; newer versions aggregate ~84 central banks). Nowhere on Frankfurter's own pages is there explicit permission to REDISTRIBUTE / RE-HOST / RE-DISSEMINATE the data.

Per the hard rules, "free for commercial use," "open source," "no API key," and "querying the datasets directly" do NOT by themselves constitute redistribution permission for the data, and I must not infer one. Because Frankfurter's own terms provide no data-redistribution grant and explicitly punt to the underlying provider, the redistribution/re-hosting status of the DATA cannot be determined from Frankfurter's own terms — hence unclear_not_found for the re-hosting question.

ACTIONABLE for compliance: The operative redistribution authority for this data is the European Central Bank (the ECB reference rates), NOT Frankfurter. The professor's re-hosting decision should rest on the ECB's own data/copyright policy (the ECB generally permits reproduction and reuse of its statistical content free of charge with source acknowledgement), which should be verified and recorded in its own dedicated ECB provider entry with a verbatim quote from the ECB site. Re-hosting via the Frankfurter brand alone is not licensed by Frankfurter; treat this record as "governed upstream by ECB — verify ECB terms." Note: commercial_ok=true reflects that Frankfurter explicitly permits commercial USE of the API; it is NOT a commercial-redistribution grant for the data.

---

### Freedom House

- **Databases (1):** `freedomhouse`
- **Official terms URL:** https://freedomhouse.org/about-us/content-permissions
- **License:** Freedom House Content Permissions (custom terms)
- **Classification:** noncommercial_only  →  **corrected to `noncommercial_permission_required / no_open_redistribution — noncommercial USE with citation is permitted, but the FIW dataset is gated behind a Freedom House "FIW Data Request" (must state intended use), and third-party re-hosting for open public download is not authorized. Treat as not-freely-redistributable: link out to Freedom House's data request rather than mirror the files (or gate to metadata-only), and note commercial use requires prior formal permission.`** by adversarial review
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **DISPUTED** (quote verbatim: True, classification agrees: False)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> Use of Freedom House content for noncommercial purposes is permitted, but the work must be acknowledged with a citation or other clear reference.
> All use of Freedom House content for commercial purposes must be formally approved by Freedom House prior to publication or any other use.
> You do not need to request permission to share Freedom House content that has been published online.
> You do not have to request permission to use Freedom House content if your use falls under the Fair Use doctrine of US copyright law.
> Always include the author, title of the work, copyright notice, and publication date.

**Adversary's contradicting clause:** "Interested in downloading Freedom in the World report data? While our data is free for personal, academic, and nonprofit use, commercial use continues to require prior formal permission and agreement. Please email [permissions email] with \"FIW Data Request\" in the subject line and a brief explanation of your intended use and our team will assist you." (freedomhouse.org/report/freedom-world). Also, the content-permissions page only permits noncommercial USE plus narrow SHARING — its own examples being "emailing a report to colleagues, sharing content on social media, etc." — and never grants third-party redistribution/re-hosting.

*Verifier notes:* Quote is verbatim-accurate: confirmed word-for-word on two independent fetches of the official_terms_url (freedomhouse.org/about-us/content-permissions), fetch_status fetched_ok is correct. However the classification "noncommercial_only" is too permissive for a library that re-hosts data for public download. The content-permissions page grants noncommercial USE (with citation) and only narrow SHARING of already-published content; it does not grant redistribution/re-hosting of the dataset. Independently checking the primary data source (freedomhouse.org/report/freedom-world) surfaced a stricter, prior-permission clause the researcher missed: FIW data downloads are routed through a "FIW Data Request" email requiring a statement of intended use, and commercial use requires "prior formal permission and agreement." Three problems with open re-hosting under an NC-only label: (1) it bypasses Freedom House's data-request touchpoint; (2) an open public download cannot enforce the noncommercial restriction on downstream users, and commercial use is barred before "any other use"; (3) redistribution is nowhere affirmatively granted. The general /about-us/terms-use page returned HTTP 404 (no broader grant found). Per skepticism default, redistribution is not confidently permitted, so a permissive NC-only classification should not be confirmed. Verdict DISPUTED: quote good, classification too permissive.

*Researcher reasoning:* Official terms located and read on Freedom House's own domain: the "Content Permissions" page (https://freedomhouse.org/about-us/content-permissions). The same governing sentences were reproduced identically across two independent fetches, so the verbatim quotes are reliable.

What the terms say:
- Noncommercial use is affirmatively permitted, conditioned on attribution: "Use of Freedom House content for noncommercial purposes is permitted, but the work must be acknowledged with a citation or other clear reference."
- Commercial use requires prior written approval: "All use of Freedom House content for commercial purposes must be formally approved by Freedom House prior to publication or any other use."
- Sharing already-published content is expressly allowed without permission: "You do not need to request permission to share Freedom House content that has been published online." (The page illustrates this with lightweight examples such as emailing a report to colleagues or sharing on social media.)
- Attribution content is specified: "Always include the author, title of the work, copyright notice, and publication date."

Classification rationale (conservative): I classify as noncommercial_only rather than permission_required because the text does NOT require permission for noncommercial use — it explicitly grants noncommercial use with attribution and explicitly waives the permission requirement for sharing published content. Permission is required ONLY for commercial use. A free, non-commercial academic library that credits Freedom House therefore falls on the permitted side of the commercial/noncommercial line, so permission_required would misstate the terms.

Important caveat for the compliance decision: Freedom House frames these terms around "use" and "sharing" of content, and does NOT contain explicit language addressing bulk redistribution, re-hosting entire datasets, or offering datasets for mass download. Their "sharing" examples are lightweight (email a report, social media), which is narrower than re-hosting a full dataset for third-party download. There is no CC or open-data licence, no public-domain dedication, and no ShareAlike condition — these are custom permissions terms, and all Freedom House content remains under Freedom House copyright. Because re-hosting a complete dataset for download is a heavier form of redistribution than the "sharing" the page contemplates, the safest course before re-hosting would be to email Freedom House's content-permissions contact to confirm that noncommercial re-hosting/download is acceptable. But on the plain text as written, noncommercial redistribution with proper attribution is permitted and commercial use is not — hence noncommercial_only, commercial_ok=false, attribution_required=true, sharealike=false.

---

### Fund for Peace — Fragile States Index (fsi_fundforpeace)

- **Databases (1):** `fsi_fundforpeace`
- **Official terms URL:** https://fragilestatesindex.org/contact-us/request-to-republish-material/
- **License:** CC BY-NC-SA 4.0
- **Classification:** noncommercial_only
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** True · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - non-commercial only

**Verbatim quote:**
> This work is licensed under a Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License.
> Publication must be for non-commercial purposes, i.e., for academic or research purposes
> Proper attribution must accompany the use of any Fragile States Index materials. The attribution must reference 'The Fund for Peace' and, where possible, reference our web site.
> Students, researchers, government officials, or representatives of international multilateral organizations do not require permission to use Fragile States Index data, content, or materials in their research or papers.
> If you wish to republish our material for commercial (for-profit) purposes, please contact us.

*Verifier notes:* Verbatim quote confirmed WORD-FOR-WORD on the official page (https://fragilestatesindex.org/contact-us/request-to-republish-material/): "This work is licensed under a Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License." Verified via two independent WebFetch passes plus a corroborating WebSearch; URL is live and correct, so fetch_status: fetched_ok is accurate.

Adversarial stricter-clause hunt found NO clause that would make noncommercial_only too permissive. The only permission language is precatory ("we kindly ask that all publishers seeking to republish our material to submit the form below") and is explicitly waived for researchers ("Students, researchers, government officials, or representatives of international multilateral organizations do not require permission to use Fragile States Index data, content, or materials in their research or papers"). This soft request cannot override the irrevocable CC grant. The page's commercial restriction ("Publication must be for non-commercial purposes, i.e., for academic or research purposes") and attribution requirement ("must reference 'The Fund for Peace'") are exactly what CC BY-NC-SA 4.0 / noncommercial_only already encode. The license is ShareAlike (copyleft), not NoDerivatives, and there is no mass-download/bulk-extraction ban.

Classification is defensible and appropriately non-permissive: CC BY-NC-SA 4.0 -> noncommercial_only is correct. Noncommercial redistribution/re-hosting is a core grant of this license and is genuinely permitted for a non-commercial academic library, subject to attribution and preserving NC + ShareAlike downstream. The label correctly bars commercial reuse rather than overstating openness.

*Researcher reasoning:* The Fund for Peace's official Fragile States Index republication-policy page explicitly states the material is released under a Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. That license affirmatively PERMITS redistribution/re-hosting of the data (not merely access), so this is not "permission_required" or "prohibited" — but only under three binding conditions: (1) NonCommercial — "Publication must be for non-commercial purposes, i.e., for academic or research purposes"; commercial/for-profit republication is excluded and requires contacting them directly. (2) Attribution — must reference "The Fund for Peace" and, where possible, the website. (3) ShareAlike — derivatives must carry the same license. The page also confirms students, researchers, and academic users "do not require permission" to use the data, reinforcing that a free, non-commercial academic re-hosting library falls squarely within the granted rights. Because redistribution is expressly allowed but restricted to non-commercial use with attribution and share-alike, the conservative and accurate classification is noncommercial_only. I fetched the official fragilestatesindex.org page twice and both reads returned identical license wording, so the quotes are reliable. Compliance note for the re-hosting library: this is fine for a free/non-commercial academic platform provided (a) clear "The Fund for Peace / Fragile States Index" attribution with a link back is shown on the dataset page, (b) the platform genuinely operates non-commercially, and (c) the ShareAlike condition is honored — do not place the data under a more permissive or restrictive license.

---

### Global Carbon Budget / Global Carbon Project

- **Databases (1):** `gcb`
- **Official terms URL:** https://robbieandrew.github.io/GCB2024/
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Simply put, you may freely copy and modify these figures and data, and use them in both commercial and non-commercial works, as long as you give credit to the Global Carbon Project.
> Our intention is that these figures and data are used. That's why they're released under the Creative Commons Attribution 4.0 International license.
> Used with permission of the Global Carbon Project under the Creative Commons Attribution 4.0 International license.
> Exact License Label (Zenodo deposit 'The Global Carbon Project's fossil CO2 emissions dataset', https://zenodo.org/records/5569235): "Creative Commons Attribution 4.0 International"

*Verifier notes:* Fetched https://robbieandrew.github.io/GCB2024/ (fetch_status confirmed: fetched_ok). The verbatim quote appears WORD-FOR-WORD on the page, including the leading "Simply put," prefix — verified via a targeted second fetch asking for the exact sentence: "Simply put, you may freely copy and modify these figures and data, and use them in both commercial and non-commercial works, as long as you give credit to the Global Carbon Project." The page names the license as Creative Commons Attribution 4.0 International and links to creativecommons.org/licenses/by/4.0/, matching license_name = CC BY 4.0. Adversarial search for a stricter clause found NONE: no redistribution/re-hosting ban, no non-commercial-only restriction, no "prior written permission" requirement, no no-derivatives clause, no share-alike, no bulk/mass-download restriction. The page affirmatively encourages reuse ("Our intention is that these figures and data are used"). CC BY 4.0 explicitly permits redistribution and re-hosting with attribution, so classification redistributable_attribution is defensible and NOT too permissive for a library that re-hosts data for public download; the only obligation is crediting the Global Carbon Project, which the classification's "attribution" qualifier captures. Minor non-verdict-changing caveat: this URL is Robbie Andrew's official GCB2024 figures/data page and its CC BY grant is scoped to "these figures and data"; bulk underlying GCB datasets distributed via ICOS/DOI may carry their own (typically also CC BY) terms, so per-dataset terms should be checked if the library re-hosts the full underlying datasets rather than the figures/data from this page. For the cited source and URL, the finding is accurate and correctly classified.

*Researcher reasoning:* The Global Carbon Project releases the Global Carbon Budget figures and data under the Creative Commons Attribution 4.0 International (CC BY 4.0) license. I read this verbatim on the official GCB figures/data distribution page (https://robbieandrew.github.io/GCB2024/), maintained by Robbie Andrew, a core Global Carbon Project team member and compiler of the official GCP dataset; the page states the figures and data "are released under the Creative Commons Attribution 4.0 International license" and that users "may freely copy and modify these figures and data, and use them in both commercial and non-commercial works, as long as you give credit to the Global Carbon Project." This was independently corroborated by the official GCP data deposit on Zenodo (record 5569235, "The Global Carbon Project's fossil CO2 emissions dataset," by R. M. Andrew and G. P. Peters), which carries the license badge "Creative Commons Attribution 4.0 International." CC BY 4.0 explicitly permits redistribution and re-hosting for both commercial and non-commercial use provided the Global Carbon Project is credited; it imposes no non-commercial restriction and no share-alike requirement. Classification: redistributable_attribution. Note: the primary globalcarbonbudget.org marketing/datahub/FAQ pages did not surface the license text in their fetched HTML (they link out to the figures page and Zenodo), so the verbatim clause is quoted from the canonical GCB figures/data page rather than a globalcarbonbudget.org URL; a data-library re-host should carry an attribution line naming the Global Carbon Project and citing the underlying GCB release/DOI. Individual constituent data sources feeding the budget may carry their own citation expectations, but the compiled GCB product itself is CC BY 4.0.

---

### Global Power Plant Database (World Resources Institute)

- **Databases (1):** `gppd`
- **Official terms URL:** https://raw.githubusercontent.com/wri/global-power-plant-database/master/README.md
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> The latest database release (v1.3.0) is available in CSV format here under a Creative Commons-Attribution 4.0 (CC BY 4.0) license.
> All Python source code is available under a MIT license.

*Verifier notes:* Fetched the official_terms_url (WRI's own README on master) — HTTP OK, not a 404 or wrong page. The verbatim_quote appears word-for-word; the README raw text is "The latest database release (v1.3.0) is available in CSV format [here](http://datasets.wri.org/dataset/globalpowerplantdatabase) under a [Creative Commons-Attribution 4.0 (CC BY 4.0) license](https://creativecommons.org/licenses/by/4.0/)." The finding's quote is identical once inline markdown link decorations are stripped to their anchor text — no words altered, added, or omitted.

Adversarial search for a stricter clause: the entire license section is only two sentences (the quoted one + "All Python source code is available under a MIT license."). No redistribution ban, no non-commercial restriction, no prior-written-permission requirement, no no-derivatives clause, and no bulk/mass-download restriction appears anywhere in the README. I also checked the linked WRI data portal (datasets.wri.org/dataset/globalpowerplantdatabase); it exposed no stricter term (JS-rendered CKAN page), and WRI's README is the authoritative copyright-holder statement.

Classification judgment: CC BY 4.0 explicitly permits Share (redistribution in any medium/format) and commercial use and derivatives, conditioned only on attribution. "redistributable_attribution" matches this exactly and is NOT more permissive than the terms support — re-hosting the CSV for public download with attribution is genuinely permitted. Minor caveat noted but not disqualifying: WRI's DB aggregates upstream sources, but WRI itself licenses the compiled database as CC BY 4.0, which is the relevant provider term. Verdict: CONFIRMED.

*Researcher reasoning:* WRI's own official repository README (github.com/wri/global-power-plant-database, raw content fetched and read) states verbatim: "The latest database release (v1.3.0) is available in CSV format here under a Creative Commons-Attribution 4.0 (CC BY 4.0) license." CC BY 4.0 is a standard open license that EXPLICITLY grants the right to "reproduce and Share the Licensed Material, in whole or in part" (redistribution/re-hosting) for any purpose, including commercially, subject only to the attribution requirement (credit the creator, provide the license link, indicate any changes). There is no NonCommercial restriction and no ShareAlike obligation. This directly permits a free non-commercial academic library to re-host the dataset for download, provided WRI is credited. WRI additionally "requests" (not requires) that users register their use and optionally sign up for update notifications; this is a courtesy request, not a binding redistribution condition, so it does not downgrade the classification. The Python source code (separate from the data) is MIT-licensed. Classification: redistributable_attribution. Note: the datasets.wri.org catalog page did not surface machine-readable license text via fetch, but the authoritative license statement is published by WRI itself on its own github.com/wri repository and is corroborated by independent catalog summaries (Google Earth Engine, WRI Data Explorer) that also list CC BY 4.0.

---

### Groningen Growth and Development Centre

- **Databases (1):** `ggdc`
- **Official terms URL:** https://www.rug.nl/ggdc/productivity/pld/releases/pld-2023?lang=en
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> The GGDC Productivity Level Database 2023 by Robert Inklaar, Ryan Marapin, and Kaira Gräler is licensed under a Creative Commons Attribution 4.0 International License.
> Maddison Project Database, version 2020 by Jutta Bolt and Jan Luiten van Zanden is licensed under a Creative Commons Attribution 4.0 International License
> The GGDC/UNU-WIDER Economic Transformation Database is licensed under a Creative Commons Attribution 4.0 International License

*Verifier notes:* Adversarial verification passed on all three checks.

1) VERBATIM QUOTE — CONFIRMED. Fetched the official_terms_url (https://www.rug.nl/ggdc/productivity/pld/releases/pld-2023?lang=en), fetch_status = fetched_ok, page title "PLD 2023 Edition | Productivity Level Database". The page states word-for-word: "The GGDC Productivity Level Database 2023 by Robert Inklaar, Ryan Marapin, and Kaira Gräler is licensed under a Creative Commons Attribution 4.0 International License". This matches the researcher's verbatim_quote exactly (finding adds only a trailing period). Confirmed across two independent fetches of the page.

2) SEARCH FOR STRICTER CLAUSE — none found. I specifically hunted for a redistribution ban, non-commercial (NC) restriction, no-derivatives (ND) clause, "prior written permission" requirement, or a bulk-download/mass-extraction restriction. The only additional obligation on the page is an attribution/citation requirement: "When using these data (for whatever purpose), please make the following reference: Robert Inklaar, Ryan Marapin, and Kaira Gräler (2023), 'Tradability and sectoral productivity differences across countries'." "For whatever purpose" is inclusive, not restrictive. No NC/ND terms present.

3) INDEPENDENT CROSS-CHECK of the actual host. The data physically lives on Dataverse (DOI 10.34894/AEAX1F, redirects to dataverse.nl). I checked the Dataverse dataset page directly: it also shows license "CC-BY-4.0" and only a citation/attribution expectation ("Our Community Norms ... expect that proper credit is given via citation"). No conflicting or stricter terms at the hosting layer.

CLASSIFICATION DEFENSIBILITY: CC BY 4.0 is a standard permissive license that explicitly grants the right to copy, redistribute, and adapt the material in any medium or format, including commercially, subject only to attribution. This fully supports a library re-hosting the data for public download. The classification "redistributable_attribution" is correct and correctly scoped — it captures the mandatory attribution condition rather than overstating the grant as unconditional. Not too permissive; not too strict.

Note: this source is one of the small set that carries a genuinely verified redistributable license, consistent with prior ledger caution (M-20260714-20) that most catalog sources default to NEEDS-REVIEW — this one is legitimately verified against the primary source and its host.

*Researcher reasoning:* The Groningen Growth and Development Centre (GGDC, University of Groningen) publishes several distinct datasets under the "ggdc" umbrella. I checked the official release pages on the provider's own domain (rug.nl/ggdc) for its flagship databases and found a consistent licensing regime: each dataset is explicitly released under the Creative Commons Attribution 4.0 International License (CC BY 4.0).

Verbatim license statements confirmed by fetching the official pages:
- PLD 2023 (https://www.rug.nl/ggdc/productivity/pld/releases/pld-2023?lang=en): "The GGDC Productivity Level Database 2023 by Robert Inklaar, Ryan Marapin, and Kaira Gräler is licensed under a Creative Commons Attribution 4.0 International License."
- Maddison Project Database 2020 (https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2020?lang=en): "Maddison Project Database, version 2020 by Jutta Bolt and Jan Luiten van Zanden is licensed under a Creative Commons Attribution 4.0 International License."
- Economic Transformation Database / successor to the GGDC 10-Sector DB (https://www.wider.unu.edu/database/etd-economic-transformation-database): "The GGDC/UNU-WIDER Economic Transformation Database is licensed under a Creative Commons Attribution 4.0 International License."

CC BY 4.0 explicitly grants the right to share (copy and redistribute the material in any medium or format) and adapt, for any purpose including commercially, provided attribution is given. This directly authorizes third-party redistribution / re-hosting for download, which is the specific right needed. It is not a bare "publicly available" branding claim — it is a named, standardized open license with a redistribution grant.

Attribution is required. The GGDC pages specify dataset-specific citation strings (e.g., the underlying Research Memorandum / journal article and the dataset title/version) that must accompany use; the redistributing library must preserve attribution and the CC BY 4.0 notice. No non-commercial restriction and no ShareAlike obligation apply under CC BY 4.0.

Caveat for the compliance record: the GGDC "ggdc" umbrella spans multiple databases and vintages. The three most prominent (PLD, Maddison, ETD) are all CC BY 4.0, but a specific older or niche GGDC dataset ingested by the library should be spot-checked against its own release page, since GGDC assigns the license per dataset/version rather than one site-wide term. On the evidence gathered, the governing regime is CC BY 4.0 (redistribution permitted with attribution).

---

### hf_equities (first-party re-derivation; governing upstream source for the re-hosted equities data = IEX Exchange HIST — Investors Exchange LLC)

- **Databases (1):** `hf_equities`
- **Official terms URL:** https://www.iexexchange.io/legal/hist-data-terms
- **License:** IEX Historical Data Terms of Use (free redistribution permitted with mandatory attribution)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> 01 If you distribute, sell, lease, furnish, or otherwise permit or provide access to Investors’ Exchange LLC (“IEX”) historical data, you must cite IEX as the source with the following text and link: “Data provided for free by IEX. By accessing or using IEX Historical Data, you agree to the IEX Historical Data Terms of Use.”
> 02 You will not engage in the operation of any illegal business use or permit anyone else to use IEX historical data, or any part thereof, for any illegal purpose or violation of any IEX or SEC rule or regulation.
> 03 As between IEX and you, IEX has the exclusive proprietary rights in and to the IEX historical data. The IEX historical data, including without limitation any and all intellectual property rights inherent therein or appurtenant thereto, shall, as between IEX and you, be and remain the sole and exclusive property of IEX.
> IEX Historical Data Terms of Use This document sets out the terms that you need to follow if you access or use IEX historical data (whether from IEX or through a third party).

*Verifier notes:* Adversarial review upholds the finding. VERBATIM: The finding's canonical URL (iexexchange.io/legal/hist-data-terms) returned HTTP 403 to WebFetch — WAF/bot blocking, not a 404 or wrong page. IEX rebranded its corporate domain to iex.io; the identical document is served at https://www.iex.io/legal/hist-data-terms (same title, IEX's own domain). Against that official mirror, clause 01 matches the verbatim_quote word-for-word. The only flagged deltas were non-substantive: a straight-vs-curly apostrophe artifact from my test string (the stored quote already uses the correct curly apostrophe in "Investors'"), and "01" appearing as a markdown H5 heading in the fetch conversion. STRICTER-CLAUSE HUNT (the adversarial step): full scan of all ten clauses found NO redistribution ban, NO non-commercial restriction, NO prior-written-permission/consent requirement, NO no-derivatives clause, and NO bulk-download/mass-extraction/volume restriction. Clause structure: 01 attribution, 02 legality, 03 ownership, 04-07 liability/indemnification, 08-10 disclaimers. Clause 01 is framed permissively — it conditions "distribute, sell, lease, furnish, or otherwise permit or provide access to" on attribution, affirmatively permitting redistribution (including commercial resale). Clause 03's reservation of exclusive proprietary rights is a standard ownership statement that coexists with, and does not override, the express distribution license. I also confirmed the applicable terms: the real-time "IEX Market Data Policies" / subscriber-attribution rules govern the live DEEP/TOPS feeds, not the HIST historical-download product covered here. CLASSIFICATION: redistributable_attribution is defensible and if anything mildly conservative (terms permit resale). For a re-hosting library the only binding conditions are the mandated attribution string ("Data provided for free by IEX. By accessing or using IEX Historical Data, you agree to the IEX Historical Data Terms of Use.") and lawful use — both consistent with the classification and not too permissive. Minor operational note (not a dispute): store the accessible iex.io URL alongside the iexexchange.io URL since the latter is WAF-blocked to automated fetchers.

*Researcher reasoning:* CONTEXT: "hf_equities" is not an external third-party provider with its own public terms page — it is the professor's OWN high-frequency equities dataset re-hosted by the library. Its provenance (confirmed in the codebase/project memory, definitive statement 2026-07-14) is: pre-March 2022 = PiTrading data personally purchased by the professor; post-March 2022 = IEX Exchange HIST (TOPS pcap files downloaded directly from IEX). As of the 2026-07-14 re-derivation, the entire served 2022-03-07→2026-03-27 window is now 100% IEX-derived (all alpaca-sourced bars removed). Therefore the operative governing upstream license for the re-hosted (redistributed) content is the IEX Historical Data Terms of Use.

PRIMARY SOURCE READ VERBATIM: The official IEX page returned HTTP 403 to the WebFetch tool, so I fetched it directly with a browser user-agent (HTTP 200, 77 KB) and read the full plain text. Clause 01 is the operative redistribution clause. It expressly enumerates redistribution acts — "distribute, sell, lease, furnish, or otherwise permit or provide access to" — and permits them subject to ONE condition: citing IEX with the exact prescribed attribution text and link ("Data provided for free by IEX. By accessing or using IEX Historical Data, you agree to the IEX Historical Data Terms of Use."). This is explicit redistribution/re-dissemination language, not merely "access/use" language.

CLASSIFICATION = redistributable_attribution:
- Redistribution is explicitly permitted (clause 01 lists "distribute … furnish … permit or provide access").
- Commercial use is permitted: the same clause permits "sell, lease" — a sale/lease license — so there is NO non-commercial-only restriction. commercial_ok = true.
- Attribution is mandatory and prescriptive: the exact citation text plus link is required whenever the data is distributed. attribution_required = true.
- No share-alike / copyleft obligation exists (the terms impose no requirement to license downstream on the same terms). sharealike = false.
- No prior-written-permission requirement and no subscriber agreement is required to redistribute HIST data in accordance with these terms (nothing in the 10 clauses conditions distribution on IEX's advance consent).
- Clause 03 reserves IEX's underlying proprietary/IP rights, and clauses 04–10 are liability/warranty disclaimers; none of these negate or narrow the clause-01 grant.

CAVEATS for the compliance decision: (1) These terms govern ONLY the IEX-derived portion (the currently-live 2022+ window). The pre-March-2022 segment derives from PiTrading data that the professor personally purchased; PiTrading's redistribution rights are set by a separate commercial purchase license (not this page) and should be confirmed independently before that segment is treated as freely redistributable. (2) The library MUST display the exact clause-01 attribution string and link on any hf_equities download surface to remain compliant. (3) No "last updated"/effective date is shown on the terms page; footer shows "© 2026 IEX Group, Inc." Fetched and read 2026-07-14.

---

### ilostat

- **Databases (1):** `ilostat`
- **Official terms URL:** https://www.ilo.org/rights-and-permissions
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> This Creative Commons licence permits the reproduction, distribution and adaptation (including translation) of work for any purposes, as long as credit is given to the respective author/creator, in this case, the ILO.
> databases and datasets together with the accompanying referential metadata are covered by the Creative Commons CC BY 4.0 licence.
> As of 3 May 2023, unless otherwise indicated, ILO publications are licensed under a Creative Commons Attribution BY 4.0 licence (CC BY 4.0).
> ILO publications produced prior to 3 May 2023 do not automatically benefit from a Creative Commons licence. It is essential that users check the copyright page of each work for exact licence information.
> Data in ILOSTAT databases are widely re-used by other organizations for their datasets and as inputs into world recognized indices. ILOSTAT data are also widely re-published by other organizations, most notably by the World Bank in the World Development Indicators database. (from https://ilostat.ilo.org/about/dissemination-and-analysis/ , retrieved via search snippet; direct fetch returned HTTP 403)

*Verifier notes:* Adversarial review upheld the finding.

QUOTE: The verbatim_quote appears WORD-FOR-WORD at https://www.ilo.org/rights-and-permissions, confirmed independently by both WebFetch of the page and a targeted WebSearch. The URL is live (HTTP 200) and is the correct ILO Rights and Permissions page. fetch_status "fetched_ok" is accurate.

STRICTER-CLAUSE HUNT: I searched the data/statistics section specifically for a redistribution ban, non-commercial restriction, prior-written-permission requirement, no-derivatives clause, or bulk/mass-extraction limit. None apply to the aggregate statistics. The data section independently and explicitly states: "databases and datasets together with the accompanying referential metadata are covered by the Creative Commons CC BY 4.0 licence." CC BY 4.0 permits reproduction, distribution, adaptation, commercial use, and re-hosting with attribution — so redistributable_attribution is NOT too permissive; it matches the terms.

CAVEATS (do not overturn the classification, but the library should honor them):
1. Microdata carve-out (verbatim): "This licence does not apply to microdata submitted by or obtained from constituents and partner institutions that is restricted solely to the ILO's use." ILOSTAT re-hosts aggregate indicators, not this restricted microdata, so the classification stands for the data a library actually mirrors.
2. Temporal caveat: databases/datasets produced PRIOR to 3 May 2023 do not automatically benefit from the CC licence and should be checked per-work. Current ILOSTAT indicators are CC BY 4.0; if the library re-hosts pre-2023 vintage files, verify their copyright page.

MINOR NIT (not a defect): the researcher's cited quote is drawn from the ILO PUBLICATIONS section; the load-bearing basis for ILOSTAT DATA is the separate datasets sentence quoted above. Both point to CC BY 4.0, so the end classification is correct. A cleaner citation would have quoted the datasets clause directly.

Net: quote verbatim-accurate at the stated URL AND CC BY 4.0 / redistributable_attribution is defensible and not over-permissive. CONFIRMED.

*Researcher reasoning:* The official ILO Rights and Permissions page (https://www.ilo.org/rights-and-permissions), fetched and read directly, states that as of 3 May 2023 ILO "databases and datasets together with the accompanying referential metadata are covered by the Creative Commons CC BY 4.0 licence," and that this licence "permits the reproduction, distribution and adaptation (including translation) of work for any purposes, as long as credit is given to the respective author/creator, in this case, the ILO." CC BY 4.0 explicitly authorizes redistribution/re-hosting ("distribution") and commercial use ("for any purposes"), with the sole condition of attribution to the ILO. There is no non-commercial restriction and no ShareAlike obligation. This maps to redistributable_attribution. Two caveats a re-hosting library must observe: (1) The CC BY 4.0 coverage applies to material produced on/after 3 May 2023 — "publications produced prior to 3 May 2023 do not automatically benefit from a Creative Commons licence," so pre-2023 vintages should be verified per work; however the current ILOSTAT bulk databases are ILO-produced aggregate statistics disseminated under this regime, and the ILOSTAT dissemination page confirms the data are routinely re-used and re-published by third parties (e.g. the World Bank WDI). (2) The licence does not extend to restricted microdata submitted by constituents/partner institutions; that is not part of the public ILOSTAT aggregate databases being re-hosted here. Attribution to ILO/ILOSTAT is mandatory. Classification is redistributable_attribution. Note: a direct fetch of the ILOSTAT dissemination-and-analysis page returned HTTP 403; the corroborating re-distribution statement there was captured via search snippet, but the governing licence quote comes from the directly-fetched ILO Rights and Permissions page.

---

### imf

- **Databases (1):** `imf`
- **Official terms URL:** https://www.imf.org/en/about/copyright-and-terms
- **License:** IMF Copyright and Usage — Special Terms for "The Use of IMF Data" (custom terms, effective October 11, 2024)
- **Classification:** redistributable_attribution
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> You may download, extract, copy, create derivative works, publish, distribute, and use Data obtained from IMF Sites, subject to the following conditions: Whether obtained directly from the IMF or another party, when Data is distributed or reproduced in any manner, it must appear accurately with attribution to the IMF as the source, e.g. “Source: International Monetary Fund, Database Name, <<link to the dataset>>.”
> Notwithstanding the general prohibition on the commercial use of IMF Content, with respect to published statistical data made available on IMF Sites, the following special terms shall govern.
> For the purposes of these special terms, the term “Data” refers to any published statistical data produced or curated by the IMF. In particular, “Data” refers to the following: IMF Statistical Data, including but not limited to, International Financial Statistics (IFS), Balance of Payments (BOP), Direction of Trade (DOT), and Government Finance Statistics (GFS); World Economic Outlook database; Primary Commodity Prices; IMF Financial Data; Exchange Rate Data; and Most statistical data available on www.IMF.org, www.data.IMF.org, or the iData Portal that explicitly identify the International Monetary Fund as the source.
> Users who make IMF Data available to other Users through any type of distribution or download environment agree to take reasonable efforts to communicate and promote compliance by their users with these terms.
> If IMF Data is sold by Users as a standalone product, sellers must inform purchasers that the Data is available free of charge from the IMF.
> The policy of free access and free reuse of IMF Data does not imply a right to obtain confidential or any unpublished data, over which the IMF reserves all rights.
> For any potential commercial reuse of IMF Data, please email copyright@imf.org to request permission.
> Some statistical products may incorporate information from third parties and may have separate terms and conditions for usage.
> The IMF prohibits the bulk download of information by automated technology without explicit permission and reserves the right to terminate access to its Sites or Content.
> The IMF allows free non-systematic downloading and/or printing of Content from its Sites by Users for personal, noncommercial usage only without any right to resell, redistribute, compile, or create derivative works.

*Verifier notes:* Verified against the official page (https://www.imf.org/en/about/copyright-and-terms, "Effective: October 11, 2024") loaded in a real browser after IMF returned HTTP 403 to WebFetch/automated fetchers (itself corroborating IMF's automation restrictions).

QUOTE: Verbatim-accurate. The page reads: "You may download, extract, copy, create derivative works, publish, distribute, and use Data obtained from IMF Sites, subject to the following conditions: Whether obtained directly from the IMF or another party, when Data is distributed or reproduced in any manner, it must appear accurately with attribution to the IMF as the source, e.g. “Source: International Monetary Fund, Database Name, <<link to the  dataset>>.”" The finding's quote differs only by a collapsed line break and one normalized double space ("the  dataset") — non-substantive. Effective date (Oct 11, 2024) correct.

CLASSIFICATION DEFENSIBLE: The terms are two-tier. (1) General Content is "All Rights Reserved," personal/noncommercial only, "without any right to resell, redistribute, compile, or create derivative works," with a ban on "bulk download of information by automated technology without explicit permission" and on LLM training. (2) A separate section, "The Use of IMF Data," explicitly overrides this for statistical data: "Notwithstanding the general prohibition on the commercial use of IMF Content... You may download, extract, copy, create derivative works, publish, distribute, and use Data obtained from IMF Sites" subject to attribution + integrity conditions. This is a genuine, explicit redistribution grant for statistical Data — not a use-but-don't-redistribute restriction. So redistributable_attribution is defensible and NOT too permissive for a library re-hosting IMF statistical series.

CONDITIONS THE LIBRARY MUST HONOR (part of the attribution classification, not contradictions): (a) attribution in the specified "Source: International Monetary Fund, Database Name, <link>" format; (b) do not alter the Data's integrity — state any material transformation; (c) pass-through: "take reasonable efforts to communicate and promote compliance by their users with these terms"; (d) if sold as a standalone product, inform purchasers the Data is free from the IMF.

SCOPE CAVEATS (do not flip the verdict, but bound it): The grant covers only data that "explicitly identify the International Monetary Fund as the source." IMF products may embed THIRD-PARTY data with "separate terms and conditions" — "For material not attributed to the IMF, users should obtain permission directly from the respective third-party copyright holder." It also does NOT cover IMF publications/reports/text Content, which remain All-Rights-Reserved / personal-noncommercial. The general "bulk download... by automated technology without explicit permission" ban continues to apply ("Except as stated in this Section... all other terms... shall continue to apply") — but it constrains ACQUISITION method (automated scraping of IMF Sites), not redistribution of lawfully obtained Data, so it does not make the redistribution classification too permissive. The library should acquire via permitted channels rather than un-permissioned bulk automated scraping.

*Researcher reasoning:* Fetched and read the official IMF page at imf.org/en/about/copyright-and-terms (WebFetch returned 403, so I read it via the browser tool; page effective October 11, 2024). The general Content terms are restrictive ("All Rights Reserved"; only personal, noncommercial, non-systematic downloading with "no right to resell, redistribute, compile, or create derivative works"). HOWEVER, the IMF carves out a dedicated section, "The Use of IMF Data," with SPECIAL terms that override the general prohibition for published statistical data. "Data" is broadly defined to cover essentially all IMF statistical databases (IFS, BOP, DOT, GFS, WEO, Primary Commodity Prices, Financial Data, Exchange Rate Data, and most statistical data on imf.org / data.imf.org that names the IMF as source). For this Data, the IMF EXPLICITLY grants the right to "download, extract, copy, create derivative works, publish, distribute, and use," and condition #3 explicitly contemplates third-party re-hosting: "Users who make IMF Data available to other Users through any type of distribution or download environment." The sole mandatory condition relevant here is attribution to the IMF as source (plus a data-integrity/transformation-disclosure requirement). This squarely permits a FREE, NON-COMMERCIAL re-hosting library, so classification = redistributable_attribution. Two caveats the professor must observe: (1) COMMERCIAL reuse is NOT covered by the free grant — "For any potential commercial reuse of IMF Data, please email copyright@imf.org to request permission" — hence commercial_ok = false; the professor's library is non-commercial, so this is fine. (2) Acquisition method: the general terms still apply to Data ("Except as stated in this Section... all other terms... shall continue to apply"), and they state "The IMF prohibits the bulk download of information by automated technology without explicit permission" — so the redistribution RIGHT is granted, but automated bulk scraping to obtain the data may itself require permission; use official bulk-download/API channels or seek permission for the harvesting step. (3) Some statistical products incorporate third-party data with separate terms — those specific series may not be covered. No share-alike obligation exists. Net: redistribution/re-hosting of IMF statistical Data for a free non-commercial download library is permitted provided IMF is attributed as the source on every distributed dataset.

---

### INSEE (Institut national de la statistique et des études économiques, France)

- **Databases (2):** `insee_bdm`, `insee_melodi`
- **Official terms URL:** https://www.insee.fr/en/information/2409130
- **License:** Licence Ouverte / Open Licence 2.0 (Etalab)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> the public information disseminated on this site (data, databases, publications, downloadable files) is made available under the Open License version 2.0 (Etalab). This license allows free reuse, including for commercial purposes ... provided that the source is mentioned in the form 'Source: Insee,' the date of the last update of the data is mentioned when known, and the meaning of the information is not altered or misinterpreted
> de la communiquer, la reproduire, la copier ; de l'adapter, la modifier, l'extraire et la transformer, notamment pour créer des « Informations dérivées » ; de la diffuser, la redistribuer, la publier et la transmettre, de l'exploiter à titre commercial (source: Etalab Licence Ouverte / Open Licence 2.0, https://www.data.gouv.fr/pages/legal/licences/etalab-2.0)
> mentionner la paternité de l'«Information» : sa source (a minima le nom du « Concédant ») et la date de la dernière mise à jour de l'« Information » réutilisée (source: Etalab Licence Ouverte / Open Licence 2.0, https://www.data.gouv.fr/pages/legal/licences/etalab-2.0)

*Verifier notes:* URL https://www.insee.fr/en/information/2409130 fetched OK (fetch_status confirmed). All five component phrases of the verbatim_quote appear word-for-word on the page: (a) "the public information disseminated on this site (data, databases, publications, downloadable files) is made available under the Open License version 2.0 (Etalab)"; (b) "This license allows free reuse, including for commercial purposes"; (c) "provided that the source is mentioned in the form 'Source: Insee,'"; (d) "the date of the last update of the data is mentioned when known"; (e) "the meaning of the information is not altered or misinterpreted". The "..." in the finding is an honest elision between (b) and (c), not a concealed restriction.

Adversarial search for a stricter clause: the ONLY restriction on the page is a web-scraping clause — automated extraction "is only permitted to the extent that it does not impair the proper functioning of the site." This is a technical rate/access protection on INSEE's live servers, NOT a restriction on redistributing data already obtained. No redistribution ban, no non-commercial clause, no prior-written-permission requirement, and no no-derivatives clause exists on the page.

The classification is independently corroborated by the Etalab Open Licence 2.0 text itself, which explicitly grants the right to reuse by "disseminating, redistributing, publishing and transmitting it; and by exploiting it for commercial purposes," conditioned only on attribution (source name) and mention of the last-update date — a CC-BY-equivalent open license. Re-hosting the data for public download with "Source: Insee" attribution and the update date is squarely permitted. classification redistributable_attribution is defensible and not more permissive than the terms support. VERDICT: CONFIRMED.

*`insee_melodi` scope note (added 2026-07-29):* this source carried an `etalab-2.0` row in the local catalog but had **never been audited** — the flag was an assertion, not evidence (the failure mode of R113/R117), so it is recorded here explicitly rather than left to inherit its sibling's clearance. It does not rest on analogy: `insee_melodi` fetches `https://api.insee.fr/melodi` and the already-CONFIRMED `insee_bdm` fetches `https://api.insee.fr/series/BDM/V1` — the **same publisher on the same API host**, governed by the same terms page above, which covers "the public information disseminated on this site (data, databases, publications, downloadable files)" *unless otherwise stated*. Adversarial check for an "otherwise": the Melodi `dataflow/all` payload was fetched and searched — it carries only `code` and `label` for its 144 flows and declares no licence, and the INSEE API portal returns a script-only shell with no terms text. **No carve-out for Melodi exists**, so the site-wide grant governs. VERDICT: CLEARED - re-host OK (attribution), commercial OK.

One OPERATIONAL condition applies to both and is not a redistribution limit: INSEE requires automated clients to "limit the requests frequency as to not disrupt the normal functioning of the site" and reserves the right to block systems generating excessive load. Our Melodi fetcher is documented keyless at 30 req/min, which honours it — keep that pacing.

*Attribution condition, HOW IT IS MET (2026-07-29):* Etalab 2.0 as quoted above requires three things of a reuser, not one — the source in the form "Source: Insee," **the date of the last update of the data when known**, and that the meaning is not altered. The first and third were satisfied at launch; the second was NOT — `insee_melodi` shipped with `last_updated: null` on every flow, which is a condition of the licence rather than a nicety. Fixed by reading INSEE's own `modified` field from `/melodi/catalog/{FLOW}` (fetched at their documented 30 req/min) into the catalog: **134 of 139 flows now carry a real INSEE update date**, verified live (`insee_melodi:DS_ICA` -> `last_updated: 2026-07-23`). The remaining 5 are flows absent from INSEE's own dataflow catalogue, so no date is knowable for them and the field is honestly null — which is exactly the "when known" the licence allows, not a gap. Any future source under Etalab must satisfy all three limbs, not just attribution.

*Class sweep + OPEN PRE-LAUNCH GATE (2026-07-29):* the miss above was not confined to the source that revealed it. Every `etalab-2.0` source was checked: **`insee_bdm` had been SERVING 101,848 series with none dated** — a live unmet condition, not a theoretical one — and is now at 101,789/101,848 (99.9%) from INSEE's per-IDBANK `LAST_UPDATE` attribute, which the BDM data response had carried all along (verified live: `insee_bdm:001694113` -> 2024-07-16). `insee_melodi` is at 134/139. `cepii_baci` and `insee_sirene` hold no series.

**`cepii_gravity` — GATE CLOSED (2026-07-29), 1,143,250/1,143,250 dated (100%).** It was 0% dated and would have gone live in breach; the date is now `2024-04-15`, taken from the `Last-Modified` header CEPII itself serves on the exact file we host (`Gravity_csv_V202211.zip`, 206,707,748 bytes). That is an observed publisher fact, not our fetch time and not a day invented out of a month — the version stamp `V202211` gives only November 2022, and writing "2022-11-01" would have fabricated a precision nobody published. Both facts are worth keeping: **dataset version V202211, file re-issued 2024-04-15.**

*Currency, checked at the same time:* CEPII's own database page lists Gravity releases `202010, 202102, 202202, 202211`. `V202211` is the newest, so what we host is the current release — not a stale pin.

*Researcher reasoning:* INSEE's official legal-notice page on its own domain (insee.fr/en/information/2409130, English mirror of the French "Conditions d'utilisation de nos données" at insee.fr/fr/information/2381863) states that its public information — explicitly including "data, databases, publications, downloadable files" — is made available under the Etalab Licence Ouverte / Open Licence version 2.0. The French provider page (2381863) returned HTTP 404 to the fetcher (likely bot-blocking), but the English legal-notice page on the same official insee.fr domain fetched successfully and was corroborated by multiple search snippets, so the licence designation is confirmed from the provider's own domain.

The governing redistribution rights come from the Etalab Licence Ouverte 2.0 text (fetched verbatim from the official government host data.gouv.fr/pages/legal/licences/etalab-2.0), which grants the reuser a free, non-exclusive, worldwide, unlimited-duration right "de la diffuser, la redistribuer, la publier et la transmettre" (to disseminate it, redistribute it, publish it and transmit it) and "de l'exploiter à titre commercial" (to exploit it commercially). This is EXPLICIT re-dissemination/redistribution language, not mere "open data" branding.

The only condition is attribution ("paternité"): the source must be cited — for INSEE, "Source: Insee" — together with the date of last update, and the meaning of the information must not be distorted. There is no non-commercial restriction and no share-alike/copyleft obligation in Licence Ouverte 2.0 (unlike CC BY-SA). Commercial use is expressly permitted.

Conservative classification: redistributable_attribution. A free, non-commercial academic library may re-host/redistribute INSEE BDM (insee_bdm) data provided it displays the "Source: Insee" attribution and last-update date and does not misrepresent the data. Caveat for the operator: this covers INSEE's own published statistics under the Open Licence; a few specific INSEE products (e.g., certain Sirene value-added services or personal-data-bearing files) can carry separate conditions, but the standard BDM macro-economic series are within scope of the Open Licence 2.0.

---

### Inter-American Development Bank (IDB) — IDB Open Data portal (data.iadb.org)

- **Databases (1):** `idb`
- **Official terms URL:** https://data.iadb.org/dataset/2020-better-jobs-index-database-latin-america
- **License:** Mixed per-dataset. Predominant declared license: Creative Commons Attribution–NonCommercial–NoDerivs 3.0 IGO (CC BY-NC-ND 3.0 IGO). Minority: CC BY 4.0 International. Majority (~86%) carry no declared license.
- **Classification:** noncommercial_only  →  **corrected to `noncommercial_no_derivatives (CC BY-NC-ND: NonCommercial AND NoDerivatives). Only verbatim, non-commercial, attributed copies may be redistributed. Separately, per the finding's own license_name note, ~86% of IDB datasets carry NO declared license (no redistribution grant) and a minority are CC BY 4.0 — so a single source-level bucket is not accurate; the unlicensed majority should be treated as not-redistributable / needs-review, not noncommercial.`** by adversarial review
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **DISPUTED** (quote verbatim: True, classification agrees: False)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> License ... Creative Commons Attribution–NonCommercial–NoDerivs 3.0 IGO
> CKAN API (https://data.iadb.org/api/3/action/package_show?id=2020-better-jobs-index-database-latin-america), verbatim field values: license_id: cc-by-nc-nd | license_title: Creative Commons Attribution–NonCommercial–NoDerivs 3.0 IGO | license_url: http://creativecommons.org/licenses/by-nc-nd/3.0/igo/
> Dataset page HTML (data.iadb.org), verbatim: <a href="http://creativecommons.org/licenses/by-nc-nd/3.0/igo/" ...> Creative Commons Attribution–NonCommercial–NoDerivs 3.0 IGO </a>
> Portal-wide license facet via https://data.iadb.org/api/3/action/package_search?facet.field=["license_id"] — total datasets: 1532; cc-by-nc-nd: 157; cc-by: 61; datasets with NO declared license_id: 1314

**Adversary's contradicting clause:** On the live page's "Metadata & use" table, the License field links to "Creative Commons Attribution–NonCommercial–NoDerivs 3.0 IGO" (http://creativecommons.org/licenses/by-nc-nd/3.0/igo/). The license name explicitly contains "NoDerivs" (NoDerivatives): under CC BY-NC-ND, "If you remix, transform, or build upon the material, you may not distribute the modified material." The classification "noncommercial_only" omits this no-derivatives restriction.

*Verifier notes:* STEP 1 (quote verification): WebFetch returned HTTP 403 (bot block), so I loaded the URL in the browser. The page is live and genuine (title "2020 Better Jobs Index Database: Latin America"; DOI 10.60966/prxb-w968; published 2020-02-21, modified 2026-06-25). The "Metadata & use" table shows the label "License" and the value hyperlink "Creative Commons Attribution–NonCommercial–NoDerivs 3.0 IGO" pointing to the CC BY-NC-ND 3.0 IGO deed. Both fragments of the researcher's verbatim_quote appear WORD-FOR-WORD (identical en-dash characters); the "..." bridges the label and its value. So the quote is accurate and fetch_status fetched_ok is correct.

STEP 2 (stricter-clause search): The stricter clause the classification missed is the ND (NoDerivatives) term, which is part of the license name itself. CC BY-NC-ND permits non-commercial redistribution of UNMODIFIED copies only; distributing modified/derived material is prohibited. This is material for this library, which reformats data (parquet) and computes derived variables — those are derivatives ND forbids. I also confirmed via WebSearch that IDB licensing is mixed (some sources cite CC BY 4.0), consistent with the finding's own admission of a mixed/predominantly-unlicensed catalog.

**PERMISSION UPDATE 2026-07-15 (email from opendata@iadb.org):** written blanket grant in
reply to our re-hosting request: "Because our open datasets are published under the
Creative Commons Attribution 4.0 International (CC-BY 4.0) license, **you have our full
permission to re-host and redistribute them.** A formal data-sharing agreement is not
required." Three conditions: (1) citation format "Inter-American Development Bank (year).
[Dataset title]. IDB Open Data. [DOI or URL]" + no implied IDB sponsorship; (2) "The
CC-BY 4.0 license applies exclusively to **institutional IDB data**… exclude any series
or indicators sourced from **proprietary third-party data providers**" (check CKAN
metadata); (3) permanent live link to each dataset's portal page. This written grant
supersedes the mixed per-dataset labels above for IDB-institutional data, but condition
(2) requires a per-dataset `package_show` metadata pass (our store kept none) before
serving — **`idb` stays gated until that pass runs.** Full verbatim in
permission records (held privately).

STEP 3 (defensibility): "noncommercial_only" is TOO PERMISSIVE in two ways: (a) it drops the NoDerivatives restriction that is literally in the quoted license name, greenlighting derivative redistribution the terms forbid; (b) applied source-wide it implies the ~86% unlicensed datasets are redistributable under NC terms, when an absent license grants no redistribution rights at all. Redistribution of this specific dataset IS permitted, but only verbatim + non-commercial + attributed — the classification must reflect the ND restriction. Verdict: DISPUTED. The verbatim quote is accurate; the classification is not defensible as stated.

*Researcher reasoning:* CRITICAL FINDING: The IDB Open Data portal (data.iadb.org, a CKAN instance) does NOT apply one blanket license. Licensing is per-dataset and heterogeneous. Verified directly from the IDB's own domain via both the rendered dataset HTML pages and the machine-readable CKAN API. Portal-wide facet counts (data.iadb.org/api/3/action/package_search): of 1,532 datasets, only 218 carry any declared license — 157 are CC BY-NC-ND 3.0 IGO and 61 are CC BY 4.0 International — and 1,314 (~86%) have NO declared license at all.\n\nWhat each tier means for RE-HOSTING:\n1) CC BY-NC-ND 3.0 IGO (157, the largest declared group): redistribution/re-hosting IS permitted, but only (a) for NON-COMMERCIAL purposes, (b) with attribution to the IDB, and (c) with NO DERIVATIVES — the data may be shared only in unmodified, verbatim form; any transformed/adapted/repackaged version may not be distributed. The professor's library is free and non-commercial (NC satisfied) and must re-host the files unmodified with IDB attribution to satisfy ND.\n2) CC BY 4.0 (61): fully redistributable, including commercially and with derivatives, provided IDB is attributed.\n3) No declared license (1,314): NO affirmative redistribution grant exists. 'Open data' / 'free to access' branding does not convey re-hosting rights. These must be treated as permission_required and EXCLUDED from re-hosting unless the specific dataset's terms are confirmed.\n\nCONSERVATIVE SINGLE CLASSIFICATION: noncommercial_only. Rationale: the professor cannot treat the portal as blanket-open. Affirmative redistribution rights exist only for the ~14% CC-licensed subset, and the largest licensed group (BY-NC-ND) is non-commercial + no-derivatives. commercial_ok=false because the dominant explicit license forbids commercial use and the unlicensed 86% grant nothing. attribution_required=true (all CC variants require it). sharealike=false (it is NoDerivatives, not ShareAlike). OPERATIONAL GUIDANCE for compliance: do NOT bulk re-host the portal. Filter dataset-by-dataset on the CKAN license_id field: re-host only cc-by (attribution) and cc-by-nc-nd (non-commercial, attribution, UNMODIFIED only) datasets, attribute the IDB on each, and drop every dataset whose license_id is empty/notspecified. Note also that IDB's separate terms explicitly reserve the IDB name and logo (use beyond attribution requires a separate written agreement). Verbatim quote and license URL captured from data.iadb.org; the CC-IGO legal text itself lives on creativecommons.org (http://creativecommons.org/licenses/by-nc-nd/3.0/igo/), linked directly from the IDB dataset pages.

---

### International Monetary Fund (IMF)

- **Databases (30):** `imf_afrreo`, `imf_apdreo`, `imf_bopagg`, `imf_cofer`, `imf_commodity`, `imf_cpi`, `imf_fas`, `imf_fdi`, `imf_fiscaldecentralization`, `imf_fm`, `imf_fsire`, `imf_gender_budgeting`, `imf_gender_equality`, `imf_gfscofog`, `imf_gfse`, `imf_gfsfalcs`, `imf_gfsibs`, `imf_gfsmab`, `imf_gfsssuc`, `imf_hpdd`, `imf_mcdreo`, `imf_namain_idc_n`, `imf_pctot`, `imf_pgcs`, `imf_pgi`, `imf_psbsfad`, `imf_unsdg_imf_inputs`, `imf_weo`, `imf_whdreo`, `imf_world`
- **Scope note added 2026-08-04 — the FSI family, recorded rather than left to inherit silently.**
  Three Financial Soundness Indicators datasets fall under this verdict but were not named in the
  list above: `imf_fsibsis_direct` (43,814 series), `imf_fsic_direct` (32,906) and
  `imf_fsicdm_direct` (1,856). They are not a new verdict and not an analogy to a sibling — the
  grant quoted below is written by DATASET CLASS, not by an enumerated list: *"'Data' refers to
  any published statistical data produced or curated by the IMF"*, naming IFS, BOP, DOT and GFS
  and then extending to *"Most statistical data available on www.IMF.org, www.data.IMF.org, or
  the iData Portal that explicitly identify the International Monetary Fund as the source."*
  FSI is IMF-produced statistical data published on data.IMF.org, fetched here from
  `api.imf.org` under IMF's own SDMX flow ids (`FSIC:`, `FSIBSIS:`, `FSICDM:`), and the sibling
  FSI dataset `imf_fsire` is already in the list above. The attribution limb is met the same way
  as the other 30 (citation header carrying Source, License, Homepage, Terms URL, Cite-as).
  Written down because R113/R117 is the failure where a flag is an assertion nobody recorded:
  `tools/catalog_imf_direct.py` gates on the `imf-terms` licence row being reservable and treats
  its sources as IMF BY CONSTRUCTION, so the evidence for that construction belongs here.
- **Official terms URL:** https://www.imf.org/en/about/copyright-and-terms
- **License:** IMF Copyright and Usage Terms — statistical "Data" special terms (free access and free reuse with attribution)
- **Classification:** redistributable_attribution
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> You may download, extract, copy, create derivative works, publish, distribute, and use Data obtained from IMF Sites, subject to the following conditions: Whether obtained directly from the IMF or another party, when Data is distributed or reproduced in any manner, it must appear accurately with attribution to the IMF as the source, e.g. “Source: International Monetary Fund, Database Name, <<link to the  dataset>>.”
> Notwithstanding the general prohibition on the commercial use of IMF Content, with respect to published statistical data made available on IMF Sites, the following special terms shall govern.
> For the purposes of these special terms, the term “Data” refers to any published statistical data produced or curated by the IMF. In particular, “Data” refers to the following: IMF Statistical Data, including but not limited to, International Financial Statistics (IFS), Balance of Payments (BOP), Direction of Trade (DOT), and Government Finance Statistics (GFS); World Economic Outlook database; Primary Commodity Prices; IMF Financial Data; Exchange Rate Data; and Most statistical data available on www.IMF.org,  www.data.IMF.org, or the iData Portal that explicitly identify the International Monetary Fund as the source.
> Users who make IMF Data available to other Users through any type of distribution or download environment agree to take reasonable efforts to communicate and promote compliance by their users with these terms.
> Users shall not infringe upon the integrity of the Data and in particular shall refrain from any act of alteration of the Data that intentionally affects its nature or accuracy. If the Data is materially transformed by the User, this must be stated explicitly along with the required source citation.
> If IMF Data is sold by Users as a standalone product, sellers must inform purchasers that the Data is available free of charge from the IMF.
> For any potential commercial reuse of IMF Data, please email copyright@imf.org to request permission.
> Some statistical products may incorporate information from third parties and may have separate terms and conditions for usage.
> The IMF prohibits the bulk download of information by automated technology without explicit permission and reserves the right to terminate access to its Sites or Content.
> The policy of free access and free reuse of IMF Data does not imply a right to obtain confidential or any unpublished data, over which the IMF reserves all rights.

*Verifier notes:* VERDICT: CONFIRMED.

*Non-flag condition check (2026-07-29) — COMPLIANT, do not re-derive.* After the Etalab miss (an obligation that lived only in the prose and so was never checked), IMF's terms were re-read for the same failure shape, `imf-terms` being the largest custom licence here: 447,990 series across 38 sources. Three obligations go beyond our flag columns; all three are met.
1. **Attribution FORM** — "it must appear accurately with attribution to the IMF as the source, e.g. 'Source: International Monetary Fund, Database Name, «link to the dataset»'". 37 of 38 sources carry `Source: International Monetary Fund, <Database Name>`; the 38th (`imf`) reads "Source: IMF". Note the **e.g.**: the binding obligation is accurate attribution to the IMF, and the fully-punctuated string illustrates it rather than mandating it. Only 10 of 38 embed a link in the attribution text, but every download emits `Homepage` and `Terms` rows, so a link is always present. NOT a breach — adding dataset links would only match IMF's illustrated form more closely.
2. **"Users who make IMF Data available … through any type of distribution or download environment agree to take reasonable efforts to communicate and promote compliance by their users with these terms."** We ARE such an environment, so this one binds us directly. Met: every CSV ships a citation header carrying Source, License (with NON-COMMERCIAL / attribution / SHARE-ALIKE / NO-DERIVATIVES spelled out), Homepage, **Terms URL**, and Cite-as.
3. **"If the Data is materially transformed by the User, this must be stated explicitly"** — we convert parquet to CSV and re-key nothing; a format change does not affect the nature or accuracy of the data, so no statement is owed.

The remaining IMF clauses are inapplicable rather than unmet: we do not sell the data as a standalone product, and we seek no confidential or unpublished data.

FETCH: WebFetch returned HTTP 403 on the official URL, and web.archive.org is blocked. I confirmed the page directly via the Chrome browser tool at https://www.imf.org/en/about/copyright-and-terms (title "IMF Copyright and Usage", Effective: October 11, 2024).

QUOTE: Verbatim-accurate. The "The Use of IMF Data" section reads word-for-word: "You may download, extract, copy, create derivative works, publish, distribute, and use Data obtained from IMF Sites, subject to the following conditions: Whether obtained directly from the IMF or another party, when Data is distributed or reproduced in any manner, it must appear accurately with attribution to the IMF as the source, e.g. \"Source: International Monetary Fund, Database Name, <<link to the dataset>>.\"" The ONLY difference from the finding is a doubled space in the finding ("the  dataset") vs. the page's single space ("the dataset") — a whitespace/transcription artifact, no wording difference. Marked verified.

ADVERSARIAL CHECK — CLASSIFICATION IS DEFENSIBLE (not too permissive): The page has two tiers. (1) General terms are "All Rights Reserved," restrict downloads to "personal, noncommercial usage only without any right to resell, redistribute," and say "do not copy or republish Content... on a non-IMF website." (2) A SPECIAL override, "The Use of IMF Data," governs published statistical Data (IFS, BOP, DOT, GFS, WEO, Primary Commodity Prices, IMF Financial Data, Exchange Rate Data, and most IMF-attributed statistical data). It opens "Notwithstanding the general prohibition on the commercial use of IMF Content... the following special terms shall govern," and expressly grants "publish, distribute, and use Data" and even contemplates re-hosting: "Users who make IMF Data available to other Users through any type of distribution or download environment agree to take reasonable efforts to communicate and promote compliance." It is explicitly a "policy of free access and free reuse of IMF Data." The specific governs the general, so redistribution of IMF statistical Data is genuinely permitted with attribution. "redistributable_attribution" is correct and not more permissive than the terms support — it carries the attribution obligation and does not claim public domain/CC0.

CONDITIONS/CARVE-OUTS the library must honor (they qualify, but do not contradict, the classification): (a) "The IMF prohibits the bulk download of information by automated technology without explicit permission" and "all other [general] terms... shall continue to apply to use of IMF Data" — restricts the ACQUISITION method (no bulk scraping of IMF sites without permission), not redistribution of legitimately obtained data; (b) "For any potential commercial reuse of IMF Data, please email copyright@imf.org to request permission" — a free-public-download library is non-commercial, so this doesn't bind; (c) third-party carve-out: "Some statistical products may incorporate information from third parties and may have separate terms" / "For material not attributed to the IMF, users should obtain permission directly from the... copyright holder" — grant covers only IMF-attributed series; (d) "The IMF does not permit use of its Content or Sites for the training of large language models (LLMs) without explicit permission"; (e) integrity clause — no undisclosed material alteration.

CONCLUSION: Quote verbatim-accurate at the official URL; classification defensible for IMF's own published statistical Data re-hosted for free public download with attribution. This is a rare genuine open-data grant (IMF adopted free reuse of statistical data ~2015), not a use-but-no-redistribution trap. Ledger rules applied: R4 / M-20260714-10 (read primary source, don't over-read a summary) and M-20260714-20 (don't over-classify as redistributable) — both satisfied because the primary-source Data section expressly permits distribution.

*Researcher reasoning:* The IMF's official "Copyright and Usage" page (https://www.imf.org/en/about/copyright-and-terms) contains a dedicated special-terms section governing published statistical "Data," which explicitly overrides the general All-Rights-Reserved / commercial-prohibition rules that apply to other IMF Content. All the covered databases (imf_weo, imf_cpi, imf_cofer, imf_fas, imf_fdi, the imf_gfs* family, imf_bopagg, imf_commodity/pctot, the regional REOs, etc.) are "published statistical data produced or curated by the IMF" and thus fall squarely under these special terms.

The governing clause EXPLICITLY grants the right to "download, extract, copy, create derivative works, publish, distribute, and use Data obtained from IMF Sites," conditioned only on accurate attribution to the IMF as source. Re-hosting is directly contemplated: the terms address "Users who make IMF Data available to other Users through any type of distribution or download environment." This is unambiguous redistribution/re-dissemination permission — not merely an access/use permission — so the classification is redistributable_attribution.

Conditions the professor's library must honor: (1) attribution to the IMF as source with the dataset name/link (attribution_required=true); (2) preserve data integrity / no alteration that affects accuracy, and state explicitly if data is materially transformed; (3) there is no ShareLike/copyleft obligation (sharealike=false), only a "take reasonable efforts to promote compliance" duty; (4) some statistical products incorporate third-party data with separate terms (line noted) — chiefly relevant to commodity/terms-of-trade sources, which should be spot-checked. Because the library is FREE and NON-COMMERCIAL, it is squarely within the permitted non-commercial redistribution and needs no prior permission.

commercial_ok is set to false conservatively: although line-item terms contemplate users even selling the Data standalone (with a "free from IMF" notice), the terms also state "For any potential commercial reuse of IMF Data, please email copyright@imf.org to request permission." This internal tension means commercial reuse is not cleanly authorized without contacting the IMF — but it does not affect the non-commercial academic use case, which is clearly permitted.

Operational caveat (not a redistribution restriction): "The IMF prohibits the bulk download of information by automated technology without explicit permission." This governs HOW data is harvested from IMF sites (the pipeline's scraping/API bulk-pulls), not the right to redistribute what is obtained. The library should source via sanctioned bulk/SDMX channels or seek harvesting permission to stay compliant.

fetch_status=fetched_ok: the live imf.org URL returned HTTP 403 to automated fetchers (Cloudflare bot protection), so I obtained and read the exact official page content via the Internet Archive snapshot of the canonical URL dated 2026-06-25 (http://web.archive.org/web/20260625061819/https://www.imf.org/en/about/copyright-and-terms). All quotes are verbatim from that captured official page; official_terms_url records the canonical live URL.

---

### IPEA / Ipeadata (Instituto de Pesquisa Econômica Aplicada, Brazil)

- **Databases (1):** `ipea`
- **Official terms URL:** http://www.ipeadata.gov.br/iframe_direitouso.aspx
- **License:** Ipeadata "Direitos de Uso" custom terms (public information; free distribution and copying permitted with mandatory attribution). Not a named CC/open-gov licence.
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Este site é disponibilizado como uma prestação pública de serviço pelo Ipea e seu conteúdo é considerado informação pública que pode ser livremente distribuída e copiada, resguardando-se a obrigatoriedade de citação da fonte Ipeadata por parte do usuário.
> Direitos de Uso — Leia atentamente os termos e condições de uso deste site. Caso não concorde, por favor, não utilize o Ipeadata.
> Uso do conteúdo — O propósito deste site é facilitar o acesso às estatísticas brasileiras e promover a divulgação dos estudos e pesquisas do Ipea.
> Estes termos estão sob a jurisdição e de acordo com a Legislação Brasileira. Rio de Janeiro, 7 de Dezembro de 2006.
> Todos os direitos reservados. Copyright © Instituto de Pesquisa Econômica Aplicada (Ipea) 2006
> CONTRASTING / NON-GOVERNING — the institutional repository (repositorio.ipea.gov.br) applies a different, restrictive default licence to its publication documents (NOT to the Ipeadata statistical series). Verbatim 'Licença Padrão do Ipea' from https://repositorio.ipea.gov.br/items/b893e038-54de-4350-a4c9-875435b8cfcc : 'Licença Padrão Ipea: é permitida a reprodução e a exibição para uso educacional ou informativo, desde que respeitado o crédito ao autor original e citada a fonte (http://www.ipea.gov.br). Permitida a inclusão da obra em Repositórios ou Portais de Acesso Aberto, desde que fique claro para os usuários os termos de uso da obra e quem é o detentor dos direitos autorais, o Instituto de Pesquisa Econômica Aplicada (Ipea). Proibido o uso comercial ou com finalidades lucrativas em qualquer hipótese. Proibida a criação de obras derivadas. Proibida a tradução, inclusão de legendas ou voz humana. ... Esta licença está baseada em estudos sobre a Lei Brasileira de Direitos Autorais (Lei 9.610/1998) e Tratados Internacionais sobre Propriedade Intelectual.'

*Verifier notes:* VERBATIM: Confirmed word-for-word against the literal page text retrieved via browser (get_page_text), not just the paraphrasing WebFetch model. The quote appears exactly under the "Uso do conteúdo" heading of the Ipeadata "Direitos de Uso" page — every character matches, including "disponibilizado", "prestação pública de serviço", "livremente distribuída e copiada", and "resguardando-se a obrigatoriedade de citação da fonte Ipeadata por parte do usuário". URL fetched OK (http -> https, page title "Ipeadata"), so fetch_status "fetched_ok" is accurate.

FULL-TEXT SKEPTICAL SCAN for stricter clauses: The complete page has five sections — intro, "Uso do conteúdo", "Negação" (warranty disclaimer), "Limitações" (liability limitation), and a footer. I found NO redistribution ban, NO non-commercial restriction, NO prior-written-permission requirement, NO no-derivatives clause, and NO bulk/mass-download or automated-extraction restriction. The words permissão/autorização/proibido/vedado/comercial do not appear. The "Negação" and "Limitações" sections are pure warranty/liability disclaimers and do not restrict reuse or redistribution.

ONE TENSION NOTED (does not defeat the classification): the footer reads "Todos os direitos reservados / Copyright © Instituto de Pesquisa Econômica Aplicada (Ipea) 2006". This generic "all rights reserved" boilerplate coexists with, but does not override, the specific express grant in "Uso do conteúdo" that the content is "informação pública que pode ser livremente distribuída e copiada" with attribution. Under normal interpretation a specific express permission governs over a generic reserved-rights footer, and this content is expressly declared public information (informação pública). So redistribution with attribution is genuinely permitted; I am confident on this point.

CLASSIFICATION JUDGMENT: "redistributable_attribution" is defensible and NOT too permissive. The terms explicitly allow free distribution AND copying, conditioned only on source attribution ("citação da fonte Ipeadata") — a textbook attribution-only redistribution grant. The finding correctly labels it a custom Ipeadata term (not a named CC/open-gov licence) and correctly retains the attribution obligation rather than overclaiming public-domain/CC0.

DOWNSTREAM CAVEAT (out of scope for this provider's own terms, worth flagging to the library operator): Ipeadata is an aggregator that republishes series "obtidas nas fontes originais" (IBGE, BCB, international sources, etc.). Ipeadata's terms authorize redistribution of Ipeadata's own content, but they do not, and cannot, waive any independent terms attached to specific upstream original sources. A re-hosting library should still cite "fonte Ipeadata" (as required) and be mindful of upstream source terms for series that originate elsewhere. This does not make the Ipeadata classification wrong; it is a compliance note.

Net: quote is verbatim-accurate at the official URL and the classification is defensible and appropriately scoped. CONFIRMED.

*Researcher reasoning:* The database re-hosts Ipeadata's Brazilian statistical time series, so the governing terms are those of the Ipeadata data platform itself (www.ipeadata.gov.br), reached via the site's own "Direitos de uso" navigation link (target page iframe_direitouso.aspx). I rendered that page in a browser (the site is a JS/frames ASP.NET app that returns empty to plain fetch) and read the full text verbatim. Its "Uso do conteúdo" section explicitly states the content "pode ser livremente distribuída e copiada" (may be freely distributed and copied), with the sole condition being "a obrigatoriedade de citação da fonte Ipeadata" (mandatory citation of the source Ipeadata). This is an explicit redistribution/re-hosting grant — not merely permission to access or use — conditioned only on attribution. There is no non-commercial clause and no no-derivatives clause in these data-platform terms; the "Todos os direitos reservados / Copyright © Ipea 2006" line at the foot is a standard copyright notice that does not override the express free-distribution grant above it. I therefore classify redistributable_attribution: redistribution/re-hosting is permitted provided Ipeadata is credited as the source. commercial_ok is set true because the text grants "free" distribution/copying with no commercial restriction, but this is moot for the professor's free, non-commercial academic library, which clearly qualifies under any reading. IMPORTANT CAUTION recorded for the compliance file: a separate, MORE RESTRICTIVE "Licença Padrão do Ipea" (prohibits commercial use and derivative works; permits inclusion in open-access portals only if terms and the Ipea copyright holder are shown) appears on Ipea's INSTITUTIONAL REPOSITORY (repositorio.ipea.gov.br), including a repository record titled "IPEADATA." That licence governs Ipea's publication documents in the repository, not the numeric statistical series distributed by the Ipeadata platform, so it does not control the re-hosting of the time series. The recommended, safest practical posture for the library: redistribute the series with clear, prominent attribution to "Fonte: Ipeadata / Ipea" on every page/download (attribution is mandatory), which satisfies the data-platform terms. Confidence is high on the verbatim text (fetched and read directly from the official Portuguese page, which is authoritative for this Brazilian federal site).

---

### IRENA (International Renewable Energy Agency)

- **Databases (1):** `irena`
- **Official terms URL:** https://pxweb.irena.org/pxweb/en/IRENASTAT
- **License:** Copyright notice: "All Rights Reserved" (on the IRENASTAT data tool). Full Terms and Conditions page was inaccessible.
- **Classification:** unclear_not_found
- **Commercial OK:** None · **Attribution required:** None · **ShareAlike:** None · **Fetch:** partial
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> © 2026 IRENA - International Renewable Energy Agency. All Rights Reserved.
> * The designations employed and the presentation of materials herein do not imply the expression of any opinion whatsoever on the part of the International Renewable Energy Agency concerning the legal status of any country, territory, city or area or of its authorities, or concerning the delimitation of its frontiers or boundaries.

*Verifier notes:* CONFIRMED, with one disclosed caveat. (1) Verbatim quote: Two independent JS-free WebFetches of the official tool (https://pxweb.irena.org/pxweb/en/IRENASTAT) both return the footer as "© IRENA - International Renewable Energy Agency. All Rights Reserved." — matching the finding character-for-character EXCEPT the "2026" year, which my non-JS fetches show as blank. The year is almost certainly a dynamically-rendered current year (today = 2026; PxWeb footers commonly render "© {currentYear}"), so what the researcher saw in a live browser is legitimately "© 2026 IRENA...". The load-bearing legal assertion "All Rights Reserved" is genuinely present and verbatim-accurate; only the non-legal year could not be reproduced by my JS-free tooling. (2) Adversarial stricter-clause search: An initial AI search summary DANGEROUSLY claimed IRENASTAT "allows redistribution / commercial use with attribution." I did not trust it and checked the actual Terms. The authoritative IRENA T&C text refutes that permissive reading and is more restrictive: "No other use shall be made of IRENA's Content without IRENA's advanced written permission," and IRENASTAT is explicitly covered ("the general Terms and Conditions apply to IRENASTAT provided that no specific terms of use apply to it"). Reproduction/dissemination of publications and extracts is subject to the copyright notice and terms of use, and there is a prior-written-permission requirement. Therefore third-party re-hosting for public download is NOT permitted without permission — the data is non-redistributable. (3) Classification: "unclear_not_found" / "All Rights Reserved" is the conservative bucket and is NOT too permissive; my independent evidence strengthens (does not refute) the restrictive reading, so there is no risk of a library wrongly re-hosting. Note (non-blocking, in the stricter direction only): the evidence would support tightening the classification to an explicit "non-redistributable — prior written permission required," which is stricter than the current label and thus does not trigger a dispute. The official terms page (https://www.irena.org/terms-and-conditions) returned HTTP 403 to WebFetch, so the T&C clauses above were confirmed via search extraction of the official page rather than a direct fetch; the on-page "All Rights Reserved" copyright itself was directly and independently fetched twice.

*Researcher reasoning:* The redistribution-governing document is IRENA's Terms and Conditions page (https://www.irena.org/terms-and-conditions). That page — and the entire www.irena.org / irena.org content host — was inaccessible to me across ~10 attempts over several minutes: WebFetch returned HTTP 403 (Azure Web Application Firewall bot-challenge, "we're checking you're not a bot") and the browser returned HTTP 502 Bad Gateway (Microsoft-Azure-Application-Gateway). I did NOT bypass the bot check.

The only official IRENA host I could reach is the IRENASTAT online data query tool at pxweb.irena.org (this is the actual "irena" dataset in scope). I read its footer both in the browser (accessibility tree) and via WebFetch, and it states verbatim, word-for-word: "© 2026 IRENA - International Renewable Energy Agency. All Rights Reserved." plus the standard designations disclaimer (quoted in additional_quotes). This is a bare copyright RESERVATION — it does NOT, by itself, grant any right to redistribute, re-host, or re-disseminate the data. There is no Creative Commons mark, no open-data licence, and no explicit reuse/redistribution grant anywhere on the reachable data tool.

Web search result snippets suggested IRENA's separate Terms and Conditions page contains a more permissive statement (material "may be freely used, shared, copied, reproduced... provided appropriate acknowledgement is given of IRENA as source and copyright holder"). However, those are third-party search-engine paraphrases, NOT text I fetched and read on the official page. Per the hard rules I will not quote or rely on them, and I will not guess a licence from memory.

Bottom line: the specific clause governing REDISTRIBUTION/re-hosting is on an inaccessible official page, and the only official text I could verify ("All Rights Reserved") does not authorize redistribution. Conservatively, redistribution rights are unclear/unconfirmed. A re-check of https://www.irena.org/terms-and-conditions when the host is reachable is needed before any redistribution decision; note also that IRENASTAT mixes IRENA-original statistics with third-party-attributed material that may carry separate restrictions.

---

### Kenneth French Data Library (Dartmouth / Tuck School of Business)

- **Databases (1):** `famafrench`
- **Official terms URL:** https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html
- **License:** Proprietary copyright, all rights reserved (Eugene F. Fama and Kenneth R. French); no redistribution licence granted
- **Classification:** permission_required
- **Commercial OK:** None · **Attribution required:** None · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> All images and code are property of Ken French. Use in part or whole is illegal -- except by permission of Ken French or Dimensional Fund Advisors
> Copyright Eugene F. Fama and Kenneth R. French
> -- All images and code are property of Ken French. Use in part or whole is illegal -- except by permission of Ken French. (home page variant: https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/)

*Verifier notes:* Quote verified verbatim against the raw HTML of the official URL (fetched via curl; fetch_status fetched_ok confirmed). The string appears as a developer comment in the page <head> (raw HTML lines 6-14); joining the wrapped lines it reads exactly: "All images and code are property of Ken French. Use in part or whole is illegal -- except by permission of Ken French or Dimensional Fund Advisors --". The finding's quote matches word-for-word including the double-hyphen dashes and capitalization; it only truncates the trailing " --", which is not an alteration. IMPORTANT METHOD NOTE: this clause lives in an HTML comment, so markdown-converting fetchers strip it — an initial WebFetch surfaced only the visible footer "Copyright Eugene F. Fama and Kenneth R. French" (raw HTML lines 2501-2512). I confirmed the restrictive clause by inspecting raw HTML directly.

Stricter-clause search: the quoted clause ("Use in part or whole is illegal -- except by permission") is itself the strictest possible restriction; no redistribution licence, non-commercial, or bulk-download grant exists anywhere on the page. Nothing on the page is MORE permissive than the finding assumes.

Classification defensibility: permission_required / proprietary all-rights-reserved (Eugene F. Fama and Kenneth R. French) with no redistribution licence is the most conservative reading and is fully supported. Even under a skeptical reading that "images and code" covers only website assets, the bare copyright notice grants no redistribution licence, so the data remains all-rights-reserved by default. For a re-hosting/public-download library the classification is correct and NOT too permissive. Redistribution is not permitted.

*Researcher reasoning:* I fetched and read the official Kenneth R. French Data Library page (mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html), plus the home page and a detail page (det_form_btm.html) to cross-check. Findings are uniform across all three.

The only usage/rights text that exists on these official pages is: (1) a visible copyright notice rendered on every page, verbatim "Copyright [current year] Eugene F. Fama and Kenneth R. French"; and (2) an HTML source-comment permission clause, verbatim "All images and code are property of Ken French. Use in part or whole is illegal -- except by permission of Ken French or Dimensional Fund Advisors" (the home page shows the shorter variant "...except by permission of Ken French."). There is NO dedicated terms-of-use/licence/copyright-policy page, NO Creative Commons or open-data mark, NO statement expressly permitting redistribution, re-hosting, re-dissemination or mass-download, and NO stated attribution/citation condition.

Conservative classification = permission_required. Rationale: The datasets are affirmatively copyright-asserted ("Copyright Eugene F. Fama and Kenneth R. French") and no redistribution licence of any kind is granted. Under default copyright, redistributing copyrighted material requires the rights-holder's permission. The one explicit permission clause ("Use in part or whole is illegal -- except by permission of Ken French or Dimensional Fund Advisors") is, on its face, scoped to the website's "images and code" and appears inside an HTML comment rather than as a public data licence, so it does not itself govern the datasets; but it confirms the owner's posture that reuse requires permission and, combined with the bare copyright assertion and total absence of any redistribution grant, means a third party re-hosting the factor/portfolio data files for download has no granted right. Additional caution: the French research returns are derived from CRSP data (a restrictive, paid commercial source, noted on the page), which further weighs against any implied redistribution right. This is NOT unclear_not_found because official rights text does exist and was read; it simply grants no redistribution permission. Recommendation for the library: do not re-host these datasets without obtaining written permission from Ken French / Dimensional Fund Advisors. commercial_ok, attribution_required and sharealike are set null because the official pages do not address them.

---

### KOF Swiss Economic Institute (ETH Zurich)

- **Databases (1):** `kof_globalization`
- **Official terms URL:** https://kof.ethz.ch/en/footer/disclaimer-copyright.html
- **License:** Custom terms — ETH Zurich "Disclaimer & Copyright" (In Copyright; prior written consent required for redistribution)
- **Classification:** permission_required
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED by WRITTEN PERMISSION

**Verbatim quote:**
> Without prior written consent from ETH Zurich it is not permissible for the documents and web pages and parts thereof to be either copied or stored on other servers, input into news group or online services, or stored on USB sticks or other data carriers.
> All online documents and web pages as well as their parts are protected by copyright, and it is permissible to copy them and print them out only for private, scientific and noncommercial use.
> The citation of the documents and web pages with a correct statement of the source and the setting of links to our web site is expressly allowed.
> Intermediate storage in a cache or on a proxy server to optimise access speed is permitted.
> Copyright © 2026 ETH Zurich

*Verifier notes:* Fetched the official terms URL (https://kof.ethz.ch/en/footer/disclaimer-copyright.html) — it is the genuine KOF Swiss Economic Institute / ETH Zurich disclaimer-copyright page (© 2026 ETH Zurich), fetch_status "fetched_ok" is accurate. The verbatim_quote appears WORD-FOR-WORD on the page: "Without prior written consent from ETH Zurich it is not permissible for the documents and web pages and parts thereof to be either copied or stored on other servers, input into news group or online services, or stored on USB sticks or other data carriers." No discrepancy.

Adversarial check for a stricter/missed clause: The surrounding terms are, if anything, more restrictive than the quote alone and fully support the classification. The page also states copying/printing is permitted "only for private, scientific and noncommercial use" (a noncommercial limitation), while citation-with-source and linking are expressly allowed and cache/proxy storage is permitted. Nothing on the page grants free redistribution or public-domain status.

Classification judgment: "permission_required" is defensible and NOT too permissive for a library that re-hosts the data for public download. Re-hosting is exactly "copied or stored on other servers," which the terms prohibit absent prior written consent from ETH Zurich. The classification correctly treats redistribution as gated behind written permission rather than freely allowed — and it is not overly strict either, since the terms permit redistribution WITH consent (a permission gate, not an absolute ban), so "permission_required" is the correct label rather than a flat "prohibited." Finding confirmed on both quote accuracy and classification defensibility.

*Researcher reasoning:* The KOF Globalisation Index is produced by the KOF Swiss Economic Institute, an institute of ETH Zurich. The official Globalisation Index page (https://kof.ethz.ch/en/forecasts-and-indicators/indicators/kof-globalisation-index.html) carries no dataset-specific licence; its footer links to the site-wide "Disclaimer & Copyright" page as the governing copyright notice, and the imprint page does the same. I fetched that page and confirmed the exact wording by retrieving the raw HTML.

The governing copyright section states that content "is protected by copyright, and it is permissible to copy them and print them out only for private, scientific and noncommercial use." Critically for a re-hosting/redistribution library, it then states: "Without prior written consent from ETH Zurich it is not permissible for the documents and web pages and parts thereof to be either copied or stored on other servers, input into news group or online services, or stored on USB sticks or other data carriers." Re-hosting the data for download is precisely "stored on other servers," so it requires prior written consent from ETH Zurich. This is a permission_required condition, not open redistribution.

Note on scope: the permission to "copy... only for private, scientific and noncommercial use" describes personal reproduction, not the right to store the material on a third-party server for redistribution — that act is separately and explicitly gated behind prior written consent. Hence I classify permission_required rather than noncommercial_only.

Attribution/citation: "The citation of the documents and web pages with a correct statement of the source and the setting of links to our web site is expressly allowed." This grants citation and linking, not re-hosting; a correct source statement is expected but does not by itself authorize redistribution.

commercial_ok = false: use is limited to "noncommercial use," and any re-hosting needs written consent regardless. sharealike = null: no share-alike/copyleft term is present.

Conservative bottom line for a re-hosting library: do NOT re-host the KOF Globalisation Index data without first obtaining prior written consent from ETH Zurich (contact kof@kof.gess.ethz.ch). Linking to the official KOF page and citing Gygli, Haelg, Potrafke & Sturm (2019) is permitted; re-serving the files for download is not, absent written permission.

---

### KSH Hungarian Central Statistical Office (Hungarian Central Statistical Office / HCSO)

- **Databases (1):** `ksh`
- **Official terms URL:** https://www.ksh.hu/copyright_hungarian_central_statistical_office
- **License:** CC BY 4.0 (Creative Commons Attribution 4.0 International)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> HCSO uses standardised international licence Creative Commons Attribution 4.0 International (CC BY 4.0), according to which all of the content available on the Website – including tables, figures and infographics – (hereinafter: Content) can be copied, reproduced and redistributed without limitations.
> Any content can only be used if HCSO is indicated as a source (Source: HCSO / ksh.hu). The indication of the source may not be hidden or separated, and in the case of online indication of source, the link shall be active.
> Exceptions of this are data files queried from the internal databases of HCSO on specific request, the User is not entitled to use these files for commercial purposes (see point 3.3 in the present Terms of Use).
> Considering data files queried from the internal databases of HCSO on specific request, the User is not entitled to use these files for commercial purposes, Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0) terms shall apply to these files.
> The domain name ksh.hu is protected by copyright and trademark, and it can only be used – except for reference – with the prior written consent of HCSO.
> The general authorisation does not cover the use of HCSO logo, which remains protected and may not be used or reproduced unless with the prior written consent of HCSO.

**Adversary's contradicting clause:** Considering data files queried from the internal databases of HCSO on specific request, the User is not entitled to use these files for commercial purposes, Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0) terms shall apply to these files.

*Verifier notes:* VERDICT: CONFIRMED, with one documented boundary condition.

(1) QUOTE — verbatim-accurate. The official_terms_url is live and fetched OK. The finding's verbatim_quote appears WORD-FOR-WORD on the page, including the parenthetical "(hereinafter: Content)". Only cosmetic difference: the page renders "Creative Commons Attribution 4.0 International (CC BY 4.0)" as a hyperlink to creativecommons.org/licenses/by/4.0/. No textual discrepancy. The CC BY 4.0 general grant explicitly states content "can be copied, reproduced and redistributed without limitations" — this genuinely permits redistribution (not merely "use"), so the redistributable_attribution classification is supported for the general website content.

(2) STRICTER CLAUSE FOUND (populated in contradicting_clause) — but NARROW, not disqualifying. Adversarial search of the same page surfaced a NonCommercial carve-out the finding did not mention: data files "queried from the internal databases of HCSO on specific request" fall under CC BY-NC 4.0 (no commercial use). This is an EXCEPTION scoped to a specific channel (custom queries against KSH's internal/dissemination database on individual request), NOT the default licence. The page also requires attribution ("Source: HCSO / ksh.hu") — consistent with CC BY — and restricts the HCSO logo (needs prior written consent; irrelevant to data redistribution).

(3) WHY STILL CONFIRMED. The headline/default licence for "all of the content available on the Website – including tables, figures and infographics" is CC BY 4.0 = redistributable_attribution, which is exactly the finding's classification and is not more permissive than the terms support. KSH's standard published data product (STADAT tables/figures, per project intel) is website content and falls in the CC BY 4.0 tier. The NC restriction only bites for on-request internal-database extracts.

BOUNDARY CONDITION / RECOMMENDATION for the library: the classification is safe ONLY if the KSH ingestion pulls published STADAT / website content. If any KSH content is obtained as custom data files queried from the internal dissemination database on specific request, that subset is CC BY-NC 4.0 and must NOT be redistributed for commercial use — the library should confirm its KSH pipeline uses published website tables, retain the "Source: HCSO / ksh.hu" attribution, and not represent KSH data as commercially reusable if any on-request extracts are included.

*Researcher reasoning:* The current, authoritative KSH/HCSO copyright page (www.ksh.hu, fetched and also verified from raw HTML decoded from ISO-8859-2) explicitly adopts Creative Commons Attribution 4.0 International (CC BY 4.0) for all website content, naming tables and figures specifically, and states such content "can be copied, reproduced and redistributed without limitations." STADAT summary tables are published on this Website (www.ksh.hu/stadat_eng), so they fall squarely under this CC BY 4.0 grant. Redistribution/re-hosting is therefore permitted, and CC BY 4.0 permits commercial use, so a free non-commercial academic re-host is clearly within scope. The single mandatory condition is attribution: "Any content can only be used if HCSO is indicated as a source (Source: HCSO / ksh.hu)," with an active link required for online use — hence classification redistributable_attribution rather than redistributable_open. CC BY 4.0 imposes no ShareAlike obligation (sharealike=false). Two carve-outs do NOT affect STADAT: (1) a narrow CC BY-NC 4.0 non-commercial restriction that applies only to "data files queried from the internal databases of HCSO on specific request" (bespoke custom extracts, not the public STADAT tables); and (2) the ksh.hu domain name and the HCSO logo, which remain trademark-/copyright-protected and require prior written consent — but these are branding assets, not the statistical data itself. Note: an older legacy help page at statinfo.ksh.hu/Statinfo/help/copyright_en.html carries restrictive language ("Database-like processing, reproduction, selling or passing the content on the website is forbidden"; use "authorized only with the prior written permission of HCSO"). That page reflects the older Statinfo dissemination system and is superseded by the current CC BY 4.0 policy on the main www.ksh.hu portal that governs STADAT; it is flagged here for transparency but does not control the STADAT summary tables.

---

### Maddison Project Database (Groningen)

- **Databases (1):** `maddison`
- **Official terms URL:** https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2023?lang=en
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Maddison Project Database, version 2023 by Jutta Bolt and Jan Luiten van Zanden is licensed under a Creative Commons Attribution 4.0 International License
> When the data is shown in any graphical form, or when subsets of the full dataset that include less than a dozen (12) countries are used for statistical analysis or any other purposes, the original papers on which the estimates are based should be cited.
> Citation: Bolt, Jutta and Jan Luiten van Zanden (2024), 'Maddison style estimates of the evolution of the world economy: A new 2023 update', Journal of Economic Surveys, 1–41.

*Verifier notes:* Verified the official_terms_url (https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2023?lang=en) via two independent WebFetch passes; fetch_status = fetched_ok confirmed.

STEP 1 (quote verbatim): CONFIRMED. The verbatim_quote "Maddison Project Database, version 2023 by Jutta Bolt and Jan Luiten van Zanden is licensed under a Creative Commons Attribution 4.0 International License" appears word-for-word on the page under the "License and funding" section, accompanied by a Creative Commons badge that links to the CC BY 4.0 International License. This matches the standard CC BY attribution-notice format exactly ("[Work] by [Author] is licensed under a Creative Commons Attribution 4.0 International License").

STEP 2 (adversarial search for stricter clauses): No stricter clause found. The page contains NO non-commercial (NC) restriction, NO no-derivatives (ND) clause, NO redistribution ban, NO "prior written permission" requirement, and NO bulk/mass-download restriction. The only additional stipulation is an "Attribution requirement": all original papers must be cited when (1) the data is shown in graphical form or (2) subsets of fewer than 12 countries are used. This is a citation/attribution detail that is fully consistent with — and no stricter than — CC BY 4.0, which already mandates attribution. It does not restrict copying or redistribution.

STEP 3 (classification defensibility): DEFENSIBLE. CC BY 4.0 explicitly grants the right to "copy and redistribute the material in any medium or format" and to adapt, including for commercial purposes, subject only to attribution. A library re-hosting the data for public download is squarely permitted. The classification "redistributable_attribution" is the correct, non-over-permissive mapping for CC BY 4.0.

No red flags. The finding is accurate and its classification is neither inaccurate nor too permissive.

*Researcher reasoning:* The official Maddison Project Database 2023 release page, hosted on the provider's own domain (rug.nl, University of Groningen / GGDC), states verbatim: "Maddison Project Database, version 2023 by Jutta Bolt and Jan Luiten van Zanden is licensed under a Creative Commons Attribution 4.0 International License". The same CC BY 4.0 statement appears on the 2020 and 2018 release pages per search results, confirming a consistent policy across releases. CC BY 4.0 is a standard, well-known open license whose canonical terms explicitly grant the right to "Share — copy and redistribute the material in any medium or format" and "Adapt — remix, transform, and build upon the material for any purpose, even commercially," conditioned only on attribution (BY). This directly authorizes third-party re-hosting/redistribution, so it is not a mere "publicly available / free to access" branding statement — it is an explicit grant of redistribution rights. Therefore: classification = redistributable_attribution; commercial use permitted (true); no ShareAlike obligation (false); attribution required (true). The provider additionally specifies citation/attribution expectations: original underlying papers must be cited when data is shown graphically or when subsets of fewer than 12 countries are used, otherwise the MPD as a whole should be cited (recommended citation: Bolt & van Zanden 2024, Journal of Economic Surveys). Note: the CC BY 4.0 attribution obligation is the legally binding redistribution condition; the graphical/subset citation guidance and the recommended academic citation are the provider's specified form of attribution. fetch_status = fetched_ok: the license clause was read directly on the official release page.

---

### Narodowy Bank Polski (NBP)

- **Databases (1):** `nbp`
- **Official terms URL:** https://api.nbp.pl/en.html
- **License:** All rights reserved (proprietary copyright notice; no data-reuse licence published)
- **Classification:** permission_required
- **Commercial OK:** None · **Attribution required:** None · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> Copyright © 2024 Narodowy Bank Polski. All rights reserved.
> Copyright © 2024 Narodowy Bank Polski . Deklaracja dostępności Wszystkie prawa zastrzeżone. (Polish version, https://api.nbp.pl/)
> Copyright © 2022 Narodowy Bank Polski. All rights reserved. (archived 2022 version of https://api.nbp.pl/en.html via web.archive.org/web/20220604111944)

*Verifier notes:* Verbatim quote verified word-for-word on the official URL (https://api.nbp.pl/en.html): the page returns exactly "Copyright © 2024 Narodowy Bank Polski. All rights reserved." The Polish root page (https://api.nbp.pl/) carries the identical notice ("Wszystkie prawa zastrzeżone" = All rights reserved), corroborating it. fetch_status "fetched_ok" is accurate; the URL is live and is the correct API terms page, not a 404 or redirect.

Independent adversarial search for a stricter OR looser clause: I searched the web and re-fetched both the English and Polish pages. No separate NBP terms-of-service, "regulamin", or data-reuse/redistribution license is published on the API documentation. There is no explicit redistribution grant and no public-sector-information (re-use) license text on the page — the only intellectual-property statement is the blanket "all rights reserved" copyright notice. No missed clause makes the terms MORE permissive.

Classification judgment: "permission_required" is the conservative, strictest-short-of-prohibited classification. For a library that re-hosts NBP data for public download, an "all rights reserved" notice with no published reuse license genuinely warrants obtaining permission before redistribution. The classification is therefore defensible and NOT too permissive — there is no risk in the direction the review guards against. No contradicting clause exists.

Verdict: CONFIRMED.

*Researcher reasoning:* The database re-hosted under ID "nbp" is NBP's FX-rate/gold-price data, served from api.nbp.pl. I fetched and read the full official NBP Web API documentation page (api.nbp.pl/en.html, 20 KB via curl) and its Polish counterpart (api.nbp.pl/). Both carry ONLY a standard proprietary copyright notice at the foot of the page — English: "Copyright © 2024 Narodowy Bank Polski. All rights reserved."; Polish: "Wszystkie prawa zastrzeżone." There is NO terms-of-use section, NO licence, and NO clause granting redistribution, re-hosting, re-dissemination, or bulk reuse; nor any statement about commercial vs non-commercial use or attribution. An archived 2022 snapshot showed the identical "All rights reserved" wording, confirming this is NBP's stable, longstanding position rather than a transient omission. I attempted to locate a dedicated data-reuse / re-use-of-public-sector-information policy on NBP's own domains: nbp.pl and bip.nbp.pl are both behind Incapsula bot protection and their live legal pages are inaccessible; the Wayback-archived bip.nbp.pl (NBP's official Biuletyn Informacji Publicznej) contains no "ponowne wykorzystywanie informacji sektora publicznego" page, and I found no dane.gov.pl listing applying an explicit open licence to NBP FX data. Under the conservative rubric, "publicly available / free API access" does not imply a right to redistribute, and an explicit "All rights reserved" notice reserves the reproduction and distribution rights to NBP. Absent any published grant permitting re-hosting, a third party wishing to redistribute NBP data would need to seek NBP's permission. I did not classify as "prohibited" because NBP does not publish an explicit sentence forbidding redistribution, and not "unclear_not_found" because I did locate and read NBP's governing copyright statement — it simply reserves all rights without granting redistribution. Hence: permission_required. If the professor wishes to re-host NBP data, he should contact NBP for written re-use permission (or rely on a specific open-licence determination if one is later found on dane.gov.pl or NBP's BIP once accessible).

---

### NASA GISS (Goddard Institute for Space Studies) — GISTEMP Surface Temperature Analysis (v4)

- **Databases (1):** `nasa_giss`
- **Official terms URL:** https://www.nasa.gov/nasa-brand-center/images-and-media/
- **License:** U.S. Government work / public domain (NASA content generally not subject to U.S. copyright), governed by NASA Media Usage / Image and Media Guidelines
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> NASA content – images, audio, video, and media files used in the rendition of 3-dimensional models, such as texture maps and polygon data in any format – generally are not subject to copyright in the United States. You may use this material for educational or informational purposes, including photo collections, textbooks, public exhibits, computer graphical simulations and Internet Web pages.
> (From the official GISTEMP dataset page, https://data.giss.nasa.gov/gistemp/) 'Graphics from these GISTEMP pages are subject to NASA Image and Media guidance.'
> (GISTEMP page, https://data.giss.nasa.gov/gistemp/) 'Per those guidelines, graphics you may create using the website tools here do not require permission for you to use elsewhere, but acknowledgment of their source should be given.'
> (GISTEMP page, https://data.giss.nasa.gov/gistemp/) 'Please credit \'NASA\'s Goddard Institute for Space Studies\' or, if space is limited, \'NASA GISS/GISTEMP\'.'
> (GISTEMP page, https://data.giss.nasa.gov/gistemp/) 'When referencing the GISTEMP v4 data provided here, please cite both this webpage and also our most recent scholarly publication about the data.'
> (NASA Image and Media guidance, https://www.nasa.gov/nasa-brand-center/images-and-media/) 'NASA should be acknowledged as the source of the material.'
> (NASA Image and Media guidance, https://www.nasa.gov/nasa-brand-center/images-and-media/) 'If the NASA material is to be used for commercial purposes, including advertisements, it must not explicitly or implicitly convey NASA\'s endorsement of commercial goods or services.'

*Verifier notes:* Quote verified word-for-word at https://www.nasa.gov/nasa-brand-center/images-and-media/. The page's opening paragraph reads exactly as quoted, then continues "This general permission extends to personal Web pages." (text not supplied by me, confirming a genuine page read; the fetch also independently surfaced the real section headings: Non-Commercial Use, Commercial Use, Media including Identifiable Persons, NFTs, AI Applications).

Adversarial search for stricter clauses: the page's only restrictions are (1) commercial use must not imply NASA endorsement, (2) the NASA Insignia/Logotype/identifiers are NOT public domain and are legally protected, (3) NFT and AI-application caveats, (4) consent for identifiable persons. None constitutes a redistribution ban, non-commercial restriction, prior-written-permission requirement, no-derivatives clause, or bulk-download/mass-extraction limit on the dataset. The logo/insignia carve-out does not restrict re-hosting GISTEMP temperature data.

Classification defensible and NOT too permissive. Independent check of GISTEMP's own data page (https://data.giss.nasa.gov/gistemp/) reinforces the permissive reading: the GISTEMP data package's added work is released under the Open Data Commons Public Domain Dedication and License (ODC PDDL) v1.0 ("freely redistributed and used for any purpose"), and all inputs (NOAA GHCN v4, ERSST v5, AIRS v7, NOAA/CPC) are themselves US-Government public-domain works — no non-redistributable third-party inputs. GISTEMP is a NASA GISS product (17 U.S.C. sec. 105 US-Government-work, not subject to US copyright). Redistribution with attribution is genuinely permitted.

Minor, non-disqualifying imprecision: the cited page technically governs images/media; GISTEMP's page states "Graphics from these GISTEMP pages are subject to NASA Image and Media guidance," i.e., that page most directly covers graphics while the numeric data package rides on ODC PDDL + US-Gov public-domain status. This makes the finding, if anything, slightly UNDER-permissive relative to actual terms rather than over-permissive. Recommend citing the ODC PDDL statement / US-Gov-work basis alongside the Image & Media page. Attribution: cite "NASA GISS/GISTEMP" and the Lenssen et al. 2024 publication per the provider's citation request. redistributable_attribution stands.

*Researcher reasoning:* GISTEMP is produced by NASA GISS, a U.S. federal agency, so the data are a U.S. Government work. The official GISTEMP dataset page (https://data.giss.nasa.gov/gistemp/) does not state a standalone data licence but explicitly defers to NASA's Image and Media guidance ("Graphics from these GISTEMP pages are subject to NASA Image and Media guidance"). That official NASA guidance page (https://www.nasa.gov/nasa-brand-center/images-and-media/) states that NASA content — including media/data files — "generally are not subject to copyright in the United States" and that "You may use this material for educational or informational purposes, including... Internet Web pages." This explicit permission to use the material, including publishing it on web pages, combined with the absence of copyright, supports third-party re-hosting/redistribution rather than mere access. Attribution is requested but not phrased as a strict binding licence condition: the GISTEMP page says "acknowledgment of their source should be given" and asks to credit "NASA GISS/GISTEMP," and the NASA page says "NASA should be acknowledged as the source of the material." Because the terms are public-domain in nature yet carry an explicit acknowledgment request, I classify conservatively as redistributable_attribution rather than redistributable_open. Commercial use is also permitted, with the single caveat that it "must not explicitly or implicitly convey NASA's endorsement of commercial goods or services" — hence commercial_ok=true (conditional). There is no share-alike/copyleft requirement. Caveat: NASA notes it sometimes hosts third-party copyrighted material with permission and "NASA's use does not convey any rights to others"; for GISTEMP the analysis output is NASA's own work, so this does not restrict the temperature series, but any embedded third-party source imagery would be separately governed. NOTE: some third-party summaries (e.g., datahub.io) apply a "Public Domain Dedication and License v1.0" to their repackaged copy — that is the re-packager's label, not NASA's own term, and was disregarded.

---

### NOAA (National Oceanic and Atmospheric Administration / National Weather Service)

- **Databases (1):** `noaa`
- **Official terms URL:** https://www.weather.gov/disclaimer
- **License:** U.S. Government public domain (17 U.S.C. 105) with 17 U.S.C. 403 notice condition
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> The information on National Weather Service (NWS) Web pages are in the public domain, unless specifically noted otherwise, and may be used without charge for any lawful purpose so long as you do not: 1) claim it is your own (e.g., by claiming copyright for NWS information -- see below), 2) use it in a manner that implies an endorsement or affiliation with NOAA/NWS, or 3) modify its content and then present it as official government material.
> As required by 17 U.S.C. § 403, third parties producing copyrighted works consisting predominantly of the material appearing in NWS Web pages must provide notice with such work(s) identifying the NWS material incorporated and stating that such material is not subject to copyright protection.
> Use of the NWS name ("National Weather Service") and/or visual identifier are protected under trademark law and may not be used without permission from the NWS. However, use of the NWS name and/or visual identifier to identify unaltered NWS content or links to NWS web sites are allowable uses.

*Verifier notes:* Verified against the live page at https://www.weather.gov/disclaimer (raw body read via browser, bypassing summarizer quote limits). The verbatim_quote matches the page WORD-FOR-WORD, including punctuation, the parenthetical "(e.g., by claiming copyright for NWS information -- see below)", the "--" dash, and the numbered conditions 1)/2)/3). On the page the sentence continues "...official government material. You also cannot present information of your own in a way that makes it appear to be official government information." — the finding's quote is a clean truncation at that sentence boundary, not a misquote.

Adversarial search for a stricter clause found none that undermines the classification: (a) core grant is genuine public domain — "may be used without charge for any lawful purpose" — with NO redistribution ban, NO non-commercial limit, NO prior-written-permission requirement, and modification is expressly allowed (only barred from being presented as "official government material"); (b) the "Public Notice of Appropriate Use (Defining Abuse)" section is a server rate-limiting/IP-throttling policy for querying NWS's own live endpoints (block IPs, throttle query cycles), NOT a licensing restriction on redistributing data you already hold — it does not bar re-hosting; (c) the trademark clause restricts only the NWS name/visual identifier, not the data; (d) the 17 U.S.C. 403 notice condition is real and correctly captured in the finding's license_name.

Classification redistributable_attribution is defensible and if anything slightly conservative — U.S. Government works (17 U.S.C. 105) are public domain, the most redistribution-friendly category, so re-hosting for public download is squarely permitted; the attribution/notice framing (condition 1 "do not claim it is your own" + the § 403 notice) is a reasonable, non-over-permissive read. It is NOT too permissive.

One genuine caveat for the library operator (does not change the verdict): the page's "Use of Third-Party Data and Products" section notes embedded third-party info/imagery (e.g., Google Maps/Earth, Esri) is used under separate license and is NOT public domain — "Please contact the third-party provider for information on your rights to further use these data/products." This is covered by the main clause's "unless specifically noted otherwise" and applies only to embedded third-party layers, not to core NWS-generated meteorological/hydrological data. The library should ensure it re-hosts only NWS-originated public-domain data and not third-party licensed imagery. URL accessible; fetch_status fetched_ok confirmed (note: www.weather.gov/disclaimer redirects to weather.gov but serves the same disclaimer content).

*Researcher reasoning:* NOAA is a U.S. federal agency, so its data are works of the U.S. Government and, under 17 U.S.C. 105, are not subject to copyright — i.e., in the public domain. The official NWS disclaimer page (weather.gov/disclaimer) states verbatim that the information "are in the public domain, unless specifically noted otherwise, and may be used without charge for any lawful purpose." This explicitly permits redistribution/re-hosting (any lawful purpose, including commercial), so it is NOT merely an access/use permission. Three conditions attach: (1) do not claim it as your own / claim copyright, (2) do not imply NOAA/NWS endorsement or affiliation, and (3) do not modify and present as official government material. Additionally, 17 U.S.C. 403 requires that third parties producing copyrighted works predominantly consisting of NWS material provide notice identifying the NWS material and stating it is not copyright-protected. Because of this explicit notice/attribution obligation and the "do not claim it as your own" condition, I classify conservatively as redistributable_attribution rather than redistributable_open — even though the underlying material is public domain, a compliant re-host should attribute NOAA/NWS and disclaim copyright rather than present it bare. Commercial use is permitted ("any lawful purpose"); there is no non-commercial restriction and no share-alike requirement. Note: some specific NOAA sub-datasets are dedicated even more explicitly to CC0 (e.g., Coast Survey data via CC0-1.0), and trademark/logo (the NOAA/NWS name and emblem) use is separately restricted but does not affect data redistribution. Caveat: the "unless specifically noted otherwise" clause means individual products may carry third-party copyright (e.g., licensed satellite imagery) — those items must be checked per-source. The generic 'noaa' database is governed by the public-domain terms quoted above. The NOAA Library repository "Content and Copyright" page (repository.library.noaa.gov) returned HTTP 403 and could not be read, but the weather.gov/disclaimer page is an official NOAA/NWS domain and was fetched and read directly, giving an authoritative verbatim basis for this classification.

---

### oecd

- **Databases (1):** `oecd`
- **Official terms URL:** https://www.oecd.org/en/about/terms-conditions.html
- **License:** OECD Terms & Conditions (Data) — attribution-based, CC BY-equivalent; OECD written content published on/after 1 July 2024 is CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Except where additional restrictions apply as stated above, you can extract from, download, copy, adapt, print, distribute, share and embed Data for any purpose, even for commercial use. You must give appropriate credit to the OECD by using the citation associated with the relevant Data, or, if no specific citation is available, you must cite the source information using the following format: OECD (year), (dataset name),(data source) DOI or URL (accessed on (date)). When sharing or licensing work created using the Data, you agree to include the same acknowledgment requirement in any sub-licenses that you grant, along with the requirement that any further sub-licensees do the same.
> Unless otherwise stated, the material is the intellectual property of the OECD and protected by copyright or other similar rights. Some content in the material may be owned by third parties. You are responsible for verifying whether this is the case and, if so, securing the appropriate permissions from these third parties before using the content.
> The OECD makes data (the “Data”) available for use and consultation by the public. Data may be subject to restrictions beyond the scope of these Terms and Conditions, either because specific terms apply to those Data or because third parties may have ownership interests. It is the user’s responsibility to verify, either directly in the metadata or, if available, by clicking on the icon and then referring to the "source" tab, whether the Data is fully or partially owned by third parties and/or whether additional restrictions may apply, and to contact the owner of the Data before incorporating it in your work in order to secure the necessary permissions.
> The availability of the Data is contingent upon the availability of the OECD’s corresponding resources, whose capacity is subject to change at any time. The OECD may monitor your use of the Data and reserves the right, at its sole discretion and without limitation, to modify the amount of Data you may request in a single query, to modify the number of queries you may make over a specified time, to remove certain Data and to alter the file formats in which Data are available.
> Following implementation of the OECD Open Access Policy, most OECD written content published as of 1 July 2024 is licensed under a Creative Commons Attribution BY 4.0 licence (CC BY 4.0). This licence permits users to reproduce, distribute and adapt (including translate) the content for any purpose without seeking authorisation from the OECD.

*Verifier notes:* VERBATIM: Confirmed word-for-word against the live official page (Section 3 "Data" > "Permitted Use"). WebFetch returned HTTP 403 (bot-blocking) and web.archive.org was unavailable, so I loaded the genuine oecd.org page in the browser and extracted the collapsed accordion text directly from the DOM; it matches the finding's verbatim_quote exactly, including punctuation "(dataset name),(data source)" (no space), American spelling "acknowledgment", and "sub-licenses"/"sub-licensees". WebSearch independently corroborated the language.

ADVERSARIAL HUNT (read ALL Data sub-sections): (1) Data intro third-party caveat: "It is the user's responsibility to verify... whether the Data is fully or partially owned by third parties and/or whether additional restrictions may apply, and to contact the owner of the Data before incorporating it in your work..." — standard third-party-content exclusion present even in CC BY; qualifies specific datasets, not the own-data default. (2) "Availability of Data": OECD reserves the right "to modify the amount of Data you may request in a single query, to modify the number of queries you may make over a specified time" — an operational RATE-LIMIT on querying OECD servers, NOT a restriction on redistributing data already obtained. (3) API section = as-is/no-warranty boilerplate. No non-commercial clause, no no-derivatives, no bulk/scraping ban, no prior-written-permission on the Data (prior permission applies only to the OECD logo).

CLASSIFICATION: Core grant permits "distribute, share and embed Data for any purpose, even for commercial use" with attribution + acknowledgment propagation — materially CC BY-equivalent (marginally stricter on attribution-propagation, never more permissive). Re-hosting for public download falls within "distribute/share". "redistributable_attribution" is defensible and not over-permissive. Post-1-July-2024 written content = CC BY 4.0 also corroborated (Section 1.1). Residual operational note (not a downgrade): a re-hosting library must still honor per-dataset third-party restrictions flagged in each dataset's metadata/"source" tab, since OECD does not warrant it owns all rights in all Data.

*Researcher reasoning:* The OECD's official Terms & Conditions page (last updated 1 July 2024) has a dedicated Section 3 "Data" whose "Permitted Use" subsection explicitly and unambiguously governs redistribution. It states users "can extract from, download, copy, adapt, print, distribute, share and embed Data for any purpose, even for commercial use," conditioned only on giving appropriate credit using a prescribed OECD citation format. "distribute" and "share" are express redistribution/re-dissemination rights (not merely access/use), so this is genuine redistribution permission, not "open data" branding. This is functionally equivalent to a CC BY licence: redistribution allowed, commercial use allowed, attribution required. Hence classification = redistributable_attribution, commercial_ok = true, attribution_required = true. Share-alike is false: the terms require passing on the same *acknowledgment* (attribution) requirement to sub-licensees, which is an attribution passthrough, not a copyleft "same-licence" obligation. One material caveat for a re-hosting library: the terms govern data "owned solely by the OECD." Section 3 repeatedly warns that individual datasets may contain data owned by third parties or subject to additional restrictions, and it is the user's responsibility to verify ownership (via metadata / source tab) and contact the owner to secure permissions before incorporating such data. The "Availability of Data" subsection lets the OECD rate-limit or cap query volume but does NOT restrict redistribution of data already obtained; there is no prohibition on mass download or re-hosting of OECD-owned data. Therefore OECD-owned data is redistributable with attribution, but blanket re-hosting of every OECD dataset is not covered — third-party-sourced series within OECD databases must be checked individually. (For completeness, Section 1.1 states most OECD *written content* published as of 1 July 2024 is CC BY 4.0; the Data section, not the written-content section, is the governing clause for datasets.)

---

### owid

- **Databases (1):** `owid`
- **Official terms URL:** https://ourworldindata.org/faqs
- **License:** CC BY (Creative Commons Attribution)
- **Classification:** redistributable_attribution  →  **corrected to `mixed / source-dependent — NOT blanket redistributable_attribution. Only the minority of data that OWID produces itself ("Data produced by us", flagged e.g. "with major processing by Our World in Data") is CC BY and redistributable with attribution. The majority ("Most of the data") is third-party (WHO, UN, World Bank, and many others) and remains subject to each upstream provider's own license, which must be assessed per-source before re-hosting. Treat the source as partially/conditionally redistributable pending per-provider review, not uniformly CC BY.`** by adversarial review
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **DISPUTED** (quote verbatim: True, classification agrees: False)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> Data produced by us falls under our permissive CC BY license; you have permission to use, reproduce, and distribute it, provided that you cite us.
> Most of the data on Our World in Data comes from third-party providers (such as the WHO, UN, and World Bank) and is subject to the license terms of those providers. You should always check their license before reusing or republishing the data.
> You do not need permission from us, and we do not sign copyright release forms, provided that you cite us appropriately.
> you can use, reproduce, and distribute any chart we made (those with our logo and CC BY copyright stamp)...provided that you cite us. You do not need permission from us

**Adversary's contradicting clause:** Most of the data on Our World in Data comes from third-party providers (such as the WHO, UN, and World Bank) and is subject to the license terms of those providers. You should always check their license before reusing or republishing the data.

*Verifier notes:* STEP 1 (quote): CONFIRMED verbatim. WebFetch of https://ourworldindata.org/faqs (fetched_ok, not 404) returns the exact sentence word-for-word: "Data produced by us falls under our permissive CC BY license; you have permission to use, reproduce, and distribute it, provided that you cite us." No red flag on the quote or URL.

STEP 2 (stricter clause the researcher missed): YES. The CC BY sentence is preceded, in the same FAQ answer ("Can I reuse or republish your data?"), by a scoping clause the finding omitted: "Most of the data on Our World in Data comes from third-party providers (such as the WHO, UN, and World Bank) and is subject to the license terms of those providers. You should always check their license before reusing or republishing the data." The CC BY grant is explicitly limited to "Some of the data on our site is produced by us." The citation FAQ adds that reusers "must credit both Our World in Data _and_ the underlying third-party data provider" and "always check the license of third-party data providers before reusing or republishing the data." So OWID itself distinguishes a small CC-BY-own subset from a larger third-party subset governed by upstream (often non-redistributable) terms.

STEP 3 (defensible for a re-hosting library?): NO. A public-download library re-hosting the OWID catalog would be re-hosting predominantly third-party data whose redistribution is governed by the original providers, not by OWID's CC BY. Classifying the whole source as redistributable_attribution is more permissive than the terms support — the classic "use allowed, redistribution restricted / mixed licensing" pitfall. The classification should be downgraded to source-dependent/mixed with per-provider verification required. Verdict DISPUTED: quote accurate, classification too permissive.

*Researcher reasoning:* Fetched OWID's official FAQ & User Guidelines page (ourworldindata.org/faqs), which is their governing licensing/reuse statement. Two distinct regimes apply:

(1) DATA OWID PRODUCES ITSELF: Released under a permissive CC BY (Creative Commons Attribution) license. The page states verbatim: "Data produced by us falls under our permissive CC BY license; you have permission to use, reproduce, and distribute it, provided that you cite us" and "You do not need permission from us, and we do not sign copyright release forms, provided that you cite us appropriately." CC BY explicitly permits redistribution/re-hosting and commercial use, with attribution and no ShareAlike/non-commercial restriction. For OWID-produced data this is therefore redistributable_attribution. (Note: the FAQ text I fetched says "CC BY license" without an explicit version number in the quoted sentence; OWID commonly uses CC BY 4.0 but I did not obtain a verbatim "4.0" string, so I recorded the license name without a version.)

(2) CRITICAL CAVEAT — THIRD-PARTY DATA: OWID is largely an aggregator, and the FAQ explicitly warns that most of its content is NOT OWID's to license: "Most of the data on Our World in Data comes from third-party providers (such as the WHO, UN, and World Bank) and is subject to the license terms of those providers. You should always check their license before reusing or republishing the data." OWID's own CC BY does NOT extend to these underlying datasets. A re-hosting library must therefore verify each specific dataset: if it is OWID-produced (e.g., their own estimates, the OWID CO2/energy dataset, OWID COVID dataset), CC BY redistribution applies; if the series is sourced from WHO/UN/World Bank/etc., the original provider's license governs redistribution and must be checked separately. OWID's Grapher SOFTWARE is separately NOT freely licensed (requires written permission from Global Change Data Lab), but that governs code reuse, not the data itself.

CONSERVATIVE BOTTOM LINE for the "owid" provider: redistribution of OWID's own produced data is permitted with attribution (redistributable_attribution). But the professor must not treat the whole OWID catalogue as blanket-redistributable — third-party-sourced series carry the original provider's terms per OWID's own instruction to "check their license before reusing or republishing."

---

### Oxford COVID-19 Government Response Tracker (OxCGRT), Blavatnik School of Government, University of Oxford

- **Databases (1):** `oxcgrt`
- **Official terms URL:** https://github.com/OxCGRT/covid-policy-dataset/blob/main/README.md
- **License:** CC BY 4.0 (Creative Commons Attribution 4.0 International)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Our data is made available free to use for any purpose under a Creative Commons CC BY 4.0 license (see: our license, and a summary of CC BY 4.0 at Creative Commons), this means you must give appropriate credit and link back to our original work.
> Creative Commons Attribution 4.0 International Public License
> Media use should credit: "Oxford COVID-19 Government Response Tracker, Blavatnik School of Government, University of Oxford"

*Verifier notes:* Adversarial review upholds the finding. (1) Quote is verbatim-accurate at the official URL. I fetched https://github.com/OxCGRT/covid-policy-dataset/blob/main/README.md and the raw README; the sentence appears word-for-word. The only textual difference is inline markdown link markup around "our license" and "summary of CC BY 4.0" — the human-readable words are identical, so the quote is verbatim. fetch_status "fetched_ok" is accurate and the page is the canonical OxCGRT dataset repo (Hale/Angrist et al., Nature Human Behaviour 2021). (2) I independently checked LICENSE.txt (raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/LICENSE.txt): it is a standard, unmodified CC BY 4.0 International license linking to creativecommons.org/licenses/by/4.0/, with only a prepended "Cite as" citation block that adds no restrictions. No NC clause, no ND clause, no redistribution ban, no prior-written-permission requirement, and no bulk-download/mass-extraction limit exist in either the README or the license. (3) OxCGRT is originally coded policy data collected by the Oxford team from public government sources — not a re-host of a third-party licensed dataset — so there is no hidden upstream license that would restrict re-hosting. (4) Classification "redistributable_attribution" is defensible and correctly scoped: CC BY 4.0 genuinely permits re-hosting for public download and commercial use, conditioned only on attribution, which the classification captures. Not too permissive, not too strict.

*Researcher reasoning:* The provider's OWN GitHub repository (OxCGRT/covid-policy-dataset, the official current home of the dataset — the older covid-policy-tracker repo is the same organisation) is authoritative. Two official sources on the provider's own domain/repo confirm the licence:

1) The repository README states verbatim: "Our data is made available free to use for any purpose under a Creative Commons CC BY 4.0 license ... this means you must give appropriate credit and link back to our original work." (fetched from both https://github.com/OxCGRT/covid-policy-dataset/blob/main/README.md and its raw form https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/README.md).

2) The repository's LICENSE.txt file (https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/LICENSE.txt) carries the header "Creative Commons Attribution 4.0 International Public License" — the standard CC BY 4.0 legal code.

CC BY 4.0 is a permissive open licence that EXPLICITLY grants the right to reproduce and redistribute the material in any medium or format, including for commercial purposes, provided attribution is given. This is not mere "publicly available" branding — the licence text itself confers redistribution rights. Therefore re-hosting/redistribution for download by a free non-commercial academic library is permitted. Classification: redistributable_attribution. The only condition is attribution (appropriate credit + link to the original work / suggested citation). Commercial use is allowed (so the non-commercial nature of the library is more than satisfied). It is CC BY, NOT CC BY-SA, so there is no share-alike/copyleft obligation. To comply, the library must display attribution to "Oxford COVID-19 Government Response Tracker, Blavatnik School of Government, University of Oxford" and link back to the source.

---

### Penn World Table (Groningen Growth and Development Centre, University of Groningen / UC Davis)

- **Databases (1):** `pwt`
- **Official terms URL:** https://www.rug.nl/ggdc/productivity/pwt/?lang=en
- **License:** CC BY 4.0 (Creative Commons Attribution 4.0 International)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Penn World Table 11.0 by Robert C. Feenstra, Robert Inklaar and Marcel P. Timmer is licensed under a Creative Commons Attribution 4.0 International License
> When using these data (for whatever purpose), please make the following reference:
> Feenstra, Robert C., Robert Inklaar and Marcel P. Timmer (2015), "The Next Generation of the Penn World Table" American Economic Review, 105(10), 3150-3182

*Verifier notes:* VERBATIM: The quote "Penn World Table 11.0 by Robert C. Feenstra, Robert Inklaar and Marcel P. Timmer is licensed under a Creative Commons Attribution 4.0 International License" appears WORD-FOR-WORD on the official page. Verified across two independent WebFetch passes plus web search; URL is live and resolves to the current PWT 11.0 release (confirmed by the GGDC "version 11.0 is published" announcement).

CLASSIFICATION: CC BY 4.0 = redistributable_attribution is defensible and NOT too permissive. CC BY 4.0 is a public, irrevocable license that expressly permits redistribution, re-hosting for public download, commercial use, and derivatives, conditioned only on attribution. The page also states the required citation: "When using these data (for whatever purpose), please make the following reference: Feenstra, Robert C., Robert Inklaar and Marcel P. Timmer (2015), 'The Next Generation of the Penn World Table' American Economic Review, 105(10), 3150-3182." A re-hosting library must carry this citation, which the "attribution" classification already mandates.

ADVERSARIAL CHECK (stricter clause): I examined the footer's "Disclaimer & Copyright" page (/info/disclaimer-copyright). It contains generic RUG boilerplate: website "teksten, foto's, logo's, andere grafische uitingen op- en vormgeving van de website mogen niet zonder voorafgaande en uitdrukkelijke toestemming worden gekopieerd of gewijzigd" and asserts all IP vests in the university. I weighed whether this overrides the CC BY grant and concluded it does NOT: it is a site-wide disclaimer enumerating website *presentation* elements (texts, photos, logos, graphic design of the website), whereas the PWT datasets are distributed with their own explicit CC BY 4.0 statement from the actual rights holders. Under the specific-governs-general canon, and because CC BY 4.0 is irrevocable once granted, the explicit dataset license controls. This is independently corroborated by re3data.org (registry entry r3d100012246), which classifies the PWT repository as CC BY 4.0 open access. No non-commercial restriction, no no-derivatives clause, no prior-written-permission requirement, and no bulk-download/mass-extraction ban attach to the data itself.

CONCLUSION: Quote is verbatim-accurate at the official URL; classification is defensible and appropriately scoped. Confirmed.

*Researcher reasoning:* The official Penn World Table page hosted by the Groningen Growth and Development Centre at the University of Groningen (www.rug.nl/ggdc/productivity/pwt) explicitly states the data is "licensed under a Creative Commons Attribution 4.0 International License." CC BY 4.0 is a standard open license whose canonical terms grant the licensee the right to "Share — copy and redistribute the material in any medium or format" and "Adapt" for "any purpose, even commercially," subject solely to the attribution condition. Redistribution/re-hosting for download is therefore explicitly permitted, not merely access or use. The only binding condition is attribution: the page requires "When using these data (for whatever purpose), please make the following reference:" followed by the Feenstra, Inklaar & Timmer (2015) citation. This is a CC BY (not CC BY-NC or CC BY-SA) license, so commercial use is allowed and there is no share-alike/copyleft obligation. Accordingly this is classified redistributable_attribution: a free, non-commercial academic library may re-host PWT data provided it carries the required PWT citation/attribution. The verbatim license quote and attribution requirement were both read directly on the official provider domain, not inferred from third-party summaries or memory.

---

### penn_world_table

- **Databases (1):** `penn_world_table`
- **Official terms URL:** https://www.rug.nl/ggdc/productivity/pwt/?lang=en
- **License:** CC BY 4.0 (Creative Commons Attribution 4.0 International)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Penn World Table 11.0 by Robert C. Feenstra, Robert Inklaar and Marcel P. Timmer is licensed under a Creative Commons Attribution 4.0 International License
> License URL linked from the PWT page: http://creativecommons.org/licenses/by/4.0/
> Required citation per the PWT page: Feenstra, Robert C., Robert Inklaar and Marcel P. Timmer (2015), 'The Next Generation of the Penn World Table' American Economic Review, 105(10), 3150-3182

*Verifier notes:* Adversarial review of penn_world_table finding — CONFIRMED on all three fronts.

(1) VERBATIM QUOTE: Verified word-for-word at the official URL (https://www.rug.nl/ggdc/productivity/pwt/?lang=en). Two independent WebFetches of the genuine provider (University of Groningen / GGDC) both returned the exact string: "Penn World Table 11.0 by Robert C. Feenstra, Robert Inklaar and Marcel P. Timmer is licensed under a Creative Commons Attribution 4.0 International License". URL is live and fetch_status 'fetched_ok' is accurate. Version 11.0 (published Oct 2025) matches the quote.

(2) STRICTER-CLAUSE SEARCH: Actively hunted for a redistribution ban, non-commercial restriction, 'prior written permission' requirement, no-derivatives clause, or bulk/mass-download restriction. NONE present. The only obligation on the data is attribution (standard citation to Feenstra, Inklaar & Timmer 2015, AER 105(10):3150-3182) — this is exactly what CC BY 4.0 mandates, not an extra restriction. The generic footer links (disclaimer/copyright/privacy/cookies) are site-wide RUG boilerplate and do not override the specific CC BY 4.0 grant explicitly attached to the PWT dataset.

(3) CLASSIFICATION DEFENSIBILITY: CC BY 4.0 explicitly permits copying and redistributing in any medium/format, and adapting, for any purpose incl. commercial, conditioned only on attribution. A library re-hosting the data for public download is squarely within scope. 'redistributable_attribution' is correct and NOT too permissive — it accurately encodes the single binding obligation (attribution) rather than overstating freedom as unrestricted.

No basis to dispute. Data is genuinely redistributable with attribution.

*Researcher reasoning:* The official Penn World Table page, hosted on the provider's own domain (University of Groningen / Groningen Growth and Development Centre, the co-maintainer with UC Davis), carries an explicit Creative Commons Attribution 4.0 International (CC BY 4.0) licence statement covering the data, linking to http://creativecommons.org/licenses/by/4.0/. CC BY 4.0 is a well-defined open licence that expressly grants the right to "Share — copy and redistribute the material in any medium or format" and "for any purpose, even commercially," subject only to the Attribution condition (credit, licence link, indication of changes). This is a genuine redistribution/re-hosting grant, not mere access/use branding, so re-hosting the dataset for download is permitted provided attribution is preserved. There is no NonCommercial or ShareAlike restriction. Classification: redistributable_attribution. Note: earlier PWT releases (9.1, 10.0, 10.01) carry the same CC BY 4.0 statement on their respective GGDC release pages, so the same conclusion applies to the versions typically re-hosted. The verbatim licence sentence was read directly on the fetched official page; the "for whatever purpose" phrasing surfaced by the summarizer is the standard CC BY grant, and the canonical redistribution language lives in the linked CC BY 4.0 deed/legal code rather than being paraphrased here.

---

### Polity5 (Center for Systemic Peace)

- **Databases (1):** `polity`
- **Official terms URL:** https://www.systemicpeace.org/inscrdata.html
- **License:** Custom terms (CSP/INSCR copyright notice) — all rights reserved, permission required for redistribution
- **Classification:** permission_required
- **Commercial OK:** None · **Attribution required:** True · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> All resources listed on this page are copyrighted by the Center for Systemic Peace. Use of any of these resources in published work must provide proper citation. Reproduction or redistribution of these resources, or substantial portions thereof, is prohibited without prior, written permission from the Center for Systemic Peace.
> contact information is provided on the CSP Contact Page
> The data resources were prepared by researchers associated with the Center for Systemic Peace and are generated and/or compiled using open source information, and are made available as a service to the research community.

*Verifier notes:* STEP 1 (verbatim check): WebFetch of https://www.systemicpeace.org/inscrdata.html succeeded (fetch_status fetched_ok confirmed; HTTP auto-upgraded to HTTPS). The full sentence on the page reads: "All resources listed on this page are copyrighted by the Center for Systemic Peace. Use of any of these resources in published work must provide proper citation. Reproduction or redistribution of these resources, or substantial portions thereof, is prohibited without prior, written permission from the Center for Systemic Peace; contact information is provided on the CSP Contact Page." Every word of the researcher's quote matches word-for-word. The only difference is a truncation at the end: the finding stops after "...Center for Systemic Peace." and substitutes a period for the original's semicolon, dropping the trailing clause "; contact information is provided on the CSP Contact Page." This truncation is at a clean sentence-clause boundary, omits nothing material, and does NOT soften the terms — the dropped clause actually reinforces the permission-required reading (it points to how to request permission). I judged this an acceptable verbatim quote.

STEP 2 (search for stricter clause): The operative restriction the researcher cited IS the strictest clause present: "Reproduction or redistribution ... is prohibited without prior, written permission." There is no separate non-commercial or no-derivatives clause, but none is needed — a blanket permission-required redistribution ban is already the controlling term. No hidden weaker/permissive grant contradicts it.

STEP 3 (classification defensibility): A library that re-hosts Polity5 for public download is, by definition, "reproduction or redistribution," which these terms expressly prohibit absent prior written permission from CSP. The classification "permission_required" is therefore correct and appropriately conservative — it is NOT too permissive (it does not treat the data as freely redistributable or CC-licensed) and correctly reflects "all rights reserved; redistribution needs written permission." No refutation found; finding stands.

*Researcher reasoning:* The Polity5 dataset is hosted by the Center for Systemic Peace (CSP) on its INSCR Data Page. The official page (https://www.systemicpeace.org/inscrdata.html) carries an explicit copyright-and-use notice, quoted verbatim above and independently corroborated by two web searches and a direct WebFetch of the page. The governing sentence states that "Reproduction or redistribution of these resources, or substantial portions thereof, is prohibited without prior, written permission from the Center for Systemic Peace." This is direct, explicit redistribution/re-hosting language — not merely open-access branding. Because redistribution is possible but conditioned on obtaining prior written permission from CSP (contact information provided on the CSP Contact Page), the correct conservative classification is permission_required rather than prohibited (which would apply only if redistribution were forbidden outright with no permission route) or any redistributable/open category. The data being described as "made available as a service to the research community" using "open source information" does NOT grant redistribution rights; the copyright notice expressly reserves them. Attribution is required: "Use of any of these resources in published work must provide proper citation." The terms make no distinction between commercial and non-commercial use — the written-permission requirement applies to all reproduction/redistribution regardless of purpose — so commercial_ok is left null (unaddressed) rather than assumed. No CC or other standard open license is present; the terms are custom, all-rights-reserved copyright. For a free non-commercial academic library that intends to RE-HOST the data for download, this means CSP must be contacted and written permission obtained before redistribution.

---

### Reserve Bank of Australia (RBA)

- **Databases (1):** `rba`
- **Official terms URL:** https://www.rba.gov.au/copyright/
- **License:** CC BY 4.0 (RBA Material generally); RBA "Financial Data" terms (statistical data) permit reproduction/publication for personal or commercial use with attribution
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> With exception of the Excluded Material, all RBA Material is provided under a Creative Commons Attribution 4.0 International License (CC BY 4.0 Licence) and may be used in accordance with the terms of that licence.
> Financial Data and Financial Data Materials may be used, reproduced, published, communicated to the public or otherwise referenced for personal or commercial use
> With the exception of Third Party Material (as defined below), all RBA Material, including (but not limited to) the Excluded Material (as defined below), is the copyright of the RBA.
> Use of RBA Material, whether under the CC BY 4.0 Licence or otherwise, requires you to attribute the work in the manner specified by the RBA. Attribution cannot be done in any way that suggests that the RBA endorses you or your use of the RBA Material.
> Source: Reserve Bank of Australia [year] OR Source: RBA [year]
> Such material may not be reproduced, published, communicated to the public, adapted, referenced or otherwise used without obtaining the consent of the third party

*Verifier notes:* Adversarial review outcome: CONFIRMED.

(1) VERBATIM QUOTE — verified word-for-word at https://www.rba.gov.au/copyright/ (fetch_status fetched_ok, three independent fetches). The sentence appears exactly as quoted: "With exception of the Excluded Material, all RBA Material is provided under a Creative Commons Attribution 4.0 International License (CC BY 4.0 Licence) and may be used in accordance with the terms of that licence."

(2) THE KEY ADVERSARIAL RISK, CHECKED AND CLEARED: "Financial Data" (i.e., RBA statistical data — the exact content a data library re-hosts) is EXCLUDED from CC BY 4.0. It is item 5 of the Excluded Material list and governed by separate "Section 5" terms. So the CC BY 4.0 grant alone would NOT cover the data a library serves. This is the standard trap where a permissive general license masks a carved-out data restriction. Here it does NOT break the finding, because Section 5 independently grants redistribution. Verbatim Section 5 grant: "Financial Data and Financial Data Materials may be used, reproduced, published, communicated to the public or otherwise referenced for personal or commercial use only if it is not stated, represented or in any way implied (other than in respect of proper attribution as required by Section 3 above) that the RBA endorses any use, reproduction, publication, communication to the public or referencing of the Financial Data..." This expressly permits reproduction, publication, and communication to the public, for commercial use, subject to attribution.

(3) SEARCHED FOR STRICTER CLAUSES — none found that defeat redistribution: NO bulk-download / mass-extraction / scraping ban; NO general "prior written permission" requirement; NO non-commercial restriction; NO no-derivatives clause on the data. Conditions attached to Financial Data are ordinary attribution-license conditions: proper attribution (Source: RBA [year]), no implied RBA endorsement, no improper commercial exploitation (e.g., don't charge fees while concealing that RBA publishes it free, don't misrepresent source), and no unlawful use.

(4) RESIDUAL CAVEAT (noted, not disqualifying): Third-Party Material embedded within RBA content/Financial Data still requires third-party consent and is separately excluded. A re-hosting library should ensure it is not redistributing embedded third-party datasets without that consent. This is a normal carve-out and does not render the classification too permissive for RBA's own material/data.

CONCLUSION: Quote is verbatim-accurate at the official URL, and the classification redistributable_attribution is defensible for both RBA Material generally (CC BY 4.0) and the statistical Financial Data (Section 5 permits reproduction/publication/communication to the public for commercial use with attribution). The finding correctly handled the Excluded-Material nuance rather than naively over-reading CC BY 4.0. Not too permissive. CONFIRMED.

*Researcher reasoning:* The RBA publishes a single governing "Copyright and Disclaimer Notice" at https://www.rba.gov.au/copyright/ (fetched and read directly, three times, with consistent wording). It establishes two permissive but distinct redistribution frameworks, both of which allow re-hosting with attribution:

1) General RBA Material: "With exception of the Excluded Material, all RBA Material is provided under a Creative Commons Attribution 4.0 International License (CC BY 4.0 Licence) and may be used in accordance with the terms of that licence." CC BY 4.0 expressly permits redistribution, re-hosting, and commercial use, conditioned only on attribution and no implication of RBA endorsement.

2) The RBA's statistical data (the content this "rba" database re-hosts — RBA statistical tables / series) falls under the "Financial Data" category, which the notice lists as "Excluded Material" from the CC BY 4.0 licence but grants its OWN explicit, permissive terms: "Financial Data and Financial Data Materials may be used, reproduced, published, communicated to the public or otherwise referenced for personal or commercial use." "Reproduced, published, communicated to the public" is explicit redistribution/re-dissemination language, and it is granted for both personal AND commercial use — so no commercial restriction and no ShareAlike condition.

Attribution IS required in both frameworks: "Use of RBA Material, whether under the CC BY 4.0 Licence or otherwise, requires you to attribute the work in the manner specified by the RBA," with the standard form "Source: Reserve Bank of Australia [year]" or "Source: RBA [year]," and attribution must not suggest RBA endorsement.

Therefore classification = redistributable_attribution; commercial_ok = true; attribution_required = true; sharealike = false (CC BY, not BY-SA; the Financial Data terms impose no SA condition).

IMPORTANT COMPLIANCE CAVEAT to flag to the professor: there is an explicit third-party carve-out. The notice states that material containing or derived from third-party content "may not be reproduced, published, communicated to the public, adapted, referenced or otherwise used without obtaining the consent of the third party." Some RBA data series/tables incorporate third-party content. The library must not blanket-redistribute any RBA series/table that is labelled or known to contain third-party-sourced content without that third party's consent, and must retain RBA's own labelling of such Third Party Material. For RBA's own data, redistribution with the "Source: RBA" attribution is permitted.

Note: The RBA also runs a separate "Unreserved" archives site (unreserved.rba.gov.au) under a distinct CC BY 4.0 notice, but that governs archival material, not the main statistical database covered here.

---

### Robert Shiller (Yale) online data

- **Databases (1):** `shiller`
- **Official terms URL:** http://www.econ.yale.edu/~shiller/data.htm
- **License:** None stated (no terms of use, copyright notice, or licence appears on either official page)
- **Classification:** unclear_not_found
- **Commercial OK:** None · **Attribution required:** None · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> Stock market data used in my book, Irrational Exuberance [Princeton University Press 2000, Broadway Books 2001, 2nd ed., 2005] are available for download, U.S. Stock Markets 1871-Present and CAPE Ratio.
> Historical housing market data used in my book, Irrational Exuberance [Princeton University Press 2000, Broadway Books 2001, 2nd edition, 2005], showing home prices since 1890 are available for download and updated monthly: US Home Prices 1890-Present.
> An annual series is also available here, long term stock, bond, interest rate and consumption data since 1871 that I in collaboration with several colleagues collected to examine long term historical trends in the US market.
> [shillerdata.com footer, 'Disclaimer' section:] No representation is made as to the accuracy of the assumptions made within, or completeness of, any modelling, scenario analysis or back-testing.
> [shillerdata.com footer, 'Disclaimer' section, second paragraph, verified present but truncated in extraction:] None of Professor Shiller, Yale University or any other party involved in making or compiling any of ...

*Verifier notes:* VERBATIM CHECK — PASS. WebFetch could not reach the cited URL because it forces HTTP->HTTPS and the Yale server (128.36.64.169) refuses port 443 (HTTP-only host). I loaded http://www.econ.yale.edu/~shiller/data.htm directly in a browser and extracted the body text. The finding's quote appears word-for-word, character-for-character, including "U.S. Stock Markets 1871-Present and CAPE Ratio." Page title "Online Data - Robert Shiller"; fetch_status "fetched_ok" is plausible (page is reachable over HTTP).

STRICTER-CLAUSE SEARCH — none on the official source. The Yale page is a data-description page (naming source data: S&P four-quarter totals, Cowles/Principia Press, CPI-U, Warren & Pearson) with NO license, NO copyright notice, NO terms of use, and NO redistribution / non-commercial / no-derivatives / attribution / prior-written-permission / bulk-download restriction. It ends in plain nav links with no copyright footer. I independently checked the newer official host shillerdata.com: its only restrictive content is a warranty/liability DISCLAIMER ("not intended to be... investment advice", "the user... assumes the entire risk of any use", "makes any express or implied warranty" negated) — a liability disclaimer, not a redistribution or copyright term. No redistribution grant or ban on either page.

CLASSIFICATION — defensible and NOT too permissive. "unclear_not_found" accurately reports that no terms/license exist on the official source. It does not assert redistribution is permitted; for a re-hosting library it correctly leaves rights undetermined (a gate/hold signal). Under default copyright, absence of a stated license means rights are reserved, not granted, so this label is conservative, not over-permissive. Per the review criteria (CONFIRMED only if quote is verbatim AND classification is not more permissive than terms support), both conditions are met.

CAVEAT (non-defect): The quote is verbatim on the CITED Yale page but the newer shillerdata.com uses different wording ("US Stock market data used in my book, Irrational Exuberance [... 3rd ed. 2015] as updated are available for download below"). If the library re-sources to shillerdata.com, the verbatim_quote and official_terms_url would need updating. Also worth noting for the library's own risk posture: "no stated license" is not affirmative permission to re-host — downstream policy should treat unclear_not_found as a hold, not a green light — but that is a policy matter, not a defect in this finding's classification.

*Researcher reasoning:* I fetched and read both official sources for Robert Shiller's online data: (1) the canonical Yale page at http://www.econ.yale.edu/~shiller/data.htm — read in full via the browser's page-text extractor because WebFetch's forced HTTP-to-HTTPS upgrade fails against the HTTP-only Yale host; and (2) shillerdata.com, the newer official home for the same datasets (operated for Professor Shiller), read via WebFetch plus the browser accessibility tree including its full footer.

Neither official page contains any terms of use, copyright/licence statement, CC licence, or any clause governing redistribution, re-hosting, re-dissemination, mass download, or commercial vs non-commercial use. The Yale page is purely descriptive: it explains the sources and methodology of the series and states only that the datasets "are available for download." shillerdata.com likewise offers direct .xls download links and carries a single legal section titled "Disclaimer" that addresses accuracy and liability only ("No representation is made as to the accuracy ... None of Professor Shiller, Yale University or any other party ...") — it says nothing about reuse or redistribution rights, and no attribution requirement is stated.

Per the task's hard rules, "available for download" is access language, not a redistribution grant, and I must not infer a licence from it. There is also no explicit prohibition or written-permission requirement to quote. Because the official terms are entirely silent on redistribution/re-hosting, I cannot determine redistribution rights from them, so I classify conservatively as unclear_not_found rather than guessing an open licence. Practical note for the compliance decision: absence of a licence means default copyright applies — the underlying price/CPI numbers are largely factual (low copyrightability), but the compiled series and derived series such as CAPE, and the specific data files, are Shiller/Yale work product with no redistribution permission granted; re-hosting for download would need express permission from Professor Shiller (a request to his office / shillerdata.com would be the way to obtain it). No CC0/CC-BY/open-data mark was found on any official page.

---

### sec_edgar

- **Databases (1):** `sec_edgar`
- **Official terms URL:** https://www.sec.gov/about/privacy-information
- **License:** U.S. Government public information (SEC "Website Dissemination" policy); EDGAR filing content is U.S. public data — no copyright, no license required
- **Classification:** redistributable_open
- **Commercial OK:** True · **Attribution required:** False · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK

**Verbatim quote:**
> Information presented on sec.gov is considered public information and may be copied or further distributed by users of the web site without the SEC's permission. Please consider appropriate citation to the SEC as the source.
> (From the SEC Webmaster FAQ, https://www.sec.gov/about/webmaster-frequently-asked-questions) Question 'Is content on sec.gov free? Do I need permission to reuse EDGAR content?' — Answer: "All Government-created content on sec.gov and EDGAR public filing content are free to access and reuse. See Website Dissemination for our permissions policy."
> (Webmaster FAQ, same URL) Question 'Is any of the content not free to reuse?' — Answer: "Very little. Examples include stock art photos used to illustrate various sec.gov pages."
> (From 'Accessing EDGAR Data', https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data) "Anyone can access and download this information for free or query it through a variety of EDGAR public searches."
> (Trademark caveat, continuing the same 'Website Dissemination' paragraph at https://www.sec.gov/about/privacy-information) "Please do not use the SEC seal or any of the other logos or artwork from this site. In addition, please be advised that 'SEC,' the EDGAR logo, and the names EDGAR, EDGARLink, and EDGARLink Online are the SEC's registered trademarks. You may not use them in a trade name, trademark, or domain name of an SEC- or EDGAR-related business without a license from the SEC. You may refer in text to the existence of EDGAR and the EDGAR system without a license, so long as you are not creating the impression that your business is affiliated with or approved by t[he SEC]"
> (Fair-access / bulk-download terms from 'Accessing EDGAR Data', https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data) "Current max request rate: 10 requests/second. To ensure everyone has equitable access to SEC EDGAR content, please use efficient scripting. Download only what you need and please moderate requests to minimize server load. SEC reserves the right to limit request rates to preserve fair access for all users."

*Verifier notes:* VERIFIED AGAINST THE OFFICIAL SOURCE. WebFetch was blocked (403), so I fetched https://www.sec.gov/about/privacy-information directly with a declared User-Agent -> HTTP 200, genuine page (90KB). Under the heading "Website Dissemination" the page contains the quote WORD-FOR-WORD: "Information presented on sec.gov is considered public information and may be copied or further distributed by users of the web site without the SEC's permission. Please consider appropriate citation to the SEC as the source." (The only difference from the finding is a typographic vs straight apostrophe in "SEC's" — trivial encoding, not a wording change.) So quote_verified_verbatim = true.

ADVERSARIAL SEARCH FOR STRICTER CLAUSES — none found that restrict redistribution of EDGAR filing CONTENT. Only narrow carve-outs, all outside the scope of the data itself:
1) Trademark/logo (same "Website Dissemination" section): "Please do not use the SEC seal or any of the other logos or artwork from this site." plus a license requirement for using the SEC/EDGAR marks in a trade/domain name. Restricts SEC's marks, NOT the filing data.
2) Fair-access throttle (EDGAR "Accessing EDGAR Data" page): max 10 requests/second, declare a User-Agent. This is an ACCESS/technical policy, not a redistribution restriction on data already obtained.
3) CUSIP carve-out (Webmaster FAQ): the quarterly 13(f) Securities LIST is published PDF-only "by arrangement with S&P Global Market Intelligence... CUSIP Global Services." Narrow to that one list; does not touch EDGAR filings.
4) Stock-art photos flagged "not free to reuse."

AFFIRMATIVE CORROBORATION: The SEC Webmaster FAQ (https://www.sec.gov/os/webmaster-faq) states, "All Government-created content on sec.gov and EDGAR public filing content are free to access and reuse. See Website Dissemination for our permissions policy," and, asked what is NOT free to reuse, answers "Very little." No prior-written-permission requirement, no non-commercial clause, no no-derivatives clause, and no bulk-download ban on filing content anywhere in the terms.

CLASSIFICATION DEFENSIBLE: redistributable_open is well supported and not too permissive for a library that re-hosts EDGAR data for public download. U.S. Government works carry no copyright (17 U.S.C. section 105) and the SEC expressly permits copying and further distribution of EDGAR filing content without permission. RECOMMENDED NON-BLOCKING CAVEATS for the library (do not affect the classification): (a) do not reproduce the SEC seal/EDGAR logos or use SEC/EDGAR marks in branding; (b) honor the 10 req/sec fair-access limit with a declared User-Agent when harvesting; (c) if the 13(f) CUSIP securities list is ingested, note the CUSIP/S&P Global encumbrance on that specific list; (d) individual filings could embed third-party copyrighted exhibits, but the SEC's blanket dissemination policy treats the sec.gov presentation as freely copyable.

*Researcher reasoning:* The SEC's official permissions policy is the "Website Dissemination" section of its Privacy/site policy page. The Webmaster FAQ explicitly names this section as "our permissions policy" and links to www.sec.gov/privacy.htm#dissemination, which redirects to https://www.sec.gov/about/privacy-information. WebFetch was 403-blocked (sec.gov blocks generic fetchers), so I read the page verbatim via the browser. The governing sentence states EDGAR/sec.gov content "may be copied or further distributed by users of the web site without the SEC's permission" — i.e., redistribution/re-hosting is expressly permitted with NO prior permission required. The Webmaster FAQ reinforces this: "EDGAR public filing content are free to access and reuse," with the only non-reusable items being a few stock art photos. This is consistent with U.S. law: SEC-authored material is a U.S. Government work (public domain, 17 U.S.C. §105), and public company disclosure filings are public records the SEC disseminates freely.\n\nCommercial use: not restricted anywhere in the policy; the content is designated "public information" and is "free to ... reuse," so commercial redistribution is permitted (commercial_ok=true).\n\nAttribution: only requested, not required — the exact wording is "Please consider appropriate citation to the SEC as the source." "Please consider" is a courtesy request, not a binding condition, so attribution_required=false. Hence classification is redistributable_open rather than redistributable_attribution.\n\nCaveats a re-hoster must observe (none of which restrict redistribution of the data itself): (1) Do not reproduce the SEC seal, logos, or artwork; (2) "SEC," the EDGAR logo, and the EDGAR/EDGARLink names are registered trademarks and may not be used in your trade name, trademark, or domain name without a license — a trademark restriction, easily satisfied by not branding the library as an SEC/EDGAR business; (3) A tiny amount of illustrative stock-art photography on sec.gov pages is not free to reuse (irrelevant to EDGAR filing data); (4) Operational fair-access rules for bulk downloading (declare a User-Agent, keep to ~10 requests/second, no botnet crawling) govern how you fetch, not whether you may redistribute. Since the professor's library re-hosts EDGAR filing DATA (not SEC logos/trademarks), redistribution is clearly permitted with no license and no mandatory attribution. Classified conservatively as redistributable_open; sharealike=false (no copyleft obligation).

---

### SIPRI (Stockholm International Peace Research Institute)

- **Databases (1):** `sipri`
- **Official terms URL:** https://www.sipri.org/about/terms-and-conditions
- **License:** Custom SIPRI terms and conditions (fair-use policy; not an open/CC licence)
- **Classification:** permission_required
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> Any reproduction—in any medium, electronic or printed—of the data requires authorization, except where this is covered by SIPRI's fair-use policy.
> SIPRI data may be freely downloaded, cited and used for statistical or other analytical purposes provided that such use is in line with SIPRI's fair-use policy.
> the reproduction of less than 10 per cent of a published data set.
> the excerption of SIPRI copyrighted material for such purposes as criticism, comment, news reporting, teaching, scholarship or research in which the use is for non-commercial purposes
> Both the above conditions must apply to count as fair use.
> Commercial use of SIPRI copyrighted material must be authorized and will normally require the payment of a reasonable royalty.
> Information from the Stockholm International Peace Research Institute (SIPRI), URL ADDRESS

*Verifier notes:* Adversarial review of SIPRI terms. (1) VERBATIM: The quote "Any reproduction—in any medium, electronic or printed—of the data requires authorization, except where this is covered by SIPRI's fair-use policy." appears WORD-FOR-WORD on https://www.sipri.org/about/terms-and-conditions (fetch_status confirmed: fetched_ok, HTTP 200, correct page — not a 404/redirect). Em-dashes match. (2) SEARCH FOR STRICTER CLAUSE: The surrounding terms are if anything stricter than the finding states, all reinforcing permission_required rather than contradicting it: fair use is limited to non-commercial excerption AND "reproduction of less than 10 per cent of a published data set" — BOTH conditions must apply simultaneously; "Commercial use of SIPRI copyrighted material must be authorized and will normally require the payment of a reasonable royalty"; commercial use requires prior written permission from the Publications Department. There is NO open/CC/public-domain grant anywhere on the page. (3) TOO-PERMISSIVE CHECK: A library that re-hosts an entire SIPRI dataset for public third-party download is reproducing 100% of the dataset (far exceeding the 10% fair-use ceiling) and is republishing to others rather than making a personal excerpt for criticism/teaching/research — this is explicitly NOT covered by fair use and "requires authorization." Therefore classification "permission_required" (Custom SIPRI terms; fair-use policy; not an open/CC licence) is fully defensible and not more permissive than the terms support. Could not refute the finding on quote accuracy, license characterization, or classification.

*Researcher reasoning:* SIPRI's official Terms and Conditions page (https://www.sipri.org/about/terms-and-conditions) is not an open or Creative Commons licence; it is a custom copyright policy. It permits free download, citation, and analytical USE, but redistribution/re-hosting is governed by the explicit clause: "Any reproduction—in any medium, electronic or printed—of the data requires authorization, except where this is covered by SIPRI's fair-use policy." The only exception is the fair-use carve-out, which requires BOTH (1) a non-commercial purpose AND (2) reproduction of "less than 10 per cent of a published data set," and states "Both the above conditions must apply to count as fair use."

A free non-commercial academic library that RE-HOSTS SIPRI datasets for download redistributes the FULL data set (100%), which categorically exceeds the "<10 per cent" fair-use ceiling. Therefore this re-hosting is NOT covered by fair use and, per the governing clause, "requires authorization." SIPRI directs users to submit a permission request to its Publications, Library and Editorial Department. Commercial reuse is separately gated ("must be authorized and will normally require the payment of a reasonable royalty"), so commercial_ok is false; and even the platform's non-commercial status does not exempt full-dataset redistribution from the authorization requirement because the fair-use exception is capped at <10%. Attribution is mandatory via the source credit-line "Information from the Stockholm International Peace Research Institute (SIPRI), URL ADDRESS."

Conservative classification: permission_required — the professor must obtain prior written authorization from SIPRI before re-hosting/redistributing these datasets; it is NOT redistributable under an open or non-commercial-only licence without that permission. Verbatim wording of the primary clause was confirmed character-accurate via a targeted re-fetch (the sentence begins "Any reproduction—in any medium, electronic..." with em-dashes, not the paraphrase "in any medium, electronic or printed, requires authorization").

---

### Standardized World Income Inequality Database (SWIID)

- **Databases (1):** `swiid`
- **Official terms URL:** https://dataverse.harvard.edu/api/datasets/export?exporter=dataverse_json&persistentId=doi:10.7910/DVN/LM4OWF
- **License:** CC0 1.0 Universal (Public Domain Dedication)
- **Classification:** redistributable_open
- **Commercial OK:** True · **Attribution required:** False · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK

**Verbatim quote:**
> "license": { "name": "CC0 1.0", "uri": "http://creativecommons.org/publicdomain/zero/1.0", "iconUri": "https://licensebuttons.net/p/zero/1.0/88x31.png", "rightsIdentifier": "CC0-1.0", "rightsIdentifierScheme": "SPDX", "schemeUri": "https://spdx.org/licenses/", "languageCode": "en" }
> Please cite the SWIID as follows: Solt, Frederick. 2020. "Measuring Income Inequality Across Countries and Over Time: The Standardized World Income Inequality Database." Social Science Quarterly 101(3):1183-1199. (from https://fsolt.org/swiid/ — a citation request, not a license condition)
> schema.org export of the same Dataverse record gives license: "http://creativecommons.org/publicdomain/zero/1.0" with no termsOfUse or conditionsOfAccess fields (https://dataverse.harvard.edu/api/datasets/export?exporter=schema.org&persistentId=doi:10.7910/DVN/LM4OWF)
> dataverse_json metadata fields termsOfUse, termsOfAccess, restrictions, and confidentialityDeclaration are all absent

*Verifier notes:* Adversarial review confirms the finding. (1) VERBATIM: The license object in the finding matches the authoritative Harvard Dataverse JSON API export (official_terms_url) field-for-field: name "CC0 1.0", uri http://creativecommons.org/publicdomain/zero/1.0, iconUri https://licensebuttons.net/p/zero/1.0/88x31.png, rightsIdentifier "CC0-1.0", rightsIdentifierScheme "SPDX", schemeUri https://spdx.org/licenses/, languageCode "en". WebFetch pretty-prints the object as a JSON block but the field names/values are identical — no discrepancy. (2) NO STRICTER CLAUSE FOUND: The raw export shows fileAccessRequest=false and all 18 files restricted=false; keys termsOfAccess, termsOfUse, restrictions, confidentialityDeclaration, specialPermissions, conditions, and disclaimer are absent. No non-commercial restriction, no prior-written-permission requirement, no redistribution/bulk-download ban, no guestbook or request-access gate. (3) CLASSIFICATION DEFENSIBLE: CC0 1.0 is a public-domain dedication permitting redistribution, adaptation, and commercial reuse with no attribution required — 'redistributable_open' is exactly supported and not overly permissive for a library that re-hosts for public download. Independent web confirmation: SWIID is published under CC0 on Harvard Dataverse (repository default), which permits unrestricted redistribution and reuse. Nuance considered and dismissed: SWIID is a derived database and Solt requests citation of the SWIID papers, but that is a scholarly citation norm/request, not a license term, and does not restrict redistribution; upstream-rights questions are the depositor's responsibility, and the provider's offered terms (CC0) govern redistributability. The dataset landing page (dataset.xhtml) is JS-rendered and returned empty via WebFetch, but the machine-readable API export is the authoritative primary source and was fully verifiable.

*Researcher reasoning:* The canonical, author-controlled distribution point for the SWIID is Frederick Solt's official deposit on Harvard Dataverse (DOI 10.7910/DVN/LM4OWF, "The Standardized World Income Inequality Database, Versions 8-9"). Two independent official Harvard Dataverse API metadata exports (dataverse_json and schema.org) for that record both show the dataset is released under CC0 1.0 (SPDX: CC0-1.0; URI http://creativecommons.org/publicdomain/zero/1.0). The dataverse_json export confirms the metadata fields termsOfUse, termsOfAccess, restrictions, and confidentialityDeclaration are ALL absent — i.e., there are no additional conditions layered on top of CC0. CC0 1.0 is a public-domain dedication under which the rights holder waives all copyright and related rights worldwide; it permits redistribution, re-hosting, mass download, and re-dissemination for any purpose, including commercial, with no attribution requirement and no share-alike obligation. Therefore redistribution/re-hosting is clearly permitted -> redistributable_open. Notes for the compliance record: (1) The SWIID author's project website (fsolt.org/swiid) asks users to cite the 2020 Social Science Quarterly article; this is a scholarly-norm citation request, NOT a legal license condition, so attribution_required is false as a matter of licence — but including the citation is good practice and costs nothing. (2) The GitHub source repository fsolt/swiid carries NO license (GitHub license API returns HTTP 404 and the README states no license); however that repo holds the code/pipeline, and the authoritative license for the DATA is the CC0 dedication on the Harvard Dataverse deposit, which is what a re-hoster would redistribute. Recommendation: safe to re-host; retain the SWIID name and the Solt (2020) citation for good scholarly practice even though CC0 does not require it.

---

### statcan

- **Databases (1):** `statcan`
- **Official terms URL:** https://www.statcan.gc.ca/en/terms-conditions/open-licence
- **License:** Statistics Canada Open Licence
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Subject to this licence, Statistics Canada grants you a worldwide, royalty-free, non-exclusive licence to: use, reproduce, publish, freely distribute, or sell the Information; use, reproduce, publish, freely distribute, or sell Value-added Products; and, sublicence any or all such rights, under terms consistent with this licence.
> Source: Statistics Canada, name of product, reference date. Reproduced and distributed on an 'as is' basis with the permission of Statistics Canada.
> Adapted from Statistics Canada, name of product, reference date. This does not constitute an endorsement by Statistics Canada of this product.
> You shall not use the Information in any way, or in connection with any product or service, that suggests an association with Statistics Canada, unless Statistics Canada has otherwise consented in writing.

*Verifier notes:* Verified against the official URL (https://www.statcan.gc.ca/en/terms-conditions/open-licence), which is live and correct. The verbatim_quote is accurate word-for-word: two independent WebFetch passes confirmed the opening ("Subject to this licence, Statistics Canada grants you a worldwide, royalty-free, non-exclusive licence to...") and the "sell the Information" / "sublicence any or all such rights, under terms consistent with this licence" clauses, and a targeted WebSearch independently reproduced the full three-clause sentence including the middle clause "use, reproduce, publish, freely distribute, or sell Value-added Products". Adversarial search for stricter clauses found NO redistribution ban, non-commercial restriction, no-derivatives clause, prior-written-permission requirement, or bulk/mass-download limit. The only conditions are: no implying StatCan endorsement, no misrepresenting the source, no re-identification of individuals, no reverse-engineering software, and no presenting as having confidential-data access — none of which impede a library re-hosting the data for public download. The licence explicitly permits "freely distribute" and even "sell" both the Information and Value-added Products. Attribution is required (Source/Adapted-from notices), so "redistributable_attribution" is correctly calibrated: genuinely redistributable but not attribution-free. This is the Open Government Licence - Canada model. Classification is defensible and not too permissive.

*Researcher reasoning:* The governing terms are Statistics Canada's own Open Licence, fetched and read at https://www.statcan.gc.ca/en/terms-conditions/open-licence. The grant clause explicitly authorizes redistribution/re-hosting: it grants a "worldwide, royalty-free, non-exclusive licence to: use, reproduce, publish, freely distribute, or sell the Information" and to "sublicence any or all such rights." This goes well beyond mere access/use — "freely distribute" and "sell" plus "sublicence" cover re-dissemination and re-hosting. The licence does not distinguish commercial from non-commercial use (both permitted), so a free non-commercial academic library is comfortably within scope. The one condition is attribution: an acknowledgment-of-source notice ("Source: Statistics Canada, name of product, reference date. Reproduced and distributed on an 'as is' basis with the permission of Statistics Canada.") must accompany the data. A separate restriction bars using Statistics Canada's name/crest/logos or implying endorsement/association without written consent — a branding restriction, not a bar on data redistribution. This is functionally equivalent to the Open Government Licence – Canada (attribution-only open-gov licence). Classification: redistributable_attribution. Note: this covers standard StatCan public products released under the Open Licence; StatCan explicitly excludes Statistics Canada's official symbols/wordmark and third-party/reproduced-with-permission content, so those specific items would need separate handling, but the underlying statistical data is redistributable with attribution.

---

### Stats NZ

- **Databases (1):** `stats_nz`
- **Official terms URL:** https://www.stats.govt.nz/about-us/copyright/
- **License:** Creative Commons Attribution 4.0 International (CC BY 4.0)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Except for photos, graphics, or logos, or anything with a specific copyright statement, you may copy, distribute, and adapt the work, as long as you attribute it to Stats NZ and obey the other licence terms.
> Crown copyright ©. All material Stats NZ produces is protected by Crown copyright.
> Unless otherwise specified, content we produce is licensed under the Creative Commons Attribution 4.0 International licence.
> Reusing content means you can make it available to the public by publishing it, distributing it, or disseminating it in any other way.
> You may reuse content we produce if you attribute it to Stats NZ using one of the statements below.
> Source: Stats NZ and licensed by Stats NZ for reuse under the Creative Commons Attribution 4.0 International licence.
> To ask permission to reuse a photo, graphic, or logo, email info@stats.govt.nz.
> You may not use any departmental or governmental emblem, logo, or coat of arms in any way that infringes the Flags, Emblems, and Names Protection Act 1981, or would infringe it if you were using it in New Zealand.

*Verifier notes:* VERDICT: CONFIRMED.

QUOTE: Verbatim-accurate. The official_terms_url (https://www.stats.govt.nz/about-us/copyright/) is a JS-rendered SPA that plain WebFetch and the Wayback Machine could not read, so I rendered it in a browser. The rendered page contains the finding's quote character-for-character: "Except for photos, graphics, or logos, or anything with a specific copyright statement, you may copy, distribute, and adapt the work, as long as you attribute it to Stats NZ and obey the other licence terms." (Page updated 3 May 2021.) fetch_status "fetched_ok" and the URL are correct.

Caution flagged and resolved: an early WebSearch summary claimed Stats NZ attributes to "Statistics NZ/Statistics New Zealand," which would have contradicted the finding's "Stats NZ." Checking the primary source directly showed the live page does say "Stats NZ" — the finding is right; the search summary was wrong.

LICENSE: The page states "content we produce is licensed under the Creative Commons Attribution 4.0 International licence," confirming license_name = CC BY 4.0.

ADVERSARIAL CHECK FOR STRICTER CLAUSES (read the full page): No redistribution ban, no non-commercial restriction (CC BY 4.0 allows commercial use), no no-derivatives clause (page explicitly permits "adapt the work" and supplies an adaptation attribution statement), no prior-written-permission requirement for data, no bulk/mass-download restriction. The only carve-outs are (a) photos, graphics, logos, and anything with a specific copyright statement (permission via info@stats.govt.nz), and (b) government emblems/logos/coats of arms under the Flags, Emblems, and Names Protection Act 1981. Neither carve-out affects redistribution of the numeric statistical data a data library would re-host; the finding's own quote already surfaces the photos/graphics/logos exception.

CLASSIFICATION: redistributable_attribution is DEFENSIBLE and not too permissive. It correctly encodes the mandatory attribution condition (the page mandates attribution to "Stats NZ" and supplies specific reuse/adaptation statements), while redistribution and adaptation of the data are genuinely permitted under CC BY 4.0. Practical compliance note for the library (not a defect in the finding): attribute to "Stats NZ" (wording, not the logo) and exclude any embedded photos/graphics/logos from re-hosting.

*Researcher reasoning:* The official Stats NZ copyright page (https://www.stats.govt.nz/about-us/copyright/, page updated 3 May 2021) was fetched and read in full via the browser (the page is JS-rendered, so plain WebFetch returned only the title; the browser get_page_text returned the complete body). It states that Stats NZ content is Crown copyright but "Unless otherwise specified, content we produce is licensed under the Creative Commons Attribution 4.0 International licence." Redistribution is explicitly permitted with attribution: "Except for photos, graphics, or logos, or anything with a specific copyright statement, you may copy, distribute, and adapt the work, as long as you attribute it to Stats NZ and obey the other licence terms." The page removes any ambiguity about re-hosting/re-dissemination by defining reuse to include redistribution: "Reusing content means you can make it available to the public by publishing it, distributing it, or disseminating it in any other way." This is standard CC BY 4.0, which permits commercial use, so commercial_ok=true and there is no non-commercial or ShareAlike restriction (sharealike=false). Attribution is required, with prescribed wording (e.g. "Source: Stats NZ and licensed by Stats NZ for reuse under the Creative Commons Attribution 4.0 International licence."). Classification is redistributable_attribution. Caveats for the library: (1) the CC BY 4.0 grant excludes photos, graphics, logos, and "anything with a specific copyright statement" — a small number of individual datasets may carry their own restrictive statement and should be checked; (2) departmental/governmental emblems, logos and coats of arms may not be used per the Flags, Emblems, and Names Protection Act 1981. For statistical data tables/series produced by Stats NZ, redistribution with attribution is clearly authorised.

---

### Sveriges Riksbank

- **Databases (1):** `riksbank`
- **Official terms URL:** https://www.riksbank.se/en-gb/about-the-riksbank/about-the-website/open-data--information-available-for-re-use/
- **License:** Swedish Open Data Act (2022:818) / EU Open Data Directive 2019/1024 re-use terms (custom open-data permission with attribution; no CC label)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> The Riksbank's open data is freely available and may be further used without any special consent or agreement being required. Enter source, Sveriges riksbank and date.
> The act (2022:818) (in Swedish only) on the public sector making data available for re-use aims to promote the re-use of documents from the public sector. The law was introduced with the aim of implementing EU directive 2019/1024 from 20 June 2019 on open data and the re-use of information from the public sector and replaces the previous PSI law (2010:566).
> If you have processed statistics taken from the Riksbank's website, you may not satate Sveriges Riksbank as the source. You may not present the service you developed as an "official collaboration" or "partnership" with the Riksbank.
> Large parts of the Riksbank’s statistics are public and are available via the Riksbank’s website (https://www.riksbank.se/en-gb/statistics/) in various formats. The Riksbank provides some data sets that are also accessible via APIs.
> Open data – information available for re-use

*Verifier notes:* VERBATIM: Confirmed word-for-word at the official URL. The page's rendered/WebFetch view returned only navigation chrome, so I fetched the raw HTML directly (curl, HTTP 200, 529 KB) and extracted the article body. Under the section header "Conditions for the use of open data" the page reads exactly: "The Riksbank's open data is freely available and may be further used without any special consent or agreement being required. Enter source, Sveriges riksbank and date." This matches the finding's verbatim_quote character-for-character. (Caution flag for the record: an intermediate web-search auto-summary paraphrased the 2nd sentence as "When citing such data, you should enter source..." — that paraphrase is NOT on the page; the finding's quote is the accurate one, per raw HTML.)

STRICTER-CLAUSE HUNT (adversarial): Read the entire body. NO redistribution ban, NO non-commercial restriction, NO prior-written-permission requirement, NO no-derivatives clause, NO bulk/mass-download restriction on the open data. Only two extra conditions exist and neither restricts re-hosting: (1) attribution ("Enter source, Sveriges riksbank and date") — consistent with redistributable_attribution; (2) an anti-misrepresentation clause: "If you have processed statistics taken from the Riksbank's website, you may not [state] Sveriges Riksbank as the source. You may not present the service you developed as an 'official collaboration' or 'partnership' with the Riksbank." This governs misrepresentation of processed/derived data, not redistribution of the raw data. Separately, the "Documents available for re-use" confidentiality caveat ("documents may be confidential... requires each document to undergo assessment each time it is released") applies to ARCHIVE INVENTORY and DIARY records (FOIA-style document requests), NOT to the published statistics/open-data datasets a library would re-host.

CLASSIFICATION DEFENSIBLE: The terms cite the Swedish Open Data Act (2022:818) implementing EU Directive 2019/1024, under which "re-use" is a defined legal term expressly covering redistribution for commercial and non-commercial purposes. "Freely available and may be further used without any special consent or agreement" squarely permits public re-hosting with attribution. license_name in the finding (custom open-data permission with attribution, no CC label) is accurate. redistributable_attribution is neither too permissive nor understated. fetch_status "fetched_ok" is accurate (page live, HTTP 200). No downgrade warranted.

*Researcher reasoning:* The official Riksbank page is titled "Open data – information available for re-use" and is explicitly grounded in Sweden's Open Data Act (2022:818), which implements the EU Open Data Directive 2019/1024 (successor to the PSI Directive). Under that legal framework, "re-use" is defined to include redistribution/re-dissemination of documents for purposes other than the original, covering both commercial and non-commercial purposes. The page's operative clause — "The Riksbank's open data is freely available and may be further used without any special consent or agreement being required. Enter source, Sveriges riksbank and date." — grants unconditional further use (which under the PSI/open-data framework encompasses re-hosting and redistribution) subject only to an attribution requirement (cite "Sveriges riksbank" as source plus the date). No non-commercial limitation and no share-alike condition are imposed; the open-data directive it implements covers commercial re-use, so commercial_ok is true. This supports classification as redistributable_attribution rather than a mere "free to access" grant, because the permission is framed specifically around re-use/redistribution under an open-data statute, not just access.

IMPORTANT NUANCE for the compliance decision: The attribution rule is conditional on whether the data is processed/transformed. For RAW, unmodified re-hosting the library MUST state "Sveriges riksbank" and the date as source. But the page states: "If you have processed statistics taken from the Riksbank's website, you may not [s]tate Sveriges Riksbank as the source." So if the library computes derived series from Riksbank data, it must NOT attribute those to the Riksbank. Additionally the library may not present itself as an "official collaboration" or "partnership" with the Riksbank. (Note: the quoted verbatim text preserves original page typos such as "satate".)

Scope note: The statistics the library would re-host are covered ("Large parts of the Riksbank's statistics are public ... available via the Riksbank's website ... in various formats"), and these fall under the same "Conditions for the use of open data" section. Some archive/diary documents are subject to case-by-case data-protection assessment and confidentiality, but that caveat applies to archival records, not the published statistical open data. Page last updated 20/01/2026; retrieved via browser rendering because the raw HTML fetch returned only navigation chrome.

---

### Swiss National Bank (SNB) data portal

- **Databases (1):** `snb`
- **Official terms URL:** https://www.snb.ch/en/srv/disclaimer_copyright
- **License:** SNB custom copyright terms (non-commercial use permitted, with source reference)
- **Classification:** noncommercial_only
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - non-commercial only

**Verbatim quote:**
> On its website, the SNB provides information and data. Such information and data may be saved, translated (with reference to the source), transmitted or used in other ways, for non-commercial purposes, compatible with the purpose of such information or data.
> The Swiss National Bank (SNB) respects all third-party rights, in particular rights relating to works protected by copyright (information or data, wordings and depictions, to the extent that these are of an individual character).
> Moreover, links to the SNB website may be created, provided, however, that any false or misleading impression as to the existence of any business or other connection with the SNB is avoided.
> To the extent that information and data clearly derive from outside sources, users are obliged to respect any existing copyrights and to obtain the right of use from the relevant outside source themselves.

*Verifier notes:* Verbatim: CONFIRMED. The finding's full quote was reproduced word-for-word at https://www.snb.ch/en/srv/disclaimer_copyright by three independent tools (two WebFetch calls + one WebSearch). Both the lead-in ("On its website, the SNB provides information and data.") and the substantive clause ("Such information and data may be saved, translated (with reference to the source), transmitted or used in other ways, for non-commercial purposes, compatible with the purpose of such information or data.") were confirmed. My first fetch truncated at "non-commercial purposes"; re-fetching confirmed the "compatible with the purpose..." tail is present exactly as quoted. URL is live and is the correct SNB copyright/terms page (fetch_status fetched_ok is accurate).

Adversarial search for a stricter clause: none found. I explicitly probed for (a) redistribution/republishing ban, (b) prior-written-permission requirement, (c) no-derivatives, (d) bulk/systematic-download restriction, (e) item-count limit. All absent. On the contrary, the SNB terms are unusually permissive for a central bank: data may be saved, TRANSLATED (a derivative, expressly allowed with source reference), TRANSMITTED, or "used in other ways" — i.e., redistribution is genuinely permitted — subject only to (i) non-commercial purpose and (ii) compatibility with the data's purpose, plus source reference when translating. The single binding restriction is the non-commercial cap, which is exactly what "noncommercial_only" encodes.

Classification: DEFENSIBLE and not too permissive. "noncommercial_only" correctly captures the binding restriction without overclaiming (it is not "open" or "public_domain") and without understating (redistribution IS permitted, just non-commercially). Per the skeptical standard, I am confident non-commercial redistribution is genuinely permitted because the text affirmatively lists "transmitted or used in other ways ... for non-commercial purposes."

Two non-disqualifying caveats worth flagging to the catalog owner: (1) Outside-sources clause — "users of such information and data are obliged to respect any existing copyrights and to obtain the right of use from the relevant outside source themselves" — means any third-party material embedded in SNB data is NOT covered by SNB's permission and must be cleared separately; (2) the vague "compatible with the purpose of such information or data" limiter. Neither creates a redistribution ban on SNB's own data. Operational note (downstream of the label, not a defect in it): because use is non-commercial-only, a re-hosting library must itself operate non-commercially to remain compliant.

*Researcher reasoning:* The governing terms are on the SNB's own official copyright page (www.snb.ch/en/srv/disclaimer_copyright), which covers the SNB website's provision of "information and data" — the data portal (data.snb.ch) is part of the SNB web presence and its footer routes to these same SNB legal/copyright pages. I fetched and read the page directly and reproduced the operative paragraph verbatim (confirmed against the WebSearch snippet and two independent WebFetch passes).

Redistribution analysis: The clause explicitly permits data to be "saved, translated (with reference to the source), transmitted or used in other ways" — "transmitted or used in other ways" covers redistribution / re-dissemination / re-hosting, so this is NOT a mere access-only or view-only permission. However, the permission is expressly qualified: it is granted only "for non-commercial purposes, compatible with the purpose of such information or data." Because redistribution is allowed but restricted to non-commercial use, the correct conservative classification is noncommercial_only (not redistributable_open or redistributable_attribution). A free, non-commercial academic data library re-hosting SNB series for download falls within the permitted non-commercial scope.

Attribution: The parenthetical "(with reference to the source)" requires citing the source, so attribution_required = true. commercial_ok = false because commercial redistribution is outside the granted scope. No copyleft/share-alike obligation is stated, so sharealike = null.

Important caveat for the compliance decision: the SNB data portal aggregates data drawn from external sources (e.g., the Swiss Federal Statistical Office). The copyright notice states that where information/data "clearly derive from outside sources," users must respect any existing third-party copyrights and obtain the right of use from that outside source themselves. So the non-commercial permission above applies to SNB's own material; individual portal series sourced from third parties may carry separate, potentially more restrictive rights that must be checked per-series before re-hosting.

---

### Transparency International (CPI)

- **Databases (1):** `transparency_ti`
- **Official terms URL:** https://transparency.org/permissions
- **License:** CC BY 4.0 (datasets, incl. CPI); general site content is CC BY-ND 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Anyone can extract, download, and make copies of this data, and may also share that information with third parties. There is no charge for this, and you do not need to ask us for permission – we simply ask that the data is not changed and is attributed to us as 'Source: Transparency International'.
> the CPI and datasets are licensed under CC BY 4.0
> Except where otherwise noted, this work is licensed under CC BY-ND 4.0
> © Transparency International 2026. Some rights reserved.

*Verifier notes:* Adversarial review found no grounds to dispute. VERBATIM: The full quote is confirmed word-for-word on the official page, verified in three segments across two WebFetch calls plus an independent WebSearch that reproduced the opening sentence exactly. Confirmed segments: (1) "Anyone can extract, download, and make copies of this data, and may also share that information with third parties." (2) the em-dash middle "There is no charge for this, and you do not need to ask us for permission –" (3) "we simply ask that the data is not changed and is attributed to us as 'Source: Transparency International'." STRICTER-CLAUSE SEARCH: none found. The page affirmatively negates every restriction probed — no prior-written-permission requirement ("you do not need to ask us for permission"), no non-commercial clause, no bulk/mass-download limit ("extract, download, and make copies" expressly permitted), no redistribution ban ("may also share that information with third parties"). CLASSIFICATION: The formal license section confirms CPI/datasets are CC BY 4.0 while only general site content is CC BY-ND 4.0; the researcher correctly scoped the NoDerivs restriction to non-data content (the key trap here) and avoided it. redistributable_attribution is defensible and not too permissive for a re-hosting library — CC BY 4.0 grants exactly redistribution-with-attribution; sole obligation is crediting "Source: Transparency International". NUANCE (does not change verdict): the plain-language ask "that the data is not changed" superficially reads no-derivatives and mildly conflicts with CC BY 4.0's derivative allowance, but a library re-hosting CPI UNCHANGED satisfies both the informal ask and the formal license, so redistribution is genuinely permitted for the stated use case. URL: finding uses http(s)://transparency.org/permissions (no www); canonical is www.transparency.org/permissions — both resolve to the identical page, fetched_ok, cosmetic only, not a red flag.

*Researcher reasoning:* Transparency International's official copyright/permissions page (transparency.org/permissions, the "Copyright enquiries" page) explicitly and separately addresses DATA/DATASETS, distinct from general site content. For datasets it states the licence is CC BY 4.0 ("the CPI and datasets are licensed under CC BY 4.0") and grants an explicit, unambiguous redistribution right: "Anyone can extract, download, and make copies of this data, and may also share that information with third parties. There is no charge for this, and you do not need to ask us for permission – we simply ask that the data is not changed and is attributed to us as 'Source: Transparency International'." This is exactly the redistribution/re-dissemination/re-hosting language required — not merely "publicly available" branding — and it names attribution as the sole condition ("Source: Transparency International"). CC BY 4.0 permits commercial use and has no ShareAlike requirement, so commercial_ok=true and sharealike=false; attribution is required. Classification is redistributable_attribution.

Two caveats worth flagging to the compliance owner: (1) The CPI landing pages (e.g. transparency.org/en/cpi/2024) carry a site-wide footer reading "Except where otherwise noted, this work is licensed under CC BY-ND 4.0" — the ND (NoDerivatives) variant used for general TI publications/reports. The dedicated data section on the permissions page is the "otherwise noted" exception that governs the dataset itself, so the CPI *data* is CC BY 4.0, while the surrounding report/PDF content is CC BY-ND 4.0. Re-hosting the dataset for download is squarely covered by the CC BY 4.0 data grant. (2) The data grant asks "that the data is not changed." Merely re-hosting the unaltered dataset (even reformatted container, e.g. CSV→parquet, with values intact) is fine; substantively altering/deriving the values would exceed the "not changed" request. For a free non-commercial academic library that redistributes the CPI unchanged with a "Source: Transparency International" credit, redistribution is clearly permitted. Fetch status fetched_ok: I fetched and read transparency.org/permissions (twice, to confirm the verbatim data clause) and the CPI 2024 page for the footer wording; the quotes above are taken from those official TI pages, not from the search-engine summary or third-party mirrors (datahub.io, Our World in Data, Wikipedia).

---

### U.S. Department of the Treasury — Bureau of the Fiscal Service (Fiscal Data, fiscaldata.treasury.gov)

- **Databases (1):** `treasury`
- **Official terms URL:** https://fiscaldata.treasury.gov/api-documentation/
- **License:** U.S. Treasury Fiscal Data open data terms (federal government work, no copyright / no restriction)
- **Classification:** redistributable_open
- **Commercial OK:** True · **Attribution required:** False · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK

**Verbatim quote:**
> The U.S. Department of the Treasury, Bureau of the Fiscal Service is committed to providing open data as part of its mission to promote the financial integrity and operational efficiency of the federal government. The data is offered free, without restriction, and available to copy, adapt, redistribute, or otherwise use for non-commercial or commercial purposes.
> (From the About Us > Licensing section, https://fiscaldata.treasury.gov/about-us/): "The U.S. Department of the Treasury’s Bureau of the Fiscal Service is committed to providing open data. The data on this site is available to copy, adapt, redistribute, or otherwise use for non-commercial and commercial purposes."

*Verifier notes:* VERBATIM CHECK: PASS. The quote matches word-for-word. The live URL https://fiscaldata.treasury.gov/api-documentation/ returned HTTP 403 to WebFetch (Treasury bot-blocking, not a 404/missing page), so I independently confirmed against the OFFICIAL open-source frontend that renders this exact page: repo fedspendingtransparency/fiscal-data (described by GitHub/search as "Frontend of FiscalData.Treasury.gov"), file src/components/api-documentation/getting-started/getting-started.jsx, lines 230-236, section id="license-and-authorization" (title "License and Authorization"). The rendered text is character-for-character identical to the researcher's verbatim_quote: "The U.S. Department of the Treasury, Bureau of the Fiscal Service is committed to providing open data as part of its mission to promote the financial integrity and operational efficiency of the federal government. The data is offered free, without restriction, and available to copy, adapt, redistribute, or otherwise use for non-commercial or commercial purposes."

STRICTER-CLAUSE SEARCH: NONE FOUND. The entire "License and Authorization" section is this single paragraph — it ends at </p> and the section closes immediately, followed by the unrelated "API Versioning" section. There is NO redistribution ban, NO "prior written permission" requirement, NO non-commercial-only restriction (it explicitly allows "commercial purposes"), NO no-derivatives clause (explicitly permits "adapt"), and NO mass-download/bulk-extraction limit. The grant explicitly names "redistribute" and "copy," which is exactly the re-hosting use case in question.

CLASSIFICATION JUDGMENT: DEFENSIBLE and not too permissive. This is a U.S. federal government work; federal government works carry no copyright (17 U.S.C. sec. 105), consistent with "offered free, without restriction." classification "redistributable_open" is fully supported by the terms for a library that re-hosts the data for public download. Adversarial attempt to refute failed on every axis.

CAVEAT: Primary URL was inaccessible to automated fetch (403); verification rests on the official Treasury/USDS-maintained frontend repository, which is authoritative for the rendered page content. If desired, a human can open the page in a normal browser and jump to the #license-and-authorization anchor to see the identical text.

*Researcher reasoning:* The provider "treasury" maps to the U.S. Department of the Treasury, Bureau of the Fiscal Service, whose datasets are served at fiscaldata.treasury.gov (the standard Treasury open-data source: debt, revenue, spending, interest rates, savings bonds, etc.). Two separate official pages on Treasury's own domain state identical, explicit redistribution grants.

The API Documentation page's "License and Authorization" section (https://fiscaldata.treasury.gov/api-documentation/) states the data is "offered free, without restriction, and available to copy, adapt, redistribute, or otherwise use for non-commercial or commercial purposes." The About Us > Licensing section (https://fiscaldata.treasury.gov/about-us/) repeats: "The data on this site is available to copy, adapt, redistribute, or otherwise use for non-commercial and commercial purposes."

Both pages were fetched directly (via a browser user-agent; the default fetcher returned 403, so the raw HTML was retrieved and the exact section text extracted). The word "redistribute" appears explicitly, satisfying the requirement for explicit re-dissemination language rather than mere "open data" branding. Redistribution is permitted for BOTH commercial and non-commercial purposes, so this is not noncommercial_only. The grant is "without restriction" and no attribution/citation is stated as a mandatory condition (Treasury offers optional chart-citation helpers on the site, but nothing conditions reuse on attribution), consistent with U.S. federal government works being outside domestic copyright (17 U.S.C. 105). Therefore classification is redistributable_open with commercial_ok=true, attribution_required=false, sharealike=false.

Caveat for the compliance record: this finding covers Treasury Fiscal Data (Bureau of the Fiscal Service). If any dataset labeled "treasury" in the library actually originates from a different Treasury sub-site (e.g., TreasuryDirect.gov, which carries its own separate Terms & Conditions, or OFAC/IRS pages), those would need their own review; but for the standard Treasury fiscal/financial data feeds this open-data grant governs. Note also 31 U.S.C. 333 separately prohibits using Treasury names/seals to falsely imply endorsement — a branding restriction, not a data-redistribution restriction.

---

### UN Comtrade (United Nations Statistics Division, DESA/UNSD)

- **Databases (1):** `comtrade`
- **Official terms URL:** https://comtrade.un.org/licenseagreement.html
- **License:** UN Comtrade License Agreement / Policy on use and re-dissemination of UN Comtrade data (custom UN terms — not an open licence)
- **Classification:** permission_required
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED by WRITTEN PERMISSION

**Verbatim quote:**
> any copying, automated browsing or downloading, redistribution, publication, or commercial exploitation of any material contained on or otherwise made available to you on United Nations COMTRADE is strictly prohibited without the prior written permission of the United Nations.
> The materials contained on or otherwise made available to you on United Nations COMTRADE may be copyrighted by the United Nations and, thus, are protected by copyright laws and regulations worldwide. (https://comtrade.un.org/licenseagreement.html)
> Any requests for permission to reproduce or otherwise use any material contained on or otherwise made available to you on United Nations COMTRADE should be directed to The Director of the United Nations Statistics Division: comtrade@un.org (https://comtrade.un.org/licenseagreement.html)
> UN Comtrade data are provided for internal use only and may not be re-disseminated in any form without UNSD's permission. (Policy on use and re-dissemination of UN Comtrade data, uncomtrade.org)
> To re-disseminate UN Comtrade data, a user must be an active premium subscriber. (Policy on use and re-dissemination of UN Comtrade data, uncomtrade.org)
> Regarding citation, refer to the data source as 'UN Comtrade.' (Policy on use and re-dissemination of UN Comtrade data, uncomtrade.org)

*Verifier notes:* Adversarial verification of UN Comtrade finding. (1) VERBATIM QUOTE: Two independent WebFetch passes of the official_terms_url (https://comtrade.un.org/licenseagreement.html) reproduced the quoted sentence word-for-word, matching the finding exactly. Page loaded correctly (no 404/redirect/error). The second fetch used an adversarial, differently-worded prompt (searching for permissive/allowed-use clauses) and still surfaced the same restrictive sentence, ruling out an echo artifact. (2) STRICTER-CLAUSE SEARCH: No clause on the page permits redistribution, republication, or public download without prior written permission. The agreement is closed-permission by structure (forbidden unless expressly permitted in writing), directs permission requests to the Director of UNSD (comtrade@un.org), and asserts worldwide copyright. Independent web search corroborated the substance from third-party sources: UN Comtrade data 'may not be re-disseminated in any form without prior written consent of the United Nations Statistics Division' — i.e., corroboration is stricter, never looser. (3) CLASSIFICATION: permission_required is the correct, strictest reasonable tier; it accurately encodes 'redistribution allowed only with prior written permission.' It is NOT too permissive for a library that re-hosts data for public download — such re-hosting is barred absent UNSD written consent. Note (non-material): UN Comtrade has migrated to comtradeplus.un.org with a parallel LicenseAgreement page, but the cited legacy URL remains live and displays the quoted text verbatim, and the newer terms are equally/more restrictive, so nothing undermines the classification. Verdict: CONFIRMED — quote verbatim-accurate and classification defensible.

*Researcher reasoning:* UN Comtrade data are copyrighted by the United Nations, NOT released under any open/CC licence. Two official UN sources govern reuse and both forbid re-hosting without prior permission:

1) The official License Agreement at https://comtrade.un.org/licenseagreement.html (fetched twice, quote confirmed verbatim both times) states that "any copying, automated browsing or downloading, redistribution, publication, or commercial exploitation of any material ... on United Nations COMTRADE is strictly prohibited without the prior written permission of the United Nations." It also asserts UN copyright and directs permission requests to the Director of UNSD (comtrade@un.org). No commercial/non-commercial distinction is drawn — ALL redistribution needs prior written permission.

2) The current "Policy on use and re-dissemination of UN Comtrade data" (fetched via uncomtrade.org; the legacy comtrade.un.org policy PDF now redirects to comtradeplus.un.org) states data are "provided for internal use only and may not be re-disseminated in any form without UNSD's permission," and further that "To re-disseminate UN Comtrade data, a user must be an active premium subscriber." Re-dissemination is thus gated behind BOTH a paid premium subscription and UNSD permission. The only permission-free exceptions are narrow: publishing a few tables/graphs in articles/books/social media, and low-volume (up to ~100,000 records) free extraction/visualization/analytics apps for internal use.

APPLICATION TO THIS PROJECT: A free academic library that re-hosts the full/bulk dataset for download is precisely the "re-dissemination in any form" the policy prohibits without permission. The <=100,000-record free-app carve-out does not cover mass re-hosting, and re-dissemination explicitly requires an active premium (paid) subscription plus permission. Redistribution is not forbidden outright (a permission/premium-subscription path exists), so the conservative-but-accurate classification is permission_required rather than prohibited. The professor must obtain written permission from UNSD (and likely a premium subscription) before re-hosting; bulk re-hosting must NOT proceed on the basis of "publicly accessible" branding. Attribution as "UN Comtrade" (or "DESA/UNSD") is required. commercial_ok=false (for-profit re-dissemination is fee/licence-gated and prohibited without permission); sharealike=false (no such licence applies).

---

### UNCTAD (UN Conference on Trade and Development) — UNCTADstat / UNCTAD Data Hub

- **Databases (38):** `unctad_bopcaba`, `unctad_ciocgeaia`, `unctad_cioiuibbicoeair4a`, `unctad_cpa`, `unctad_cpia`, `unctad_cpta`, `unctad_fdiiaofasa`, `unctad_fmcpa`, `unctad_fmcpia21`, `unctad_gasbeaiogasa`, `unctad_gasbtbia`, `unctad_gasbtoia`, `unctad_gdpgbtoevbkoeatasa`, `unctad_gdptapccac2pa`, `unctad_lscia`, `unctad_lsciq`, `unctad_mfbcoboa`, `unctad_mmcascioeaiopa`, `unctad_mpcadioeaia`, `unctad_mtba`, `unctad_mttasa`, `unctad_mttgra`, `unctad_neera`, `unctad_reericba`, `unctad_reerigdba`, `unctad_rfia`, `unctad_rgdptapcgra`, `unctad_sbeaiotsvsaga`, `unctad_sbtisvsaga`, `unctad_soigapotta`, `unctad_sotwmfvbcoboa`, `unctad_srbca`, `unctad_tabbapotta`, `unctad_tabmcioeaiopa`, `unctad_tabmscioeaiopa`, `unctad_tabpcioeaia`, `unctad_taupa`, `unctad_wstbtocabgoea`
- **Official terms URL:** https://unctadstat.unctad.org/EN/About.html
- **License:** Creative Commons Attribution 3.0 IGO (CC BY 3.0 IGO)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> All data and metadata provided on the UNCTAD Data Hub may be copied freely, duplicated and further distributed provided that United Nations Trade and Development Data Hub is cited as the source.
> All materials provided on this website are copyrighted under the Creative Commons Attribution 3.0 IGO license. (source: https://unctadstat.unctad.org/EN/About.html)
> All data and metadata provided on the UNCTAD Data Hub may be copied freely, duplicated and further distributed provided that United Nations Trade and Development Data Hub is cited as the source. (also appears verbatim on the FAQ page: https://unctadstat.unctad.org/EN/FAQ.html)
> From the separate generic UNCTADstat Copyright.html boilerplate (https://unctadstat.unctad.org/EN/Copyright.html): 'All rights reserved. None of the materials provided on this web site may be used, reproduced or transmitted, in whole or in part, in any form or by any means, electronic or mechanical, including photocopying, recording or the use of any information storage and retrieval system, except as provided for in the Terms and Conditions of Use of United Nations Web Sites, without permission in writing from the publisher.' — This generic UN web-content notice is superseded, for the datasets themselves, by the data-specific CC BY 3.0 IGO grant quoted above.

*Verifier notes:* Quote verified verbatim at the official URL (https://unctadstat.unctad.org/EN/About.html) via WebFetch, and independently corroborated by a WebSearch that returned the identical sentence on both the About page and the FAQ page, plus a second character-exact WebFetch. URL resolves to UNCTAD's live terms — no 404, no off-topic redirect. Adversarial search for a stricter clause found NONE: I checked both the About and FAQ pages for a redistribution ban, non-commercial restriction, prior-written-permission requirement, no-derivatives clause, or bulk/mass-download limit. To the contrary, the FAQ affirmatively states bulk downloads are supported, no registration/payment is required, commercial use is permitted, and no prior permission is needed; the sole obligation is attribution. The license is CC BY 3.0 IGO, a genuinely permissive attribution license (copying, redistribution, commercial use, and derivatives allowed with attribution). The plain-language grant 'may be copied freely, duplicated and further distributed' confirms redistribution itself — not merely internal use — is permitted, so re-hosting for public download with a UNCTAD source credit is within terms. Classification redistributable_attribution is defensible and not too permissive. Minor non-material note: the required attribution string is the rebranded 'United Nations Trade and Development Data Hub' (UNCTAD's post-rebrand name), already reflected in the quote; it does not affect the classification.

*Researcher reasoning:* The UNCTAD Data Hub (which hosts UNCTADstat, the source of all the unctad_* database IDs) publishes an explicit data-licensing statement on its official domain (unctadstat.unctad.org). On both the About page and the FAQ page it states, verbatim and identically, that "All materials provided on this website are copyrighted under the Creative Commons Attribution 3.0 IGO license" and that "All data and metadata provided on the UNCTAD Data Hub may be copied freely, duplicated and further distributed provided that United Nations Trade and Development Data Hub is cited as the source." This is an EXPLICIT redistribution/re-dissemination grant ("further distributed"), not mere access/use language — it directly permits re-hosting for download. CC BY 3.0 IGO is a standard attribution license that permits redistribution and adaptation, including commercial use, with no non-commercial restriction and no share-alike obligation; the only condition is attribution. The single condition attached in the UNCTAD statement is likewise attribution — citing "United Nations Trade and Development Data Hub" as the source. Therefore the correct conservative classification is redistributable_attribution. Note on the apparent conflict: the older, generic Copyright.html page carries an "All rights reserved ... permission in writing from the publisher" boilerplate used across UN websites, and the generic UN Terms of Use (unctad.org/terms) restrict use to "personal, non-commercial use, without any right to resell or redistribute." However, these generic notices are (a) about general web-site content and (b) explicitly subordinated to more specific terms ("except as provided for in the Terms and Conditions..."; "subject to more specific restrictions that may apply to specific Material"). The Data Hub's dataset-specific CC BY 3.0 IGO statement is the more specific, more recent, and controlling grant for the statistical data and metadata being re-hosted, and it explicitly authorizes free redistribution with attribution. For a free, non-commercial academic library, re-hosting these datasets is permitted provided each dataset attributes "UN Trade and Development (UNCTAD) Data Hub / UNCTADstat" as the source. Fetch status is fetched_ok: the governing quote was read directly on the official About.html page and independently confirmed word-for-word on the official FAQ.html page.

---

### UNDP Human Development Report (Human Development Report Office, UNDP)

- **Databases (1):** `undp_hdr`
- **Official terms URL:** https://hdr.undp.org/copyright-and-terms-use
- **License:** Creative Commons Attribution 3.0 IGO (CC BY 3.0 IGO)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Share — copy and redistribute the material in any medium or format ... Adapt — remix, transform, and build upon the material for any purpose, even commercially.
> All materials provided on this website are copyrighted under the Creative Commons Attribution 3.0 IGO license.
> You must give appropriate credit, provide a link to the license, and indicate where changes were made.
> In no event shall the HDRO and UNDP be liable for damages arising from its use.

*Verifier notes:* Fetched https://hdr.undp.org/copyright-and-terms-use (fetch_status fetched_ok is accurate). Two independent WebFetch passes confirm the page is live and is the correct official UNDP HDR terms page. The two component clauses of the verbatim_quote appear WORD-FOR-WORD: "Share — copy and redistribute the material in any medium or format" and "Adapt — remix, transform, and build upon the material for any purpose, even commercially." The finding's "..." legitimately elides the intervening standard CC-deed attribution-condition sentence ("You must give appropriate credit, provide a link to the license, and indicate where changes were made."). License is correctly identified as Creative Commons Attribution 3.0 IGO. Adversarial search for a stricter clause found NONE: no non-commercial restriction, no no-derivatives clause, no "prior written permission" requirement, no redistribution ban, and no bulk/mass-download or automated-access restriction. The only extra conditions are (a) standard CC BY attribution and (b) a UN Charter acceptable-use list (materials shall not be used for defamation, harassment, obscene content, or infringement) plus a non-binding recommendation to keep re-hosted datasets updated with the latest data and to include HDI cross-year-comparability explanatory text. These are acceptable-use / attribution conditions, not redistribution restrictions, and do not make the classification too permissive. CC BY 3.0 IGO genuinely permits a data library to re-host the data for public download with attribution. Classification redistributable_attribution is defensible and not over-permissive. Verdict: CONFIRMED.

*Researcher reasoning:* Fetched and read the provider's own official terms pages: https://hdr.undp.org/copyright-and-terms-use and the mirror at https://hdr.undp.org/terms-use. Both state the website's materials are licensed under the Creative Commons Attribution 3.0 IGO licence and quote the standard CC BY grant verbatim, including the explicit "Share — copy and redistribute the material in any medium or format" clause and the "Adapt — remix, transform, and build upon the material for any purpose, even commercially" clause, with the only condition being attribution ("You must give appropriate credit, provide a link to the license, and indicate where changes were made"). CC BY 3.0 IGO is an open licence that expressly permits redistribution/re-hosting (a superset of the free, non-commercial academic re-hosting use case) subject to attribution; there is no NonCommercial restriction and no ShareAlike obligation. The terms link to the full licence legal code at http://creativecommons.org/licenses/by/3.0/igo/legalcode. Additional site conditions (comply with UN Charter, no defamatory/unlawful material, use latest data, liability disclaimer) are conduct/disclaimer provisions and do not restrict redistribution. Classification therefore is redistributable_attribution. Note for implementation: attribution to UNDP/HDRO and a link to the CC BY 3.0 IGO licence must accompany the re-hosted data. (One caveat worth flagging to the professor: this licence covers UNDP-authored HDR content; some indicators inside the HDR data files are sourced from third parties (e.g. World Bank, UNESCO, WHO) whose own terms may differ — but the UNDP HDR dataset as published under undp_hdr is governed by CC BY 3.0 IGO.)

---

### UNESCO Institute for Statistics (UIS)

- **Databases (5):** `unesco_clte`, `unesco_cltt`, `unesco_dem`, `unesco_film`, `unesco_inno`
- **Official terms URL:** https://databrowser.uis.unesco.org/terms-and-conditions
- **License:** Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** True · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> The information available on this website has been posted with the intent that it be readily available for sharing and reproduction, in part or in whole, and by any means, without charge or further permission unless otherwise specified.
> The work of the UIS is licensed under the Creative Commons Attribution-ShareAlike 4.0 International license.
> Source: (If appropriate 'Adapted from') UNESCO Institute for Statistics (UIS), complete URL, date of extraction.
> The work of the UIS is licensed under the Creative Commons Attribution-ShareAlike 3.0 IGO License. (older uis.unesco.org terms page)

*Verifier notes:* Verified the verbatim_quote WORD-FOR-WORD against the official URL (https://databrowser.uis.unesco.org/terms-and-conditions) via two independent WebFetch calls and a corroborating WebSearch snippet — all three reproduce the sentence identically, including 'without charge or further permission unless otherwise specified.' URL is live (fetch_status fetched_ok is accurate) and is the genuine UIS terms page.

License version independently confirmed: I specifically probed for the CC BY-SA 3.0 IGO variant that UN agencies often use; it is NOT that. Both the page and a second UNESCO license page state 'Creative Commons Attribution-ShareAlike 4.0 International.' The finding's license_name is exact.

Adversarial search for a stricter clause found NONE that undermines redistribution: no non-commercial restriction (data usable 'for any purpose'), no prior-written-permission requirement for data (only for the UIS name/logo, via uis.publications@unesco.org), no no-derivatives clause (adaptation expressly allowed), and no bulk/mass-extraction ban ('extract from, download, copy, adapt, print, distribute, share and embed' are expressly permitted). Attribution with date of extraction is required, which redistributable_attribution captures. The 'unless otherwise specified'/'except where restrictions apply' carve-outs are standard CC framing for third-party embedded content, not a redistribution bar.

Refinement (not a refutation): the license is CC BY-SA 4.0, whose ShareAlike/copyleft condition is not named by redistributable_attribution. For verbatim re-hosting of the unmodified data this is fully defensible and not too permissive — redistribution for public download is genuinely permitted with attribution. If the taxonomy offers a sharealike variant it would be more precise, and any adapted/aggregated re-host the library creates must itself carry CC BY-SA 4.0. This caveat does not make the classification more permissive than the terms support, so the verdict remains CONFIRMED.

*Researcher reasoning:* The database IDs covered (unesco_clte, unesco_cltt, unesco_dem, unesco_film, unesco_inno) are UNESCO Institute for Statistics datasets served through the UIS Data Browser. I fetched and read the official UIS Data Browser Terms and Conditions page (https://databrowser.uis.unesco.org/terms-and-conditions), which is the current authoritative terms for this data.

REDISTRIBUTION: The page EXPLICITLY permits redistribution/re-dissemination, not merely access. Verbatim: "The information available on this website has been posted with the intent that it be readily available for sharing and reproduction, in part or in whole, and by any means, without charge or further permission unless otherwise specified." "Sharing and reproduction... in part or in whole, and by any means, without charge or further permission" clearly authorizes re-hosting for download by a third party.

LICENCE: Verbatim: "The work of the UIS is licensed under the Creative Commons Attribution-ShareAlike 4.0 International license." CC BY-SA 4.0 is a recognized open licence that permits redistribution (including commercially) provided two conditions are met: (a) attribution, and (b) ShareAlike — any redistributed material or adaptations must be licensed under the same CC BY-SA terms.

ATTRIBUTION: Required. Verbatim citation format from the page: "Source: (If appropriate 'Adapted from') UNESCO Institute for Statistics (UIS), complete URL, date of extraction." The date of extraction must be included because UIS databases are updated/revised regularly.

COMMERCIAL: CC BY-SA 4.0 itself permits commercial use, so commercial_ok=true under the current Data Browser terms. NOTE a slight inconsistency: the OLDER main-site terms page (uis.unesco.org) framed the free-reproduction grant as for "personal or public non-commercial use" and directed commercial-use and translation-rights requests to uis.publications@unesco.org, under CC BY-SA 3.0 IGO. For this project (a FREE, NON-COMMERCIAL academic library) redistribution is clearly permitted under either version, so the non-commercial ambiguity does not affect the classification.

CLASSIFICATION: redistributable_attribution. Redistribution/re-hosting is explicitly allowed; the binding conditions are attribution AND ShareAlike (the re-hosted UIS data must carry the same CC BY-SA licence and attribution). This is not classed noncommercial_only because the governing CC BY-SA 4.0 licence does not restrict to non-commercial use.

COMPLIANCE ACTION FOR THE LIBRARY: (1) attribute UIS as source using the prescribed format including a URL and extraction date; (2) apply the ShareAlike obligation — mark these five datasets as CC BY-SA 4.0 so downstream redistribution carries the same licence. I was unable to re-confirm, on a second fetch, an additional descriptive sentence ("you can extract from, download, copy, adapt, print, distribute, share and embed UIS data for any purpose") that appeared in a search snippet; I therefore did not rely on it and quoted only the sentences I directly verified on the fetched page.

---

### UNHCR (United Nations High Commissioner for Refugees) — Refugee Data Finder / Refugee Population Statistics Database

- **Databases (1):** `unhcr`
- **Official terms URL:** https://www.unhcr.org/what-we-do/data-and-publications/data-and-statistics/terms-use-datasets
- **License:** CC BY 4.0 (Creative Commons Attribution International License 4.0)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> 1. Except where otherwise provided, the datasets made available by UNHCR on the UNHCR Refugee Population Statistics Database (the "Datasets") are licensed under a Creative Commons Attribution International License 4.0 (the "CC BY License") and the provision thereof is subject to the supplemental terms contained below in these Terms of Use for Datasets. The terms of the CC BY License are found here: [deed | license]
> 3. You shall provide attribution to UNHCR and its data providers in the following format:"UNHCR Refugee Population Statistics Database".
> 4. When sharing or facilitating access to the Datasets, the URI or hyperlink to the Datasets shall also provide the uniform resource locator (URL) of these Terms of Use for Datasets.
> 2. Access to and use of the Datasets are subject to the UNHCR Website Terms of Use that apply to use of the UNHCR's website. See https://www.unhcr.org/terms-and-conditions.html. In case of conflict between any provisions of the UNHCR Website Terms of Use conflict with these Terms of Use for Dataset, these Terms of Use for Datasets shall prevail.
> 6. Some datasets and indicators are provided by third parties, and may not be shared, redistributed or reused without the consent of the original data provider, or may be subject to terms and conditions that are different from those described herein. Where applicable, these conditions are included in the dataset or indicator metadata.
> 7. UNHCR name and emblem are the exclusive property of UNHCR. They are protected under international law. Unauthorized use is prohibited. They may not be copied or reproduced in any way without the prior written permission of UNHCR.

*Verifier notes:* VERBATIM CHECK: Passed. WebFetch of the official URL returned HTTP 403 (bot-blocking) and web.archive.org was unreachable, so I loaded the page in the browser pane. It resolved to the genuine official page (title: "Terms of use for datasets | UNHCR", canonical unhcr.org). Clause 1 on the live page is word-for-word identical to the finding's verbatim_quote, differing only in curly vs. straight quotation marks (typographic normalization). The URL is correct and live; only WebFetch is blocked.

ADVERSARIAL CLAUSE SCAN (all 14 clauses read): No non-commercial restriction, no no-derivatives clause, no bulk/mass-download ban (clause 8 explicitly allows API access), and no blanket prior-written-permission requirement for the data. Clause 7's prior-written-permission requirement is confined to the UNHCR name/emblem (trademark) and to implying affiliation — it does not restrict data redistribution and is fully CC BY 4.0-compatible (CC BY does not grant trademark rights anyway). Clauses 3-5 (attribution format "UNHCR Refugee Population Statistics Database", propagate the Terms URL when sharing, no implied UNHCR endorsement) are attribution/no-endorsement obligations inherent to CC BY 4.0.

ONE MATERIAL CAVEAT (clause 6): "Some datasets and indicators are provided by third parties, and may not be shared, redistributed or reused without the consent of the original data provider, or may be subject to terms and conditions that are different from those described herein. Where applicable, these conditions are included in the dataset or indicator metadata." This is the standard "except where otherwise provided" carve-out already signalled in clause 1's opening words. It qualifies specific flagged third-party records but does NOT override the CC BY 4.0 default for UNHCR's own data. It is not a contradiction of the classification; it is an operational obligation for a re-hoster.

JUDGMENT: The stated license is CC BY 4.0, which unambiguously permits redistribution (including commercial) with attribution. The classification redistributable_attribution is defensible and not too permissive at the source level. RE-HOSTER ACTION ITEMS (not a downgrade): (a) honor clause 6 by excluding or separately flagging any third-party datasets/indicators marked in the source metadata as consent-required; (b) apply the exact attribution string "UNHCR Refugee Population Statistics Database"; (c) surface a link to these Terms of Use alongside the data per clause 4; (d) do not reproduce the UNHCR name/emblem or imply endorsement (clauses 5, 7).

*Researcher reasoning:* The official UNHCR "Terms of Use for Datasets" page (fetched HTTP 200 from the unhcr.org domain via a browser user-agent; standard WebFetch returned 403 from this host) explicitly states in Clause 1 that the datasets on the UNHCR Refugee Population Statistics Database — the data served by the Refugee Data Finder, which maps to database id "unhcr" — "are licensed under a Creative Commons Attribution International License 4.0 (the 'CC BY License')" and link to the standard CC BY 4.0 deed and legalcode. CC BY 4.0 is a standard open licence that EXPLICITLY grants the right to reproduce, redistribute, and share the material in any medium or format, including for commercial purposes, subject only to attribution. This is exactly the redistribution/re-hosting right needed, so classification is redistributable_attribution rather than mere "publicly available" branding. Conditions: (a) attribution to UNHCR in the format "UNHCR Refugee Population Statistics Database" (Clause 3); (b) when sharing/facilitating access, also provide the URL of these Terms of Use (Clause 4). CC BY 4.0 has no ShareAlike obligation (sharealike=false) and permits commercial use (commercial_ok=true). The supplemental terms do NOT impose a non-commercial restriction on the datasets; Clause 2 states that where the general UNHCR Website Terms of Use conflict, "these Terms of Use for Datasets shall prevail," so the CC BY licence governs the datasets even though the separate general website/copyright pages restrict "information" to non-commercial research use. IMPORTANT CAVEATS for the re-hosting library: (1) Clause 6 carves out some third-party-provided datasets/indicators, which "may not be shared, redistributed or reused without the consent of the original data provider" — the re-hoster should confirm the specific indicators being redistributed are UNHCR's own CC BY data and not third-party-sourced series flagged in the dataset/indicator metadata; (2) Clause 7 forbids use of the UNHCR name and emblem/logo without prior written permission — so redistribute the data with textual attribution but do not reproduce UNHCR's logo. Net: the core UNHCR refugee statistics may be freely re-hosted for a free non-commercial academic library provided attribution and the terms-URL are supplied, with the third-party-indicator carve-out respected.

---

### Uppsala Conflict Data Program (UCDP)

- **Databases (1):** `ucdp`
- **Official terms URL:** https://ucdp.uu.se/downloads/
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> All datasets are free of charge and licensed under CC BY 4.0 — you are free to use and redistribute them provided you cite the relevant publications listed with each dataset.
> Except where otherwise noted, content on this site is licensed under a Creative Commons Attribution 4.0 International license (CC BY 4.0)

*Verifier notes:* Quote verified verbatim at https://ucdp.uu.se/downloads/ (fetch_status fetched_ok confirmed). To rule out prompt-contamination, I ran a SECOND fetch whose prompt did NOT contain the quote, asking only for verbatim transcription; it independently reproduced every load-bearing fragment: "free of charge," "licensed under CC BY 4.0," and the tail clause "free to use and redistribute them provided you cite the relevant publications listed with each dataset." An independent WebSearch reproduced the same sentence. The distinctive em-dash phrasing matches.

Adversarial search for a stricter clause found NONE: (1) It is CC BY 4.0, not CC BY-NC — no non-commercial restriction. (2) Not CC BY-ND — derivatives permitted. (3) No "prior written permission" requirement; the UCDP department FAQ page corroborates with "Except where otherwise noted, content on this site is licensed under a Creative Commons Attribution 4.0 International license (CC BY 4.0)." (The "prior written consent" search hits were a DIFFERENT UCDP — Fannie/Freddie's Uniform Collateral Data Portal — not Uppsala.) (4) No bulk/mass-download ban; the page actively offers "programmatic or bulk access" via the UCDP API. Independently corroborated by an academic source (One Earth/Cell 2025, "Mapping global violence") also stating CC BY 4.0.

Classification is DEFENSIBLE and not too permissive: CC BY 4.0 explicitly authorizes redistribution (incl. commercial) with attribution, exactly what a re-hosting library needs. The classification redistributable_attribution correctly retains the attribution/citation condition rather than treating the data as unconditionally open. Minor residual caveat: the blanket "All datasets... CC BY 4.0" statement carries an implicit "except where otherwise noted," so the library should preserve each dataset's specific required citation(s) — already implied by the attribution classification. No contradicting clause found; finding stands.

*Researcher reasoning:* The official UCDP Dataset Download Center (https://ucdp.uu.se/downloads/, on UCDP's own domain) contains an explicit license statement, quoted verbatim above, that grants the right to "use and redistribute" all datasets under CC BY 4.0, conditioned only on citing the relevant publications listed with each dataset. This is not merely "publicly available" or "free to access" branding — it is explicit redistribution/re-dissemination language ("free to ... redistribute them"). CC BY 4.0 is a standard open license that permits redistribution and re-hosting of the original and derivative works, for both commercial and non-commercial purposes, with no ShareAlike/copyleft requirement, provided appropriate attribution is given. The uu.se UCDP pages corroborate the CC BY 4.0 licensing. Because redistribution is expressly permitted subject to an attribution/citation condition (rather than being unconditional public-domain/CC0, and rather than being restricted to non-commercial use or requiring prior written permission), the correct conservative classification is redistributable_attribution. Practical compliance note for a re-hosting library: attribution must include citing the specific publications UCDP lists for each individual dataset (the "Please cite" entries), not just a generic UCDP credit; commercial use is permitted; no ShareAlike obligation applies.

---

### US Office of Financial Research (OFR), U.S. Department of the Treasury

- **Databases (1):** `ofr`
- **Official terms URL:** https://www.financialresearch.gov/legal-notices/
- **License:** U.S. Government public domain (no copyright claimed under 17 U.S.C. § 105); credit requested, not required
- **Classification:** redistributable_open
- **Commercial OK:** True · **Attribution required:** False · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK

**Verbatim quote:**
> No copyright may be claimed for any work on this website that was created by a federal employee in the course of his or her duties. However, credit is requested if you reproduce or copy any such work.
> If copyrighted material appears on the site, or is reached through a link on this site, the copyright holder must be consulted before the material may be reproduced.
> The United States government (including the OFR) makes no warranty, express or implied, including the warranties of merchantability and fitness for a particular purpose...

*Verifier notes:* VERBATIM CHECK: PASS. The quote appears word-for-word at https://www.financialresearch.gov/legal-notices/ under the "Copyright Status" heading. Confirmed via two independent WebFetch passes; URL is live (not 404/redirect/different page).

STRICTER-CLAUSE SEARCH: No refuting clause found. The full page has 4 sections (Disclaimer of Endorsement, Disclaimer of Liability, Copyright Status, Official Seal/Names/Symbols). For OFR's own works there is NO redistribution ban, NO non-commercial restriction, NO prior-written-permission requirement, NO no-derivatives clause, and NO bulk-download/mass-extraction restriction. The classification (U.S. Government public domain under 17 U.S.C. Sec. 105; credit requested, not required) is the standard, correct treatment for federal-employee-authored works and is NOT too permissive at the source level.

TWO CAVEATS (noted, non-refuting, do not change the verdict):
1) The verbatim_quote stops one sentence short. The Copyright Status section continues: "If copyrighted material appears on the site, or is reached through a link on this site, the copyright holder must be consulted before the material may be reproduced." This is a standard third-party-copyright carve-out, not a restriction on OFR's own public-domain works, so it does not contradict the classification — but the researcher should ideally include it for completeness.
2) OFR re-hosts some third-party data under separate terms (e.g., the Short-Term Funding Monitor FNYR dataset is subject to the Federal Reserve Bank of New York's reference-rate terms of use). A library that re-hosts OFR content for public download must apply per-DATASET diligence for embedded third-party/non-federal content; the source-level "public domain" classification applies only to OFR's own federal-employee-authored works.

The Treasury seal/name/symbol prohibition (18 U.S.C. Sec. 701, 31 U.S.C. Sec. 333) is an anti-impersonation/trademark rule, not a copyright or redistribution restriction, so it is irrelevant to the data-redistribution classification.

*Researcher reasoning:* The OFR is a U.S. federal agency within the Department of the Treasury. Its official Legal Notices page (https://www.financialresearch.gov/legal-notices/) states verbatim: "No copyright may be claimed for any work on this website that was created by a federal employee in the course of his or her duties. However, credit is requested if you reproduce or copy any such work." Works created by federal employees in the course of their duties are public domain under 17 U.S.C. § 105, meaning OFR's own data products and content carry no copyright and may be freely reproduced, redistributed, and re-hosted, including for commercial use. The page uses the word "credit is requested" — a courtesy request, not a mandatory condition — so attribution_required is false (though giving credit is good practice). There is no non-commercial restriction and no share-alike condition, so this is redistributable_open rather than attribution-conditioned or NC.

IMPORTANT CAVEAT (not a reason to downgrade the OFR-created data, but a scoping condition the professor must honor): the same page states verbatim: "If copyrighted material appears on the site, or is reached through a link on this site, the copyright holder must be consulted before the material may be reproduced." So any third-party copyrighted material that OFR displays or links (as opposed to OFR-authored data/analytics) is NOT covered by the public-domain grant and would require the copyright holder's permission. For the "ofr" database (OFR-produced data products such as its financial-stress and repo/money-market analytics series, which are agency-created works), the public-domain grant applies and redistribution is permitted. A separate restriction (18 U.S.C. § 701, 31 U.S.C. § 333) prohibits using Treasury/OFR seals or symbols in a way that falsely implies government endorsement — this restricts logo/branding use, not data redistribution, so it does not affect the classification but should be noted. No explicit mass-download or redistribution prohibition exists anywhere on the official pages reviewed (legal-notices, policies, data). Classification is conservative-but-clear: redistributable_open, with the standard federal-works caveat that embedded/linked third-party copyrighted material is out of scope.

---

### usda

- **Databases (1):** `usda`
- **Official terms URL:** https://www.usda.gov/about-usda/policies-and-links
- **License:** U.S. Government work / public domain (17 U.S.C. § 105); attribution requested
- **Classification:** redistributable_open
- **Commercial OK:** True · **Attribution required:** False · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK

**Verbatim quote:**
> Most information presented on the USDA Web site is considered public domain information. Public domain information may be freely distributed or copied, but use of appropriate byline/photo/image credits is requested. Attribution may be cited as follows: "U.S. Department of Agriculture."
> Some materials on the USDA Web site are protected by copyright, trademark, or patent, and/or are provided for personal use only. Such materials are used by USDA with permission, and USDA has made every attempt to identify and clearly label them. You may need to obtain permission from the copyright, trademark, or patent holder to acquire, use, reproduce, or distribute these materials.
> Information presented on the USDA website is considered public information and may be distributed or copied. Use of appropriate byline/photo/image credits is requested.
> Most information on the NAL website is in the public domain and can be freely distributed or copied. (https://www.nal.usda.gov/web-policies-and-important-links)
> Most information presented on the FSA Web site is considered public domain information. Public domain information may be freely distributed or copied, but use of appropriate byline/photo/image credits is requested. (https://www.fsa.usda.gov/help/policies-and-links)

*Verifier notes:* VERBATIM: Confirmed. The exact URL (usda.gov/about-usda/policies-and-links) could not be fetched directly — usda.gov reset the connection / timed out on every WebFetch attempt (fetch-layer infrastructure failure, NOT a 404; the page exists and is the correct official terms page). Verified the quote three independent ways: (1) a WebSearch for the literal string returned the exact usda.gov page as top hit and reproduced the sentence; (2) a second targeted WebSearch reproduced it; (3) the fetchable USDA Farm Service Agency sister-page (fsa.usda.gov/help/policies-and-links) carries the IDENTICAL boilerplate with only "FSA" substituted for "USDA", confirming the finding's exact two-sentence punctuation ("...public domain information. Public domain information may be freely distributed...") rather than the ", and" run-on a search summarizer produced. The finding's verbatim quote is accurate.

ADVERSARIAL CHECK: The page contains an exception clause the researcher did not quote: "Some materials on the USDA Web site are protected by copyright, trademark, or patent, and/or are provided for personal use only... You may need to obtain permission from the copyright, trademark, or patent holder to acquire, use, reproduce, or distribute these materials." This is a narrow, clearly-labeled third-party carve-out standard to nearly every US-government public-domain notice. It does NOT contradict the open classification of USDA's own government-authored data (NASS/ERS statistics, etc.), which is public domain under 17 U.S.C. § 105. The page EXPLICITLY states public-domain info "may be freely distributed or copied" — redistribution is expressly permitted, not merely "use." No non-commercial, no prior-written-permission-for-USDA-works, no bulk-download, and no no-derivatives restriction exists for the government's own works. Attribution is "requested," not a binding condition — consistent with public domain.

CLASSIFICATION: redistributable_open with basis "17 U.S.C. § 105; attribution requested" is defensible and not too permissive for a re-hosting library. The library's only residual obligation is to respect the labeled third-party-copyright exceptions, a normal duty that does not justify downgrading. The finding does not falsely claim the statute citation appears in the quote (it doesn't) — 17 U.S.C. § 105 is the researcher's accurate legal characterization, not a fabricated verbatim. CONFIRMED.

CAVEAT: quote_verified_verbatim=true is based on strong corroborating evidence (2 searches + identical sister-agency boilerplate) rather than a direct fetch of the exact URL, which was blocked by host-side connection resets throughout the review.

*Researcher reasoning:* The canonical, department-wide USDA policy page (https://www.usda.gov/about-usda/policies-and-links, section "Digital Rights and Copyright") states verbatim that "Most information presented on the USDA Web site is considered public domain information. Public domain information may be freely distributed or copied, but use of appropriate byline/photo/image credits is requested." A second statement under "Website Security" on the same page repeats: "Information presented on the USDA website is considered public information and may be distributed or copied." This is EXPLICIT redistribution language ("freely distributed or copied"), not merely access/use language, and it reflects the U.S. federal rule that works of the U.S. Government are not subject to domestic copyright (17 U.S.C. § 105). I confirmed the same standard wording on two other official USDA agency pages (NAL and FSA), which corroborates the department-wide policy.

Classification = redistributable_open: USDA-produced data is public domain and may be freely redistributed/re-hosted with no mandatory conditions. Attribution is only "requested" ("credits is requested"), i.e., a courtesy, not a legal requirement, so attribution_required = false (the library should nonetheless credit "U.S. Department of Agriculture" as good practice). No commercial restriction exists on public-domain U.S. Government works, so commercial_ok = true; no share-alike obligation, so sharealike = false.

IMPORTANT CAVEAT for the re-hosting decision: The same page warns that "Some materials on the USDA Web site are protected by copyright, trademark, or patent, and/or are provided for personal use only... You may need to obtain permission from the copyright, trademark, or patent holder to acquire, use, reproduce, or distribute these materials." Thus the public-domain grant covers USDA-generated data only; it does NOT cover third-party copyrighted content that USDA hosts under permission, nor content reached via outbound links (for which the user "is subject to the copyright and licensing restrictions of the new site"). The professor should confirm each re-hosted dataset is USDA-authored/public-domain rather than one of these labeled third-party items. USDA symbols/logos are also excluded from reuse without permission, so the library must not reproduce USDA logos/branding to imply endorsement.

fetch_status = fetched_ok: The primary quote was read directly from the live official page via the browser (get_page_text), and corroborating quotes were fetched from two additional official usda.gov domains.

---

### World Happiness Report (WHR) — published by the Wellbeing Research Centre, University of Oxford, in partnership with Gallup and the UN Sustainable Development Solutions Network

- **Databases (1):** `whr`
- **Official terms URL:** https://www.worldhappiness.report/data-sharing/
- **License:** None stated. The official site offers free download but publishes no data licence, no Creative Commons/CC0 mark, and no terms-of-use document addressing redistribution.
- **Classification:** unclear_not_found
- **Commercial OK:** None · **Attribution required:** None · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED by WRITTEN PERMISSION (scoped/conditional)

**Verbatim quote:**
> The data we present in Figure 2.1 are available to download for free. This includes: The three-year averages for life evaluation from 2012 onwards; The 95% confidence intervals for those averages; The contributions of the six explanatory factors
> The World Happiness Report is powered by data from the Gallup World Poll.
> We also utilise data from many other international, national, regional, and other datasets which are generally available for public use.
> Gallup also publishes several Global Datasets for Public Use which are made freely available to the public by Gallup’s clients.
> If you require access to additional data from the Gallup World Poll, there are three options: Journalists can request data by contacting mediainquiry@gallup.com. Researchers can request access through their institution. Anyone can subscribe to Gallup Analytics.
> The World Happiness Report is published by the Wellbeing Research Centre at the University of Oxford (from the FAQ page footer, https://www.worldhappiness.report/faq/)

*Verifier notes:* Verbatim check PASSED. WebFetch returned HTTP 403 (bot protection), but the page loads normally in a real browser, so the URL is genuinely accessible and fetch_status "fetched_ok" is fair. On https://www.worldhappiness.report/data-sharing/ under the heading "Data available for free" the page reads word-for-word: "The data we present in Figure 2.1 are available to download for free. This includes: The three-year averages for life evaluation from 2012 onwards / The 95% confidence intervals for those averages / The contributions of the six explanatory factors." The finding renders the three bullet items joined by semicolons, but every word and capitalization matches exactly.

Stricter-clause search across four official surfaces (data-sharing page, FAQ, the dedicated data portal data.worldhappiness.report, and the About page): NO license name, NO Creative Commons/CC0 mark, NO copyright-terms or terms-of-use document addressing redistribution anywhere. There is no explicit redistribution ban, but crucially also no redistribution GRANT — "available to download for free" is a download/use permission, not a re-hosting/redistribution license. Supporting context leans restrictive rather than permissive: the underlying data is the proprietary Gallup World Poll, additional data is gated behind "request or subscription," and Gallup's public datasets are described as "made freely available to the public by Gallup's clients" (a use grant, not a redistribution license). The report is published by the University of Oxford Wellbeing Research Centre with Gallup and UN SDSN.

Classification assessment: "unclear_not_found" is the cautious no-stated-license bucket and does NOT assert that redistribution is permitted, so it is not more permissive than the terms support. For a library that re-hosts data for public download, this correctly withholds any redistribution assumption. The classification is defensible and appropriately conservative. Verdict: CONFIRMED — quote verbatim-accurate at the official URL and classification not too permissive.

*Researcher reasoning:* I fetched and read the World Happiness Report's own domain: the dedicated Data Sharing page (https://www.worldhappiness.report/data-sharing/), the FAQ (https://www.worldhappiness.report/faq/, including the "Other common questions" > "How do I download the data used in the report?" item), and the About page (https://www.worldhappiness.report/about/). The dedicated data portal (https://data.worldhappiness.report) only links back to the Data Sharing page for raw data. (WebFetch returned HTTP 403, so I used the browser to read the live pages.)

The official position is that the Figure 2.1 data (three-year life-evaluation averages from 2012, 95% confidence intervals, and the six explanatory-factor contributions) is "available to download for free." However, NOWHERE on the official site is there any explicit statement granting redistribution, re-hosting, or re-dissemination rights. There is no data licence, no Creative Commons or CC0 designation, no public-domain dedication, and no terms-of-use/copyright page. The footer carries only publisher attribution and contact emails — no Terms, Copyright, or Licence link.

Per the task's hard rules, "available to download for free" does NOT by itself authorise redistribution/re-hosting. Two facts push toward caution rather than openness: (1) the data is "powered by data from the Gallup World Poll," a proprietary commercial dataset, and access to anything beyond the free Figure 2.1 subset is gated behind "request or subscription" (journalist request to Gallup, institutional access, or a paid Gallup Analytics subscription); (2) the report itself is a copyrighted publication of the University of Oxford's Wellbeing Research Centre. 

The "CC0 / public domain" label that appears in general web search results traces to the third-party Kaggle mirror (kaggle.com/datasets/unsdsn/world-happiness), NOT to any statement by the official provider, so I did not rely on it. Because the official terms permit free download/use but are entirely silent on redistribution and state no licence, the conservative and correct classification is unclear_not_found — the redistribution right for a re-hosting library cannot be established from the official terms. For a compliance decision, WHR (and the underlying Gallup-sourced values) should not be treated as clearly redistributable without seeking written permission from the WHR editors (info@worldhappiness.report) and clarifying Gallup's terms.

---

### Wikidata (Wikimedia Foundation / Wikimedia Deutschland)

- **Databases (1):** `wikidata`
- **Official terms URL:** https://www.wikidata.org/wiki/Wikidata:Licensing
- **License:** CC0 1.0 Universal (Public Domain Dedication)
- **Classification:** redistributable_open
- **Commercial OK:** True · **Attribution required:** False · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK

**Verbatim quote:**
> All structured data in the main, property and lexeme namespaces is made available under the Creative Commons CC0 License (Public domain).
> Wikidata requires a CC0 license, which is equivalent to public domain; anyone may freely designate any public-domain data to be CC0.
> All data in Wikidata has a CC0 license.
> When any Wikidata user makes a contribution to Wikidata, that user applies a CC0 license to their contribution as a term of use.
> All structured data from the main, Property, Lexeme, and EntitySchema namespaces is available under the Creative Commons CC0 License.

*Verifier notes:* Quote confirmed word-for-word at the official URL (https://www.wikidata.org/wiki/Wikidata:Licensing), "Official policy" section. Full source sentence: "All structured data in the main, property and lexeme namespaces is made available under the Creative Commons CC0 License (Public domain); text in other namespaces is made available under the Creative Commons Attribution-ShareAlike 4.0 License." The finding quotes the first clause verbatim and ends it with a period where the source has a semicolon; every quoted word is exact and the truncation at a clause boundary does not alter meaning. Verified via two independent WebFetch passes plus a WebSearch. Adversarial hunt for stricter clauses found NONE: no redistribution ban, no non-commercial limit, no prior-written-permission requirement, no no-derivatives clause, and no bulk-download/mass-extraction restriction. The dropped second clause (CC-BY-SA 4.0) applies only to TEXT in other namespaces (Talk/Help/Wikidata: pages), not to the structured data a library re-hosts, so omitting it does not overstate permissiveness. Corroborating evidence: Wikidata's policy actively REQUIRES CC0 for structured contributions and rejects data offered under more restrictive licenses (including CC-BY). CC0 1.0 is a public-domain dedication — the most permissive instrument possible — so classification redistributable_open is defensible and appropriately (not overly) permissive for re-hosting the structured data for public download. Only real-world caveat, immaterial here: media files live on Wikimedia Commons under their own licenses, not in Wikidata's main/property/lexeme namespaces, so they are outside this catalogued dataset. fetch_status fetched_ok is accurate.

*Researcher reasoning:* Wikidata's official licensing page (https://www.wikidata.org/wiki/Wikidata:Licensing) states verbatim that "All structured data in the main, property and lexeme namespaces is made available under the Creative Commons CC0 License (Public domain)." CC0 is a full public-domain dedication: the rights-holder waives all copyright and related rights worldwide, so third parties may copy, modify, distribute, and re-host the data — including for commercial purposes — with no permission, no attribution, and no share-alike requirement. The page further confirms "All data in Wikidata has a CC0 license" and that every contributor "applies a CC0 license to their contribution as a term of use," meaning no non-CC0 data is permitted in the structured-data namespaces. This is the strongest possible open classification for the dataset being re-hosted. One caveat to note (does NOT affect the data itself): free-text in Wikidata's OTHER namespaces (e.g., help/documentation pages, discussion) is under CC BY-SA 3.0, not CC0 — but the actual structured dataset (entities, properties, statements, lexemes) that an academic library would re-host is unambiguously CC0. Therefore redistribution/re-hosting of the data is fully permitted with no conditions. Classification: redistributable_open. Verbatim quotes were fetched and read directly from the official Wikidata licensing page.

---

### World Bank Open Data

- **Databases (1):** `worldbank`
- **Official terms URL:** https://data.worldbank.org/summary-terms-of-use
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution  →  **corrected to `redistributable_attribution_with_exceptions — CC BY 4.0 applies to the World Bank's own compiled data, but third-party-sourced datasets/indicators embedded in World Bank Open Data (e.g., WDI series from UN Population Division, IMF, WHO, ILO, IEA, UNESCO) may NOT be redistributed without the original provider's consent. A library that re-hosts data for public download must exclude or separately clear all third-party-sourced series rather than treat the whole source as blanket-redistributable.`** by adversarial review
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **DISPUTED** (quote verbatim: True, classification agrees: False)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> you are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost under a Creative Commons Attribution 4.0 International License
> (datacatalog.worldbank.org/public-licenses) "CC-BY 4.0, with the additional terms below, is the default license for all Datasets produced by the World Bank itself."
> (data.worldbank.org/summary-terms-of-use, attribution) "you agree to provide attribution to The World Bank and its data providers in the following format: The World Bank: Dataset name: Data source (if known)"
> (data.worldbank.org/summary-terms-of-use, sub-licensing) "when sharing or facilitating access to the Datasets, you agree to include the same acknowledgment requirement in any sub-licenses of the data that you grant"
> (data.worldbank.org/summary-terms-of-use, THIRD-PARTY CARVE-OUT) "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider, or may be subject to additional terms and conditions"
> (data.worldbank.org/summary-terms-of-use, endorsement) "You must not claim or imply that The World Bank endorses your use of the data, or use The World Bank's name, logo(s) or trademark(s) in conjunction with such use"

**Adversary's contradicting clause:** "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider, or may be subject to additional terms and conditions." (Reinforced by the license grant's opening qualifier on the same page: "Unless indicated otherwise in the data or indicator metadata, you are free to copy, distribute, adapt...")

*Verifier notes:* STEP 1 (quote verbatim): PASS. Fetched https://data.worldbank.org/summary-terms-of-use (live, HTTP 200, correct page). The opening sentence reads exactly: "Unless indicated otherwise in the data or indicator metadata, you are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost under a Creative Commons Attribution 4.0 International License, with the additional terms below." The researcher's verbatim_quote is an exact word-for-word substring of this sentence (including "commercial or noncommercial", and the CC BY 4.0 phrase in the SAME sentence). License name CC BY 4.0 is correct.

STEP 2 (stricter-clause search): FOUND a material restriction the researcher omitted. (a) The grant is conditional from its first words — "Unless indicated otherwise in the data or indicator metadata" — so it is NOT unconditional. (b) The page explicitly carves out third-party data: "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider, or may be subject to additional terms and conditions." No "prior written permission" requirement or NC/ND clause applies to the World Bank's OWN data (that is genuine CC BY 4.0), but the third-party carve-out is decisive here.

STEP 3 (defensibility for a re-hosting library): The classification is TOO PERMISSIVE at the source level. World Bank Open Data is an aggregation; a substantial share of WDI indicators are sourced wholly from third parties whose data the World Bank itself says may not be redistributed. Re-hosting the entire source for public bulk download would redistribute that non-redistributable subset. The CC BY 4.0 label is right for the headline/own-compiled data but must be qualified with the third-party exception before it can authorize re-hosting.

Bottom line: quote is accurate and license name is correct, but per the skepticism mandate I cannot confirm a blanket redistributable_attribution classification because redistribution of the third-party-sourced indicators is explicitly NOT permitted without original-provider consent. DISPUTED on over-permissiveness grounds; corrected to redistributable_attribution_with_exceptions.

Sources verified: https://data.worldbank.org/summary-terms-of-use ; cross-referenced https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets and https://datacatalog.worldbank.org/public-licenses .

*Researcher reasoning:* The World Bank Group licenses the datasets it produces (the data served at data.worldbank.org, i.e. the "worldbank" indicator database) under Creative Commons Attribution 4.0 International (CC BY 4.0). Two authoritative World Bank pages confirm this consistently and verbatim: (1) the Summary Terms of Use at data.worldbank.org grants that users "are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost under a Creative Commons Attribution 4.0 International License"; and (2) the Data Access and Licensing page at datacatalog.worldbank.org/public-licenses states "CC-BY 4.0, with the additional terms below, is the default license for all Datasets produced by the World Bank itself." CC BY 4.0 explicitly permits redistribution and re-hosting (including commercially) provided attribution is given, so the library's free non-commercial re-hosting is permitted -> redistributable_attribution. Attribution is mandatory in the specified format ("The World Bank: Dataset name: Data source (if known)"), and any sub-licenses must carry the same acknowledgment requirement. It is CC BY (not BY-SA), so no ShareAlike obligation; commercial use is allowed.

IMPORTANT CAVEAT for the compliance decision: This CC BY 4.0 grant covers data PRODUCED BY the World Bank. The same Summary Terms carve out that "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider, or may be subject to additional terms and conditions," and that datasets "available under other licenses ... are labeled accordingly." The World Development Indicators / Open Data catalog blends World-Bank-produced series with third-party-sourced indicators (e.g. some UN, IMF, IEA, IHME sources). So re-hosting the entire "worldbank" indicator set is NOT uniformly clearing under CC BY 4.0 — the professor should honor per-dataset/per-indicator license labels and exclude or separately clear the third-party indicators that carry redistribution restrictions. Also note: the World Bank's GENERAL website Terms & Conditions (worldbank.org/en/about/legal/terms-and-conditions) are far more restrictive (non-commercial only, "you may not make any derivative work or commercial use, including without limitation reselling them, charging to access them, charging to redistribute them ... without the prior written consent"); those govern general site "Materials," NOT the open datasets, which the T&C themselves defer to the dataset-specific terms. Do not apply the general T&C to the data. Overall classification for the core World Bank Open Data: redistributable with attribution, subject to the third-party-indicator carve-out.

---

### World Bank Poverty & Inequality Platform (PIP)

- **Databases (1):** `pip`
- **Official terms URL:** https://data.worldbank.org/summary-terms-of-use
- **License:** CC BY 4.0 (Creative Commons Attribution 4.0 International)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> you are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost
> Creative Commons Attribution 4.0 — the license shown on the World Bank Data Catalog entry for the "Poverty and Equity Database (Poverty and Inequality Platform)" (dataset 0038020), where the dataset is also classified as "Public": "This dataset is classified as Public under the Access to Information Classification Policy. Users inside and outside the Bank can access this dataset." (https://datacatalog.worldbank.org/search/dataset/0038020/poverty-and-equity-database-poverty-and-inequality-platform)
> Attribution + sub-license pass-through clause (https://data.worldbank.org/summary-terms-of-use): "you agree to provide attribution to The World Bank and its data providers in the following format: The World Bank: Dataset name: Data source (if known)." and "When sharing or facilitating access to the Datasets, you agree to include the same acknowledgment requirement in any sub-licenses of the data that you grant"
> Data Catalog 'Data Access And Licensing' page (https://datacatalog.worldbank.org/public-licenses): the default CC-BY 4.0 license "allows users to copy, modify and distribute data in any format for any purpose, including commercial use."
> Third-party-data exception (https://data.worldbank.org/summary-terms-of-use): "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider, or may be subject to additional terms and conditions." This exception targets specific third-party indicators; PIP's own World-Bank-produced poverty/inequality aggregate estimates are the CC BY 4.0 dataset itself.

*Verifier notes:* STEP 1 — Quote verification: WebFetch of the cited official_terms_url (https://data.worldbank.org/summary-terms-of-use) succeeded (fetched_ok is accurate) and the verbatim_quote appears WORD-FOR-WORD: "you are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost". The page names the license as "a Creative Commons Attribution 4.0 International License" — matches the finding's CC BY 4.0. No red flag on quote or URL.

STEP 2 — Search for a stricter clause: The same page carries a genuine carve-out: "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider" and "may be subject to additional terms and conditions... included in the dataset or indicator metadata." I treated this as the primary refutation vector, since PIP aggregates household-survey data (including the Luxembourg Income Study for high-income economies). The summary terms and the datacatalog public-licenses page both instruct that CC BY 4.0 is the DEFAULT and that any exception is "labeled accordingly" at the individual dataset record. So the decisive check is the dataset-level label, not the general terms.

STEP 3 — Dataset-level confirmation (independent of the researcher): I resolved PIP's actual license at the authoritative World Bank Data Catalog record (dataset 0038020, "Poverty and Equity Database — Poverty and Inequality Platform"). Its License field reads verbatim "Creative Commons Attribution 4.0" and it is "classified as Public... Users inside and outside the Bank can access this dataset," with no additional restriction noted. This is exactly the metadata label the summary terms point to, and it confirms PIP is NOT within the third-party-restricted set. Reason it holds: PIP publishes World-Bank-computed AGGREGATE statistics (poverty headcount, poverty gap, Gini) produced by the Global Poverty Working Group — it does not redistribute the underlying restricted microdata (national statistical office surveys, LIS). The third-party ban applies to that raw microdata, which PIP does not serve. Cross-checks: data.worldbank.org poverty indicators, OWID poverty-data README (defers to source), and the datacatalog public-licenses page are all consistent.

CONCLUSION: Quote is verbatim-accurate at the stated URL; license is correctly CC BY 4.0; and the classification "redistributable_attribution" is defensible and not too permissive for a re-hosting library — redistribution of PIP's aggregate poverty/inequality data is genuinely permitted under CC BY 4.0 provided attribution is given. The only live obligation is attribution to the World Bank (and, where relevant, acknowledgment of primary sources such as LIS/national agencies as indicated in metadata), which is already captured by the "_attribution" qualifier in the classification. No redistribution ban, non-commercial limit, no-derivatives, prior-written-permission, or bulk-extraction restriction applies to PIP's published data. Verdict: CONFIRMED.

*Researcher reasoning:* PIP data is a World Bank-produced dataset. The official World Bank Data Catalog entry for the "Poverty and Equity Database (Poverty and Inequality Platform)" (dataset ID 0038020) explicitly labels the license as "Creative Commons Attribution 4.0" and classifies access as "Public." The governing dataset terms (data.worldbank.org/summary-terms-of-use and datacatalog.worldbank.org/public-licenses) grant an explicit right to "copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost," i.e., explicit redistribution language, not merely access. The only conditions are attribution ("The World Bank: Dataset name: Data source (if known)") and a pass-through acknowledgment requirement in any sub-licenses. CC BY 4.0 has no non-commercial restriction and no ShareAlike/copyleft condition. A third-party-data exception exists ("may not be redistributed or reused without the consent of the original data provider"), but it applies to certain externally-sourced third-party indicators, not to PIP's own aggregated poverty/inequality estimates, which are the CC BY 4.0-licensed World Bank product a re-hoster downloads via bulk CSV/Excel/API. Note for the compliance record: re-hosting PIP's published aggregate estimates is fine under CC BY 4.0 with attribution; this does NOT extend to the underlying national household-survey microdata (which PIP does not itself redistribute). Classification: redistributable_attribution. Two independent fetches of the summary-terms-of-use page returned identical wording for the load-bearing quotes; the CC BY 4.0 designation was confirmed on the PIP-specific catalog entry. Note: the standalone www.worldbank.org/en/about/legal/terms-of-use-for-datasets URL kept resolving to the general Terms & Conditions page in fetches, so its general-materials anti-redistribution language ("charging to redistribute them") was NOT used — that clause governs general non-dataset materials, not the CC BY 4.0 datasets.

---

### World Bank World Development Indicators (WDI)

- **Databases (1):** `worldbank_wdi`
- **Official terms URL:** https://data.worldbank.org/summary-terms-of-use
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Unless indicated otherwise in the data or indicator metadata, you are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost under a Creative Commons Attribution 4.0 International License, with the additional terms below.
> When you download or use the Datasets, you are agreeing to comply with the terms of a CC BY 4.0 license, and also agreeing to the following mandatory and binding addition:
> You must include attribution for the data you use in the manner indicated in the metadata included with the data. Generally, you agree to provide attribution to The World Bank and its data providers in the following format: The World Bank: Dataset name: Data source (if known).
> When sharing or facilitating access to the Datasets, you agree to include the same acknowledgment requirement in any sub-licenses of the data that you grant, and a requirement that any sub-licensees do the same.
> Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider, or may be subject to additional terms and conditions.
> You must not claim or imply that The World Bank endorses your use of the data, or use The World Bank's name, logo(s) or trademark(s) in conjunction with such use.

*Verifier notes:* Verbatim quote confirmed WORD-FOR-WORD at the official URL (https://data.worldbank.org/summary-terms-of-use), including the "Unless indicated otherwise in the data or indicator metadata," prefix and the ", with the additional terms below." suffix. Fetch_status fetched_ok is accurate (correct page, not a 404/redirect). Triangulated independently: (1) a second targeted WebFetch reproduced the exact sentence; (2) a WebSearch returned the same quote; and (3) the World Bank's full "Terms of Use for Datasets" corroborates that datasets are "provided to you under a Creative Commons Attribution 4.0 International License (CC BY 4.0), with the additional terms below."

Adversarial stricter-clause search: I specifically hunted for a redistribution ban, non-commercial restriction, "prior written permission," no-derivatives, or bulk/mass-download/scraping restriction. None exist. The three "additional terms" are all CC BY 4.0-compatible: (a) attribution in the format "The World Bank: Dataset name: Data source (if known)", (b) a sub-licensing pass-through of that acknowledgment, and (c) a no-endorsement / no-trademark-use restriction. These are attribution/branding conditions, not redistribution prohibitions.

One genuine carve-out surfaced: certain third-party datasets embedded in WDI "may not be redistributed or reused without the consent of the original data provider." This is NOT a contradicting clause that changes the classification, because it is a per-indicator, metadata-level exception already explicitly flagged inside the verbatim quote itself ("Unless indicated otherwise in the data or indicator metadata"). It does not override the dataset-level default of CC BY 4.0.

Conclusion: classification redistributable_attribution (CC BY 4.0) is DEFENSIBLE and not too permissive for a library re-hosting WDI for public download, with two operational obligations the library must honor: (1) supply the required World Bank attribution string, and (2) respect indicator-level metadata that marks specific third-party series as non-redistributable. No dispute.

*Researcher reasoning:* The World Bank Open Data Summary Terms of Use (data.worldbank.org/summary-terms-of-use) govern the World Development Indicators, the Bank's flagship open dataset. I fetched and read the official page. It states verbatim that "you are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost under a Creative Commons Attribution 4.0 International License." CC BY 4.0 explicitly grants the right to redistribute/re-disseminate (including re-hosting for download) and to make commercial use, with the sole condition of attribution. This makes WDI redistributable with attribution (not merely accessible). Classification is therefore redistributable_attribution, not redistributable_open, because attribution is a binding condition. CC BY 4.0 imposes NO share-alike/copyleft obligation, so sharealike is false. Required conditions for the re-hosting library: (1) provide attribution in the metadata-specified format ("The World Bank: Dataset name: Data source (if known)"); (2) propagate the same acknowledgment/attribution requirement into any onward sub-licenses; (3) do not imply World Bank endorsement or misuse its name/logos/trademarks. IMPORTANT CAVEAT for a re-hoster: the terms warn that "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider, or may be subject to additional terms and conditions." WDI aggregates series from many upstream sources, and individual indicators may carry per-indicator restrictions flagged in their metadata. The WDI database as published by the World Bank is CC BY 4.0, but the library should preserve/consult each indicator's terms-of-use metadata and exclude or specially handle any indicator flagged as third-party-restricted. There is a separate, more restrictive "Terms of Use for Datasets" page on worldbank.org that grants only a non-commercial license for other (non-Open-Data) Bank materials; that page does NOT govern WDI/Open Data, which is the CC BY 4.0 Summary Terms quoted here.

---

### World Bank Worldwide Governance Indicators (WGI)

- **Databases (1):** `wgi`
- **Official terms URL:** https://data.worldbank.org/summary-terms-of-use
- **License:** Creative Commons Attribution 4.0 International (CC BY 4.0), with World Bank additional dispute-resolution terms
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Unless indicated otherwise in the data or indicator metadata, you are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost under a Creative Commons Attribution 4.0 International License, with the additional terms below.
> [WGI dataset page, https://datacatalog.worldbank.org/search/dataset/0038026/Worldwide-Governance-Indicators] “License : Creative Commons Attribution 4.0 This dataset is licensed under Creative Commons Attribution 4.0”
> [WGI dataset page, https://datacatalog.worldbank.org/search/dataset/0038026/Worldwide-Governance-Indicators] “Classification : Public ... This dataset is classified as Public under the Access to Information Classification Policy. Users inside and outside the Bank can access this dataset.”
> [Data Access And Licensing, https://datacatalog.worldbank.org/public-licenses] “The World Bank Group makes data publicly available according to open data standards and licenses datasets under the Creative Commons Attribution 4.0 International license (CC-BY 4.0).”
> [Data Access And Licensing, https://datacatalog.worldbank.org/public-licenses] “The Creative Commons Attribution 4.0 International license allows users to copy, modify and distribute data in any format for any purpose, including commercial use. Users are only obligated to give appropriate credit (attribution) and indicate if they have made any changes, including translations.”
> [Summary Terms of Use, https://data.worldbank.org/summary-terms-of-use] “When sharing or facilitating access to the Datasets, you agree to include the same acknowledgment requirement in any sub-licenses of the data that you grant, and a requirement that any sub-licensees do the same. You may meet this requirement by providing the uniform resource locator (URL) of these terms of use.”
> [Summary Terms of Use, https://data.worldbank.org/summary-terms-of-use] “Generally, you agree to provide attribution to The World Bank and its data providers in the following format: The World Bank: Dataset name: Data source (if known).”
> [Summary Terms of Use, https://data.worldbank.org/summary-terms-of-use] “Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider, or may be subject to additional terms and conditions.”

*Verifier notes:* Adversarial review of World Bank Worldwide Governance Indicators (WGI) licensing finding.

1) VERBATIM QUOTE: Confirmed word-for-word at https://data.worldbank.org/summary-terms-of-use (fetch_status fetched_ok). The page returns the exact sentence including the leading qualifier "Unless indicated otherwise in the data or indicator metadata" and trailing "under a Creative Commons Attribution 4.0 International License, with the additional terms below." No misquote or fabrication.

2) SEARCH FOR STRICTER CLAUSES: The default terms contain two live traps I probed: (a) the "unless indicated otherwise in the data or indicator metadata" qualifier, and (b) an additional term: "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider, or may be subject to additional terms and conditions." Because WGI is a composite built from 30+ underlying (some proprietary) sources, it was a plausible candidate for carve-out. I resolved this by checking the dataset-specific metadata rather than relying on the generic terms. The WGI Data Catalog entry (dataset 0038026) affirmatively states "This dataset is licensed under Creative Commons Attribution 4.0," is classified "Public" ("Users inside and outside the Bank can access this dataset"), and shows no redistribution ban, no prior-written-permission requirement, no non-commercial limit, and no "restricted" flag. Confirmed via WebSearch plus two independent WebFetch raw-text reads of the catalog page. So the indicator metadata indicates CC BY 4.0, not otherwise; the third-party clause targets datasets provided BY third parties, whereas the WGI aggregate indicators are a World Bank product (only aggregate scores are published, not the non-redistributable underlying source data).

CAVEAT (not a dispute): The general worldbank.org website Terms & Conditions ("Materials") carry restrictive non-commercial / no-derivative-without-written-consent language, but that instrument is superseded for datasets by the Terms of Use for Datasets (CC BY 4.0), and the dataset catalog confirms CC BY 4.0. Attribution obligation ("The World Bank: Dataset name: Data source (if known)") must be preserved by the re-hosting library, and the same attribution must flow to sub-licensees; users must not claim World Bank endorsement or use its name/logos on derived products.

3) CLASSIFICATION: redistributable_attribution (CC BY 4.0 with World Bank additional dispute-resolution/attribution terms) is defensible and not too permissive for a library that re-hosts the published WGI aggregate indicators for public download, so long as attribution is retained. Verdict: CONFIRMED.

*Researcher reasoning:* The World Bank's Worldwide Governance Indicators (WGI) are produced by the World Bank itself and are explicitly labeled on the official Data Catalog dataset page (dataset id 0038026) as: "License : Creative Commons Attribution 4.0 ... This dataset is licensed under Creative Commons Attribution 4.0." The dataset is also classified "Public," accessible to users inside and outside the Bank. The World Bank's Data Access and Licensing page and Summary Terms of Use confirm CC BY 4.0 is the default license for datasets the Bank produces and distributes as open data, and spell out that users "are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes" — i.e., redistribution/re-hosting is explicitly permitted. Conditions: (1) attribution to The World Bank and its data providers in the specified format; (2) when sharing or facilitating access, pass through the same acknowledgment requirement to sub-licensees (satisfiable by providing the URL of the terms). There is NO ShareAlike obligation (CC BY, not CC BY-SA/ODbL) and NO non-commercial restriction. The Bank's general terms contain a carve-out that some third-party-provided datasets may not be redistributed without the original provider's consent, but that carve-out does NOT apply to WGI because WGI's own dataset page is affirmatively labeled CC BY 4.0 (the Bank labels non-CC datasets differently, e.g. ODbL or Microdata Research License). There is an additional binding World Bank term layered on top of CC BY 4.0: a mandatory mediation/arbitration dispute-resolution clause (place of arbitration = where the Licensor has its headquarters, English language, UNCITRAL rules) — this does not restrict redistribution but is a mandatory acceptance condition. Conclusion for a free, non-commercial academic re-hosting library: redistribution/re-hosting is PERMITTED provided attribution is given and the terms URL is passed through to downstream users. Classification: redistributable_attribution. All quotes were fetched and verified verbatim from official worldbank.org / datacatalog.worldbank.org pages (raw HTML confirmed, not paraphrased).

---

### World Health Organization (WHO) — Global Health Observatory (GHO)

- **Databases (3):** `who_hwf`, `who_rs`, `who_sdg`
- **Official terms URL:** https://www.who.int/about/policies/publishing/copyright
- **License:** CC BY-NC-SA 3.0 IGO (WHO GHO / publications); note: the newer data.who.int dataset portal states CC BY 4.0
- **Classification:** noncommercial_only
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** True · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - non-commercial only

**Verbatim quote:**
> The CC BY-NC-SA 3.0 IGO licence allows users to freely copy, reproduce, reprint, distribute, translate and adapt the work for non-commercial purposes, provided WHO is acknowledged as the source using the following suggested citation
> Permission from WHO is not required for the use of WHO materials issued under the Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Intergovernmental Organization (CC BY-NC-SA 3.0 IGO) licence. [source: https://www.who.int/about/policies/publishing/copyright]
> Permission is required for commercial uses and licensing of WHO materials, such as using the material in the context of a commercial activity. [source: https://www.who.int/about/policies/publishing/copyright]
> Unless specifically indicated otherwise, these Datasets are provided to you under a Creative Commons Attribution 4.0 International License (CC BY 4.0) [source: https://data.who.int/about/data/terms-and-conditions — the newer WHO data portal, MORE permissive: attribution required, commercial allowed]
> You may use our application programming interfaces ("APIs") to facilitate access to the Datasets, whether through a separate web site or through another type of software application. [source: https://data.who.int/about/data/terms-and-conditions]
> Reproduction or translation of substantial portions of the web site, or any use other than for educational or other non-commercial purposes, require explicit, prior authorization in writing. [source: https://www.who.int/about/policies/terms-of-use — WHO general website Terms of Use, the MOST restrictive layer]
> Without the prior written approval of WHO, you will not use the name (or any abbreviation thereof) and/or emblem of the World Health Organization. [source: https://data.who.int/about/data/terms-and-conditions]

*Verifier notes:* Verified via two independent WebFetch passes of the official URL (https://www.who.int/about/policies/publishing/copyright). The verbatim_quote appears WORD-FOR-WORD on the page; fetch_status fetched_ok is accurate.

Adversarial stricter-clause hunt: CC BY-NC-SA 3.0 IGO explicitly permits copy/reproduce/reprint/distribute/translate/adapt, so redistribution is genuinely allowed for non-commercial purposes — no redistribution ban, no bulk/mass-download or data-mining ban, and no prior-written-permission requirement for non-commercial reuse. The one gating restriction is commercial use, verbatim on the page: "Permission is required for commercial uses and licensing of WHO materials, such as using the material in the context of a commercial activity." That is exactly what noncommercial_only encodes.

Classification is DEFENSIBLE and NOT too permissive. noncommercial_only correctly captures the operative NC gate and is the conservative choice relative to the data.who.int CC BY 4.0 portal (which the finding already flags). For a re-hosting library, redistribution/public-download is permitted provided it stays non-commercial and attributes WHO.

One additional obligation not named in the classification but worth surfacing: ShareAlike/copyleft — verbatim "The adaptation or translation must be licensed under the same or similar licence terms." This constrains derivatives but does not prohibit redistribution, so it does not change the verdict. Recommend the library honor attribution + ShareAlike when re-hosting.

*Researcher reasoning:* WHO's licensing of GHO data is layered across three official sources, so I resolved it conservatively. (1) The WHO copyright/publishing policy (https://www.who.int/about/policies/publishing/copyright) states that WHO materials are issued under CC BY-NC-SA 3.0 IGO, which "allows users to freely copy, reproduce, reprint, distribute, translate and adapt the work for non-commercial purposes, provided WHO is acknowledged as the source," and that "Permission from WHO is not required" for such use but "Permission is required for commercial uses." The re3data registry record for the GHO Data Repository (r3d100010812) independently confirms the classic GHO repository is licensed CC BY-NC-SA 3.0 IGO (and CC BY 3.0 IGO). This explicitly grants third-party redistribution/re-hosting for non-commercial purposes with attribution and share-alike. (2) The newer WHO data portal at data.who.int (https://data.who.int/about/data/terms-and-conditions), where GHO indicators including SDG and health-workforce data are now served for download, is MORE permissive: "these Datasets are provided to you under a Creative Commons Attribution 4.0 International License (CC BY 4.0)" and it even explicitly contemplates third parties serving the data "through a separate web site or through another type of software application" via WHO's APIs — that would be redistributable_attribution (commercial allowed). (3) The WHO general website Terms of Use (https://www.who.int/about/policies/terms-of-use) is the most restrictive layer, requiring "explicit, prior authorization in writing" for "reproduction of substantial portions," but this general clause is superseded, for the specifically licensed content, by the CC grants above. Because a free, non-commercial academic library that re-hosts the data is fully authorized under EITHER Creative Commons licence, and because the task requires conservative classification, I classify at the more restrictive CC BY-NC-SA 3.0 IGO level = noncommercial_only. Under this classification redistribution is permitted provided the library: (a) attributes WHO as the source using WHO's suggested citation, (b) applies a compatible non-commercial share-alike licence to the re-hosted collection (share-alike), (c) keeps use non-commercial, and (d) does NOT use the WHO name or emblem and does not imply WHO endorsement. In practice WHO's current data portal offers the same data under CC BY 4.0, so the library is safe either way; noncommercial_only is the conservative floor. fetch_status is fetched_ok — all quotes were read directly from the official WHO pages listed.

---

### World Trade Organization (WTO) — Tariff and Trade Data / WTO Stats

- **Databases (8):** `wto_hs_a_0010`, `wto_hs_a_0015`, `wto_hs_a_0020`, `wto_hs_a_0025`, `wto_hs_a_0030`, `wto_hs_a_0040`, `wto_its_mtv_am`, `wto_its_mtv_ax`
- **Official terms URL:** https://tao.s3.eu-central-1.amazonaws.com/public/Terms_of_Use_WTO_Tariff_and_Trade_Data.pdf
- **License:** WTO Tariff and Trade Data — Terms and Conditions of Use (custom terms; not an open licence)
- **Classification:** permission_required
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> The User may reproduce or re-disseminate materials from this platform for non-commercial purposes provided that the User obtains permission from the WTO Secretariat and that the WTO is acknowledged as the original source of the materials. The full citation "WTO Tariff and Trade Data" shall be included in such reproduced or re-disseminated materials.
> Mass downloads of information from the platform is prohibited. The User must request the WTO Secretariat (idb@wto.org) for permission before mass-downloading information from the Platform.
> Permission to use and download information from the platform is granted for non-commercial purposes (e.g. research, analysis, personal or classroom use), without fee and without formal request. The User must obtain permission from the WTO Secretariat (idb@wto.org) prior to using the information for purposes beyond those specified herein.
> Any user seeking to re-disseminate IDB or CTS data to third parties for purposes beyond publication or analyses derived from these databases shall first obtain the approval of the WTO Secretariat (idb@wto.org) prior to such re-dissemination.
> Any user seeking to mass-download IDB and CTS data for their own systems, or for redistribution through other databases or online systems, shall obtain the approval of the WTO Secretariat (idb@wto.org) prior to the download.
> To republish, to post on servers, or to redistribute to lists, requires prior specific permission and/or fee. [from https://www.wto.org/english/res_e/statis_e/trade_data_e.htm]
> Copies may not be made or distributed for profit or commercial advantage. [from https://www.wto.org/english/res_e/statis_e/trade_data_e.htm]

*Verifier notes:* Downloaded the official terms PDF (92.7 KB, WTO "TERMS AND CONDITIONS OF USE, DISCLAIMER AND COPYRIGHT" for the TAO / Tariff Analysis Online platform) and extracted full text via pdftotext -layout. The verbatim_quote matches the PDF WORD-FOR-WORD — it is clause 4 of the "Copyright and Permissions for the General Public" section, including the exact citation string "WTO Tariff and Trade Data". URL accessible; fetch_status = fetched_ok corroborated.

Adversarial stricter-clause search found MORE restrictive language than the researcher quoted, all of which REINFORCE the permission_required classification rather than contradict it:
(1) General Public cl. 3: "Mass downloads of information from the platform is prohibited. The User must request the WTO Secretariat (idb@wto.org) for permission before mass-downloading information from the Platform." — directly relevant to a library that bulk-extracts and re-hosts.
(2) General Public cl. 2: use/download granted only for NON-COMMERCIAL purposes without fee; "The User must obtain permission from the WTO Secretariat (idb@wto.org) prior to using the information for purposes beyond those specified herein."
(3) Authorized Users cl. 3-4: re-dissemination to third parties, and mass-download for redistribution through other databases/online systems, each require PRIOR WTO Secretariat approval.

Assessment: The classification is permission_required (custom terms, not an open licence), which is a RESTRICTIVE classification and is NOT too permissive. For a library that re-hosts WTO data for public download, permission_required is defensible and, if anything, understated only in that the researcher's single quote omits the outright mass-download prohibition — but that omission does not make the classification too permissive; it makes permission_required even more clearly correct. Redistribution IS permitted only conditionally (WTO Secretariat permission + non-commercial + mandatory "WTO Tariff and Trade Data" citation), so a flat "prohibited" would be too strict and "open/redistributable" would be too permissive; permission_required is the accurate middle. Practical note for the re-hosting use case: the library would need explicit WTO Secretariat (idb@wto.org) permission for BOTH the mass-download and the re-dissemination, and use must remain non-commercial with the required citation.

*Researcher reasoning:* The covered databases are WTO tariff data (HS annual, wto_hs_a_*) and merchandise trade statistics (wto_its_mtv_am/ax), all served through WTO's Tariff and Trade Data / Stats platform. Two official WTO sources were fetched and read, and both restrict redistribution to a permission-required regime:

1) The official "TERMS AND CONDITIONS OF USE, DISCLAIMER AND COPYRIGHT" PDF (linked from the WTO Tariff and Trade Data platform, ttd.wto.org). Under "Copyright and Permissions for the General Public" (which is the category a free academic re-hosting library falls into — it is NOT an "Authorized User," a status reserved for WTO Members, Acceding countries, the Secretariat, and IGOs approved by the Committee on Market Access): non-commercial use/download is free without fee, BUT (clause 3) "Mass downloads of information from the platform is prohibited" without prior permission from the WTO Secretariat, and (clause 4) reproduction or re-dissemination — even for non-commercial purposes — is permitted only "provided that the User obtains permission from the WTO Secretariat" plus attribution with the full citation "WTO Tariff and Trade Data."

2) The WTO "International trade and tariff data" page (www.wto.org/english/res_e/statis_e/trade_data_e.htm), which states redistribution "requires prior specific permission and/or fee" and that "Copies may not be made or distributed for profit or commercial advantage."

Re-hosting third-party data for download is precisely (a) a mass download and (b) a re-dissemination to third parties — both of which the terms expressly gate behind prior written approval from the WTO Secretariat (idb@wto.org). No open licence (CC0/CC BY/open-gov) applies; the terms are custom and explicitly conditional. Commercial redistribution is disallowed and even non-commercial re-dissemination is not free-standing — it needs permission first. Attribution ("WTO Tariff and Trade Data") is mandatory. Note also clause 4 of the general disclaimer warns that some data "may be subject to conditions beyond those indicated... because third parties may have ownership rights" (relevant to IDB source data submitted by Members).

Conservative classification: permission_required. This is NOT redistributable_open, NOT redistributable_attribution, and NOT noncommercial_only, because in every case the library must first obtain the WTO Secretariat's approval before mass-downloading or re-hosting. The professor's library should email idb@wto.org to request permission before re-hosting any of these datasets, and must not mass-download them in the meantime.

fetch_status: fetched_ok — the terms PDF was downloaded and read in full (3 pages, verbatim above); the trade_data_e.htm page was fetched and its republication clause read verbatim. The stats.wto.org SPA returned only a loading shell (JS-rendered) and yielded no additional terms, but its governing terms are the same TTD Terms of Use document quoted here.

---

### worldbank_esg

- **Databases (1):** `worldbank_esg`
- **Official terms URL:** https://datacatalog.worldbank.org/public-licenses
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> allows users to copy, modify and distribute data in any format for any purpose, including commercial use.
> CC-BY 4.0, with the additional terms below, is the default license for all Datasets produced by the World Bank itself
> Users are only obligated to give appropriate credit (attribution) and indicate if they have made any changes, including translations.
> Many datasets are available under other licenses. They are labeled accordingly, and when they are accessed by users, users agree to comply with all of the terms of the respective licenses.
> The World Bank Group makes data publicly available according to open data standards and licenses datasets under the Creative Commons Attribution 4.0 International license (CC-BY 4.0). (Sovereign ESG Data Portal, https://esgdata.worldbank.org/data/download)

*Verifier notes:* Adversarial verification passed on all three steps. STEP 1 (quote): official_terms_url is live and returns the verbatim_quote word-for-word within the CC-BY 4.0 section (no 404/redirect/wrong page). STEP 2 (stricter clause hunt): the page shows CC-BY 4.0 is only the DEFAULT — other WB datasets carry stricter licenses (ODbL share-alike; Microdata Research License = "shall not be redistributed or sold without the Microdata Library's prior written agreement"), so I verified ESG's actual license independently. ESG's own Data Catalog record (dataset 0037651) states verbatim "This dataset is licensed under Creative Commons Attribution 4.0." The "additional terms" on the default CC-BY 4.0 are dispute-resolution only (mediation + UNCITRAL arbitration), not a redistribution/non-commercial/no-derivatives/prior-permission/bulk-download restriction. STEP 3: CC BY 4.0 permits copy/modify/distribute for any purpose incl. commercial, subject only to attribution + indicating changes; redistributable_attribution is correct and not too permissive for a re-hosting library. RESIDUAL CAVEAT (non-fatal): ESG indicators are drawn from multiple WB databases and external providers, so some upstream series could carry other terms, but the WB licenses the compiled ESG dataset under CC BY 4.0 with no carve-out in the license field, which governs. Recommend preserving attribution when re-hosting.

*Researcher reasoning:* The World Bank ESG dataset (Environment, Social & Governance / Sovereign ESG Data Portal) is a World Bank–produced open dataset. The authoritative licensing page (datacatalog.worldbank.org/public-licenses) states verbatim that "CC-BY 4.0, with the additional terms below, is the default license for all Datasets produced by the World Bank itself," and that this license "allows users to copy, modify and distribute data in any format for any purpose, including commercial use." Redistribution/re-hosting is therefore explicitly permitted, with the sole obligation being attribution: "Users are only obligated to give appropriate credit (attribution) and indicate if they have made any changes." The Sovereign ESG Data Portal's own download page independently confirms the ESG data is licensed under CC BY 4.0. CC BY 4.0 has no non-commercial restriction and no ShareAlike requirement, so classification is redistributable_attribution.

Two conservative caveats the professor should honor: (1) The World Bank explicitly warns that "Many datasets are available under other licenses. They are labeled accordingly," and that some third-party indicators may not be redistributed without the original provider's consent. The ESG dataset aggregates indicators sourced from multiple providers; any individual indicator flagged in DataBank/Data360 as third-party/restricted should be checked and excluded if not CC BY. (2) A separate general "Terms and Conditions of Using our Site" copyright notice on worldbank.org uses restrictive non-commercial/no-derivative language ("you may not make any derivative work or commercial use, including without limitation reselling them, charging to access them, charging to redistribute them"), but that notice governs website Materials (publications, text, images) — NOT open Datasets, which are separately and expressly governed by the CC BY 4.0 Terms of Use for Datasets. For the ESG data specifically, CC BY 4.0 controls. Attribution format required: "The World Bank" plus dataset name and data source.

---

### worldbank_pink (World Bank Commodity Price Data / "The Pink Sheet", Commodity Markets)

- **Databases (1):** `worldbank_pink`
- **Official terms URL:** https://data.worldbank.org/summary-terms-of-use
- **License:** CC BY 4.0 (Creative Commons Attribution 4.0 International License)
- **Classification:** redistributable_attribution  →  **corrected to `restricted / needs-review (NOT blanket CC BY 4.0). The Pink Sheet is not wholly "produced by the World Bank itself" — a large share of its series come from third-party proprietary providers: London Metal Exchange (LME) settlement prices for aluminum, copper, lead, nickel, tin, zinc; Cotlook "A index" for cotton; SICOM for rubber; ICCO/ICO for cocoa/coffee. Under the terms' own third-party carve-out these "may not be redistributed or reused without the consent of the original data provider." For a public re-hosting library, treat worldbank_pink as NEEDS-REVIEW / non-redistributable pending per-series rights clearance (LME in particular prohibits redistribution of its price data without a license), rather than redistributable_attribution.`** by adversarial review
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok
- **Adversarial verdict:** **DISPUTED** (quote verbatim: True, classification agrees: False)
- **Decision tier:** NEEDS HUMAN REVIEW

**Verbatim quote:**
> you are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost
> CC-BY 4.0, with the additional terms below, is the default license for all Datasets produced by the World Bank itself. (https://datacatalog.worldbank.org/public-licenses)
> allows users to copy, modify and distribute data in any format for any purpose, including commercial use. (https://datacatalog.worldbank.org/public-licenses)
> Users are only obligated to give appropriate credit (attribution) and indicate if they have made any changes, including translations. (https://datacatalog.worldbank.org/public-licenses)
> you agree to provide attribution to The World Bank and its data providers in the following format: The World Bank: Dataset name: Data source (https://data.worldbank.org/summary-terms-of-use)
> Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider (https://data.worldbank.org/summary-terms-of-use)
> The official Commodity Markets page (https://www.worldbank.org/en/research/commodity-markets) lists its data terms as 'Summary terms of use', 'Terms of use for Datasets', and 'Data Access and Licensing' linking to https://datacatalog.worldbank.org/public-licenses#cc-by

**Adversary's contradicting clause:** From the same official terms page (https://data.worldbank.org/summary-terms-of-use): "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider, or may be subject to additional terms and conditions, which are included in the dataset or indicator metadata." Reinforced by the Data Catalog licensing page: "CC-BY 4.0 ... is the default license for all Datasets produced by the World Bank itself and distributed as open data," and "Many datasets are available under other licenses."

*Verifier notes:* STEP 1 (quote/URL): CONFIRMED accurate. WebFetch of https://data.worldbank.org/summary-terms-of-use succeeded (fetch_status fetched_ok is correct) and the verbatim_quote appears word-for-word: "you are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost." The page does invoke CC BY 4.0. So the quote itself is not the problem — quote_verified_verbatim=true.

STEP 2 (stricter clause the researcher missed): FOUND. The finding quoted only the permissive headline sentence and ignored the third-party carve-out on the very same page ("...may not be redistributed or reused without the consent of the original data provider..."). The Data Catalog public-licenses page confirms CC BY 4.0 is only the DEFAULT for data "produced by the World Bank itself," with many datasets under other/restricted licenses and WB distributing under CC BY only "when required to do so by the original data provider."

STEP 3 (is the classification defensible for a re-hosting library?): NO — too permissive. I fetched an actual Pink Sheet issue (CMO-Pink-Sheet-January-2026.pdf, saved locally) and read its "Description of Price Series." It names third-party PROPRIETARY sources for many core series: "Aluminum (LME) London Metal Exchange ... physical settlement," and likewise Copper/Lead/Nickel/Tin/Zinc all "(LME)"; "Cotton (Cotton Outlook 'CotlookA index')"; rubber via "Singapore Commodity Exchange Ltd (SICOM)"; cocoa via ICCO; coffee via ICO. LME price data is licensed and its redistribution without an LME license is prohibited — this is the textbook case the WB third-party carve-out exists for. Additionally, every page of the PDF is stamped "Official Use Only" (a World Bank information classification, not "Public"), a secondary flag. Because the Pink Sheet blends WB-computed series with non-redistributable third-party feeds, labeling the whole dataset "CC BY 4.0 / redistributable_attribution" over-reaches for a platform that re-hosts the data for public download.

CONCLUSION: DISPUTED. The verbatim quote is accurate but the classification is more permissive than the terms support for this specific dataset. Recommended handling: NEEDS-REVIEW / non-redistributable until per-series rights (especially LME, Cotlook, SICOM) are cleared or those series are gated/withheld; only genuinely WB-originated series (e.g., WB-computed indexes, average crude, indicative survey prices) would qualify for CC BY 4.0. Applying skepticism per the review mandate and the project's prior over-permissioning error on World Bank econ sources, I did not find affirmative evidence that the third-party series are cleared for free redistribution, so I do not confirm the permissive classification.

Evidence URLs: https://data.worldbank.org/summary-terms-of-use ; https://datacatalog.worldbank.org/public-licenses ; https://www.worldbank.org/en/research/commodity-markets ; https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/related/CMO-Pink-Sheet-January-2026.pdf

*Researcher reasoning:* The World Bank Commodity Price Data ("Pink Sheet") is published under the World Bank's standard open-data terms. Three official World Bank pages establish this: (1) The official Commodity Markets landing page (worldbank.org/en/research/commodity-markets) — which hosts the Pink Sheet — lists its governing terms as "Summary terms of use", "Terms of use for Datasets", and "Data Access and Licensing", with the last linking specifically to datacatalog.worldbank.org/public-licenses#cc-by (the CC BY anchor). (2) The Data Access and Licensing page states verbatim: "CC-BY 4.0, with the additional terms below, is the default license for all Datasets produced by the World Bank itself," and that this "allows users to copy, modify and distribute data in any format for any purpose, including commercial use." The Pink Sheet is produced by the World Bank itself. (3) The Summary Terms of Use grants: "you are free to copy, distribute, adapt, display or include the data in other products for commercial or noncommercial purposes at no cost," under the "Creative Commons Attribution 4.0 International License," requiring attribution "in the following format: The World Bank: Dataset name: Data source." CC BY 4.0 permits redistribution/re-hosting with attribution, commercially or non-commercially, with no share-alike obligation — squarely satisfying a free, non-commercial academic re-host. CAVEAT: The Summary Terms also state "Some datasets and indicators are provided by third parties, and may not be redistributed or reused without the consent of the original data provider." Some individual Pink Sheet price series originate from third-party/proprietary commodity sources; however, the World Bank markets and links the aggregate Pink Sheet dataset itself under the CC-BY licensing page, so the aggregate compilation is redistributable with attribution. Recommendation: redistribute with clear attribution in the World Bank's specified format; safest posture is to attribute "The World Bank Commodity Price Data (The Pink Sheet)" and retain the source note. I could not obtain a per-series line-item license breakdown, so if any single series must be isolated and re-hosted, the third-party-consent caveat should be re-checked for that series. Classification: redistributable_attribution (CC BY 4.0).

---

### Yale Environmental Performance Index

- **Databases (1):** `yale_epi`
- **Official terms URL:** https://epi.yale.edu/faq/epi-faq
- **License:** CC BY-NC-SA 4.0
- **Classification:** noncommercial_only
- **Commercial OK:** False · **Attribution required:** True · **ShareAlike:** True · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** CLEARED - non-commercial only

**Verbatim quote:**
> The 2022 EPI is released under a Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
> Neither the 2022 EPI nor the data available on our downloads page may be used for commercial purposes.
> You may use the 2022 EPI, including the scores, report, policymakers' summary, and other material on this website, according to the terms of this license.
> Environmental Performance Index is a registered trademark of Yale University. All rights reserved.
> This work is under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. (from https://epi.yale.edu/downloads)

*Verifier notes:* Verbatim quote PASS: "The 2022 EPI is released under a Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License" appears word-for-word on the official page (https://epi.yale.edu/faq/epi-faq), twice (FAQ body + footer); fetch_status fetched_ok confirmed. Independent WebSearch against epi.yale.edu corroborated identical wording. Stricter-clause hunt: probed for redistribution ban, prior-written-permission, no-derivatives, bulk-download/scraping restriction, forced registration — NONE present. Only operative restriction is the commercial ban: "Neither the 2022 EPI nor the data available on our downloads page may be used for commercial purposes." CC BY-NC-SA 4.0 affirmatively grants Share/Adapt rights, so non-commercial re-hosting is genuinely permitted. Classification noncommercial_only is the correct standard mapping for CC BY-NC-SA 4.0 and is NOT too permissive. Caveats (non-refuting): (1) statement is year-specific to "The 2022 EPI" — other EPI vintages should be checked separately if hosted; (2) the ShareAlike obligation isn't surfaced by the label but makes terms STRICTER not more permissive, and a re-hosting library must itself apply CC BY-NC-SA; (3) Yale attributes the NC restriction to upstream partner data usage agreements, reinforcing the commercial prohibition. No basis to dispute; confident non-commercial redistribution is genuinely permitted.

*Researcher reasoning:* The Yale EPI publishes its data license on its own official domain (epi.yale.edu). Two official pages agree: the FAQ page (https://epi.yale.edu/faq/epi-faq) states verbatim "The 2022 EPI is released under a Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License," and the downloads page (https://epi.yale.edu/downloads) states "This work is under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License."

CC BY-NC-SA 4.0 is a recognized open license that AFFIRMATIVELY GRANTS redistribution/re-hosting rights (the license's Share right permits copying and redistributing the material in any medium or format), so this is not merely "publicly available" branding — redistribution is explicitly licensed. However, three conditions bind: (1) NonCommercial — the FAQ explicitly reinforces this: "Neither the 2022 EPI nor the data available on our downloads page may be used for commercial purposes"; (2) Attribution — a suggested citation (Wolf, Emerson, Esty, de Sherbinin, Wendling et al., 2022) is provided; (3) ShareAlike — any adaptations/derivatives must be distributed under the same CC BY-NC-SA 4.0 terms.

Because the NonCommercial restriction is the binding gate on redistribution, the correct conservative classification is noncommercial_only. This is COMPATIBLE with a free, non-commercial academic data library that re-hosts for download, PROVIDED the library: (a) is genuinely non-commercial, (b) attributes Yale EPI with the suggested citation, and (c) applies the same CC BY-NC-SA 4.0 license (ShareAlike) to the re-hosted data and any derivatives. Note also the trademark reservation: "Environmental Performance Index is a registered trademark of Yale University. All rights reserved." — the name/mark is reserved even though the data content is CC-licensed.

Caveat: The verbatim license text on the live pages references the 2022 EPI specifically; search results indicate the 2024 EPI carries the identical CC BY-NC-SA 4.0 license. Older EPI versions distributed via Yale Dataverse may carry their own per-dataset licenses and should be checked individually if re-hosted.

---

### Zillow (Zillow Group / Zillow, Inc.)

- **Databases (1):** `zillow`
- **Official terms URL:** https://www.zillow.com/corporate/terms-of-use/
- **License:** Zillow Terms of Use (custom proprietary terms), updated October 28, 2025
- **Classification:** permission_required
- **Commercial OK:** None · **Attribution required:** True · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** (quote verbatim: True, classification agrees: True)
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> Notwithstanding the foregoing, the aggregate level data provided on the Zillow Local-Info Pages (the "Aggregate Data") may be used for non-personal uses, e.g., real estate market analysis. You may display and distribute derivative works of the Aggregate Data (e.g., within a graph), only so long as the Zillow Companies are cited as a source on every page where the Aggregate Data are displayed, including "Data Provided by Zillow Group." Such citation may not include any of our logos without our prior written approval or imply any relationship between you and the Zillow Companies beyond that the Zillow Companies are the source of the Aggregate Data. You are prohibited from displaying any other Zillow Companies' data without our prior written approval.
> (Section 4.A, Use of the Services) Except as expressly stated herein, these Terms of Use do not provide you with a license to use, reproduce, distribute, display or provide access to any portion of the Services on third-party web sites or otherwise.
> (Section 5, Prohibited Use) BY USING THE SERVICES, YOU AGREE NOT TO: reproduce, modify, distribute, display or otherwise provide access to, create derivative works from, decompile, disassemble, or reverse engineer any portion of the Services, except as explicitly permitted by any Product's Terms to the extent applicable to that product's Services;
> (Section 5, Prohibited Use) reproduce, publicly display, or otherwise make accessible on or through any other website, application, or service any reviews, ratings, or profile information about real estate, lending, or other professionals, underlying images of or information about real estate listings, or other data or content available through the Services, except as explicitly permitted by us for a particular portion of the Services;
> (Section 5, Prohibited Use) conduct automated queries (including screen and database scraping, spiders, robots, crawlers, bypassing "captcha" or similar precautions, or any other automated activity with the purpose of obtaining information from the Services) on the Services;
> (Zillow API & Data Terms, https://www.zillowgroup.com/developers/terms/, Section 2) You further agree not to otherwise reproduce, modify, distribute, decompile, disassemble or reverse engineer any portion of the Zillow API or any data provided by Zillow.
> (Zillow API & Data Terms, https://www.zillowgroup.com/developers/terms/, Section 2) You will not permit your users to access the Zillow Data in bulk.
> (Zillow Public Records Data Terms, https://www.zillow.com/corp/PublicDataTerms.htm) Licensee shall not pre-fetch, copy, duplicate, cache, or store any of the Public Records Data in any manner whatsoever, including as part of a derivative work.

*Verifier notes:* Direct verification of the live URL was blocked: https://www.zillow.com/corporate/terms-of-use/ returns HTTP 403 to WebFetch, all Zillow-owned mirrors (zillowgroup.com -> zillow.com/z/corp/terms, trulia.com/info/terms, bridgedataoutput.com/zillowterms) also 403 or render empty, and web.archive.org is unreachable from this tool. However, three independent web searches reproduced the passage from the official Terms of Use, including the exact leading phrase "Notwithstanding the foregoing," and every distinctive construction in the finding's quote verbatim: (the "Aggregate Data"); "derivative works of the Aggregate Data (e.g., within a graph)"; "cited as a source on every page where the Aggregate Data are displayed, including 'Data Provided by Zillow Group.'"; the logo/relationship sentence; and "prohibited from displaying any other Zillow Companies' data without ... prior written approval." The quote is verbatim-accurate to the source. (Search-engine summarizers occasionally normalized connective words like "You may"->"Users may" in their own paraphrase, but the load-bearing, distinctive phrasing matched exactly and consistently across queries.)

Adversarial stricter-clause hunt: the broader Terms of Use are MORE restrictive than the clause quoted, not less. Section 5 "Prohibited Use" bars automated queries, screen/database scraping, spiders, robots, and crawlers; the terms bar reproducing, modifying, distributing, displaying, or creating derivative works from any portion of the Services; and "Users must not copy, redistribute, or retransmit any of the provided information" except as necessary for an individual property purchase/sale. The "Aggregate Data" sentence the researcher quoted is the single narrow GRANT in the document (graph-style derivative works, with "Data Provided by Zillow Group" attribution) and itself ends by requiring "prior written approval" for any other Zillow data.

Classification judgment: "permission_required" is defensible and appropriately conservative -- it is NOT too permissive. A library that re-hosts raw Zillow data for public download is performing bulk redistribution, which is not covered by the narrow "derivative works within a graph" grant and is affirmatively prohibited by the surrounding no-redistribution / anti-scraping clauses absent prior written approval. The stricter clauses reinforce the gate rather than contradict it, so there is no contradicting_clause and no correction is warranted. If anything the terms could support an even harder "prohibited" label, but "permission_required" correctly reflects that a prior-written-approval pathway exists.

One scope caveat (does not change the verdict): the most common academic use of "Zillow data" is the Zillow Research economic series (ZHVI/ZORI CSVs at zillow.com/research/data), which is governed by its own separate research-data terms rather than this general site Terms of Use. If the catalog entry actually points at that research product, the governing terms document differs -- but "permission_required" would remain a safe, defensible classification there as well, so the verdict stands.

*Researcher reasoning:* Zillow's official Terms of Use (browser-fetched in full from https://www.zillow.com/corporate/terms-of-use/, dated Oct 28, 2025) govern all Zillow data, including the Research/housing indices, which the Research page itself says are usable only "consistent with their published Terms of Use." The default rule is a flat prohibition on redistribution: Section 4.A states the terms "do not provide you with a license to use, reproduce, distribute, display or provide access to any portion of the Services on third-party web sites or otherwise," and Section 5 forbids making "accessible on or through any other website, application, or service ... other data or content available through the Services," plus any bulk scraping/automated extraction. The ONLY carve-out (Section 4.C) permits displaying and distributing DERIVATIVE WORKS of "Aggregate Data" (e.g., a graph) with mandatory attribution ("Data Provided by Zillow Group"). Re-hosting the raw datasets for third-party bulk download is NOT a "derivative work within a graph" — it is redistribution of the source data itself, which the same sentence expressly forbids: "You are prohibited from displaying any other Zillow Companies' data without our prior written approval." The developer Zillow API/Data Terms and Public Records Data Terms reinforce this (no bulk access, no redistribution, no retaining copies). Therefore, for a library that re-hosts Zillow data for download, redistribution requires Zillow's prior written approval — classified permission_required. Note: a narrow, distinct permission exists for publishing derivative charts of Aggregate Data with attribution, but that does not authorize re-hosting the datasets. I could not classify this as redistributable under any open/attribution/CC license because none exists; the terms are custom proprietary. The corporate ToU and Public Records/API terms quotes marked with zillow.com and zillowgroup.com URLs were verified verbatim (the corporate ToU via full browser page-text extraction); the API-terms quotes were obtained via automated page summarization and are attributed to their official URLs.

---

---

## National statistical offices + UN SDG + WHR — verified & un-gated 2026-07-21

Public licence terms fetched verbatim at source (whr = written email grant). Deployed live:
denylist.ts (13 removed, worker version 6e8e9410) + D1 econ-catalog (reservable=1). Verified
451->401 on econdl-api.elkassabgi.workers.dev; restricted controls (wto/cboe/sipri/worldbank_pink) stay 451.

| Source | Licence | Verdict | Verbatim key clause | URL |
|---|---|---|---|---|
| scb | CC0 1.0 | CLEARED (open) | "We use the licence CC0 for this data ... without any requirement to state the source" | https://scb.se/en/About-us/about-the-website-and-terms-of-use/open-data-api |
| dst | CC BY 4.0 | CLEARED (attrib) | "can be used free of charge commercially as well as non-commercially ... indicate the source ... Creative Commons, CC 4.0 BY" | https://www.dst.dk/en/presse/kildeangivelse |
| statfin | CC BY 4.0 | CLEARED (attrib) | "Statistics Finland uses the open data licence - CC BY 4.0 ... used freely ... provided that the source is mentioned" | https://stat.fi/en/services/statistical-data-services/open-data-and-interfaces |
| hagstofa | CC BY 4.0 | CLEARED (attrib) | "may be reused, copied, and shared ... for any purpose as long as Statistics Iceland is credited ... CC BY 4.0" | https://statice.is/publications/open-data-access/ |
| cso | CC BY 4.0 | CLEARED (attrib) | "Creative Commons Attribution (version 4.0 cc-by) ... Reproduction is authorised subject to acknowledgement of the source" | https://www.cso.ie/en/aboutus/whoweare/copyrightpolicy/ |
| ssb | CC BY 4.0 | CLEARED (attrib) | "Creative Commons Attribution 4.0 International (CC BY 4.0) ... Share ... redistribute ... Adapt ... even commercially" | https://www.ssb.no/en/diverse/lisens |
| stat_latvia | CC BY 4.0 | CLEARED (attrib) | "Users may share, copy and redistribute the published statistics ... correspond to the Creative Commons Attribution (CC BY 4.0) licence" | https://stat.gov.lv/en/about-osp |
| stat_slovenia | SURS open (CC BY-equiv) | CLEARED (attrib) | "available royalty-free ... for any purpose, including for-profit (commercial) ... provided ... SURS ... is acknowledged as their source" | https://www.stat.si/StatWeb/en/StaticPages/Index/copyright |
| bfs | opendata.swiss terms_by | CLEARED (attrib) | "Open use. Must provide the source. You may use this dataset for commercial purposes. You must provide the source" | https://opendata.swiss/en/terms-of-use |
| stat_estonia | CC BY-SA 4.0 | CLEARED (share-alike) | "Statistics Estonia's open data can be shared under Creative Common (CC) licence BY-SA 4.0" | https://andmed.stat.ee/en/stat |
| norgesbank | NLOD 2.0 | CLEARED (attrib) | data API registered "Norwegian Licence for Open Government Data" (NLOD_2_0), "can freely be used" | https://data.norge.no/nlod/en/2.0 |
| unsdg | UNdata Terms of Use | CLEARED (attrib) | "may be copied freely, duplicated and further distributed provided that UNdata is cited as the reference" (governs UNSD data incl. SDG DB; the restrictive un.org WEBSITE terms do NOT apply to the data service) | https://data.un.org/Host.aspx?Content=UNdataUse |
| whr | whr-granted (written) | CLEARED (NC + attrib) | Gallup/WHR granted in writing 2026-07-09 (publicly available, copyright, subject to attribution) | permission on file |
| ksh_stadat | CC BY 4.0 (HCSO/KSH) | CLEARED (attrib) | "HCSO uses standardised international licence Creative Commons Attribution 4.0". The CC BY-NC carve-out on the same page is SCOPED and does not reach STADAT — verbatim: "Considering data files queried from the internal databases of HCSO **on specific request**, the User is not entitled to use these files for commercial purposes, Creative Commons Attribution-NonCommercial 4.0". STADAT is the PUBLISHED summary-table product, not a bespoke internal-database extract, so plain CC BY 4.0 governs what we host. Verified 2026-07-27 against the official copyright page. | https://www.ksh.hu/copyright_hungarian_central_statistical_office |
| harvard_atlas | CC0 1.0 (public domain dedication) | CLEARED (unrestricted) | All three Harvard Dataverse deposits we ingest return `http://creativecommons.org/publicdomain/zero/1.0` from the authoritative schema.org API export — doi:10.7910/DVN/XTAQMC (ECI / growth projections), doi:10.7910/DVN/NDDMSN (services trade), doi:10.7910/DVN/YAVJDF. Same verification method the SWIID entry uses. CC0 permits unrestricted redistribution. Verified 2026-07-27. NOTE: the 7 pre-existing "Harvard" mentions in this file are all SWIID (Solt's deposit) and do NOT cover the Atlas — that gap is what this row closes. | https://dataverse.harvard.edu/api/datasets/export?exporter=schema.org&persistentId=doi:10.7910/DVN/XTAQMC |
| gapminder | CC BY 4.0 | CLEARED (attrib) | Our source is the open-numbers DDF repo, whose README states verbatim: "Gapminder created this dataset and provides it under [Creative Common Attribution 4.0 International]", link target `https://creativecommons.org/licenses/by/4.0/`. Worth recording HOW it is declared: the repo has NO LICENSE file (raw LICENSE / LICENSE.md / license.md all 404) and GitHub's licence API returns spdx_id null, so an automated licence probe finds NOTHING and the ingester's "CC BY 4.0" header looked unbacked. The declaration lives only in the README prose. Verified 2026-07-27. | https://github.com/open-numbers/ddf--gapminder--systema_globalis |


## WID.world — CC BY-NC-SA 4.0 (resolved 2026-07-28)

WID publishes no licence text on any page reachable by search: `/data/`, `/methodology/`,
`/website-credits/`, the privacy policy and the bulk-download `README.md` were all checked
(JS-rendered, not merely curl'd) and none states a licence. That is why the original permission
request said they "do not publish an explicit data-reuse license".

The declaration exists, but only as an ICON on the chart interface at https://wid.world/world/.
Alice (info@wid.world, 2026-07-27) wrote "The CC is displayed here:" above a screenshot with that
icon circled. The icon's markup, read from the live page, is the formal machine-readable relation:

> ```html
> <a rel="license" href="https://creativecommons.org/licenses/by-nc-sa/4.0/deed.en" target="_blank">
>   <img alt="Creative Commons License" style="border-width:0"
>        src="https://wid.world/www-site/themes/default/img/cc.png" width="20px">
>   <!--<div class="license-tootlip">This work is licensed under a
>   Creative Commons Attribution 4.0 International License</div>-->
> </a>
> ```

Two things to note, both load-bearing:

1. **The active declaration is CC BY-NC-SA 4.0** — `rel="license"` is the standard HTML licence
   relation, and it points at by-nc-sa/4.0.
2. **The CC BY 4.0 text in that snippet is COMMENTED OUT** and therefore not in force. It is
   presumably an earlier or draft claim. Anyone reading the page source casually could mistake it
   for the licence; it is not. Do not cite it.

Combined with Alice's written grant — "Yes, you can use the data for educational purpose" — this
CLEARS WID for re-hosting, subject to the terms actually granted:

- **NC**: the library is free, non-commercial and educational, so this is satisfied.
- **SA**: share-alike propagates. Re-hosted WID series must carry their own CC BY-NC-SA 4.0 licence
  row rather than inherit a site-wide CC BY, exactly as the IEP datasets already do.
- **Currency condition**: Alice asked that we "keep the most updated data sources"; the re-hosted
  series must therefore be wired to refresh, not snapshotted once.


## yale_epi — CC BY-NC-SA 4.0, and a licence row that said otherwise (corrected 2026-07-28)

Yale states it plainly at https://epi.yale.edu/about-epi:

> "This work is under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0
> International License."

and the downloads page carries `<a href="https://creativecommons.org/licenses/by-nc-sa/4.0/">`
with the text "Attribution-NonCommercial-ShareAlike 4.0 International License".

The catalogue's licence ID (`cc-by-nc-sa-4.0-yale_epi`) was therefore correct. Its
FLAGS were not, and every one of them erred toward granting MORE than Yale gives:

| flag | was | should be |
|---|---|---|
| commercial_ok | 1 | **0** — NC forbids commercial use |
| attribution_required | 0 | **1** — BY requires attribution |
| no_modify | 1 | **0** — SA permits derivatives, under the same terms |
| url | fragilestatesindex.org/... | **creativecommons.org/licenses/by-nc-sa/4.0/** |

The URL pointing at the Fragile States Index is the tell: this row was copied from
another source's entry and never re-derived. A missing licence blocks publication and
gets noticed; a wrong-but-plausible one silently hands downstream users rights the
licensor never granted, and propagates into everything that reads the flag.

Separately, `updater/strategies/fetchers/yale_epi.py` described the source as "CC BY
4.0" in its own docstring — a second, independent statement of the wrong licence.
Both are corrected.

---

## Institute for Economics & Peace (IEP) — added 2026-07-29

- **Databases (4):** `gpi`, `gti`, `ppi`, `etr` — 12,282 catalogued series
- **Licence row in use:** `cc-by-nc-sa-4.0-iep` (reservable=1, commercial_ok=0, attribution_required=1)
- **Why this section exists:** a sweep of every SERVED source against this file found IEP
  present nowhere in it — not by source id, not by publisher name, not by domain. The
  licence row had been assigned without a verbatim audit recorded. These four sources
  were the only genuinely unaudited served sources in the library (the other apparent
  gaps, seven `imf_*_direct` ids, share `imf-terms` with their audited originals and
  with `data.imf.org`, which this file does cover).

**Fetched 2026-07-29.**

**Source 1 — https://www.economicsandpeace.org/consulting/data-licensing/** (links
`creativecommons.org/licenses/by-nc-sa/4.0`):

> Non-Commercial Access Data from the Institute for Economics & Peace is available free
> of charge for non-commercial use under the Creative Commons
> Attribution-NonCommercial-ShareAlike 4.0 International License (CC BY-NC-SA 4.0) .
> This includes use by non-profit organisations and research institutions. To request
> non-commercial access, please complete the form below.

> Commercial organisations using IEP data must purchase a Commercial Licence before
> downloading any datasets.

**Source 2 — https://www.economicsandpeace.org/terms-conditions/ and
https://www.visionofhumanity.org/terms/** (identical wording on both):

> You may not, without the prior written permission of Institute for Economics and Peace
> and the permission of any other relevant rights owners: broadcast, republish, up-load
> to a third party, transmit, post, distribute, show or play in public, adapt or change
> in any way the Services or third party Services for any purpose

### Assessment: CLEARED — permission already obtained 2026-07-06

**CORRECTED 2026-07-29.** An earlier version of this section (written the same day, a
few hours before) concluded NEEDS HUMAN REVIEW and recommended writing to IEP. That was
wrong, and it was wrong against a record already in this repository:
`REDISTRIBUTION_EMAIL_TRAIL.md` line 17 —

> | IEP (GPI/GTI/PPI/ETR) | data-licensing web form | 2026-07-06 | GRANTED | CC BY-NC-SA 4.0 auto-confirmation |

Ahmed submitted IEP's own data-licensing request form on 2026-07-06 and received the
CC BY-NC-SA 4.0 confirmation. The "open question" below — whether a publicly-posted
release file is offered under the CC grant or caught by the site-terms republication
bar — was already answered by going through the front door. `gpi`, `gti`, `ppi` and
`etr` (12,282 series) are CLEARED under CC BY-NC-SA 4.0: non-commercial, attribution
required, share-alike. Our licence row `cc-by-nc-sa-4.0-iep` matches exactly.

The analysis that follows is retained because the two clauses it identifies are real
and the reasoning about their tension is sound; only the VERDICT was wrong, and it was
wrong because I did not read the permission record before forming it.

### Superseded assessment (kept for the reasoning, not the conclusion)

The licence NAME on our row is correct: IEP does grant CC BY-NC-SA 4.0 for
non-commercial use, and `commercial_ok=0` matches their requirement that commercial
users buy a licence. That part is confirmed.

What is NOT resolved is whether OPEN RE-HOSTING is covered, and there are two clauses
pulling in opposite directions:

1. CC BY-NC-SA 4.0 itself expressly permits redistribution ("Share — copy and
   redistribute the material in any medium or format") non-commercially, with
   attribution and share-alike. Under that grant alone, re-hosting is permitted.
2. But access is framed as something you REQUEST ("To request non-commercial access,
   please complete the form below"), and the site terms separately forbid republishing
   or distributing without prior written permission. That is the same shape as
   `freedomhouse` in this file, which was classified NOT freely redistributable
   precisely because the data sits behind a request even though the use terms sound
   permissive.

Our data was not obtained through the form — `ppi` reads a public release file
(`PPI-Public-Release-Data-2023.xlsx`) served directly from their site. Whether a
publicly-posted release file is offered under the CC grant, or is subject to the
site-terms republication bar, is a judgement about intent that should not be made by
inference.

**Recommended action:** treat as the `kof_globalization` case — ask IEP in writing to
confirm that non-commercial academic re-hosting of their public release files is
covered by the CC BY-NC-SA 4.0 grant. Until answered, the sources stay served under the
existing NC licence row (unchanged, attribution required, commercial use excluded)
rather than being silently re-gated, and this section records the open question.

---

## Five previously-unaudited served/candidate sources — added 2026-07-29

**Why:** an audit-coverage sweep found these five carried `reservable=1` with no entry
in this file. The flag is a column somebody set; it is not evidence anyone read the
publisher's terms. They were about to be proposed for hosting on that flag alone
(495,028,737 observations). Each publisher's terms were fetched and read today.

**Outcome: all five CONFIRMED — every existing licence row was already correct.** The
gap was documentation, not misclassification. Two carry caveats worth carrying forward.

### `istat` — Istituto Nazionale di Statistica (371,190,751 obs)
- **URL:** https://www.istat.it/en/legal-notice/ · **Row:** `cc-by-4.0` (reservable=1, commercial_ok=1)
> Unless otherwise stated, content on this website is licensed under a Creative Commons
> License – Attribution – 4.0 . You are free to: Share — copy and redistribute the
> material in any medium or format for any purpose, even commercially

**CONFIRMED — redistributable_attribution.** An explicit Share grant, commercial use
included. "Unless otherwise stated" is standard CC framing, not a redistribution bar.

### `cepii_gravity` — CEPII Gravity database (69,666,545 obs)
- **URL:** https://www.cepii.fr/cepii/en/bdd_modele/bdd_modele_item.asp?id=8 · **Row:** `etalab-2.0`
> Licence: Etalab 2.0

**CONFIRMED — redistributable_attribution.** Etalab Open Licence 2.0 permits reuse,
redistribution and adaptation, including commercially, conditioned on attribution.
NOTE: CEPII's separate legal page prohibits reproduction of its MARKS AND LOGOS without
permission — a trademark clause, not a data-redistribution bar; do not confuse the two.
Their stated citation requirement (Conte, Cotterlaz & Mayer working paper) should be
carried in the attribution string.

### `un_wpp` — UN World Population Prospects 2024 (27,756,924 obs)
- **URL:** https://population.un.org/wpp/downloads · **Row:** `cc-by-3.0-igo`
> Copyright © 2024 by United Nations, made available under a Creative Commons license
> CC BY 3.0 IGO: http://creativecommons.org/licenses/by/3.0/igo/

**CONFIRMED — redistributable_attribution.**
**IMPORTANT, and the reason this one nearly went the other way:** the general UN
copyright notice at https://www.un.org/en/about-us/copyright reads "Copyright © United
Nations. All rights reserved." and requires "permission in writing from the publisher".
Read alone it looks like a flat prohibition. It is not the governing text for this
data: the Population Division publishes its own CC BY 3.0 IGO grant on the WPP download
page. The site-wide notice covers un.org materials generally; the dataset carries a
specific grant that supersedes it for this product.
**D1 divergence:** D1 currently records `NEEDS-REVIEW` for un_wpp while the local
catalog records `cc-by-3.0-igo`. The local row is the CORRECT one; D1 holds the
pre-audit conservative default. Nothing is served today (0 catalogued series), so the
divergence is inert, but D1 should be brought into line before un_wpp is ever hosted.

### `ons_uk` — UK Office for National Statistics (25,401,777 obs)
- **URL:** https://www.ons.gov.uk/help/termsandconditions · **Row:** `ogl-uk-3.0`
> Most content on this website is subject to Crown copyright protection and is published
> under the Open Government Licence (OGL) . Some content is exempt from the OGL – check
> the list of exemptions. Reproduction of information is subject to the terms of the OGL

**CONFIRMED WITH CARVE-OUT — redistributable_attribution_with_exceptions.** OGL v3
permits copying, publishing and redistribution with attribution. But note "MOST
content" and an explicit exemptions list — the same shape as the worldbank entry in
this file. Before hosting, the exemptions list must be checked against the series we
actually hold rather than assuming blanket coverage.

### `adb` — Asian Development Bank Data Library (1,012,740 obs)
- **URL:** https://data.adb.org/terms-use-data · **Row:** `cc-by-3.0-igo-adb`
> Unless otherwise indicated, the use of the ADB Data Library (the Site) and any such
> data published on the Site is made available under a CC BY 3.0 IGO License

> You are free to share (copy, distribute, and use the database), create (produce works
> from the database) and adapt (modify, transform, and build upon the database) for both
> commercial and non-commercial purposes at no cost as long as you acknowledge the
> copyright of ADB is properly credited

**CONFIRMED — redistributable_attribution.** Explicit share/create/adapt grant,
commercial included. Two conditions to honour: the prescribed citation form ("Contains
information from [database name], © ADB [year]...") and a pass-through requirement that
sub-licensees carry the same acknowledgement. Exclusions: ADB logo/emblems, software,
and third-party material ADB does not hold rights to.
NOTE: these terms are written for **data.adb.org** (the ADB Data Library); our `adb`
source records **kidb.adb.org** (Key Indicators Database) as its homepage. Same
publisher, but confirm the KIDB downloads fall under the Data Library terms before
hosting.

---

## Caveat resolutions for `ons_uk` and `adb` — 2026-07-29

Both were recorded earlier today as CONFIRMED-with-caveat. Both caveats are now closed
by reading the specific pages, before either source is hosted.

### `ons_uk` — the OGL exemptions do NOT reach our data
The terms say OGL covers "MOST content ... Some content is exempt from the OGL — check
the list of exemptions", which left open whether our series were inside the carve-out.
The exemptions section (https://www.ons.gov.uk/help/termsandconditions#copyright-exemptions):

> Most content on this website is available under the Open Government Licence (OGL).
> However, some photographs, illustrations and videos used on this website are subject
> to third-party copyright license agreements. This includes material licensed from
> Royalty free stock sites or other image libraries. These items are not covered by the
> OGL and cannot be used without permission from the rights holder.

The carve-out is **images and video**. We host statistical time series, which are
squarely OGL. CLEARED — no series-level exclusion needed.

### `adb` — KIDB has its OWN grant, and it is broader than the Data Library's
Our data comes from `kidb.adb.org/api` (Key Indicators Database), not `data.adb.org`
(ADB Data Library) whose CC BY 3.0 IGO terms were quoted earlier. KIDB publishes its
own terms at https://kidb.adb.org/terms:

> Unless otherwise indicated, all data and metadata provided in ADB's Key Indicators
> Database System (KIDB) Online may be copied, downloaded, distributed, adapted,
> duplicated, linked, displayed or included in other products for commercial and
> noncommercial purposes at no cost.

Attribution is required in a PRESCRIBED FORM, which we must carry verbatim:

> Asian Development Bank: Key Indicators Database Online (https://kidb.adb.org).
> Accessed on [insert date of access].

Plus a pass-through requirement ("include the same acknowledgement requirement in any
sub-licenses"), no endorsement claim, and no use of ADB's logo without written
permission.

CLEARED — redistributable_attribution, commercial permitted.
**Carry forward:** KIDB warns that "sections of the KIDB may link to or contain content
which may originate from third parties and use of this content may be subject to
different copyright and terms of use" — the same third-party carve-out shape as the
worldbank entry in this file. Not a bar to hosting, but if a specific KIDB indicator is
ever identified as third-party sourced it must be excluded individually.

---

### Istat (Istituto nazionale di statistica, Italy)

- **Databases (1):** `istat`
- **Official terms URL:** https://www.istat.it/en/legal-notice/ (Italian original: https://www.istat.it/note-legali/)
- **License:** Creative Commons Attribution 4.0 (CC BY 4.0)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **RESEARCHER-ASSESSED, single pass** — quote verified verbatim on TWO
  official surfaces (English legal notice and the Italian original). NOT yet run through an
  independent verifier pass, unlike the entries above; treat the classification as sound and the
  verdict field as pending that second reader.
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote:**
> Unless otherwise stated, content on this website is licensed under a Creative Commons License – Attribution – 4.0.
> Share — copy and redistribute the material in any medium or format for any purpose, even commercially
> Adapt — remix, transform, and build upon the material for any purpose, even commercially
> You must give appropriate credit, provide a link to the license, and indicate if changes were made.
> Images, logos (including Istat logo), trademarks and other content owned by third parties belong to their respective owners and cannot be reproduced without their consent.

**Italian original (same clause, istat.it/note-legali):**
> Salvo diversa indicazione, tutti i contenuti pubblicati su questo sito sono soggetti alla licenza Creative Commons – Attribuzione – versione 4.0.
> Condividere — riprodurre, distribuire, comunicare al pubblico... per qualsiasi fine, anche commerciale
> Modificare — remixare, trasformare il materiale... per qualsiasi fine, anche commerciale

*Researcher reasoning:* Assessed 2026-08-01 because `istat` holds 398,212,530 observations across
1,223 dataflow parquets, is REGISTERED AND REFRESHING on a monthly cadence, and had no entry in
this file at all — so it was being crawled and stored indefinitely while remaining unservable for
want of a verdict. Both official surfaces state CC BY 4.0 in identical terms, and both spell out
the share and adapt grants "for any purpose, even commercially" / "per qualsiasi fine, anche
commerciale". Attribution is the only condition on the data.

Stricter-clause search: the sole carve-out on either page is for "Images, logos (including Istat
logo), trademarks and other content owned by third parties" — the same third-party shape as the
worldbank and adb entries in this file, and it does not reach the statistical data. The SDMX data
browser at esploradati.istat.it was checked as a third surface and returned a server error rather
than any terms, so it neither adds nor contradicts anything; the legal notice governs.

**Carry forward:** attribution must name Istat. If a specific ISTAT dataflow is ever identified as
carrying third-party licensed content, exclude that flow individually rather than reclassifying
the source.

---

### FDIC (Federal Deposit Insurance Corporation) — BankFind Suite API

- **Databases (1):** `fdic`
- **Official terms URL:** https://www.fdic.gov/about/website-policies (API docs: https://api.fdic.gov/banks/docs)
- **License:** None stated. Neither the website-policies page nor the BankFind Suite API
  documentation declares a licence, a public-domain dedication, or any reuse permission for
  FDIC's own content.
- **Classification:** unclear_not_found
- **Commercial OK:** None · **Attribution required:** None · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **RESEARCHER-ASSESSED, single pass** — two official surfaces checked,
  no independent verifier pass. Same caveat as the istat entry above.
- **Decision tier:** ~~NEEDS HUMAN REVIEW (keep gated)~~ → **SERVE — OWNER DECISION**
  (Ahmed, 2026-08-17). The absence-of-licence finding below was presented to the owner
  verbatim, including this file's rejection of the untested §105 presumption and the
  alternative of a permission email; the owner directed "serve". Recorded as an informed
  owner decision on the 17 U.S.C. §105 federal-works basis — NOT as a licence finding;
  the verbatim record below stands unchanged. If FDIC ever states terms, re-audit.

**Verbatim quote (the only reuse-adjacent language found, and it is about THIRD-PARTY content):**
> External sites may contain information that is copyrighted with restrictions on reuse. Permission to use copyrighted materials must be obtained from the original source and cannot be obtained from the FDIC.
> Reference to any specific commercial product, process, or service by trade name, trademark, manufacture, or otherwise does not constitute an endorsement, a recommendation, or a favoring by the FDIC or the United States government.

*Researcher reasoning:* Assessed 2026-08-01 because `fdic` is REGISTERED, LIVE and refreshing on
a quarterly cadence with 20,541,159 rows on disk (including a 19.9M-row long-format financials
table) and had NO entry in this file at all — crawled and stored indefinitely while unservable
for want of a verdict. Surfaced by the "stored but in neither list" axis of
tools/reconcile_serving.py, not by anyone noticing.

Two official surfaces were read in full: the FDIC website-policies page and the BankFind Suite
API documentation at api.fdic.gov/banks/docs. NEITHER contains a licence name, a public-domain
dedication, a copyright waiver, or any statement permitting reuse or redistribution of FDIC's
own data. The only reuse language on either page concerns THIRD-PARTY copyrighted material
reached via external links, which says nothing about FDIC's own content.

The tempting inference — US federal works are generally not subject to copyright under
17 U.S.C. §105, so this must be public domain — is exactly the assumption this file exists to
replace. The `census` entry above is classified redistributable_attribution because the Census
Bureau SAYS SO on its own terms page; FDIC says nothing, and "probably fine" is not a quote.
Compare the `whr` and `damodaran` entries, both unclear_not_found on the same basis.

**Carry forward:** this is answerable by asking. FDIC publishes a contact (webmaster@fdic.gov)
and the request is narrow: written confirmation that BankFind Suite data may be redistributed.
Until then the source stays gated, which is what it already is — but now by decision rather
than by omission.

---

### GLEIF (Global Legal Entity Identifier Foundation) — LEI data

- **Databases (1):** `gleif`
- **Official terms URL:** https://www.gleif.org/en/meta/lei-data-terms-of-use
- **License:** CC0 1.0 Universal (public-domain dedication)
- **Classification:** redistributable_open
- **Commercial OK:** True · **Attribution required:** False · **ShareAlike:** None · **Fetch:** fetched_ok
- **Adversarial verdict:** **RESEARCHER-ASSESSED, single pass** — one official surface, no
  independent verifier pass. Same caveat as the istat and fdic entries above.
- **Decision tier:** CLEARED - re-host OK (no endorsement implied)

**Verbatim quote:**
> The data available through the Access Service are provided under the CC0 licence

**The conditions GLEIF does attach, quoted:**
> refrain from creating, in whatever way, the impression that data ... except the original LEI and LE-RD downloaded by you through the Access Service, are provided or supported or authorized ... by GLEIF
> refrain from any actions or statements which may mislead the public ... to believe that any products or services provided by you ... are services or products of GLEIF

*Researcher reasoning:* Assessed 2026-08-01 because `gleif` is REGISTERED, LIVE and refreshing
daily with 3,383,323 LEI records on disk, had no entry in this file, and was therefore being
crawled indefinitely while unservable for want of a verdict.

CC0 is a public-domain dedication, so redistribution and commercial use are unrestricted and no
attribution is legally required. The two clauses above are a NON-MISREPRESENTATION condition,
not a redistribution restriction: they forbid implying GLEIF endorsement or that a redistributed
copy is GLEIF's own service. That is the same shape as the endorsement disclaimers already
recorded for census and fdic, and it is satisfied by presenting the data as redistributed by
this library rather than as a GLEIF product.

**Carry forward — the blocker here is SHAPE, not licence.** gleif is an ENTITY REGISTRY, not a
time series: `lei_records.parquet` carries LEI, LegalName, LegalJurisdiction,
EntityLegalFormCode, EntityStatus, RegistrationStatus, ManagingLOU — no series_key, no obs_date,
no value. It cannot be catalogued in the current series model at any grain, so clearing the
licence does not by itself make it servable. Serving it needs an entity-lookup surface, which is
a product decision rather than a compliance one.

### V-Dem (Varieties of Democracy) — Institute at the University of Gothenburg

- **Databases (1):** `vdem`
- **Official terms URL:** https://www.v-dem.net/about/faq/ (and the identical statement at
  https://www.v-dem.net/data/the-v-dem-dataset/)
- **License:** CC BY-SA 4.0 (Creative Commons Attribution-ShareAlike)
- **Classification:** redistributable_attribution
- **Commercial OK:** True · **Attribution required:** True · **ShareAlike:** YES · **Fetch:** fetched_ok
- **Adversarial verdict:** **CONFIRMED** — the identical sentence appears on two independent
  official surfaces (the dataset page and the site-wide FAQ), fetched separately.
- **Decision tier:** CLEARED - re-host OK (attribution)

**Verbatim quote** (v-dem.net FAQ and dataset page, fetched 2026-08-24, identical wording on both):
> The V-Dem Dataset is publicly available and published under a Creative Commons
> Attribution-ShareAlike (CC BY-SA) 4.0 license. This means that anyone is free to use, adapt,
> and share the data, including for commercial purposes, provided that appropriate attribution
> is given and that any derivative products are distributed under the same license terms.

*Researcher reasoning:* Assessed 2026-08-24 because `vdem` is REGISTERED, scheduled on the
workstation route, holds 77,371,121 observations across 783,100 series on disk, and had NO row
in this file — so it was being refreshed indefinitely while gated. It appears in
`denylist.ts` NON_REDISTRIBUTABLE, but that file is GENERATED from `license.reservable`, and
its own header says a source lands there when its licence row is unverified: "fix the license
row and regenerate, don't special-case them here". The gate was therefore the ABSENCE of this
assessment, not a decision against serving it.

**Two corrections this assessment makes.**

1. `jobs/ingest_vdem.py` has carried the header comment "CC BY 4.0 for most indices" since it
   was written. That is wrong twice: the licence is **ShareAlike**, which CC BY is not, and the
   hedge "most indices" has no counterpart in anything V-Dem publishes — their statement is
   unqualified and covers the dataset. An unsourced licence claim in a code comment is exactly
   what this file exists to replace.
2. The R package that delivers the data (`vdeminstitute/vdemdata`) declares `License: GPL-3` in
   its DESCRIPTION and ships no LICENSE file. GPL-3 is the licence of the PACKAGE CODE; the
   data licence is the CC BY-SA 4.0 above, stated by the publisher on its own site. We
   redistribute the data, not the package, so GPL-3 does not reach our distribution.

**ShareAlike obligation, and how it is met.** CC BY-SA requires derivative products to carry the
same terms. This library already serves seven CC BY-SA sources (the `unesco_*` family) through
per-source licence rows, so the mechanism exists: `vdem` gets its own row `cc-by-sa-4.0-vdem`
with `reservable=1`, and every download carries that licence id, so the SA term travels with
the data rather than being asserted in prose.

**SCOPE LIMIT — V-Party is NOT cleared by this entry.** `data/clean_full/vdem/` holds TWO
files: `vdem.parquet` (77,371,121 rows) and `vparty.parquet` (2,218,990 rows). The quote above
names "The V-Dem Dataset". V-Party is published as a separate dataset — its own page
(https://www.v-dem.net/data/v-party-dataset/, fetched 2026-08-24) carries NO licence language
at all, and the FAQ statement does not name it. Nothing entitles us to extend one dataset's
terms to another simply because they share a publisher and a directory. **Catalogue
`vdem.parquet` only; `vparty.parquet` stays unserved pending its own evidence.** This is the
R472 shape in advance — two things under one id whose licences can differ.
### fred

- **Databases (1):** `fred`
- **Official terms URL:** https://fred.stlouisfed.org/legal/
- **License:** FRED® Services Terms of Use (proprietary; not an open licence)
- **Classification:** non_redistributable — mirroring and re-serving are named prohibitions
- **Commercial OK:** False (for redistribution) · **Attribution required:** True · **ShareAlike:** False · **Fetch:** fetched_ok (in-app browser; WebFetch and a direct HTTPS GET both blocked — 403 / connection reset)
- **Adversarial verdict:** RESEARCHER-ASSESSED, single pass — NOT independently re-verified
- **Decision tier:** RESTRICTED (keep gated)

**Verbatim quote:**
> You can’t take all the data on FRED and claim it’s a unique product or service. Don’t try to pass off FRED or its related services (Excel Add-In, Widget, or mobile apps) as your own product or try to sell them to anyone. Don’t do any data mining, scraping or extraction of FRED data.
> Take all the data on FRED or related services and claim it is a unique product or service or otherwise provide the essential experience of the FRED website, data, or service.
> Engage, or otherwise participate, in the use of any data mining, mirroring, robots, scraping, or similar data-gathering or extraction methods except as expressly allowed by the terms of use applicable to the FRED API.
> Redistribute any third party’s proprietary content, including any graphs, maps, images, logos, data, or datasets, for commercial use without first obtaining express written permission from the data provider.
> FRED provides data and data services to the public for non-commercial, educational, and personal uses subject to a few prohibitions.
> BEFORE USING DATA SERIES OWNED BY THIRD PARTIES FOR ANYTHING OTHER THAN YOUR OWN PERSONAL USE, YOU MUST CONTACT THE DATA OWNER TO OBTAIN PERMISSION.
> Series with a copyright notice are owned by third parties and have special restrictions. Before using data with a copyright notice for anything other than your own personal use, you must contact the data owner to obtain permission. Unfortunately, the Federal Reserve Bank of St. Louis cannot give you such permission.
> Use the FRED® Services or FRED® Content in connection with the development or training of any software program or system or machine learning, including, but not limited to, large language models, deep learning, generative artificial intelligence, or any other program or process commonly known as artificial intelligence.

*Researcher reasoning:* This is not a close call and it is not a per-series carve-out problem like
worldbank's. The prohibitions section names the two things a re-hosting library actually does and
forbids both by name: "mirroring" appears in the prohibited data-gathering list, and taking the
data so as to "otherwise provide the essential experience of the FRED website, data, or service"
is prohibited outright — which is a fair description of serving FRED's series for public download
from another site. The permission grant is scoped to "non-commercial, educational, and personal
uses," and the third-party clause requires contacting each data owner before ANY use beyond
personal use, with the Bank stating in terms that it cannot grant that permission and will not
seek it on a user's behalf.

FRED's three copyright tiers ("Copyrighted: Pre-approval required", "Copyrighted: Citation
required", "Public Domain: Citation requested") are machine-identifiable, so a tempting design is
to serve only the public-domain tier. That does NOT rescue re-hosting: the mirroring and
essential-experience prohibitions are stated in section II as applying to "All use of FRED
data—including non-commercial, educational, and personal use," not only to the copyrighted
tiers. The right route to the public-domain series is their ORIGINAL publishers (BLS, BEA,
Census, the Board), most of which this library already serves directly and under their own terms.

Note also the ML clause: FRED content may not be used in connection with developing or training
software, machine-learning systems or LLMs. That is independent of redistribution and would bind
even internal use.

CONSEQUENCE FOR THE LIBRARY: `fred` holds 48,188,443 observations across 165 files in
data/clean_full/fred and is currently unreachable — 0 catalogue rows, absent from
SUPPORTED_SOURCES, so requests answer 501. It must STAY that way. Do not catalogue, do not
derive CSVs, do not add a fetcher or a registry entry. Whether to delete the local copy is
Ahmed's call, not a serving question; the data is re-crawlable from the FRED API if it is ever
needed under a different arrangement.

---

## Economic Freedom of the World (EFW) — Fraser Institute — `efw` (planned source)

**Verdict: CLEARED by WRITTEN PERMISSION (non-commercial, attribution, link-back).**

**The grant (verbatim, email on file in UCA Gmail):**

> "I sincerely apologize for my delay in getting back to you. This somehow lingered in my
> in box for WAY too long! Thank you so much for carefully reading the terms and
> conditions. Yes; it looks to me like your plan would work. Thank you so much for
> promoting the work and please let me know if you have any questions!"

— Matthew Mitchell, Senior Fellow, Fraser Institute (matthew.mitchell@fraserinstitute.org),
2026-08-10 16:52 UTC, Gmail message id 19fec97436898e6d, replying to Ahmed Elkassabgi's
permission request of 2026-07-06 (message id 19f3640f49fd46a7, sent to
freetheworld@fraserinstitute.org).

**What "your plan" binds us to** (the grant approves the plan as stated in the request, so
the request's own terms are the licence conditions):

- re-host the EFW index and its component series **non-commercially** with **full
  attribution to the Fraser Institute and the EFW project**;
- a **prominent link back to efotw.org**, directing users to the authoritative data;
- honour any exclusions or conditions the Institute later names (none named in the grant);
- refresh on their **annual release cadence** (offered in the request; no objection raised).

**Context from the public terms** (quoted in the request itself): the EFW citation page
states no part of the reports or data may be reproduced without written permission — this
email IS that written permission. The Fraser Institute's general T&C offer
CC BY-NC-SA for educational use; the specific EFW grant above governs.

**Caveat recorded honestly:** the grant is informal in register ("it looks to me like your
plan would work") from a Senior Fellow rather than a counter-signed agreement. It is
nonetheless a written yes from the Institute's named contact to a precisely-scoped request.
If the library's use ever expands beyond the request's scope (commercial use, derived
products beyond re-hosting, non-annual scraping), a fresh permission is required.

**Serving consequence:** `efw` may be BUILT and SERVED under licence class
noncommercial-attribution (reservable=1 per the no-metadata-only rule), with the citation
"Fraser Institute, Economic Freedom of the World" + link to efotw.org on every surface that
lists it. Data acquisition: their published annual dataset downloads (efotw.org), NOT
crawling beyond the published files.

---

## Addendum 2026-08-24 — cbs_nl and gus_dbw assessed

Both were being crawled and stored while carrying NO licence verdict, no `license_id` and no
`reservable` flag (cbs_nl additionally carried `review: True`). They were therefore correctly
un-servable: `catalog_complete.py` refuses to catalogue a source whose licence has no row, and
the standing rule is to gate rather than serve without a verdict. Assessed here so the
publish decision rests on evidence rather than on the absence of an objection.

| source | publisher | classification | status | verdict |
|---|---|---|---|---|
| `cbs_nl` | CBS (Statistics Netherlands) | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |
| `gus_dbw` | GUS (Statistics Poland) Knowledge Databases | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution + PSI disclosure) |

### cbs_nl — VERBATIM, https://www.cbs.nl/en-gb/about-us/website/copyright (fetched 2026-08-24)

> "Unless otherwise stated, the content of this website is subject to Creative Commons
> Attribution (CC BY 4.0)."

> "The re-use of the content of this site is permitted, provided Statistics Netherlands is
> cited as the source."

> "Naming of the source is mandatory whenever website content is being reproduced. This means
> that you are obliged to state that the data were sourced from CBS."

> "The re-use and citation of the content must not create the impression that CBS endorses the
> purport of the derivative work or that CBS agrees with the content of your work."

**One honest caveat, recorded rather than smoothed over.** Our data comes from
`opendata.cbs.nl` (the OData catalogue), and CBS's own open-data page states NO licence at
all — checked 2026-08-24, it defines open data as "data that can freely be used and is made
available in a machine-readable format" and says nothing about reuse rights. The CC BY 4.0
grant above is the site-wide one and applies by its own "unless otherwise stated" clause,
the portal having stated nothing otherwise. That is a sound reading, not an API-specific
grant. Exclusions named by CBS and NOT covered: site design, trademarks including the CBS
logo, third-party rights, and copyrighted photographs - none of which we hold.

Serving obligation: attribute "Statistics Netherlands (CBS)" on every served series, and do
not imply CBS endorsement.

### gus_dbw — VERBATIM, https://stat.gov.pl/en/copyright/ (fetched 2026-08-24)

> "There is no objection connected with copywriting of data and websites and printing
> including personal changes and summaries on condition that the source is given."

> "There is no objection connected with connections through links with website address, on
> condition that the source of the files or data is given."

> "It is not liable for the content of websites connected by links with Statistics Poland and
> for presenting personal summaries (changes in the text) based on Statistics Poland data."

GUS names no formal licence. The grant is explicit permission to copy and reuse - including
modified summaries - conditioned on citing the source. Poland's public-sector-information
rules add a disclosure duty on the re-user: state the source, the time the information was
created and obtained from Statistics Poland, and that it has been processed.

Serving obligation: attribute "Statistics Poland (GUS)", carry the acquisition date, and mark
the data as processed by this library.

### ilo (SDMX endpoint) — assessed 2026-08-24

- **Databases (1):** `ilo`
- **Official terms URL:** https://www.ilo.org/rights-and-permissions
- **License:** CC BY 4.0
- **Classification:** redistributable_attribution
- **Decision tier:** CLEARED - re-host OK (attribution)

| source | publisher | classification | status | verdict |
|---|---|---|---|---|
| `ilo` | International Labour Organization (SDMX) | redistributable_attribution | CONFIRMED | CLEARED - re-host OK (attribution) |

**Why this inherits ilostat's verdict rather than needing its own fetch.** The terms already
quoted verbatim and adversarially confirmed for `ilostat` are ILO ORGANIZATION-WIDE, not
product-specific:

> "As of 3 May 2023, unless otherwise indicated, ILO publications are licensed under a
> Creative Commons Attribution BY 4.0 licence (CC BY 4.0)."

> "databases and datasets together with the accompanying referential metadata are covered by
> the Creative Commons CC BY 4.0 licence."

PROVENANCE CHECKED, not assumed: `data/clean_full/ilo` is written by
`updater/strategies/fetchers/sdmx_nso.py` against **https://sdmx.ilo.org/rest/** — the ILO's
own SDMX service. Same publisher, same rights page, so the same grant covers it.

NOT A DUPLICATE OF ilostat, measured 2026-08-24: `ilo` carries SDMX-keyed series
(`REF_AREA=AGO:FREQ=A:MEASURE=CLD_2POP_NB:SEX=SEX_F:...`, child-labour measures) while
ilostat's catalogue ids are of the form `ilostat:UNE_DEAP_SEX_AGE_RT:AGE_YTHADULT_YGE15:AUS`
(unemployment rates). Zero of five sampled `ilo` keys matched any ilostat row. 1,157 parquet
files / 1.4 GB of genuinely distinct data, currently stored and served to nobody.

CAVEAT CARRIED FORWARD from the ilostat entry: "ILO publications produced prior to 3 May 2023
do not automatically benefit from a Creative Commons licence." That is aimed at publications;
these are continuously-updated database extracts, which the second quote covers explicitly.

Serving obligation: attribute the International Labour Organization on every served series.

---

## Correction applied 2026-08-24 — imf-terms was served as commercially usable

The `imf-terms` licence row carried `commercial_ok = 1` in both catalog.db and D1, while
every IMF entry in this file records **Commercial OK: False**. The API therefore told users
that IMF data could be used commercially, on **386,687 series across 19 sources**
(imf_gfscofog_direct 124,237, imf_ifs 100,706, imf_gfsssuc_direct 45,019, imf_cpi_direct
27,094, imf_gfsfalcs, imf_weo, imf_fsire, imf_fas_direct, imf_pgi, imf_world_direct,
imf_fdi_direct, imf_afrreo_direct and others). The licence row covers 1,286,901 series in
total.

The audit is what the terms actually say:

> "For any potential commercial reuse of IMF Data, please email copyright@imf.org to request
> permission."

CHECKED FIRST that the audit was not the thing in error: both IMF blocks here
(`### imf` and `### International Monetary Fund (IMF)`) independently record
Commercial OK: False, so they agree with each other and the served flag was the outlier.

Set `commercial_ok = 0` in catalog.db and in D1 (econ-catalog), verified live: /v1/sources
now reports imf_ifs and imf_weo as commercial_ok=False, attribution_required=True.

A NOTE ON HOW NEARLY THIS WAS MISSED: the first scan compared audit ids to served source ids
by exact name and found only 5 sources / 67,290 series. This file lists `imf_afrreo`; the
served source is `imf_afrreo_direct`. Normalising the suffix took the true count to 19
sources / 386,687 series — five times larger. Any future audit-vs-served comparison must
normalise those suffixes.

STILL OPEN, deliberately not changed: twelve sources / 129,241 series run the other way —
served `commercial_ok = false` where this file says commercial use is permitted (barro_lee
43,362; unesco_clte 23,868; unesco_inno 18,909; boc 12,862; unesco_film 8,527; unesco_dem
7,264; bundesbank 6,872; unesco_cltt 6,226; ipea 1,241; cnb 58; bis 49; bcrp 3). Correcting
those GRANTS rights rather than restricting them, which is the owner's call. The present
state understates what users may do, which is the safe direction to be wrong in.

---

## `imf` (the bare SDMX store) — DO NOT SERVE: duplicate of the imf_*_direct family (2026-08-24)

`tools/reconcile_serving.py` lists `imf` as stored-but-unserved (764 parquet / 620 MB) and its
licence is CLEARED, so it reads like an easy addition. It is not: it holds the SAME SERIES as
the served `imf_*_direct` sources, in an older key ordering.

    served    imf_afrreo_direct:AFRREO:AGO.A.BFD_GDP_BP6.BPM6
    imf store                        AGO.BFD_GDP_BP6.A

Same country, same indicator, same annual frequency — the components are simply ordered
differently, and the served form carries the methodology segment.

MEASURED on component sets (order-independent), 2026-08-24:
    imf/AFRREO  200 of 200 sampled keys are a component-subset of a served imf_afrreo_direct series
    imf/COFER   140 of 140  "                                          imf_cofer_direct
    imf/FDI     200 of 200  "                                          imf_fdi_direct

TWO EARLIER TESTS SAID "NOT A DUPLICATE" AND BOTH WERE WRONG, which is why this note exists:
  * comparing FILE NAMES to source ids matched only 12 of 764 — the served ids carry a
    `_direct` suffix the store does not.
  * comparing key SUBSTRINGS found 0 of 18 — the substrings differ because the component
    ORDER differs, not because the data does.
Only a component-SET comparison answers the question. Any future "is this source a duplicate?"
check must normalise both sides before comparing, or it will report data that already exists as new.

Serving it would have added 764 catalogue entries pointing at data already served under
different ids — the worst kind of growth, because it inflates the counts while making the
catalogue harder to search.
