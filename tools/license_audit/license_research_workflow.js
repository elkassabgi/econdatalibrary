export const meta = {
  name: 'econ-license-verbatim-audit',
  description: 'For every econ database, fetch the provider\'s official terms and quote redistribution rights VERBATIM, then adversarially verify each finding',
  phases: [
    { title: 'Research', detail: 'per-provider: fetch official terms, quote verbatim, classify' },
    { title: 'Verify', detail: 'adversarial: re-fetch, confirm quote is verbatim, refute over-permissive reads' },
  ],
}

// Clusters passed via args (array of {provider, hint, sources}). Each provider is
// researched once (its terms govern all its database ids); the per-database file is
// assembled by the caller from the local cluster->sources map.
const clusters = [{"provider": "UNCTAD (UN Conference on Trade and Development) UNCTADstat", "hint": "UNCTADstat terms of use / UN data policy", "sources": ["unctad_bopcaba", "unctad_ciocgeaia", "unctad_cioiuibbicoeair4a", "unctad_cpa", "unctad_cpia", "unctad_cpta", "unctad_fdiiaofasa", "unctad_fmcpa", "unctad_fmcpia21", "unctad_gasbeaiogasa", "unctad_gasbtbia", "unctad_gasbtoia", "unctad_gdpgbtoevbkoeatasa", "unctad_gdptapccac2pa", "unctad_lscia", "unctad_lsciq", "unctad_mfbcoboa", "unctad_mmcascioeaiopa", "unctad_mpcadioeaia", "unctad_mtba", "unctad_mttasa", "unctad_mttgra", "unctad_neera", "unctad_reericba", "unctad_reerigdba", "unctad_rfia", "unctad_rgdptapcgra", "unctad_sbeaiotsvsaga", "unctad_sbtisvsaga", "unctad_soigapotta", "unctad_sotwmfvbcoboa", "unctad_srbca", "unctad_tabbapotta", "unctad_tabmcioeaiopa", "unctad_tabmscioeaiopa", "unctad_tabpcioeaia", "unctad_taupa", "unctad_wstbtocabgoea"]}, {"provider": "International Monetary Fund (IMF) Data", "hint": "IMF eLibrary Data / IMF copyright & usage terms", "sources": ["imf_afrreo", "imf_apdreo", "imf_bopagg", "imf_cofer", "imf_commodity", "imf_cpi", "imf_fas", "imf_fdi", "imf_fiscaldecentralization", "imf_fm", "imf_fsire", "imf_gender_budgeting", "imf_gender_equality", "imf_gfscofog", "imf_gfse", "imf_gfsfalcs", "imf_gfsibs", "imf_gfsmab", "imf_gfsssuc", "imf_hpdd", "imf_mcdreo", "imf_namain_idc_n", "imf_pctot", "imf_pgcs", "imf_pgi", "imf_psbsfad", "imf_unsdg_imf_inputs", "imf_weo", "imf_whdreo", "imf_world"]}, {"provider": "FAO (UN Food and Agriculture Organization) FAOSTAT", "hint": "FAOSTAT / FAO open data licence CC BY-4.0 IGO", "sources": ["fao_ae", "fao_af", "fao_ec", "fao_ep", "fao_es", "fao_et", "fao_ew", "fao_fo", "fao_ga", "fao_gb", "fao_ge", "fao_gf", "fao_gl", "fao_gn", "fao_gr", "fao_gt", "fao_gy", "fao_ic", "fao_oa", "fao_pp", "fao_qa", "fao_qcl", "fao_ql", "fao_qp", "fao_rp"]}, {"provider": "World Trade Organization (WTO)", "hint": "WTO Tariff & Trade Data terms / WTO Stats", "sources": ["wto_hs_a_0010", "wto_hs_a_0015", "wto_hs_a_0020", "wto_hs_a_0025", "wto_hs_a_0030", "wto_hs_a_0040", "wto_its_mtv_am", "wto_its_mtv_ax"]}, {"provider": "UNESCO Institute for Statistics (UIS)", "hint": "UIS terms of use / UNESCO open data", "sources": ["unesco_clte", "unesco_cltt", "unesco_dem", "unesco_film", "unesco_inno"]}, {"provider": "World Health Organization (WHO) Global Health Observatory", "hint": "WHO GHO data policy / CC BY-NC-SA 3.0 IGO", "sources": ["who_hwf", "who_rs", "who_sdg"]}, {"provider": "abs", "hint": "", "sources": ["abs"]}, {"provider": "Barro-Lee Educational Attainment", "hint": "", "sources": ["barro_lee"]}, {"provider": "Banco Central do Brasil (BCB) SGS", "hint": "", "sources": ["bcb"]}, {"provider": "Banco Central de Reserva del Peru (BCRP)", "hint": "", "sources": ["bcrp"]}, {"provider": "bea", "hint": "", "sources": ["bea"]}, {"provider": "bis", "hint": "", "sources": ["bis"]}, {"provider": "bls", "hint": "", "sources": ["bls"]}, {"provider": "Bank of Canada Valet", "hint": "", "sources": ["boc"]}, {"provider": "boe", "hint": "", "sources": ["boe"]}, {"provider": "Deutsche Bundesbank time series", "hint": "", "sources": ["bundesbank"]}, {"provider": "cboe", "hint": "", "sources": ["cboe"]}, {"provider": "census", "hint": "", "sources": ["census"]}, {"provider": "Czech National Bank (CNB) ARAD", "hint": "", "sources": ["cnb"]}, {"provider": "UN Comtrade", "hint": "", "sources": ["comtrade"]}, {"provider": "Correlates of War", "hint": "", "sources": ["cow"]}, {"provider": "Aswath Damodaran (NYU Stern) datasets", "hint": "", "sources": ["damodaran"]}, {"provider": "DBnomics (per-provider passthrough)", "hint": "", "sources": ["dbnomics"]}, {"provider": "defillama", "hint": "", "sources": ["defillama"]}, {"provider": "ecb", "hint": "", "sources": ["ecb"]}, {"provider": "EU JRC EDGAR (Emissions Database for Global Atmospheric Research)", "hint": "EDGAR JRC data policy / EU Commission reuse", "sources": ["edgar_jrc"]}, {"provider": "Energy Institute Statistical Review of World Energy", "hint": "", "sources": ["ei_statreview"]}, {"provider": "eia", "hint": "", "sources": ["eia"]}, {"provider": "ember", "hint": "", "sources": ["ember"]}, {"provider": "Economic Policy Uncertainty Index (Baker/Bloom/Davis)", "hint": "", "sources": ["epu"]}, {"provider": "eurostat", "hint": "", "sources": ["eurostat"]}, {"provider": "Kenneth French Data Library (Dartmouth)", "hint": "", "sources": ["famafrench"]}, {"provider": "faostat", "hint": "", "sources": ["faostat"]}, {"provider": "fed_board", "hint": "", "sources": ["fed_board"]}, {"provider": "fhfa", "hint": "", "sources": ["fhfa"]}, {"provider": "frankfurter", "hint": "", "sources": ["frankfurter"]}, {"provider": "Freedom House", "hint": "", "sources": ["freedomhouse"]}, {"provider": "Fund for Peace Fragile States Index", "hint": "", "sources": ["fsi_fundforpeace"]}, {"provider": "Global Carbon Budget / Global Carbon Project", "hint": "", "sources": ["gcb"]}, {"provider": "Groningen Growth and Development Centre", "hint": "", "sources": ["ggdc"]}, {"provider": "Global Power Plant Database (WRI)", "hint": "", "sources": ["gppd"]}, {"provider": "hf_equities", "hint": "", "sources": ["hf_equities"]}, {"provider": "Inter-American Development Bank (IDB)", "hint": "", "sources": ["idb"]}, {"provider": "ilostat", "hint": "", "sources": ["ilostat"]}, {"provider": "imf", "hint": "", "sources": ["imf"]}, {"provider": "INSEE (France, Institut national de la statistique)", "hint": "INSEE open licence / Etalab", "sources": ["insee_bdm"]}, {"provider": "IPEA / Ipeadata (Brazil)", "hint": "", "sources": ["ipea"]}, {"provider": "IRENA (Int'l Renewable Energy Agency)", "hint": "", "sources": ["irena"]}, {"provider": "KOF Swiss Economic Institute (ETH Zurich)", "hint": "", "sources": ["kof_globalization"]}, {"provider": "KSH Hungarian Central Statistical Office", "hint": "KSH STADAT terms of use", "sources": ["ksh"]}, {"provider": "Maddison Project Database (Groningen)", "hint": "", "sources": ["maddison"]}, {"provider": "NASA GISS (Goddard Institute for Space Studies) GISTEMP", "hint": "", "sources": ["nasa_giss"]}, {"provider": "Narodowy Bank Polski (NBP)", "hint": "", "sources": ["nbp"]}, {"provider": "NOAA", "hint": "", "sources": ["noaa"]}, {"provider": "Federal Reserve Bank of New York", "hint": "", "sources": ["nyfed"]}, {"provider": "oecd", "hint": "", "sources": ["oecd"]}, {"provider": "US Office of Financial Research", "hint": "", "sources": ["ofr"]}, {"provider": "owid", "hint": "", "sources": ["owid"]}, {"provider": "Oxford COVID-19 Government Response Tracker", "hint": "", "sources": ["oxcgrt"]}, {"provider": "penn_world_table", "hint": "", "sources": ["penn_world_table"]}, {"provider": "World Bank Poverty & Inequality Platform (PIP)", "hint": "", "sources": ["pip"]}, {"provider": "Polity5 (Center for Systemic Peace)", "hint": "", "sources": ["polity"]}, {"provider": "Penn World Table (Groningen GGDC)", "hint": "", "sources": ["pwt"]}, {"provider": "Reserve Bank of Australia (RBA)", "hint": "", "sources": ["rba"]}, {"provider": "Sveriges Riksbank", "hint": "", "sources": ["riksbank"]}, {"provider": "sec_edgar", "hint": "", "sources": ["sec_edgar"]}, {"provider": "Robert Shiller (Yale) online data", "hint": "", "sources": ["shiller"]}, {"provider": "SIPRI (Stockholm Int'l Peace Research Institute)", "hint": "", "sources": ["sipri"]}, {"provider": "Swiss National Bank (SNB) data portal", "hint": "", "sources": ["snb"]}, {"provider": "statcan", "hint": "", "sources": ["statcan"]}, {"provider": "Stats NZ", "hint": "", "sources": ["stats_nz"]}, {"provider": "Standardized World Income Inequality Database (SWIID)", "hint": "", "sources": ["swiid"]}, {"provider": "Central Bank of Turkey (TCMB) EVDS", "hint": "", "sources": ["tcmb"]}, {"provider": "Transparency International (CPI)", "hint": "", "sources": ["transparency_ti"]}, {"provider": "treasury", "hint": "", "sources": ["treasury"]}, {"provider": "Uppsala Conflict Data Program (UCDP)", "hint": "", "sources": ["ucdp"]}, {"provider": "UNDP Human Development Report", "hint": "", "sources": ["undp_hdr"]}, {"provider": "UNHCR Refugee Data", "hint": "", "sources": ["unhcr"]}, {"provider": "usda", "hint": "", "sources": ["usda"]}, {"provider": "World Bank Worldwide Governance Indicators", "hint": "", "sources": ["wgi"]}, {"provider": "whr", "hint": "", "sources": ["whr"]}, {"provider": "wikidata", "hint": "", "sources": ["wikidata"]}, {"provider": "World Bank Open Data", "hint": "", "sources": ["worldbank"]}, {"provider": "worldbank_esg", "hint": "", "sources": ["worldbank_esg"]}, {"provider": "worldbank_pink", "hint": "", "sources": ["worldbank_pink"]}, {"provider": "World Bank World Development Indicators", "hint": "", "sources": ["worldbank_wdi"]}, {"provider": "Yale Environmental Performance Index", "hint": "", "sources": ["yale_epi"]}, {"provider": "zillow", "hint": "", "sources": ["zillow"]}];
log(`license audit: ${clusters.length} providers covering ${clusters.reduce((n,c)=>n+(c.sources?c.sources.length:1),0)} databases`)

const FINDING_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    provider: { type: 'string' },
    official_terms_url: { type: 'string', description: 'exact URL the verbatim quote was fetched from' },
    verbatim_quote: { type: 'string', description: 'exact governing redistribution/reuse clause, word-for-word from the page' },
    additional_quotes: { type: 'array', items: { type: 'string' } },
    license_name: { type: 'string', description: 'e.g. "CC BY 4.0", "public domain", "custom terms"' },
    classification: { type: 'string', enum: ['redistributable_open','redistributable_attribution','noncommercial_only','permission_required','prohibited','unclear_not_found'] },
    commercial_ok: { type: ['boolean','null'] },
    attribution_required: { type: ['boolean','null'] },
    sharealike: { type: ['boolean','null'] },
    reasoning: { type: 'string' },
    fetch_status: { type: 'string', enum: ['fetched_ok','partial','not_found','inaccessible'] },
  },
  required: ['provider','official_terms_url','verbatim_quote','classification','fetch_status','reasoning'],
}

const VERDICT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    verdict: { type: 'string', enum: ['CONFIRMED','DISPUTED','UNVERIFIABLE'] },
    quote_verified_verbatim: { type: 'boolean' },
    classification_agrees: { type: 'boolean' },
    contradicting_clause: { type: ['string','null'] },
    corrected_classification: { type: ['string','null'] },
    notes: { type: 'string' },
  },
  required: ['verdict','quote_verified_verbatim','classification_agrees','notes'],
}

function researchPrompt(cl) {
  return `You research REDISTRIBUTION / RE-HOSTING rights for a FREE, NON-COMMERCIAL academic data library that RE-HOSTS (redistributes for download) third-party datasets.

PROVIDER: ${cl.provider}
${cl.hint ? 'SEARCH HINT: ' + cl.hint : ''}
DATABASE IDS COVERED: ${(cl.sources||[]).join(', ')}

TASK: Find this provider's OFFICIAL terms of use / data licence / copyright policy governing whether a third party may REDISTRIBUTE / RE-HOST / RE-DISSEMINATE the data (not merely access or use it).

STEPS:
1. WebSearch for the provider's official terms-of-use / data-policy / licence / copyright page. Strongly prefer the provider's OWN domain over third-party summaries or Wikipedia.
2. WebFetch that page and READ it.
3. Quote VERBATIM (exact words) the clause(s) governing redistribution/re-hosting/re-dissemination, commercial vs non-commercial use, and attribution. Record the EXACT URL each quote came from.
4. Classify conservatively.

HARD RULES (this feeds a real compliance decision - the professor's reputation is on the line):
- Quote ONLY text you actually fetched and read on the official page. NEVER invent, paraphrase-as-quote, or reconstruct terms from memory. If you cannot find or access the official terms, set fetch_status to not_found/inaccessible and classification to unclear_not_found. DO NOT GUESS a licence.
- "Publicly available" / "free to access" / "open data" branding does NOT by itself mean "may redistribute". Look for EXPLICIT redistribution / re-dissemination / re-hosting / mass-download language. Many providers permit use but restrict redistribution.
- If the terms forbid redistribution or require prior written permission, classify prohibited or permission_required and quote the exact restrictive sentence.
- classification meanings: redistributable_open = public domain / CC0 / open with no conditions; redistributable_attribution = redistribution OK with attribution (e.g. CC BY, open-gov licences); noncommercial_only = redistribution OK but non-commercial only (e.g. CC BY-NC / -NC-SA); permission_required = must obtain written permission first; prohibited = redistribution forbidden outright; unclear_not_found = could not determine from official terms.

Return the structured finding for this provider.`
}

function verifyPrompt(r, cl) {
  return `You are an ADVERSARIAL licensing reviewer. A researcher produced the finding below about ${cl.provider}. Try to REFUTE it - do not rubber-stamp.

FINDING:
- official_terms_url: ${r.official_terms_url}
- verbatim_quote: "${r.verbatim_quote}"
- license_name: ${r.license_name || '(none)'}
- classification: ${r.classification}
- fetch_status: ${r.fetch_status}

STEPS:
1. WebFetch the official_terms_url. Confirm the verbatim_quote appears there WORD-FOR-WORD. If the quote is not present verbatim, or the URL is inaccessible / 404 / a different page, that is a serious red flag.
2. Independently search the provider's terms for a STRICTER clause the researcher may have missed: a redistribution ban, a non-commercial restriction, a "prior written permission" requirement, a no-derivatives clause, or a mass-download / bulk-extraction restriction. Providers routinely allow "use" while restricting "redistribution".
3. Judge whether the classification is DEFENSIBLE and NOT TOO PERMISSIVE for a library that re-hosts the data for public download.

RETURN:
- CONFIRMED only if the quote is verbatim-accurate at that URL AND the classification is defensible (not more permissive than the terms support).
- DISPUTED if the quote is inaccurate/absent OR the classification is too permissive - supply the contradicting_clause (verbatim if possible) and a corrected_classification.
- UNVERIFIABLE if the URL is inaccessible and you cannot independently confirm from the official source.
Default to SKEPTICISM: if you are not confident redistribution is genuinely permitted, do NOT confirm a permissive classification.`
}

const results = await pipeline(
  clusters,
  cl => agent(researchPrompt(cl), { label: `research:${cl.provider.slice(0,28)}`, phase: 'Research', schema: FINDING_SCHEMA, agentType: 'general-purpose' }),
  (r, cl) => {
    if (!r) return null
    return agent(verifyPrompt(r, cl), { label: `verify:${cl.provider.slice(0,28)}`, phase: 'Verify', schema: VERDICT_SCHEMA, agentType: 'general-purpose' })
      .then(v => ({ provider: cl.provider, sources: cl.sources || [], finding: r, verdict: v }))
      .catch(() => ({ provider: cl.provider, sources: cl.sources || [], finding: r, verdict: null }))
  }
)

const clean = results.filter(Boolean)
log(`done: ${clean.length}/${clusters.length} providers researched+verified`)
return clean
