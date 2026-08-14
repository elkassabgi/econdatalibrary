// ---------------------------------------------------------------------------
// src/util.ts  --  honest-status responses, license blocks, cadence math.
//
// HONEST-STATUS IS NON-NEGOTIABLE (CONTRACT.md):
//   200 only with >=1 row; 404 unknown id; 501 not_migrated; 502 resolver_empty;
//   400 unsupported_filter. NEVER an empty 200, NEVER a fabricated date.
// These helpers are the single place those codes are emitted, so the rule is
// enforced uniformly across every handler.
// ---------------------------------------------------------------------------

import type { Env, LicenseRow, LicenseBlock } from "./types";

// The 191 sources with an at-rest resolver (econdl._resolve.supported_sources()).
// A series whose source is NOT in this set returns 501 not_migrated -- loud and
// actionable, exactly as the dev shim does via ResolveError. KEEP IN SYNC with
// clients/python/econdl/_resolve.py::_RESOLVERS (regenerate via supported_sources()).
// Regenerated 2026-07-02 from supported_sources() (was a stale 33-entry list, which
// made ~158 genuinely-migrated sources return 501 instead of the honest 502).
export const SUPPORTED_SOURCES: readonly string[] = [
  // zillow REMOVED 2026-08-01. Unlike the 17 removed above, this one WAS being served: 52
  // catalogue rows, 52 derived CSVs in R2, and this listing. Zillow's Terms of Use (updated
  // 2025-10-28) are CONFIRMED permission_required and the audit's tier is RESTRICTED (keep
  // gated) — Section 5 forbids reproducing or "otherwise mak[ing] accessible on or through any
  // other website, application, or service" its data without prior written approval. The 52
  // rows and the 52 objects were withdrawn (manifest in logs/zillow_gate_manifest.json); the
  // 412 local parquet files are KEPT, so this is reversible the moment permission exists.

  // REMOVED 2026-08-01 — 17 ids whose licence verdict in DATABASE_LICENSES_VERBATIM.md is
  // RESTRICTED (keep gated) or NEEDS HUMAN REVIEW, so this list must not offer them:
  //   gated:  cboe, cow, famafrench, nbp, polity, sipri, tcmb, and the eight wto_* flows
  //   unreviewed: irena, shiller (classification unclear_not_found)
  // Nothing was withdrawn from service: all 17 already had ZERO derived CSVs in R2 and ZERO
  // catalogue rows, so a request for one answered 404 either way. What they did do was make
  // this list — the thing that is supposed to say what we serve — disagree with the licence
  // audit, and hide that disagreement behind an error code that looks like "no such series".
  // Re-add one only after its verdict changes in the audit AND its CSVs are verified present.

  "abs", "barro_lee", "bcb", "bcrp", "bea", "bis",
  "bls", "boc", "boe", "bundesbank", "census",
  // cepii_gravity added 2026-07-30: 1,143,250 series were catalogued and SEARCHABLE but
  // absent from this list, so every one of them answered 501 not_migrated. The derive is
  // complete and verified both directions (MISSING 0, ORPHANED 0 against a full listing
  // of all 1,143,250 objects), and the licence is CONFIRMED redistributable_attribution
  // (Etalab Open Licence 2.0, 100% dated from CEPII's own Last-Modified, 2024-04-15).
  "cepii_gravity",
  // cepii_baci added 2026-08-04 (cycle 1 of the econ-updater loop): 90,582 pair-grain series
  // (BACI:tv/tq:<EXP>:<IMP>) projected HS96-only from the V202601 vintage — the product
  // dimension is aggregated away and every catalogue title says so. Licence etalab-2.0 with
  // the publisher's own version string as the last-update statement ("2026-01", month
  // precision — a day invented from a month would be fabricated precision, the cepii_gravity
  // rule). verify 300/300 byte-identical through the bespoke pairs resolver before the flip.
  "cepii_baci",
  "cnb", "comtrade", "damodaran", "dbnomics", "defillama",
  "ecb", "edgar_jrc", "efw", "ei_statreview", "eia", "ember", "epu",
  "eurostat", "fao_ae", "fao_af", "fao_ec", "fao_ep",
  "fao_es", "fao_et", "fao_ew", "fao_fo", "fao_ga", "fao_gb",
  "fao_ge", "fao_gf", "fao_gl", "fao_gn", "fao_gr", "fao_gt",
  "fao_gy", "fao_ic", "fao_oa", "fao_pp", "fao_qa", "fao_qcl",
  "fao_ql", "fao_qp", "fao_rp", "faostat", "fed_board", "fhfa",
  "frankfurter", "fsi_fundforpeace", "gcb", "ggdc", "gppd",
  // IEP (CC BY-NC-SA 4.0, granted 2026-07-06). These four were CATALOGUED and
  // searchable on the site with ZERO CSVs in R2, so every Download button on them
  // failed — 12,282 series advertised with nothing behind them. Derived 2026-07-27
  // (12,282/12,282, verified 100% present in R2 before this flip).
  "gpi", "gti", "ppi", "etr",
  // harvard_atlas (CC0 1.0, verified against all three Dataverse DOIs). LIVE and
  // updated daily but had ZERO catalog rows, so 255,217 series were fetched nightly
  // and served to nobody. Catalogued + derived 2026-07-27 (251,217 uploaded this
  // run, 0 failures; 255,217/255,217 verified present in R2 before this flip).
  "harvard_atlas",
  // gapminder (CC BY 4.0, declared in the repo README prose — no LICENSE file and
  // GitHub's licence API returns null, so automated probes find nothing). LIVE and
  // updated daily with ZERO catalog rows: 86,684 series served to nobody.
  // 86,684/86,684 verified in R2 and in D1 before this flip.
  "gapminder",
  // IMF DIRECT (imf-terms). Same IMF datasets we relay via DBnomics, pulled from
  // api.imf.org instead. 21,382 series; derive verified 21,382/21,382 present in R2
  // and in live D1 BEFORE this flip — CSVs first, flag second, because flag-first
  // turns a 501 into a 404 and a 404 says the series does not exist.
  "imf_afrreo_direct", "imf_apdreo_direct", "imf_cofer_direct", "imf_fas_direct",
  "imf_fdi_direct", "imf_whdreo_direct", "imf_world_direct",
  // GFS direct, added 2026-08-01. These held data in R2 with ZERO catalogue rows - hosted and
  // downloadable by id, invisible to search. Now catalogued with titles decoded from IMF's own
  // codelists and their CSVs derived, so listing them here is an offer we can actually meet.
  "imf_gfssoef_direct", "imf_gfsssuc_direct",
  // FSI direct, added 2026-08-04 — same shape as the GFS pair above, found the same way:
  // scheduled every run, data in R2, ZERO catalogue rows, so 78,576 series were refreshed daily
  // and reachable by nobody. Their fetchers had ALSO been stuck at `partial` for ever, with
  // "csv coherence unmet: 43,814 / 32,906 / 1,856" — exactly their series counts — because a
  // source with no catalogue rows can never satisfy coherence, and a `partial` never sets
  // last_success_utc (R231). Cataloguing them fixes the serving AND unsticks the fetchers.
  // Order was CSVs -> D1 -> this flag, per the note above. Verified before flipping:
  //   imf_fsicdm_direct   1,856 catalogue == 1,856 R2 · MISSING 0 · ORPHANED 0 · 200/200 bytes
  //   imf_fsic_direct    32,906 catalogue == 32,906 R2 · MISSING 0 · ORPHANED 0 · 200/200 bytes
  //   imf_fsibsis_direct 43,814 catalogue == 43,814 R2 · MISSING 0 · ORPHANED 0 · 200/200 bytes
  // Titles decode from IMF's own codelists: 0 unresolved, 0 raw-key fallbacks. Licence is the
  // CONFIRMED IMF statistical-Data grant, and the three are now NAMED in
  // DATABASE_LICENSES_VERBATIM.md rather than inheriting it silently.
  "imf_fsibsis_direct", "imf_fsic_direct", "imf_fsicdm_direct",
  // IRFCL + CPI direct, added 2026-08-04. `imf_irfcl` (54,126) and `imf_cpi` (28,420) are
  // relay-era stores with NO fetcher, so neither has ever auto-updated. Built from IMF's own
  // /dataflow catalogue (IRFCL v12.0.0, CPI v5.0.0, both agency IMF.STA — read, not guessed).
  // Verified before flipping:
  //   imf_irfcl_direct 58,861 catalogue == 58,861 R2 == 58,861 D1 · MISSING 0 · ORPHANED 0 · 150/150 bytes
  //   imf_cpi_direct   27,094 catalogue == 27,094 R2 == 27,094 D1 · MISSING 0 · ORPHANED 0 · 150/150 bytes
  //
  // COVERAGE vs the relay copies, recorded because the ingester's header says "do not switch
  // blind": IRFCL 58,861 vs 54,126 = 1.09x MORE. CPI 27,094 vs 28,420 = 0.95x, i.e. SMALLER.
  // Nothing is switched or removed — these are new ids and imf_cpi keeps all 28,420 series — so
  // adding imf_cpi_direct is a strict addition of auto-updating coverage. But RETIRING imf_cpi in
  // favour of it would be a 4.7% coverage regression and is a reserved decision, not a follow-up.
  //
  // imf_bop_direct JOINS THEM 2026-08-04, and the reason it was held back is now fixed rather
  // than worked around. It was excluded because the cataloguer could not title it — 5 codelisted
  // dims against 7 key parts — which would have spent ~16% of D1 headroom on raw-key titles
  // nobody can search. The cause was not the codelists: the ingest RECORDS the authoritative key
  // order in a sidecar, but wrote it one directory above where the reader looks AND the reader
  // used open() on a store that lives in R2. Both fixed (R344); the recorded order titles it
  // completely.
  //   imf_bop_direct 260,931 catalogue == 260,931 R2 == 260,931 D1 · MISSING 0 · ORPHANED 0 · 150/150 bytes
  //   titles: 260,931/260,931 resolve 5/5, 0 blank, 0 fell back to the raw key
  // Same fix re-titled imf_cpi_direct in place: 27,094 better, 0 worse, ids unchanged
  // ("... — CPI — Index" is now "... — Consumer price index (CPI) — Index").
  "imf_irfcl_direct", "imf_cpi_direct", "imf_bop_direct",
  // imf_imts_direct added 2026-08-05 (cycle 2): TABLE grain — 2,937 catalog ids
  // (COUNTRY x FREQ x INDICATOR) serving 472,234 partner series inside their table CSVs.
  // Grain forced by arithmetic: D1 measured 9.31 GB of its 10 GB ceiling; series grain would
  // have consumed the library's entire remaining catalogue budget (#104/#45).
  "imf_imts_direct",
  // imf_pip_direct added 2026-08-05 (cycle 3): TABLE grain — 8,876 catalog ids
  // (COUNTRY x FREQ x INDICATOR, mid-key positions 4-6 of the 7-part PIP key) serving
  // 3,126,127 series inside their table CSVs. PIP = the CPIS successor per IMF's own
  // dataflow description; NOT the World Bank's `pip`. Grain forced by the same D1
  // arithmetic as IMTS (#105/#45): series grain would be ~6-7x the remaining budget.
  "imf_pip_direct",
  // imf_dip_direct added 2026-08-05 (cycle 4): TABLE grain — 5,180 catalog ids
  // (COUNTRY x FREQ x INDICATOR, mid-key positions 2/4/5 of the 5-part DIP key,
  // counterpart economy FIRST) serving 776,752 series inside their table CSVs.
  // DIP = the CDIS successor per IMF's own dataflow name ("formerly CDIS").
  // Same #45 D1 arithmetic: 5,180 rows ≈ 1% of remaining headroom.
  "imf_dip_direct",
  // imf_mfsdc_direct added 2026-08-05 (cycle 5): TABLE grain COUNTRY x FREQ — 539
  // catalog ids serving 36,506 series / 4,494,366 obs. MFS_DC is one of the four
  // flows IMF split the former MFS dataset into. C.F.I grain would have been 35,949
  // rows for 36,506 series (saves nothing); C.F costs ~0.1% of remaining D1
  // headroom (#45). Dims sit at the key PREFIX — plain starts_with resolver.
  "imf_mfsdc_direct",
  // imf_mfsma_direct added 2026-08-05 (cycle 5): TABLE grain COUNTRY x FREQ — 468
  // catalog ids serving 3,016 series / 344,652 obs. Monetary aggregates flow of the
  // MFS split; same measured shape and prefix resolver as MFS_DC.
  "imf_mfsma_direct",
  // imf_mfsofc_direct + imf_mfsfmp_direct added 2026-08-05 (cycle 5): TABLE grain
  // COUNTRY x FREQ — 231 ids / 4,704 series and 207 ids / 276 series. Other
  // financial corporations + financial markets and positions flows of the MFS
  // split; same measured shape and prefix resolver as MFS_DC/MA.
  "imf_mfsofc_direct", "imf_mfsfmp_direct",
  // imf_mfsir_direct added 2026-08-05 (cycle 5, closing the family): TABLE grain
  // COUNTRY x FREQ — 510 ids / 3,382 series / 537,710 obs. Interest-rates flow,
  // the fifth MFS sibling (found by the IFS-families probe); 4-part keys (no tail
  // dim) but the same positions-1/2 prefix, so the shared resolver applies.
  "imf_mfsir_direct",
  // imf_bopagg_direct added 2026-08-05 (cycle 6): TABLE grain COUNTRY x FREQ — 208
  // ids serving 7,839 series / 140,907 obs. BOP_AGG = the renamed BOPAGG (headline
  // BOP/IIP aggregates; distinct from the served BOP detailed flow). 6-part keys
  // with a sometimes-empty phantom part; positions 1/2 are the C.F prefix, so the
  // shared MFS-family resolver applies.
  "imf_bopagg_direct",
  // imf_psbs_direct added 2026-08-05 (cycle 7): TABLE grain COUNTRY x FREQ — 86 ids
  // serving 14,018 series / 209,229 obs. PSBS = the renamed PSBSFAD (agency
  // IMF.FAD); distinct-series count EXACTLY equals the frozen legacy catalogue,
  // the R75 same-dataset proof. Clean 5-dim keys, C.F prefix resolver.
  "imf_psbs_direct",
  // imf_ctot_direct added 2026-08-05 (cycle 9): TABLE grain COUNTRY x FREQ — 360 ids
  // serving 4,320 series / 1,264,128 obs. CTOT = the renamed PCTOT (agency IMF.RES);
  // distinct-series count EXACTLY equals the frozen legacy catalogue (R75). Clean
  // 4-dim keys, C.F prefix resolver.
  "imf_ctot_direct",
  // imf_sdg_direct added 2026-08-05 (cycle 10): SERIES grain — 2,577 series /
  // 42,789 obs (0.5% of D1 headroom; table grain saves nothing at this size).
  // SDG = the renamed UNSDG IMF-inputs dataset. 16-part UN-SDMX keys decoded via
  // the IAEG-SDGs codelists (dimension-level enumeration fallback in
  // imf_direct_titles). Served by the generic uniform-long resolver.
  "imf_sdg_direct",
  // imf_namain_direct added 2026-08-05 (cycle 11): SERIES grain — 1,969 series /
  // 86,928 obs (0.4% of D1 headroom). NA_MAIN = the renamed NAMAIN_IDC_N national
  // accounts aggregates; 22-part ESA/SNA keys decoded via the recovered dims
  // sidecar (the _staging_ strand bug's third occurrence). Generic resolver.
  "imf_namain_direct",
  // imf_icsd_direct added 2026-08-05 (cycle 12): SERIES grain — 3,195 series /
  // 155,840 obs (0.6% of D1 headroom). ICSD = the pgcs successor by name and
  // content, and a coverage SUPERSET of the frozen legacy (3,195 vs 2,262).
  // First source whose dims sidecar arrived canonically via the carry fix.
  "imf_icsd_direct",
  // imf_fd_direct added 2026-08-05 (cycle 13): SERIES grain — 8,398 series /
  // 160,957 obs, EXACTLY the frozen legacy count (R75). Fiscal decentralization,
  // titles resolved 0-fallback via the carried sidecar. Generic resolver.
  "imf_fd_direct",
  // imf_hpd_direct added 2026-08-05 (cycle 14): SERIES grain — 191 series /
  // 9,628 obs, EXACTLY the frozen legacy count (R75). Historical public debt,
  // titles resolved 0-fallback. Generic resolver.
  "imf_hpd_direct",
  // GS gender family added 2026-08-05 (cycle 15) — five flows replacing the frozen
  // 583-series legacy pair with 109,417 series total:
  //   imf_gslgrghts_direct  8,084 series (legal rights)        — series grain
  //   imf_gslepm_direct       973 series (labor/employment)    — series grain
  //   imf_gssdo_direct      7,599 series (seats/decision)      — series grain
  //   imf_gsatf_direct     12,057 series (access to finance)   — series grain
  //   imf_gsli_direct      80,394 series via 233 C.F TABLE ids — the #45 arithmetic
  //     (a 345x collapse; COUNTRY.FREQ sit MID-KEY at positions 3-4 of 11, so it
  //     gets the position-anchored resolver, not the family prefix)
  "imf_gslgrghts_direct", "imf_gslepm_direct", "imf_gssdo_direct",
  "imf_gsatf_direct", "imf_gsli_direct",
  // imf_er_direct added 2026-08-05 (cycle 16): TABLE grain COUNTRY x FREQ — 681 ids
  // serving 10,474 series / 2,427,666 obs (15x collapse, the R356 arithmetic run
  // explicitly). NEW coverage: the successor home of dismembered IFS's orphaned
  // exchange-rate family. C.F prefix resolver.
  "imf_er_direct",
  // imf_eer_direct added 2026-08-05 (cycle 17): SERIES grain — 732 series /
  // 179,736 obs (a 2x C.F collapse makes tables pointless; the R356 arithmetic
  // picks per-series titles and search). Effective exchange-rate indices, new
  // coverage. Generic resolver.
  "imf_eer_direct",
  // imf_ls_direct added 2026-08-05 (cycle 18): SERIES grain — 4,160 series /
  // 339,635 obs (10x C.F collapse, but at 0.8% of headroom per-series search
  // wins). Labor statistics — new coverage for IFS's orphaned L* family.
  "imf_ls_direct",
  // imf_pi_direct added 2026-08-05 (cycle 19): SERIES grain — 3,100 series /
  // 447,095 obs. Production indexes — new coverage for IFS's AIP family.
  "imf_pi_direct",
  // imf_piwca_direct added 2026-08-05 (cycle 20): SERIES grain — 16 world/group
  // aggregate production-index series / 2,168 obs. PI's aggregate companion.
  "imf_piwca_direct",
  // imf_qgfs_direct added 2026-08-05 (cycle 21 — the LAST actionable queue item):
  // TABLE grain COUNTRY x FREQ — 267 ids serving 20,502 series / 1,243,439 obs
  // (77x collapse, the #45 arithmetic run explicitly; PSBS at 14,018 already went
  // table grain). Quarterly GFS, new coverage. COUNTRY.FREQ sit MID-KEY at
  // positions 2-3 of 7, so the position-anchored resolver, not the family prefix.
  "imf_qgfs_direct",
  // imf_mcdreo_direct added 2026-08-06 (cycle 32, Class B1): SERIES grain — 623 series /
  // 17,997 obs from flow MCDREO v8.0.0 (agency IMF.MCD). Ahmed's "match the publisher"
  // ruling resolves the 57%-of-legacy scope; the frozen imf_mcdreo retires via Class A.
  "imf_mcdreo_direct",
  // imf_fm_direct added 2026-08-06 (cycle 34 — Class B1 COMPLETE): SERIES grain — 128
  // series / 5,077 obs from flow FM v5.0.0 (agency IMF.FAD). The thinnest direct (9% of
  // relay scope), resolved by the same ruling; the frozen imf_fm retires via Class A.
  "imf_fm_direct",


  // hf_equities REMOVED 2026-08-06 (Ahmed: "remove hf") — 1,391 metadata-only listings econ
  // could never serve (R29); catalog + D1 rows deleted the same day, zero R2 objects existed.
  "idb", "ilostat", "imf_commodity", "imf_fsire", "imf_gender_budgeting", "imf_gfsfalcs", // Four GFS *_direct sources added 2026-08-02 — 549,843 series / 8,853,880 observations that
  // did not exist before today. All four had held ZERO rows and failed every run with
  // OverflowError('size does not fit in an int') or ParseError('out of memory'), which I first
  // read as pyexpat's 2 GiB document ceiling. It was not: GFS_BS parses at 2,293,565,648 bytes
  // on a workstation. The cause was a retained-node leak in the ingest's iterparse loop —
  // el.clear() empties an element but its PARENT keeps holding it, so the tree grew by one node
  // per series (297,673 for GFS_BS) and never freed. Invisible on 383 GB, fatal on a 16 GB
  // runner. Detaching from the parent took the same pull to a 137 MB peak with byte-identical
  // output (R223).
  //   imf_gfssoo_direct 319,571 · imf_gfscofog_direct 124,237 · imf_gfsbs_direct 64,284 ·
  //   imf_gfssfcp_direct 41,751
  // Each derived with put == streamed and errors 0, and titles DECODED (fallback count 0) —
  // the GFS flows still resolve their SDMX structures, unlike the retired IFS/DOT/CDIS/CPIS.
  "imf_gfsbs_direct", "imf_gfscofog_direct", "imf_gfssfcp_direct", "imf_gfssoo_direct",
  
  "imf_pgi", 
  "imf_weo", // Eight IMF datasets added 2026-08-01, 694,300 series over 37,971,568 observations. They held
  // real data and ZERO catalogue rows, while the correspondingly-named imf_*_direct sources the
  // registry schedules hold nothing at all: the serving pipeline was built against the _direct
  // names and the crawler filled these. imf_fsi already served from exactly this layout, so it
  // was a template rather than a new design, and _resolve_generic_long needed no change.
  // Each verified both directions before being listed here: MISSING 0, ORPHANED 0, and the
  // derive self-checked 300/300 byte-identical against core.derive_csv before writing an object.
  //   imf_ifs 100,706 · imf_dot 101,000 · imf_cpis 100,783 · imf_bop 99,636
  //   imf_cdis 97,723 · imf_mfs 88,271 · imf_irfcl 54,126 · imf_gfsr 52,055
  // Titles are RAW KEYS, deliberately and not for want of trying: IMF retired both the dataflow
  // ids (IFS/DOT/CDIS/CPIS answer 204, while BOP and IRFCL still return 4 MB structures) and the
  // code vocabulary (the stored IRFCL key decodes A->Annual and S121->Central bank, but '4F' is
  // absent from COUNTRY because COUNTRY is now ISO-3). Fixing them needs an area-code -> ISO-3
  // crosswalk, not a flow rename. Downloadable now; searchable when that lands.
  "imf_ifs", "insee_bdm", "ipea",
  // "ksh" RETIRED 2026-07-29 (Ahmed-approved). 394 of its 415 tables were already in
  // ksh_stadat, which carries MORE series for them; its fetcher could not even import
  // (jobs/ingest_ksh_hungary.py does not exist). Its 21 unique tables — 903 series — were
  // MIGRATED into ksh_stadat first and verified live, so nothing was lost: 719 that KSH has
  // retired (not re-crawlable) and 184 that KSH still publishes but ksh_stadat's parser
  // skips with "no parseable time dimension". Verified before removal: 0 ksh tables absent
  // from ksh_stadat. The parquets under clean_full/ksh/ are KEPT, so this is reversible.
  // ksh WITHDRAWN 2026-08-02, the same day I added it, because adding it was the mistake.
  // jobs/ingest_ksh_hungary.py was DELETED on 2026-07-02 by an explicit owner decision —
  // "Retire ... ingest_ksh_hungary.py (ksh_stadat is the owner)", under a stated "one owner per
  // source" policy. The store it left behind is frozen at 2026-06-23 (25 files, 512,995 rows)
  // while ksh_stadat is current to 2026-07-29 with 1,260,990 rows over 98,423 series — four
  // times the data and actually updating. Serving ksh published a stale duplicate of a source
  // the library already owns properly.
  // What misled me: broaden_catalog's hostability gate passed it (the licence IS cc-by-4.0
  // CLEARED), and a key-overlap sample showed 0 of 109 ksh keys inside ksh_stadat, which I read
  // as "distinct data" when it only meant "different key convention" — the same trap ilo/ilostat
  // set. A retirement decision is not visible in either check; it lives in git history and in
  // the absence of an ingest file (R226).
  // owid REMOVED 2026-08-06 (Ahmed: "remove owids") — 64 residual listed rows deleted from
  // catalog + D1; licence DISPUTED, the gated store on R2 stays untouched, denylist entry kept.
  // norgesbank added 2026-08-06 (cycle 37, Ahmed's authorized serve): 35,135 series /
  // 3,768,215 obs rebuilt in full from data.norges-bank.no (run 31129475260) after the
  // 07-23 purge; NLOD 2.0 CLEARED, floor pin removed the same day (see gen_denylist.py).
  "kof_globalization", "ksh_stadat", "maddison", "nasa_giss", "noaa", "norgesbank", "nyfed", "oecd", "ofr", "oxcgrt",
  "penn_world_table", "pip", "pwt", "rba", "riksbank",
  "sec_edgar", "snb", "statcan", "stats_nz",
  "swiid", "transparency_ti", "treasury", "ucdp", "unctad_bopcaba",
  "unctad_ciocgeaia", "unctad_cioiuibbicoeair4a", "unctad_cpia", "unctad_cpta", "unctad_fdiiaofasa",
  "unctad_fmcpa", "unctad_fmcpia21", "unctad_gasbeaiogasa", "unctad_gasbtbia", "unctad_gasbtoia", "unctad_gdpgbtoevbkoeatasa",
  "unctad_gdptapccac2pa", "unctad_lscia", "unctad_lsciq", "unctad_mfbcoboa", "unctad_mmcascioeaiopa", "unctad_mpcadioeaia",
  "unctad_mtba", "unctad_mttasa", "unctad_mttgra", "unctad_neera", "unctad_reericba", "unctad_reerigdba",
  "unctad_rfia", "unctad_rgdptapcgra", "unctad_sbeaiotsvsaga", "unctad_sbtisvsaga", "unctad_soigapotta", "unctad_sotwmfvbcoboa",
  "unctad_srbca", "unctad_tabbapotta", "unctad_tabmcioeaiopa", "unctad_tabmscioeaiopa", "unctad_tabpcioeaia", "unctad_taupa",
  // unctad_trademerchtotal added 2026-08-08: FIRST successor source on UNCTAD's own data
  // API (unctadstat-user-api, ClientId/ClientSecret from .env — contract proven live).
  // 1,220 series / 81,760 obs, series_key = Economy.Flow.Mmeasure. The 38 legacy
  // DBnomics-era slugs above retire via Class A as successors land (#70).
  "unctad_trademerchtotal", "unctad_trademerchgr", "unctad_trademerchbalance",
  "unctad_merchvolumequarterly", "unctad_termsoftrade", "unctad_tradepriceindicesq",
  "unctad_concentdiversindices", "unctad_concentstructindices", "unctad_rca",
  "unctad_tariff", "unctad_merchtheilindices", "unctad_totandcomservicesquarterly",
  "unctad_ucpia", "unctad_ucpim", "unctad_commoditypriceindicesa",
  "unctad_commoditypriceindicesm", "unctad_commoditypricea", "unctad_commoditypricem",
  "unctad_ifftrademisinvoicing", "unctad_ictproductionsector", "unctad_iffcrimesrelated",
  "unctad_creativeservgroupe", "unctad_sdgporfvol", "unctad_contportthroughput",
  "unctad_shipscrapping", "unctad_vesselvaluebyownership", "unctad_shipbuilding",
  "unctad_vesselvaluebyregistration", "unctad_biotrademerchgdpshare", "unctad_sdglulfrg",
  "unctad_biotrademerchprodconcent", "unctad_curraccbalance",
  // unctad_cpi_annual, NOT "unctad_cpia": that is the LEGACY source above (R399).
  "unctad_cpi_annual",
  "unctad_lscim", "unctad_gni", "unctad_inclusivegrowth", "unctad_lsci",
  "unctad_portcallsarrivals", "unctad_portcallsarrivalss", "unctad_remittances",
  "unctad_poptotal", "unctad_popdependency", "unctad_ictuselocation",
  "unctad_creativeservindivtot", "unctad_ftri", "unctad_seafarers", "unctad_plsci",
  "unctad_portcalls", "unctad_portcallss", "unctad_gdptotal",
  "unctad_genderdomesticvalueadded", "unctad_tradeservict",
  "unctad_wastewatertreatment", "unctad_fdiflowsstock", "unctad_pci",
  "unctad_seabornetrade", "unctad_goodsandservicesbpm6", "unctad_goodsandservbalancebpm6",
  "unctad_govexpenditures", "unctad_gendertradableindustries", "unctad_environmentalgoodsrca",
  "unctad_goodsandservtradeopennessbpm6", "unctad_oceanservices",
  "unctad_biotrademerchmarketindices", "unctad_merchantfleet", "unctad_ictuseenterprsize",
  "unctad_gdpcomponent", "unctad_ictuseeconactivity", "unctad_tradefoodproccatcatrca",
  "unctad_tradefoodproccatprocrca", "unctad_popagestruct", "unctad_fleetbeneficialowners",
  "unctad_environmentalgoodstrade", "unctad_digitallydeliverableservices", "unctad_lsbci",
  "unctad_exchangeratecrosstab", "unctad_ictuseeconactivityisic4",
  // Gate cases at DOT-prefix table grain (1,353 ids / 622,540 series, #70):
  "unctad_intratrade", "unctad_tradeservcattotal", "unctad_tradeservcatbypartner", "unctad_biotrademerchshare",
  "unctad_biotrademerchrca", "unctad_biotrademerch", "unctad_ecommercetotal",
  "unctad_associatedplasticstradebypartner", "unctad_hiddenplasticstradebypartner",
  "unctad_ecommerceinternational", "unctad_plasticstradebypartner", "unctad_ictgoods",
  "unctad_gstptradematrix", "unctad_creativegoodsvalue",
  "unctad_creativegoodsgr", "unctad_oceantrade",
  "unctad_criticalmineralstradebypart", "unctad_nonplasticsubststradebypartner",
  "unctad_wstbtocabgoea", "undp_hdr", "unesco_clte", "unesco_cltt", "unesco_dem", "unesco_film",
  // unesco_natmon + unesco_sdg added 2026-07-29: 199,661 series / 2,610,984 obs that
  // were on disk and hosted NOWHERE — no catalog rows, no R2 objects, no registry unit.
  // Re-ingested direct from UIS, MISSING 0 / ORPHANED 0, values checked against parquet,
  // and un-pinned from the denylist floor on Ahmed's decision (the UIS terms are
  // publisher-wide, so their five cleared siblings above already covered them).
  // unesco_sci stays OUT: only 12 of its 1,230 indicator codes exist in the current UIS
  // API, so it cannot be kept current and would be a frozen 2019 snapshot.
  // unsdg added 2026-08-07: 396 SDG indicator series that were CATALOGUED BUT NOT RESOLVABLE —
  // findable in search and a 501 on download, which is the worst of the three states to be in.
  // The data plane was already complete and nobody had flipped the flag: 396 catalogue rows
  // against 396 R2 objects (MISSING 0, ORPHANED 0), byte-compare 30/30 identical against a
  // mirror the footer diff proved level with the store (1 of 1 parquet).
  // D1 WAS SYNCED FIRST — 396 series rows plus the parent `source` and `un-data-terms` licence
  // rows — because flipping this array while D1 is empty converts a 501 into a 404 and calls it
  // progress. Verified after the sync: series 396, source row "UN SDG Indicators Database".
  // Licence CLEARED with attribution (UNdata Terms of Use): the data "may be copied freely,
  // duplicated and further distributed provided that UNdata is cited as the reference".
  // Coverage is honest rather than complete: the source is live:true on a weekly check and its
  // backfill still defers ~591 sub-units per pass, so this serves 396 indicators now and grows.
  "unesco_inno", "unesco_natmon", "unesco_sdg", "unhcr", "unsdg", "usda", "wgi", "who_hwf", "who_rs",
  // wid added 2026-07-29 — the largest single source in the library: 2,465,197 series /
  // ~124M observations of World Inequality Database data that were held locally and
  // served to NOBODY. Added only after the derive COMPLETED and was verified
  // (catalog 2,465,197 == R2 CSVs 2,465,197, missing 0); listing it earlier would have
  // served 404s. CC BY-NC-SA 4.0 + written grant (Alice, info@wid.world, 2026-07-27).
  "wid",
  // imf_fsi (73,288) + adb (53,458) added 2026-07-29 — two of the four series-shaped,
  // licence-verified sources that were idle and catalogued nowhere. Both derived and
  // verified before listing (MISSING 0, ORPHANED 0). adb's terms are KIDB's own, not
  // the ADB Data Library's, and its attribution carries KIDB's prescribed citation.
  "adb",
  // cso (Ireland) — flow-grain per-table publish, 7,896 tables / 49,057,386 rows
  // (2026-07-29). Table grain is what makes it hostable: per SERIES it is 9,993,368 keys
  // at 4.90 obs/series because CSO publishes many short cross-sectional tables, and at
  // table grain a thin table costs exactly one row and one CSV, same as a dense one.
  // 92 of its tables span two subject parquets; those are assembled before upload and all
  // 92 were verified row-for-row against the store (see ledger R133), because presence
  // checks pass on a truncated file.
  "cso",
  // insee_melodi — flow-grain per-dataflow publish, 139 flows / 36,436,053 rows
  // (2026-07-29). Flow grain because no codelist RETRIEVAL PATH was found — /codelist/all and
  // /dsd/{id} both 404 and a flow's catalog entry only names a DSD. That is "not found", NOT
  // "does not exist" (ledger R145); if a path turns up, per-series titles become possible.
  // On what is known today, per-series ids could only be titled with the key
  // itself — 21.3M rows of opaque codes. The flow is the unit INSEE actually titles, and
  // 134/139 carry its own label. It also hosts the source WHOLE: 84 flows are single-period
  // censuses (DS_BPE*, DS_FLORES_*) that are honest cross-sectional micro-data, not the
  // date-in-the-key defect ons_uk has. Licence audited 2026-07-29 (etalab-2.0, same
  // publisher and API host as the already-confirmed insee_bdm).
  "insee_melodi",
  // istat added 2026-08-01 — FLOW grain, 14,267 catalogue rows over 14,258 R2 objects.
  // 398,619,720 observations across 43,564,079 series is 9.2 obs per series, so series grain
  // would have meant 43.5M CSVs averaging nine rows each; the unit is the dataflow. The 123
  // flows over 500,000 rows are split on one of their own dimensions — or on a PAIR of them:
  // three flows holding 96,725,407 rows had no single dimension that divided them and were
  // refused by the first derive, under a summary that said "errors 0, skipped 0" (R219).
  // The split choice per flow lives in _split_map.json beside the store and the resolver
  // reads it; without it a part id like `istat:101_1015#ART` means nothing. Licence CC BY 4.0,
  // CLEARED, quoted verbatim from ISTAT's own legal notice.
  "istat",
  // ons_uk — dataset-grain publish, 42 datasets / 25,408,157 rows / 3,897,884 series
  // (2026-07-29), after the approved re-key. The stored ids used to embed the observation
  // date AND `CV` (a coefficient of variation — a property of one measurement), so every
  // row was its own series: 25,408,157 rows, 25,408,157 keys, and a cursor dict that hit
  // 32.26 GB RSS on a 16 GB runner. Re-keyed to 3,897,884 real series with 0 (key,date)
  // collisions. Dataset grain, so the CATALOG holds 42 rows and all 42 carry ONS's own dataset
  // title (0 titled by their key — verified). Dropping the label columns from the KEY is
  // deliberate: ONS re-words display strings, and baking one into an id invites silent
  // re-keying. The opaque `dim=code` strings a user sees are the native series_id column INSIDE
  // a downloaded CSV — the data payload, which is what it should be, and identical in shape to
  // cso and insee_melodi. An earlier revision of this comment claimed ons_uk was "under-titled";
  // that was wrong and is retracted (ledger R145/R146): it confused payload keys with catalog
  // titles. ons_uk has no titling defect.
  // OGL v3.0; the "some content is exempt" carve-out resolves to photographs and video,
  // which cannot reach a statistical series.
  "ons_uk",
  // un_wpp — UN World Population Prospects 2024, 334,236 series (2026-07-29). Derive
  // verified both directions (catalog 334,236 == R2 334,236, MISSING 0, ORPHANED 0) and the
  // download body confirmed. Titles are CURRENTLY the native key — and the comment here used
  // to justify that with "WPP publishes no per-series title", which is WRONG and is corrected
  // rather than quietly deleted (ledger R145). The INDICATOR long name is genuinely absent from
  // WPP's CSVs, but the COUNTRY name is present: ingest_un_wpp.py reads it at line 100 and
  // discards it at line 128 because ISO3 is set. So readable titles ARE derivable from the
  // publisher's own file and this source is under-titled pending that fix. Date ranges
  // ARE real (computed per series from the two published parquets, 334,236/334,236 dated).
  // CC BY 3.0 IGO — and note the licence nearly went the other way: un.org's site-wide notice
  // reads "All rights reserved", but the Population Division publishes its own CC BY 3.0 IGO
  // grant on the WPP download page, which governs this product. D1 held a pre-audit
  // NEEDS-REVIEW default for this source; that divergence was corrected before serving.
  "un_wpp",
  "who_sdg", "whr", "wikidata", "worldbank", "worldbank_esg", "worldbank_pink",
  "worldbank_wdi", "yale_epi", // 9 national-statistical PxWeb sources — flow-grain per-table publish (2026-07-22).
  "ssb", "stat_slovenia", "stat_latvia", "dst", "scb", "statfin", "hagstofa", "stat_estonia", "bfs",
];

// Languages with OFFICIAL, source-provided translations loaded into the catalog
// (stored at series.metadata.titles[<lang>]). 'en' is the native `title` column.
// Translations are NEVER machine-generated -- only labels the producer itself
// publishes. KEEP IN SYNC with api/devserver.py::_LANGS. A ?lang= outside this
// set is a 400 (we never silently hand back English for a language we lack).
export const SUPPORTED_LANGS: readonly string[] = ["en", "ar", "es", "fr", "ru", "zh"];

/** The provider segment of a catalog id: the first ':'-delimited token.
 *  Mirrors econdl._catalog.source_of. */
export function sourceOf(seriesId: string): string {
  const i = seriesId.indexOf(":");
  return i === -1 ? seriesId : seriesId.slice(0, i);
}

// --- D1 SHARDING (task #45). The primary econ-catalog measured 9.35 GB of its
// 10 GB per-database ceiling; these sources' `series` + `series_fts` rows live in
// the CATALOG_CLIMATE shard instead. Their `source`/`license` rows stay in the
// PRIMARY (so /v1/sources and provenance need no cross-DB union), and
// unit_state/source_state stay primary too (the state sync only targets it).
// Every series-table query must therefore route through dbFor()/dbForSeries();
// GLOBAL series queries (unscoped search/browse/counts) must hit BOTH databases
// and merge, or sharded sources silently vanish from search — see catalog.ts.
export const SHARDED_SOURCES: ReadonlySet<string> = new Set(["noaa"]);

export function dbFor(env: Env, source: string | null | undefined): D1Database {
  return source && SHARDED_SOURCES.has(source) ? env.CATALOG_CLIMATE : env.CATALOG;
}

export function dbForSeries(env: Env, seriesId: string): D1Database {
  return dbFor(env, sourceOf(seriesId));
}

export function supportedSources(env: Env): Set<string> {
  if (env.SUPPORTED_SOURCES && env.SUPPORTED_SOURCES.trim()) {
    return new Set(env.SUPPORTED_SOURCES.split(",").map((s) => s.trim()).filter(Boolean));
  }
  return new Set(SUPPORTED_SOURCES);
}

const JSON_HEADERS: Record<string, string> = {
  "content-type": "application/json; charset=utf-8",
  "access-control-allow-origin": "*",
  "cache-control": "public, max-age=300",
};

const CSV_HEADERS: Record<string, string> = {
  "content-type": "text/csv; charset=utf-8",
  "access-control-allow-origin": "*",
  "cache-control": "public, max-age=300",
};

export function json(body: unknown, status = 200, extra?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...JSON_HEADERS, ...(extra ?? {}) },
  });
}

export function csv(body: string, extra?: Record<string, string>): Response {
  // Exact UTF-8 byte length so the download gate can record real "data served"
  // (econ_download_log.bytes) without re-reading the streamed body.
  const bytes = new TextEncoder().encode(body).length;
  return new Response(body, {
    status: 200,
    headers: { ...CSV_HEADERS, "content-length": String(bytes), ...(extra ?? {}) },
  });
}

// --- honest-status error bodies (machine-readable `error` codes) -----------

/** 404: the id is not in the catalog. */
export function notFound(seriesId: string): Response {
  return json({ error: "not_found", series_id: seriesId, detail: "unknown series id" }, 404);
}

/** 501: the source has no resolver yet (loud, actionable). Mirrors ResolveError. */
export function notMigrated(source: string, seriesId: string): Response {
  return json(
    {
      error: "not_migrated",
      source,
      series_id: seriesId,
      detail:
        `source '${source}' has no at-rest resolver yet; the store is mid-migration. ` +
        `This series cannot be served until its source is wired up. Refusing to ` +
        `silently emit an empty response.`,
    },
    501,
  );
}

/** 502: the source is migrated (has a resolver) but its at-rest object/file is
 *  ABSENT (not published yet). Distinct from resolver_empty (object present, 0
 *  rows) and from not_migrated (no resolver at all). CONTRACT.md v1.1 status pin. */
export function dataUnavailable(source: string, seriesId: string): Response {
  return json(
    {
      error: "data_unavailable",
      source,
      series_id: seriesId,
      detail:
        `source '${source}' is migrated but the at-rest object for this series is ` +
        `not published yet. Refusing to silently emit an empty response.`,
    },
    502,
  );
}

/** 502: the id resolves to a file/object but it has zero rows. */
export function resolverEmpty(seriesId: string): Response {
  return json(
    {
      error: "resolver_empty",
      series_id: seriesId,
      detail:
        "the series id resolves to a stored object but it has zero observations. " +
        "Refusing to emit an empty series silently.",
    },
    502,
  );
}

/** 400: a filter the store cannot honor yet (never a silently unfiltered 200). */
export function unsupportedFilter(detail: string): Response {
  return json({ error: "unsupported_filter", detail }, 400);
}

export function badRequest(detail: string): Response {
  return json({ error: "bad_request", detail }, 400);
}

// --- license block: identical shape used by metadata.json and /v1/sources ---

export function licenseBlock(lic: LicenseRow | null): LicenseBlock | null {
  if (!lic || !lic.license_id) return null;
  return {
    id: lic.license_id,
    name: lic.name,
    url: lic.url,
    reservable: !!lic.reservable,
    commercial_ok: !!lic.commercial_ok,
    attribution_required: !!lic.attribution_required,
    no_modify: !!lic.no_modify,
  };
}

// --- cadence math: last_success + interval, HONEST about unknown cadences ---

// CONTRACT.md spells out {daily:1d, weekly:7d, monthly:30d, quarterly:91d}.
// The live state.db cadence vocabulary is {daily, weekly, monthly, annual,
// static, irregular, null}. We extend the contract's table with annual:365d
// (the only other deterministic cadence) and -- per "never fabricate a date" --
// return null next_update_expected for static/irregular/unknown/null cadences
// instead of inventing one. quarterly is kept for forward-compat though the
// current state.db has no quarterly rows.
const CADENCE_DAYS: Record<string, number> = {
  daily: 1,
  weekly: 7,
  monthly: 30,
  quarterly: 91,
  annual: 365,
};

/** last_success_utc + cadence interval -> ISO date (YYYY-MM-DD), or null when
 *  the cadence is non-deterministic / unknown / there is no last_success. */
export function nextUpdateExpected(
  lastSuccessUtc: string | null,
  cadence: string | null,
): string | null {
  if (!lastSuccessUtc || !cadence) return null;
  const days = CADENCE_DAYS[cadence];
  if (days === undefined) return null; // static / irregular / unknown -> honest null
  const t = Date.parse(lastSuccessUtc);
  if (Number.isNaN(t)) return null;
  const next = new Date(t + days * 86_400_000);
  return next.toISOString().slice(0, 10);
}

/** clamp(parse(limit), 1, max) with a default; for /v1/catalog pagination. */
export function clampInt(raw: string | null, dflt: number, min: number, max: number): number {
  if (raw === null) return dflt;
  const n = Number.parseInt(raw, 10);
  if (Number.isNaN(n)) return dflt;
  return Math.max(min, Math.min(max, n));
}

export function offsetInt(raw: string | null): number {
  if (raw === null) return 0;
  const n = Number.parseInt(raw, 10);
  return Number.isNaN(n) || n < 0 ? 0 : n;
}

// --- i18n: ?lang= resolution + official-title localization -----------------
// Byte-for-byte with api/devserver.py::_req_lang / _localized_title so the
// Worker and the dev shim localize identically.

/** Resolve ?lang=. Returns {lang:'en', error:null} for absent/'en' (the response
 *  is then byte-identical to the pre-i18n contract). For a language we have no
 *  official translations for, returns {lang:'en', error:<400 Response>}; the
 *  caller must return that error -- never a silent English fallback. */
export function reqLang(url: URL): { lang: string; error: Response | null } {
  const raw = (url.searchParams.get("lang") ?? "").trim().toLowerCase();
  if (!raw || raw === "en") return { lang: "en", error: null };
  if (!SUPPORTED_LANGS.includes(raw)) {
    return {
      lang: "en",
      error: json(
        {
          error: "unsupported_language",
          parameter: "lang",
          value: raw,
          detail: `no official translations loaded for '${raw}'`,
          supported: [...SUPPORTED_LANGS],
        },
        400,
      ),
    };
  }
  return { lang: raw, error: null };
}

/** The source-official title for `lang` (meta.titles[lang]) when present, else
 *  the English native title (graceful fallback). `meta` is the PARSED series
 *  metadata object (or null). lang None/'en' returns the English title. */
export function localizedTitle(
  meta: { titles?: Record<string, string> } | null | undefined,
  enTitle: unknown,
  lang: string | null,
): unknown {
  if (!lang || lang === "en") return enTitle;
  const titles = meta && typeof meta === "object" ? meta.titles : null;
  if (titles && typeof titles === "object" && titles[lang]) return titles[lang];
  return enTitle;
}
