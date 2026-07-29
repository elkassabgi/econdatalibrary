# Auto-update coverage of SERVED sources (generated 2026-07-29)

Served = catalogued AND present in the worker resolver: **201 sources / 4,740,072 series**.
Refreshed on a schedule (updater-daily `live: true`, the updater-heavy matrix, or a dedicated workflow): **58**.
NOT refreshed: **143 sources / 1,314,126 series**.

Not-refreshed is not automatically a defect: a one-off academic release (Maddison,
CEPII Gravity, Barro-Lee) is CORRECTLY frozen, and saying otherwise would be the
aggregate error of R111/R127 again. The split below is on the registry's own cadence,
so the two cases are not conflated — an ONGOING statistical series that never
refreshes is the actual problem; a static release is not.

## A. Ongoing cadence but NOT refreshing — 40 sources / 306,819 series

These have a real cadence in the registry and still sit outside the nightly run.
This is the work queue for "hosted data current + auto-updating".

| source | series | cadence | strategy | in registry |
|---|---:|---|---|---|
| `insee_bdm` | 101,848 | monthly | sdmx_delta | True |
| `imf_fsi` | 73,288 | monthly | extend_by_date | True |
| `adb` | 53,458 | annual | sdmx_delta | True |
| `ksh` | 25,057 | annual | overwrite_if_changed | True |
| `imf_fas_direct` | 14,081 | annual | bulk_snapshot_if_changed | True |
| `eurostat` | 7,637 | monthly | giant_changed_units | True |
| `ssb` | 5,568 | monthly | sdmx_delta | True |
| `worldbank_esg` | 4,447 | weekly | extend_by_date | True |
| `stat_slovenia` | 4,134 | monthly | sdmx_delta | True |
| `stat_estonia` | 3,437 | monthly | sdmx_delta | True |
| `imf_world_direct` | 3,244 | annual | bulk_snapshot_if_changed | True |
| `imf_fdi_direct` | 1,728 | annual | bulk_snapshot_if_changed | True |
| `statfin` | 1,539 | monthly | sdmx_delta | True |
| `worldbank_wdi` | 1,486 | quarterly | extend_by_date | True |
| `stats_nz` | 1,320 | quarterly | bulk_snapshot_if_changed | True |
| `ipea` | 1,241 | monthly | extend_by_date | True |
| `hagstofa` | 1,068 | monthly | sdmx_delta | True |
| `comtrade` | 713 | annual | extend_by_date | True |
| `wikidata` | 250 | monthly | overwrite_if_changed | True |
| `bea` | 240 | monthly | extend_by_date | True |
| `imf_cofer_direct` | 140 | quarterly | bulk_snapshot_if_changed | True |
| `insee_melodi` | 139 | monthly | sdmx_delta | True |
| `imf` | 131 | monthly | bulk_snapshot_if_changed | True |
| `ilostat` | 80 | monthly | bulk_snapshot_if_changed | True |
| `owid` | 64 | monthly | bulk_snapshot_if_changed | True |
| `fhfa` | 61 | monthly | bulk_snapshot_if_changed | True |
| `ember` | 60 | monthly | bulk_snapshot_if_changed | True |
| `zillow` | 52 | monthly | bulk_snapshot_if_changed | True |
| `bis` | 49 | monthly | bulk_snapshot_if_changed | True |
| `faostat` | 47 | monthly | bulk_snapshot_if_changed | True |
| `ecb` | 35 | daily | sdmx_delta | True |
| `oecd` | 28 | monthly | giant_changed_units | True |
| `usda` | 25 | monthly | bulk_snapshot_if_changed | True |
| `defillama` | 24 | daily | overwrite_if_changed | True |
| `census` | 22 | monthly | extend_by_date | True |
| `fed_board` | 21 | daily | bulk_snapshot_if_changed | True |
| `statcan` | 20 | weekly | extend_by_date | True |
| `abs` | 18 | monthly | sdmx_delta | True |
| `noaa` | 10 | monthly | bulk_snapshot_if_changed | True |
| `bls` | 9 | weekly | bulk_snapshot_if_changed | True |

## B. Static / irregular / unregistered — 103 sources / 1,007,307 series

Each still needs a one-line verdict: genuinely a frozen release, or a missing
registry entry masquerading as one. `in registry = False` means no updater path
exists at all, which is the more likely defect.

| source | series | cadence | in registry |
|---|---:|---|---|
| `unesco_sdg` | 100,997 | irregular | True |
| `unesco_natmon` | 98,664 | irregular | True |
| `ksh_stadat` | 97,520 | irregular | True |
| `imf_gfse` | 48,750 | None | False |
| `imf_gfsmab` | 43,179 | None | False |
| `imf_gfsssuc` | 36,901 | None | False |
| `imf_gfscofog` | 34,731 | None | False |
| `pip` | 32,490 | irregular | True |
| `imf_gfsibs` | 29,390 | None | False |
| `unctad_tabbapotta` | 29,358 | None | False |
| `imf_cpi` | 28,420 | None | False |
| `who_sdg` | 28,160 | None | False |
| `unctad_rfia` | 24,720 | None | False |
| `unesco_clte` | 23,868 | None | False |
| `unctad_gdpgbtoevbkoeatasa` | 21,158 | None | False |
| `imf_gfsfalcs` | 20,249 | None | False |
| `fao_ql` | 20,179 | None | False |
| `unesco_inno` | 18,909 | None | False |
| `idb` | 18,838 | irregular | True |
| `imf_fsire` | 18,620 | None | False |
| `fao_ga` | 15,018 | None | False |
| `imf_psbsfad` | 14,018 | None | False |
| `imf_fas` | 13,960 | None | False |
| `boc` | 12,862 | None | False |
| `fao_ge` | 11,813 | None | False |
| `fao_gt` | 10,506 | None | False |
| `imf_pgi` | 8,891 | None | False |
| `unesco_film` | 8,527 | None | False |
| `unctad_sbtisvsaga` | 7,920 | None | False |
| `cso` | 7,896 | irregular | True |
| `imf_bopagg` | 7,801 | None | False |
| `fao_gb` | 6,980 | None | False |
| `unctad_gasbtoia` | 6,776 | None | False |
| `unesco_cltt` | 6,226 | None | False |
| `fao_rp` | 5,440 | None | False |
| `unctad_fdiiaofasa` | 5,107 | None | False |
| `unctad_gasbeaiogasa` | 5,076 | None | False |
| `fao_gn` | 4,761 | None | False |
| `who_hwf` | 4,421 | None | False |
| `imf_pctot` | 4,320 | None | False |
| `unctad_tabmcioeaiopa` | 4,250 | None | False |
| `unctad_tabmscioeaiopa` | 4,250 | None | False |
| `unctad_gasbtbia` | 3,402 | None | False |
| `fao_gl` | 3,057 | None | False |
| `unctad_sbeaiotsvsaga` | 3,010 | None | False |
| `fao_gf` | 2,591 | None | False |
| `imf_unsdg_imf_inputs` | 2,515 | None | False |
| `fao_gy` | 2,491 | None | False |
| `fao_ic` | 2,468 | None | False |
| `imf_world` | 2,268 | None | False |
| `imf_pgcs` | 2,262 | None | False |
| `who_rs` | 2,207 | None | False |
| `imf_namain_idc_n` | 1,926 | None | False |
| `unctad_gdptapccac2pa` | 1,734 | None | False |
| `imf_fdi` | 1,728 | None | False |
| `imf_afrreo` | 1,654 | None | False |
| `imf_afrreo_direct` | 1,652 | irregular | True |
| `hf_equities` | 1,391 | None | False |
| `imf_fm` | 1,356 | None | False |
| `unctad_soigapotta` | 1,226 | None | False |
| `imf_mcdreo` | 1,095 | None | False |
| `unctad_taupa` | 898 | None | False |
| `unctad_rgdptapcgra` | 867 | None | False |
| `unctad_bopcaba` | 842 | None | False |
| `unctad_mpcadioeaia` | 816 | None | False |
| `snb` | 762 | None | False |
| `unctad_lsciq` | 760 | None | False |
| `unctad_mttasa` | 704 | None | False |
| `worldbank` | 692 | None | False |
| `unctad_cpia` | 637 | None | False |
| `fao_gr` | 617 | None | False |
| `fao_es` | 595 | None | False |
| `unctad_mtba` | 584 | None | False |
| `fao_ep` | 519 | None | False |
| `unctad_srbca` | 414 | None | False |
| `unctad_sotwmfvbcoboa` | 373 | None | False |
| `unctad_reericba` | 352 | None | False |
| `unctad_mttgra` | 351 | None | False |
| `unctad_lscia` | 344 | None | False |
| `maddison` | 338 | irregular | True |
| `unctad_reerigdba` | 333 | None | False |
| `imf_whdreo` | 322 | None | False |
| `unctad_tabpcioeaia` | 308 | None | False |
| `imf_gender_equality` | 295 | None | False |
| `imf_gender_budgeting` | 288 | None | False |
| `imf_whdreo_direct` | 287 | irregular | True |
| `unctad_neera` | 280 | None | False |
| `imf_apdreo` | 265 | None | False |
| `imf_apdreo_direct` | 250 | irregular | True |
| `unctad_cpta` | 177 | None | False |
| `fao_ew` | 169 | None | False |
| `fao_ae` | 164 | None | False |
| `fao_af` | 162 | None | False |
| `unctad_mfbcoboa` | 155 | None | False |
| `imf_cofer` | 154 | None | False |
| `unctad_mmcascioeaiopa` | 86 | None | False |
| `unctad_cpa` | 50 | None | False |
| `unctad_fmcpa` | 50 | None | False |
| `fao_ec` | 49 | None | False |
| `unctad_cioiuibbicoeair4a` | 15 | None | False |
| `unctad_fmcpia21` | 14 | None | False |
| `unctad_ciocgeaia` | 8 | None | False |
| `unctad_wstbtocabgoea` | 8 | None | False |

## A1. WHY each ongoing-cadence source is frozen (fetcher readiness)

`implemented()` is the orchestrator's OWN resolution, not a filename guess -- which
would have been wrong twice: `imf_fsi` has no per-source module but many `imf_*_direct`
siblings, and `ksh` has a module that cannot be imported at all.

**25 ready to promote** (fetcher resolves; only `live: true` + a forced proof run stand between them and the nightly), **1 broken**, **14 with no fetcher**.

### Ready to promote -- 204,509 series

| source | series | cadence |
|---|---:|---|
| `insee_bdm` | 101,848 | monthly |
| `adb` | 53,458 | annual |
| `imf_fas_direct` | 14,081 | annual |
| `eurostat` | 7,637 | monthly |
| `ssb` | 5,568 | monthly |
| `worldbank_esg` | 4,447 | weekly |
| `stat_slovenia` | 4,134 | monthly |
| `stat_estonia` | 3,437 | monthly |
| `imf_world_direct` | 3,244 | annual |
| `imf_fdi_direct` | 1,728 | annual |
| `statfin` | 1,539 | monthly |
| `worldbank_wdi` | 1,486 | quarterly |
| `hagstofa` | 1,068 | monthly |
| `wikidata` | 250 | monthly |
| `imf_cofer_direct` | 140 | quarterly |
| `insee_melodi` | 139 | monthly |
| `owid` | 64 | monthly |
| `ember` | 60 | monthly |
| `faostat` | 47 | monthly |
| `ecb` | 35 | daily |
| `oecd` | 28 | monthly |
| `defillama` | 24 | daily |
| `statcan` | 20 | weekly |
| `abs` | 18 | monthly |
| `bls` | 9 | weekly |

### BROKEN -- the fetcher raises on import

Currently harmless only because `AQUEDUCT_LIVE_ONLY=1` never reaches a non-live
source. It is a landmine for whoever promotes it.

| source | series | error |
|---|---:|---|
| `ksh` | 25,057 | `FileNotFoundError: [Errno 2] No such file or directory: 'E:\\research\\econfindatalibrary\\jobs\\ingest_ksh_h` |

### No fetcher at all -- 77,253 series served with no updater path

| source | series | cadence |
|---|---:|---|
| `imf_fsi` | 73,288 | monthly |
| `stats_nz` | 1,320 | quarterly |
| `ipea` | 1,241 | monthly |
| `comtrade` | 713 | annual |
| `bea` | 240 | monthly |
| `imf` | 131 | monthly |
| `ilostat` | 80 | monthly |
| `fhfa` | 61 | monthly |
| `zillow` | 52 | monthly |
| `bis` | 49 | monthly |
| `usda` | 25 | monthly |
| `census` | 22 | monthly |
| `fed_board` | 21 | daily |
| `noaa` | 10 | monthly |

## The one BROKEN fetcher, examined: `ksh` may be superseded, not repairable

`ksh` (25,057 series) is the only frozen source whose fetcher RAISES: `updater/strategies/
fetchers/ksh.py` imports `parse_ksh_csv` from `jobs/ingest_ksh_hungary.py`, and that file does
not exist -- only `jobs/ingest_ksh_stadat.py` does, which does NOT define that function. So it
is not a rename: the parsing helper exists nowhere in the tree.

Before writing a replacement parser, the more useful question is whether the source should
exist at all. Both `ksh` and `ksh_stadat` pull the same publisher's STADAT tables from
ksh.hu/stadat_files, and `ksh` looks like an earlier, worse keying of them:

| | series | key shape | title |
|---|---:|---|---|
| `ksh` | 25,057 | `KSH:ara0003:1` -- numeric COLUMN INDEX | "Harmonized index of consumer prices is a class" (truncated mid-sentence) |
| `ksh_stadat` | 97,520 | `KSH:ara0001:Consumer price index` -- real column LABEL | "Consumer price index (ara0001)" |

A key ending in `:1` cannot tell a user which column it is, which is the same class of defect
the ons_uk re-key just fixed. `ksh_stadat` covers ~4x more series with legible keys and real
titles.

**But `ksh` is NOT simply a subset: only 3,363 of its 25,057 keys appear in `ksh_stadat`.** So
retiring it would drop coverage unless those ~21,700 keys are duplicates under a different
spelling rather than distinct tables. That has to be established, not assumed.

**RESERVED FOR AHMED** -- retiring a source and its ids is his call, not mine. The options are:
(a) establish whether the ~21,700 non-overlapping keys are genuinely absent from `ksh_stadat`
and, if they are duplicates, retire `ksh`; (b) write the missing parser and keep both;
(c) leave `ksh` served-but-frozen, which is the status quo and the least honest of the three.
Doing nothing is safe today only because `AQUEDUCT_LIVE_ONLY=1` never reaches a non-live
source -- promoting it without fixing the import would crash the run.
