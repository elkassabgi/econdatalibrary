# Date conventions, measured

> ## CORRECTION — 2026-07-29, after adversarial review
>
> **The headline ratio below is wrong. It is 70.5x, not 270x, and the per-convention
> observation totals understate reality by 8-33x.** An independent recomputation over
> every source found the cause, and it is the METHOD, not arithmetic:
>
> - The classifier assigns **one convention per SOURCE**, then whole sources are summed
>   into that convention's bucket. `statcan` is **74.6% of this entire library** and
>   **94.94% of its stamps are 12-31**, but it carries a single "daily/exact dates"
>   label — so **53,969,462,901 period-END observations never entered the annual-END
>   total at all**. A per-group label cannot be summed as though it described every
>   member.
> - Measured directly at observation level: period-END **68,904,800,937** (not
>   8,036,676,385) and period-START **977,477,558** (not 29,812,857).
> - The scan also silently dropped data. A bare `except: continue` swallowed **58 bls
>   files** whose `obs_date` is stored as a STRING, so bls appears here as 57,359,640
>   observations when it holds **328,077,765**. The claim below that this covers "the
>   COMPLETE store... Not a sample" is therefore FALSE as published. The tool now names
>   files it cannot read; this document was generated before that fix.
> - Several per-source labels are simply wrong: `un_wpp` is annual mid-year (every
>   observation on 07-01) but labelled quarterly-START; `bis` is quarterly, not monthly;
>   `ilostat` is annual-dominant; `abs` is annual-END; `fred`, `cbs_nl` and `eurostat`
>   are not "daily" in any meaningful sense.
>
> **The DIRECTION of the finding survives** — period-END genuinely dominates by
> observation count, and standardising on END is still the cheaper target. Every
> absolute number below should be treated as unreliable until the classifier is rebuilt
> at FILE grain rather than source grain.
>
> No re-stamp has been applied. `tools/restamp_period_end.py` now refuses any source
> whose actual stamps are not calendar period-starts, which is what stopped `un_wpp`
> (27,756,617 rows) and `stats_nz` from being converted.


Every source here writes an ISO date in one `obs_date` column, so this is
NOT a formatting difference. What differs is **which day inside the period**
carries the observation: an annual 2024 point can be stamped `2024-01-01`
(period-START) or `2024-12-31` (period-END).

That distinction is invisible until it bites, and it bites three ways:

- **Merging mixes them.** Dedup keys on `(series_key, obs_date)`, so one
  observation under two conventions becomes two rows. Merging a period-END pull
  into period-START `imf_commodity` would have written a second row for
  every month of 34 years instead of extending the series.
- **Joins silently misalign.** An annual series stamped `01-01` joined to
  one stamped `12-31` yields an empty join, or a one-year lag nobody notices.
- **Filters cut differently.** `obs_date <= 2024-12-31` includes a
  period-END 2024 annual point; `obs_date < 2024-12-31` does not.

## What is actually stored

Measured by `tools/audit_date_conventions.py --full` over the COMPLETE
store: every row of every file of all 238 sources, 75,871,207,327 observations.
Not a sample.

| convention | sources | observations | share |
|---|---:|---:|---:|
| daily/exact dates | 37 | 64,771,282,499 | 85.4% |
| annual END (12-31) | 59 | 8,036,676,385 | 10.6% |
| MIXED (no single convention) | 18 | 1,360,930,338 | 1.8% |
| monthly END (month-end) | 3 | 1,108,458,128 | 1.5% |
| monthly START (day 1) | 14 | 516,116,940 | 0.7% |
| annual START (01-01) | 100 | 29,812,857 | 0.0% |
| quarterly START | 5 | 28,008,623 | 0.0% |
| quarterly END | 2 | 19,921,557 | 0.0% |

**The source count and the observation count disagree, and that disagreement
is the whole point.** By SOURCES, period-START leads 119 to 64. By
OBSERVATIONS, annual period-END leads 8,036,676,385 to
29,812,857 — a factor of 270x — because the largest
sources all stamp 12-31. An earlier pass that sampled 200k rows per source
reported the opposite, and would have aimed any standardisation at the
expensive direction.

## If a single convention is ever adopted

Period-END is both cheaper and more representative: standardising on START
re-stamps ~8,036,676,385 observations; on END, ~29,812,857.
Re-stamping is a **breaking change** — it moves dates users have already
downloaded and cited — and is not a decision this file makes.

**Do not blanket-convert.** Some stamps are fiscal, not calendar: `rba` uses
06-30 and `stats_nz` 03-01 because those are real fiscal year-ends. Forcing
them to calendar-START would destroy information, not normalise it.

## Sources with no single convention

Mostly MULTI-FREQUENCY, not broken: an annual series stamped 12-31 living
beside a quarterly one stamped 01-01 inside one source is legitimate.

| source | observations | most common stamps |
|---|---:|---|
| `bls` | 57,359,640 | 12-31:52440119, 07-01:1195202, 04-01:1163732, 01-01:1163007 |
| `census` | 44,242,170 | 12-31:4105145, 03-01:3938833, 01-01:3752856, 02-01:3720996 |
| `damodaran` | 23,343 | 01-01:21939, 12-31:1404 |
| `dst` | 9,198,885 | 12-31:7806094, 01-01:145826, 07-01:140765, 03-01:114567 |
| `hagstofa` | 7,207,289 | 12-31:5272136, 04-01:376318, 12-01:322963, 05-01:233487 |
| `ilo` | 208,345,152 | 12-31:116595251, 04-01:23713008, 10-01:22730876, 01-01:22717559 |
| `insee_bdm` | 10,823,103 | 12-31:1891011, 01-01:1128065, 04-01:1121072, 07-01:1110743 |
| `insee_sdmx` | 10,818,461 | 12-31:1890970, 01-01:1135634, 07-01:1117294, 04-01:1115296 |
| `istat` | 371,190,751 | 12-31:296031917, 01-01:12040202, 04-01:11906946, 10-01:11856122 |
| `ksh_stadat` | 1,249,215 | 12-31:774965, 01-01:72237, 04-01:66413, 10-01:62666 |
| `nasa_giss` | 9,946 | 12-31:4672, 04-01:441, 02-01:441, 01-01:441 |
| `noaa` | 549,412,914 | 07-01:43263264, 08-01:43254164, 09-01:43150666, 04-01:43070449 |
| `scb` | 19,671,743 | 12-31:14595712, 01-01:900684, 04-01:823472, 07-01:807675 |
| `ssb` | 28,283,649 | 12-31:22360782, 10-01:995435, 01-01:986648, 04-01:900975 |
| `stat_estonia` | 17,446,510 | 12-31:16489441, 01-01:193119, 04-01:185572, 07-01:182667 |
| `stat_slovenia` | 14,250,339 | 12-31:10062244, 10-01:1012456, 07-01:698670, 01-01:675420 |
| `statfin` | 11,323,867 | 12-31:8966712, 01-01:402268, 07-01:368290, 04-01:327543 |
| `worldbank_pink` | 73,361 | 12-31:10442, 02-01:5294, 03-01:5293, 04-01:5293 |

## Per-source

| source | convention | observations |
|---|---|---:|
| `abs` | monthly END (month-end) | 976,632,535 |
| `adb` | annual END (12-31) | 1,012,740 |
| `barro_lee` | annual END (12-31) | 597,432 |
| `bcb` | daily/exact dates | 102,938 |
| `bcrp` | daily/exact dates | 20,236 |
| `bfs` | annual END (12-31) | 5,337,621 |
| `bis` | monthly START (day 1) | 88,421,620 |
| `bls` | MIXED (no single convention) | 57,359,640 |
| `boc` | daily/exact dates | 2,732,162 |
| `boe` | daily/exact dates | 3,844,743 |
| `bundesbank` | daily/exact dates | 3,873,801 |
| `cboe` | daily/exact dates | 270,055 |
| `cbs_nl` | daily/exact dates | 4,304,423,589 |
| `census` | MIXED (no single convention) | 44,242,170 |
| `cepii_gravity` | annual END (12-31) | 69,666,545 |
| `cnb` | daily/exact dates | 264,605 |
| `comtrade` | annual END (12-31) | 24,086 |
| `cow` | annual END (12-31) | 385,966 |
| `cso` | daily/exact dates | 49,057,386 |
| `damodaran` | MIXED (no single convention) | 23,343 |
| `defillama` | daily/exact dates | 38,436,439 |
| `dst` | MIXED (no single convention) | 9,198,885 |
| `ecb` | daily/exact dates | 217,925,235 |
| `ecb_sdmx` | daily/exact dates | 79,930,444 |
| `edgar_jrc` | annual END (12-31) | 195,391 |
| `ei_statreview` | annual END (12-31) | 782,908 |
| `eia` | daily/exact dates | 312,552,766 |
| `ember` | daily/exact dates | 13,976,171 |
| `epu` | monthly START (day 1) | 14,172 |
| `etr` | annual END (12-31) | 1,035 |
| `eurostat` | daily/exact dates | 2,430,929,754 |
| `famafrench` | daily/exact dates | 238,797 |
| `fao_ae` | annual START (01-01) | 3,094 |
| `fao_af` | annual START (01-01) | 3,154 |
| `fao_ec` | annual START (01-01) | 807 |
| `fao_ep` | annual START (01-01) | 15,452 |
| `fao_es` | annual START (01-01) | 595 |
| `fao_et` | monthly START (day 1) | 379,316 |
| `fao_ew` | annual START (01-01) | 428 |
| `fao_fo` | annual START (01-01) | 593,850 |
| `fao_ga` | annual START (01-01) | 765,072 |
| `fao_gb` | annual START (01-01) | 367,749 |
| `fao_ge` | annual START (01-01) | 638,041 |
| `fao_gf` | annual START (01-01) | 76,611 |
| `fao_gl` | annual START (01-01) | 87,664 |
| `fao_gn` | annual START (01-01) | 198,228 |
| `fao_gr` | annual START (01-01) | 34,390 |
| `fao_gt` | annual START (01-01) | 523,661 |
| `fao_gy` | annual START (01-01) | 126,220 |
| `fao_ic` | annual START (01-01) | 51,036 |
| `fao_oa` | annual START (01-01) | 169,142 |
| `fao_pp` | monthly START (day 1) | 320,605 |
| `fao_qa` | annual START (01-01) | 164,547 |
| `fao_qcl` | annual START (01-01) | 1,064,551 |
| `fao_ql` | annual START (01-01) | 946,096 |
| `fao_qp` | annual START (01-01) | 103,383 |
| `fao_rp` | annual START (01-01) | 112,333 |
| `fao_tp` | annual START (01-01) | 3,976,572 |
| `faostat` | annual END (12-31) | 169,794,738 |
| `fdic` | quarterly END | 19,918,427 |
| `fed_board` | daily/exact dates | 13,669,798 |
| `fhfa` | monthly END (month-end) | 3,227,560 |
| `frankfurter` | daily/exact dates | 264,774 |
| `fred` | daily/exact dates | 48,188,443 |
| `freedomhouse` | annual END (12-31) | 16,989 |
| `fsi_fundforpeace` | annual END (12-31) | 41,253 |
| `gapminder` | annual END (12-31) | 3,763,088 |
| `gcb` | annual END (12-31) | 386,777 |
| `ggdc` | annual END (12-31) | 883,641 |
| `global_findex` | annual END (12-31) | 381,036 |
| `gpi` | annual END (12-31) | 92,550 |
| `gppd` | annual END (12-31) | 88,968 |
| `gti` | annual END (12-31) | 14,670 |
| `gus` | annual END (12-31) | 995,909 |
| `gus_dbw` | annual END (12-31) | 469,491,576 |
| `hagstofa` | MIXED (no single convention) | 7,207,289 |
| `harvard_atlas` | annual END (12-31) | 3,480,458 |
| `ibge` | annual END (12-31) | 31,893,504 |
| `idb` | annual END (12-31) | 15,066,444 |
| `ilo` | MIXED (no single convention) | 208,345,152 |
| `ilostat` | monthly START (day 1) | 388,161,420 |
| `imf` | daily/exact dates | 120,552,486 |
| `imf_afrreo` | annual START (01-01) | 43,518 |
| `imf_afrreo_direct` | annual END (12-31) | 45,053 |
| `imf_apdreo` | annual START (01-01) | 9,922 |
| `imf_apdreo_direct` | annual END (12-31) | 9,816 |
| `imf_bop` | annual START (01-01) | 1,879,519 |
| `imf_bopagg` | annual START (01-01) | 134,110 |
| `imf_cdis` | annual START (01-01) | 923,485 |
| `imf_cofer` | quarterly START | 5,699 |
| `imf_cofer_direct` | quarterly END | 3,130 |
| `imf_commodity` | monthly START (day 1) | 230,092 |
| `imf_cpi` | monthly START (day 1) | 4,012,294 |
| `imf_cpis` | annual START (01-01) | 1,097,722 |
| `imf_dot` | annual START (01-01) | 3,245,824 |
| `imf_fas` | annual START (01-01) | 192,561 |
| `imf_fas_direct` | annual END (12-31) | 197,286 |
| `imf_fdi` | annual START (01-01) | 72,576 |
| `imf_fdi_direct` | annual END (12-31) | 70,848 |
| `imf_fiscaldecentralization` | annual START (01-01) | 160,957 |
| `imf_fm` | annual START (01-01) | 47,033 |
| `imf_fsi` | monthly START (day 1) | 2,403,361 |
| `imf_fsire` | annual START (01-01) | 88,487 |
| `imf_gender_budgeting` | annual START (01-01) | 288 |
| `imf_gender_equality` | annual START (01-01) | 5,981 |
| `imf_gfscofog` | annual START (01-01) | 553,676 |
| `imf_gfse` | annual START (01-01) | 817,921 |
| `imf_gfsfalcs` | annual START (01-01) | 232,215 |
| `imf_gfsibs` | annual START (01-01) | 373,160 |
| `imf_gfsmab` | annual START (01-01) | 829,589 |
| `imf_gfsr` | annual START (01-01) | 976,336 |
| `imf_gfsssuc` | annual START (01-01) | 516,017 |
| `imf_hpdd` | annual START (01-01) | 9,628 |
| `imf_ifs` | monthly START (day 1) | 13,460,864 |
| `imf_irfcl` | monthly START (day 1) | 4,315,910 |
| `imf_mcdreo` | annual START (01-01) | 26,769 |
| `imf_mfs` | monthly START (day 1) | 12,071,908 |
| `imf_namain_idc_n` | quarterly START | 83,066 |
| `imf_pctot` | monthly START (day 1) | 1,231,728 |
| `imf_pgcs` | annual START (01-01) | 118,179 |
| `imf_pgi` | monthly START (day 1) | 1,077,057 |
| `imf_psbsfad` | annual START (01-01) | 209,229 |
| `imf_unsdg_imf_inputs` | annual START (01-01) | 39,844 |
| `imf_weo` | annual END (12-31) | 588,836 |
| `imf_whdreo` | annual START (01-01) | 14,380 |
| `imf_whdreo_direct` | annual END (12-31) | 12,855 |
| `imf_world` | annual START (01-01) | 51,510 |
| `imf_world_direct` | annual END (12-31) | 89,369 |
| `ine_spain` | monthly END (month-end) | 128,598,033 |
| `insee_bdm` | MIXED (no single convention) | 10,823,103 |
| `insee_melodi` | daily/exact dates | 34,683,679 |
| `insee_sdmx` | MIXED (no single convention) | 10,818,461 |
| `ipea` | daily/exact dates | 621,981 |
| `irena` | annual END (12-31) | 196,314 |
| `istat` | MIXED (no single convention) | 371,190,751 |
| `kof_globalization` | annual END (12-31) | 280,421 |
| `ksh` | annual END (12-31) | 512,995 |
| `ksh_stadat` | MIXED (no single convention) | 1,249,215 |
| `maddison` | annual END (12-31) | 36,905 |
| `nasa_giss` | MIXED (no single convention) | 9,946 |
| `nbp` | daily/exact dates | 298,895 |
| `noaa` | MIXED (no single convention) | 549,412,914 |
| `norgesbank` | daily/exact dates | 8,363,603 |
| `nyfed` | daily/exact dates | 16,656 |
| `oecd` | annual END (12-31) | 6,979,047,823 |
| `ofr` | daily/exact dates | 426,300 |
| `ons_uk` | annual END (12-31) | 25,401,777 |
| `owid` | daily/exact dates | 72,484,733 |
| `oxcgrt` | daily/exact dates | 7,837,551 |
| `penn_world_table` | annual END (12-31) | 418,397 |
| `pip` | annual END (12-31) | 453,474 |
| `polity` | annual END (12-31) | 376,867 |
| `ppi` | annual END (12-31) | 75,306 |
| `pwt` | annual END (12-31) | 389,098 |
| `qog` | annual END (12-31) | 6,921,874 |
| `rba` | daily/exact dates | 1,107,561 |
| `riksbank` | daily/exact dates | 608,004 |
| `scb` | MIXED (no single convention) | 19,671,743 |
| `shiller` | monthly START (day 1) | 16,593 |
| `sipri` | annual END (12-31) | 82,268 |
| `snb` | daily/exact dates | 303,358 |
| `ssb` | MIXED (no single convention) | 28,283,649 |
| `stat_estonia` | MIXED (no single convention) | 17,446,510 |
| `stat_latvia` | daily/exact dates | 9,281,101 |
| `stat_slovenia` | MIXED (no single convention) | 14,250,339 |
| `statcan` | daily/exact dates | 56,845,456,057 |
| `statfin` | MIXED (no single convention) | 11,323,867 |
| `stats_nz` | quarterly START | 118,822 |
| `swiid` | annual END (12-31) | 37,536 |
| `tcmb` | daily/exact dates | 511,229 |
| `transparency_ti` | annual END (12-31) | 2,312 |
| `treasury` | daily/exact dates | 18,424,684 |
| `ucdp` | annual END (12-31) | 21,610 |
| `un_wpp` | quarterly START | 27,756,924 |
| `unctad_bopcaba` | annual START (01-01) | 32,036 |
| `unctad_ciocgeaia` | annual START (01-01) | 112 |
| `unctad_cioiuibbicoeair4a` | annual START (01-01) | 64 |
| `unctad_cpa` | annual START (01-01) | 1,194 |
| `unctad_cpia` | annual START (01-01) | 24,265 |
| `unctad_cpta` | annual START (01-01) | 1,731 |
| `unctad_fdiiaofasa` | annual START (01-01) | 202,446 |
| `unctad_fmcpa` | annual START (01-01) | 1,154 |
| `unctad_fmcpia21` | annual START (01-01) | 350 |
| `unctad_gasbeaiogasa` | annual START (01-01) | 79,216 |
| `unctad_gasbtbia` | annual START (01-01) | 53,281 |
| `unctad_gasbtoia` | annual START (01-01) | 106,156 |
| `unctad_gdpgbtoevbkoeatasa` | annual START (01-01) | 938,882 |
| `unctad_gdptapccac2pa` | annual START (01-01) | 83,116 |
| `unctad_lscia` | annual START (01-01) | 4,780 |
| `unctad_lsciq` | quarterly START | 44,112 |
| `unctad_mfbcoboa` | annual START (01-01) | 155 |
| `unctad_mmcascioeaiopa` | annual START (01-01) | 2,322 |
| `unctad_mpcadioeaia` | annual START (01-01) | 21,570 |
| `unctad_mtba` | annual START (01-01) | 37,484 |
| `unctad_mttasa` | annual START (01-01) | 46,418 |
| `unctad_mttgra` | annual START (01-01) | 13,675 |
| `unctad_neera` | annual START (01-01) | 4,466 |
| `unctad_reericba` | annual START (01-01) | 6,209 |
| `unctad_reerigdba` | annual START (01-01) | 6,384 |
| `unctad_rfia` | annual START (01-01) | 662,155 |
| `unctad_rgdptapcgra` | annual START (01-01) | 40,693 |
| `unctad_sbeaiotsvsaga` | annual START (01-01) | 37,142 |
| `unctad_sbtisvsaga` | annual START (01-01) | 113,935 |
| `unctad_soigapotta` | annual START (01-01) | 20,923 |
| `unctad_sotwmfvbcoboa` | annual START (01-01) | 1,105 |
| `unctad_srbca` | annual START (01-01) | 2,688 |
| `unctad_tabbapotta` | annual START (01-01) | 285,650 |
| `unctad_tabmcioeaiopa` | annual START (01-01) | 46,722 |
| `unctad_tabmscioeaiopa` | annual START (01-01) | 46,718 |
| `unctad_tabpcioeaia` | annual START (01-01) | 3,144 |
| `unctad_taupa` | annual START (01-01) | 85,526 |
| `unctad_wstbtocabgoea` | annual START (01-01) | 400 |
| `undp_hdr` | annual END (12-31) | 202,691 |
| `unesco_clte` | annual START (01-01) | 36,538 |
| `unesco_cltt` | annual START (01-01) | 76,793 |
| `unesco_dem` | annual START (01-01) | 278,720 |
| `unesco_film` | annual START (01-01) | 70,467 |
| `unesco_inno` | annual START (01-01) | 43,802 |
| `unesco_natmon` | annual START (01-01) | 1,876,322 |
| `unesco_sci` | annual START (01-01) | 759,045 |
| `unesco_sdg` | annual START (01-01) | 734,662 |
| `unhcr` | annual END (12-31) | 213,937 |
| `unicef` | daily/exact dates | 23,111,730 |
| `unsdg` | annual END (12-31) | 2,918,435 |
| `vdem` | annual END (12-31) | 79,590,111 |
| `wgi` | annual END (12-31) | 194,418 |
| `who_gho` | annual END (12-31) | 8,188,819 |
| `who_hwf` | annual START (01-01) | 54,320 |
| `who_rs` | annual START (01-01) | 2,211 |
| `who_sdg` | annual START (01-01) | 172,598 |
| `whr` | annual END (12-31) | 2,270 |
| `wid` | annual END (12-31) | 124,367,162 |
| `worldbank_esg` | annual END (12-31) | 473,896 |
| `worldbank_extra` | annual END (12-31) | 21,880,696 |
| `worldbank_pink` | MIXED (no single convention) | 73,361 |
| `worldbank_wdi` | annual END (12-31) | 8,894,931 |
| `yale_epi` | annual END (12-31) | 84,654 |
| `zillow` | daily/exact dates | 106,490,755 |
