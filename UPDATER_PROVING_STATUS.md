# Updater proving status — measured, not assumed

Denominator: **105 registry sources · 92 fetchers built · 15 live.** Building is not the
bottleneck; **proving** is, and CI is serialized to one run at a time by the
`aqueduct-updater` concurrency group.

## How a source gets promoted

1. Local smoke with `--force` (catches crashes and memory cheaply, does NOT count as proof —
   R36: local uses the local store, CI uses R2).
2. CI `workflow_dispatch` with **`force=true`**.
3. Read the actual status line. A run is only a proof if it processed units.
4. Set `live: true`, validate the registry, push with `git rev-list --count origin/main..HEAD == 0`.

> **`force=true` is mandatory.** A plain `--source` dispatch still honours the cadence gate,
> so a not-due source prints `0 unit(s) processed` and the run goes GREEN having executed no
> fetcher code at all. A local sweep of 14 sources showed 13 doing exactly that (R35).

## Blocked — do NOT promote

| Source | Measured | Blocker |
|---|---|---|
| `ons_uk` | peak RSS **32.26 GB** | Time axis + a measurement are baked into `series_key` (`CV=14.0:calendar-years=2018:…`), so all 5,323,152 rows are distinct "series". Fixing it changes on-disk keys ⇒ re-ingest, not a patch. Also **0 catalog rows**, so coherence can never pass. |
| `un_wpp` | peak RSS **14.93 GB** on a 16 GB runner | Full re-fetch/parse blows the runner. Reports `no_change` only because nothing changed — the day UN WPP republishes, production does the 14.93 GB path and OOMs. |
| `bundesbank` | **6.28 GB**, >420 s | Memory + runtime. |
| `adb` | 24 min at 0.16 GB | Hung on IO, not memory — same shape as the `worldbank_esg` 4xx-retry bug; likely the same class. |
| `ksh` | crashes on import | `jobs/ingest_ksh_hungary.py` was deleted in `5095976` ("retire legacy duplicates", 2026-07-02) but the registry entry and fetcher stayed. **See the migration note below — this one is user-facing.** |
| `eia`, `cepii_gravity`, `vdem`, `oecd` (58 GB), `statcan` (175 GB) | — | Scale; need a streaming design. |

## Slow but working (>7 min local cap; fine in CI, bad for a serial cron)

`ksh_stadat`, `insee_bdm`, `insee_melodi` — all hit the 420 s local cap without failing.

## Healthy in local smoke (`--force`, rc=0)

`transparency_ti`, `nasa_giss`, `swiid`, `gti`, `etr`, `sipri_polity`, `fsi_fundforpeace`,
`gpi`, `damodaran`, `gppd`, `barro_lee`, `wgi`, `kof_globalization`, `undp_hdr`, `ppi`, `epu`,
`yale_epi`, `wikidata`, `edgar_jrc`, `imf_commodity`, `pwt`, `gcb`, `ei_statreview`,
`penn_world_table`, `oxcgrt` (4.81 GB), `imf_weo`, `ggdc`, `harvard_atlas`, `gapminder`,
`fdic` (4.26 GB) — all `no_change`/`ok` and cheap unless noted. These are the CI queue.

Returning `partial` locally (worth reading before promoting): `sec_edgar` (3.12 GB), `pip`,
`ember`, `gleif`, `cso`, `idb`, `defillama`.

## Open decisions (need Ahmed)

**1. `ksh` → `ksh_stadat` migration is half-done, in the worst direction.**

| | script | registry | fetcher | catalog rows |
|---|---|---|---|---|
| `ksh` (retired) | deleted | present | present, crashes | **25,057 — what users are served** |
| `ksh_stadat` (successor) | present | present | present, CI-passing | **0 — invisible to users** |

The pipeline moved on; the catalog never did. Completing it means recataloguing in **both**
D1 and R2 (R38) and dropping `ksh` — that changes what 25,057 series resolve to publicly, so
it is not being done unilaterally.

**2. The orchestrator is strictly serial** (`orchestrate.py:218`, `for unit in units:`).
Fine at 15 live. At ~68 live, with PxWeb crawls at 30–50 min each, the daily job cannot finish
inside `timeout-minutes: 300`. `_common.Deadline` bounds a single source; it does not bound the
sum. Parallelising means concurrent writers to the SQLite state DB and R2.

## Sources with no updater path at all

`unctad` 38 (one `UNCTAD_*` provider family — a single parameterized fetcher could cover all
38, but they are `catalogued: false`, so they would hit the same coherence wall), `unesco` 5,
`who` 3. Plus 27 registry sources with no fetcher yet.
