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

## The derive-all fallback is incompatible with the CI scratch mirror

Established by measurement, not inference — record this before re-diagnosing it:

* Both stores are **complete**. Local: 22,793 / 22,793 catalog flows present across the nine
  PxWeb sources. R2: dst 706 / 706 parquets, 1,963 / 1,963 flows. Nothing is missing anywhere.
* In CI (`AQUEDUCT_BACKEND=r2`) `blob.write_table_atomic` PUTs to R2 **and keeps the local
  file as a "scratch-store mirror"** (`blob.py:43-46`) so the same-run CSV derive has the
  bytes without re-downloading.
* `ECONDL_DATA` is never set in the workflow, so `default_data_root()` resolves to
  `<repo>/data/clean_full` — on a runner that contains **only the files this run wrote**.
* `_DERIVE_ALL_CAP = 5000` (`orchestrate.py:138`): if ANY changed key fails to map, a source
  with ≤ 5000 catalog ids re-derives **every** id.

Those last two cannot both hold. dst has 1,963 ids ≤ 5000, so it asked to derive all 1,963
while only **12 files** were on the runner → `csv_derive failed 1923/1963`, every failure
"zero rows matched in 12 files". `bfs` passed only because it is a **single-file** source, so
its one written file held everything.

So the design assumes *the changed series always live inside the files this run just wrote* —
true for one-file-per-unit sources, false for any multi-file source that hits derive-all.

Options, cheapest last:
1. Skip derive-all when the backend is r2 and derive only the mapped ids (honest, cheap; still
   reports the unmapped keys as a coherence gap rather than 1,923 failing derives).
2. Have derive READ through the blob layer so it resolves against the complete R2 store —
   correct, but the resolver currently reads local paths via `ds.dataset()`.

Still open underneath: with the flow-grain mapping in place dst STILL had some unmapped key
(otherwise derive-all would not have fired at all). Find that key class before tuning anything.

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
