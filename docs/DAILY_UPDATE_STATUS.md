# Daily Update Readiness — econdatalibrary

**The problem:** We have ~40 sources. For autonomous daily updates like hfdatalibrary,
each source needs either (a) a connector with `fetch(since=date)` incremental support,
or (b) a bulk script that checks what changed and only re-pulls that.

---

## ✅ READY FOR DAILY AUTOMATION (29 connector-framework sources)

These live in `connectors/<source>/connector.py`. Running `python jobs/run_connector.py <source>`
with a `--since` date will pull ONLY new/changed data. The `daily_update.py` orchestrator
handles cadence (daily/weekly/monthly) and tracks last-run dates in `data/_last_run.json`.

| Source | Cadence | Notes |
|---|---|---|
| eia | daily | EIA publishes daily prices |
| ecb / frankfurter | daily | Daily FX reference rates |
| fed_board | daily | H.4.1 / H.8 / H.15 (mix of daily/weekly) |
| defillama | daily | TVL updated continuously |
| ofr | daily | OFR fnyr/repo rates daily |
| nyfed | daily | SOFR/OBFR via FRED API |
| bls | weekly | BLS publishes monthly releases |
| worldbank / worldbank_esg / worldbank_pink | weekly | WB updates quarterly/annually |
| treasury | weekly | FiscalData updated daily/monthly per dataset |
| statcan | weekly | StatCan publishes many series weekly |
| zillow | weekly | Zillow monthly; weekly pull catches any updates |
| gleif | weekly | GLEIF publishes daily delta files |
| bea | monthly | BEA quarterly/monthly releases |
| usda | monthly | NASS monthly |
| ilostat | monthly | ILO monthly |
| faostat | monthly | FAO annual but check monthly |
| imf | monthly | IMF monthly/quarterly |
| oecd | monthly | OECD monthly |
| noaa | monthly | NOAA monthly station summaries |
| ember | monthly | Ember monthly electricity |
| owid | monthly | OWID charts updated irregularly |
| fhfa | monthly | FHFA monthly HPI |
| abs | monthly | ABS quarterly |
| boe | monthly | BoE daily/monthly mix |
| census | monthly | Census monthly releases |
| bis | monthly | BIS quarterly |
| famafrench | monthly | FF monthly updates |
| penn_world_table | annual | PWT ~annual new version |

---

## ⚠️ NEEDS DELTA LOGIC ADDED (bulk scripts, currently full-rebuild only)

These work for the initial backfill but need `--incremental`/`--delta` flags
that check what changed since last run:

| Script | What's needed | Difficulty |
|---|---|---|
| `ingest_sec_edgar.py` | SEC publishes daily filing index (`full-index/YYYY/QTR/company.gz`) — compare against our latest filing dates per CIK | Medium |
| `ingest_eurostat.py` | TOC has `last_updated` per dataset — only re-pull datasets newer than last run | Easy (TOC already downloaded) |
| `ingest_fred_releases.py` | FRED API supports `realtime_start` — already cursor-paginated; filter by series `last_updated` | Medium |
| `ingest_statcan.py` | StatCan WDS has `getChangedCubeList` endpoint — returns cubes changed since a date | Medium |
| `ingest_gleif.py` | GLEIF publishes daily delta ZIPs at leidata.gleif.org/api/v1/delta-files/lei2/latest/zip | Easy |
| `ingest_noaa.py` | NOAA publishes monthly station updates — just re-run --build (stations already downloaded) | Medium |
| `ingest_worldbank_wdi.py` | WB API returns `lastUpdated` per indicator — skip unchanged | Medium |
| `ingest_cepii_baci.py` | Annual only; check if new vintage released | Easy (check filename/date) |
| `ingest_cftc.py` | CFTC publishes new annual zip each year + weekly current file | Easy |

---

## ⛔ STATIC / TRULY INFREQUENT (no daily update needed)

| Source | Why |
|---|---|
| `ingest_bis_cbs_lbs.py` | BIS publishes bulk zips ~quarterly; run quarterly |
| `ingest_famafrench.py` | Monthly updated; script already skips existing files |
| `ingest_worldbank_extra.py` | Various WB extra DBs; monthly at most |

---

## NOT YET WIRED TO GITHUB ACTIONS

The workflow file `.github/workflows/daily.yml` is written but:
1. **Repo doesn't exist yet** — need to create `github.com/elkassabgi/econdatalibrary`
2. **Secrets not set** — API keys need to go in GitHub Secrets
3. **R2 migration not done** — data currently on local disk; needs upload to `econdatalibrary-data` R2 bucket
4. **D1 catalog not created** — `data/catalog.db` needs to migrate to Cloudflare D1
5. **Worker API not deployed** — `/v1` endpoint doesn't exist yet

These are all cloud-build tasks (Phase 3 in PLAN.md), which come after data is verified complete.

---

## Summary

- **29 sources**: ready for daily automation right now (connector framework ✅)
- **9 scripts**: need delta logic added (a few hours each; medium priority)
- **Cloud infrastructure**: not yet wired (the next major phase)
- **Estimated time to full autonomy**: 1-2 weeks of engineering after the cloud build is live
