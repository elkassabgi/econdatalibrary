# Econ Data Library — local quickstart (test everything before cutover)

Everything below runs against the LOCAL store — no Cloudflare needed. All commands
verified working 2026-06-26.

## 1. Run the API locally (the dev shim — same contract as the prod Worker)
```
cd D:\research\econfindatalibrary
python api/devserver.py --port 8787
```
Then, in another shell:
```
# zero-install series download (curl / R / Stata / browser)
curl "http://127.0.0.1:8787/v1/series/bls%3ACUUR0000SA0.csv" | head
curl "http://127.0.0.1:8787/v1/series/bls%3ACUUR0000SA0.metadata.json"
# search + freshness + sources
curl "http://127.0.0.1:8787/v1/catalog?q=gdp&limit=5"
curl "http://127.0.0.1:8787/v1/last-updates"
curl "http://127.0.0.1:8787/v1/sources" | head -c 600
```

## 2. The econdl client — the reproducible multi-source bundle (the differentiator)
```python
import sys; sys.path.insert(0, "clients/python")
import econdl

# multi-source bundle -> tidy frame + a re-runnable datapackage.json lockfile
df = econdl.bundle(
    ["bls:CUUR0000SA0", "oecd:GDP_GROWTH_QOQ:USA", "worldbank_wdi:AG.CON.FERT.PT.ZS"],
    out="mystudy.zip",
)

# reproduce the EXACT pinned numbers later (verifies sha256) — this is the headline feature
df_again = econdl.pull("mystudy.zip")          # row-for-row identical
df_fresh  = econdl.pull("mystudy.zip", latest=True)   # opt in to refreshed data

# cross-section query: one indicator family across geographies
gdp = econdl.fetch("worldbank", geo=["USA", "DEU", "JPN"])

# point the client at the API instead of local files (same lockfile semantics)
df_http = econdl.bundle(["bls:CUUR0000SA0"], out="h.zip", api="http://127.0.0.1:8787")
```

## 3. Browse the catalog / landing pages
Open `catalog/site/index.html` in a browser — 309 dataset pages with license,
provenance, "How to cite" (producer-first), "Important notes" (e.g. the HF
survivorship-bias disclosure), and inline schema.org/Croissant JSON-LD + `sitemap.xml`.

## 4. Run the tests
```
python -m pytest api/test_conformance.py -q      # 12 passed (API contract conformance)
```

## What's covered today
33 sources are discoverable + bundleable (BLS, Eurostat, OECD, IMF, ECB, World Bank,
StatCan, BEA, EIA, FAOSTAT, SEC/EDGAR, …). ~207 more data-bearing sources are staged
to be cataloged next (see the broadening plan). The cloud cutover (custom domain) is
staged + verified — it just needs real Cloudflare credentials in `.env` (see api/DEPLOY.md).
