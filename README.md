# Econ-Fin Data Library (working name)

The open, research-grade data commons for economics & finance — an expansion of
[HF Data Library](https://hfdatalibrary.com) from one dataset (high-frequency US
equities) to a unified, citable home for the world's **free, re-serveable**
economic & financial data.

**Built on the same serverless stack as HF Data Library** (GitHub Actions cron →
R2 Parquet + D1 catalog → Cloudflare Worker API + Pages), so it runs itself and
costs single-digit dollars/month. See `PLAN.md` for the full build plan and
`configs/sources.yaml` for the licensed source set.

## Layout
```
connectors/   one folder per data source (the only thing that grows)
core/         schema, license gate, normalize, clean, validate, storage, db
jobs/         run_connector, backfill, build_bulk_exports, fetch_* bootstrap jobs
configs/      sources.yaml  -- the source & license registry (the legal backbone)
api/          Cloudflare Worker (REST /v1, mirrors api.hfdatalibrary.com)
db/           D1 schema + migrations
docs/         per-source methodology + auto-built licensing page
data/         LOCAL STAGING ONLY (gitignored) -- raw/ + clean/ before R2 upload
```

## Status
- **Phase 0 (in progress):** scaffold + SEC EDGAR bulk backfill downloading to
  `data/raw/sec_edgar/`. See `jobs/fetch_sec_edgar_bulk.py`.

## The one rule
Nothing gets cached & re-served unless its license is `reservable: true` in
`configs/sources.yaml`. The ingest refuses everything else (see `core/licenses.py`).
