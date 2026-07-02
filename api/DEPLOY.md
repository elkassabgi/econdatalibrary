# Cloud cutover — deploy runbook

Going live on a custom domain. Everything below is **scripted with `wrangler`** (already
installed at `api/worker/node_modules/.bin/wrangler`, v3.114) — no Cloudflare MCP needed.
The whole cutover can be driven from this repo once the credentials in step 0 exist.

## 0. What you provision once (I cannot create your account or mint your tokens)
Add these to `D:\research\econfindatalibrary\.env` (already gitignored as `.env*`).
Your `.env` already has the **read** R2 keys (`R2_READ_ENDPOINT`, `R2_READ_ACCESS_KEY_ID`,
`R2_READ_SECRET_ACCESS_KEY`); the cutover also needs **write/deploy** authority:

```
CLOUDFLARE_API_TOKEN=...        # scopes: Account → D1 Edit, Workers Scripts Edit, Workers R2 Storage Edit
CLOUDFLARE_ACCOUNT_ID=...
R2_WRITE_ACCESS_KEY_ID=...      # an R2 token with Object Read & Write
R2_WRITE_SECRET_ACCESS_KEY=...
R2_WRITE_ENDPOINT=...           # https://<account>.r2.cloudflarestorage.com
```

With those present I run every step below; without them the cutover stops at the credential
boundary (by design — I will not provision your production account silently).

## 1. D1 — catalog + freshness (READY NOW; export self-verified)
```
python core/export_d1.py                       # -> dist/d1/econ_catalog.sql (verified locally)
wrangler d1 create econ-catalog                # note the database_id it prints
wrangler d1 execute econ-catalog --remote --file=dist/d1/econ_catalog.sql
```
Loads license(32) / source(309) / series(34,368) / series_fts / unit_state(48) /
source_state(39). D1 is SQLite, so the Worker's SQL runs unchanged.

## 2. R2 — canonical parquet
```
wrangler r2 bucket create econ-data            # or reuse an existing bucket
python core/upload_r2.py --bucket econ-data    # uploads clean_full/<source>/** (resumable, hash-skip)
```
R2 is the canonical store (HF mirror is regenerable; Zenodo holds the version DOIs).

## 3. Per-series CSV objects (makes /v1/series/{id}.csv live on the Worker)
```
python core/derive_csv.py --bucket econ-data   # resolver -> series/<urlenc id>.csv objects in R2
```
Until this runs, the Worker serves catalog / sources / last-updates / metadata / bundle from
D1, but /v1/series/{id}.csv returns 501 (honestly). The dev shim already serves .csv locally.

## 4. Worker — bind + deploy + custom domain
Fill `api/worker/wrangler.toml` with the D1 `database_id` (step 1) and the R2 bucket (step 2),
then:
```
cd api/worker
wrangler secret put ...        # any secrets the Worker needs
wrangler deploy
wrangler deployments domains add v1.<yourdomain>   # custom domain, NOT *.workers.dev / r2.dev
```

## 5. Verify live
```
curl https://v1.<yourdomain>/v1/last-updates
curl "https://v1.<yourdomain>/v1/catalog?q=gdp&limit=3"
curl "https://v1.<yourdomain>/v1/series/bls%3ACUUR0000SA0.csv" | head
```
Then point `econdl` at it: `econdl.bundle([...], api="https://v1.<yourdomain>")`.

## 6. Daily updates (GitHub Actions, after the cloud backend lands)
`.github/workflows/update.yml` runs `python -m updater.run` on a cron with
`AQUEDUCT_BACKEND=cloud`, reading/writing state via D1 and data via R2 (see core/cloud_backend).
Secrets are the same `.env` keys, stored as GitHub Actions secrets.

---
Status: step 1 is built + verified. Steps 2–6 are scripted but gated on the step-0 credentials.
