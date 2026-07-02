# Econ Data Library — Cloudflare Worker (`/v1` API)

Production implementation of `api/CONTRACT.md`, in TypeScript over **D1**
(catalog + freshness) and **R2** (per-series CSV objects). Same contract, same
SQL, same honest-status codes as the Python dev shim (`api/devserver.py`) and the
`econdl` client — they cannot disagree because the catalog/freshness SQL is
verbatim (`src/sql.ts`) and the per-series CSV bytes are derived by the same
`econdl` resolver before they are published to R2 (see “The `.csv` path” below).

> Deployable, **not runtime-tested here** (no Cloudflare credentials in this
> environment). Typecheck is clean (`npm run typecheck`). The deploy steps below
> are what you run once you have an account.

## Endpoint status

| Endpoint | Status | Backend |
|---|---|---|
| `GET /v1/catalog` | **Fully live now** | D1 (FTS5 + LIKE fallback) |
| `GET /v1/sources` | **Fully live now** | D1 (source ⋈ license ⋈ source_state) |
| `GET /v1/last-updates` | **Fully live now** | D1 (canonical SQL + cadence math) |
| `GET /v1/series/{id}.metadata.json` | **Fully live now** | D1 (series ⋈ source ⋈ license ⋈ unit_state) |
| `GET /v1/bundle` | **Fully live now** | D1 (manifest; client fans out) |
| `GET /v1/series/{id}.csv` | **Needs the R2 per-series CSV objects** | R2 (streams `series/<id>.csv`) |

The five D1 endpoints work the moment you load `catalog.db` + `state.db` into D1.
`/v1/series/{id}.csv` additionally needs the per-series CSV objects published to
R2 (next section). Until a given (cataloged, migrated-source) series’ object
exists, the handler returns an honest **502 `data_unavailable`** (the source is
migrated; the object just isn’t published yet) — never an empty 200, never a
fabricated series. A source with no resolver at all is **501 `not_migrated`**.

## The `.csv` path (honest design — read this)

The Worker **does not parse parquet**. Decoding parquet in a Worker
(parquet-wasm + predicate pushdown) is heavy and fragile, and re-implementing the
33 bespoke per-source resolvers (`clients/python/econdl/_resolve.py`) in TS would
create a second source of truth that *will* drift. So the simplest correct design
is used:

- A build/migration job runs the **real `econdl` resolver**
  (`read_native → native_to_tidy → project [series_id, obs_date, value]`) for
  every resolvable series and writes the resulting long CSV
  (`series_id,obs_date,value`, header present, sorted by `obs_date`) to R2 at:

  ```
  series/<encodeURIComponent(series_id)>.csv
  ```

- The Worker `GET`s that object, applies the `from`/`to` date window
  (server-side predicate), and streams it. Because the bytes come from the same
  resolver as the dev shim, the two **cannot disagree**.

`geo=`/`freq=`/`unit=` filters return **400 `unsupported_filter`** (those are not
columns in the derived CSV yet — never a silently-unfiltered 200). See the header
comment in `src/series.ts` for the full decision tree.

## Provisioning (one-time)

You need a Cloudflare account with Workers, D1, and R2 enabled.

1. **Install the CLI**
   ```bash
   cd api/worker
   npm install
   npx wrangler login
   ```

2. **Create the D1 database** (holds catalog + freshness tables)
   ```bash
   npx wrangler d1 create econdl-catalog
   ```
   Paste the printed `database_id` into `wrangler.toml` (`[[d1_databases]]`).

3. **Load the data into D1.** Export the SQLite DBs to SQL and import. Both
   `catalog.db` and `state.db` go into the one `CATALOG` D1 database (the SQL in
   `src/sql.ts` references tables from both). FTS5 is supported by D1, so the
   `series_fts` virtual table imports as-is.
   ```bash
   # from repo root
   sqlite3 data/catalog.db .dump > /tmp/catalog.sql
   sqlite3 data/_aqueduct/state.db .dump > /tmp/state.sql
   # import (run each; --remote targets the deployed D1)
   npx wrangler d1 execute econdl-catalog --remote --file=/tmp/catalog.sql
   npx wrangler d1 execute econdl-catalog --remote --file=/tmp/state.sql
   ```
   > For large dumps, split into batches or use `wrangler d1 import` (CSV/SQL).
   > Verify: `wrangler d1 execute econdl-catalog --remote --command "SELECT COUNT(*) FROM series"`
   > should report 34368; `SELECT COUNT(*) FROM unit_state` should report 48.

4. **Create the R2 bucket** (per-series CSV objects)
   ```bash
   npx wrangler r2 bucket create econdl-series
   ```
   Then run your build job to publish `series/<id>.csv` objects (see above). You
   can also `wrangler r2 object put econdl-series/series/<encoded-id>.csv --file=...`
   one at a time for testing.

5. **Custom domain.** In the dashboard: Workers & Pages → `econdl-api` →
   Settings → Domains & Routes → **Add Custom Domain** (e.g. `api.<your-domain>`).
   This auto-provisions TLS. Alternatively uncomment the `[[routes]]` block in
   `wrangler.toml` (zone must already be on Cloudflare) and redeploy.

6. **Secrets.** None are required for the current endpoints (D1 + R2 are
   bindings, not secrets). If you later add an upstream key or an auth token:
   ```bash
   npx wrangler secret put SOME_TOKEN
   ```
   and read it from `env.SOME_TOKEN` (add it to `Env` in `src/types.ts`).

## Deploy

```bash
cd api/worker
npm install
npm run typecheck      # tsc --noEmit, must be clean
npm run deploy         # wrangler deploy
```

Smoke-test after deploy (replace the host):
```bash
curl https://api.example.com/v1/last-updates
curl https://api.example.com/v1/sources
curl "https://api.example.com/v1/catalog?q=unemployment&limit=5"
curl "https://api.example.com/v1/series/bls%3ACUUR0000SA0.metadata.json"
curl "https://api.example.com/v1/series/bls%3ACUUR0000SA0.csv"   # needs the R2 object
```

## Files

| File | Purpose |
|---|---|
| `wrangler.toml` | D1 + R2 bindings, custom-domain note |
| `package.json` / `tsconfig.json` | strict TS, typecheck + deploy scripts |
| `src/index.ts` | router for the `/v1` contract + honest 404/405/500 |
| `src/sql.ts` | the **verbatim** D1 SQL (same as the dev shim) |
| `src/types.ts` | `Env` bindings + typed D1 row shapes (no `any`) |
| `src/util.ts` | honest-status responses, license block, cadence math, supported-source list |
| `src/catalog.ts` | `/v1/catalog` (FTS5 + LIKE) |
| `src/sources.ts` | `/v1/sources` |
| `src/lastUpdates.ts` | `/v1/last-updates` |
| `src/metadata.ts` | `/v1/series/{id}.metadata.json` |
| `src/series.ts` | `/v1/series/{id}.csv` (R2; honest design documented in-file) |
| `src/bundle.ts` | `/v1/bundle` (manifest; client fan-out) |

## Keeping in sync with the resolver

`src/util.ts::SUPPORTED_SOURCES` is the list of the 33 sources that have an
at-rest resolver — a series whose source is not in it returns **501
`not_migrated`**. Regenerate it from the single source of truth whenever a
resolver is added:

```bash
python -c "import sys; sys.path.insert(0,'clients/python'); \
import econdl._resolve as r; print(r.supported_sources())"
```

(Or set the `SUPPORTED_SOURCES` var in `wrangler.toml` to override without a code
change.)
