# Econ Data Library — Public API contract (`/v1`)

*One contract, three implementations: the Cloudflare **Worker** (prod, TS over R2+D1),
the local **dev shim** (`api/devserver.py`, Python over the on-disk store + SQLite —
testable today with no cloud creds), and the **`econdl`** client (a convenience over
these URLs). All three MUST agree byte-for-byte on response shape; the dev shim is the
executable reference while the giants/migration land.*

Design rules (from ARCHITECTURE §9 + STRATEGY honest-status):
- **Zero-install:** every series is a stable `.csv` + `.metadata.json` URL — curl / R /
  Stata / browser, no SDK (copy OWID `.csv?csvType=full` + `.metadata.json`).
- **Honest, never silent:** a series whose source is not yet migrated returns **501 +
  a machine reason**, never an empty 200. A freshness probe that can't be determined
  says so. We never launder "unknown" into "fresh".
- **Reproducible by default:** bundle manifests pin `snapshot_date` + per-resource
  `sha256`; refreshing is an explicit opt-in (mirrors `econdl` lockfile semantics).
- **Free-tier safe:** bundles are a **manifest the client fans out** (presigned/stable
  resource URLs), never a server-assembled zip — so a >50-object bundle never trips the
  Worker 50-subrequest cap ([w10]).

## Identity in URLs
The catalog id grammar is `provider:tail` with `:` separators and a **variable** number
of segments per source (`bls:CUUR0000SA0`, `worldbank_wdi:AG.CON.FERT.PT.ZS`,
`worldbank:NY.GDP.MKTP.CD:AFE`, `penn_world_table:rgdpe:USA`). Mapping `:`→`/` path
segments is therefore ambiguous, so the canonical retrieval URL is:

```
/v1/series/{series_id}.csv            # series_id is the EXACT catalog id, URL-encoded
/v1/series/{series_id}.metadata.json
```

The pretty `/v1/{PROVIDER}/{DATASET}/{SERIES}` form is reserved for after the Stage-1
global-id migration (gated on the bijectivity test [w13]); until then `/v1/series/{id}`
is the truthful surface because it is exactly what the catalog holds.

## Endpoints

### `GET /v1/series/{id}.csv`
Long CSV `series_id,obs_date,value` (header always present), sorted by `obs_date`.
Query params:
- `format=full|filtered` (default `full`).
- `from=YYYY-MM-DD`, `to=YYYY-MM-DD` — inclusive date window (server-side predicate).
- `geo=`, `freq=`, `unit=` — dimension filters (no-ops until those become columns; a
  requested filter the store can't honor yet returns **400** with `unsupported_filter`,
  never a silently unfiltered 200).

Status contract: **200** with ≥1 row; **404** if the id is not in the catalog;
**501** `{"error":"not_migrated","source":"<src>","detail":...}` if the source has no
resolver yet (loud, actionable); **502** `{"error":"resolver_empty",...}` if the id
resolves to a file but zero rows (refuse to emit an empty series silently); **502**
`{"error":"data_unavailable",...}` if the stored object does not start with the contract
header (malformed at rest: refused, never served as data). The R2 ETag is attached only to
`?raw=1` downloads of the WHOLE object (no window, no projection).

Large objects (stored size ≥ 256 KiB — table-grain series can be hundreds of MB gzipped;
ledger R582): the body is never materialised in the worker.
- Unfiltered (no `from`/`to`/`geo`): the response is the **stored gzip bytes**
  (`content-encoding: gzip`, exact `content-length`, the R2 ETag on `?raw=1`). The edge
  negotiates the encoding with the client. The in-body citation header is **omitted** on
  this path and declared by `x-econdl-citation-omitted: large-object` plus
  `link: </v1/series/{id}.metadata.json>; rel="describedby"` (the citation lives there).
- Filtered: the object is inflated and filtered as a stream — chunked transfer, **no
  `content-length`** — and the same 200/502 rules apply. Above 1.5 GB decompressed the
  request is refused up front with **400** `unsupported_filter` (download `?raw=1` and
  filter locally); a malformed stored object is **502** `data_unavailable`.
**Completeness line (mandatory since 2026-09-02).** A CSV response that carries NO
`content-length` (the server inflated a stored object at or above 256 KiB for a `from`/`to`/`geo`
filter, or a plain-stored large object) ends with one comment line:

    # econdl-complete rows=<N>

where N is the number of data rows delivered. Comment lines start with `#` and are skipped by
`pandas.read_csv(..., comment="#")` and R's `comment.char="#"`. A response that does not end with
this line was cut off: the edge delivers a server-side abort (a corrupt object detected
mid-stream, a stalled read) as an ordinary end of body, so the line is the only way a client can
tell a complete 200 from a truncated one. A response WITH `content-length` carries no marker and
needs none: that covers every object below 256 KiB (filtered or not - the string path, exact
UTF-8 length) and every unfiltered large gzip passthrough (`x-econdl-citation-omitted`); there
the declared length is the completeness check. Rule for clients: no `content-length` → require
the marker; `content-length` → compare the bytes received. A short read (fewer bytes than declared) is a truncated transfer. Responses on the string path (objects below 256 KiB) carry
`cache-control: no-transform` so an intermediary does not recode them and the declared length
survives to the client. The gzip passthrough stays edge-negotiated (see above): a client that
wants the stored bytes with their length sends `Accept-Encoding: gzip`; a passthrough that
arrives WITHOUT `content-length` (an intermediary inflated it) carries no completeness line and
is unverifiable - every reference client refuses it. What each client actually sends differs,
and callers should know which they are using: the Python client sets `Accept-Encoding: gzip`
itself and decodes the body; the R client sets nothing, because httr hands the request to
libcurl with `CURLOPT_ACCEPT_ENCODING` empty, which advertises every encoding that libcurl
build supports (gzip and deflate always, br and zstd on newer builds) and decodes the body
before R sees a byte - so an R caller can legitimately receive a re-coded passthrough, and the
refusal above is the check that catches it. The MCP server does not read large passthroughs at
all: it refuses any passthrough above 4 MiB stored, or one with no `content-length`, and asks
for a date window instead, because reading one whole would exceed its isolate's memory limit.

**Filter refusals (400 `unsupported_filter`).** Server-side filtering is refused up front for a
stored object above 114 MB (4 GiB / 37.5, the fleet's largest measured compression ratio: above
it the gzip ISIZE can have wrapped), for a declared decompressed size above 1.5 GB, and for a
declared size above 37.5x the stored size (the fleet's largest measured ratio; the message names
the ratio). All three are 400 `unsupported_filter`. The full object remains available at `?raw=1`.


### `GET /v1/series/{id}.metadata.json`
```jsonc
{
  "series_id": "bls:CUUR0000SA0",
  "source": "bls",
  "title": "...", "frequency": "M", "unit": "...", "geography": "US",
  "start_date": "1913-01-01", "end_date": "2026-05-01", "obs_count": 1357,
  "license": {"id":"...","name":"...","url":"...","reservable":true,
              "commercial_ok":true,"attribution_required":true,"no_modify":false},
  "attribution": "...", "homepage": "...", "terms_url": "...",
  "description_key": ["<caveats; HF equities survivorship-bias bullet is FIRST>"],
  "description_processing": "what we did to the raw source",
  "citation_short": "BLS (2026).",            // producer FIRST, library second
  "citation_long": "U.S. Bureau of Labor Statistics ... Compiled by Elkassabgi Data Library.",
  "last_updated": "2026-06-24T00:00:00Z",
  "csv_url": "/v1/series/bls%3ACUUR0000SA0.csv"
}
```
`description_key` / `description_processing` / `citation_*` come from the series-tier
metadata pass (Task #5); fields absent until populated are omitted, never faked.

### `GET /v1/catalog`  (search + browse)
Params: `q=` (FTS5 over title/geography), `source=`, `limit=` (default 50, max 500),
`offset=`. Returns `{total, limit, offset, results:[{series_id,source,title,frequency,
unit,geography,license_id,start_date,end_date}]}`. FTS5 with a LIKE fallback (mirrors
`core/catalog.py::search`). **Catalogue grain is NOT uniform**, and the response says so:
`"catalog_coverage":"mixed grain: some sources are catalogued per series, others per table or
flow — absence from this catalogue does not mean a series is unavailable"`. Where a source is
catalogued per table or per flow, one catalogue row stands for a whole table and every series
in it lives inside that row's CSV. The flow- and table-grain sets are registered in
`clients/python/econdl/_resolve.py` — `_FLOW_GRAIN` (11 sources: `bfs, cso, dst, hagstofa, scb,
ssb, stat_estonia, stat_latvia, stat_slovenia, statfin, unsdg`) and `_DOT_TABLE_GRAIN` (13
`unctad_*` sources) — and several more are documented individually: `ons_uk` holds 42 catalogue
rows for 3,897,884 series, `istat` 14,267 flows for 43,564,079, `insee_melodi` 139 flows,
`usda` table grain. **Each source's generated page states its own grain**
(`catalog/site/istat.html`: "Served at FLOW grain"), and that page is the authority.

Do **not** infer grain from a source's catalogue row count in either direction — an earlier
version of this paragraph did, and was wrong. A small count often means a small hand-curated
*per-series* selection (`bls` 9 rows, `statcan` 20, `oecd` 28, `abs` 18 — `bls:CUUR0000SA0` is
one series), and a large count with sparse metadata is still per-series (`wid`, 2,465,197 rows,
each one series).

So a `/v1/catalog` search that returns nothing does **not** establish that a series is
unavailable — fetch it by id, or read the source's page. This line said "33 sources" until
2026-08-30 (true when written, rotted as the catalogue grew past 300); the first replacement
claimed series-level coverage for everything, which was false in the opposite direction and was
caught in review before it deployed. The current wording asserts no count and no uniform grain.

### Internationalization — `?lang=` (metadata.json + catalog)
Both `GET /v1/series/{id}.metadata.json` and `GET /v1/catalog` accept an optional
`lang=` selecting an OFFICIAL localized title. Supported: `en, ar, es, fr, ru, zh`
(the set actually loaded from producers' own multilingual APIs — World Bank
`/v2/<lang>/`, IMF/ILO SDMX `xml:lang`; NEVER machine-translated). Titles live at
`series.metadata.titles{<lang>}`. Rules:
- **absent or `lang=en`** → response is **byte-for-byte the pre-i18n shape** (no
  extra keys); this is why the v1.1 conformance pins still hold.
- **a supported lang** → `title` becomes the official localized label; metadata.json
  also adds `title_en` (only when a translation was actually applied) and echoes
  `"lang"`; catalog adds a top-level `"lang"`. Series lacking that language fall
  back to the English `title` (no `title_en`) — graceful, never fabricated.
- **an unsupported lang** → `400 {"error":"unsupported_language","parameter":"lang",
  "value":...,"supported":[...]}` — we never silently return English for a language
  we don't actually have.
The catalog selects `metadata` internally to localize but NEVER emits it in a result
row. Search (FTS) is English-indexed; `?lang=` localizes the *display* title only.
The dev shim (`_LANGS`) and Worker (`SUPPORTED_LANGS`) keep this set in sync.

### `GET /v1/sources`
Every registered source with license/attribution + a freshness summary
(`status`, `last_updated`, `cadence`) joined from `source_state`. The row count is
deliberately NOT pinned here — it changes whenever a source is added, or removed because
we cannot host it (20 were removed 2026-07-22/23). Read it from the endpoint.

### `GET /v1/last-updates`   ([w8], copy DBnomics `/last-updates`)
Per dataset, projected from `unit_state` + `source_state` + registry cadence:
```jsonc
{ "generated": "2026-06-25T...Z",
  "datasets": [{
    "source": "bcb", "unit": "_all",
    "status": "ok",                       // ok | no_change | partial | transient_fail
    "last_updated": "2026-06-24T00:10:55Z",   // unit_state.last_success_utc
    "source_date_accessed": "2026-06-24T00:10:55Z",
    "source_version": "<upstream_vintage>",   // unit_state.upstream_vintage (may be null)
    "last_obs_date": "2026-06-23",
    "next_update_expected": "2026-06-25",     // last_success + cadence interval
    "obs_count": 12345 }] }
```
Canonical SQL (runs verbatim on D1 — D1 *is* SQLite):
```sql
SELECT u.source_id, u.unit_id, u.status, u.last_success_utc, u.upstream_vintage,
       u.last_obs_date, u.obs_count, s.cadence
FROM unit_state u LEFT JOIN source_state s ON s.source_id = u.source_id
ORDER BY u.source_id, u.unit_id;
```
`next_update_expected` = `last_success_utc` + {daily:1d, weekly:7d, monthly:30d,
quarterly:91d}. A unit with no `last_success_utc` reports `last_updated:null,
status:"<current>"` — never a fabricated date.

### `GET /v1/bundle`  →  bundle MANIFEST (client-side fan-out, [w10])
Params: `ids=` (repeatable/comma) **or** `source=`; `snapshot=YYYY-MM-DD` (default today).
Returns a Frictionless-shaped `datapackage.json` skeleton: one resource per source with
`path` = the resource's stable URL, `econdl:series_ids`, `econdl:provenance`
(license/attribution/citation from the registry), and — when known — `bytes`+`hash`.
The client fetches each resource URL and assembles the zip locally. **The Worker never
streams the zip** (subrequest/memory limits). Unresolvable ids are returned under
`"econdl:unresolved":[{id,reason}]` — loud, never dropped.

## Backend binding (one code path, two backends)
| concern | dev shim (now) | Worker (prod) |
|---|---|---|
| catalog/license/series | `data/catalog.db` (SQLite, ro) | D1 (same SQL) |
| freshness | `data/_aqueduct/state.db` | D1 (`unit_state`/`source_state`) |
| series rows | `econdl._resolve` over `data/clean_full` | R2 GET native parquet + range/predicate |
| identity | URL-decode `{id}` → resolver | same |

The dev shim imports `econdl._resolve` directly, so **the API and the client resolve a
series through the exact same code** — they cannot disagree.

## Canonical response shapes (v1.1 — reconciled 2026-06-26)
The dev shim and the Worker MUST be byte-for-byte identical. Where the first build
diverged, these are the pinned canonical shapes (the verify pass enforces them):

- **`/v1/series/{id}.csv` identity column** — the `series_id` column is exactly what
  `econdl._resolve.native_to_tidy` emits: the **native key** (or, for filename-identity
  sources, the stamped catalog id). NOT the requested catalog id for 1:1 series. This is
  what makes a LOCAL bundle and an HTTP bundle of the same ids row-for-row identical,
  key column included.

- **`/v1/series/{id}.metadata.json`** — include `category`. For human context, emit
  `description_key`/`description_processing`/`citation_short`/`citation_long` when the
  series metadata carries them (Task #5), ELSE fall back to the catalog `description`/
  `citation` keys. `last_updated` falls back to the source's `unit_state` (`_all`)
  `last_success_utc` when the series row's own `last_updated` is null — never fabricated.

- **`/v1/sources`** — NESTED per source:
  `{ "source": <id>, "name", "homepage", "license": {id,name,url,reservable,commercial_ok,attribution_required,no_modify}|null, "freshness": {status,last_updated,cadence}|null }`.

- **`/v1/last-updates` cadence** — `next_update_expected` = `last_success_utc` +
  {daily:1, weekly:7, monthly:30, quarterly:91, **annual:365**} days. Any other cadence
  (`irregular`, `static`, unknown, or no `last_success_utc`) → `next_update_expected: null`.

- **`/v1/bundle` manifest** — Frictionless-shaped: top-level `name`, `profile`,
  `econdl:schema_version`, `econdl:client`, `econdl:snapshot_date`,
  `econdl:series_requested`, `econdl:resource_url_count`, `econdl:fanout_note`,
  `licenses[]`, `resources[]`, and `econdl:unresolved[]` (loud, never dropped). Each
  resource: `{name, profile, format, mediatype, path: [<stable URLs>],
  econdl:series_ids, econdl:provenance (incl. citation)}`. The client fans out over
  `path`; the server never streams a zip.

- **Status codes (reconciled).** `501 not_migrated` ONLY when the source has no resolver.
  A supported source whose at-rest object/file is **absent** → `502 {"error":"data_unavailable"}`
  (the source is migrated; the object just isn't published yet). A present file/window
  that yields **zero rows** → `502 {"error":"resolver_empty"}`. `404` only when the id is
  not in the catalog. Both implementations return the SAME code for the same condition.

A pytest conformance test (`api/test_conformance.py`) asserts the shim's response shapes
against these pins so the byte-for-byte agreement is enforced, not just asserted.

## Consumers of this contract

A change to a served byte shape is tested against EVERY reader below before it ships (R601:
the citation header of 2026-07-09 broke the Python client for 55 days because nobody ran it).

| consumer | where | what pins it |
|---|---|---|
| Python client `econdl` (0.1.1) | `clients/python/econdl/_http.py` (`parse_series_csv`, `_decode_body`, `_read_body`) | `tests/test_python_client_csv_parse.py` (citation form, bare form, gzip passthrough, marker rule, mid-id `#`, empty body, short read / cut gzip) |
| R client `econdatalibrary` | `clients/r/econdatalibrary/R/client.R` (`edl_series`) | read-only review (no R runtime on the build box) - strips `#`-leading lines, marker rule keyed on content-length / `x-econdl-citation-omitted` |
| MCP server | `mcp/src/index.ts` (`get_econ_series`, requests `raw=1`) | `npx tsc --noEmit` (one pre-existing SDK type clash, unrelated); strips `#`-leading lines, marker rule |
| Worker itself | `api/worker/src/series.ts`, `csvStream.ts` | `node --test api/worker/test/csvStream.test.ts`; local e2e against `wrangler dev --local` (launch config `econ-api-local-test`) |
| Pipelines quoting the header text | the citation header says `pandas pd.read_csv(url, comment="#")` | keep that sentence true whenever the header or the completeness line changes |
