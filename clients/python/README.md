# econdl — the Econ Data Library Python client

`econdl` does the one thing single-provider catalogs (FRED, World Bank, OECD) structurally **cannot**: it assembles a **multi-source** dataset into a single versioned, citable, **one-command-updatable** artifact.

```python
import econdl

# 1. find series
econdl.search("unemployment rate")

# 2. bundle across sources -> tidy DataFrame + a pinned lockfile + a zip
df = econdl.bundle(
    ["bls:LNS14000000",
     "worldbank_wdi:NY.GDP.MKTP.KD.ZG",
     "penn_world_table:rgdpe:USA"],
    out="mystudy.zip",
)

# 3. rebuild the EXACT numbers in your paper, any time, from the lockfile
df = econdl.pull("mystudy/datapackage.json")

# 3b. or opt in to refreshed data
df = econdl.pull("mystudy/datapackage.json", latest=True)
```

## Why this matters

The bundle's `datapackage.json` is a [Frictionless](https://frictionlessdata.io/) data package that doubles as a **re-runnable lockfile**. It pins:

- `econdl:snapshot_date` — the version you cite,
- a per-resource **`sha256`** — so reproduction is verifiable, not hopeful,
- **license / attribution / citation** for every source, pulled straight from the central registry.

`pull()` reproduces the pinned snapshot **by default** and **verifies every hash** (a corrupted or altered resource raises, it is never returned). `pull(..., latest=True)` is an explicit opt-in to refreshed data, and it **loudly warns — never silently skips** — any pinned series it cannot satisfy.

## API

| call | returns | does |
|---|---|---|
| `econdl.search(query, limit=20)` | list of catalog rows (dicts) | FTS5 search over the series catalog |
| `econdl.bundle(series_ids=[...], out=...)` | tidy `DataFrame` `[series_id, source, obs_date, value]` | writes `datapackage.json` lockfile + one native-parquet resource per source + a `.zip` |
| `econdl.bundle(source="bls", out=...)` | tidy `DataFrame` | bundles every catalog series of one source the client can resolve |
| `econdl.pull(datapackage)` | tidy `DataFrame` | reproduces the pinned snapshot, verifying `sha256` |
| `econdl.pull(datapackage, latest=True)` | tidy `DataFrame` | re-projects fresh data; warns on anything it can't satisfy |
| `econdl.supported_sources()` | list of source ids | sources with an at-rest resolver today |

`datapackage` may be a path to `datapackage.json`, a bundle directory, or the `.zip`.

## Bundle layout (ARCHITECTURE.md §6)

```
mystudy/
  datapackage.json        # Frictionless package == the lockfile (pins snapshot + sha256 + license)
  README.md               # human-readable sources, licenses, citations
  data/
    bls.parquet           # one resource per source, native long parquet
    worldbank_wdi.parquet
    penn_world_table.parquet
```

The same tree is also emitted as `mystudy.zip`.

## Install

```bash
pip install -e clients/python        # editable, from this repo
```

Dependencies are `pandas` + `pyarrow` only.

## Configuration

The client reads the local registry and store by default; override with env vars:

- `ECONDL_CATALOG` — path to `catalog.db` (default: the repo's `data/catalog.db`)
- `ECONDL_DATA` — path to the at-rest store (default: the repo's `data/clean_full`)

## Note on coverage

The at-rest store is mid-migration (ARCHITECTURE.md §7): not every one of the ~299 sources has been normalised to a uniform physical layout yet, so the client resolves a growing subset (`econdl.supported_sources()`). Sources without a resolver raise a clear error at `bundle()` time — they are **never silently dropped**. Adding a source is a single declarative entry in `econdl/_resolve.py`; the public API does not change.

## End-to-end demo

```bash
python clients/python/examples/quickstart.py
```

bundles 5 real series across 3 sources, pulls the lockfile back, and asserts the data reproduces row-for-row; then demonstrates tamper detection and the loud-warning path.
