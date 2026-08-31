# econdatalibrary — R client

R client for the [Econ Data Library](https://econdatalibrary.com): economic and financial
time series from international organizations, national statistical offices, central banks
and research datasets — each served with its producer's licence, attribution and citation.

It mirrors the Python client (`clients/python/econdl`) so the two answer the same questions
with the same words.

## Install

```r
# install.packages("remotes")
remotes::install_github("elkassabgi/econdatalibrary", subdir = "clients/r/econdatalibrary")
```

Requires `httr`. Nothing else — series come back through base R's CSV reader.

## Use

```r
library(econdatalibrary)

# Catalogue search and metadata need no key.
sources <- edl_sources()                        # every source we actually serve
hits    <- edl_search("unemployment rate")      # find series
meta    <- edl_metadata("bls:LNS14000000")      # licence, attribution, citation

# Downloading data does.  Registration is free.
edl_set_key("YOUR_API_KEY")                     # or set EDL_API_KEY
df <- edl_series("bls:LNS14000000")             # -> data.frame(series_id, obs_date, value)
df <- edl_series("bls:LNS14000000", from = "2000-01-01")

# One indicator across many geographies, as one tidy frame.
panel <- edl_fetch("worldbank", "NY.GDP.MKTP.CD", geo = c("DEU", "FRA", "ITA"))
```

## Two things worth knowing before you conclude anything

**A search that returns nothing does not mean the series is unavailable.** Catalogue grain is
not uniform: large sources are catalogued per *table* or per *flow*, with every series inside
that row's CSV. `ons_uk` holds 42 catalogue rows for 3,897,884 series. The API says so in its
`catalog_coverage` field, which `edl_search()` attaches to the result:

```r
attr(hits, "coverage")
#> "mixed grain: some sources are catalogued per series, others per table or flow —
#>   absence from this catalogue does not mean a series is unavailable"
```

**Headline totals may be under recalculation.** `edl_stats()` says so out loud when they are,
rather than letting you treat a provisional figure as settled.

## Cite the producer first

Every series carries the terms it came with. `edl_metadata(id)` returns the licence, the
required attribution, and a ready-made citation:

```r
m <- edl_metadata("bls:LNS14000000")
m$license$id        #> "us-public-domain"
m$attribution       #> "Source: U.S. BLS (public domain)"
m$citation_long     #> producer first, this library second
```

The compilation is CC BY 4.0; each underlying series stays governed by its original
producer's terms. Sources whose licence does not permit re-hosting are not served and are not
listed — `edl_sources()` shows what you can actually obtain, and a request for a gated source
returns a clear error rather than an empty frame.

## Errors are loud, never empty

An empty result and a refused request must not look alike, so each failure raises with its
reason: no API key, id not in the catalogue, source not redistributable (451), source not yet
resolvable (501), or a series that resolved to zero rows (502). `edl_fetch()` reports any
geography it could not serve and returns the rest, listing the failures in
`attr(result, "failed")`.
