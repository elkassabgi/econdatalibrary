"""econdl -- the Econ Data Library client.

The one thing no single-provider catalog ships: a multi-source dataset bundled as
a versioned, citable, **one-command-updatable** artifact.

    import econdl
    econdl.search("unemployment rate")           # find series in the catalog
    df = econdl.fetch(                            # cross-section by dimension mask
        "worldbank", "NY.GDP.MKTP.CD", geo=["DEU", "FRA", "ITA"],
    )                                            # -> tidy frame for those 3 countries
    df = econdl.bundle(                           # tidy frame + a pinned lockfile
        ["bls:LNS14000000", "worldbank_wdi:NY.GDP.MKTP.KD.ZG"],
        out="mystudy.zip",
    )
    df = econdl.pull("mystudy/datapackage.json")  # rebuild the EXACT numbers
    df = econdl.pull("mystudy/datapackage.json", latest=True)  # opt in to refresh

The bundle's ``datapackage.json`` is a Frictionless data package that doubles as
a re-runnable lockfile: it pins ``snapshot_date`` + a per-resource ``sha256`` +
license / attribution / citation from the central registry. ``pull()`` reproduces
the pinned snapshot by default (verifying hashes) and loudly warns -- never
silently skips -- any series it cannot satisfy.
"""
from __future__ import annotations

from ._bundle import SCHEMA_VERSION, bundle, pull
from ._catalog import get_license, get_series, get_source, search
from ._fetch import fetch, resolve_mask
from ._http import HttpClient, HttpResolveError
from ._proxy import is_proxied, reservable_state
from ._resolve import ResolveError, supported_sources

__version__ = "0.1.0"

__all__ = [
    "search",
    "bundle",
    "pull",
    "fetch",
    "resolve_mask",
    "get_series",
    "get_source",
    "get_license",
    "is_proxied",
    "reservable_state",
    "supported_sources",
    "HttpClient",
    "HttpResolveError",
    "ResolveError",
    "SCHEMA_VERSION",
    "__version__",
]
