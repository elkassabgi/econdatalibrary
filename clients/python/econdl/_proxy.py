"""Proxy / pull-through mode for NON-redistributable sources (ARCHITECTURE §9 [w5]).

Some cataloged sources carry a license whose ``reservable`` flag is 0 -- we are
NOT permitted to re-host their observations or stamp our DOI on them. The honest
posture (copying DBnomics' license-passthrough stance) is: for such a series,
``bundle()`` must NEVER read or copy our parquet and must NEVER emit our citation
as if it were ours. Instead it emits a *manifest-only* resource that points at the
UPSTREAM provider with full license + provenance + attribution, explicitly marked
"not redistributed -- obtain from the source under its terms".

This module is the single place that:
  1. decides, per series, whether it is redistributable -- via the source's
     license ``reservable`` flag (``_catalog.get_source`` -> ``license_id`` ->
     ``_catalog.get_license.reservable``), the EXACT gate the brief specifies; and
  2. builds the upstream-pointing proxy resource (path + provenance) for a
     non-redistributable series, drawing the true upstream provider from the
     series' own catalog metadata (for an aggregator like DBnomics, each series
     names its real underlying provider/website/terms -- we point THERE, never at
     our re-hosted copy).

The gate is honest-tri-state: ``True`` (redistributable), ``False`` (proxy), or
``None`` (cannot determine -- e.g. no local catalog, no license row). ``None`` is
NEVER laundered into ``True``; callers treat an undeterminable gate loudly.
"""
from __future__ import annotations

import json
from typing import Any

from . import _catalog


# --------------------------------------------------------------------------- #
# the gate
# --------------------------------------------------------------------------- #

def reservable_state(series_id: str, *, db: str | None = None) -> bool | None:
    """Tri-state redistributability for one catalog series.

    Returns ``True`` if the source's license is reservable (we may re-host),
    ``False`` if it is NOT (must be proxied), or ``None`` if it cannot be
    determined (no catalog row / no license_id / no license row). ``None`` is an
    honest "unknown" -- callers must NOT treat it as redistributable.

    The gate is on the SOURCE's license row (ARCHITECTURE §3: ``reservable`` lives
    on ``license``, reached via ``source.license_id``), exactly as the brief
    pins it.
    """
    src = _catalog.source_of(series_id)
    try:
        src_row = _catalog.get_source(src, db=db)
    except FileNotFoundError:
        return None
    if not src_row:
        return None
    lic_id = src_row.get("license_id")
    if not lic_id:
        return None
    lic = _catalog.get_license(lic_id, db=db)
    if not lic or lic.get("reservable") is None:
        return None
    return bool(lic.get("reservable"))


def is_proxied(series_id: str, *, db: str | None = None) -> bool:
    """True iff the series must be proxied (its source license reservable == 0).

    An undeterminable gate (``None``) is NOT proxied here -- it is surfaced by the
    caller as an honest resolve question, not silently re-hosted nor silently
    proxied.
    """
    return reservable_state(series_id, db=db) is False


# --------------------------------------------------------------------------- #
# upstream provenance for a proxied series
# --------------------------------------------------------------------------- #

_PROXY_NOTE = (
    "Not redistributed under this source's license; fetch from the provider "
    "under the original terms."
)


def _series_metadata(series_id: str, db: str | None) -> dict[str, Any]:
    """The series' catalog ``metadata`` JSON (the upstream provider facts), or {}."""
    try:
        row = _catalog.get_series(series_id, db=db)
    except FileNotFoundError:
        return {}
    if not row or not row.get("metadata"):
        return {}
    try:
        md = json.loads(row["metadata"])
        return md if isinstance(md, dict) else {}
    except (ValueError, TypeError):
        return {}


def _license_block(source_row: dict[str, Any], db: str | None) -> dict[str, Any] | None:
    lic_id = (source_row or {}).get("license_id")
    if not lic_id:
        return None
    lic = _catalog.get_license(lic_id, db=db) or {}
    return {
        "id": lic_id,
        "name": lic.get("name"),
        "url": lic.get("url"),
        "reservable": bool(lic.get("reservable")),
        "commercial_ok": bool(lic.get("commercial_ok")),
        "attribution_required": bool(lic.get("attribution_required")),
        "no_modify": bool(lic.get("no_modify")),
    }


def upstream_url(series_id: str, *, db: str | None = None) -> str:
    """The best UPSTREAM URL to point a researcher at to obtain the data themselves.

    Preference order (most specific first), all pointing AWAY from our store:
      1. the series' own upstream landing page derived from metadata
         (for legacy relay-era series only, DBnomics: ``https://db.nomics.world/<dbnomics_path>`` -- fetching from it is BANNED (R251); this is a provenance pointer, the
         provider-attributed series page), then the provider website / terms;
      2. the source's homepage / terms_url from the registry;
      3. a bare honest marker if nothing upstream is on record.
    """
    md = _series_metadata(series_id, db)
    # DBnomics-style aggregator: the path locates the provider-attributed series.
    path = md.get("dbnomics_path")
    if path:
        return f"https://db.nomics.world/{path}"  # provenance pointer ONLY - fetching BANNED (R251)
    for k in ("provider_website", "provider_terms_of_use"):
        if md.get(k):
            return str(md[k])
    src_row = _catalog.get_source(_catalog.source_of(series_id), db=db) or {}
    for k in ("homepage", "terms_url"):
        if src_row.get(k):
            return str(src_row[k])
    return f"upstream://{_catalog.source_of(series_id)}"


_NOT_REDISTRIBUTED = (
    "Not redistributed by Elkassabgi Data Library; obtained directly from the "
    "provider under its terms"
)


def _citation(series_id: str, md: dict[str, Any], src_row: dict[str, Any],
              snapshot_date: str) -> str:
    """Producer-first, honest citation for a proxied series (we are NOT the source).

    Honors the producer-first rule (ARCHITECTURE §9 [w7]): credit the underlying
    provider. CRITICAL honesty fix: a proxied series is NOT redistributed by us, so
    we must never let a curated "compiled and redistributed by the Elkassabgi Data
    Library" claim ride along (the dbnomics ``citation_long`` carries exactly that).
    We therefore prefer the producer-first ``citation_short`` (no redistribution
    claim), fall back to the upstream provider name, and ALWAYS append an explicit
    "not redistributed by us" clause stamped with the access date.
    """
    base = md.get("citation_short")
    if not base:
        provider = (md.get("provider_name") or (src_row or {}).get("name")
                    or _catalog.source_of(series_id))
        base = f"{provider}."
    return f"{base.rstrip()} {_NOT_REDISTRIBUTED} (accessed {snapshot_date})."


def proxy_provenance(series_id: str, snapshot_date: str, *,
                     db: str | None = None) -> dict[str, Any]:
    """Full upstream provenance block for a proxied (non-redistributable) series.

    Carries everything a researcher needs to obtain + cite the data from the
    ORIGINAL provider: license, attribution, citation, homepage, terms_url, plus
    the upstream provider's name/website when the source is an aggregator. Nothing
    here points at our re-hosted parquet or our DOI.
    """
    src = _catalog.source_of(series_id)
    src_row = _catalog.get_source(src, db=db) or {}
    md = _series_metadata(series_id, db)

    homepage = md.get("provider_website") or src_row.get("homepage")
    terms_url = md.get("provider_terms_of_use") or src_row.get("terms_url")
    attribution = src_row.get("attribution")

    return {
        "source_id": src,
        "name": src_row.get("name"),
        "upstream_provider": md.get("provider_name"),     # the TRUE origin (aggregator case)
        "upstream_dataset": md.get("dataset_name") or md.get("dataset_code"),
        "license": _license_block(src_row, db),
        "license_hint": md.get("provider_license_hint"),  # provider-specific license, if known
        "attribution": attribution,
        "homepage": homepage,
        "terms_url": terms_url,
        "citation": _citation(series_id, md, src_row, snapshot_date),
    }


def proxy_resource(series_id: str, snapshot_date: str, *,
                   db: str | None = None) -> dict[str, Any]:
    """A datapackage resource of the PROXY kind for one non-redistributable series.

    Shape (per the brief): ``econdl:reservable=False``, ``econdl:proxy=True``,
    ``path`` = [upstream URL] (NOT a local parquet), full ``econdl:provenance``,
    and an explicit ``econdl:note``. There is no ``hash``/``bytes``/``schema`` --
    we hold no bytes for it by design.
    """
    return {
        "name": series_id,
        "profile": "data-resource",
        "econdl:reservable": False,
        "econdl:proxy": True,
        "path": [upstream_url(series_id, db=db)],
        "econdl:series_ids": [series_id],
        "econdl:provenance": proxy_provenance(series_id, snapshot_date, db=db),
        "econdl:note": _PROXY_NOTE,
    }
