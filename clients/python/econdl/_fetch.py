"""fetch() -- cross-section query by dimension mask ([w12], ARCHITECTURE §9).

DBnomics SDMX-mask style: instead of naming every ``series_id`` by hand, give a
provider plus a few dimensions and let the CATALOG resolve the matching
``series_id`` set, then return the SAME tidy frame ``bundle()`` returns.

    import econdl
    df = econdl.fetch("worldbank", "NY.GDP.MKTP.CD", geo=["DEU", "FRA", "ITA"])
    # -> tidy [series_id, source, obs_date, value] for those three countries' GDP

This is a THIN convenience over ``data/catalog.db`` + the existing
``_resolve``/``read_native``/``native_to_tidy`` projection path -- it adds no new
storage machinery. The dimension mask (``freq``/``geo``, plus an optional
``dataset`` predicate) maps directly onto catalog columns
(``frequency``/``geography``) which ARE the universal dimensions the at-rest model
promotes (ARCHITECTURE §1). ``unit`` and ``category`` are honoured the same way.

Honest-status is the law: ids the catalog matches but the local store cannot
resolve (a coverage gap, mid-migration per ARCHITECTURE §7) are surfaced in a
LOUD warning and listed on ``df.attrs['econdl_unresolved']`` -- they are NEVER
silently dropped. A mask that matches nothing returns an empty (well-formed) tidy
frame with a loud warning, never a crash.
"""
from __future__ import annotations

import warnings
from typing import Iterable

import pandas as pd

from . import _catalog, _proxy, _resolve
from ._resolve import ResolveError, _CANON


def _as_list(v) -> list[str] | None:
    """Accept a scalar or an iterable of scalars; None stays None (no filter)."""
    if v is None:
        return None
    if isinstance(v, str):
        return [v]
    if isinstance(v, Iterable):
        out = [str(x) for x in v]
        return out
    return [str(v)]


def _in_clause(col: str, values: list[str]) -> tuple[str, list[str]]:
    """A parameterised ``col IN (?, ?, ...)`` fragment (empty list -> never-true)."""
    if not values:
        return "0", []
    placeholders = ",".join("?" for _ in values)
    return f"{col} IN ({placeholders})", list(values)


def resolve_mask(
    provider: str,
    dataset: str | None = None,
    *,
    freq=None,
    geo=None,
    unit=None,
    category=None,
    db: str | None = None,
) -> list[str]:
    """Resolve a dimension mask to the matching catalog ``series_id`` set.

    Pure catalog query (no store I/O). ``provider`` is the ``source_id``; the
    optional ``dataset`` is matched against the id/title (the DBnomics DATASET
    level lives in the SERIES tail of our ids today, ARCHITECTURE §2); ``freq``/
    ``geo``/``unit``/``category`` are matched against the catalog's
    ``frequency``/``geography``/``unit``/``category`` columns and each accepts a
    scalar OR a list. Returns ids sorted, deduplicated.
    """
    where = ["source_id = ?"]
    params: list[str] = [provider]

    if dataset is not None:
        # The DATASET namespace level is encoded in the SERIES tail of our catalog
        # ids today (ARCHITECTURE §2: PROVIDER/DATASET/SERIES; pre-migration the
        # whole tail lives after 'provider:'). Match it as a token of the id OR an
        # exact category, so e.g. worldbank 'NY.GDP.MKTP.CD' selects that indicator
        # across geos without depending on the giants/migration landing first.
        where.append("(series_id LIKE ? OR series_id LIKE ? OR category = ?)")
        params += [f"{provider}:{dataset}:%", f"{provider}:{dataset}", dataset]

    for col, raw in (
        ("frequency", freq),
        ("geography", geo),
        ("unit", unit),
        ("category", category),
    ):
        vals = _as_list(raw)
        if vals is not None:
            frag, p = _in_clause(col, vals)
            where.append(frag)
            params += p

    sql = (
        "SELECT series_id FROM series WHERE "
        + " AND ".join(where)
        + " ORDER BY series_id"
    )
    conn = _catalog.connect(db)
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    # dedupe defensively (ORDER BY already deterministic)
    seen: dict[str, None] = {}
    for r in rows:
        seen.setdefault(r["series_id"], None)
    return list(seen)


def fetch(
    provider: str,
    dataset: str | None = None,
    *,
    freq=None,
    geo=None,
    unit=None,
    category=None,
    db: str | None = None,
    data_root: str | None = None,
) -> pd.DataFrame:
    """Cross-section query: resolve a dimension mask -> the tidy frame for it.

    A DBnomics SDMX-mask-style convenience ([w12]): name a ``provider`` and a few
    dimensions instead of every ``series_id`` by hand. The catalog resolves the
    matching ``series_id`` set, then the SAME ``_resolve``/``read_native``/
    ``native_to_tidy`` path ``bundle()`` uses projects each id, so the returned
    frame is the identical tidy ``[series_id, source, obs_date, value]`` shape.

    Parameters
    ----------
    provider : catalog ``source_id`` (e.g. ``"worldbank"``, ``"ecb"``).
    dataset  : optional dataset/indicator predicate matched against the id tail or
               ``category`` (e.g. ``"NY.GDP.MKTP.CD"``, ``"EXR"``). ``None`` = the
               whole provider.
    freq, geo, unit, category : dimension filters; each a scalar OR a list, matched
               against the catalog ``frequency`` / ``geography`` / ``unit`` /
               ``category`` columns (``IN`` semantics for a list).
    db        : catalog override (defaults to the bundled ``catalog.db``).
    data_root : at-rest store override (defaults to ``$ECONDL_DATA``).

    Returns
    -------
    Tidy ``DataFrame[series_id, source, obs_date, value]``. Two honest-status
    invariants (never silent, ARCHITECTURE §9):

    * Ids the catalog matched but the local store could NOT resolve (a coverage
      gap, mid-migration) are LOUDLY warned and listed on
      ``df.attrs['econdl_unresolved']`` -- never dropped from the accounting.
    * A mask that matches nothing returns an EMPTY well-formed tidy frame with a
      loud warning -- never a crash.

    Example
    -------
    >>> import econdl
    >>> df = econdl.fetch("worldbank", "NY.GDP.MKTP.CD", geo=["DEU", "FRA", "ITA"])
    >>> sorted(df["series_id"].unique())
    ['worldbank:NY.GDP.MKTP.CD:DEU', 'worldbank:NY.GDP.MKTP.CD:FRA', 'worldbank:NY.GDP.MKTP.CD:ITA']
    """
    matched = resolve_mask(
        provider, dataset, freq=freq, geo=geo, unit=unit, category=category, db=db
    )

    if not matched:
        warnings.warn(
            f"econdl.fetch({provider!r}"
            + (f", {dataset!r}" if dataset is not None else "")
            + f", freq={freq!r}, geo={geo!r}, unit={unit!r}, category={category!r}): "
            "the dimension mask matched ZERO catalog series. Returning an empty "
            "tidy frame. Check the provider/dataset spelling and that freq/geo/unit "
            "use the catalog's codes (e.g. ISO-3 geographies, single-letter freq).",
            stacklevel=2,
        )
        empty = pd.DataFrame(columns=_CANON)
        empty.attrs["econdl_matched_ids"] = []
        empty.attrs["econdl_resolved_ids"] = []
        empty.attrs["econdl_unresolved"] = []
        return empty

    # PROXY GATE (ARCHITECTURE §9 [w5]): a cross-section must not re-serve values
    # from a NON-redistributable source either. Partition proxied ids out BEFORE
    # any store read -- their values are never read into the tidy frame; they are
    # surfaced loudly (attrs['econdl_proxied']) so a caller is never misled into
    # thinking the mask "missed" them. Obtain those from the upstream provider.
    proxied: list[str] = [sid for sid in matched if _proxy.is_proxied(sid, db=db)]
    matched = [sid for sid in matched if sid not in set(proxied)]

    parts: list[pd.DataFrame] = []
    resolved: list[str] = []
    unresolved: list[tuple[str, str]] = []
    native_only: list[str] = []

    for sid in matched:
        try:
            res = _resolve.resolve(sid, root=data_root)   # raises loudly if unsupported
            table = _resolve.read_native(res)             # raises loudly if empty
        except (ResolveError, FileNotFoundError) as e:
            unresolved.append((sid, str(e)))
            continue
        if res.tidy_ok:
            parts.append(_resolve.native_to_tidy(res, table))
        else:
            # relational/wide source with no canonical value column: it resolved
            # (the rows exist) but has no place in a tidy cross-section frame.
            native_only.append(sid)
        resolved.append(sid)

    if unresolved:
        msg = "\n".join(f"  - {sid}: {err}" for sid, err in unresolved)
        warnings.warn(
            f"econdl.fetch({provider!r}): {len(unresolved)} of {len(matched)} "
            "catalog-matched series could NOT be resolved in the local store "
            "(store-coverage gap, mid-migration -- NOT silently dropped):\n"
            f"{msg}",
            stacklevel=2,
        )
    if native_only:
        warnings.warn(
            f"econdl.fetch({provider!r}): {len(native_only)} matched series come "
            "from a relational/wide source (no canonical value column) and are "
            "NOT in the returned tidy frame; bundle() them to get the native "
            f"parquet. ids: {', '.join(native_only[:6])}"
            f"{' ...' if len(native_only) > 6 else ''}",
            stacklevel=2,
        )
    if proxied:
        warnings.warn(
            f"econdl.fetch({provider!r}): {len(proxied)} matched series come from a "
            "NON-redistributable source (license reservable=0) and are NOT re-served "
            "in the tidy frame. Obtain them from the upstream provider under its "
            f"terms (bundle() them to get the upstream proxy manifest). ids: "
            f"{', '.join(proxied[:6])}{' ...' if len(proxied) > 6 else ''}",
            stacklevel=2,
        )

    tidy = (
        pd.concat(parts, ignore_index=True)
        .sort_values(["source", "series_id", "obs_date"])
        .reset_index(drop=True)
        if parts
        else pd.DataFrame(columns=_CANON)
    )
    tidy.attrs["econdl_matched_ids"] = list(matched)
    tidy.attrs["econdl_resolved_ids"] = resolved
    tidy.attrs["econdl_unresolved"] = [sid for sid, _ in unresolved]
    tidy.attrs["econdl_native_only"] = native_only
    tidy.attrs["econdl_proxied"] = proxied
    return tidy
