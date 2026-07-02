"""bundle() / pull() -- the differentiator (STRATEGY.md goal #4).

A bundle is one self-describing folder/zip whose ``datapackage.json`` is a
re-runnable LOCKFILE: it pins the ``snapshot_date``, a per-resource ``sha256``,
and the license / attribution / citation pulled from the registry. One resource
per source, native long parquet copied straight from the store (ARCHITECTURE §6).

  bundle(...)  -> tidy DataFrame  +  writes datapackage.json (+ native parquet) + zip
  pull(dp)     -> rebuilds the EXACT pinned bundle (verifies sha256) by default;
                  pull(dp, latest=True) re-projects fresh data from the store.

pull() LOUDLY WARNS and never silently skips a series it cannot satisfy.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import warnings
import zipfile
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import _catalog, _proxy, _resolve
from ._resolve import ResolveError

SCHEMA_VERSION = "1.0"
_PROFILE = "tabular-data-package"


def _tidy_to_native_table(tidy: pd.DataFrame) -> pa.Table:
    """Build the per-source native parquet body from an HTTP tidy frame.

    Over the HTTP transport the rows arrive as the contract's long
    ``series_id,obs_date,value`` CSV (tidy), so the bundled resource parquet IS
    that tidy projection -- its sha256 honestly pins exactly the bytes the API
    delivered. ``obs_date`` is stored as a string (YYYY-MM-DD) so the column is
    stable across pandas/pyarrow date round-trips and pull() reconstructs the
    same tidy frame via the recorded ``econdl:key_col='series_id'``.
    """
    body = pd.DataFrame({
        "series_id": tidy["series_id"].astype(str).values,
        "obs_date": pd.to_datetime(tidy["obs_date"]).dt.strftime("%Y-%m-%d").values,
        "value": pd.to_numeric(tidy["value"], errors="coerce").values,
    })
    return pa.Table.from_pandas(body, preserve_index=False)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _today() -> str:
    return date.today().isoformat()


def _resource_name(source: str) -> str:
    return f"{source}.parquet"


def _build_citation(source_row: dict[str, Any], snapshot_date: str) -> str:
    """Synthesize a citation string from registry facts + the pinned snapshot.

    Citation identity is derived, not stored, and is snapshot-stamped
    (ARCHITECTURE §5: never encode version in series_id; stamp snapshot_date).
    """
    name = (source_row or {}).get("name") or (source_row or {}).get("source_id", "")
    home = (source_row or {}).get("homepage")
    year = snapshot_date[:4]
    cite = f"{name} ({year}). Accessed via Econ Data Library, snapshot {snapshot_date}."
    if home:
        cite += f" {home}"
    return cite


def _source_block(source: str, snapshot_date: str, db: str | None) -> dict[str, Any]:
    """License / attribution / citation for one source, straight from the registry."""
    src = _catalog.get_source(source, db=db) or {}
    lic_id = src.get("license_id")
    lic = _catalog.get_license(lic_id, db=db) if lic_id else None
    return {
        "source_id": source,
        "name": src.get("name"),
        "homepage": src.get("homepage"),
        "attribution": src.get("attribution"),
        "terms_url": src.get("terms_url"),
        "license": {
            "id": lic_id,
            "name": (lic or {}).get("name"),
            "url": (lic or {}).get("url"),
            "reservable": bool((lic or {}).get("reservable")),
            "commercial_ok": bool((lic or {}).get("commercial_ok")),
            "attribution_required": bool((lic or {}).get("attribution_required")),
            "no_modify": bool((lic or {}).get("no_modify")),
        } if lic_id else None,
        "citation": _build_citation(src, snapshot_date),
    }


# --------------------------------------------------------------------------- #
# HTTP acquisition (api=...) -- the SINGLE place the transport differs.
# --------------------------------------------------------------------------- #

def _acquire_via_http(
    api: str,
    series_ids: list[str],
    by_source: dict[str, list[pa.Table]],
    source_meta: dict[str, dict],
    tidy_parts: list[pd.DataFrame],
    resolved_ids: list[str],
) -> None:
    """Fetch every requested series over the /v1 .csv endpoint, populating the
    same structures the local path builds. LOUDLY WARNS and never silently skips
    a series the server cannot satisfy (parity with the local goal #4).

    The HTTP transport only serves tidy-able sources (a relational/wide source
    returns 501 over .csv), so every HTTP resource is tidy with
    key_col='series_id'; its parquet body is the tidy projection (sha256 pins the
    exact bytes the API delivered).
    """
    from ._http import HttpClient, HttpResolveError

    client = HttpClient(api)
    missing: list[tuple[str, str]] = []
    for sid in series_ids:
        try:
            tidy = client.fetch_series_csv(sid)            # raises HttpResolveError on non-200
        except HttpResolveError as e:
            missing.append((sid, str(e)))
            continue
        src = sid.split(":", 1)[0]
        by_source.setdefault(src, []).append(_tidy_to_native_table(tidy))
        source_meta[src] = {"key_col": "series_id", "tidy_ok": True}
        tidy_parts.append(tidy)
        resolved_ids.append(sid)

    if missing:
        msg = "\n".join(f"  - {sid}: {err}" for sid, err in missing)
        warnings.warn(
            f"econdl.bundle(api=...): {len(missing)} of {len(series_ids)} requested "
            f"series could NOT be fetched from {api!r} and were NOT silently "
            f"dropped:\n{msg}",
            stacklevel=3,
        )
    if not resolved_ids:
        raise ResolveError(
            f"econdl.bundle(api={api!r}): none of the requested series could be "
            "fetched from the API. Nothing to bundle."
        )


# --------------------------------------------------------------------------- #
# bundle
# --------------------------------------------------------------------------- #

def bundle(
    series_ids: list[str] | None = None,
    *,
    source: str | None = None,
    out: str = "bundle.zip",
    db: str | None = None,
    data_root: str | None = None,
    snapshot_date: str | None = None,
    api: str | None = None,
) -> pd.DataFrame:
    """Project series into a tidy frame and write a pinned, citable bundle.

    Parameters
    ----------
    series_ids : explicit catalog ids to bundle.
    source     : alternatively, bundle every catalog series of one source that the
                 client can resolve today.
    out        : output zip path. The datapackage.json + native parquet are also
                 written (unzipped) next to it in <out_stem>/ for direct use.
    api        : if set (e.g. ``'http://127.0.0.1:8787'`` or ``$ECONDL_API``),
                 resolve every series via the ``/v1`` Worker ``.csv`` endpoint
                 instead of the local store. The datapackage.json lockfile is the
                 SAME shape either way (snapshot_date pin, per-resource sha256,
                 provenance). Without ``api`` the local path is byte-identical to
                 before. Honest status is preserved: a 404/501/502 from the
                 server raises (and is surfaced loudly), never silently dropped.

    Returns the tidy DataFrame [series_id, source, obs_date, value].
    """
    snapshot_date = snapshot_date or _today()
    if not series_ids and not source:
        raise ValueError("bundle() needs series_ids=[...] or source=...")

    if source and not series_ids:
        if api:
            from ._http import HttpClient
            man = HttpClient(api).bundle_manifest(source=source, snapshot=snapshot_date)
            series_ids = sorted(
                sid for r in man.get("resources", []) for sid in r.get("econdl:series_ids", []))
            if not series_ids:
                raise ValueError(f"no catalog series found for source {source!r} (via api)")
        else:
            conn = _catalog.connect(db)
            try:
                rows = conn.execute(
                    "SELECT series_id FROM series WHERE source_id = ? ORDER BY series_id", (source,)
                ).fetchall()
            finally:
                conn.close()
            series_ids = [r["series_id"] for r in rows]
            if not series_ids:
                raise ValueError(f"no catalog series found for source {source!r}")

    # 0) PROXY / PULL-THROUGH GATE (ARCHITECTURE §9 [w5]). Partition out every
    #    series whose source license is NOT reservable -- we may not re-host it.
    #    These are removed from `series_ids` BEFORE any resolve/read/copy: a
    #    proxied series' parquet is never opened and our DOI/citation is never
    #    stamped on it. It becomes a manifest-only resource pointing UPSTREAM.
    #    The gate is on the source's license row (reservable flag), exactly the
    #    brief's contract. An UNDETERMINABLE gate (None -- e.g. an HTTP-only caller
    #    with no local catalog) is surfaced loudly and the series is left on the
    #    normal path rather than silently mis-classified.
    proxied_ids: list[str] = []
    undeterminable: list[str] = []
    kept_ids: list[str] = []
    for sid in series_ids:
        state = _proxy.reservable_state(sid, db=db)
        if state is False:
            proxied_ids.append(sid)
        elif state is None:
            undeterminable.append(sid)
            kept_ids.append(sid)
        else:
            kept_ids.append(sid)
    if undeterminable:
        warnings.warn(
            f"econdl.bundle(): could NOT determine the redistributability "
            f"(license reservable flag) of {len(undeterminable)} series "
            f"(no catalog/license on record): "
            f"{', '.join(undeterminable[:6])}"
            f"{' ...' if len(undeterminable) > 6 else ''}. They are processed on "
            f"the normal redistributable path; verify each source's license before "
            f"relying on the re-hosted copy.",
            stacklevel=2,
        )
    series_ids = kept_ids

    # 1) resolve + read every requested series, grouped by source. The two
    #    transports populate the SAME structures (by_source native tables,
    #    per-source key_col/tidy_ok, tidy_parts) so step 2/3 are shared verbatim.
    by_source: dict[str, list[pa.Table]] = {}
    source_meta: dict[str, dict] = {}        # per-source key_col + tidy_ok (consistent within a source)
    tidy_parts: list[pd.DataFrame] = []
    resolved_ids: list[str] = []
    native_only_ids: list[str] = []          # relational/wide sources excluded from the tidy frame

    # Only the REDISTRIBUTABLE ids reach the transports; proxied ids were removed
    # above and are NEVER resolved/read/copied. (When every requested series is
    # proxied, `series_ids` is empty and we skip acquisition entirely -- a
    # manifest-only bundle of upstream pointers is a legitimate, honest artifact.)
    if series_ids:
        if api:
            _acquire_via_http(
                api, series_ids, by_source, source_meta, tidy_parts, resolved_ids)
        else:
            for sid in series_ids:
                res = _resolve.resolve(sid, root=data_root)          # raises loudly if unsupported
                table = _resolve.read_native(res)                    # raises loudly if empty
                by_source.setdefault(res.source, []).append(table)
                source_meta[res.source] = {"key_col": res.key_col, "tidy_ok": res.tidy_ok}
                if res.tidy_ok:
                    tidy_parts.append(_resolve.native_to_tidy(res, table))
                else:
                    native_only_ids.append(sid)  # shipped native-verbatim, not in the tidy frame
                resolved_ids.append(sid)
    elif not proxied_ids:
        # nothing kept AND nothing proxied -> there is genuinely nothing to bundle.
        raise ValueError("bundle(): no resolvable or proxied series after the gate.")

    # The tidy frame covers only tidy-able sources; relational/wide sources are
    # present in the bundle as native parquet and listed below (never silently dropped).
    tidy = (
        pd.concat(tidy_parts, ignore_index=True)
        .sort_values(["source", "series_id", "obs_date"])
        .reset_index(drop=True)
        if tidy_parts else pd.DataFrame(columns=_resolve._CANON)
    )
    if native_only_ids:
        tidy.attrs["econdl_native_only"] = sorted(native_only_ids)
        warnings.warn(
            f"econdl.bundle(): {len(native_only_ids)} series from relational/wide sources "
            f"are shipped as NATIVE parquet in the bundle but are NOT in the returned tidy "
            f"frame (no canonical value column): {', '.join(sorted(native_only_ids)[:6])}"
            f"{' ...' if len(native_only_ids) > 6 else ''}. Read them from the bundle's "
            f"data/<source>.parquet directly.",
            stacklevel=2,
        )

    # Proxied (non-redistributable) series are OMITTED from the tidy frame -- we
    # re-serve none of their bytes. They are surfaced both on the frame's attrs and
    # via a LOUD warning, and live in the datapackage as upstream-pointing manifest
    # entries (built below). Never silently dropped.
    if proxied_ids:
        tidy.attrs["econdl_proxied"] = sorted(proxied_ids)
        warnings.warn(
            f"econdl.bundle(): {len(proxied_ids)} series come from NON-redistributable "
            f"sources (license reservable=0) and are NOT re-hosted, NOT in the returned "
            f"tidy frame, and carry NO Elkassabgi DOI. They are listed in the bundle's "
            f"datapackage.json under 'econdl:proxied' with full upstream provenance + a "
            f"link to obtain them from the original provider under its terms: "
            f"{', '.join(sorted(proxied_ids)[:6])}"
            f"{' ...' if len(proxied_ids) > 6 else ''}.",
            stacklevel=2,
        )

    # 2) write the bundle dir (one native parquet resource per source) + datapackage.json
    out = os.path.abspath(out)
    stem = os.path.splitext(out)[0]
    bundle_dir = stem
    data_dir = os.path.join(bundle_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    resources = []
    members_by_source: dict[str, list[str]] = {}
    for sid in resolved_ids:
        members_by_source.setdefault(_catalog.source_of(sid), []).append(sid)

    # Provenance is read from the SAME registry the rows came from: catalog.db for
    # the local path, the server's /v1/bundle manifest for the api path (so a
    # caller with no local catalog.db still gets honest license/attribution).
    http_provenance: dict[str, dict] = {}
    if api:
        from ._http import HttpClient
        try:
            man = HttpClient(api).bundle_manifest(
                ids=sorted(resolved_ids), snapshot=snapshot_date)
            for r in man.get("resources", []):
                if r.get("name") and r.get("econdl:provenance"):
                    prov = dict(r["econdl:provenance"])
                    prov["citation"] = _build_citation(
                        {"name": prov.get("name"), "homepage": prov.get("homepage"),
                         "source_id": prov.get("source_id")}, snapshot_date)
                    http_provenance[r["name"]] = prov
        except ResolveError:
            # manifest unreachable: provenance is omitted (None) rather than read
            # from a local catalog.db an HTTP-only caller may not have. None is
            # handled gracefully by _distinct_licenses / _write_readme (both `or {}`).
            http_provenance = {}

    for src, tables in sorted(by_source.items()):
        # Concatenate this source's native projections (unify schema across files).
        combined = pa.concat_tables(tables, promote_options="default")
        rel = os.path.join("data", _resource_name(src))
        abspath = os.path.join(bundle_dir, rel)
        pq.write_table(combined, abspath)
        sha = _sha256(abspath)
        nbytes = os.path.getsize(abspath)
        resources.append({
            "name": src,
            "path": rel.replace(os.sep, "/"),
            "format": "parquet",
            "mediatype": "application/vnd.apache.parquet",
            "bytes": nbytes,
            "hash": f"sha256:{sha}",
            "schema": {"fields": [{"name": f.name, "type": str(f.type)} for f in combined.schema]},
            "econdl:series_ids": sorted(members_by_source.get(src, [])),
            "econdl:key_col": source_meta[src]["key_col"],   # used by pull() to reconstruct tidy
            "econdl:tidy": source_meta[src]["tidy_ok"],       # False => native-only, skip in tidy frame
            "econdl:provenance": (http_provenance.get(src)
                                  if api else _source_block(src, snapshot_date, db)),
        })

    # Proxy / pull-through resources: one per non-redistributable series. These
    # carry NO local path, NO hash, NO bytes -- only an UPSTREAM url + provenance +
    # the "not redistributed" note. They are appended AFTER the redistributable
    # resources so the byte-identical local path above is untouched when nothing is
    # proxied. Provenance comes from the registry (catalog.db); an HTTP-only caller
    # with no local catalog still gets the upstream pointer (path) and the note.
    for sid in sorted(proxied_ids):
        resources.append(_proxy.proxy_resource(sid, snapshot_date, db=db))

    datapackage = {
        "name": os.path.basename(stem).lower().replace(" ", "-") or "econdl-bundle",
        "profile": _PROFILE,
        "econdl:schema_version": SCHEMA_VERSION,
        "econdl:snapshot_date": snapshot_date,        # <- the pin (reproduce by default)
        "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "econdl:client": "econdl-python",
        "econdl:series_requested": sorted(resolved_ids + proxied_ids),
        "econdl:native_only_sources": sorted({_catalog.source_of(s) for s in native_only_ids}),
        # Non-redistributable series, surfaced as upstream-pointing manifest entries
        # (never re-hosted, never carrying our DOI). pull() handles these as
        # unfetched-by-design.
        "econdl:proxied": sorted(proxied_ids),
        "licenses": _distinct_licenses(resources),
        "resources": resources,
    }

    dp_path = os.path.join(bundle_dir, "datapackage.json")
    with open(dp_path, "w", encoding="utf-8") as f:
        json.dump(datapackage, f, indent=2, ensure_ascii=False)

    _write_readme(bundle_dir, datapackage)

    # 3) zip it (datapackage.json + data/*.parquet + README.md)
    if os.path.exists(out):
        os.remove(out)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for rootdir, _dirs, files in os.walk(bundle_dir):
            for name in files:
                full = os.path.join(rootdir, name)
                zf.write(full, os.path.relpath(full, bundle_dir))

    return tidy


def _distinct_licenses(resources: list[dict]) -> list[dict]:
    seen: dict[str, dict] = {}
    for r in resources:
        prov = r.get("econdl:provenance") or {}
        lic = prov.get("license")
        if lic and lic.get("id") and lic["id"] not in seen:
            seen[lic["id"]] = {"name": lic["id"], "title": lic.get("name"), "path": lic.get("url")}
    return list(seen.values())


def _write_readme(bundle_dir: str, dp: dict) -> None:
    lines = [
        f"# {dp['name']}",
        "",
        f"Econ Data Library bundle. Snapshot pinned: **{dp['econdl:snapshot_date']}**.",
        "",
        "This is a re-runnable lockfile. Rebuild the exact data with:",
        "",
        "```python",
        "import econdl",
        "df = econdl.pull('datapackage.json')        # exact snapshot (verifies sha256)",
        "df = econdl.pull('datapackage.json', latest=True)  # opt in to refreshed data",
        "```",
        "",
        "## Sources, licenses & citations",
        "",
    ]
    for r in dp["resources"]:
        prov = r.get("econdl:provenance") or {}
        lic = (prov.get("license") or {}).get("id", "unknown")
        # Proxy / pull-through resources hold no bytes -- render them as upstream
        # pointers, NOT as a re-hosted file (no bytes/hash). Honest by construction.
        if r.get("econdl:proxy"):
            path = r.get("path")
            upstream = path[0] if isinstance(path, list) and path else path
            lines += [
                f"### {prov.get('upstream_provider') or prov.get('name') or r['name']}  "
                f"(`{r['name']}`)  — NOT redistributed",
                f"- License: {lic}",
                f"- Attribution: {prov.get('attribution') or '(none on record)'}",
                f"- Citation: {prov.get('citation')}",
                f"- Obtain from upstream: {upstream}",
                f"- Note: {r.get('econdl:note')}",
                "",
            ]
            continue
        lines += [
            f"### {prov.get('name') or r['name']}  (`{r['name']}`)",
            f"- License: {lic}",
            f"- Attribution: {prov.get('attribution') or '(none on record)'}",
            f"- Citation: {prov.get('citation')}",
            f"- Resource: `{r['path']}`  ({r['bytes']} bytes, {r['hash']})",
            "",
        ]
    if dp.get("econdl:proxied"):
        lines += [
            "## Proxied (non-redistributable) series",
            "",
            "These series come from sources whose license does not permit "
            "re-hosting. The Elkassabgi Data Library does NOT redistribute their "
            "values and stamps NO DOI on them. Obtain them directly from each "
            "provider under the original terms (see the upstream links above).",
            "",
        ]
    with open(os.path.join(bundle_dir, "README.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# --------------------------------------------------------------------------- #
# pull
# --------------------------------------------------------------------------- #

def _load_datapackage(dp: str) -> tuple[dict, str]:
    """Load datapackage.json from a path, a bundle dir, or a .zip. Returns (dp, base_dir)."""
    dp = os.path.abspath(dp)
    if os.path.isdir(dp):
        dp = os.path.join(dp, "datapackage.json")
    if dp.endswith(".zip") or (os.path.isfile(dp) and zipfile.is_zipfile(dp)):
        tmp = tempfile.mkdtemp(prefix="econdl_pull_")
        with zipfile.ZipFile(dp) as zf:
            zf.extractall(tmp)
        dp = os.path.join(tmp, "datapackage.json")
    if not os.path.isfile(dp):
        raise FileNotFoundError(f"datapackage.json not found at {dp!r}")
    with open(dp, encoding="utf-8") as f:
        return json.load(f), os.path.dirname(dp)


def _native_table_to_tidy(source: str, table: pa.Table, key_col: str | None = None) -> pd.DataFrame:
    """Re-normalise a copied native resource (possibly multi-series) to tidy form.

    Uses the key_col the bundle pinned (``econdl:key_col``) so the reproduced frame
    is a row-for-row identity with the originally-bundled frame; falls back to a
    sensible guess for older bundles written before key_col was recorded.
    """
    if not key_col:
        key_col = "series_key" if "series_key" in table.column_names else "series_id"
    return _resolve.native_table_to_tidy(source, key_col, table)


def pull(
    datapackage: str,
    *,
    latest: bool = False,
    db: str | None = None,
    data_root: str | None = None,
    api: str | None = None,
) -> pd.DataFrame:
    """Rebuild the bundle pinned in a datapackage.json lockfile.

    Default: reproduce the EXACT pinned snapshot from the copied resources and
    VERIFY each resource's sha256 (raises on mismatch). This path is
    transport-agnostic -- it reads the bundle's own copied parquet, so ``api`` is
    ignored when ``latest=False`` (an exact reproduction never re-fetches).
    latest=True: re-project the requested series from the live store -- or, when
    ``api`` is set, from the ``/v1`` Worker ``.csv`` endpoint. Either way it
    LOUDLY WARNS and never silently skips a series it cannot satisfy.

    PROXIED (non-redistributable) resources are handled the same in both modes:
    we hold none of their bytes by design, so they are surfaced as
    *unfetched-by-design* (loud warning + ``df.attrs['econdl_proxied']``), NEVER
    an error -- the researcher obtains them from the upstream provider under its
    terms (the upstream URL is in each proxy resource's ``path``).
    """
    dp, base = _load_datapackage(datapackage)
    snapshot = dp.get("econdl:snapshot_date", "?")
    proxied = sorted(dp.get("econdl:proxied", []))

    def _warn_proxied() -> None:
        if not proxied:
            return
        links = []
        for r in dp.get("resources", []):
            if r.get("econdl:proxy") and r.get("name") in proxied:
                p = r.get("path")
                links.append(f"  - {r['name']}: {p[0] if isinstance(p, list) and p else p}")
        warnings.warn(
            f"econdl.pull(): {len(proxied)} series in this bundle are "
            f"NON-redistributable (proxied) and were NOT fetched BY DESIGN -- the "
            f"library holds none of their bytes. Obtain each from its upstream "
            f"provider under the original terms:\n" + "\n".join(links),
            stacklevel=2,
        )

    if not latest:
        # ---- exact reproduction: read copied native resources, verify sha256 ----
        parts: list[pd.DataFrame] = []
        for r in dp["resources"]:
            # Proxy resources hold no local bytes -- skip file/sha256 logic entirely
            # and surface them below as unfetched-by-design (never an error).
            if r.get("econdl:proxy"):
                continue
            path = os.path.join(base, r["path"])
            if not os.path.exists(path):
                raise FileNotFoundError(
                    f"pinned resource {r['path']!r} missing from bundle -- cannot reproduce."
                )
            want = r.get("hash", "")
            if want.startswith("sha256:"):
                got = "sha256:" + _sha256(path)
                if got != want:
                    raise ValueError(
                        f"sha256 MISMATCH for {r['path']!r}: bundle pinned {want}, got {got}. "
                        "The data has been altered -- refusing to return a corrupted snapshot."
                    )
            # native-only (relational/wide) resources are sha256-verified above but
            # carry no canonical value column, so they are NOT folded into the tidy
            # frame (matching bundle()); read them from the resource parquet directly.
            if not r.get("econdl:tidy", True):
                continue
            table = pq.read_table(path)
            parts.append(_native_table_to_tidy(r["name"], table, r.get("econdl:key_col")))
        tidy = (pd.concat(parts, ignore_index=True) if parts
                else pd.DataFrame(columns=_resolve._CANON))
        tidy = tidy.sort_values(["source", "series_id", "obs_date"]).reset_index(drop=True)
        _warn_proxied()
        if proxied:
            tidy.attrs["econdl_proxied"] = proxied
        return tidy

    # ---- latest=True: re-project from the live store, warn on anything missing ----
    requested = list(dp.get("econdl:series_requested", []))
    if not requested:
        # fall back to the per-resource series_ids
        for r in dp["resources"]:
            if r.get("econdl:proxy"):
                continue  # proxied ids are never auto-refreshed
            requested.extend(r.get("econdl:series_ids", []))
    # Proxied series are NEVER auto-fetched on refresh either -- exclude them from
    # the live re-projection and surface them as unfetched-by-design (below). We
    # do not reach upstream on the researcher's behalf under a no-redistribute
    # license; we only point at it.
    proxied_set = set(proxied)
    requested = [sid for sid in requested if sid not in proxied_set]
    parts = []
    satisfied, missing = [], []
    native_only = []
    http_client = None
    if api:
        from ._http import HttpClient, HttpResolveError
        http_client = HttpClient(api)
    for sid in requested:
        try:
            if http_client is not None:
                # refresh over the /v1 .csv endpoint (relational/wide ids 501 there,
                # surfaced loudly via HttpResolveError -> missing, never dropped).
                parts.append(http_client.fetch_series_csv(sid))
            else:
                res = _resolve.resolve(sid, root=data_root)
                table = _resolve.read_native(res)
                if res.tidy_ok:
                    parts.append(_resolve.native_to_tidy(res, table))
                else:
                    native_only.append(sid)  # refreshed + verified, but not tidy-able
            satisfied.append(sid)
        except (ResolveError, FileNotFoundError) as e:
            missing.append((sid, str(e)))

    where = f"the API {api!r}" if api else "the live store"
    if missing:
        msg = "\n".join(f"  - {sid}: {err}" for sid, err in missing)
        warnings.warn(
            f"econdl.pull(latest=True): {len(missing)} of {len(requested)} pinned "
            f"series could NOT be refreshed from {where} and were NOT silently "
            f"dropped:\n{msg}\nPinned snapshot was {snapshot}. Use pull() without "
            f"latest=True to reproduce the exact pinned data instead.",
            stacklevel=2,
        )
    # A bundle of ONLY proxied series legitimately refreshes to an empty tidy
    # frame (there is nothing redistributable to re-project) -- that is NOT a
    # failure, so only raise when there were redistributable ids to satisfy.
    if not satisfied and requested:
        raise ResolveError(
            f"pull(latest=True): none of the pinned series could be refreshed from "
            f"{where}. Reproduce the snapshot with pull() (no latest=True)."
        )
    tidy = (pd.concat(parts, ignore_index=True) if parts
            else pd.DataFrame(columns=_resolve._CANON))
    tidy = tidy.sort_values(["source", "series_id", "obs_date"]).reset_index(drop=True)
    tidy.attrs["econdl_satisfied"] = satisfied
    tidy.attrs["econdl_missing"] = [m[0] for m in missing]
    tidy.attrs["econdl_native_only"] = native_only
    _warn_proxied()
    if proxied:
        tidy.attrs["econdl_proxied"] = proxied
    return tidy
