"""HTTP transport for the econdl client -- a thin layer over the `/v1` contract.

This lets ``bundle()`` / ``pull()`` target the public API URL (the Cloudflare
Worker in prod, or the local dev shim ``api/devserver.py``) instead of resolving
series over the local on-disk store. The lockfile semantics are IDENTICAL either
way (snapshot_date pin, per-resource sha256, provenance, loud-never-silent) --
only the *source of the rows* changes.

Design constraints (from the task brief + CONTRACT.md):
  * STDLIB ONLY -- urllib, no `requests` dependency.
  * Honest status passes through: a 404/501/502/400 from the server is raised as
    an ``HttpResolveError`` (a ``ResolveError`` subclass) carrying the server's
    machine reason, so bundle()/pull() warn/skip with the SAME loudness as local.
  * The contract surface this client speaks:
      GET /v1/series/{id}.csv
      GET /v1/series/{id}.metadata.json
      GET /v1/catalog?q=&source=&limit=&offset=
      GET /v1/last-updates
      GET /v1/bundle?ids=&source=&snapshot=
    where {id} is the EXACT catalog series_id, URL-encoded (it contains ':').
"""
from __future__ import annotations

import io
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import pandas as pd

from ._resolve import ResolveError


class HttpResolveError(ResolveError):
    """A `/v1` request failed (or the server returned an honest non-200).

    Subclasses ResolveError so the existing bundle()/pull() except-clauses catch
    it and surface it loudly -- an HTTP-sourced series we cannot satisfy is
    treated EXACTLY like a local-resolve miss (never silently dropped).
    """

    def __init__(self, message: str, *, status: int | None = None,
                 error: str | None = None, payload: dict | None = None):
        super().__init__(message)
        self.status = status
        self.error = error          # the server's machine reason, e.g. "not_migrated"
        self.payload = payload or {}


def default_api() -> str | None:
    """Base API URL from $ECONDL_API (e.g. https://api.econdl...), or None."""
    return os.environ.get("ECONDL_API") or None


class HttpClient:
    """Thin client over the `/v1` contract. One instance per base URL."""

    def __init__(self, base_url: str, *, timeout: float = 60.0):
        if not base_url:
            raise ValueError("HttpClient needs a base URL (e.g. http://127.0.0.1:8787)")
        # Normalise: strip a trailing slash and an optional trailing '/v1' so the
        # caller can pass either form; we always speak '/v1/...'.
        b = base_url.rstrip("/")
        if b.endswith("/v1"):
            b = b[: -len("/v1")]
        self.base = b
        self.timeout = timeout

    # ---- low-level GET -----------------------------------------------------
    def _url(self, path: str, params: dict[str, Any] | None = None) -> str:
        url = f"{self.base}{path}"
        if params:
            # Drop None; keep repeatable params (list values) intact.
            pairs: list[tuple[str, str]] = []
            for k, v in params.items():
                if v is None:
                    continue
                if isinstance(v, (list, tuple)):
                    pairs += [(k, str(x)) for x in v]
                else:
                    pairs.append((k, str(v)))
            if pairs:
                url += "?" + urllib.parse.urlencode(pairs)
        return url

    def _get(self, path: str, params: dict | None = None) -> tuple[int, bytes, str]:
        url = self._url(path, params)
        req = urllib.request.Request(url, method="GET",
                                     headers={"Accept": "application/json, text/csv"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read()
                ctype = resp.headers.get("Content-Type", "")
                return resp.status, body, ctype
        except urllib.error.HTTPError as e:
            body = e.read()
            ctype = e.headers.get("Content-Type", "") if e.headers else ""
            return e.code, body, ctype
        except urllib.error.URLError as e:
            raise HttpResolveError(
                f"econdl HTTP transport: cannot reach {url!r}: {e.reason}",
                status=None, error="unreachable") from e

    @staticmethod
    def _decode_error(status: int, body: bytes, what: str) -> HttpResolveError:
        """Turn an honest non-200 into a loud, machine-readable HttpResolveError."""
        payload: dict = {}
        try:
            payload = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            payload = {}
        err = payload.get("error") if isinstance(payload, dict) else None
        detail = payload.get("detail") if isinstance(payload, dict) else None
        msg = (f"{what}: server returned {status} {err or ''}".rstrip()
               + (f" -- {detail}" if detail else f" -- {body[:200]!r}"))
        return HttpResolveError(msg, status=status, error=err, payload=payload)

    # ---- /v1/series/{id}.csv ----------------------------------------------
    def fetch_series_csv(self, series_id: str, *, fmt: str = "full",
                         date_from: str | None = None,
                         date_to: str | None = None) -> pd.DataFrame:
        """Fetch one series as a tidy DataFrame [series_id, source, obs_date, value].

        Honest status is preserved: a 404/501/502/400 raises HttpResolveError
        (a ResolveError), so bundle()/pull() treat an HTTP miss exactly like a
        local-resolve miss. Returns a frame guaranteed to have >=1 row (the
        server only emits 200 with rows).
        """
        path = f"/v1/series/{urllib.parse.quote(series_id, safe='')}.csv"
        params = {"format": fmt, "from": date_from, "to": date_to}
        status, body, _ctype = self._get(path, params)
        if status != 200:
            raise self._decode_error(status, body, f"{series_id}")
        df = pd.read_csv(io.BytesIO(body))
        if df.empty:
            # The contract forbids an empty 200; if one slips through, refuse loudly.
            raise HttpResolveError(
                f"{series_id}: server returned 200 but zero rows -- contract violation "
                "(200 must carry >=1 row). Refusing to accept an empty series.",
                status=200, error="resolver_empty")
        # The canonical .csv is the long tidy shape `series_id,obs_date,value`. A
        # relational/wide source is served by the contract as a NATIVE-column CSV
        # (no canonical value column); that cannot be folded into the tidy frame, so
        # we refuse it LOUDLY rather than fabricate one -- the caller (bundle/pull)
        # surfaces it as a not-satisfiable series, never a silent mangle.
        if not {"series_id", "obs_date", "value"}.issubset(df.columns):
            raise HttpResolveError(
                f"{series_id}: served as a native/wide CSV (columns "
                f"{list(df.columns)}), which has no canonical value column and cannot "
                "be tidied. This source ships native-verbatim; fetch it from the bundle "
                "manifest's resource URL directly.",
                status=200, error="native_only")
        # Normalise to the canonical tidy shape the local path produces. The CSV
        # already carries series_id (the catalog id for single-curve series, or the
        # native per-curve key for a fanned-out indicator); add `source` from the
        # catalog id prefix so the frame matches _resolve.native_to_tidy output.
        source = series_id.split(":", 1)[0]
        out = pd.DataFrame({
            "series_id": df["series_id"].astype(str),
            "source": source,
            "obs_date": pd.to_datetime(df["obs_date"]).dt.date,
            "value": pd.to_numeric(df["value"], errors="coerce"),
        })
        return out.sort_values(["series_id", "obs_date"]).reset_index(drop=True)

    # ---- /v1/series/{id}.metadata.json ------------------------------------
    def fetch_metadata(self, series_id: str) -> dict:
        path = f"/v1/series/{urllib.parse.quote(series_id, safe='')}.metadata.json"
        status, body, _ = self._get(path)
        if status != 200:
            raise self._decode_error(status, body, f"{series_id} metadata")
        return json.loads(body.decode("utf-8"))

    # ---- /v1/catalog ------------------------------------------------------
    def search(self, q: str, *, source: str | None = None,
               limit: int = 50, offset: int = 0) -> list[dict]:
        status, body, _ = self._get(
            "/v1/catalog",
            {"q": q, "source": source, "limit": limit, "offset": offset})
        if status != 200:
            raise self._decode_error(status, body, "catalog search")
        return json.loads(body.decode("utf-8")).get("results", [])

    # ---- /v1/last-updates -------------------------------------------------
    def last_updates(self) -> dict:
        status, body, _ = self._get("/v1/last-updates")
        if status != 200:
            raise self._decode_error(status, body, "last-updates")
        return json.loads(body.decode("utf-8"))

    # ---- /v1/bundle (manifest) --------------------------------------------
    def bundle_manifest(self, ids: list[str] | None = None, *,
                        source: str | None = None,
                        snapshot: str | None = None) -> dict:
        """The server-side bundle manifest (provenance + per-series resource URLs).

        Used by bundle(api=...) to pull provenance straight from the server's
        registry, so the lockfile's per-source provenance is identical to what
        the local path reads from catalog.db.
        """
        params: dict[str, Any] = {}
        if ids:
            params["ids"] = ",".join(ids)
        if source:
            params["source"] = source
        if snapshot:
            params["snapshot"] = snapshot
        status, body, _ = self._get("/v1/bundle", params)
        if status != 200:
            raise self._decode_error(status, body, "bundle manifest")
        return json.loads(body.decode("utf-8"))
