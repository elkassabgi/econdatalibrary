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

import gzip
import zlib
import io
import re
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


def _read_body(resp, url: str) -> bytes:
    """Read the whole body; a short read against a declared content-length is a TRUNCATED
    transfer (R613: http.client.IncompleteRead is not a URLError and escaped every caller)."""
    import http.client
    import socket
    try:
        return resp.read()
    except http.client.IncompleteRead as e:
        raise HttpResolveError(
            f"econdl HTTP transport: the transfer from {url!r} was cut off after {len(e.partial):,} bytes "
            f"(the server declared more); retry",
            status=None, error="truncated") from e
    except (TimeoutError, socket.timeout, ConnectionResetError, ConnectionAbortedError,
            http.client.HTTPException) as e:
        # R614: a STALLED read (the common failure of a 427 MB passthrough on a campus network)
        # raises TimeoutError, a reset raises ConnectionResetError, a chunked body cut before
        # its terminator raises an HTTPException - all of them are the transfer ending early.
        # R615: socket.timeout only BECAME an alias of TimeoutError in 3.10, and this package
        # declares requires-python >=3.9 - on the declared floor the stalled read escaped raw.
        raise HttpResolveError(
            f"econdl HTTP transport: the transfer from {url!r} stalled or was reset before it completed "
            f"({type(e).__name__}); retry", status=None, error="truncated") from e


def _decode_body(body: bytes, hdrs: dict) -> bytes:
    """gzip-decode when the body IS gzip (content-encoding header, or the 1f 8b magic) - the
    unfiltered large-object shape is a gzip passthrough that urllib does not decode (R607)."""
    enc = (hdrs.get("content-encoding") or "").lower()
    if "gzip" in enc or (len(body) >= 2 and body[0] == 0x1F and body[1] == 0x8B):
        try:
            return gzip.decompress(body)
        except EOFError as e:   # a gzip stream cut before its trailer: the transfer was truncated (R613)
            raise HttpResolveError("econdl HTTP transport: the gzip stream ended before its trailer - the "
                                   "transfer was cut off; retry", status=None, error="truncated") from e
        except (gzip.BadGzipFile, zlib.error, OSError, ValueError) as e:
            raise HttpResolveError(f"econdl HTTP transport: the response claims gzip but does not decode: {e}",
                                   status=None, error="undecodable") from e
    return body


class EmptyBody(Exception):
    """No data rows at all (an empty or marker-only body): the contract forbids an empty 200."""


class TruncatedTransfer(Exception):
    """The body ended without the completeness line the contract requires when no
    content-length is declared: a server-side abort reaches the client as a clean end of body."""


class UnverifiableTransfer(TruncatedTransfer):
    """A passthrough (x-econdl-citation-omitted) arrived WITHOUT content-length: it carries no
    completeness line by design, so nothing can prove it whole (R614: a proxy that inflates the
    stored gzip produces exactly this shape, and a cut on a row boundary would pass silently).
    The reference client sends Accept-Encoding: gzip so the edge passes the stored bytes through
    with their length; refuse anything else."""


_COMPLETE_RE = re.compile(rb"^#\s*econdl-complete\s+rows=(\d+)\s*$")


def parse_series_csv(body: bytes, *, content_length: str | int | None,
                     citation_omitted: bool = False) -> pd.DataFrame:
    """Parse a /v1/series/{id}.csv body into a DataFrame.

    Every .csv carries a '#'-prefixed citation header (since 2026-07-09) and, when the server
    inflated a large object (no content-length), ends with `# econdl-complete rows=<N>`.
    R601: from 2026-07-09 to 2026-09-02 this client read the body with a bare pd.read_csv and
    every fetch raised ParserError ("Expected 1 fields in line 11, saw 3") - the citation
    header's own text told users to pass comment='#', the client did not. R607: the unfiltered
    large-object shape (gzip passthrough) was still unreadable, and pandas' comment='#' acts
    mid-line, so '#'-comment lines are stripped only where the LINE starts with '#'.

    Completeness: with no content-length the `# econdl-complete rows=N` line is REQUIRED and N
    must equal the rows parsed - EXCEPT for a passthrough response (x-econdl-citation-omitted),
    which carries no marker by design and, if the edge recoded it, no content-length either;
    its bytes are what the server stored. With a content-length the caller received exactly
    that many bytes (urllib raises on a short read), so the line is not required.
    """
    no_length = content_length is None or str(content_length).strip() == ""
    if no_length and citation_omitted:
        raise UnverifiableTransfer(
            "a large-object passthrough arrived without content-length (an intermediary inflated it): "
            "its completeness cannot be verified. Retry with a client that sends Accept-Encoding: gzip "
            "(this one does) or fetch a windowed request, which carries the completeness line.")
    if no_length and not citation_omitted:
        stripped = body.rstrip(b"\r\n")
        last = stripped[stripped.rfind(b"\n") + 1:] if b"\n" in stripped else stripped
        m = _COMPLETE_RE.match(last.strip())
        if not m:
            raise TruncatedTransfer(
                "the response declared no content-length and does not end with the "
                "'# econdl-complete rows=N' line the contract requires: the transfer was cut off "
                "(a corrupt object or a stalled read). Retry; if it persists, report the series id.")
        expected = int(m.group(1))
    else:
        expected = None
    kept = b"\n".join(ln for ln in body.split(b"\n") if not ln.lstrip(b" \t").startswith(b"#"))
    if not kept.strip():
        raise EmptyBody("the body holds no CSV rows (empty, or comment lines only)")
    try:
        df = pd.read_csv(io.BytesIO(kept))
    except pd.errors.EmptyDataError as e:
        raise EmptyBody("the body holds no CSV rows") from e
    if expected is not None and len(df) != expected:
        raise TruncatedTransfer(
            f"the completeness line says {expected:,} rows but {len(df):,} were parsed: "
            "the transfer was cut off or the body was altered in transit. Retry.")
    return df


class HttpClient:
    """Thin client over the `/v1` contract. One instance per base URL."""

    def __init__(self, base_url: str, *, timeout: float = 60.0,
                 api_key: str | None = None):
        if not base_url:
            raise ValueError("HttpClient needs a base URL (e.g. http://127.0.0.1:8787)")
        # Normalise: strip a trailing slash and an optional trailing '/v1' so the
        # caller can pass either form; we always speak '/v1/...'.
        b = base_url.rstrip("/")
        if b.endswith("/v1"):
            b = b[: -len("/v1")]
        self.base = b
        self.timeout = timeout
        # SHARED LOGIN: data downloads need the free Data Library family key —
        # ONE account for hfdatalibrary.com AND econdatalibrary.com (an existing
        # hfdatalibrary key works as-is). Resolution order: explicit arg, then
        # $ECONDL_API_KEY, then $HFDL_API_KEY (family key under its hf name).
        # Browsing endpoints (catalog/search/metadata) never require it.
        import os as _os
        self.api_key = (api_key or _os.environ.get("ECONDL_API_KEY")
                        or _os.environ.get("HFDL_API_KEY") or None)

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
        status, body, ctype, _headers = self._get_full(path, params)
        return status, body, ctype

    def _get_full(self, path: str, params: dict | None = None) -> tuple[int, bytes, str, dict]:
        """Like _get, plus the response headers (lower-cased keys): the CSV path needs to know
        whether a content-length was declared (R601)."""
        url = self._url(path, params)
        headers = {"Accept": "application/json, text/csv",
                   # R607: say we take gzip, so the edge passes a stored-gzip object through as
                   # is (exact content-length) instead of recoding it; decoded below by what the
                   # body IS, never by what a header promises.
                   "Accept-Encoding": "gzip",
                   # urllib's default UA can trip edge bot-checks; identify honestly.
                   "User-Agent": "econdl-python-client"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        req = urllib.request.Request(url, method="GET", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = _read_body(resp, url)
                ctype = resp.headers.get("Content-Type", "")
                hdrs = {k.lower(): v for k, v in resp.headers.items()}
                return resp.status, _decode_body(body, hdrs), ctype, hdrs
        except urllib.error.HTTPError as e:
            body = _read_body(e, url)   # R614: an error body can be cut short too
            ctype = e.headers.get("Content-Type", "") if e.headers else ""
            hdrs = {k.lower(): v for k, v in e.headers.items()} if e.headers else {}
            return e.code, _decode_body(body, hdrs), ctype, hdrs
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
        status, body, _ctype, hdrs = self._get_full(path, params)
        if status != 200:
            raise self._decode_error(status, body, f"{series_id}")
        try:
            df = parse_series_csv(body, content_length=hdrs.get("content-length"),
                                  citation_omitted="x-econdl-citation-omitted" in hdrs)
        except UnverifiableTransfer as e:
            raise HttpResolveError(f"{series_id}: {e}", status=200, error="unverifiable") from e
        except TruncatedTransfer as e:
            raise HttpResolveError(f"{series_id}: {e}", status=200, error="truncated") from e
        except EmptyBody as e:
            raise HttpResolveError(
                f"{series_id}: server returned 200 but zero rows ({e}) -- contract violation. "
                "Refusing to accept an empty series.", status=200, error="resolver_empty") from e
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
