#!/usr/bin/env python3
"""Local dev shim for the `/v1` public API contract (api/CONTRACT.md).

This is the **executable reference** for the contract: a stdlib-only HTTP server
(no Flask/FastAPI) that serves the same response shapes the Cloudflare Worker
serves in prod, but backed by the on-disk store + SQLite that exist today. It
imports ``econdl._resolve`` and ``econdl._catalog`` DIRECTLY, so the API and the
client resolve a series through the EXACT same code -- they cannot disagree
(CONTRACT.md "Backend binding" table).

Honest-status is non-negotiable (CONTRACT.md):
  * 200 only with >=1 row (CSV) / a found id (metadata)
  * 404 unknown catalog id
  * 501 {"error":"not_migrated",...} for a source with no resolver
  * 502 {"error":"resolver_empty",...} for resolve-but-zero-rows
  * 400 {"error":"unsupported_filter",...} for a filter the store can't honor
We never emit an empty 200 and never launder "unknown" into "fresh".

Run:
    python api/devserver.py --port 8787
    python api/devserver.py --port 0          # ephemeral port (printed as PORT=<n>)

Backends are overridable for tests:
    --data-root <dir>   (else $ECONDL_DATA or the bundled clean_full)
    --catalog <db>      (else $ECONDL_CATALOG or the bundled catalog.db)
    --state <db>        (else <repo>/data/_aqueduct/state.db)
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sqlite3
import sys
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, unquote, urlsplit

import pandas as pd

# Make the sibling client package importable so the shim resolves a series
# through the SAME code as the client (CONTRACT.md backend-binding invariant).
_THIS = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_THIS)
sys.path.insert(0, os.path.join(_REPO, "clients", "python"))

from econdl import _catalog, _resolve  # noqa: E402
from econdl._resolve import ResolveError  # noqa: E402

# --------------------------------------------------------------------------- #
# config (resolved once at startup, stashed on the server)
# --------------------------------------------------------------------------- #

# KEEP IN SYNC with api/worker/src/catalog.ts::COVERAGE and api/CONTRACT.md. The dev shim
# must answer exactly what the worker answers, or a caller who develops against it and then
# points at production sees a field change meaning underneath them.
# Carries no COUNT (the old "33 sources" rotted), and keeps the caveat: grain is NOT uniform.
# Some sources are catalogued per table or flow — ons_uk holds 42 catalogue rows for 3,897,884
# series, istat 14,267 flows, insee_melodi 139; the sets are registered in econdl/_resolve.py
# (_FLOW_GRAIN, _DOT_TABLE_GRAIN) and each source's page states its own grain. Saying
# "series-level for every served source" would delete the very warning this field exists to
# give. Do not infer grain from row count: statcan (20), oecd (28) and bls (9) are small
# PER-SERIES catalogues, and wid's 2,465,197 rows are series too.
_CATALOG_COVERAGE = (
    "mixed grain: some sources are catalogued per series, others per table or flow — "
    "absence from this catalogue does not mean a series is unavailable"
)

# next_update_expected = last_success + cadence interval (CONTRACT.md v1.1
# canonical /v1/last-updates pin). Any cadence NOT in this map -- irregular,
# static, unknown, or no last_success_utc -- yields next_update_expected:null
# (never a fabricated date). Mirrors api/worker/src/util.ts::CADENCE_DAYS exactly.
_CADENCE_DAYS = {"daily": 1, "weekly": 7, "monthly": 30, "quarterly": 91, "annual": 365}

# Languages with OFFICIAL, source-provided translations loaded into the catalog
# (stored at series.metadata.titles[<lang>]). 'en' is the native `title` column.
# Translations are NEVER machine-generated -- only labels the producer itself
# publishes (e.g. World Bank /v2/<lang>/, ILO/IMF SDMX xml:lang). This tuple is
# the set of langs actually present in the store and MUST stay in sync with
# api/worker/src/util.ts::SUPPORTED_LANGS. A ?lang= outside this set is a 400 --
# we never silently hand back English for a language we don't really have.
_LANGS = ("en", "ar", "es", "fr", "ru", "zh")


def _state_db_default() -> str:
    return os.path.join(_REPO, "data", "_aqueduct", "state.db")


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _csv_url(series_id: str) -> str:
    return f"/v1/series/{quote(series_id, safe='')}.csv"


def _localized_title(extra: dict, en_title, lang: str | None):
    """Return the source-official title for `lang` (extra['titles'][lang]) when it
    exists, else the English native title (graceful fallback). lang None/'en'
    returns the English title unchanged. Mirrors api/worker/src/util.ts::
    localizedTitle so the shim and Worker localize identically."""
    if not lang or lang == "en":
        return en_title
    titles = extra.get("titles") if isinstance(extra, dict) else None
    if isinstance(titles, dict) and titles.get(lang):
        return titles[lang]
    return en_title


def _next_update_expected(last_success_utc: str | None, cadence: str | None) -> str | None:
    """last_success + {daily:1d,weekly:7d,monthly:30d,quarterly:91d}; null if unknown."""
    if not last_success_utc or not cadence:
        return None
    days = _CADENCE_DAYS.get((cadence or "").strip().lower())
    if days is None:
        return None
    raw = last_success_utc.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return (dt + timedelta(days=days)).date().isoformat()


# --------------------------------------------------------------------------- #
# the request handler
# --------------------------------------------------------------------------- #

class Handler(BaseHTTPRequestHandler):
    server_version = "econdl-devshim/1.0"
    protocol_version = "HTTP/1.1"

    # ---- response writers (always Content-Length; HTTP/1.1 keep-alive) ----
    def _write(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, code: int, obj: dict | list) -> None:
        body = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self._write(code, body, "application/json; charset=utf-8")

    def _error(self, code: int, error: str, **extra) -> None:
        payload = {"error": error}
        payload.update(extra)
        self._json(code, payload)

    def _req_lang(self, qs: dict):
        """Resolve the optional ?lang= param. Returns (lang, ok). For an absent or
        'en' lang returns ('en', True) and the caller serves the native title with
        NO extra keys (byte-identical to the pre-i18n contract). For a language we
        have no official translations for, emits a 400 unsupported_language and
        returns (None, False) -- never a silent English fallback."""
        raw = (qs.get("lang", [""])[0] or "").strip().lower()
        if not raw or raw == "en":
            return "en", True
        if raw not in _LANGS:
            self._error(400, "unsupported_language", parameter="lang", value=raw,
                        detail=f"no official translations loaded for {raw!r}",
                        supported=list(_LANGS))
            return None, False
        return raw, True

    # ---- access the server's resolved config ----
    @property
    def cfg(self):
        return self.server.cfg  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # quieter than the default stderr spam
        if os.environ.get("ECONDL_DEVSHIM_VERBOSE"):
            super().log_message(fmt, *args)

    # ---- routing ----
    def do_HEAD(self):
        self.do_GET()

    # Any write/other verb on this read-only contract is 405 (a clean Method Not
    # Allowed), NOT the stdlib's default 501 -- in our contract 501 means
    # "not_migrated", so we must never let a wrong HTTP verb masquerade as it.
    def _method_not_allowed(self):
        self.send_response(405)
        body = json.dumps({"error": "method_not_allowed", "method": self.command,
                           "allow": "GET, HEAD"},
                          separators=(",", ":")).encode("utf-8")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Allow", "GET, HEAD")
        self.end_headers()
        self.wfile.write(body)

    do_POST = do_PUT = do_DELETE = do_PATCH = do_OPTIONS = _method_not_allowed

    def do_GET(self):
        parts = urlsplit(self.path)
        path = parts.path
        qs = parse_qs(parts.query, keep_blank_values=True)
        try:
            if path == "/v1/series" or path == "/v1/series/":
                return self._error(404, "not_found", detail="missing series id")
            if path.startswith("/v1/series/"):
                tail = path[len("/v1/series/"):]
                if tail.endswith(".metadata.json"):
                    return self.h_metadata(unquote(tail[:-len(".metadata.json")]), qs)
                if tail.endswith(".csv"):
                    return self.h_csv(unquote(tail[:-len(".csv")]), qs)
                return self._error(404, "not_found", detail="unknown series resource suffix")
            if path == "/v1/catalog":
                return self.h_catalog(qs)
            if path == "/v1/sources":
                return self.h_sources()
            if path == "/v1/last-updates":
                return self.h_last_updates()
            if path == "/v1/bundle":
                return self.h_bundle(qs)
            if path in ("/", "/v1", "/v1/", "/health", "/v1/health"):
                return self._json(200, {"ok": True, "service": "econdl-devshim",
                                        "contract": "/v1", "generated": _now_iso()})
            return self._error(404, "not_found", detail=f"no route for {path!r}")
        except BrokenPipeError:
            pass
        except Exception as e:  # never leak a stack as a 200; honest 500
            self._error(500, "internal_error", detail=f"{type(e).__name__}: {e}")

    # ---- GET /v1/series/{id}.csv ----
    def h_csv(self, series_id: str, qs: dict):
        # 1) unknown id -> 404 (catalog is the authority on existence).
        row = _catalog.get_series(series_id, db=self.cfg.catalog)
        if row is None:
            return self._error(404, "not_found", series_id=series_id,
                               detail="series id not in catalog")

        # 2) honor only filters the store can support; anything else -> 400
        #    (never a silently-unfiltered 200). from/to are server-side predicates;
        #    geo/freq/unit are not yet columns in the tidy projection -> 400.
        fmt = (qs.get("format", ["full"])[0] or "full").lower()
        if fmt not in ("full", "filtered"):
            return self._error(400, "unsupported_filter", parameter="format", value=fmt,
                               detail="format must be 'full' or 'filtered'")
        for unsupported in ("geo", "freq", "unit"):
            if qs.get(unsupported, [""])[0] != "":
                return self._error(
                    400, "unsupported_filter", parameter=unsupported,
                    value=qs[unsupported][0],
                    detail=(f"{unsupported}= filtering is not a column in the tidy "
                            "projection yet; refusing a silently-unfiltered response"))
        date_from = qs.get("from", [""])[0] or None
        date_to = qs.get("to", [""])[0] or None
        for label, v in (("from", date_from), ("to", date_to)):
            if v is not None:
                try:
                    datetime.strptime(v, "%Y-%m-%d")
                except ValueError:
                    return self._error(400, "unsupported_filter", parameter=label, value=v,
                                       detail="expected YYYY-MM-DD")

        # 3) resolve through the SAME code the client uses.
        #    Status-code contract (CONTRACT.md v1.1 "Status codes (reconciled)"):
        #      * source has NO resolver        -> 501 not_migrated
        #      * supported source but the at-rest object/FILE is absent
        #                                       -> 502 data_unavailable
        #      * present file/window, ZERO rows -> 502 resolver_empty
        try:
            res = _resolve.resolve(series_id, root=self.cfg.data_root)
        except ResolveError as e:
            src = _catalog.source_of(series_id)
            if src not in _resolve.supported_sources():
                return self._error(501, "not_migrated", source=src, series_id=series_id,
                                   detail=str(e))
            # The source IS migrated (has a resolver) but its at-rest object/file
            # isn't present yet. The id IS in the catalog (checked above), so 404
            # would be a lie -> 502 data_unavailable (object not published yet),
            # matching the Worker's "R2 object missing" branch.
            return self._error(502, "data_unavailable", source=src, series_id=series_id,
                               detail=str(e))

        try:
            table = _resolve.read_native(res)
        except ResolveError as e:
            # resolves to a file but zero rows -> 502 resolver_empty.
            return self._error(502, "resolver_empty", series_id=series_id, detail=str(e))

        if not res.tidy_ok:
            # relational/wide source: NO canonical value column, so a long
            # series_id,obs_date,value CSV would be a lie. CONTRACT.md: emit a 200
            # NATIVE CSV of the native columns verbatim (the honest projection),
            # optionally date-windowed when an obs_date column exists.
            return self._native_csv(series_id, res, table, qs)

        tidy = _resolve.native_to_tidy(res, table)
        # Canonical long CSV: series_id,obs_date,value. The series_id column is
        # EXACTLY what econdl._resolve.native_to_tidy emits -- the NATIVE key (or,
        # for filename-identity sources, the stamped catalog id) -- NOT the
        # requested catalog id for 1:1 series (CONTRACT.md v1.1 ".csv identity
        # column" pin). This is what makes a LOCAL bundle and an HTTP bundle of the
        # same ids row-for-row identical INCLUDING the key column. The Worker
        # streams the same bytes because its R2 object is derived by this same
        # resolver, so the two cannot disagree. No single-series catalog-id
        # special-casing -- that would break HTTP/local identity.
        out = pd.DataFrame({
            "series_id": tidy["series_id"].astype(str),
            "obs_date": pd.to_datetime(tidy["obs_date"]).dt.strftime("%Y-%m-%d"),
            "value": tidy["value"],
        })
        if date_from is not None:
            out = out[out["obs_date"] >= date_from]
        if date_to is not None:
            out = out[out["obs_date"] <= date_to]
        # sort by (series_id, obs_date) so a fanned-out indicator groups by curve;
        # a single-curve series is just chronological (CONTRACT.md "sorted by obs_date").
        out = out.sort_values(["series_id", "obs_date"]).reset_index(drop=True)

        if len(out) == 0:
            # window (or filter) selected nothing -- honest, never an empty 200.
            return self._error(502, "resolver_empty", series_id=series_id,
                               detail="no observations in the requested window")

        buf = io.StringIO()
        out.to_csv(buf, index=False, lineterminator="\n")
        self._write(200, buf.getvalue().encode("utf-8"), "text/csv; charset=utf-8")

    def _native_csv(self, series_id: str, res, table, qs: dict):
        """200 native-column CSV for a relational/wide (tidy_ok=False) source.

        Ships the native projection VERBATIM -- no fabricated canonical value column.
        Honors from/to when the native table carries an obs_date column; refuses an
        empty result honestly (502) rather than emitting a header-only 200.
        """
        df = table.to_pandas()
        date_from = qs.get("from", [""])[0] or None
        date_to = qs.get("to", [""])[0] or None
        if (date_from or date_to) and "obs_date" in df.columns:
            obs = df["obs_date"].astype(str)
            if date_from:
                df = df[obs >= date_from]
            if date_to:
                df = df[df["obs_date"].astype(str) <= date_to]
        if len(df) == 0:
            return self._error(502, "resolver_empty", series_id=series_id,
                               detail="no native rows in the requested window")
        if "obs_date" in df.columns:
            df = df.sort_values("obs_date").reset_index(drop=True)
        buf = io.StringIO()
        df.to_csv(buf, index=False, lineterminator="\n")
        self._write(200, buf.getvalue().encode("utf-8"), "text/csv; charset=utf-8")

    # ---- GET /v1/series/{id}.metadata.json ----
    def h_metadata(self, series_id: str, qs: dict | None = None):
        lang, ok = self._req_lang(qs or {})
        if not ok:
            return
        row = _catalog.get_series(series_id, db=self.cfg.catalog)
        if row is None:
            return self._error(404, "not_found", series_id=series_id,
                               detail="series id not in catalog")
        # Mirror the Worker (metadata.ts): source = provider segment of the id;
        # license = series license, falling back to the source's license.
        source = _catalog.source_of(series_id)
        src = _catalog.get_source(source, db=self.cfg.catalog) or {}
        lic_id = row.get("license_id") or src.get("license_id")
        lic = _catalog.get_license(lic_id, db=self.cfg.catalog) if lic_id else None

        # last_updated: prefer the series row's own column; fall back to the
        # source's unit_state('_all').last_success_utc (CONTRACT.md v1.1 pin).
        # Never fabricate -> null if neither exists.
        last_updated = row.get("last_updated")
        if not last_updated:
            last_updated = self._source_all_last_success(source)

        # CANONICAL v1.1 shape (byte-for-byte with api/worker/src/metadata.ts):
        # includes `category`; obs_count is OMITTED (it requires reading the
        # parquet rows -- the .csv body is the source of truth, never faked).
        meta: dict = {
            "series_id": series_id,
            "source": source,
            "title": row.get("title"),
            "frequency": row.get("frequency"),
            "unit": row.get("unit"),
            "geography": row.get("geography"),
            "category": row.get("category"),
            "start_date": row.get("start_date"),
            "end_date": row.get("end_date"),
            # license block present only when the license ROW exists (mirrors the
            # Worker's licenseBlock(licRow)).
            "license": ({
                "id": lic.get("license_id"),
                "name": lic.get("name"),
                "url": lic.get("url"),
                "reservable": bool(lic.get("reservable")),
                "commercial_ok": bool(lic.get("commercial_ok")),
                "attribution_required": bool(lic.get("attribution_required")),
                "no_modify": bool(lic.get("no_modify")),
            } if lic and lic.get("license_id") else None),
            "attribution": src.get("attribution"),
            "homepage": src.get("homepage"),
            "terms_url": src.get("terms_url"),
            "last_updated": last_updated,  # series row, else unit_state _all; never faked
            "csv_url": _csv_url(series_id),
        }

        # Human-context fields. Prefer the series-tier metadata pass (Task #5)
        # keys verbatim when present; ELSE fall back to the catalog's existing
        # `description` / `citation` metadata keys (CONTRACT.md v1.1 pin). Fields
        # absent under both are OMITTED, never faked.
        extra: dict = {}
        if row.get("metadata"):
            try:
                extra = json.loads(row["metadata"]) or {}
            except (ValueError, TypeError):
                extra = {}
        if extra.get("description_key"):
            meta["description_key"] = extra["description_key"]
        elif extra.get("description"):
            meta["description"] = extra["description"]
        if extra.get("description_processing"):
            meta["description_processing"] = extra["description_processing"]
        if extra.get("citation_short") or extra.get("citation_long"):
            if extra.get("citation_short"):
                meta["citation_short"] = extra["citation_short"]
            if extra.get("citation_long"):
                meta["citation_long"] = extra["citation_long"]
        elif extra.get("citation"):
            # producer citation FIRST (CONTRACT.md), library compiled-by appended.
            meta["citation_short"] = extra["citation"]
            meta["citation_long"] = f"{extra['citation']}. Compiled by Elkassabgi Data Library."
        # i18n: serve the source-official localized title when ?lang= was asked
        # for AND we have it; English otherwise. `title_en` is preserved so the
        # native label is never lost. For lang=en this block is a no-op, keeping
        # the response byte-identical to the pre-i18n contract.
        if lang != "en":
            localized = _localized_title(extra, meta["title"], lang)
            if localized != meta["title"]:
                meta["title_en"] = meta["title"]
                meta["title"] = localized
            meta["lang"] = lang
        self._json(200, meta)

    def _source_all_last_success(self, source_id: str) -> str | None:
        """unit_state('_all').last_success_utc for a source, else the first unit's
        (mirrors api/worker/src/metadata.ts). null when there is no unit_state row
        at all -- a metadata last_updated fallback, never a fabricated date."""
        if not os.path.exists(self.cfg.state):
            return None
        conn = sqlite3.connect(f"file:{self.cfg.state}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT unit_id, last_success_utc FROM unit_state "
                "WHERE source_id = ? ORDER BY unit_id", (source_id,)).fetchall()
        finally:
            conn.close()
        if not rows:
            return None
        for r in rows:
            if r["unit_id"] == "_all":
                return r["last_success_utc"]
        return rows[0]["last_success_utc"]

    # ---- GET /v1/catalog ----
    def h_catalog(self, qs: dict):
        lang, ok = self._req_lang(qs)
        if not ok:
            return
        q = qs.get("q", [""])[0]
        source = qs.get("source", [""])[0] or None
        try:
            limit = min(int(qs.get("limit", ["50"])[0]), 500)
        except ValueError:
            limit = 50
        try:
            offset = max(int(qs.get("offset", ["0"])[0]), 0)
        except ValueError:
            offset = 0

        conn = _catalog.connect(self.cfg.catalog)
        try:
            # Search via an FTS5 JOIN (NOT a `series_id IN (...)` list): an IN-list
            # of every matched id blows SQLite's bound-variable limit on broad
            # terms (e.g. q=GDP). The JOIN is O(matches) inside the engine, returns
            # the identical set/order, and the same SQL runs on D1 in the Worker.
            where, args = [], []
            join = ""
            if q:
                try:  # probe FTS availability once; LIKE-fallback mirrors core/catalog.py
                    conn.execute(
                        "SELECT 1 FROM series_fts WHERE series_fts MATCH ? LIMIT 1",
                        (q,)).fetchone()
                    join = " JOIN series_fts ON series_fts.series_id = s.series_id"
                    where.append("series_fts MATCH ?")
                    args.append(q)
                except sqlite3.OperationalError:
                    where.append("(s.title LIKE ? OR s.series_id LIKE ?)")
                    args += [f"%{q}%", f"%{q}%"]
            if source:
                where.append("s.source_id = ?")
                args.append(source)
            wsql = (" WHERE " + " AND ".join(where)) if where else ""
            total = conn.execute(
                f"SELECT COUNT(*) FROM series s{join}{wsql}", args).fetchone()[0]
            # Pull metadata only when localizing -- keeps the lang=en page byte-
            # identical to the pre-i18n contract (no metadata column, no lang key).
            cols = ("s.series_id, s.source_id AS source, s.title, s.frequency, s.unit, "
                    "s.geography, s.license_id, s.start_date, s.end_date")
            if lang != "en":
                cols += ", s.metadata AS metadata"
            rows = conn.execute(
                f"SELECT {cols} FROM series s{join}{wsql} "
                "ORDER BY s.series_id LIMIT ? OFFSET ?", (*args, limit, offset)).fetchall()
        finally:
            conn.close()
        results = []
        for r in rows:
            d = dict(r)
            if lang != "en":
                md = d.pop("metadata", None)
                extra = {}
                if md:
                    try:
                        extra = json.loads(md) or {}
                    except (ValueError, TypeError):
                        extra = {}
                d["title"] = _localized_title(extra, d.get("title"), lang)
            results.append(d)
        out = {
            "total": total, "limit": limit, "offset": offset,
            "catalog_coverage": _CATALOG_COVERAGE,
            "results": results,
        }
        if lang != "en":
            out["lang"] = lang
        self._json(200, out)

    # ---- GET /v1/sources ----
    def h_sources(self):
        # CANONICAL v1.1 NESTED shape (CONTRACT.md "Canonical response shapes"),
        # byte-for-byte with api/worker/src/sources.ts:
        #   { source, name, homepage, license:{...}|null, freshness:{status,
        #     last_updated, cadence}|null }
        # attribution/terms_url are NOT in this pin (they live in metadata.json +
        # bundle provenance). freshness is null when the source has no source_state
        # row at all (honest absence, never a fabricated {null,null,null}).
        conn = _catalog.connect(self.cfg.catalog)
        try:
            srcs = [dict(r) for r in conn.execute(
                "SELECT source_id, name, homepage, license_id "
                "FROM source ORDER BY source_id").fetchall()]
        finally:
            conn.close()
        sstate = self._source_state_map()
        out = []
        for s in srcs:
            lic_id = s.get("license_id")
            lic = (_catalog.get_license(lic_id, db=self.cfg.catalog)
                   if lic_id else None)
            st = sstate.get(s["source_id"])
            out.append({
                "source": s["source_id"],
                "name": s.get("name"),
                "homepage": s.get("homepage"),
                "license": ({
                    "id": lic.get("license_id"),
                    "name": lic.get("name"),
                    "url": lic.get("url"),
                    "reservable": bool(lic.get("reservable")),
                    "commercial_ok": bool(lic.get("commercial_ok")),
                    "attribution_required": bool(lic.get("attribution_required")),
                    "no_modify": bool(lic.get("no_modify")),
                } if lic and lic.get("license_id") else None),
                "freshness": ({
                    "status": st.get("status"),
                    "last_updated": st.get("last_success_utc"),
                    "cadence": st.get("cadence"),
                } if st else None),
            })
        self._json(200, {"total": len(out), "sources": out})

    def _source_state_map(self) -> dict[str, dict]:
        if not os.path.exists(self.cfg.state):
            return {}
        conn = sqlite3.connect(f"file:{self.cfg.state}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            return {r["source_id"]: dict(r) for r in conn.execute(
                "SELECT source_id, cadence, status, last_success_utc FROM source_state"
            ).fetchall()}
        finally:
            conn.close()

    # ---- GET /v1/last-updates ([w8]) ----
    def h_last_updates(self):
        if not os.path.exists(self.cfg.state):
            return self._json(200, {"generated": _now_iso(), "datasets": []})
        conn = sqlite3.connect(f"file:{self.cfg.state}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            # Canonical SQL straight from CONTRACT.md (runs verbatim on D1).
            rows = conn.execute(
                "SELECT u.source_id, u.unit_id, u.status, u.last_success_utc, "
                "       u.upstream_vintage, u.last_obs_date, u.obs_count, s.cadence "
                "FROM unit_state u LEFT JOIN source_state s ON s.source_id = u.source_id "
                "ORDER BY u.source_id, u.unit_id").fetchall()
        finally:
            conn.close()
        datasets = []
        for r in rows:
            last = r["last_success_utc"]
            datasets.append({
                "source": r["source_id"],
                "unit": r["unit_id"],
                # A unit with no last_success reports its current status + null date,
                # never a fabricated freshness date (CONTRACT.md).
                "status": r["status"],
                "last_updated": last,
                "source_date_accessed": last,
                "source_version": r["upstream_vintage"],
                "last_obs_date": r["last_obs_date"],
                "next_update_expected": _next_update_expected(last, r["cadence"]),
                "obs_count": r["obs_count"],
            })
        self._json(200, {"generated": _now_iso(), "datasets": datasets})

    # ---- GET /v1/bundle -> a MANIFEST the client fans out ([w10]) ----
    def h_bundle(self, qs: dict):
        ids: list[str] = []
        for raw in qs.get("ids", []):
            ids += [x for x in raw.split(",") if x]
        source = qs.get("source", [""])[0] or None
        snapshot = qs.get("snapshot", [""])[0] or datetime.now(timezone.utc).date().isoformat()
        if source and not ids:
            conn = _catalog.connect(self.cfg.catalog)
            try:
                ids = [r["series_id"] for r in conn.execute(
                    "SELECT series_id FROM series WHERE source_id = ? ORDER BY series_id",
                    (source,)).fetchall()]
            finally:
                conn.close()
        if not ids:
            return self._error(400, "unsupported_filter", parameter="ids",
                               detail="bundle needs ids= (repeatable/comma) or source=")

        by_source: dict[str, list[str]] = {}
        unresolved: list[dict] = []
        for sid in ids:
            row = _catalog.get_series(sid, db=self.cfg.catalog)
            if row is None:
                unresolved.append({"id": sid, "reason": "not_found: unknown series id"})
                continue
            src = row["source_id"]
            if src not in _resolve.supported_sources():
                unresolved.append({"id": sid, "reason": f"not_migrated: source '{src}' has no resolver yet"})
                continue
            by_source.setdefault(src, []).append(sid)

        # One resource per source. CANONICAL v1.1 Frictionless shape (CONTRACT.md
        # "/v1/bundle manifest" pin), byte-for-byte with api/worker/src/bundle.ts:
        #   resource = {name, profile, format, mediatype, path:[stable URLs],
        #               econdl:series_ids, econdl:provenance(incl citation)}
        # `path` lists the per-series CSV URLs the client fans out to (it assembles
        # the zip locally; the server streams nothing -- [w10]).
        resources = []
        for src, sids in sorted(by_source.items()):
            member_ids = sorted(sids)
            resources.append({
                "name": src,
                "profile": "tabular-data-resource",
                "format": "csv",
                "mediatype": "text/csv",
                "path": [_csv_url(s) for s in member_ids],
                "econdl:series_ids": member_ids,
                "econdl:provenance": self._provenance_block(src, snapshot),
            })

        total_urls = sum(len(r["path"]) for r in resources)

        # Distinct license blocks across resources (Frictionless licenses[]),
        # de-duplicated by id. Mirrors econdl._bundle._distinct_licenses and the
        # Worker: {name: <license id>, title: <license name>, path: <url>}.
        seen_lic: set[str] = set()
        licenses = []
        for r in resources:
            lic = r["econdl:provenance"].get("license")
            if lic and lic.get("id") and lic["id"] not in seen_lic:
                seen_lic.add(lic["id"])
                licenses.append({"name": lic["id"], "title": lic.get("name"),
                                 "path": lic.get("url")})

        # CANONICAL v1.1 top-level key ORDER (CONTRACT.md "Canonical response
        # shapes"), byte-for-byte with api/worker/src/bundle.ts. No `created`
        # field (it is not in the pin; the reproducibility anchor is snapshot_date).
        self._json(200, {
            "name": "econdl-bundle",
            "profile": "tabular-data-package",
            "econdl:schema_version": "1.0",
            "econdl:client": "econdl-worker-manifest",
            "econdl:snapshot_date": snapshot,
            "econdl:series_requested": sorted(ids),
            "econdl:resource_url_count": total_urls,
            "econdl:fanout_note": (
                "Client fetches each resource path URL and assembles the zip "
                "locally. The Worker streams no zip and makes no R2 fan-out "
                "(50-subrequest cap respected)."),
            "licenses": licenses,
            "resources": resources,
            "econdl:unresolved": unresolved,
        })

    def _provenance_block(self, source: str, snapshot: str) -> dict:
        # byte-for-byte with api/worker/src/bundle.ts::provenance: the citation
        # uses the source NAME (falling back to the source id) and the snapshot's
        # year, with the homepage appended when present.
        src = _catalog.get_source(source, db=self.cfg.catalog) or {}
        lic_id = src.get("license_id")
        lic = _catalog.get_license(lic_id, db=self.cfg.catalog) if lic_id else None
        name = src.get("name") or source
        year = snapshot[:4]
        citation = f"{name} ({year}). Accessed via Econ Data Library, snapshot {snapshot}."
        if src.get("homepage"):
            citation += f" {src['homepage']}"
        return {
            "source_id": source,
            "name": src.get("name"),
            "homepage": src.get("homepage"),
            "attribution": src.get("attribution"),
            "terms_url": src.get("terms_url"),
            # license block present only when the license ROW exists (mirrors the
            # Worker's licenseBlock(lic): null when the row is absent, even if the
            # source carries a license_id pointer that doesn't resolve).
            "license": ({
                "id": lic.get("license_id"), "name": lic.get("name"), "url": lic.get("url"),
                "reservable": bool(lic.get("reservable")),
                "commercial_ok": bool(lic.get("commercial_ok")),
                "attribution_required": bool(lic.get("attribution_required")),
                "no_modify": bool(lic.get("no_modify")),
            } if lic and lic.get("license_id") else None),
            "citation": citation,
        }


# --------------------------------------------------------------------------- #
# server bootstrap
# --------------------------------------------------------------------------- #

class _Cfg:
    def __init__(self, data_root, catalog, state):
        self.data_root = data_root
        self.catalog = catalog
        self.state = state


def make_server(host: str, port: int, cfg: _Cfg) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.cfg = cfg  # type: ignore[attr-defined]
    httpd.daemon_threads = True
    return httpd


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="econdl /v1 dev shim")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8787,
                    help="TCP port (0 = ephemeral, printed as PORT=<n>)")
    ap.add_argument("--data-root", default=None)
    ap.add_argument("--catalog", default=None)
    ap.add_argument("--state", default=None)
    args = ap.parse_args(argv)

    cfg = _Cfg(
        data_root=args.data_root or _resolve.default_data_root(),
        catalog=args.catalog or _catalog.default_db(),
        state=args.state or _state_db_default(),
    )
    httpd = make_server(args.host, args.port, cfg)
    bound_port = httpd.server_address[1]
    # Machine-parseable line a launcher can grep for (esp. with --port 0).
    print(f"PORT={bound_port}", flush=True)
    print(f"econdl dev shim on http://{args.host}:{bound_port}/v1  "
          f"(data_root={cfg.data_root!r})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
