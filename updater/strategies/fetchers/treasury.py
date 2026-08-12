"""S2 fetcher — U.S. Treasury FiscalData. Public domain, no key.

Layout (set by jobs/ingest_treasury.py): ONE parquet per ENDPOINT ("cube") under
clean_full/treasury/<slug>__<endpoint_leaf>.parquet. Each file holds the endpoint's
full relational table — columns:
  series_key : the endpoint path (constant within a file, e.g. "v2/accounting/od/debt_to_penny")
  obs_date   : date32, parsed from the endpoint's primary date field (record_date / ...)
  <data cols>: every original source column, preserved verbatim as STRINGS
              (Treasury returns decimal strings; keeping them avoids lossy float()
               of multi-trillion-dollar amounts).

There are typically MANY rows per (series_key, obs_date) — a date carries many
category/line rows — so (series_key, obs_date) is NOT a unique key. We therefore
dedup on a STABLE IDENTITY KEY = (series_key, obs_date, <all classification /
dimension columns>) and EXCLUDE the mutable value/measure columns (amounts, counts,
rates, balances, ...). That way a re-fetched row with a REVISED value supersedes the
prior vintage (merge keeps the last row per key) instead of being appended as a
conflicting duplicate — which is exactly what full-row dedup did wrong. The identity
key was verified to be collision-free across all 181 on-disk cubes, so no genuine
distinct row is ever collapsed.

Four endpoints have NO date field on disk (obs_date all-null): their identity is the
dimension columns alone (e.g. account_nbr; redemp_period+issue_name+issue_year+...;
series_cd+issue_year+...). These are re-fetched in FULL every run; the identity key
keeps that lossless while letting revisions overwrite.

Incremental: for each date-bearing endpoint we read the existing parquet's max(obs_date)
and ask the FiscalData REST API for the boundary window via its native filter
  filter=<date_field>:gte:<YYYY-MM-DD>
(re-fetching the last stored date so a same-date revision is seen), sorted by that
field, paged to the last page. The per-endpoint date field is detected from the
existing file's actual columns. New source columns that appear in fetched rows are
preserved (the column set is the UNION of on-disk columns and fetched-row keys).

This fetcher OWNS writing all of treasury's parquet files and returns ONE aggregate
Result built from a Tally (honest status: any transient sub-failure -> 'partial';
a 200-but-0-rows-from-a-real-body structural break -> DefinitiveError; otherwise
ok/no_change). It reuses the ingester's catalog (data/_treasury_catalog_final.json)
to map filename -> endpoint; the on-disk parquet schema is the source of truth for
columns and date field.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import random
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/json"}
API = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
PAGE_SIZE = 10000          # API hard cap
MAX_ATTEMPTS = 6
TIMEOUT = 90
PAGE_CEILING = 5000        # absolute page bound so a malformed total-pages can't loop forever
# Same precedence the ingester used to pick a primary date field.
DATE_CANDS = ("record_date", "reporting_date", "effective_date", "date",
              "auction_date", "issue_date", "index_date")

# Mutable value/measure column suffixes — these change on revision, so they must NOT
# be part of the dedup identity (else a revised value forms a new key and duplicates).
# Conservative on purpose: anything not clearly a measure stays in the identity key,
# so the only failure mode is "revision duplicates" (harmless, == old behavior), never
# "two distinct rows collapse" (which would be data loss). Verified collision-free
# across all 181 cubes.
VALUE_SUFFIXES = ("_amt", "_bal", "_cnt", "_rate", "_pct", "_price",
                  "_yield", "_per1000", "_par")
VALUE_PREFIXES = ("redemp_value_", "int_earned_")


# --------------------------------------------------------------------------- #
# helpers (mirror jobs/ingest_treasury.py so storage stays identical)
# --------------------------------------------------------------------------- #
def _leaf(endpoint: str) -> str:
    return endpoint.rstrip("/").split("/")[-1]


def _parse_date(s):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y/%m/%d", "%m/%d/%Y", "%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def _pick_date_field(cols):
    """Choose the endpoint's primary date field from its real (non-meta) columns."""
    real = [c for c in cols if c not in ("series_key", "obs_date")]
    for c in DATE_CANDS:
        if c in real:
            return c
    for c in real:
        if c.endswith("_date"):
            return c
    return None


def _is_value_col(c: str) -> bool:
    cl = c.lower()
    if any(cl.startswith(p) for p in VALUE_PREFIXES):
        return True
    return any(cl.endswith(s) for s in VALUE_SUFFIXES)


def _identity_keys(cols):
    """Stable dedup key: series_key, obs_date, then every non-value (dimension) column.
    Excludes mutable measure columns so a revised value overwrites the prior vintage."""
    dims = [c for c in cols
            if c not in ("series_key", "obs_date") and not _is_value_col(c)]
    return ["series_key", "obs_date"] + dims


def _load_catalog(out_dir):
    # blob-routed + co-located under the source dir: under AQUEDUCT_BACKEND=r2 the catalog is
    # an R2 object (clean_full/treasury/_treasury_catalog_final.json) — a raw local open() of
    # the old root-data/ path sees nothing on a CI runner and aborts every run (ledger R36,
    # the same two-part bug scb had). Local mode falls back to the co-located file.
    cat_file = os.path.join(out_dir, "_treasury_catalog_final.json")
    cat_raw = blob.read_bytes(cat_file)
    if cat_raw is None:
        raise DefinitiveError(f"treasury catalog missing: {cat_file}")
    try:
        cat = json.loads(cat_raw.decode("utf-8"))
    except ValueError as e:
        raise DefinitiveError(f"treasury catalog unreadable: {e}")
    # filename -> endpoint (matches ingester's <slug>__<leaf>.parquet)
    fmap = {}
    for ep, meta in cat.items():
        fn = f"{meta['slug']}__{_leaf(ep)}.parquet"
        fmap[fn] = ep
    return cat, fmap


def _session():
    s = requests.Session()
    # The API drops keep-alive sockets under load (RemoteDisconnected); force a
    # fresh connection per request to avoid stale-socket errors.
    s.headers.update(dict(UA, Connection="close"))
    return s


def _request(sess, url, params):
    """GET with retry/backoff. Transient (timeout/5xx/429/drop/bad-json) -> TransientError
    after exhausting attempts; hard 4xx -> DefinitiveError immediately; 400 no_data -> empty."""
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, params=params, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"treasury GET {url}: {last}")
            time.sleep(min(1.5 * (attempt + 1), 20) + random.uniform(0, 1.0))
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if attempt == MAX_ATTEMPTS - 1:
                    raise TransientError(f"treasury GET {url}: {last}")
                time.sleep(min(1.5 * (attempt + 1), 20) + random.uniform(0, 1.0))
                continue
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"treasury GET {url}: {last}")
            time.sleep(min(1.5 * (attempt + 1), 20) + random.uniform(0, 1.0))
            continue
        # 400 with no_data is FiscalData's "filter matched nothing" — treat as empty.
        if r.status_code == 400:
            try:
                body = r.json()
            except ValueError:
                body = {}
            if str(body.get("error", "")).lower().startswith("no data"):
                return {"data": [], "meta": {"total-pages": 1, "total-count": 0}}
            raise DefinitiveError(f"treasury GET {url}: HTTP 400 {str(body)[:120]}")
        # other hard 4xx
        raise DefinitiveError(f"treasury GET {url}: HTTP {r.status_code}")
    raise TransientError(f"treasury GET {url}: {last}")


def _fetch_rows(sess, endpoint, date_field, since_date):
    """Page all rows for an endpoint, optionally filtered to date_field:gte:since_date.

    Returns (rows, structural_zero) where structural_zero is True only when the FIRST
    page is a 200 with a non-trivial envelope (meta present / non-empty data contract)
    yet yields 0 rows on a FULL (unfiltered) fetch — i.e. a structural break, not a
    legitimately-quiet incremental tail. since_date is a date or None (full fetch).

    Pagination hardening (defense-in-depth so a missing/wrong total-pages can't stop a
    still-full page or loop forever):
      - stop on a SHORT page (len(rows) < PAGE_SIZE);
      - only trust a positive-int total-pages, else fall back to short-page stop;
      - cap absolute page count at PAGE_CEILING.
    """
    base = {"page[size]": str(PAGE_SIZE)}
    if date_field:
        base["sort"] = date_field
        if since_date is not None:
            base["filter"] = f"{date_field}:gte:{since_date.isoformat()}"
    out = []
    page = 1
    total_pages = None
    first_meta = {}
    while True:
        params = dict(base, **{"page[number]": str(page)})
        payload = _request(sess, f"{API}/{endpoint}", params)
        rows = payload.get("data", []) or []
        out.extend(rows)
        if total_pages is None:
            first_meta = payload.get("meta", {}) or {}
            tp = first_meta.get("total-pages", 1)
            total_pages = tp if isinstance(tp, int) and tp > 0 else None
        nrows = len(rows)
        # short page => no more full pages regardless of (possibly wrong) total-pages
        if nrows < PAGE_SIZE:
            break
        if total_pages is not None and page >= total_pages:
            break
        if page >= PAGE_CEILING:
            break
        page += 1
        time.sleep(0.12)  # polite between pages

    # Structural signal: an UNFILTERED (full) fetch that came back 200 with a real
    # envelope (meta block present) but 0 rows. An incremental :gte: tail returning 0
    # is legitimately "nothing new", NOT structural.
    structural_zero = (since_date is None and not out and bool(first_meta))
    return out, structural_zero


def _build_table(endpoint, date_field, out_cols, rows):
    """Assemble a table: series_key, obs_date, then each data column as string.
    out_cols is the UNION of the file's existing data columns and any NEW keys present
    in the fetched rows, so a newly-added source column is preserved (not dropped)."""
    n = len(rows)
    arrays = {
        "series_key": pa.array([endpoint] * n, type=pa.string()),
        "obs_date": pa.array(
            [_parse_date(r.get(date_field)) if date_field else None for r in rows],
            type=pa.date32()),
    }
    for c in out_cols:
        arrays[c] = pa.array(
            [(None if r.get(c) is None else str(r.get(c))) for r in rows],
            type=pa.string())
    return pa.table({k: arrays[k] for k in (["series_key", "obs_date"] + out_cols)})


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(unit.source_id)

    cat, fmap = _load_catalog(out_dir)
    # blob-routed enumeration: the endpoint-file set must be visible under
    # AQUEDUCT_BACKEND=r2 (the local store dir is absent on a CI runner).
    pfiles = blob.list_parquets(out_dir)
    if not pfiles:
        raise DefinitiveError(f"no treasury parquet files under {out_dir}")

    sess = _session()
    tally = Tally()
    total = 0
    maxd = None
    cursors: dict[str, str] = {}   # endpoint -> max obs_date written (per-file freshness)

    for fn in pfiles:
        path = os.path.join(out_dir, fn)
        endpoint = fmap.get(fn)
        before = blob.row_count(path)
        if endpoint is None:
            # Unknown file not in catalog: leave it untouched, keep its rows in total.
            total += before
            continue

        # Learn the file's exact column layout + per-endpoint last obs_date.
        schema = blob.read_schema(path)
        all_cols = list(schema.names)
        data_cols = [c for c in all_cols if c not in ("series_key", "obs_date")]
        date_field = _pick_date_field(all_cols)

        # A MISSING DATE FIELD IS NOT, BY ITSELF, A REASON TO SKIP — corrected 2026-08-03, same
        # day as the over-broad version that said it was.
        #
        # That version skipped every endpoint whose _pick_date_field returned None, on the theory
        # that obs_date=None puts all rows on one identity (endpoint, None) and dedup keeps ONE.
        # The theory was wrong: _identity_keys already appends every NON-VALUE column to the dedup
        # key, so a dateless endpoint is still keyed by its dimensions. Measured on the store, all
        # three dateless endpoints hold exactly their upstream row count —
        #     redemption_tables  125,728 = 125,728   dims redemp_period, issue_name, issue_year,
        #                                            issue_months, src_line_nbr
        #     sb_value            35,936 =  35,936   dims issue_year, redemp_period, series_cd, ...
        #     fbp_dpai_account…      185 =     185   dims account_desc, account_nbr
        # Nothing had collapsed, and skipping them stopped refreshing 161,664 rows that were
        # updating correctly. A fix aimed at a mechanism I had not verified broke two working
        # endpoints to protect them from a collapse that was not happening.
        #
        # WHAT IS STILL TRUE: treasury reports `refusing shrink 185->1` for fbp on every run, and
        # merge's never-shrink guard is what keeps those 185 rows. Since account_nbr IS in the
        # dedup key, "obs_date is null" does not explain it and the real cause is NOT yet known —
        # so nothing here pretends to fix it. The guard already prevents the loss; it costs a
        # `partial` status, which is the correct report for a fetch that cannot be applied.
        # Tracked in #87 with the measurements, to be diagnosed rather than guessed at.
        if not date_field:
            print(f"[{unit.source_id}] {os.path.basename(path)}: no date field — fetched as a "
                  f"dateless table, keyed by its dimension columns", flush=True)

        since_date = None
        if date_field and "obs_date" in all_cols:
            od = blob.read_table(path, columns=["obs_date"]).column("obs_date")
            mx = pc.max(od).as_py() if od.length() else None
            since_date = mx  # re-fetch boundary (:gte:) so same-date revisions are seen

        try:
            rows, structural_zero = _fetch_rows(sess, endpoint, date_field, since_date)
        except TransientError:
            # Leave this endpoint's existing data untouched; record & keep going so one
            # flaky endpoint can't strand the other 180. -> run becomes 'partial'.
            tally.transient_unit()
            total += before
            if before and (date_field and "obs_date" in all_cols):
                # preserve the known frontier for this endpoint in the cursor map
                fr = _existing_max(path)
                if fr:
                    cursors[endpoint] = fr
            time.sleep(0.2)
            continue

        if not rows:
            if structural_zero and before > 0:
                # 200 + real envelope but 0 rows on a FULL fetch of a previously
                # populated cube -> schema/structural break, not a quiet period.
                tally.structural_unit()
            else:
                # incremental tail with nothing newer, or genuinely-empty endpoint
                tally.empty_unit()
                if before and date_field and "obs_date" in all_cols:
                    fr = _existing_max(path)
                    if fr:
                        cursors[endpoint] = fr
            total += before
            time.sleep(0.05)
            continue

        # Preserve any NEW source column that appeared in the fetched rows.
        extra = sorted({k for r in rows for k in r}
                       - set(data_cols) - {"series_key", "obs_date"})
        out_cols = data_cols + extra

        new_tbl = _build_table(endpoint, date_field, out_cols, rows)
        # Dedup on the STABLE IDENTITY (dimensions only) so a revised value overwrites
        # the prior vintage. Identity columns come from the UNION layout.
        dedup_keys = tuple(_identity_keys(["series_key", "obs_date"] + out_cols))
        n, md = merge.merge_and_write(path, new_tbl, mode="merge",
                                      dedup_keys=dedup_keys)
        total += n
        tally.added_unit(max(0, n - before))
        if md:
            cursors[endpoint] = md
            if maxd is None or md > maxd:
                maxd = md
        time.sleep(0.1)  # polite between endpoints

    # Disable the blunt "large all-empty window => structural" floor for treasury: a
    # perfectly healthy steady-state run has ALL ~181 cubes return "nothing newer than
    # the cursor" (each cube updates only a few times a week), which is legitimate
    # no_change — NOT a break. Real structural breaks are caught precisely per-endpoint
    # via tally.structural_unit() (a 200 with a real envelope but 0 rows on a FULL
    # fetch). So raise the floor above the cube count so it never false-positives.
    return finalize(tally, total, maxd, source="treasury", series_cursors=cursors,
                    empty_window_floor=len(pfiles) + 1)


def _existing_max(path) -> str | None:
    """Max obs_date already on disk for an endpoint (for the cursor map on no-write paths)."""
    try:
        od = blob.read_table(path, columns=["obs_date"]).column("obs_date")
        mx = pc.max(od).as_py() if od.length() else None
        return mx.isoformat() if mx is not None else None
    except Exception:
        return None
