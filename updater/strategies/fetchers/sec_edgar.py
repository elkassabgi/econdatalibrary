"""S4 fetcher — SEC EDGAR structured-data sets (giant_changed_units).

WHAT THIS COVERS
================
SEC EDGAR publishes several *relational* structured-data products as quarterly
(or rolling-window) bulk ZIPs on its data-sets pages. Two of them are already
materialized on disk as multi-table parquet libraries this fetcher OWNS and keeps
fresh incrementally:

  edgar_13f      Form 13F institutional-holdings data sets
                 clean_full/edgar_13f/<TABLE>/period=<datasetKey>/<TABLE>.parquet
                 7 tables: SUBMISSION, COVERPAGE, OTHERMANAGER, OTHERMANAGER2,
                 INFOTABLE (holdings), SIGNATURE, SUMMARYPAGE  (join: ACCESSION_NUMBER)

  edgar_insider  Forms 3/4/5 insider-transaction data sets
                 clean_full/edgar_insider/<table>/year=<YYYY>/<table>_<YYYYqQ>.parquet
                 8 tables: submission, reportingowner, nonderiv_trans,
                 nonderiv_holding, deriv_trans, deriv_holding, footnotes,
                 owner_signature  (join: ACCESSION_NUMBER)

A third product, edgar_pointers (clean_full/edgar_pointers/cik_shard=NNN/part.parquet:
cik,ticker,form,filing_date,accession,primary_doc_url), is NOT a bulk-zip change
feed — it is built by a per-CIK crawl of data.sec.gov submissions and has no
single catalogue page to diff. It is intentionally OUT of scope here and noted in
the report (it needs its own daily-index delta fetcher, not this one). This module
NEVER touches edgar_pointers.

WHY giant_changed_units (S4), not extend_by_date / bulk_snapshot
================================================================
The natural incremental unit is a *new published dataset key* (a new quarter or
rolling 3-month window), NOT a row-level date tail: the SEC bulk-zip endpoints
expose NO server-side date filter, so a per-row delta (S2) is impossible. But it
is also NOT one monolithic bulk file (S5) — it is a *directory of per-period
artifacts* (one zip per quarter, fanning out to many per-table parquet files),
exactly the giant change-feed shape: download the data-sets PAGE (the catalogue),
diff the published keys against what is already on disk, and fetch ONLY the keys
that are NEW (or whose last run was partial/failed). Per-period partitioning makes
a new quarter a pure ADDITION — old partitions are never rewritten. This is the
S4 contract; the engine lives here per fetchers/_giant.py's design note, because
EDGAR's payload is relational tables (no series_key/obs_date), not the long-format
(series_key,obs_date,value) the generic _giant.run_giant driver assumes.

DUPLICATION GUARD — KEY FORMAT MATCHES EXISTING ON-DISK FORMAT
=============================================================
The on-disk EDGAR parquet has NO `series_key`/`obs_date` columns; row identity is
RELATIONAL. We therefore do NOT invent a series_key (that would not match the
existing format and would not merge). Instead each table is merged into its own
per-period file with the EXACT existing columns and a TABLE-SPECIFIC dedup key —
the SEC surrogate key (`*_SK`) plus ACCESSION_NUMBER where one exists, else the
filing's natural composite. Because new quarters land in their own partition file,
re-running is idempotent (dedup is a no-op) and a re-published quarter REVISES in
place (new rows win) and never-shrinks (merge.merge_and_write guards it). The
dataset key is carried in the partition PATH and in the provenance columns
(DATASET_PERIOD/period for 13f), never minted into a row key — so it cannot
duplicate the file on republish.

HONEST STATUS (the dominant correctness rule)
=============================================
Each (product, dataset-key) pair is one sub-unit. A 429 / 5xx / timeout / network
drop / truncated-or-bad ZIP -> TransientError -> the key stays unselected-as-done
(its vintage is NOT advanced) and the SOURCE result is `partial`; the orchestrator
does NOT stamp last_success and the key re-runs next tick. A 200 page that yields
ZERO catalogue keys is a structural break (TransientError -> retry, never laundered
into no_change). Empty individual tables inside a real quarter are legitimate (SEC
emits empty OTHERMANAGER2 etc.) and are written as 0-row partitions, not failures.
Existing data is only ever EXTENDED via merge.merge_and_write (atomic, dedup,
never-shrink), so a bad fetch can never shrink or duplicate good data.
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import time
import zipfile

import pandas as pd
import pyarrow as pa

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import CURSOR_CAP, Tally, cursors_from_table, finalize, merge_cursor_map

SOURCE = "sec_edgar"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept-Encoding": "gzip, deflate"}

# Per-product configuration. Each product is its own multi-table parquet library
# whose freshness is driven by a published-keys change feed.
#
#   page_url   : the SEC data-sets landing page (the catalogue we scrape).
#   zip_re     : regex capturing every published <key> from that page.
#   zip_url    : how to turn a <key> into the bulk-zip URL.
#   out_dir    : clean_full subdir this product owns.
#   tables     : {on_disk_table_name: TSV_member_basename}
#   layout     : "period_dir"  -> <TABLE>/period=<key>/<TABLE>.parquet     (13f)
#                "year_file"    -> <table>/year=<YYYY>/<table>_<key>.parquet (insider)
#   dedup      : {table: (col, ...)}  table-specific identity for never-shrink merge.
#   provenance : extra constant columns to stamp on each row (13f legacy schema).
PRODUCTS = {
    "edgar_13f": {
        "page_url": "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets",
        "zip_re": r"/form-13f-data-sets/([^/\"]+)_form13f\.zip",
        "zip_url": "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/{key}_form13f.zip",
        "out_dir": "edgar_13f",
        "tables": {
            "SUBMISSION": "SUBMISSION.tsv", "COVERPAGE": "COVERPAGE.tsv",
            "OTHERMANAGER": "OTHERMANAGER.tsv", "OTHERMANAGER2": "OTHERMANAGER2.tsv",
            "INFOTABLE": "INFOTABLE.tsv", "SIGNATURE": "SIGNATURE.tsv",
            "SUMMARYPAGE": "SUMMARYPAGE.tsv",
        },
        "layout": "period_dir",
        # Each table's natural identity. INFOTABLE/SUMMARYPAGE/OTHERMANAGER carry a
        # SEC surrogate *_SK; the per-filing tables key on ACCESSION_NUMBER (one
        # COVERPAGE/SUBMISSION/SIGNATURE row per filing).
        "dedup": {
            "SUBMISSION": ("ACCESSION_NUMBER",),
            "COVERPAGE": ("ACCESSION_NUMBER",),
            "OTHERMANAGER": ("ACCESSION_NUMBER", "OTHERMANAGER_SK"),
            "OTHERMANAGER2": ("ACCESSION_NUMBER", "SEQUENCENUMBER"),
            "INFOTABLE": ("ACCESSION_NUMBER", "INFOTABLE_SK"),
            "SIGNATURE": ("ACCESSION_NUMBER",),
            "SUMMARYPAGE": ("ACCESSION_NUMBER",),
        },
        # INFOTABLE.VALUE etc. are kept EXACTLY as published (no scaling); see the
        # ingest manifest's value_units_note. We mirror the legacy ingest's numeric
        # coercion so new partitions match the existing schema byte-for-byte.
        "numeric": {
            "INFOTABLE": {"VALUE": "Int64", "SSHPRNAMT": "Int64",
                          "VOTING_AUTH_SOLE": "Int64", "VOTING_AUTH_SHARED": "Int64",
                          "VOTING_AUTH_NONE": "Int64", "INFOTABLE_SK": "Int64"},
            "SUMMARYPAGE": {"OTHERINCLUDEDMANAGERSCOUNT": "Int64",
                            "TABLEENTRYTOTAL": "Int64", "TABLEVALUETOTAL": "Int64"},
            "OTHERMANAGER": {"OTHERMANAGER_SK": "Int64"},
            "OTHERMANAGER2": {"SEQUENCENUMBER": "Int64"},
        },
        # Provenance columns the legacy ingest adds (so new partitions match disk).
        "stamp_provenance": True,
    },
    "edgar_insider": {
        "page_url": "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets",
        "zip_re": r"/insider-transactions-data-sets/([^/\"]+)_form345\.zip",
        "zip_url": "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{key}_form345.zip",
        "out_dir": "edgar_insider",
        "tables": {
            "submission": "SUBMISSION.tsv", "reportingowner": "REPORTINGOWNER.tsv",
            "nonderiv_trans": "NONDERIV_TRANS.tsv", "nonderiv_holding": "NONDERIV_HOLDING.tsv",
            "deriv_trans": "DERIV_TRANS.tsv", "deriv_holding": "DERIV_HOLDING.tsv",
            "footnotes": "FOOTNOTES.tsv", "owner_signature": "OWNER_SIGNATURE.tsv",
        },
        "layout": "year_file",
        "dedup": {
            "submission": ("ACCESSION_NUMBER",),
            "reportingowner": ("ACCESSION_NUMBER", "RPTOWNERCIK"),
            "nonderiv_trans": ("ACCESSION_NUMBER", "NONDERIV_TRANS_SK"),
            "nonderiv_holding": ("ACCESSION_NUMBER", "NONDERIV_HOLDING_SK"),
            "deriv_trans": ("ACCESSION_NUMBER", "DERIV_TRANS_SK"),
            "deriv_holding": ("ACCESSION_NUMBER", "DERIV_HOLDING_SK"),
            "footnotes": ("ACCESSION_NUMBER", "FOOTNOTE_ID"),
            # owner_signature has NO unique surrogate key and the raw SEC TSV
            # contains a handful of EXACT full-row duplicates per quarter (e.g.
            # 90/71,918 in 2026q1). Dedup on the FULL row (handled by the
            # all-columns fallback in _fetch_key) so re-ingest is idempotent and no
            # DISTINCT signature line can ever be dropped — only byte-identical
            # repeats are collapsed. This is the one table whose incremental row
            # count is intentionally <= the legacy raw build (redundant rows
            # removed); all distinct content is preserved.
            "owner_signature": None,
        },
        "numeric": {},
        "stamp_provenance": False,
        # Authoritative per-table column dtypes, mirroring the existing build
        # (_target_schema.json / jobs _backfill_edgar_insider.py) and VERIFIED
        # against the on-disk production parquet schema so new files match exactly:
        # `datetime` cols are parsed from SEC's DD-MON-YYYY -> timestamp[ns];
        # `float` cols -> double; everything else stays string. A heuristic ("ends
        # in DATE") is NOT enough — DATE_OF_ORIG_SUB and the share/price/value
        # float columns would be mistyped.
        "insider_dtypes": {
            "submission": {
                "datetime": ["FILING_DATE", "PERIOD_OF_REPORT", "DATE_OF_ORIG_SUB"],
                "float": []},
            "reportingowner": {"datetime": [], "float": []},
            "nonderiv_trans": {
                "datetime": ["TRANS_DATE", "DEEMED_EXECUTION_DATE"],
                "float": ["TRANS_SHARES", "TRANS_PRICEPERSHARE",
                          "SHRS_OWND_FOLWNG_TRANS", "VALU_OWND_FOLWNG_TRANS"]},
            "nonderiv_holding": {
                "datetime": [],
                "float": ["SHRS_OWND_FOLWNG_TRANS", "VALU_OWND_FOLWNG_TRANS"]},
            "deriv_trans": {
                "datetime": ["TRANS_DATE", "DEEMED_EXECUTION_DATE", "EXCERCISE_DATE",
                             "EXPIRATION_DATE"],
                "float": ["CONV_EXERCISE_PRICE", "TRANS_SHARES", "TRANS_TOTAL_VALUE",
                          "TRANS_PRICEPERSHARE", "UNDLYNG_SEC_SHARES", "UNDLYNG_SEC_VALUE",
                          "SHRS_OWND_FOLWNG_TRANS", "VALU_OWND_FOLWNG_TRANS"]},
            "deriv_holding": {
                "datetime": ["EXERCISE_DATE", "EXPIRATION_DATE"],
                "float": ["CONV_EXERCISE_PRICE", "UNDLYNG_SEC_SHARES", "UNDLYNG_SEC_VALUE",
                          "SHRS_OWND_FOLWNG_TRANS", "VALU_OWND_FOLWNG_TRANS"]},
            "footnotes": {"datetime": [], "float": []},
            "owner_signature": {"datetime": ["OWNERSIGNATUREDATE"], "float": []},
        },
    },
}

# Only year-quarter keys (2024q1) or rolling windows (01dec2025-28feb2026) are real.
_QKEY = re.compile(r"^(\d{4})q([1-4])$")
_MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
           "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
_RANGE = re.compile(r"^\d{2}([a-z]{3})(\d{4})-\d{2}([a-z]{3})(\d{4})$")

# Refuse to fetch an absurd number of "new" keys in a single tick — that almost
# always means the on-disk inventory was wiped (so every key looks new). We fetch
# the cap and report partial so the rest run next tick, rather than re-pull the
# whole multi-year history and look falsely "fresh".
MAX_KEYS_PER_TICK = 8


def _key_sort_value(key: str):
    """Chronological sort/dedupe value for a dataset key (year, quarter)."""
    m = _QKEY.match(key)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = _RANGE.match(key)
    if m:
        end_mon, end_yr = _MONTHS[m.group(3)], int(m.group(4))
        return end_yr, (end_mon - 1) // 3 + 1
    return (0, 0)


def _valid_key(key: str) -> bool:
    return bool(_QKEY.match(key) or _RANGE.match(key))


def _http_get(url, *, timeout, retries=4, rate=0.4, session=None):
    """GET with the giant failure contract: bytes on 200; TransientError on
    429/5xx/network after retries; DefinitiveError on a hard 4xx (e.g. 404 for a
    key the page advertised but the file store hasn't published yet is rare — we
    treat a 404 as transient because SEC sometimes lists a key minutes before the
    zip is reachable)."""
    import requests
    s = session or requests
    last = None
    for attempt in range(retries):
        try:
            r = s.get(url, headers=UA, timeout=timeout)
        except Exception as e:  # noqa: BLE001 — network/timeout family
            last = f"net:{e}"
            time.sleep(min(60, rate * (attempt + 1) * 4))
            continue
        if r.status_code == 200:
            return r.content
        if r.status_code in (429, 500, 502, 503, 504, 404):
            last = f"http{r.status_code}"
            time.sleep(min(120, 10 * (attempt + 1)))
            continue
        raise DefinitiveError(f"{SOURCE}: hard HTTP {r.status_code} for {url[-80:]}")
    raise TransientError(f"{SOURCE}: {url[-80:]} -> {last} after {retries} attempts")


def _published_keys(prod_cfg, session) -> list[str]:
    """Scrape the data-sets page for every published <key>. Raises TransientError on
    a network failure OR a 200 that yields zero keys (structural break — never
    laundered into no_change)."""
    html = _http_get(prod_cfg["page_url"], timeout=90, session=session)
    keys = re.findall(prod_cfg["zip_re"], html.decode("utf-8", "replace"))
    seen, out = set(), []
    for k in keys:
        if k not in seen and _valid_key(k):
            seen.add(k)
            out.append(k)
    if not out:
        raise TransientError(
            f"{SOURCE}/{prod_cfg['out_dir']}: data-sets page parsed 0 dataset keys "
            f"(layout change or transient empty body); existing data kept")
    return sorted(out, key=_key_sort_value)


def _keys_on_disk(prod_cfg) -> set[str]:
    """The dataset keys already materialized on disk for this product."""
    base = os.path.join(config.DATA_ROOT, prod_cfg["out_dir"])
    keys: set[str] = set()
    # R36: this enumerates which partitions we ALREADY HOLD, and it walked them with nested
    # os.listdir behind os.path.isdir guards. On a runner (AQUEDUCT_BACKEND=r2) none of those
    # directories exist, every guard short-circuits, and it returns an EMPTY SET — which does
    # not read as "I could not look", it reads as "we hold nothing", the opposite of the truth
    # for a fully-ingested store.
    #
    # Partitions are DIRECTORIES and blob lists OBJECTS, so the key is recovered from each
    # object's path instead: one recursive listing per table, then the leading `period=<key>/`
    # segment, or the `<table>_<key>.parquet` basename under a `year=<YYYY>/` segment. Same
    # keys, from a listing that is true on both backends.
    if prod_cfg["layout"] == "period_dir":
        # <TABLE>/period=<key>/...  -- enumerate across tables (union).
        for tbl in prod_cfg["tables"]:
            for rel in blob.list_parquets(os.path.join(base, tbl), recursive=True):
                head = rel.split("/", 1)[0]
                if head.startswith("period="):
                    keys.add(head[len("period="):])
    else:  # year_file: <table>/year=<YYYY>/<table>_<key>.parquet
        for tbl in prod_cfg["tables"]:
            for rel in blob.list_parquets(os.path.join(base, tbl), recursive=True):
                parts = rel.split("/")
                if len(parts) < 2 or not parts[0].startswith("year="):
                    continue
                m = re.match(rf"^{re.escape(tbl)}_(.+)\.parquet$", parts[-1])
                if m:
                    keys.add(m.group(1))
    return keys


def _resolve_member(names, basename):
    """Match a TSV member by basename (SEC nests files under a folder in newer zips)."""
    target = basename.lower()
    for n in names:
        if not n.endswith("/") and os.path.basename(n).lower() == target:
            return n
    return None


def _read_tsv(zf: zipfile.ZipFile, member_basename: str) -> pd.DataFrame:
    """Read a tab-delimited member all-as-string, robust to embedded quotes."""
    resolved = _resolve_member(zf.namelist(), member_basename)
    if resolved is None:
        return pd.DataFrame()
    with zf.open(resolved) as f:
        df = pd.read_csv(
            f, sep="\t", dtype=str,
            keep_default_na=False, na_values=[],
            quoting=csv.QUOTE_NONE, engine="c",
            on_bad_lines="warn", encoding="utf-8", encoding_errors="replace",
        )
    df.columns = [c.strip() for c in df.columns]
    return df


def _coerce_13f(df: pd.DataFrame, table: str, prod_cfg: dict, key: str) -> pd.DataFrame:
    """Mirror jobs/ingest_edgar_13f.py: '' -> NA, cast numeric cols, stamp provenance.
    Keeps the new partition's schema identical to the existing on-disk format."""
    if df.empty:
        return df
    df = df.replace("", pd.NA)
    for col, dt in prod_cfg["numeric"].get(table, {}).items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dt)
    df = df.copy()
    df["DATASET_PERIOD"] = key
    df["SOURCE_ID"] = "edgar_13f"
    return df


def _coerce_insider(df: pd.DataFrame, table: str, prod_cfg: dict) -> pd.DataFrame:
    """Insider tables: parse `datetime` cols (DD-MON-YYYY -> timestamp[ns]) and cast
    `float` cols (-> double) per the authoritative per-table dtype map, '' -> None
    elsewhere. Mirrors _backfill_edgar_insider.py and is VERIFIED to reproduce the
    existing on-disk parquet schema exactly."""
    if df.empty:
        return df
    df = df.copy()
    spec = prod_cfg["insider_dtypes"].get(table, {"datetime": [], "float": []})
    dt_cols, fl_cols = set(spec["datetime"]), set(spec["float"])
    for c in df.columns:
        if c in dt_cols:
            s = df[c].replace("", pd.NA)
            s = pd.to_datetime(s, format="%d-%b-%Y", errors="coerce")
            # DEFENSIVE, AND HONESTLY UNPROVEN — read before extending or removing.
            #
            # sec_edgar has never reported a success; its state row records
            #   ArrowInvalid('Casting from timestamp[us] to timestamp[ns] would result in out of
            #    bounds timestamp: -61950355200000000')
            # which, read as microseconds, is 0006-11-15. The stored parquets are timestamp[ns]
            # deliberately (the docstring above requires new partitions to match them exactly),
            # so a us column carrying year 6 cannot be aligned to them and the whole run dies.
            #
            # WHAT I COULD NOT SHOW: that this line is where that column comes from. On pandas
            # 2.3.3 / pyarrow 23.0.0, to_datetime(..., errors="coerce") on "15-NOV-0006" already
            # returns datetime64[ns] with NaT, and the cast to ns SUCCEEDS — so the tidy
            # explanation ("pandas 2.x returns non-ns and year 6 survives") is false for this
            # version, and CI installs the same 2.3.x. All 1,019 stored parquets were also
            # scanned: none holds an out-of-ns-range timestamp. The origin is still open.
            #
            # This block is therefore a GUARD, not a repair: it makes the ns contract explicit
            # and local instead of relying on a to_datetime default that has changed before and
            # may change again. It is cheap, it cannot corrupt in-range data (see
            # tests/test_sec_edgar_out_of_range_dates.py), and it does not license the claim
            # that sec_edgar is fixed.
            #
            # NOT merge._report_impossible_dates: that REPORTS after the fact, deliberately does
            # not drop, and runs after the merge — it cannot prevent a cast that fails during
            # schema alignment.
            #
            # Nulled, not clamped: NULL says "unknown", whereas a clamp to 1677 would invent a
            # plausible date nobody could later tell from a real one.
            lo, hi = pd.Timestamp.min, pd.Timestamp.max
            try:
                oor = s.notna() & ((s < lo) | (s > hi))
                if bool(oor.any()):
                    print(f"[{SOURCE}] {table}.{c}: {int(oor.sum())} date(s) outside "
                          f"timestamp[ns] range nulled (e.g. {s[oor].iloc[0]})", flush=True)
                    s = s.mask(oor)
                s = s.astype("datetime64[ns]")
            except (TypeError, OverflowError, ValueError):
                pass          # already ns, or incomparable — leave as parsed rather than fail
            df[c] = s
        elif c in fl_cols:
            s = df[c].replace("", pd.NA)
            df[c] = pd.to_numeric(s, errors="coerce").astype("float64")
        else:
            df[c] = df[c].replace("", None)
    # NOTE: the insider `year` column is HIVE-derived from the year=<YYYY> directory
    # name, NOT stored in the file (confirmed against existing parquet schema). Do
    # NOT add it here or the new file's schema would differ from every existing one.
    return df


def _out_path(prod_cfg: dict, table: str, key: str) -> str:
    base = os.path.join(config.DATA_ROOT, prod_cfg["out_dir"])
    if prod_cfg["layout"] == "period_dir":
        return os.path.join(base, table, f"period={key}", f"{table}.parquet")
    year = key[:4]
    return os.path.join(base, table, f"year={year}", f"{table}_{key}.parquet")


def _to_table(df: pd.DataFrame, prod_cfg: dict, table: str, key: str) -> pa.Table:
    """Build the pyarrow table for one (table, key).

    IMPORTANT — match the EXACT on-disk file schema: the stored 13f parquet carries
    only DATASET_PERIOD + SOURCE_ID (added in _coerce_13f). The `period` column seen
    when reading via a partitioned-dataset API is a HIVE partition column DERIVED
    from the directory name (period=<key>); it is NOT stored in the file. We write
    single files via blob/merge, so we must NOT add a `period` column or the new
    partition's file schema would differ from every existing one. Likewise insider's
    `year` is the real stored dictionary<int32> partition column (added in
    _coerce_insider) and IS kept."""
    if df.empty:
        return pa.table({})
    return pa.Table.from_pandas(df, preserve_index=False)


def _fetch_key(prod_cfg: dict, key: str, session, tally: Tally,
               cursors: dict | None = None) -> int:
    """Download + parse one dataset key's zip and merge every table into its own
    per-period parquet (never-shrink/dedup). Returns rows added across tables.
    Raises TransientError on download/zip failure (the key re-runs next tick)."""
    url = prod_cfg["zip_url"].format(key=key)
    raw = _http_get(url, timeout=600, retries=4, session=session)
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
        names = zf.namelist()
    except zipfile.BadZipFile:
        # A truncated/rotated body is a transient fault, not data — retry next tick.
        raise TransientError(f"{SOURCE}/{prod_cfg['out_dir']} {key}: bad/truncated zip")
    if not names:
        raise TransientError(f"{SOURCE}/{prod_cfg['out_dir']} {key}: empty zip")

    added_here = 0
    is_13f = prod_cfg["stamp_provenance"]
    big_holdings_seen = False
    for table, member in prod_cfg["tables"].items():
        df = _read_tsv(zf, member)
        if is_13f:
            df = _coerce_13f(df, table, prod_cfg, key)
        else:
            df = _coerce_insider(df, table, prod_cfg)
        out_path = _out_path(prod_cfg, table, key)
        new_tbl = _to_table(df, prod_cfg, table, key)
        before = blob.row_count(out_path)
        if new_tbl.num_rows == 0:
            # Legitimately-empty table inside a real quarter (SEC emits empty
            # OTHERMANAGER2 etc.). Write a 0-row partition so it exists, exactly
            # like the existing build — but only if there is no richer file already.
            if before == 0:
                blob.write_table_atomic(out_path, new_tbl)
            tally.empty_unit()
            continue
        dedup = prod_cfg["dedup"].get(table)
        # dedup is None for tables with no unique surrogate key (owner_signature):
        # use FULL-row dedup so only byte-identical repeats collapse and no distinct
        # row is ever lost. Same fallback if a configured key column is unexpectedly
        # absent (schema drift) — never silently mis-dedup on a partial key.
        if dedup is None or not all(c in new_tbl.column_names for c in dedup):
            dedup = tuple(new_tbl.column_names)
        n, _ = merge.merge_and_write(out_path, new_tbl, mode="merge",
                                     dedup_keys=dedup, allow_empty=True)
        added = max(0, n - before)
        # Cursors only where the table IS a series table. sec_edgar merges filing tables
        # (INFOTABLE, nonderiv_trans, ...) whose identity is all columns, not
        # (series_key, obs_date) - the dedup fallback above says as much. Reporting cursors
        # for those would be meaningless; withholding them for the series-shaped ones leaves
        # their CSVs stale (§5.7), so the test is the columns, not the source.
        if cursors is not None and {"series_key", "obs_date"} <= set(new_tbl.column_names):
            merge_cursor_map(cursors, cursors_from_table(new_tbl, cap=CURSOR_CAP),
                             cap=CURSOR_CAP)
        added_here += added
        tally.added_unit(added)
        if table in ("INFOTABLE", "nonderiv_trans"):
            big_holdings_seen = new_tbl.num_rows > 0

    # Sanity guard mirroring the ingester: a multi-MB 13f zip with a 0-row holdings
    # table means we failed to locate the member (silent loss) -> structural.
    if is_13f and not big_holdings_seen and len(raw) > 5_000_000:
        raise DefinitiveError(
            f"{SOURCE}/edgar_13f {key}: INFOTABLE parsed 0 rows from a {len(raw):,}B "
            f"zip — unrecognized member layout; refusing to mark this key done")
    return added_here


def _state_path() -> str:
    return os.path.join(config.DATA_ROOT, "_sec_edgar_incr_state.json")


def _load_state() -> dict:
    p = _state_path()
    if not os.path.exists(p):
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    p = _state_path()
    tmp = f"{p}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, separators=(",", ":"))
        os.replace(tmp, p)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def current_vintage(unit) -> str | None:
    """Cheap catalogue probe for giant_changed_units.detect_change: a stable token
    over every published key across both products. Changes iff a NEW dataset key
    appears upstream (or a key we still owe is published). None on a transient
    probe failure (caller then fetches anyway, cadence-gated; the merge is safe)."""
    import hashlib
    import requests
    sess = requests.Session()
    h = hashlib.sha256()
    try:
        for pid in sorted(PRODUCTS):
            keys = _published_keys(PRODUCTS[pid], sess)
            h.update(pid.encode())
            h.update(b"=")
            h.update(";".join(keys).encode())
            h.update(b"|")
    except (TransientError, DefinitiveError):
        return None
    return "cat:" + h.hexdigest()[:16]


def update(unit, since) -> Result:
    cursors: dict[str, str] = {}
    """Fetch only the dataset keys NEW since what's on disk (per product), merge each
    table into its own per-period parquet under never-shrink/dedup, and return one
    honest source-level Result. `since` is unused (EDGAR has no row-level date param;
    the on-disk partition inventory IS the watermark)."""
    import requests
    sess = requests.Session()
    state = _load_state()
    tally = Tally()
    total_added = 0
    last_key_overall = None
    selected_total = 0
    capped = False

    for pid in sorted(PRODUCTS):
        prod_cfg = PRODUCTS[pid]
        published = _published_keys(prod_cfg, sess)  # TransientError -> whole unit partial
        on_disk = _keys_on_disk(prod_cfg)
        pstate = state.setdefault(pid, {})

        # Select a key iff it is genuinely NEW (absent from the on-disk partition
        # inventory) OR its recorded state says the last run FAILED (so a key that
        # transient/structurally-failed once is retried, never frozen). A key already
        # on disk with no failure state is materialized -> never re-fetched (state
        # absence means "trust the disk inventory", not "re-pull everything").
        # Oldest-first for deterministic forward progress under the per-tick cap.
        _FAILED = {"partial", "transient_fail", "definitive_fail"}
        want = []
        for k in published:
            st = pstate.get(k, {}).get("status")
            if k not in on_disk or st in _FAILED:
                want.append(k)
        want = sorted(set(want), key=_key_sort_value)
        if len(want) > MAX_KEYS_PER_TICK:
            want = want[:MAX_KEYS_PER_TICK]
            capped = True

        for key in want:
            selected_total += 1
            try:
                added = _fetch_key(prod_cfg, key, sess, tally, cursors=cursors)
            except TransientError:
                tally.transient_unit()
                pstate[key] = {"status": "transient_fail"}
                _save_state(state)
                time.sleep(0.5)
                continue
            except DefinitiveError as e:
                tally.structural_unit()
                pstate[key] = {"status": "definitive_fail", "error": str(e)[:200]}
                _save_state(state)
                time.sleep(0.5)
                continue
            total_added += added
            pstate[key] = {"status": "ok", "added": added}
            if last_key_overall is None or _key_sort_value(key) > _key_sort_value(last_key_overall):
                last_key_overall = key
            _save_state(state)
            time.sleep(0.5)  # politeness toward SEC fair-access limits

    _save_state(state)

    if capped:
        return Result(status="partial", obs=total_added, last_obs_date=last_key_overall,
                      new_vintage=None,
                      error=f"selected>cap: fetched {selected_total} keys; remainder re-runs "
                            f"next tick (+{total_added} rows)")

    # finalize(): structural -> DefinitiveError(partial); transient -> partial; else
    # ok (added>0) / no_change. empty_window_floor is high because most ticks add 0
    # keys (no new quarter) -> that is a legitimate no_change, never a structural break.
    return finalize(tally, total_added, last_key_overall, source=SOURCE,
                    series_cursors=cursors or None,
                    empty_window_floor=10_000)
