"""S1 fetcher — Penn World Table 11.0 (annual country x variable panel).

CC BY 4.0 (GGDC / Feenstra-Inklaar-Timmer 2015, DOI 10.34894/FABVLR). PWT ships as
ONE Excel workbook (pwt110.xlsx) on the GGDC Dataverse — a full annual panel that is
re-estimated WITH back-revisions across the whole panel between releases, so the
correct refresh is a whole-table re-fetch + MERGE (dedup series_key+obs_date, new
wins on revision, never-shrink).

GROUPED STORAGE: one parquet per VARIABLE at clean_full/penn_world_table/<var>.parquet
with schema (series_key, obs_date, value), series_key = "<variable>|<ISO3>", annual
values dated Dec-31. 42 variable files. We re-parse the workbook's `Data` sheet (185
economies x 1950-2023 x 42 numeric variables), reusing the ingester's URL + parse
logic, and publish each variable file through merge.merge_and_write.

VINTAGE: the Dataverse file record (api/files/{id}) exposes a SHA-1 content checksum
(+ filesize/version) that changes iff the workbook content changes — a cheap, exact
probe. HEAD on the access URL 403s, so we use this JSON record instead.

A 200 that parses 0 numeric observations from a real workbook body is a structural
break (the `Data` sheet shape changed) -> finalize raises DefinitiveError, leaving
existing data untouched. A download/parse transient -> partial (re-run next tick).
"""
from __future__ import annotations
import datetime as dt
import io
import os

import pandas as pd
import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

SOURCE = "penn_world_table"
DEDUP = ("series_key", "obs_date")

# GGDC Dataverse datafile id for pwt110.xlsx (DOI 10.34894/FABVLR). The access URL
# 302-redirects to the object store and serves the workbook; the file-record URL is a
# small JSON used purely for cheap vintage detection. Bump these on a new PWT release.
DATAFILE_ID = "554105"
DATA_URL = f"https://dataverse.nl/api/access/datafile/{DATAFILE_ID}"
FILE_RECORD_URL = f"https://dataverse.nl/api/files/{DATAFILE_ID}"

# Identifier columns and the categorical "Data information" columns -> NOT numeric data.
ID_COLS = {"countrycode", "country", "currency_unit", "year"}
INFO_COLS = {"i_cig", "i_xm", "i_xr", "i_outlier", "i_irr"}

# Authoritative numeric variables (the 42 published series families). A new vintage
# that renames/adds a column will surface here as a parse mismatch (logged as structural).
VAR_DEFS = {
    "rgdpe", "rgdpo", "pop", "emp", "avh", "hc", "ccon", "cda", "cgdpe", "cgdpo",
    "cn", "ck", "ctfp", "cwtfp", "rgdpna", "rconna", "rdana", "rnna", "rkna",
    "rtfpna", "rwtfpna", "labsh", "irr", "delta", "xr", "pl_con", "pl_da",
    "pl_gdpo", "cor_exp", "csh_c", "csh_i", "csh_g", "csh_x", "csh_m", "csh_r",
    "pl_c", "pl_i", "pl_g", "pl_x", "pl_m", "pl_n", "pl_k",
}


def current_vintage(unit):
    """Cheap probe: the Dataverse file record's SHA-1 content checksum (changes iff the
    workbook content changes). Falls back to filesize/version, then None. HEAD on the
    access URL 403s on this server, so the small JSON record is the right signal."""
    try:
        r = requests.get(FILE_RECORD_URL, headers=UA, timeout=60)
    except (requests.Timeout, requests.ConnectionError):
        return None
    if r.status_code != 200:
        return None
    try:
        d = r.json().get("data", {})
        f = d.get("dataFile", {})
        chk = (f.get("checksum") or {}).get("value")
        if chk:
            return f"sha1:{chk}"
        size = f.get("filesize")
        ver = d.get("version")
        if size is not None:
            return f"size:{size}/v{ver}"
    except (ValueError, AttributeError):
        return None
    return None


def _series_maxes(tbl):
    out = {}
    if tbl.num_rows == 0:
        return out
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    tally = Tally()

    # ---- 1) Re-fetch the whole workbook (reuse the ingester's URL + polite UA). ----
    try:
        r = requests.get(DATA_URL, headers=UA, timeout=300, allow_redirects=True)
    except (requests.Timeout, requests.ConnectionError) as e:
        tally.transient_unit(f"pwt workbook: {type(e).__name__} — {str(e)[:140]}")
        return finalize(tally, 0, None, source=SOURCE)
    if r.status_code in (429, 500, 502, 503, 504):
        tally.transient_unit(f"pwt workbook: HTTP {r.status_code}")
        return finalize(tally, 0, None, source=SOURCE)
    if r.status_code != 200 or len(r.content) < 100_000:
        # Hard non-200 / truncated body: not a healthy upstream -> structural, keep data.
        tally.structural_unit(
            f"pwt workbook: HTTP {r.status_code}, {len(r.content):,} bytes "
            f"(under the 100,000-byte floor)")
        return finalize(tally, 0, None, source=SOURCE)

    # ---- 2) Parse the `Data` sheet (reuse the ingester's column logic). ----
    try:
        df = pd.read_excel(io.BytesIO(r.content), sheet_name="Data", engine="openpyxl")
    except Exception as e:  # noqa: BLE001 — a 200 body we cannot parse as the workbook
        tally.structural_unit(f"pwt workbook: 200 but unparseable — {type(e).__name__}: "
                              f"{str(e)[:120]}")
        return finalize(tally, 0, None, source=SOURCE)

    if "year" not in df.columns or "countrycode" not in df.columns:
        tally.structural_unit(
            "pwt workbook: the `Data` sheet is missing `year` and/or `countrycode`")
        return finalize(tally, 0, None, source=SOURCE)

    data_cols = [c for c in df.columns if c not in ID_COLS and c not in INFO_COLS and c in VAR_DEFS]
    if not data_cols:
        # 200, real workbook, but no recognised numeric variables
        tally.structural_unit(
            f"pwt workbook: none of its {len(df.columns)} columns is a recognised variable")
        return finalize(tally, 0, None, source=SOURCE)

    df = df.copy()
    df["_obs_date"] = df["year"].astype(int).map(lambda y: dt.date(int(y), 12, 31))

    # ---- 3) Publish ONE parquet per variable via merge_and_write (atomic, dedup, never-shrink). ----
    total_rows = 0
    last_obs = None
    all_cursors: dict = {}
    for var in data_cols:
        sub = df.loc[df[var].notna(), ["countrycode", "_obs_date", var]]
        path = os.path.join(out_dir, var + ".parquet")
        before = blob.row_count(path)
        if sub.empty:
            # No values for this variable in a real body: if it has published history,
            # that's a structural drop; if it never existed, a genuine empty sub-unit.
            if before > 0:
                tally.structural_unit(
                    f"{var}: variable vanished from the workbook over {before:,} stored rows")
            else:
                tally.empty_unit(f"{var}: never published")
            total_rows += before
            continue

        keys = (var + "|" + sub["countrycode"].astype(str)).tolist()
        dates = sub["_obs_date"].tolist()
        vals = pd.to_numeric(sub[var], errors="coerce").astype(float).tolist()
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        tally.added_unit(max(0, n - before))
        total_rows += n
        if md and (last_obs is None or md > last_obs):
            last_obs = md
        all_cursors.update(_series_maxes(tbl))

    # This is a whole-table S1 source: on an UNCHANGED release every one of the 42
    # variable sub-units legitimately merges 0 new rows (merge dedups the re-fetch away),
    # which is a healthy `no_change`, not a structural break. Genuine breaks are already
    # caught explicitly above (non-200/truncated body, unparseable workbook, missing
    # `Data` sheet, no recognised variables, or a variable that LOST its published
    # history -> structural_unit). So lift the empty-window floor above the variable
    # count so finalize can't misread an all-no-change refresh as a break.
    return finalize(tally, total_rows, last_obs, source=SOURCE,
                    series_cursors=all_cursors, empty_window_floor=len(data_cols) + 1)
