"""S1 fetcher — V-Dem (Varieties of Democracy), country-year democracy indices.

CC BY 4.0 (V-Dem Institute). Two grouped parquets in one dir:
  clean_full/vdem/vdem.parquet   (the ~77M-obs core)
  clean_full/vdem/vparty.parquet (the ~2.2M-obs party-level cube)
Both share schema (series_key, obs_date, value) with series_key = "VDEM:{variable}:{country_text_id}"
and obs_date = Dec-31 of the country-year. V-Dem ships as a single wide country-year RData
regenerated per annual release (no obs-level since), so the correct refresh is: release-detect on
the GitHub repo master commit SHA, then full re-pull + MERGE both files on change (dedup
series_key+obs_date, new wins on revision, never-shrink). Each file is one sub-unit; a 200 that
melts to 0 rows from a real body is structural.

This REUSES the URL + parse logic of jobs/ingest_vdem.py (RData -> pyreadr -> melt-to-long).
"""
from __future__ import annotations
import datetime as dt
import os
import tempfile

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import DefinitiveError
from ..base import Result
from ._common import CURSOR_CAP, Tally, finalize, merge_cursor_map
from ._vintage import github_sha, UA

SOURCE = "vdem"
REPO = "vdeminstitute/vdemdata"
RAW = "https://raw.githubusercontent.com/vdeminstitute/vdemdata/master/data"
DEDUP = ("series_key", "obs_date")

# (remote RData name, local parquet label, (country_col, year_col), party_col) — from jobs/ingest_vdem.py
# party_col is the within-country-year party identifier (None for the country-year vdem cube).
# vparty is a country-year-PARTY cube: many parties share one (country_text_id, year), so the
# party id MUST enter the series_key or (series_key, obs_date) dedup collapses it. v2paid is V-Dem's
# stable party id (unique within country-year, 0 nulls); see _df_to_long.
DATASETS = [
    ("vdem.RData",   "vdem",   ("country_text_id", "year"), None),
    ("vparty.RData", "vparty", ("country_text_id", "year"), "v2paid"),
]

# Columns to always skip (metadata, text, non-numeric) — verbatim from jobs/ingest_vdem.py
ALWAYS_SKIP = {
    "country_id", "country_name", "country_text_id",
    "histname", "codingstart", "codingend", "codingstart_contemp",
    "codingend_contemp", "codingstart_core", "codingend_core",
    "gapstart1", "gapstart2", "gapstart3", "gapend1", "gapend2", "gapend3",
    "gap_idx", "project", "historical_date", "year", "id",
    "v2x_elecreg", "e_regiongeo",
    # vparty party identifiers — these are KEYS, not measures. v2paid enters the series_key
    # (see _df_to_long); both must be skipped so they aren't melted into bogus value series.
    "v2paid", "pf_party_id",
}

_TRANSIENT = (429, 500, 502, 503, 504)


def current_vintage(unit):
    """Cheap probe: the master commit SHA of the vdemdata GitHub repo (the registry's
    vintage_signal). Changes iff the repo (and thus the bundled RData) moved. Returns
    None if it can't be cheaply determined — the strategy then fetches anyway, which is
    safe (merge dedups + never shrinks)."""
    try:
        return github_sha(REPO)
    except Exception:
        return None


def _rdata_to_df(rdata_bytes: bytes):
    """Write RData to a temp file, read with pyreadr, return the largest data frame
    (verbatim shape from jobs/ingest_vdem.py.rdata_to_df)."""
    tmp = tempfile.NamedTemporaryFile(suffix=".RData", delete=False)
    try:
        tmp.write(rdata_bytes)
        tmp.close()
        import pyreadr
        result = pyreadr.read_r(tmp.name)
        best = None
        for k, df in result.items():
            if best is None or (df is not None and len(df) > len(result[best])):
                best = k
        return result[best] if best is not None else None
    finally:
        os.unlink(tmp.name)


def _df_to_long(df, country_col: str, year_col: str, party_col: str | None = None):
    """Melt wide country-year frame to long (series_key, obs_date, value) lists.
    Verbatim logic from jobs/ingest_vdem.py.df_to_long (200-col chunks, drop null/inf),
    EXCEPT: when party_col is given (vparty), the party id is appended to the series_key
    ("VDEM:{var}:{country}:{party}") so a country-year-party cube does not collapse under
    (series_key, obs_date) dedup. For the country-year vdem cube (party_col=None) the key
    is unchanged ("VDEM:{var}:{country}")."""
    import pandas as pd
    import numpy as np

    skip = set(ALWAYS_SKIP)
    skip.update([c.lower() for c in skip])
    skip.add(country_col.lower())
    skip.add(year_col.lower())

    use_party = party_col is not None and party_col in df.columns

    value_cols = []
    for col in df.columns:
        if col.lower() in skip:
            continue
        if use_party and col == party_col:
            continue
        if df[col].dtype in (object, str):
            continue
        if pd.api.types.is_numeric_dtype(df[col].dtype):
            value_cols.append(col)
    if not value_cols:
        return [], [], []

    id_vars = [country_col, year_col] + ([party_col] if use_party else [])

    CHUNK = 200
    all_keys, all_dates, all_vals = [], [], []
    for ci in range(0, len(value_cols), CHUNK):
        chunk_cols = value_cols[ci:ci + CHUNK]
        chunk_df = df[id_vars + chunk_cols].copy()
        chunk_df = chunk_df.dropna(subset=[year_col])
        try:
            melted = chunk_df.melt(
                id_vars=id_vars,
                value_vars=chunk_cols,
                var_name="variable",
                value_name="value",
            )
        except Exception:
            continue
        melted = melted.dropna(subset=["value"])
        melted = melted[np.isfinite(melted["value"].values)]
        if len(melted) == 0:
            continue
        countries = melted[country_col].fillna("").astype(str)
        variables = melted["variable"].astype(str)
        series_keys = "VDEM:" + variables + ":" + countries
        if use_party:
            # party id is numeric (e.g. 216.0); render as a stable integer-ish token
            parties = pd.to_numeric(melted[party_col], errors="coerce")
            party_tok = parties.map(lambda x: str(int(x)) if pd.notna(x) else "NA").astype(str)
            series_keys = series_keys + ":" + party_tok.values
        years = pd.to_numeric(melted[year_col], errors="coerce").dropna()
        melted = melted.loc[years.index]
        years = years.astype(int)
        obs_dates = [dt.date(yr, 12, 31) for yr in years]
        series_keys = series_keys.loc[melted.index].tolist()
        values = melted["value"].tolist()
        all_keys.extend(series_keys)
        all_dates.extend(obs_dates)
        all_vals.extend(values)
    return all_keys, all_dates, all_vals


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


def update(unit, since):
    """Re-fetch BOTH RData files, melt to long, and MERGE each parquet (never write
    parquet directly; never weaken merge guards). Honest Tally:
      success -> added_unit(n_new); 200 that melts 0 rows -> structural_unit();
      timeout/5xx/429/network -> transient_unit()."""
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    tally = Tally()
    total_rows = 0
    last_obs = None
    cursors: dict = {}
    cursors_capped = False

    for remote_name, label, (country_col, year_col), party_col in DATASETS:
        path = os.path.join(out_dir, f"{label}.parquet")
        before = blob.row_count(path)
        url = f"{RAW}/{remote_name}"

        # --- fetch (transient-aware) ---
        try:
            r = requests.get(url, headers=UA, timeout=300)
        except (requests.Timeout, requests.ConnectionError):
            tally.transient_unit()
            total_rows += before
            continue
        if r.status_code in _TRANSIENT:
            tally.transient_unit()
            total_rows += before
            continue
        if r.status_code != 200:
            # hard 4xx / moved / stale URL -> structural (surfaced, not faked)
            tally.structural_unit()
            total_rows += before
            continue

        # --- parse (reuse ingester) ---
        df = _rdata_to_df(r.content)
        if df is None:
            tally.structural_unit()
            total_rows += before
            continue
        col_map = {c.lower(): c for c in df.columns}
        ccol = col_map.get(country_col.lower(), country_col)
        ycol = col_map.get(year_col.lower(), year_col)
        if ycol not in df.columns:
            tally.structural_unit()
            total_rows += before
            continue

        pcol = col_map.get(party_col.lower(), party_col) if party_col else None
        keys, dates, vals = _df_to_long(df, ccol, ycol, pcol)
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        if tbl.num_rows == 0:
            # 200 with a real RData body but it melted to nothing -> structural break
            tally.structural_unit()
            total_rows += before
            continue

        # --- publish ONLY via merge (atomic, dedup, never-shrink @0.97) ---
        # NOTE on dedup keys: merge dedups on (series_key, obs_date). For vdem.parquet the
        # series_key is "VDEM:{var}:{country}" (genuine country-year cube — 0 collisions). For
        # vparty.parquet the series_key carries the party id ("VDEM:{var}:{country}:{v2paid}",
        # built in _df_to_long), so each party-series is distinct and (series_key, obs_date)
        # dedup is lossless (no party-row collapse, no spurious never-shrink trip). If a real
        # upstream schema break ever makes a merge refuse, DefinitiveError still surfaces as a
        # structural sub-unit (existing file left untouched by merge on refusal).
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError:
            tally.structural_unit()
            total_rows += before
            continue
        tally.added_unit(max(0, n - before))
        total_rows += n
        if md is not None and (last_obs is None or md > last_obs):
            last_obs = md
        # BOUNDED (2026-07-30). This store holds 1,465,759 distinct series against a
        # 50,000 cap, and `cursors.update(...)` took every one: each cursor is a state.db
        # row and a _catalog_ids_for query, both linear in the count. Not the runner-killer
        # abs was (376M series / ~94 GB), but the same unbounded shape.
        if merge_cursor_map(cursors, _series_maxes(tbl)):
            cursors_capped = True

    if cursors_capped:
        print(f"[vdem] cursor set hit the {CURSOR_CAP:,} cap — further changed series are "
              f"not individually reported", flush=True)

    # finalize() raises DefinitiveError on ANY structural sub-unit. When at least one file
    # merged cleanly (vdem core) but another is structurally incompatible (vparty's series_key
    # omits the party dimension, so S1 (series_key,obs_date) dedup would collapse/shrink it),
    # we keep the good merge VISIBLE and surface the bad file as a partial-with-blocker instead
    # of throwing away the successful vdem refresh. If NOTHING merged, let it raise honestly.
    try:
        return finalize(tally, total_rows, last_obs, source=SOURCE, series_cursors=cursors)
    except DefinitiveError as e:
        if tally.added > 0 or (tally.attempted - tally.structural - tally.transient) > 0:
            return Result(status="partial", obs=total_rows, last_obs_date=last_obs,
                          new_vintage="github-sha", series_cursors=cursors,
                          error=(f"{tally.structural}/{tally.attempted} file(s) structurally "
                                 f"incompatible with (series_key,obs_date) dedup "
                                 f"(vparty is party-level; series_key omits party) — kept good "
                                 f"data, surfaced: {e}"))
        raise
