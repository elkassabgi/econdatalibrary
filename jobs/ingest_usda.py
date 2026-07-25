#!/usr/bin/env python3
"""Grouped ingest of USDA NASS Quick Stats bulk files (FULL coverage).

Source of truth: the Quick Stats bulk .txt.gz dumps at
https://www.nass.usda.gov/datasets/ -- these are the ENTIRE Quick Stats
database (every survey + census observation, national/state/county/district/
watershed/zip), far more complete than per-series api_GET calls.

Storage pattern (mirrors ingest_eurostat / ingest_sec_edgar):
  ONE Parquet per "cube" = per bulk file group:
    crops, animals_products, demographics, economics, environmental,
    census2002, census2007, census2007zipcode, census2012,
    census2017, census2017zipcode, census2022
  Each Parquet has a `series_key` column (a stable composite id) plus the
  full set of self-describing NASS dimension columns, obs_date, value,
  raw value flags, and the time descriptors. Written in streamed row-group
  batches so memory stays bounded even for the >100M-row crops cube.

License: us-public-domain (USDA NASS Quick Stats is U.S. public domain).

Usage:
  python jobs/ingest_usda.py --probe qs.crops_20260603.txt.gz   # profile one file
  python jobs/ingest_usda.py --dry                              # parse all, no writes (counts only)
  python jobs/ingest_usda.py                                    # full run -> Parquet
"""
from __future__ import annotations
import datetime as dt
import glob
import gzip
import os
import sys
import collections

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)
RAW = os.path.join(ROOT, "data", "raw", "usda")
OUT = os.path.join(ROOT, "data", "clean_full", "usda")

LICENSE_ID = "us-public-domain"

# The 39 fixed columns of every Quick Stats bulk file (tab-delimited, header row).
EXPECTED_NCOLS = 39

# Value tokens that mean "no numeric value" (suppressed / withheld / not avail).
# We store value=NULL and keep the raw token in `value_flag`.
NULL_TOKENS = {"(D)", "(NA)", "(Z)", "(X)", "(S)", "(H)", "(L)", "(C)", "(IC)",
               "(NR)", "(B)", ".", "", "(NA )"}

# Columns that define the *series identity* (everything except the time + value).
# series_key = these joined; we also keep them as their own columns.
KEY_COLS = ["SOURCE_DESC", "SECTOR_DESC", "GROUP_DESC", "COMMODITY_DESC",
            "CLASS_DESC", "PRODN_PRACTICE_DESC", "UTIL_PRACTICE_DESC",
            "STATISTICCAT_DESC", "UNIT_DESC", "SHORT_DESC",
            "DOMAIN_DESC", "DOMAINCAT_DESC",
            "AGG_LEVEL_DESC", "LOCATION_DESC"]

# Columns we persist in the Parquet (self-describing). obs_date + value + flags appended.
STORE_COLS = ["series_key", "SOURCE_DESC", "SECTOR_DESC", "GROUP_DESC",
              "COMMODITY_DESC", "CLASS_DESC", "PRODN_PRACTICE_DESC",
              "UTIL_PRACTICE_DESC", "STATISTICCAT_DESC", "UNIT_DESC",
              "SHORT_DESC", "DOMAIN_DESC", "DOMAINCAT_DESC", "AGG_LEVEL_DESC",
              "STATE_ALPHA", "STATE_NAME", "COUNTY_CODE", "COUNTY_NAME",
              "ASD_DESC", "REGION_DESC", "ZIP_5", "WATERSHED_DESC",
              "CONGR_DISTRICT_CODE", "COUNTRY_NAME", "LOCATION_DESC",
              "YEAR", "FREQ_DESC", "BEGIN_CODE", "END_CODE",
              "REFERENCE_PERIOD_DESC", "WEEK_ENDING"]

BATCH_ROWS = 200_000   # flush a row-group every 200k rows -> tightly bounded memory
PART_ROWS = 1_000_000  # roll to a NEW self-contained Parquet part every 1M rows.
                       # Parts are closed immediately -> crash-resilient checkpoints.
                       # The sandbox terminates long-running subprocesses erratically
                       # (observed kills at 1M/12M/14M rows on crops, exit 127, with
                       # ample free RAM), so SMALL parts keep per-restart loss <=1M
                       # rows; an auto-restart loop then grinds each cube to 100%.


def parse_value(tok: str):
    """Return (float|None, raw_flag|None). Thousands commas stripped."""
    t = tok.strip()
    if t in NULL_TOKENS:
        return None, (t if t else None)
    cleaned = t.replace(",", "")
    try:
        return float(cleaned), None
    except ValueError:
        # Anything else non-numeric (rare): keep as flag, value NULL.
        return None, t


# Map common month/period names that can appear in REFERENCE_PERIOD_DESC.
_MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], start=1)}


def parse_date(year, freq, ref_period, begin_code, end_code, week_ending):
    """Derive a single representative obs_date (the period-END date).

    NASS encodes time via YEAR + FREQ_DESC + END_CODE (+ REFERENCE_PERIOD_DESC,
    WEEK_ENDING). We pick the *end* of the reference period so a value is dated
    to when the period closed (point-in-time-friendly, ALFRED-style spirit).

    Rules:
      * WEEK_ENDING present (YYYY-MM-DD)  -> use it verbatim (weekly survey).
      * FREQ MONTHLY / END_CODE 1..12     -> last day of that month.
      * FREQ POINT IN TIME + END_CODE     -> treat END_CODE as month, last day.
      * FREQ WEEKLY + END_CODE 1..53      -> last day (Sat) of that ISO-ish week.
      * else (ANNUAL / SEASON / YEAR)     -> Dec 31 of YEAR.
    Returns a datetime.date or None.
    """
    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    if not (1800 <= y <= 2100):
        return None

    we = (week_ending or "").strip()
    if we:
        try:
            return dt.datetime.strptime(we, "%Y-%m-%d").date()
        except ValueError:
            pass

    f = (freq or "").strip().upper()
    ec = (end_code or "").strip()
    ec_i = int(ec) if ec.isdigit() else None

    # Monthly / point-in-time keyed by month number in END_CODE (01..12)
    if ec_i is not None and 1 <= ec_i <= 12 and ("MONTH" in f or "POINT" in f or "MONTHLY" in f):
        return _month_end(y, ec_i)

    # Weekly keyed by week number in END_CODE (1..53)
    if ec_i is not None and 1 <= ec_i <= 53 and "WEEK" in f:
        try:
            # ISO week 'end' -> Sunday of that week
            return dt.date.fromisocalendar(y, min(ec_i, 52), 7)
        except ValueError:
            return dt.date(y, 12, 31)

    # Fallback: some monthly rows have FREQ ANNUAL? No -- but ref_period may name a month.
    rp = (ref_period or "").strip().upper()
    if rp[:3] in _MONTHS and ("YEAR" not in rp):
        return _month_end(y, _MONTHS[rp[:3]])

    # Annual / marketing year / season / everything else -> year end.
    return dt.date(y, 12, 31)


def _month_end(y, m):
    if m == 12:
        return dt.date(y, 12, 31)
    return dt.date(y, m + 1, 1) - dt.timedelta(days=1)


# Arrow schema (all dimension cols as string; obs_date date32; value float64).
def build_schema():
    fields = []
    for c in STORE_COLS:
        fields.append((c, pa.string()))
    fields.append(("obs_date", pa.date32()))
    fields.append(("value", pa.float64()))
    fields.append(("value_flag", pa.string()))
    return pa.schema(fields)


SCHEMA = build_schema()


class PartWriter:
    """Part-rolling Parquet writer for one cube.

    Writes <cube_dir>/part_NNN.parquet files, each capped at PART_ROWS rows and
    CLOSED immediately when full. This:
      * bounds peak memory (only BATCH_ROWS buffered, one short-lived writer),
      * gives crash-resilient checkpoints (a killed run resumes from the last
        completed part -- see rows_already_written / skip in main()),
      * keeps the grouped-storage contract (one cube = one directory of parts,
        every row carries series_key + full NASS dimensions).
    """
    def __init__(self, cube_dir, start_part=0):
        self.cube_dir = cube_dir
        self.part_idx = start_part
        self.writer = None
        self.buf = {name: [] for name in SCHEMA.names}
        self.n_in_buf = 0
        self.n_in_part = 0
        self.n_total = 0
        self.min_date = None
        self.max_date = None

    def _part_path(self):
        return os.path.join(self.cube_dir, f"part_{self.part_idx:03d}.parquet")

    def add(self, store_vals, obs_date, value, flag):
        b = self.buf
        for name, v in zip(STORE_COLS, store_vals):
            b[name].append(v)
        b["obs_date"].append(obs_date)
        b["value"].append(value)
        b["value_flag"].append(flag)
        self.n_in_buf += 1
        self.n_in_part += 1
        self.n_total += 1
        if obs_date is not None:
            if self.min_date is None or obs_date < self.min_date:
                self.min_date = obs_date
            if self.max_date is None or obs_date > self.max_date:
                self.max_date = obs_date
        if self.n_in_buf >= BATCH_ROWS:
            self._flush_buf()
        if self.n_in_part >= PART_ROWS:
            self._roll_part()

    def _flush_buf(self):
        if self.n_in_buf == 0:
            return
        arrays = []
        for name in SCHEMA.names:
            if name == "obs_date":
                arrays.append(pa.array(self.buf[name], type=pa.date32()))
            elif name == "value":
                arrays.append(pa.array(self.buf[name], type=pa.float64()))
            else:
                arrays.append(pa.array(self.buf[name], type=pa.string()))
        tbl = pa.Table.from_arrays(arrays, schema=SCHEMA)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self._part_path(), SCHEMA, compression="zstd")
        self.writer.write_table(tbl)
        self.buf = {name: [] for name in SCHEMA.names}
        self.n_in_buf = 0

    def _roll_part(self):
        """Close the current part (durable checkpoint) and start the next."""
        self._flush_buf()
        if self.writer is not None:
            self.writer.close()
            self.writer = None
        import gc
        gc.collect()
        self.part_idx += 1
        self.n_in_part = 0

    def close(self):
        self._flush_buf()
        if self.writer is not None:
            self.writer.close()
            self.writer = None


def cube_parts(cube_dir):
    """Sorted list of complete part Parquet paths in a cube directory."""
    return sorted(glob.glob(os.path.join(cube_dir, "part_*.parquet")))


def count_rows_in_parts(cube_dir):
    """Rows across the CONTIGUOUS prefix of complete parts (part_000, 001, ...).

    Parts are written strictly in order, each closed (durable) only when full, so
    valid resume state is the longest gap-free run of readable parts from index 0.
    Any incomplete/corrupt tail (or anything at/after the first bad index) is
    deleted so the writer cleanly re-creates from there. Returns (rows, good_parts).
    """
    total = 0
    good = []
    expected_idx = 0
    for p in cube_parts(cube_dir):
        # enforce contiguity by filename index
        base = os.path.basename(p)
        try:
            pidx = int(base[len("part_"):-len(".parquet")])
        except ValueError:
            pidx = -1
        ok = False
        if pidx == expected_idx:
            try:
                nr = pq.read_metadata(p).num_rows   # reads footer only, no open handle
                total += nr
                good.append(p)
                expected_idx += 1
                ok = True
            except Exception:
                ok = False
        if not ok:
            # bad/out-of-order/incomplete -> remove (and it ends the contiguous run)
            try:
                os.remove(p)
            except OSError:
                pass
    return total, good


def count_distinct_series(parquet_paths):
    """Stream series_key across one or more Parquet parts; count distinct.

    Bounded memory: only the series_key column is materialized, in batches."""
    if isinstance(parquet_paths, str):
        parquet_paths = [parquet_paths]
    seen = set()
    for p in parquet_paths:
        pf = pq.ParquetFile(p)
        for batch in pf.iter_batches(batch_size=500_000, columns=["series_key"]):
            seen.update(batch.column(0).to_pylist())
    return len(seen)


def group_for(filename):
    """Cube/group name from a bulk filename (strip qs. prefix, date, .txt.gz)."""
    base = filename
    for suf in (".txt.gz",):
        if base.endswith(suf):
            base = base[: -len(suf)]
    if base.startswith("qs."):
        base = base[3:]
    # strip trailing _YYYYMMDD date stamp on the 5 sector files
    parts = base.rsplit("_", 1)
    if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 8:
        base = parts[0]
    return base


def iter_rows(path):
    """Yield 39-field lists for one bulk file (skips malformed rows)."""
    with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as f:
        header = f.readline().rstrip("\n").rstrip("\r").replace("\x00", "").split("\t")
        idx = {c: i for i, c in enumerate(header)}
        for line in f:
            # NASS files carry occasional stray NUL bytes in empty value cells.
            r = line.rstrip("\n").rstrip("\r").replace("\x00", "").split("\t")
            if len(r) != EXPECTED_NCOLS:
                yield None, idx, r  # malformed marker
            else:
                yield r, idx, None


def make_series_key(r, idx):
    """Stable composite series id. Pipe-joined non-empty key columns + location."""
    parts = []
    for c in KEY_COLS:
        v = r[idx[c]].strip()
        parts.append(v)
    # A compact, deterministic key. SHORT_DESC + LOCATION_DESC + DOMAINCAT + SOURCE
    # already uniquely identifies a NASS series; we include the full tuple to be safe.
    return "usda:" + "|".join(parts)


def store_values(r, idx, series_key):
    out = [series_key]
    for c in STORE_COLS[1:]:
        out.append(r[idx[c]].strip())
    return out


def probe(path):
    print(f"PROBE {os.path.basename(path)}", flush=True)
    n = 0
    bad = 0
    series = set()
    freq = collections.Counter()
    val_flags = collections.Counter()
    nullc = 0
    minir = maxir = None
    bad_date = 0
    for r, idx, raw in iter_rows(path):
        if r is None:
            bad += 1
            continue
        n += 1
        sk = make_series_key(r, idx)
        series.add(sk)
        freq[r[idx["FREQ_DESC"]]] += 1
        val, flag = parse_value(r[idx["VALUE"]])
        if val is None:
            nullc += 1
            if flag:
                val_flags[flag] += 1
        d = parse_date(r[idx["YEAR"]], r[idx["FREQ_DESC"]], r[idx["REFERENCE_PERIOD_DESC"]],
                       r[idx["BEGIN_CODE"]], r[idx["END_CODE"]], r[idx["WEEK_ENDING"]])
        if d is None:
            bad_date += 1
        else:
            if minir is None or d < minir: minir = d
            if maxir is None or d > maxir: maxir = d
        if n >= 500000:
            print("  (probe sampled first 500k rows)", flush=True)
            break
    print(f"  rows={n:,} malformed={bad} unique_series={len(series):,} null_values={nullc:,} bad_dates={bad_date}")
    print(f"  date_range={minir}..{maxir}")
    print(f"  freq={dict(freq)}")
    print(f"  null_flags={val_flags.most_common(12)}")


def main():
    args = sys.argv[1:]
    if "--probe" in args:
        fn = args[args.index("--probe") + 1]
        probe(os.path.join(RAW, fn))
        return

    dry = "--dry" in args
    include_census = "--with-census" in args
    # The 5 SECTOR bulk files are the COMPLETE Quick Stats database:
    #   their record counts sum EXACTLY to the API get_counts total (57,629,841).
    # The 7 qs.censusYYYY[zipcode] files are an alternate slicing BY census year
    # of the SAME census observations already inside the sector files (verified:
    # every census2022 row is SOURCE_DESC=CENSUS,YEAR=2022 across all 5 sectors).
    # Ingesting them too would create exact (series_key, obs_date) DUPLICATES and
    # dishonestly inflate counts -> excluded by default.
    SECTOR_FILES = ["animals_products", "crops", "demographics", "economics", "environmental"]
    all_files = sorted(glob.glob(os.path.join(RAW, "qs.*.txt.gz")))
    if include_census:
        files = all_files
    else:
        files = [p for p in all_files
                 if group_for(os.path.basename(p)) in SECTOR_FILES]
    if not files:
        print("NO bulk files found in", RAW); return
    print(f"{'DRY' if dry else 'FULL'}: {len(files)} bulk cubes to ingest", flush=True)
    if not dry:
        os.makedirs(OUT, exist_ok=True)

    grand_obs = 0
    grand_series = 0
    manifest = []
    for path in files:
        grp = group_for(os.path.basename(path))
        cube_dir = os.path.join(OUT, grp)
        done_marker = os.path.join(cube_dir, "_complete")

        if not dry:
            os.makedirs(cube_dir, exist_ok=True)

        # Fully-done cube: a _complete sentinel means every input row is written.
        if not dry and os.path.exists(done_marker):
            parts = cube_parts(cube_dir)
            done_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in parts)
            ns = count_distinct_series(parts)
            print(f"  CUBE {grp:24} SKIP (complete) rows={done_rows:,} series={ns:,} "
                  f"parts={len(parts)}", flush=True)
            manifest.append((grp, done_rows, ns, None, None))
            grand_obs += done_rows
            grand_series += ns
            continue

        # Intra-cube resume: count rows already in COMPLETE parts, skip them.
        skip_rows = 0
        start_part = 0
        if not dry:
            skip_rows, good_parts = count_rows_in_parts(cube_dir)
            start_part = len(good_parts)
            if skip_rows:
                print(f"  CUBE {grp:24} RESUME from {skip_rows:,} rows "
                      f"({start_part} parts already written)", flush=True)

        gw = None if dry else PartWriter(cube_dir, start_part=start_part)
        n = 0          # rows processed from input this run
        written = 0    # rows actually written this run (after skip)
        bad = 0
        t0 = dt.datetime.now()
        for r, idx, raw in iter_rows(path):
            if r is None:
                bad += 1
                continue
            n += 1
            if not dry and n <= skip_rows:
                continue   # already persisted in a previous run
            sk = make_series_key(r, idx)
            val, flag = parse_value(r[idx["VALUE"]])
            d = parse_date(r[idx["YEAR"]], r[idx["FREQ_DESC"]], r[idx["REFERENCE_PERIOD_DESC"]],
                           r[idx["BEGIN_CODE"]], r[idx["END_CODE"]], r[idx["WEEK_ENDING"]])
            if not dry:
                gw.add(store_values(r, idx, sk), d, val, flag)
                written += 1
            if n % 5_000_000 == 0:
                el = (dt.datetime.now() - t0).total_seconds()
                print(f"  [{grp}] {n:,} rows ({el:.0f}s)", flush=True)
        if dry:
            print(f"  CUBE {grp:24} rows={n:,} malformed={bad}", flush=True)
            grand_obs += n
        else:
            gw.close()
            open(done_marker, "w").close()   # mark cube fully done
            parts = cube_parts(cube_dir)
            total_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in parts)
            ns = count_distinct_series(parts)
            sz = sum(os.path.getsize(p) for p in parts)
            print(f"  CUBE {grp:24} rows={total_rows:,} series={ns:,} parts={len(parts)} "
                  f"-> {sz/1e6:.1f}MB dates {gw.min_date}..{gw.max_date} malformed={bad}", flush=True)
            manifest.append((grp, total_rows, ns, str(gw.min_date), str(gw.max_date)))
            grand_obs += total_rows
            grand_series += ns

    print(f"\n{'DRY' if dry else 'DONE'}: {len(files)} cubes / {grand_series:,} series / {grand_obs:,} observation rows", flush=True)
    if not dry:
        import json
        with open(os.path.join(OUT, "_manifest.json"), "w") as fh:
            json.dump({"source_id": "usda", "license_id": LICENSE_ID,
                       "cubes": manifest, "total_rows": grand_obs,
                       "total_series": grand_series}, fh, indent=2)
        print("manifest ->", os.path.join(OUT, "_manifest.json"))


if __name__ == "__main__":
    main()
