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

Re-ingest is ATOMIC per cube (see the banner above main()): a cube whose live
_build.json does not name the current dump (fname + size) is rebuilt in
data/usda_staging/<cube>.new, verified, and swapped in whole; a reader never sees a
mix of two dumps, and a kill at any step leaves one complete live cube.

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
      * gives crash-resilient checkpoints (a killed STAGED build resumes from its
        last completed part, only when its _build.json names the same input --
        see _build_cube),
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

    STAGING ONLY. This os.remove()s files, and the parts it counts are only a valid resume
    point when they came from the SAME input - so it is called solely on a staging dir whose
    _build.json names the current input (_build_cube), NEVER on a live cube.

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


# ---------------------------------------------------------------------------
# ATOMIC RE-INGEST (2026-11-19). The cube a reader sees is ALWAYS one whole parse of
# ONE input file.
#
# WHAT WAS WRONG. A cube without `_complete` was "resumed" by counting the rows in the
# LIVE cube's parts (count_rows_in_parts) and skipping that many rows of whatever dump
# was on disk. Nothing recorded which input those parts came from, so when the fetcher
# cleared `_complete` for a NEW vintage the old parts were kept and only the new file's
# rows past the old row count were appended; a new file that was not longer wrote
# nothing at all - and `_complete` was stamped anyway. The live store after the
# 2026-10-02 'ok' run proves it: demographics holds 10 parts all dated 2026-06-03 under
# a `_complete` dated 2026-10-02.
#
# HOW IT WORKS NOW.
#   * A rebuild writes into STAGING, data/usda_staging/<cube>.new - OUTSIDE
#     data/clean_full, so no reader that globs clean_full/usda/**/*.parquet (the
#     resolver, the derives, the fetcher's publish) ever sees a second copy of a cube.
#     Same volume as the store, so the swap is two directory renames.
#   * The staging dir carries _build.json naming the input (fname + size). A resume is
#     allowed ONLY inside that dir and ONLY when _build.json names the current input;
#     anything else wipes the staging dir and starts over. The LIVE cube's parts are
#     never counted, never resumed from and never deleted by the resume logic.
#   * Verify before swap: every part footer reads, parts are contiguous from part_000,
#     sum(part rows) == rows parsed from the input (malformed rows excluded).
#   * Commit is INSIDE .new before anything moves: _build.json gets verified=true and
#     _complete is written there. Then: live -> staging/<cube>.old, staging/<cube>.new
#     -> live (THE COMMIT POINT), re-verify the live cube, and only then drop .old (the
#     fetcher drops it only after its publish - keep_old=True). Each rename asserts its
#     target is absent first and is a single os.rename: NOT core.atomic's retry loop,
#     which reads a PermissionError as a reader lock - for a directory that is an open
#     handle somewhere inside it (NTFS WinError 5/32) and it will not clear by waiting.
#     A failed rename leaves the old cube in place (restoring it if it had moved) and
#     raises.
#   * recover_cube() runs first for every cube, so a kill at ANY step leaves - and the
#     next run finds - exactly one complete, servable live cube (table in its docstring).
#   * The whole rebuild holds the `usda_writer` lock (updater/writer_lock.py), which the
#     usda CSV derives and the orchestrator's retry drain respect.
# ---------------------------------------------------------------------------
import json
import shutil

BUILD_FILE = "_build.json"     # {"input": {fname, size}, "verified": bool, "rows", "parts"}
DONE_FILE = "_complete"
NEW_SUFFIX = ".new"
OLD_SUFFIX = ".old"
LOCK_NAME = "usda_writer"
SECTOR_FILES = ["animals_products", "crops", "demographics", "economics", "environmental"]


class IngestRefused(RuntimeError):
    """A cube could not be built, verified or swapped. The live cube is left as it was."""


def staging_root() -> str:
    """data/usda_staging: OUTSIDE data/clean_full (every usda reader globs under it), on the
    same volume so a swap is a rename. Never named `_live` (that is the freeze-and-forward
    live half, econdl._resolve.live_sibling)."""
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(OUT))), "usda_staging")


def _new_dir(grp):
    return os.path.join(staging_root(), grp + NEW_SUFFIX)


def _old_dir(grp):
    return os.path.join(staging_root(), grp + OLD_SUFFIX)


def _crash_point(name: str) -> None:
    """No-op hook between swap steps; tests raise here to simulate a kill at each step."""


def input_id(path) -> dict:
    return {"fname": os.path.basename(path), "size": os.path.getsize(path)}


def read_build(d):
    try:
        with open(os.path.join(d, BUILD_FILE), encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _write_json_atomic(path, obj):
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def verify_dir(d, expect_input=None):
    """(ok, why, rows) for a cube directory. READ-ONLY - never removes a file.

    ok means: _build.json says verified, names expect_input (when given), the part files
    are exactly its `parts` list, contiguous from part_000, every footer reads, and their
    row sum equals its `rows`."""
    b = read_build(d)
    if not b or not b.get("verified"):
        return False, "no verified _build.json", 0
    if expect_input is not None and b.get("input") != expect_input:
        return False, f"built from {b.get('input')}, input is {expect_input}", 0
    parts = cube_parts(d)
    names = [os.path.basename(p) for p in parts]
    if names != [f"part_{i:03d}.parquet" for i in range(len(names))]:
        return False, f"parts not contiguous from part_000: {names[:5]}...", 0
    if names != list(b.get("parts") or []):
        return False, f"parts {len(names)} on disk != {len(b.get('parts') or [])} in _build.json", 0
    rows = 0
    for p in parts:
        try:
            rows += pq.read_metadata(p).num_rows
        except Exception as e:                                   # noqa: BLE001
            return False, f"footer of {os.path.basename(p)} unreadable: {e!r}", 0
    if rows != b.get("rows"):
        return False, f"part rows {rows:,} != _build.json rows {b.get('rows')}", 0
    return True, "ok", rows


def _rename_dir(src, dst):
    """One directory rename, its target asserted absent. No retry (see the banner)."""
    if os.path.lexists(dst):
        raise IngestRefused(f"refusing to rename {src} -> {dst}: the target exists")
    os.rename(src, dst)


def recover_cube(grp) -> str:
    """Bring one cube back to a single servable live copy after any earlier kill.

    live = clean_full/usda/<cube>, old/new = data/usda_staging/<cube>.old/.new
      live + old            -> verify live; ok: remove old. not ok: refuse (both kept).
      old + committed new   -> rename new -> live (finish the swap), then as above.
      old only (no live)    -> rename old -> live (roll back).
      new without old       -> left for _build_cube: resumed only if it names the input.
    Returns what it did (for the log)."""
    live = os.path.join(OUT, grp)
    old, new = _old_dir(grp), _new_dir(grp)
    if not os.path.isdir(old):
        return "nothing to recover"
    if not os.path.isdir(live):
        ok_new = os.path.isdir(new) and verify_dir(new)[0] \
            and os.path.exists(os.path.join(new, DONE_FILE))
        if ok_new:
            _rename_dir(new, live)
            did = "finished an interrupted swap (committed .new -> live)"
        else:
            _rename_dir(old, live)
            return "rolled back an interrupted swap (.old -> live)"
    else:
        did = "found a leftover .old beside a live cube"
    ok, why, _ = verify_dir(live)
    if not ok:
        raise IngestRefused(f"{grp}: live cube does not verify ({why}) while {old} exists - "
                            f"both kept for an operator; nothing removed")
    shutil.rmtree(old)
    return did + "; live verified, .old removed"


def drop_old(grp) -> bool:
    """Remove staging/<cube>.old once the live cube re-verifies. True when removed."""
    old = _old_dir(grp)
    if not os.path.isdir(old):
        return False
    ok, why, _ = verify_dir(os.path.join(OUT, grp))
    if not ok:
        raise IngestRefused(f"{grp}: not removing {old}: the live cube does not verify ({why})")
    shutil.rmtree(old)
    return True


def drop_all_old() -> list:
    """drop_old for every cube that has a .old (the fetcher calls it after its publish)."""
    out = []
    root = staging_root()
    if os.path.isdir(root):
        for n in sorted(os.listdir(root)):
            if n.endswith(OLD_SUFFIX) and drop_old(n[: -len(OLD_SUFFIX)]):
                out.append(n[: -len(OLD_SUFFIX)])
    return out


def _build_cube(grp, path):
    """Parse `path` into staging/<cube>.new (resuming only a matching build), verify it,
    and commit it there. Returns (rows, malformed, min_date, max_date)."""
    new = _new_dir(grp)
    inp = input_id(path)
    b = read_build(new) if os.path.isdir(new) else None
    if b is not None and b.get("input") == inp:
        if b.get("verified") and verify_dir(new, inp)[0]:
            print(f"  CUBE {grp:24} staged build already verified - swapping it", flush=True)
            return b["rows"], b.get("malformed", 0), b.get("min_date"), b.get("max_date")
        # Resume INSIDE the staging dir only. count_rows_in_parts may delete a torn tail
        # part here - it is our own scratch, never the live cube.
        skip_rows, good = count_rows_in_parts(new)
        try:
            os.remove(os.path.join(new, DONE_FILE))
        except FileNotFoundError:
            pass
        print(f"  CUBE {grp:24} RESUME staged build of {inp['fname']} from {skip_rows:,} rows "
              f"({len(good)} parts)", flush=True)
    else:
        if os.path.isdir(new):
            print(f"  CUBE {grp:24} staged build is for {b and b.get('input')} - wiping it",
                  flush=True)
            shutil.rmtree(new)
        os.makedirs(new)
        _write_json_atomic(os.path.join(new, BUILD_FILE), {"input": inp, "verified": False})
        skip_rows, good = 0, []

    gw = PartWriter(new, start_part=len(good))
    n = bad = 0
    t0 = dt.datetime.now()
    for r, idx, raw in iter_rows(path):
        if r is None:
            bad += 1
            continue
        n += 1
        if n <= skip_rows:
            continue
        sk = make_series_key(r, idx)
        val, flag = parse_value(r[idx["VALUE"]])
        d = parse_date(r[idx["YEAR"]], r[idx["FREQ_DESC"]], r[idx["REFERENCE_PERIOD_DESC"]],
                       r[idx["BEGIN_CODE"]], r[idx["END_CODE"]], r[idx["WEEK_ENDING"]])
        gw.add(store_values(r, idx, sk), d, val, flag)
        if n % 5_000_000 == 0:
            el = (dt.datetime.now() - t0).total_seconds()
            print(f"  [{grp}] {n:,} rows ({el:.0f}s)", flush=True)
    gw.close()

    # VERIFY BEFORE ANYTHING MOVES.
    if n == 0:
        shutil.rmtree(new)
        raise IngestRefused(f"{grp}: {inp['fname']} parsed to 0 rows - refusing to replace the cube")
    parts = cube_parts(new)
    names = [os.path.basename(p) for p in parts]
    got = 0
    try:
        for p in parts:
            got += pq.read_metadata(p).num_rows
    except Exception as e:                                       # noqa: BLE001
        shutil.rmtree(new)
        raise IngestRefused(f"{grp}: a staged part footer is unreadable ({e!r}); staging wiped, "
                            f"live cube untouched") from e
    if got != n or names != [f"part_{i:03d}.parquet" for i in range(len(names))]:
        shutil.rmtree(new)
        raise IngestRefused(f"{grp}: staged parts hold {got:,} rows in {len(names)} part(s); the "
                            f"input parsed to {n:,} (+{bad} malformed). Staging wiped, live cube "
                            f"untouched")
    mn, mx = footer_dates(parts)
    _write_json_atomic(os.path.join(new, BUILD_FILE), {
        "input": inp, "verified": True, "rows": n, "malformed": bad, "parts": names,
        "min_date": mn, "max_date": mx,
        "built_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")})
    open(os.path.join(new, DONE_FILE), "w").close()        # committed: the next rename publishes
    return n, bad, mn, mx


def swap_in(grp, keep_old=False):
    """staging/<cube>.new (committed) -> live. Old live -> staging/<cube>.old, dropped after the
    swapped cube re-verifies unless keep_old. A failed rename leaves the old cube live."""
    live, new, old = os.path.join(OUT, grp), _new_dir(grp), _old_dir(grp)
    ok, why, _ = verify_dir(new)
    if not ok or not os.path.exists(os.path.join(new, DONE_FILE)):
        raise IngestRefused(f"{grp}: refusing to swap an uncommitted staging dir ({why})")
    os.makedirs(staging_root(), exist_ok=True)
    moved = False
    if os.path.isdir(live):
        try:
            _rename_dir(live, old)
        except OSError as e:
            raise IngestRefused(f"{grp}: could not move the live cube aside ({e!r}) - an open "
                                f"handle inside it? Old cube still live, staged build kept") from e
        moved = True
    _crash_point("after_live_to_old")
    try:
        _rename_dir(new, live)                                  # THE COMMIT POINT
    except OSError as e:
        if moved:
            try:
                _rename_dir(old, live)
            except OSError as e2:
                raise IngestRefused(f"{grp}: swap failed ({e!r}) AND the restore failed ({e2!r}); "
                                    f"the old cube is at {old} - the next run's recovery renames it "
                                    f"back") from e2
        raise IngestRefused(f"{grp}: could not move the staged build live ({e!r}); old cube "
                            f"restored") from e
    _crash_point("after_new_to_live")
    ok, why, _ = verify_dir(live)
    if not ok:
        raise IngestRefused(f"{grp}: swapped cube does not re-verify ({why}); {old} kept")
    if not keep_old:
        _crash_point("before_drop_old")
        drop_old(grp)


def footer_dates(parts):
    """(min, max) obs_date as ISO strings from the parquet column statistics (footers only)."""
    mn = mx = None
    for p in parts:
        md = pq.read_metadata(p)
        try:
            ci = md.schema.names.index("obs_date")
        except ValueError:
            continue
        for g in range(md.num_row_groups):
            st = md.row_group(g).column(ci).statistics
            if st is None or not st.has_min_max:
                continue
            lo, hi = st.min, st.max
            mn = lo if mn is None or lo < mn else mn
            mx = hi if mx is None or hi > mx else mx
    return (str(mn) if mn is not None else None, str(mx) if mx is not None else None)


def served_parts() -> list:
    """Every part of every LIVE cube (clean_full/usda/<cube>/part_NNN.parquet), sorted -
    the exact set the fetcher publishes. Staging is not under OUT, so never included."""
    out = []
    if os.path.isdir(OUT):
        for n in sorted(os.listdir(OUT)):
            d = os.path.join(OUT, n)
            if os.path.isdir(d) and "." not in n:
                out += cube_parts(d)
    return out


def committed_inputs() -> dict:
    """{cube: input fname} for every live cube whose _build.json verifies. For the vintage
    stamp (PR #300 follow-up): stamp only when this names exactly the five listing files."""
    out = {}
    for grp in SECTOR_FILES:
        d = os.path.join(OUT, grp)
        ok, _why, _ = verify_dir(d)
        if ok:
            out[grp] = read_build(d)["input"]["fname"]
    return out


def all_committed(listing_fnames) -> bool:
    """True only when the five live cubes are verified builds of exactly `listing_fnames`
    (an iterable of the five qs.<sector>_YYYYMMDD.txt.gz names)."""
    want = {group_for(f): f for f in listing_fnames}
    return set(want) == set(SECTOR_FILES) and committed_inputs() == want


def write_manifest(cubes):
    """_manifest.json from the LIVE cubes' footers, atomically. Leaves every other root file
    (_provider.json, _rotation.json) alone."""
    rows_list, detail = [], {}
    grand_obs = grand_series = 0
    for grp in cubes:
        d = os.path.join(OUT, grp)
        parts = cube_parts(d)
        rows = sum(pq.read_metadata(p).num_rows for p in parts)
        ns = count_distinct_series(parts) if parts else 0
        mn, mx = footer_dates(parts)
        b = read_build(d) or {}
        rows_list.append((grp, rows, ns, mn, mx))
        detail[grp] = {"rows": rows, "series": ns, "min_date": mn, "max_date": mx,
                       "parts": [os.path.basename(p) for p in parts],
                       "input": b.get("input"), "verified": bool(b.get("verified"))}
        grand_obs += rows
        grand_series += ns
    path = os.path.join(OUT, "_manifest.json")
    _write_json_atomic(path, {"source_id": "usda", "license_id": LICENSE_ID,
                              "cubes": rows_list, "cube_detail": detail,
                              "total_rows": grand_obs, "total_series": grand_series})
    return path, grand_obs, grand_series


def main(keep_old: bool = False):
    """keep_old: leave staging/<cube>.old after a swap (the fetcher passes True and calls
    drop_all_old() only after its publish)."""
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
    all_files = sorted(glob.glob(os.path.join(RAW, "qs.*.txt.gz")))
    if include_census:
        files = all_files
    else:
        files = [p for p in all_files
                 if group_for(os.path.basename(p)) in SECTOR_FILES]
    if not files:
        print("NO bulk files found in", RAW); return
    print(f"{'DRY' if dry else 'FULL'}: {len(files)} bulk cubes to ingest", flush=True)

    if dry:
        grand = 0
        for path in files:
            grp = group_for(os.path.basename(path))
            n = bad = 0
            for r, _idx, _raw in iter_rows(path):
                if r is None:
                    bad += 1
                else:
                    n += 1
            print(f"  CUBE {grp:24} rows={n:,} malformed={bad}", flush=True)
            grand += n
        print(f"\nDRY: {len(files)} cubes / {grand:,} observation rows", flush=True)
        return

    from updater import writer_lock                                  # noqa: PLC0415
    if not writer_lock.acquire(LOCK_NAME, what="jobs/ingest_usda.py (cube rebuild)"):
        raise IngestRefused(f"usda: {writer_lock.lock_path(LOCK_NAME)} is held by "
                            f"{writer_lock.owner(LOCK_NAME)} - another usda writer is running")
    try:
        os.makedirs(OUT, exist_ok=True)
        cubes = []
        for path in files:
            grp = group_for(os.path.basename(path))
            cubes.append(grp)
            live = os.path.join(OUT, grp)
            did = recover_cube(grp)
            if did != "nothing to recover":
                print(f"  CUBE {grp:24} RECOVERY: {did}", flush=True)
            inp = input_id(path)
            ok, why, rows = verify_dir(live, inp)
            if ok:
                # The fetcher may have cleared _complete for a dump it re-downloaded; the build
                # is verified against the SAME fname+size, so the cube is current. Restore it.
                if not os.path.exists(os.path.join(live, DONE_FILE)):
                    open(os.path.join(live, DONE_FILE), "w").close()
                if os.path.isdir(_new_dir(grp)):
                    shutil.rmtree(_new_dir(grp))         # a stray staged build nobody needs
                print(f"  CUBE {grp:24} SKIP (verified build of {inp['fname']}) rows={rows:,}",
                      flush=True)
                continue
            print(f"  CUBE {grp:24} REBUILD from {inp['fname']} ({why})", flush=True)
            n, bad, mn, mx = _build_cube(grp, path)
            swap_in(grp, keep_old=keep_old)
            print(f"  CUBE {grp:24} rows={n:,} malformed={bad} dates {mn}..{mx} -> swapped live",
                  flush=True)
        mpath, grand_obs, grand_series = write_manifest(cubes)
        print(f"\nDONE: {len(cubes)} cubes / {grand_series:,} series / {grand_obs:,} observation rows",
              flush=True)
        print("manifest ->", mpath)
    finally:
        writer_lock.release(LOCK_NAME)


if __name__ == "__main__":
    main()
