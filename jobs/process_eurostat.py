#!/usr/bin/env python3
r"""Turn the downloaded Eurostat bulk dumps into series + observations.

Reads the gzipped SDMX-TSV files crawled by jobs/fetch_eurostat_bulk.py
(data/raw/eurostat/*.tsv.gz, ~7,600 datasets) and emits, per data row, one
series shaped exactly like connectors/base.py expects:

    SeriesMeta(series_id="eurostat:<dataset_code>:<dim1.dim2...>", ...)
    [Observation(series_id, obs_date, value, flags=(...)), ...]

Eurostat SDMX-TSV layout (one file = one dataset):

    freq,unit,geo\TIME_PERIOD <tab> 2018 <tab> 2019 <tab> 2020 ...
    A,PC,BE                   <tab> 57.8 <tab> 58.1 <tab> :   ...
    A,PC,BG                   <tab> 69.5 <tab> 7 p  <tab> ...

  * The first header cell is the comma-joined dimension list, a backslash, then
    the literal text TIME_PERIOD  (e.g. "freq,unit,geo\TIME_PERIOD").
  * The remaining header cells are time periods (each with a trailing space):
    "2020", "2020-Q1", "2020-01", "2020-S1".
  * Each data row starts with the comma-joined dimension VALUES (same order as
    the header dimension names), then tab-separated value cells.
  * A value cell is  "<value>[ <flags>]"  or  ":"  (missing). The flag is a
    space then one or more letters (e.g. " p", " bd", " @C"). Missing cells may
    ALSO carry flags (": b", ": @N"). A handful of datasets store non-numeric
    values (HH:MM durations like "0:42" in tus_*, category labels like "high"
    in inn_*); those are kept (value=None, raw token preserved in flags) rather
    than dropped, so no information is lost.

This is a PROCESSOR / pretty-printer for the bulk dumps, NOT a live connector.
It deliberately does NOT write data/catalog.db or data/clean/ -- it only parses
the raw files and (in smoke-test mode) prints what it found. The ingest path
(jobs/run_connector.py) is what persists series; this module exposes
iter_series() so that path can consume Eurostat the same way it consumes any
other connector.

Source: Eurostat. License: CC BY 4.0 (license_id "cc-by-4.0"). Non-EU / certain
trade carve-outs are applied later at serve time, not here.

Usage:
    python jobs/process_eurostat.py --smoke-test      # ~3 small files, verbose
    python jobs/process_eurostat.py --smoke-test -n 5 # n smallest files
    python jobs/process_eurostat.py --file teibs070   # one dataset by code
    python jobs/process_eurostat.py --all             # parse every file (counts only)
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import gzip
import os
import re
import sys
from typing import Iterable, Iterator, Optional

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import SeriesMeta, Observation  # noqa: E402

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
RAW_DIR = os.path.join(ROOT, "data", "raw", "eurostat")
LICENSE_ID = "cc-by-4.0"
SOURCE_ID = "eurostat"
ATTRIBUTION = "Source: Eurostat (CC BY 4.0)"
HOMEPAGE = "https://ec.europa.eu/eurostat"

# A value cell is "<token>[ <flags>]"; ":" alone (token) means missing.
MISSING = ":"

# Period -> frequency letter inferred from the first parseable period in a file.
_PERIOD_QUARTER = re.compile(r"^(\d{4})-?Q([1-4])$", re.IGNORECASE)
_PERIOD_SEMESTER = re.compile(r"^(\d{4})-?S([1-2])$", re.IGNORECASE)
_PERIOD_MONTH = re.compile(r"^(\d{4})-(\d{1,2})$")
_PERIOD_DAY = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$")
_PERIOD_WEEK = re.compile(r"^(\d{4})-?W(\d{1,2})$", re.IGNORECASE)
_PERIOD_YEAR = re.compile(r"^(\d{4})$")


# --------------------------------------------------------------------------- #
# Period / value parsing
# --------------------------------------------------------------------------- #
def parse_period(token: str) -> tuple[Optional[dt.date], Optional[str]]:
    """Parse an SDMX-TSV TIME_PERIOD token to (date, frequency-letter).

    Eurostat dates a period by its FIRST day (quarter -> first month-1st,
    semester -> Jan-1/Jul-1, month -> 1st, year -> Jan-1), matching the ECB
    connector's convention so series from different sources line up.

    Returns (None, None) if the token is not a recognized period.
    """
    t = token.strip()
    if not t:
        return None, None

    m = _PERIOD_YEAR.match(t)
    if m:
        try:
            return dt.date(int(m.group(1)), 1, 1), "A"
        except ValueError:
            return None, None

    m = _PERIOD_QUARTER.match(t)
    if m:
        year, q = int(m.group(1)), int(m.group(2))
        return dt.date(year, (q - 1) * 3 + 1, 1), "Q"

    m = _PERIOD_SEMESTER.match(t)
    if m:
        year, s = int(m.group(1)), int(m.group(2))
        return dt.date(year, 1 if s == 1 else 7, 1), "S"

    m = _PERIOD_DAY.match(t)  # check day before month (more specific)
    if m:
        try:
            return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3))), "D"
        except ValueError:
            return None, None

    m = _PERIOD_MONTH.match(t)
    if m:
        month = int(m.group(2))
        if 1 <= month <= 12:
            try:
                return dt.date(int(m.group(1)), month, 1), "M"
            except ValueError:
                return None, None
        return None, None

    m = _PERIOD_WEEK.match(t)
    if m:
        year, week = int(m.group(1)), int(m.group(2))
        if 1 <= week <= 53:
            try:
                # ISO week -> Monday of that week
                return dt.date.fromisocalendar(year, week, 1), "W"
            except ValueError:
                return None, None
        return None, None

    return None, None


def parse_cell(cell: str) -> tuple[Optional[float], tuple[str, ...]]:
    """Parse one value cell -> (value, flags).

    Cell shapes:
      "57.8"      -> (57.8, ())
      "7 p"       -> (7.0, ("p",))           value + status flag(s)
      "12 bd"     -> (12.0, ("bd",))         concatenated flag letters
      ":"         -> (None, ())              missing
      ": b"       -> (None, ("b",))          missing WITH a flag
      "0:42 u"    -> (None, ("u", "raw=0:42"))   non-numeric (kept, not dropped)
      "high"      -> (None, ("raw=high",))   categorical value (kept)
      ""          -> (None, ())              empty
    """
    s = cell.strip()
    if not s:
        return None, ()

    parts = s.split(None, 1)            # split on first run of whitespace
    token = parts[0]
    flags: list[str] = []
    if len(parts) > 1 and parts[1].strip():
        flags.append(parts[1].strip())  # the flag block (e.g. "p", "bd", "@C")

    if token == MISSING:
        return None, tuple(flags)

    try:
        return float(token), tuple(flags)
    except ValueError:
        # Non-numeric value (HH:MM duration, category label, ...). Keep the raw
        # token so the observation isn't silently lost; value stays None.
        flags.append(f"raw={token}")
        return None, tuple(flags)


# --------------------------------------------------------------------------- #
# Dataset parsing
# --------------------------------------------------------------------------- #
def dataset_code(path: str) -> str:
    """'.../teibs070.tsv.gz' -> 'teibs070'."""
    name = os.path.basename(path)
    for suffix in (".tsv.gz", ".tsv"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return os.path.splitext(name)[0]


def _parse_header(header_line: str) -> tuple[list[str], list[str]]:
    """Split the SDMX-TSV header into (dimension names, raw period tokens).

    First cell: "freq,unit,geo\\TIME_PERIOD"  -> dims=[freq,unit,geo].
    Remaining cells: the period columns (with their trailing spaces stripped).
    """
    cells = header_line.rstrip("\n").rstrip("\r").split("\t")
    first = cells[0]
    # The dimension list is everything before the backslash; "TIME_PERIOD" after.
    dim_part = first.split("\\", 1)[0]
    dims = [d.strip() for d in dim_part.split(",") if d.strip()]
    periods = [c.strip() for c in cells[1:]]
    return dims, periods


def iter_dataset(path: str) -> Iterator[tuple[SeriesMeta, list[Observation]]]:
    """Parse one Eurostat TSV.gz into (SeriesMeta, observations) per data row.

    Series with zero parseable observations are skipped (an all-":" row carries
    no data). Frequency is inferred from the dataset's first dimension value
    when it is the standard 'freq' code (A/Q/M/S/W/D), else from the periods.
    """
    code = dataset_code(path)
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8", newline="") as f:
        header_line = f.readline()
        if not header_line:
            return
        dims, period_tokens = _parse_header(header_line)

        # Pre-parse the period columns once (shared by every row in the file).
        col_dates: list[Optional[dt.date]] = []
        col_freqs: list[Optional[str]] = []
        for tok in period_tokens:
            d, fr = parse_period(tok)
            col_dates.append(d)
            col_freqs.append(fr)
        # File-level frequency hint from the period columns (most common).
        period_freq = _dominant(col_freqs)
        freq_dim_idx = dims.index("freq") if "freq" in dims else (
            dims.index("FREQ") if "FREQ" in dims else None)

        for raw in f:
            if not raw.strip():
                continue
            cells = raw.rstrip("\n").rstrip("\r").split("\t")
            dim_values = [v.strip() for v in cells[0].split(",")]
            value_cells = cells[1:]

            sid = f"eurostat:{code}:{'.'.join(dim_values)}"
            obs: list[Observation] = []
            for i, vc in enumerate(value_cells):
                if i >= len(col_dates):
                    break                       # ragged row: no header period
                d = col_dates[i]
                if d is None:
                    continue                    # unparseable period column
                value, flags = parse_cell(vc)
                if value is None and not flags:
                    continue                    # plain missing, nothing to keep
                obs.append(Observation(sid, d, value, version="raw", flags=flags))

            if not obs:
                continue

            obs.sort(key=lambda o: o.obs_date)

            # Frequency: prefer the explicit freq dimension value (A/Q/M/...),
            # fall back to what the period tokens implied.
            freq = period_freq or "irregular"
            if freq_dim_idx is not None and freq_dim_idx < len(dim_values):
                fv = dim_values[freq_dim_idx].upper()
                if fv in ("A", "Q", "M", "S", "W", "D"):
                    freq = fv

            unit = _dim_lookup(dims, dim_values, ("unit", "UNIT"))
            geo = _dim_lookup(dims, dim_values, ("geo", "GEO"))
            meta = SeriesMeta(
                series_id=sid,
                title=f"Eurostat {code}: {', '.join(dim_values)}",
                frequency=freq,
                unit=unit,
                geography=geo,
                category=code.split("_")[0],     # rough dataset family
                license_id=LICENSE_ID,
                metadata={
                    "dataset": code,
                    "dimensions": dict(zip(dims, dim_values)),
                    "provider": "Eurostat",
                },
            )
            yield meta, obs


def _dim_lookup(dims: list[str], values: list[str], names: tuple[str, ...]) -> Optional[str]:
    """Return the dimension value for the first matching dimension name."""
    for n in names:
        if n in dims:
            idx = dims.index(n)
            if idx < len(values):
                return values[idx]
    return None


def _dominant(items: list[Optional[str]]) -> Optional[str]:
    """Most-common non-None entry (used to guess a file's period frequency)."""
    counts: dict[str, int] = {}
    for it in items:
        if it:
            counts[it] = counts.get(it, 0) + 1
    if not counts:
        return None
    return max(counts, key=counts.get)


# --------------------------------------------------------------------------- #
# Public iterator over the whole bulk dump
# --------------------------------------------------------------------------- #
def iter_series(paths: Optional[Iterable[str]] = None
                ) -> Iterator[tuple[SeriesMeta, list[Observation]]]:
    """Yield (SeriesMeta, observations) for every series across the given files.

    Defaults to every *.tsv.gz in data/raw/eurostat. Files that fail to parse
    are skipped with a warning rather than aborting the whole run.
    """
    if paths is None:
        paths = sorted(glob.glob(os.path.join(RAW_DIR, "*.tsv.gz")))
    for path in paths:
        try:
            yield from iter_dataset(path)
        except Exception as exc:                # noqa: BLE001
            print(f"  WARN  {os.path.basename(path)}: {exc}", file=sys.stderr, flush=True)


# --------------------------------------------------------------------------- #
# CLI: smoke test + bulk counting
# --------------------------------------------------------------------------- #
def _smallest_files(n: int) -> list[str]:
    files = glob.glob(os.path.join(RAW_DIR, "*.tsv.gz"))
    files = [f for f in files if os.path.getsize(f) > 0]
    files.sort(key=os.path.getsize)
    return files[:n]


def _resolve_file(code_or_path: str) -> str:
    if os.path.isfile(code_or_path):
        return code_or_path
    cand = os.path.join(RAW_DIR, code_or_path)
    if os.path.isfile(cand):
        return cand
    cand = os.path.join(RAW_DIR, code_or_path + ".tsv.gz")
    if os.path.isfile(cand):
        return cand
    raise SystemExit(f"no such dataset/file: {code_or_path!r}")


def smoke_test(paths: list[str]) -> None:
    """Parse a few files and print dataset, #series, #obs, and a sample point."""
    print(f"SMOKE TEST -- {len(paths)} file(s) from {RAW_DIR}\n")
    grand_series = grand_obs = 0
    for path in paths:
        code = dataset_code(path)
        size = os.path.getsize(path)
        n_series = n_obs = 0
        sample: Optional[tuple[SeriesMeta, Observation]] = None
        for meta, obs in iter_dataset(path):
            n_series += 1
            n_obs += len(obs)
            if sample is None:
                # pick the first observation that actually has a numeric value
                pt = next((o for o in obs if o.value is not None), obs[0])
                sample = (meta, pt)
        grand_series += n_series
        grand_obs += n_obs
        print(f"dataset : {code}   ({size:,} bytes gz)")
        print(f"  series: {n_series:,}")
        print(f"  obs   : {n_obs:,}")
        if sample is not None:
            meta, pt = sample
            print(f"  sample series id : {meta.series_id}")
            print(f"         frequency : {meta.frequency}   unit={meta.unit}   geo={meta.geography}")
            print(f"         dimensions: {meta.metadata['dimensions']}")
            print(f"  sample point     : {pt.obs_date.isoformat()}  value={pt.value}"
                  f"  flags={pt.flags}  version={pt.version}")
        else:
            print("  (no parseable series)")
        print()
    print(f"TOTAL across {len(paths)} file(s): {grand_series:,} series / {grand_obs:,} observations")


def count_all(paths: list[str]) -> None:
    """Parse every file; print rolling counts (no persistence)."""
    print(f"Parsing {len(paths):,} Eurostat files from {RAW_DIR}")
    n_datasets = n_series = n_obs = n_fail = 0
    for path in paths:
        n_datasets += 1
        try:
            for _meta, obs in iter_dataset(path):
                n_series += 1
                n_obs += len(obs)
        except Exception as exc:                # noqa: BLE001
            n_fail += 1
            print(f"  WARN  {os.path.basename(path)}: {exc}", file=sys.stderr)
        if n_datasets % 250 == 0:
            print(f"  {n_datasets:,}/{len(paths):,} files  "
                  f"{n_series:,} series  {n_obs:,} obs  ({n_fail} failed)", flush=True)
    print(f"DONE  {n_datasets:,} files  {n_series:,} series  {n_obs:,} obs  ({n_fail} failed)")


def main(argv: Optional[list[str]] = None) -> None:
    ap = argparse.ArgumentParser(description="Process Eurostat bulk TSVs into series.")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--smoke-test", action="store_true",
                   help="parse the n smallest files and print a verbose summary")
    g.add_argument("--file", metavar="CODE",
                   help="parse a single dataset by code (or path) and print its summary")
    g.add_argument("--all", action="store_true",
                   help="parse every file and print counts (does NOT persist)")
    ap.add_argument("-n", type=int, default=3,
                    help="number of files for --smoke-test (default 3)")
    args = ap.parse_args(argv)

    if not os.path.isdir(RAW_DIR):
        raise SystemExit(f"raw dir not found: {RAW_DIR}")

    if args.file:
        smoke_test([_resolve_file(args.file)])
    elif args.all:
        count_all(sorted(glob.glob(os.path.join(RAW_DIR, "*.tsv.gz"))))
    else:  # default to smoke test
        files = _smallest_files(args.n)
        if not files:
            raise SystemExit(f"no *.tsv.gz files in {RAW_DIR}")
        smoke_test(files)


if __name__ == "__main__":
    main()
