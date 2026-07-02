#!/usr/bin/env python3
"""FULL-COVERAGE ingest of the U.S. BLS flat files (time.series database).

Source: https://download.bls.gov/pub/time.series/  -- one folder per survey
(ap, ce, cu, ln, jt, wp, pr, la, ...). For each survey we download every
`<survey>.data.N.*` file (the observations) plus `<survey>.series` (series
metadata) and parse ALL series.

GROUPED storage (mirrors jobs/ingest_eurostat.py / ingest_sec_edgar.py):
ONE Parquet per survey -> data/clean_full/bls/<survey>.parquet with a
`series_id` column inside. Columns: series_id, obs_date, value, period, footnote.
Memory is bounded: data files are streamed to disk, then parsed line-by-line and
flushed to the Parquet writer in row-group batches (never the whole survey in RAM).

License: us-public-domain (configs/sources.yaml).

BLS data-file row (tab-separated, has a header line):
    series_id <tab> year <tab> period <tab> value <tab> footnote_codes
period codes: M01..M12 monthly, M13 annual avg, Q01..Q04 quarterly, Q05 annual
avg, S01/S02 semiannual, S03 semiannual annual avg, A01 annual.

Usage:
  python jobs/ingest_bls.py --probe          # validate parser on small surveys
  python jobs/ingest_bls.py --only pr,jl     # one/few surveys
  python jobs/ingest_bls.py                   # full run (all 67 surveys)
  python jobs/ingest_bls.py --skip-download   # reuse already-downloaded raw files
"""
from __future__ import annotations
import datetime as dt
import json
import os
import re
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
RAW = os.path.join(ROOT, "data", "raw", "bls")
OUT = os.path.join(ROOT, "data", "clean_full", "bls")
BASE = "https://download.bls.gov/pub/time.series"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
LICENSE_ID = "us-public-domain"
BATCH = 1_000_000          # rows per Parquet row-group flush
CHUNK = 1 << 20            # 1 MiB download chunks

os.makedirs(RAW, exist_ok=True)
os.makedirs(OUT, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})


# ----------------------------------------------------------------------------
# period / value parsing
# ----------------------------------------------------------------------------
def parse_obs_date(year: str, period: str):
    """Map (year, period) -> a date. Returns None for unparseable.

    Annual-average pseudo-periods (M13, Q05, S03, A01) are anchored to Dec-31 of
    the year so they sort after that year's sub-annual points and stay distinct.
    """
    try:
        y = int(year)
    except (ValueError, TypeError):
        return None
    p = period.strip()
    if not p:
        return None
    tag, num = p[0], p[1:]
    try:
        n = int(num)
    except ValueError:
        return None
    if tag == "M":
        if 1 <= n <= 12:
            return dt.date(y, n, 1)
        if n == 13:                      # annual average
            return dt.date(y, 12, 31)
        return None
    if tag == "Q":
        if 1 <= n <= 4:
            return dt.date(y, (n - 1) * 3 + 1, 1)
        if n == 5:                       # annual average
            return dt.date(y, 12, 31)
        return None
    if tag == "S":
        if n == 1:
            return dt.date(y, 1, 1)
        if n == 2:
            return dt.date(y, 7, 1)
        if n == 3:                       # semiannual annual average
            return dt.date(y, 12, 31)
        return None
    if tag == "A":                       # annual
        return dt.date(y, 12, 31)
    return None


def parse_value(tok: str):
    tok = tok.strip()
    if not tok or tok in ("-", "(NA)", "N/A", "*", "(n)", "(p)", "(r)"):
        return None
    try:
        return float(tok)
    except ValueError:
        # strip any trailing footnote-ish chars, retry
        m = re.match(r"^-?\d+(?:\.\d+)?", tok)
        return float(m.group()) if m else None


# ----------------------------------------------------------------------------
# download with retry/backoff
# ----------------------------------------------------------------------------
def download(url: str, dest: str, expected: int | None = None) -> int:
    """Stream a file to dest with retry/backoff. Skips if size already matches."""
    if expected and os.path.exists(dest) and os.path.getsize(dest) == expected:
        return os.path.getsize(dest)
    last = None
    for attempt in range(5):
        try:
            with SESSION.get(url, stream=True, timeout=300) as r:
                r.raise_for_status()
                tmp = dest + ".part"
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(CHUNK):
                        if chunk:
                            fh.write(chunk)
                os.replace(tmp, dest)
            sz = os.path.getsize(dest)
            if expected and sz != expected:
                # size mismatch -> retry
                raise IOError(f"size {sz} != expected {expected}")
            return sz
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(60, 3 * (attempt + 1) ** 2))
    raise RuntimeError(f"download failed {url}: {last}")


# ----------------------------------------------------------------------------
# series metadata (.series file) -> dict[series_id] = {title, begin, end, ...}
# ----------------------------------------------------------------------------
def load_series_meta(survey: str, path: str):
    meta = {}
    if not os.path.exists(path):
        return meta
    with open(path, encoding="utf-8", errors="replace") as f:
        header = f.readline().rstrip("\n").split("\t")
        header = [h.strip() for h in header]
        try:
            i_title = header.index("series_title")
        except ValueError:
            i_title = None
        idx = {h: k for k, h in enumerate(header)}
        for line in f:
            cells = line.rstrip("\n").split("\t")
            if not cells or not cells[0].strip():
                continue
            sid = cells[0].strip()
            title = cells[i_title].strip() if (i_title is not None and i_title < len(cells)) else ""
            by = cells[idx["begin_year"]].strip() if "begin_year" in idx and idx["begin_year"] < len(cells) else ""
            ey = cells[idx["end_year"]].strip() if "end_year" in idx and idx["end_year"] < len(cells) else ""
            meta[sid] = {"title": title, "begin_year": by, "end_year": ey}
    return meta


# ----------------------------------------------------------------------------
# parse one survey's data files -> grouped Parquet (streamed)
# ----------------------------------------------------------------------------
SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
    ("period", pa.string()),
    ("footnote", pa.string()),
])


def _iter_rows(path):
    """Yield parsed (series_id, year, period, value_tok, footnote) tuples from one
    data file, skipping the header. Counters for bad rows handled by caller."""
    with open(path, encoding="utf-8", errors="replace") as fh:
        first = fh.readline()  # header: series_id\tyear\tperiod\tvalue\tfootnote_codes
        if "series_id" not in first:
            fh.seek(0)
        for line in fh:
            cells = line.rstrip("\n").split("\t")
            if len(cells) < 4:
                continue
            sid = cells[0].strip()
            if not sid:
                continue
            yield (sid, cells[1], cells[2].strip(), cells[3],
                   cells[4].strip() if len(cells) > 4 else "")


def write_survey(survey: str, data_files: list[str], raw_dir: str):
    """Parse a survey's data files -> ONE grouped Parquet (series_id column inside).

    Returns (n_obs, n_series, min_date, max_date, n_baddate, n_badval, n_dups).

    Each BLS data file is sorted ascending by (series_id, year, period). We k-way
    MERGE the files on that SAME key (heapq.merge key=...) into one globally-sorted
    stream. This does two things:
      * distinct series counted EXACTLY via last-seen, even when a series spans
        several files;
      * DEDUP of overlapping files. Many surveys ship the identical observations in
        more than one file -- e.g. QCEW (ca/cb/cc/cd/cf/ch/ci/cm/cs/...) ships
        `*.data.0.Current` BYTE-IDENTICAL to `*.data.1.AllData`, and CES/CPI/SM
        ship `Current`/state/industry cuts that re-list rows already in the full
        file. Because identical (series_id, year, period) keys are now ADJACENT in
        the merged stream, we collapse consecutive duplicates (keep first), which
        removes the double-counting WITHOUT a memory-heavy dedup set. This is the
        fix for the QCEW 2x inflation (ca was 105M rows -> correct ~52M).

    Memory is bounded: only one row per file sits in the merge heap plus one Parquet
    row-group batch -- essential for the tens-of-millions-of-series surveys.
    """
    import heapq

    out_path = os.path.join(OUT, survey + ".parquet")
    writer = pq.ParquetWriter(out_path, SCHEMA, compression="zstd")
    n_series = 0
    last_sid = None
    last_key = None                      # (sid, year, period) of last EMITTED row
    sid_b, date_b, val_b, per_b, fn_b = [], [], [], [], []
    n_obs = n_baddate = n_badval = n_dups = 0
    mn = mx = None

    def flush():
        nonlocal sid_b, date_b, val_b, per_b, fn_b
        if not sid_b:
            return
        tbl = pa.table({
            "series_id": sid_b,
            "obs_date": pa.array(date_b, type=pa.date32()),
            "value": pa.array(val_b, type=pa.float64()),
            "period": per_b,
            "footnote": fn_b,
        }, schema=SCHEMA)
        writer.write_table(tbl)
        sid_b, date_b, val_b, per_b, fn_b = [], [], [], [], []

    # Merge on (series_id, year, period) so identical observation keys from
    # overlapping files (e.g. Current vs AllData) become ADJACENT and can be
    # collapsed. Each input file is itself sorted by this key (verified empirically),
    # so heapq.merge yields a globally key-sorted stream with bounded memory.
    mkey = lambda r: (r[0], r[1], r[2])  # noqa: E731
    streams = [_iter_rows(os.path.join(raw_dir, df)) for df in data_files]
    merged = heapq.merge(*streams, key=mkey) if len(streams) > 1 else streams[0]

    for sid, year, period, vtok, fn in merged:
        od = parse_obs_date(year, period)
        if od is None:
            n_baddate += 1
            continue
        v = parse_value(vtok)
        if v is None:
            n_badval += 1
            continue
        key = (sid, year, period)
        if key == last_key:              # duplicate obs from an overlapping file
            n_dups += 1
            continue
        last_key = key
        if sid != last_sid:
            n_series += 1
            last_sid = sid
        sid_b.append(sid)
        date_b.append(od)
        val_b.append(v)
        per_b.append(period)
        fn_b.append(fn or None)
        n_obs += 1
        if mn is None or od < mn:
            mn = od
        if mx is None or od > mx:
            mx = od
        if len(sid_b) >= BATCH:
            flush()
    flush()
    writer.close()
    if n_obs == 0:
        try:
            os.remove(out_path)
        except OSError:
            pass
    return n_obs, n_series, mn, mx, n_baddate, n_badval, n_dups


# ----------------------------------------------------------------------------
# driver
# ----------------------------------------------------------------------------
def main():
    manifest = json.load(open(os.path.join(RAW, "_manifest.json")))
    surveys = sorted(manifest.keys())

    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
        surveys = [s for s in surveys if s in only]
    probe = "--probe" in sys.argv
    if probe:
        # small surveys that exercise all period types + title/no-title schemas
        surveys = ["pr", "jl", "li", "su", "gg", "bp"]
    skip_dl = "--skip-download" in sys.argv
    force = "--force" in sys.argv

    grand_obs = grand_series = 0
    summary = {}
    print(f"{'PROBE' if probe else 'RUN'}: {len(surveys)} surveys -> {OUT}", flush=True)

    for survey in surveys:
        m = manifest[survey]
        data_files = m["data"]
        if not data_files:
            print(f"{survey:6} (no data files, skip)", flush=True)
            continue
        # On a (series_id, year, period) collision between an overlapping pair the
        # dedup keeps the FIRST stream's row. Put `*.AllData` (final full history)
        # ahead of `*.Current` (recent / preliminary) so the authoritative value wins.
        data_files = sorted(data_files, key=lambda f: (not f.endswith(".AllData"), f))
        raw_dir = os.path.join(RAW, survey)
        os.makedirs(raw_dir, exist_ok=True)

        # resume guard: a completed survey has both <survey>.parquet and a sidecar
        # <survey>.meta.json. Reuse the sidecar so an interrupted run resumes cheaply.
        meta_path = os.path.join(OUT, survey + ".meta.json")
        pq_path = os.path.join(OUT, survey + ".parquet")
        if not force and os.path.exists(meta_path) and os.path.exists(pq_path):
            prev = json.load(open(meta_path))
            summary[survey] = prev
            grand_obs += prev["n_obs"]
            grand_series += prev["n_series_with_data"]
            print(f"{survey:6} (done already: obs={prev['n_obs']:,} "
                  f"series={prev['n_series_with_data']:,}) -- skip", flush=True)
            continue
        t0 = time.time()

        # 1) download data files + series file
        dl_bytes = 0
        if not skip_dl:
            for df in data_files:
                exp = m["all"].get(df)
                dl_bytes += download(f"{BASE}/{survey}/{df}", os.path.join(raw_dir, df), exp)
            if m.get("has_series"):
                sf = survey + ".series"
                download(f"{BASE}/{survey}/{sf}", os.path.join(raw_dir, sf), m["all"].get(sf))
        dl_s = time.time() - t0

        # 2) parse -> grouped parquet (streamed)
        n_obs, n_series, mn, mx, bd, bv, ndup = write_survey(survey, data_files, raw_dir)

        # 3) published series count from .series (authoritative)
        sf_path = os.path.join(raw_dir, survey + ".series")
        pub_series = None
        if os.path.exists(sf_path):
            with open(sf_path, encoding="utf-8", errors="replace") as fh:
                pub_series = sum(1 for _ in fh) - 1  # minus header

        grand_obs += n_obs
        grand_series += n_series
        rec = {
            "n_obs": n_obs, "n_series_with_data": n_series,
            "pub_series": pub_series, "data_files": len(data_files),
            "start": str(mn), "end": str(mx),
            "bad_date": bd, "bad_val": bv, "dup_obs": ndup,
            "dl_mb": round(dl_bytes / 1e6, 1),
        }
        summary[survey] = rec
        if n_obs > 0:
            json.dump(rec, open(meta_path, "w"), indent=2)  # sidecar for resume
        print(
            f"{survey:6} files={len(data_files):>3} obs={n_obs:>12,} "
            f"series(data)={n_series:>8,} series(.series)={pub_series if pub_series is not None else '?':>8} "
            f"{str(mn)}..{str(mx)} dl={dl_s:5.0f}s parse={time.time()-t0-dl_s:6.0f}s "
            f"baddate={bd} badval={bv} dup={ndup:,}",
            flush=True,
        )

    # write run summary
    json.dump(summary, open(os.path.join(OUT, "_summary.json"), "w"), indent=2)
    tot_pub = sum(v["pub_series"] for v in summary.values() if v["pub_series"])
    print("=" * 70, flush=True)
    print(f"DONE: {len(summary)} surveys / {grand_obs:,} observations / "
          f"{grand_series:,} series-with-data / {tot_pub:,} series in .series files",
          flush=True)


if __name__ == "__main__":
    main()
