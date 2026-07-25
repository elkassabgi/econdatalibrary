#!/usr/bin/env python3
"""Full-coverage grouped ingest of the Bank of England IADB (Interactive
statistical DataBase).

Enumeration (separate step, jobs/_boe_enumerate.py) crawls the database's
"Combined A to Z" category index across all facets and unions every series code
it can reach -> data/raw/boe/_series_codes.json. This job consumes that list.

Pull: the IADB CSV export
  /boeapps/database/_iadb-fromshowcolumns.asp?csv.x=yes&SeriesCodes=<a,b,..>
returns a wide CSV (a SERIES/DESCRIPTION block, then DATE,<code>,<code>,... rows)
for up to ~50 series at a time over the FULL history. We request the full date
range and clamp happens server-side.

Grouped storage (ANTI-BLOAT): ONE Parquet per 3-character series-code PREFIX
(e.g. "XUD","IUM","CFM","RPM","VPQ" ...). The 3-char prefix is the documented
type+frequency family, so each file is dense and holds many series. ~37 files
for the whole source (NOT one-file-per-series). Columns:
  series_key   : BoE series code (e.g. "XUDLUSS")      -- canonical series id
  obs_date     : date32  (parsed from "02 Jan 1975")
  value        : float64 (null where the cell is blank)
Plus a sidecar  <prefix>.titles.json  mapping series_key -> description.

License: ogl-uk-3.0 (Open Government Licence v3.0; the reservable id in
configs/sources.yaml for source "boe").

Resumable: each prefix writes <prefix>.parquet + <prefix>.done (json stats);
a present .done is skipped. Memory is bounded -- we process one prefix at a
time and stream batches to a ParquetWriter.

Usage:
  python jobs/ingest_boe.py --dry 3        # process 3 small prefixes, print, no commit-to-catalog
  python jobs/ingest_boe.py                # full run (all prefixes)
  python jobs/ingest_boe.py --workers 6 --batch 50
"""
import datetime as dt
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)
RAW = os.path.join(ROOT, "data", "raw", "boe")
OUT = os.path.join(ROOT, "data", "clean_full", "boe")
CODES_JSON = os.path.join(RAW, "_series_codes.json")
MANIFEST = os.path.join(OUT, "_manifest.jsonl")
ERRLOG = os.path.join(OUT, "_errors.log")

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
LICENSE_ID = "ogl-uk-3.0"
CSV_URL = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
DATEFROM = "01/Jan/1963"   # the IADB epoch -- earliest date the API accepts
                           # (dates before 1963 are REJECTED with an HTML error page;
                           # no IADB series predates this, so this is full history).
DATETO = "01/Jun/2026"

MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}

SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
])

_print_lock = threading.Lock()


def log(m):
    with _print_lock:
        print(m, flush=True)


def errlog(m):
    with _print_lock:
        with open(ERRLOG, "a", encoding="utf-8") as f:
            f.write(m + "\n")


def session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s


def parse_date(s):
    """'02 Jan 1975' -> date. Also tolerates 'Jan 1975' / '1975'."""
    s = s.strip().strip('"')
    if not s:
        return None
    parts = s.split()
    try:
        if len(parts) == 3:                       # DD Mon YYYY
            return dt.date(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
        if len(parts) == 2:                       # Mon YYYY (monthly)
            return dt.date(int(parts[1]), MONTHS[parts[0]], 1)
        if len(parts) == 1 and parts[0].isdigit():  # YYYY
            return dt.date(int(parts[0]), 12, 31)
    except (ValueError, KeyError):
        return None
    return None


def parse_value(c):
    c = c.strip().strip('"')
    if not c:
        return None
    try:
        return float(c)
    except ValueError:
        return None


def fetch_csv(s, codes, tries=5):
    """GET the wide CSV for a list of codes (full history). Returns text or raises."""
    params = {
        "csv.x": "yes",
        "SeriesCodes": ",".join(codes),
        "Datefrom": DATEFROM,
        "Dateto": DATETO,
        "CSVF": "TT",          # tabular, titles
        "UsingCodes": "Y",
        "VPD": "Y",
        "VFD": "N",
    }
    for i in range(tries):
        try:
            r = s.get(CSV_URL, params=params, timeout=300)
            if r.status_code == 200 and r.text.lstrip().startswith("SERIES"):
                return r.text
            # HTML body (error/empty) or 5xx -> retry with backoff
            if r.status_code in (200, 429, 500, 502, 503, 504):
                time.sleep(2 * (i + 1) + 1)
                continue
            r.raise_for_status()
        except requests.RequestException:
            if i == tries - 1:
                raise
            time.sleep(2 * (i + 1) + 1)
    raise RuntimeError(f"CSV fetch failed for batch of {len(codes)} (first={codes[0]})")


def parse_csv(text):
    """Parse the IADB wide CSV. Yield (series_key, obs_date, value) and return
    a {code: description} dict via a sentinel first item.

    Layout:
      SERIES,DESCRIPTION
      CODE1,desc...
      CODE2,desc...
      <blank>
      DATE,CODE1,CODE2,...
      02 Jan 1975,11.5,2.33,...
      ...
    """
    lines = text.splitlines()
    titles = {}
    i = 0
    n = len(lines)
    # 1) series/description block
    assert lines[0].startswith("SERIES"), "unexpected CSV header"
    i = 1
    while i < n and lines[i].strip() != "":
        # split only on first comma (descriptions contain commas)
        parts = lines[i].split(",", 1)
        if len(parts) == 2:
            titles[parts[0].strip()] = parts[1].strip().strip('"')
        i += 1
    # 2) skip blanks to the DATE header
    while i < n and not lines[i].startswith("DATE,"):
        i += 1
    if i >= n:
        return titles, []
    header = _split_csv(lines[i])
    cols = header[1:]                # series codes in column order
    i += 1
    obs = []
    while i < n:
        row = lines[i]
        i += 1
        if not row.strip():
            continue
        cells = _split_csv(row)
        od = parse_date(cells[0])
        if od is None:
            continue
        for j, code in enumerate(cols, start=1):
            if j >= len(cells):
                break
            v = parse_value(cells[j])
            if v is None:
                continue
            obs.append((code, od, v))
    return titles, obs


def _split_csv(line):
    """Minimal CSV split honouring double quotes (BoE rarely quotes data, but
    descriptions in the series block can contain commas -- handled separately)."""
    if '"' not in line:
        return line.split(",")
    out, cur, q = [], [], False
    for ch in line:
        if ch == '"':
            q = not q
        elif ch == "," and not q:
            out.append("".join(cur)); cur = []
        else:
            cur.append(ch)
    out.append("".join(cur))
    return out


def chunk(lst, k):
    for i in range(0, len(lst), k):
        yield lst[i:i + k]


def process_prefix(prefix, codes, batch, dry=False):
    """Pull all `codes` for one 3-char prefix and write ONE grouped Parquet.
    Returns stats dict or raises."""
    out_path = os.path.join(OUT, f"{prefix}.parquet")
    done_path = os.path.join(OUT, f"{prefix}.done")
    if os.path.exists(done_path) and os.path.exists(out_path):
        try:
            return json.load(open(done_path, encoding="utf-8"))
        except Exception:
            pass

    s = session()
    tmp_out = f"{out_path}.{os.getpid()}.part"   # pid-unique: no cross-process clobber
    writer = None
    titles = {}
    n_obs = 0
    seen = set()
    min_d = max_d = None
    failed_codes = []

    bk, bd, bv = [], [], []

    def flush():
        nonlocal writer, bk, bd, bv
        if not bk:
            return
        batch_tbl = pa.record_batch([
            pa.array(bk, type=pa.string()),
            pa.array(bd, type=pa.date32()),
            pa.array(bv, type=pa.float64()),
        ], schema=SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(tmp_out, SCHEMA, compression="zstd")
        writer.write_batch(batch_tbl)
        bk.clear(); bd.clear(); bv.clear()

    try:
        for bcodes in chunk(codes, batch):
            try:
                text = fetch_csv(s, bcodes)
            except Exception as e:  # noqa: BLE001
                # retry this batch one code at a time to isolate bad/stale codes
                errlog(f"{prefix}\tbatch_fail\t{bcodes[0]}..{bcodes[-1]}\t{e}")
                for c in bcodes:
                    try:
                        text = fetch_csv(s, [c])
                    except Exception as e2:  # noqa: BLE001
                        failed_codes.append(c)
                        errlog(f"{prefix}\tcode_fail\t{c}\t{e2}")
                        continue
                    t, obs = parse_csv(text)
                    titles.update(t)
                    for code, od, v in obs:
                        seen.add(code); n_obs += 1
                        bk.append(code); bd.append(od); bv.append(v)
                        if min_d is None or od < min_d:
                            min_d = od
                        if max_d is None or od > max_d:
                            max_d = od
                    if len(bk) >= 200_000:
                        flush()
                continue
            t, obs = parse_csv(text)
            titles.update(t)
            for code, od, v in obs:
                seen.add(code); n_obs += 1
                bk.append(code); bd.append(od); bv.append(v)
                if min_d is None or od < min_d:
                    min_d = od
                if max_d is None or od > max_d:
                    max_d = od
            if len(bk) >= 200_000:
                flush()
        flush()
    finally:
        if writer is not None:
            writer.close()

    if n_obs == 0 and writer is None:
        empty = pa.table({f.name: pa.array([], type=f.type) for f in SCHEMA}, schema=SCHEMA)
        pq.write_table(empty, tmp_out)

    if os.path.exists(out_path):
        os.remove(out_path)
    os.replace(tmp_out, out_path)

    # sidecar titles
    with open(os.path.join(OUT, f"{prefix}.titles.json"), "w", encoding="utf-8") as f:
        json.dump(titles, f)

    stats = {
        "prefix": prefix,
        "n_codes_requested": len(codes),
        "n_series_with_data": len(seen),
        "n_codes_failed": len(failed_codes),
        "failed_codes": failed_codes[:50],
        "n_obs": n_obs,
        "start": min_d.isoformat() if min_d else None,
        "end": max_d.isoformat() if max_d else None,
        "license_id": LICENSE_ID,
        "file": os.path.basename(out_path),
        "file_bytes": os.path.getsize(out_path),
    }
    if not dry:
        with open(done_path, "w", encoding="utf-8") as f:
            json.dump(stats, f)
    return stats


def load_codes():
    d = json.load(open(CODES_JSON, encoding="utf-8"))
    return sorted(set(d["codes"]))


def group_by_prefix(codes):
    groups = {}
    for c in codes:
        groups.setdefault(c[:3], []).append(c)
    for p in groups:
        groups[p].sort()
    return groups


def main():
    argv = sys.argv[1:]
    dry = "--dry" in argv
    limit = int(argv[argv.index("--dry") + 1]) if dry else None
    workers = int(argv[argv.index("--workers") + 1]) if "--workers" in argv else 6
    workers = max(1, min(workers, 6))
    batch = int(argv[argv.index("--batch") + 1]) if "--batch" in argv else 50

    os.makedirs(OUT, exist_ok=True)
    codes = load_codes()
    groups = group_by_prefix(codes)
    log(f"CATALOG: {len(codes):,} distinct series codes -> {len(groups)} prefix groups")
    sizes = sorted(((p, len(c)) for p, c in groups.items()), key=lambda x: x[1])
    log("  smallest: " + ", ".join(f"{p}({n})" for p, n in sizes[:6]))
    log("  largest:  " + ", ".join(f"{p}({n})" for p, n in sizes[-6:]))

    items = sorted(groups.items(), key=lambda x: len(x[1]))  # small first
    if dry:
        items = items[:limit]
        log(f"DRY-RUN: {len(items)} prefixes")

    # resume
    todo = [(p, c) for p, c in items
            if not (os.path.exists(os.path.join(OUT, f"{p}.done")) and not dry)]
    log(f"Resume: {len(items) - len(todo)} done / {len(items)} prefixes; {len(todo)} remaining")

    t0 = time.time()
    tot_obs = tot_series = n_ok = n_err = 0
    mf = open(MANIFEST, "a", encoding="utf-8")
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(process_prefix, p, c, batch, dry): p for p, c in todo}
        for fut in as_completed(futs):
            p = futs[fut]
            try:
                st = fut.result()
                with lock:
                    n_ok += 1
                    tot_obs += st["n_obs"]
                    tot_series += st["n_series_with_data"]
                    mf.write(json.dumps(st) + "\n"); mf.flush()
                    log(f"  [{n_ok + n_err}/{len(todo)}] {p}: "
                        f"series={st['n_series_with_data']:,}/{st['n_codes_requested']:,} "
                        f"obs={st['n_obs']:,} {st['start']}..{st['end']} "
                        f"{st['file_bytes']/1e6:.1f}MB"
                        + (f" FAILED={st['n_codes_failed']}" if st['n_codes_failed'] else ""))
            except Exception as e:  # noqa: BLE001
                with lock:
                    n_err += 1
                    errlog(f"{p}\tPREFIX_FAIL\t{type(e).__name__}\t{e}")
                    log(f"  ERROR prefix {p}: {type(e).__name__}: {e}")
    mf.close()
    dtm = time.time() - t0
    log("=" * 70)
    log(f"DONE in {dtm/60:.1f} min  ok={n_ok} err={n_err}")
    log(f"  catalog codes (published reachable): {len(codes):,}")
    log(f"  series with >=1 obs written: {tot_series:,}")
    log(f"  observations written this run: {tot_obs:,}")
    log(f"  output dir: {OUT}")


if __name__ == "__main__":
    main()
