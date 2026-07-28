#!/usr/bin/env python3
"""IMF — DIRECT from api.imf.org (SDMX 2.1), replacing the DBnomics relay.

WHY DIRECT: 37.4% of this library (635,750 series) arrives via DBnomics, an
aggregator. That makes our freshness a function of THEIR refresh cadence and puts a
third party between us and the source. Every IMF dataset we relay is published by
IMF itself at api.imf.org with no key required.

It is also, measurably, MORE data. Full-history comparison, 2026-07-28:

    flow      direct     ours   note
    FAS       27,603   13,960   direct has ~2x our series
    WORLD      3,245    2,268   +43%
    FDI        1,728    1,728   exact match, 192 countries both
    AFRREO     1,652    1,654   ~100%
    APDREO       250      265   94%
    COFER        140      154   91%
    WHDREO       287      322   89% of series but MORE countries (48 vs 37)
    MCDREO       623    1,095   57%  <- direct is SMALLER; do not switch blind
    FM           128    1,356   9%   <- direct is much smaller; investigate first

THE ENDPOINT THAT MATTERS: `api.imf.org/external/sdmx/2.1`. Two wrong turns cost
real time and are recorded so nobody repeats them:
  * `sdmxcentral.imf.org` is IMF's DATA-COLLECTION portal. It answers 200 and lists
    101 dataflows with internal ids (01R, BCG, BOP6) — none of the public datasets.
    It looks like success and returns the wrong catalogue.
  * `dataservices.imf.org` (the old SDMX_JSON host) does not connect at all.

AGENCY IDS ARE NOT UNIFORM — read them from the dataflow catalogue, never assume
IMF.STA. Guessing produced four spurious 404s in a first pass: FDI is IMF.MCM,
AFRREO IMF.AFR, MCDREO IMF.MCD, APDREO IMF.APD, FM/WORLD IMF.FAD.

KEY IDENTITY — the open question this script deliberately does NOT decide. Our
stored keys are legacy dotted IFS-style codes from DBnomics (`IMF_CPI:A.AE.PCPI_IX`)
while IMF's modern API uses named dimensions (COUNTRY, INDEX_TYPE, COICOP_1999,
TYPE_OF_TRANSFORMATION, FREQUENCY). IMF RETIRED IFS — there are zero IFS dataflows
in the public catalogue — so for restructured datasets there is no crosswalk to
recover the old identities, and a naive switch would both re-key every series and,
for CPI specifically, drop 41 countries and every annual frequency. This script
writes under its own source ids so the decision to retire the DBnomics-era series
stays a deliberate one.

Usage:
    python jobs/ingest_imf_direct.py --flow FDI --agency IMF.MCM
    python jobs/ingest_imf_direct.py --list
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "data", "clean_full")
BASE = "https://api.imf.org/external/sdmx/2.1"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
TIMEOUT = 300
RETRIES = 4

# Dimensions that describe PUBLICATION rather than identity. Including them in the
# series key would make the key churn whenever IMF re-tags a series, so they are
# dropped — but they are dropped by NAME, explicitly, never by position.
NON_IDENTITY = {
    "ACCESS_SHARING_LEVEL", "SECURITY_CLASSIFICATION", "OVERLAP", "SCALE",
    "DECIMALS_DISPLAYED", "COMMON_REFERENCE_PERIOD", "DERIVATION_TYPE",
}


def http_get(url: str) -> bytes:
    last = None
    for attempt in range(RETRIES):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                raise                                        # definitive: no such flow
            last = e
        except Exception as e:                               # noqa: BLE001
            last = e
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET failed after {RETRIES} tries: {url} ({last!r})")


def list_flows() -> list[tuple[str, str, str]]:
    root = ET.fromstring(http_get(f"{BASE}/dataflow"))
    out = []
    for e in root.iter():
        if e.tag.split("}")[-1] == "Dataflow" and e.get("id"):
            name = ""
            for c in e:
                if c.tag.split("}")[-1] == "Name":
                    name = (c.text or "").strip()
                    break
            out.append((e.get("id"), e.get("agencyID") or "", name))
    return sorted(out)


def parse_period(p: str):
    """SDMX TIME_PERIOD -> date. Handles 2026, 2026-M01, 2026-Q1, 2026-01, 2026-01-31.

    Period-END convention, matching the rest of the store: an annual observation is
    stamped 12-31 so it sorts after that year's monthly points rather than before.
    """
    if not p:
        return None
    p = p.strip()
    try:
        if len(p) == 4:                                      # 2026
            return dt.date(int(p), 12, 31)
        if "-M" in p:                                        # 2026-M01
            y, m = p.split("-M")
            return _month_end(int(y), int(m))
        if "-Q" in p:                                        # 2026-Q1
            y, q = p.split("-Q")
            return _month_end(int(y), int(q) * 3)
        if "-S" in p:                                        # semester
            y, s = p.split("-S")
            return _month_end(int(y), int(s) * 6)
        if len(p) == 7 and "-" in p:                         # 2026-01
            y, m = p.split("-")
            return _month_end(int(y), int(m))
        if len(p) == 10:                                     # 2026-01-31
            return dt.date(int(p[:4]), int(p[5:7]), int(p[8:10]))
    except (ValueError, TypeError):
        return None
    return None


def _month_end(y: int, m: int) -> dt.date:
    if m >= 12:
        return dt.date(y, 12, 31)
    return dt.date(y, m + 1, 1) - dt.timedelta(days=1)


def pull(flow: str, agency: str, source_id: str) -> int:
    url = f"{BASE}/data/{agency},{flow}/all"
    print(f"[imf_direct] GET {url}", flush=True)
    raw = http_get(url)
    print(f"[imf_direct] {len(raw):,} bytes", flush=True)
    root = ET.fromstring(raw)

    series = [e for e in root.iter() if e.tag.split("}")[-1] == "Series"]
    if not series:
        # A 200 that parsed no series is a STRUCTURAL signal, not an empty dataset —
        # report it rather than writing an empty file over good data.
        print(f"[imf_direct] FAIL {flow}: 200 but ZERO series parsed "
              f"({len(raw):,} bytes) — schema change or wrong flow id", flush=True)
        return 0

    # Identity dimensions = whatever this flow actually declares, minus the
    # publication-metadata ones. Order is sorted for stability: IMF may reorder
    # attributes between releases and the key must not move when they do.
    dims = sorted({k for s in series for k in s.attrib} - NON_IDENTITY)
    print(f"[imf_direct] {len(series):,} series; identity dims: {', '.join(dims)}",
          flush=True)

    keys, dates, vals = [], [], []
    # THREE counters, not one. SDMX routinely declares period slots with no value,
    # so "empty" is normal and "unparseable" is a DEFECT — lumping them together
    # sends the next reader chasing a non-issue, or worse, teaches them to ignore a
    # real one. FAS reports 164,719 empty of 362,005 observations: all genuinely
    # blank in IMF's feed, zero non-numeric. That number is alarming until it is
    # broken down, and harmless once it is.
    n_empty = n_badval = n_baddate = 0
    for s in series:
        key = f"{flow}:" + ".".join((s.attrib.get(d) or "").replace(".", "_")
                                    for d in dims)
        for o in s:
            if o.tag.split("}")[-1] != "Obs":
                continue
            raw_p = o.attrib.get("TIME_PERIOD", "")
            v = o.attrib.get("OBS_VALUE")
            if v in (None, "", "NaN"):
                n_empty += 1                                 # normal: no value published
                continue
            d = parse_period(raw_p)
            if d is None:
                n_baddate += 1                               # DEFECT: unhandled period format
                continue
            try:
                fv = float(v)
            except ValueError:
                n_badval += 1                                # DEFECT: value we cannot read
                continue
            keys.append(key)
            dates.append(d)
            vals.append(fv)

    if not keys:
        print(f"[imf_direct] FAIL {flow}: series present but NO usable observations "
              f"(empty={n_empty:,} bad_date={n_baddate:,} bad_value={n_badval:,}) "
              f"— this is a DEFECT, not an empty dataset", flush=True)
        return 0
    if n_baddate or n_badval:
        # Loud and separate: an unhandled period format or unreadable value is a
        # parser bug that silently drops real data, and it must never hide inside
        # a benign "skipped" total.
        print(f"[imf_direct] WARNING {flow}: {n_baddate:,} obs with an unparseable "
              f"TIME_PERIOD and {n_badval:,} with an unreadable OBS_VALUE — these "
              f"are DROPPED REAL DATA, investigate the parser", flush=True)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    d = os.path.join(OUT, source_id)
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"{source_id}.parquet")
    tmp = path + ".tmp"
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, path)
    print(f"[imf_direct] wrote {tbl.num_rows:,} obs / "
          f"{len(set(keys)):,} series -> {path}"
          + (f"  (empty={n_empty:,})" if n_empty else ""),
          flush=True)
    return tbl.num_rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--list", action="store_true", help="list public dataflows and exit")
    ap.add_argument("--flow", help="dataflow id, e.g. FDI")
    ap.add_argument("--agency", help="agency id; looked up from the catalogue if omitted")
    ap.add_argument("--source-id", help="output source id (default imf_<flow>_direct)")
    a = ap.parse_args()

    if a.list:
        for fid, ag, nm in list_flows():
            print(f"  {fid:<32} {ag:<10} {nm[:56]}")
        return 0
    if not a.flow:
        ap.error("--flow is required (or --list)")

    agency = a.agency
    if not agency:
        match = [f for f in list_flows() if f[0].upper() == a.flow.upper()]
        if not match:
            print(f"no such dataflow: {a.flow}", file=sys.stderr)
            return 1
        agency = match[0][1]
        print(f"[imf_direct] agency resolved from catalogue: {agency}", flush=True)

    sid = a.source_id or f"imf_{a.flow.lower()}_direct"
    return 0 if pull(a.flow, agency, sid) else 1


if __name__ == "__main__":
    sys.exit(main())
