#!/usr/bin/env python3
"""Generic UNCTADstat dataset ingest — the publisher's own documented data API.

One source per CURRENT dataset (successor family to the 38 retired DBnomics-era
unctad_* slugs; upstream re-coded all dataset ids, 0 of 38 match — see #70). The full
contract was read from the app's own generated code sample and PROVEN live 2026-08-07
(scratchpad unctad_auth_findings.md):

  catalogue+schema (keyless):
    GET https://unctadstat-api.unctad.org/api/datacenter/en            (99 datasets)
    GET https://unctadstat-api.unctad.org/api/reportMetadata/{DS}/en   (dims+measures+version)
  observations (keyed — UNCTAD_CLIENT_ID / UNCTAD_API_KEY from .env, NEVER in code):
    POST https://unctadstat-user-api.unctad.org/{DS}/cur/Facts?culture=en
    headers ClientId / ClientSecret; multipart form $select/$filter/$format=csv

Series identity: non-time dimension codes in (rowAxe, pageAxe) order + the measure code,
joined with '.', e.g. '0000.02.M0100' = World / exports / US$-current. obs_date from the
isTime dimension (Year -> Dec-31, matching the family's annual convention elsewhere).
One Facts POST per measure group, base (magnitude=1) variant only — the magnitude
variants are the same number scaled, not new data.

Licence: CC BY 3.0 IGO — verbatim audit §UNCTAD (DATABASE_LICENSES_VERBATIM.md:2501-2507),
CLEARED, re-host OK with attribution.

Run: python jobs/ingest_unctad_ds.py US.TradeMerchTotal [--dry]
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

META = "https://unctadstat-api.unctad.org/api"
FACTS = "https://unctadstat-user-api.unctad.org"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

SCHEMA = pa.schema([
    ("series_key", pa.string()), ("obs_date", pa.date32()), ("value", pa.float64()),
])


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def creds():
    # Environment first (CI secrets), .env fallback (workstation). CI has no .env file,
    # so an env-less lookup there must fail LOUDLY, not FileNotFoundError obscurely.
    cid = os.environ.get("UNCTAD_CLIENT_ID")
    key = os.environ.get("UNCTAD_API_KEY")
    if cid and key:
        return cid, key
    env_path = os.path.join(ROOT, ".env")
    if os.path.exists(env_path):
        out = {}
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("UNCTAD_") and "=" in line:
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip().strip('"').strip("'")
        cid = cid or out.get("UNCTAD_CLIENT_ID")
        key = key or out.get("UNCTAD_API_KEY")
    if not (cid and key):
        raise SystemExit("UNCTAD_CLIENT_ID / UNCTAD_API_KEY missing "
                         "(environment and .env both empty)")
    return cid, key


def report_metadata(ds_name: str) -> dict:
    r = requests.get(f"{META}/reportMetadata/{ds_name}/en", headers=UA, timeout=120)
    r.raise_for_status()
    return r.json()


def source_id_for(ds_name: str) -> str:
    # US.TradeMerchTotal -> unctad_trademerchtotal (successor naming; legacy slugs retired)
    return "unctad_" + ds_name.split(".", 1)[1].replace("_", "").lower()


def parse_time(v: str, is_year: bool) -> dt.date | None:
    s = str(v).strip()
    try:
        if is_year or (len(s) == 4 and s.isdigit()):
            return dt.date(int(s), 12, 31)
        if len(s) == 10:
            return dt.date.fromisoformat(s)
        if len(s) == 7 and s[4] == "-":
            return dt.date(int(s[:4]), int(s[5:7]), 1)
    except ValueError:
        pass
    return None


def facts_csv(ds_name: str, select: str, cid: str, key: str, flt: str | None = None) -> str:
    form = {"$select": select, "$format": "csv"}
    if flt:
        form["$filter"] = flt
    files = {k: (None, v) for k, v in form.items()}
    for attempt in range(4):
        try:
            r = requests.post(f"{FACTS}/{ds_name}/cur/Facts?culture=en",
                              headers={"ClientId": cid, "ClientSecret": key, **UA},
                              files=files, timeout=600)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 502, 503, 504):
                time.sleep(20 * (attempt + 1)); continue
            raise RuntimeError(f"Facts HTTP {r.status_code}: {r.text[:300]}")
        except requests.RequestException as e:
            log(f"  transient {type(e).__name__}; retry {attempt + 1}")
            time.sleep(15 * (attempt + 1))
    raise RuntimeError(f"Facts unreachable for {ds_name} after 4 tries")


def ingest(ds_name: str, dry: bool) -> int:
    cid, key = creds()
    meta = report_metadata(ds_name)
    defaults = meta["defaults"]
    src = source_id_for(ds_name)
    out_dir = os.path.join(ROOT, "data", "clean_full", src)

    # Dimensions: non-time in (rowAxe, pageAxe) order forms the series key; the single
    # isTime axis is the observation axis. colAxe is USUALLY the time axis but the flag
    # is authoritative — refuse layouts this generic job has not been taught.
    dims = [d for axe in ("rowAxe", "colAxe", "pageAxe") for d in defaults.get(axe) or []]
    time_dims = [d for d in dims if d.get("isTime")]
    key_dims = [d for d in dims if not d.get("isTime")]
    if len(time_dims) != 1:
        raise SystemExit(f"{ds_name}: {len(time_dims)} time dims — teach the job this layout")
    tfield = time_dims[0]["field"]
    is_year = tfield.lower() == "year"

    # Base (magnitude=1) measure per observation group.
    measures = []
    for grp in defaults.get("observations") or []:
        base = next((m for m in grp.get("measures", []) if m.get("magnitude") == 1), None)
        if base:
            measures.append(base["code"])
    if not measures:
        raise SystemExit(f"{ds_name}: no magnitude-1 measures in reportMetadata")

    kfields = [d["field"] for d in key_dims]
    log(f"{ds_name} -> {src}: key dims {kfields}, time {tfield}, "
        f"measures {['M' + c for c in measures]}, version {meta.get('version')}")

    rows_k, rows_d, rows_v = [], [], []
    for mcode in measures:
        select = ", ".join(f"{f}/Code" for f in kfields) + f", {tfield}, M{mcode}/Value"
        text = facts_csv(ds_name, select, cid, key)
        rdr = csv.DictReader(io.StringIO(text))
        n0 = len(rows_k)
        for rec in rdr:
            vals = [rec.get(f"{f}_Code", "") for f in kfields]
            tv = rec.get(tfield) or rec.get(f"{tfield}_Code", "")
            vv = rec.get(f"M{mcode}_Value", "")
            d = parse_time(tv, is_year)
            if d is None or vv in ("", None):
                continue
            try:
                v = float(vv)
            except ValueError:
                continue
            rows_k.append(".".join(vals + [f"M{mcode}"]))
            rows_d.append(d)
            rows_v.append(v)
        log(f"  M{mcode}: {len(rows_k) - n0:,} obs")

    if not rows_k:
        log(f"  {ds_name}: 0 obs — refusing to write an empty store")
        return 0
    n_series = len(set(rows_k))
    if dry:
        log(f"  DRY: {len(rows_k):,} obs / {n_series:,} series")
        return len(rows_k)
    os.makedirs(out_dir, exist_ok=True)
    tbl = pa.table({"series_key": pa.array(rows_k, pa.string()),
                    "obs_date": pa.array(rows_d, pa.date32()),
                    "value": pa.array(rows_v, pa.float64())}, schema=SCHEMA)
    path = os.path.join(out_dir, f"{src}.parquet")
    pq.write_table(tbl, path, compression="zstd")
    n = pq.read_metadata(path).num_rows
    log(f"  WROTE {path}: {n:,} obs / {n_series:,} series")
    return n


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        raise SystemExit("usage: ingest_unctad_ds.py <US.DatasetName> [--dry]")
    ingest(args[0], "--dry" in sys.argv)
