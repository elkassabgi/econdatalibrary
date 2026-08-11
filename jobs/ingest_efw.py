"""Ingest Economic Freedom of the World (Fraser Institute) -> clean_full/efw/efw.parquet.

PERMISSION: written grant on file (DATABASE_LICENSES_VERBATIM.md, "Economic Freedom of
the World" section, 2026-08-10): non-commercial re-host with attribution + link-back to
efotw.org, annual refresh cadence. This ingest touches ONLY the published API the
dataset page itself loads — no crawling beyond it.

ENDPOINT (probe-verified 2026-08-11 from this exact code path: 200, 2.8 MB):
  GET https://efotw.org/api/v1/ftw_get_all_data
JSON keyed by year ("1970".."2023", 30 keys — quinquennial pre-2000, annual after),
each an array of country records:
  {country, summary_index, rank, quartile, iso_code,
   Area1..Area5:   {label, value},        # Size of Government, Legal System &
   Area1Rank..Area5Rank: {label, value}}  # Property Rights, Sound Money, Trade, Regulation

SERIES DESIGN: series_key = "<ISO3>:<measure>", measure in
  index | rank | quartile | area1..area5 | area1_rank..area5_rank   (13 per country)
obs_date = Dec-31 of the year (modern period-END convention), value = float.
~165 jurisdictions x 13 measures ~= 2,145 series, ~25k obs — a small annual source.

SNAPSHOT SEMANTICS: the publisher revises history in place (annual editions restate),
so the store is a full overwrite per run, like the bulk_snapshot_if_changed family.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URL = "https://efotw.org/api/v1/ftw_get_all_data"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
OUT_DIR = os.path.join(ROOT, "data", "clean_full", "efw")

MEASURES = (
    ("index", lambda r: r.get("summary_index")),
    ("rank", lambda r: r.get("rank")),
    ("quartile", lambda r: r.get("quartile")),
    ("area1", lambda r: (r.get("Area1") or {}).get("value")),
    ("area2", lambda r: (r.get("Area2") or {}).get("value")),
    ("area3", lambda r: (r.get("Area3") or {}).get("value")),
    ("area4", lambda r: (r.get("Area4") or {}).get("value")),
    ("area5", lambda r: (r.get("Area5") or {}).get("value")),
    ("area1_rank", lambda r: (r.get("Area1Rank") or {}).get("value")),
    ("area2_rank", lambda r: (r.get("Area2Rank") or {}).get("value")),
    ("area3_rank", lambda r: (r.get("Area3Rank") or {}).get("value")),
    ("area4_rank", lambda r: (r.get("Area4Rank") or {}).get("value")),
    ("area5_rank", lambda r: (r.get("Area5Rank") or {}).get("value")),
)


def fetch() -> dict:
    r = requests.get(URL, headers=UA, timeout=300)
    r.raise_for_status()
    return r.json()


def build_table(data: dict):
    keys, dates, vals = [], [], []
    skipped = 0
    countries: dict[str, str] = {}
    labels: dict[str, str] = {}
    for ystr, records in data.items():
        try:
            d = dt.date(int(ystr), 12, 31)
        except (ValueError, TypeError):
            skipped += 1
            continue
        for rec in records:
            iso = (rec.get("iso_code") or "").strip()
            if not iso:
                skipped += 1
                continue
            countries.setdefault(iso, (rec.get("country") or iso).strip())
            for i in range(1, 6):
                a = rec.get(f"Area{i}") or {}
                if a.get("label"):
                    labels.setdefault(f"area{i}", a["label"].strip())
            for measure, getter in MEASURES:
                raw = getter(rec)
                if raw in (None, "", "-", "N/A"):
                    continue
                try:
                    v = float(raw)
                except (ValueError, TypeError):
                    skipped += 1
                    continue
                keys.append(f"{iso}:{measure}")
                dates.append(d)
                vals.append(v)
    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    return tbl, countries, labels, skipped


def main() -> int:
    data = fetch()
    tbl, countries, labels, skipped = build_table(data)
    if tbl.num_rows == 0:
        print("FATAL: parsed 0 rows from a real body — refusing to write")
        return 1
    os.makedirs(OUT_DIR, exist_ok=True)
    pq.write_table(tbl, os.path.join(OUT_DIR, "efw.parquet"))
    # Sidecar for the cataloguer: country names + area labels straight from the
    # publisher's own payload (never invented).
    with open(os.path.join(OUT_DIR, "_efw_meta.json"), "w", encoding="utf-8") as fh:
        json.dump({"countries": countries, "area_labels": labels,
                   "source_url": URL,
                   "fetched_utc": dt.datetime.now(dt.timezone.utc).isoformat()},
                  fh, indent=1, sort_keys=True)
    with open(os.path.join(OUT_DIR, "_provider.json"), "w", encoding="utf-8") as fh:
        json.dump({"provider": "Fraser Institute — Economic Freedom of the World",
                   "endpoint": URL, "licence": "written permission on file "
                   "(DATABASE_LICENSES_VERBATIM.md): NC, attribution, link to efotw.org"},
                  fh, indent=1)
    import collections
    distinct = len(set(tbl.column("series_key").to_pylist()))
    years = sorted({d.year for d in tbl.column("obs_date").to_pylist()})
    print(f"rows={tbl.num_rows:,} distinct_series={distinct:,} "
          f"countries={len(countries)} years={years[0]}..{years[-1]} "
          f"({len(years)}) skipped_values={skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
