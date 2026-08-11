"""One-shot: catalogue efw (Economic Freedom of the World) at series grain (task #131).

Store measured by jobs/ingest_efw.py 2026-08-11: 58,486 obs / 2,145 distinct series
(165 jurisdictions x 13 measures) / 1970..2023. Titles come from the publisher's own
payload via the _efw_meta.json sidecar (country names + area labels) — never invented.

Licence: written permission on file (DATABASE_LICENSES_VERBATIM.md "Economic Freedom
of the World" section): NC re-host, attribution, prominent link-back to efotw.org.
license row 'fraser-efw-permission' mirrors kof_globalization's written-permission
pattern: reservable=1 (no-metadata-only rule), commercial_ok=0, attribution_required=1.
"""
import json
import os
import sqlite3
import sys

import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = "efw"
STORE = os.path.join(ROOT, "data", "clean_full", "efw")
LICENSE_ID = "fraser-efw-permission"

MEASURE_DESC = {
    "index": "Economic Freedom Summary Index",
    "rank": "Economic Freedom Rank",
    "quartile": "Economic Freedom Quartile",
    "area1_rank": "Area 1 Rank (Size of Government)",
    "area2_rank": "Area 2 Rank (Legal System & Property Rights)",
    "area3_rank": "Area 3 Rank (Sound Money)",
    "area4_rank": "Area 4 Rank (Freedom to Trade Internationally)",
    "area5_rank": "Area 5 Rank (Regulation)",
}

meta = json.load(open(os.path.join(STORE, "_efw_meta.json"), encoding="utf-8"))
countries = meta["countries"]
area_labels = meta["area_labels"]          # {"area1": "Size of Government", ...}

tbl = pq.read_table(os.path.join(STORE, "efw.parquet"),
                    columns=["series_key", "obs_date"])
keys = {}
for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
    lo, hi = keys.get(k, (d, d))
    keys[k] = (min(lo, d), max(hi, d))
print(f"distinct series in store: {len(keys):,}")


def title_of(key: str) -> str:
    iso, measure = key.split(":", 1)
    c = countries.get(iso, iso)
    desc = MEASURE_DESC.get(measure) or area_labels.get(measure, measure)
    return f"{desc} — {c} (EFW)"


con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=7200)
con.execute("PRAGMA busy_timeout=7200000")
con.execute(
    "INSERT OR REPLACE INTO license (license_id, name, reservable, commercial_ok, "
    "attribution_required, no_modify, url) VALUES (?,?,?,?,?,?,?)",
    (LICENSE_ID, "Written permission (Fraser Institute) — non-commercial, attribution",
     1, 0, 1, 0, "https://efotw.org/economic-freedom/citations"))
con.execute(
    "INSERT OR REPLACE INTO source (source_id, name, homepage, license_id, "
    "attribution, terms_url) VALUES (?,?,?,?,?,?)",
    (SRC, "Fraser Institute — Economic Freedom of the World", "https://efotw.org",
     LICENSE_ID,
     "Fraser Institute, Economic Freedom of the World. Authoritative data: efotw.org",
     "https://efotw.org/economic-freedom/citations"))
rows = [(f"{SRC}:{k}", SRC, title_of(k), "A", None, k.split(":", 1)[0], None,
         LICENSE_ID, lo.isoformat(), hi.isoformat(), None, "{}")
        for k, (lo, hi) in sorted(keys.items())]
con.executemany(
    "INSERT OR REPLACE INTO series (series_id, source_id, title, frequency, unit, "
    "geography, category, license_id, start_date, end_date, last_updated, metadata) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
con.execute("DELETE FROM series_fts WHERE series_id LIKE 'efw:%'")
con.execute("INSERT INTO series_fts (series_id, title, geography) "
            "SELECT series_id, title, geography FROM series WHERE source_id=?", (SRC,))
con.commit()
total = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (SRC,)).fetchone()[0]
print(f"catalogued {total:,} (expected {len(keys):,})")
sys.exit(0 if total == len(keys) else 1)
