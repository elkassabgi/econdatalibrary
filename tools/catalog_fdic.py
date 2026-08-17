"""Catalogue fdic at SERIES grain — 298,869 bank×metric series (owner serve decision 2026-08-17).

Grain measured over the full store with duckdb: financials.parquet holds
19,918,427 rows / 298,869 distinct series_keys of form ``CERT=<n>:<METRIC>``
(23,017 banks x 13 RIS metrics). The four sibling parquets are relational
reference tables (institutions/history/failures/summary), not series — the
resolver reads financials.parquet only (see _resolve.py fdic branch).

Titles join institutions.parquet (INSTNAME, STALP by CERT) with the official
FDIC RIS metric names below; unmapped metric codes FAIL LOUDLY rather than
shipping an opaque title. Units: FDIC reports dollar amounts in thousands of
US dollars. Exact gate: keys scanned == rows inserted == rows counted.
"""
import os
import sqlite3
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIN = os.path.join(ROOT, "data", "clean_full", "fdic", "financials.parquet").replace("\\", "/")
INST = os.path.join(ROOT, "data", "clean_full", "fdic", "institutions.parquet").replace("\\", "/")
CAT = os.path.join(ROOT, "data", "catalog.db")

# Official FDIC RIS variable names (BankFind Suite financials definitions).
METRIC = {
    "ASSET": "Total assets",
    "DEP": "Total deposits",
    "EQ": "Total equity capital",
    "NETINC": "Net income",
    "LNLSNET": "Net loans and leases",
    "INTINC": "Total interest income",
    "EINTEXP": "Total interest expense",
    "NONII": "Total noninterest income",
    "NONIX": "Total noninterest expense",
    "LNATRES": "Allowance for loan and lease losses",
    "NCLNLS": "Noncurrent loans and leases",
    "SC": "Total securities",
    "ORE": "Other real estate owned",
}


def main() -> int:
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT f.series_key,
               regexp_extract(f.series_key, '^CERT=([0-9]+)', 1) AS cert,
               regexp_extract(f.series_key, ':([A-Z0-9_]+)$', 1) AS metric,
               MIN(f.obs_date) AS start_date, MAX(f.obs_date) AS end_date
        FROM '{FIN}' f GROUP BY 1, 2, 3
    """).fetchall()
    names = dict(con.execute(
        f"SELECT CAST(CERT AS VARCHAR), ANY_VALUE(INSTNAME) || '|' || ANY_VALUE(STALP) "
        f"FROM '{INST}' GROUP BY 1").fetchall())
    unmapped = sorted({m for _, _, m, _, _ in rows if m not in METRIC})
    if unmapped:
        print(f"FATAL: unmapped metric codes {unmapped} — extend METRIC, never ship opaque titles")
        return 1

    db = sqlite3.connect(CAT, timeout=7200)
    db.execute("PRAGMA busy_timeout=7200000")
    ins = 0
    batch = []
    for key, cert, metric, d0, d1 in rows:
        nm = names.get(cert)
        inst, state = (nm.split("|", 1) if nm else (f"institution CERT {cert}", None))
        title = f"{METRIC[metric]} — {inst} (FDIC CERT {cert})"
        batch.append((f"fdic:{key}", "fdic", title, "quarterly",
                      "thousands of US dollars", state, "us-public-domain", str(d0), str(d1)))
        if len(batch) >= 5000:
            db.executemany(
                "INSERT OR REPLACE INTO series (series_id, source_id, title, frequency, unit, "
                "geography, license_id, start_date, end_date) VALUES (?,?,?,?,?,?,?,?,?)", batch)
            ins += len(batch)
            batch = []
    if batch:
        db.executemany(
            "INSERT OR REPLACE INTO series (series_id, source_id, title, frequency, unit, "
            "geography, license_id, start_date, end_date) VALUES (?,?,?,?,?,?,?,?,?)", batch)
        ins += len(batch)
    db.commit()
    total = db.execute("SELECT COUNT(*) FROM series WHERE source_id='fdic'").fetchone()[0]
    print(f"keys={len(rows):,} inserted={ins:,} total={total:,}")
    if not (len(rows) == ins == total):
        print("GATE FAILED — counts must be identical")
        return 1
    print("gate EXACT")
    return 0


if __name__ == "__main__":
    sys.exit(main())
