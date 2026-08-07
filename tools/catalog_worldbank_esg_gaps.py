"""Catalogue the (indicator, economy) pairs that worldbank_esg HOLDS but does not SERVE.

THE SHAPE OF THIS SOURCE, because it is unusual and was misread once already. The store holds
every indicator the World Bank publishes under source=75 (92 files as of 2026-08-07), but the
SERVED set is a deliberate editorial curation of 24 — `INDICATORS` in
connectors/worldbank_esg/connector.py, a hand-written `code -> (title, pillar)` map whose
comment reads "Curated starter set". The other 68 are NOT a serving defect; they are scope.
Expanding that curation is Ahmed's call, not this tool's, and this tool cannot do it: it
refuses any indicator that is neither curated nor a declared successor of a curated one.

WHAT IT DOES FIX is the curation going stale underneath itself, which happens two ways:

  1. RENAMES. The World Bank re-homed all six Worldwide Governance Indicators from "WDI
     Database Archives" into the "Worldwide Governance Indicators" database, giving each a new
     id `GOV_WGI_<old id>`. The old ids still resolve and still serve, frozen: measured
     2026-08-07, CC.EST/GE.EST/PV.EST/RL.EST/RQ.EST/VA.EST all stop at 2023 across 193
     economies, while every fetched GOV_WGI_ successor carries 2024 across ~204. So the entire
     Governance pillar of the curated set — a quarter of it — is a year behind and can never
     advance again, because nothing will ever write to the retired ids. Cataloguing the
     successors is MAINTENANCE of the existing curation, not an expansion of it: same concept,
     same pillar, publisher-confirmed rename. The predecessors stay served and frozen as the
     pre-rename vintage; nothing is re-keyed, because re-keying a served id is reserved.

  2. NEW COVERAGE. An economy that gains its first observation for an already-curated
     indicator is never catalogued, because the rows were written once. Measured: 13 economies
     (ARE, BEN, BWA, EGY, GUY, IND, ...) have data for SH.H2O.SMDW.ZS and no catalogue row.

WHICH ECONOMIES. The catalogue holds real economies and excludes the World Bank's ~78
aggregates (ARB, EMU, EUU, HIC, IDA, LAC, ...). That rule is not hardcoded here — it is read
from the publisher's own country endpoint, where an aggregate is exactly `region.id == "NA"`
(R373: take a permanent verdict from structured data, never from a formatted string). The
predicate was validated before use by REPRODUCING the existing catalogue: it reproduces 23 of
the 24 curated indicators exactly, and the 24th (SH.H2O.SMDW.ZS) only because the catalogue is
the 13 rows behind that case 2 above describes. Critically, `catalogued - predicted` is empty
for all 24, so the predicate never drops a series that is already served.

SAFETY. INSERT only, and only for ids that do not exist. This tool never UPDATEs or DELETEs a
catalogue row, so it cannot change or retire anything a user can already reach.

    python tools/catalog_worldbank_esg_gaps.py                # dry run, prints the plan
    python tools/catalog_worldbank_esg_gaps.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from connectors.worldbank_esg.connector import INDICATORS  # noqa: E402  the curated set
from core import r2_util  # noqa: E402

SOURCE = "worldbank_esg"
BUCKET = "econ-data"
CATALOG = os.path.join(ROOT, "data", "catalog.db")
LICENSE_ID = "cc-by-4.0"

# Publisher-confirmed renames: each successor id is the publisher's CURRENT id for exactly the
# concept the curated predecessor names. Established by an EXACT membership test against the
# source-75 listing (does it publish `GOV_WGI_<same id>`?), not by fuzzy name similarity — a
# difflib pass on the same data proposed "Energy use per capita" as the successor to methane
# and nitrous-oxide emissions on the strength of the words "per capita" (R142).
#
# DO NOT add a pair here on resemblance. EN.ATM.CO2E.PC and the publisher's new
# EN.GHG.CO2.MT.CE.AR5 cover the same subject matter, but AR5 is a different accounting basis
# — a concept change, not a rename — and treating it as a successor would silently splice two
# incompatible series. That mapping is deliberately absent pending a human read of both
# definitions.
SUCCESSORS = {
    "CC.EST": "GOV_WGI_CC.EST",
    "GE.EST": "GOV_WGI_GE.EST",
    "PV.EST": "GOV_WGI_PV.EST",
    "RL.EST": "GOV_WGI_RL.EST",
    "RQ.EST": "GOV_WGI_RQ.EST",
    "VA.EST": "GOV_WGI_VA.EST",
}

CITATION_SHORT = "World Bank, Sovereign ESG Data."
CITATION_LONG = ("World Bank, Sovereign ESG Data Framework. Retrieved from "
                 "https://datatopics.worldbank.org/esg. Compiled and redistributed by the "
                 "Elkassabgi Data Library.")
DESC_PROCESSING = ("Retrieved from the official source, normalized to a long {series_key, "
                   "obs_date, value} schema (period-start dates), de-duplicated, and stored as "
                   "zstd Parquet. Compiled and redistributed by the Elkassabgi Data Library.")


def _wb(path: str) -> list:
    """Paginate a World Bank v2 endpoint."""
    out: list = []
    for page in range(1, 6):
        url = f"https://api.worldbank.org/v2/{path}&per_page=400&page={page}"
        req = urllib.request.Request(url, headers={"User-Agent": "econdl/1.0"})
        j = json.loads(urllib.request.urlopen(req, timeout=180).read())
        if not isinstance(j, list) or len(j) < 2 or not j[1]:
            break
        out += j[1]
        if page >= int((j[0] or {}).get("pages", 1)):
            break
    return out


def real_economies() -> set[str]:
    """Economies, excluding aggregates. FAIL CLOSED: an unreadable listing raises rather than
    returning an empty set, because empty would mean "catalogue nothing" and read as success."""
    econ = {c["id"] for c in _wb("country?format=json")
            if (c.get("region") or {}).get("id") != "NA"}
    if len(econ) < 150:
        raise SystemExit(f"country listing returned only {len(econ)} economies — refusing to "
                         f"run against a truncated reference list")
    return econ


def published_names() -> dict[str, str]:
    """id -> current official indicator name, from the source-75 listing."""
    return {i["id"]: (i.get("name") or "").strip() for i in _wb("source/75/indicator?format=json")}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="write the rows; without it this is a dry run")
    a = ap.parse_args()
    # R323: a dry run and a real run otherwise print identical numbers.
    print(f"MODE: {'APPLY (writing rows)' if a.apply else 'DRY RUN (no writes)'}\n")

    import duckdb

    econ = real_economies()
    names = published_names()
    print(f"publisher: {len(econ)} economies, {len(names)} source-75 indicators")

    con = sqlite3.connect(CATALOG)
    have = {r[0] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id=?", (SOURCE,))}
    print(f"catalogue: {len(have):,} worldbank_esg rows\n")

    s3 = r2_util.client()
    tmp = tempfile.mkdtemp()
    q = duckdb.connect()

    # Work list: every curated indicator, plus each declared successor. Nothing else can enter.
    work: list[tuple[str, str, str, str | None]] = []   # (indicator, title, pillar, successor_of)
    for ind, (title, pillar) in sorted(INDICATORS.items()):
        work.append((ind, title, pillar, None))
        succ = SUCCESSORS.get(ind)
        if succ:
            name = names.get(succ)
            if not name:
                print(f"  SKIP {succ}: publisher does not currently list it — not catalogued")
                continue
            work.append((succ, name, pillar, ind))

    rows: list[tuple] = []
    missing_store: list[str] = []
    for ind, title, pillar, succ_of in work:
        key = f"clean_full/{SOURCE}/{ind}.parquet"
        path = os.path.join(tmp, f"{ind}.parquet")
        try:
            s3.download_file(BUCKET, key, path)
        except Exception:                                             # noqa: BLE001
            missing_store.append(ind)
            continue
        pq = path.replace(os.sep, "/")
        # Per-economy span, non-null values only — a row of all-nulls is not a series.
        spans = q.execute(f"""
            SELECT country, min(obs_date)::VARCHAR, max(obs_date)::VARCHAR
            FROM read_parquet('{pq}') WHERE value IS NOT NULL GROUP BY country
        """).fetchall()
        added = 0
        for geo, start, end in spans:
            if geo not in econ:
                continue                                   # aggregate, by publisher's own flag
            sid = f"{SOURCE}:{ind}:{geo}"
            if sid in have:
                continue                                   # never touch an existing row
            md = {"indicator": ind, "source": 75, "pillar": pillar,
                  "citation_short": CITATION_SHORT, "citation_long": CITATION_LONG,
                  "description_processing": DESC_PROCESSING}
            if succ_of:
                md["successor_of"] = f"{SOURCE}:{succ_of}"
            rows.append((sid, SOURCE, f"{title} - {geo}", "A", None, geo, pillar,
                         LICENSE_ID, start, end, None,
                         json.dumps(md, ensure_ascii=False)))
            added += 1
        if added:
            tag = f"  (successor of {succ_of})" if succ_of else ""
            print(f"  {ind:24s} +{added:>4} rows{tag}")

    if missing_store:
        print(f"\n  NOT IN STORE, nothing to catalogue yet: {sorted(missing_store)}")

    print(f"\nTOTAL new catalogue rows: {len(rows):,}")
    if not a.apply:
        print("dry run — nothing written. Re-run with --apply.")
        return 0

    con.executemany("INSERT INTO series (series_id, source_id, title, frequency, unit, "
                    "geography, category, license_id, start_date, end_date, last_updated, "
                    "metadata) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    after = con.execute("SELECT count(*) FROM series WHERE source_id=?", (SOURCE,)).fetchone()[0]
    print(f"written. worldbank_esg catalogue rows: {len(have):,} -> {after:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
