"""Daily delta refresh for SEC EDGAR company fundamentals — the part that was missing.

WHAT WAS WRONG. sec_edgar serves 17,274 companies of XBRL financial statements
(income statement, balance sheet, cash flow, EPS) and has NO updater state at all —
no source_state row, no unit_state row, never executed. It is `live: null`, so
AQUEDUCT_LIVE_ONLY never runs it. Everything served came from a one-off backfill.
Measured across all 17,274: 6,074 companies (35.2%) carry 2026 data, 1,562 stop in
2025, and 8,713 (50.4%) have nothing after 2023.

WHY A DELTA AND NOT THE BULK ZIP. The registry proposes gating
Archives/edgar/.../companyfacts.zip on its HEAD vintage. That works — I verified the
signal is real (Last-Modified Tue 28 Jul 2026 04:22:35 GMT, a strong ETag, 1,390,705,602
bytes) — but it re-downloads 1.39 GB and rebuilds all 17,274 files to capture a
day's filings. EDGAR's daily-index says exactly who filed:

    2026-07-24   3,994 filings ->  52 CIKs filed 10-K/10-Q/20-F/40-F
    2026-07-27   4,056 filings ->  28 CIKs
    2026-07-28   5,983 filings -> 108 CIKs

and data.sec.gov returns one company's complete facts in ~0.3s (Apple: 3,748,682
bytes, 24,852 facts). So a day costs ~200 requests and about a minute, against 1.39 GB
— and the per-company payload is the FULL history, so a refreshed company is exactly
correct rather than patched.

FAITHFULNESS CHECK, not assumed: parsing Apple's live companyfacts with the ingester's
own rules yields 24,852 facts, matching the 24,852 rows in the stored AAPL.parquet
exactly. Same metric grammar (taxonomy:tag:unit), same obs_date (the fact's `end`),
same vintage_date (its `filed`) — so point-in-time history is preserved and a
restatement adds a row rather than overwriting one.

CSV DERIVE IS INCLUDED DELIBERATELY. Nothing in updater/ or core/ knows how to turn
this source's grouped layout (clean_grouped/sec_edgar/<ID>.parquet) into the served
object (series/sec_edgar:<ID>.csv) — the live CSVs were produced ad hoc. A refresh
that stopped at the parquet would leave every downloadable file untouched while
reporting success, which is the "merged but not served" failure this repo has already
hit on yale_epi, fao_fo and fao_pp.

Usage:
  python tools/refresh_sec_edgar.py --days 3            # dry run, names what changed
  python tools/refresh_sec_edgar.py --days 3 --apply    # write parquet + CSV + R2
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
GROUPED = os.path.join(ROOT, "data", "clean_grouped", "sec_edgar")
BUCKET = "econ-data"
# Forms that restate financial statements. 8-K carries earnings press releases but
# its XBRL is inconsistent; companyfacts is refreshed off the statement filings, and
# fetching a company's FULL facts means an omitted form type costs at most a day.
STATEMENT_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A", "20-F", "20-F/A", "40-F", "40-F/A"}
SEC_MIN_INTERVAL = 0.12          # SEC fair-access: stay well under 10 req/s


def _get(url, timeout=180, binary=False):
    with urllib.request.urlopen(
            urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        b = r.read()
    return b if binary else b.decode("utf-8", "replace")


def ticker_map():
    """{cik: [tickers]} — ALL of them, most-canonical first.

    The original ingester did `cik2tick.setdefault(cik, ticker)`, keeping only the
    first ticker SEC lists per registrant. That is precisely why sec_edgar:GOOG
    returned 404 while GOOGL served: SEC maps GOOGL, GOOG, GOOGM and GOOGN to CIK
    1652044 and three were dropped. Keeping the full list means the identity we write
    stays stable (first ticker, as before) while every alias is still known.
    """
    d = json.loads(_get("https://www.sec.gov/files/company_tickers.json"))
    rows = list(d.values()) if isinstance(d, dict) else d
    out = collections.defaultdict(list)
    for r in rows:
        out[int(r["cik_str"])].append(r["ticker"])
    return {c: t for c, t in out.items()}


def filers_since(days):
    """CIKs that filed a financial statement in the last `days` days, per EDGAR."""
    today = dt.date.today()
    ciks, scanned, missing = set(), [], []
    for back in range(days):
        day = today - dt.timedelta(days=back)
        q = (day.month - 1) // 3 + 1
        url = (f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/"
               f"QTR{q}/form.{day:%Y%m%d}.idx")
        try:
            body = _get(url)
        except Exception:                                     # noqa: BLE001
            # Weekends and holidays have no index. NOT an error, but it IS recorded —
            # a silently skipped day is indistinguishable from a day with no filings.
            missing.append(f"{day:%Y-%m-%d}")
            continue
        lines = body.splitlines()
        start = next((i for i, l in enumerate(lines) if l.startswith("---")), 10) + 1
        n = 0
        for l in lines[start:]:
            if len(l) < 80:
                continue
            form, cik = l[:12].strip(), l[74:86].strip()
            if cik.isdigit() and form in STATEMENT_FORMS:
                ciks.add(int(cik))
                n += 1
        scanned.append(f"{day:%Y-%m-%d}:{n}")
    return ciks, scanned, missing


def parse_companyfacts(data):
    """Identical grammar to jobs/ingest_sec_edgar.py — metric/obs_date/value/vintage."""
    def d(s):
        try:
            return dt.datetime.strptime(s, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None
    metric, odate, vals, vint = [], [], [], []
    for tax, tags in (data.get("facts") or {}).items():
        for tag, body in tags.items():
            for unit, points in (body.get("units") or {}).items():
                sk = f"{tax}:{tag}:{unit}"
                for p in points:
                    end, val = d(p.get("end", "")), p.get("val")
                    if end is None or val is None:
                        continue
                    try:
                        fv = float(val)
                    except (ValueError, TypeError):
                        continue
                    metric.append(sk)
                    odate.append(end)
                    vals.append(fv)
                    vint.append(d(p.get("filed", "")))
    return metric, odate, vals, vint


def csv_bytes(metric, odate, vals):
    """The served shape: series_id,obs_date,value — series_id IS the XBRL metric."""
    buf = io.StringIO()
    buf.write("series_id,obs_date,value\n")
    for m, o, v in zip(metric, odate, vals):
        buf.write(f"{m},{o.isoformat()},{v}\n")
    return buf.getvalue().encode("utf-8")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--apply", action="store_true",
                    help="write parquet + CSV + upload to R2 (default is a dry run)")
    ap.add_argument("--limit", type=int, default=0, help="cap companies (testing only)")
    ap.add_argument("--force", action="store_true",
                    help="rewrite even when the local fact count already matches "
                         "upstream (repairs an R2 copy that drifted from local)")
    a = ap.parse_args()

    os.makedirs(GROUPED, exist_ok=True)
    print(f"scanning EDGAR daily-index, last {a.days} day(s) ...", flush=True)
    ciks, scanned, missing = filers_since(a.days)
    print(f"  statement filings per day: {', '.join(scanned) or 'none'}")
    if missing:
        print(f"  no index published (weekend/holiday): {', '.join(missing)}")
    print(f"  distinct CIKs to refresh: {len(ciks):,}", flush=True)
    if not ciks:
        print("nothing to do")
        return 0

    t2c = ticker_map()
    todo = sorted(ciks)
    if a.limit:
        todo = todo[:a.limit]
        print(f"  LIMITED to {len(todo)} companies (testing)", flush=True)

    client = None
    if a.apply:
        from core import r2_util
        client = r2_util.client()

    ok = failed = 0
    changed, errors = [], []
    for i, cik in enumerate(todo, 1):
        time.sleep(SEC_MIN_INTERVAL)
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
        try:
            data = json.loads(_get(url, timeout=180))
        except Exception as e:                                # noqa: BLE001
            failed += 1
            errors.append(f"CIK{cik:010d}:{type(e).__name__}")
            continue
        metric, odate, vals, vint = parse_companyfacts(data)
        if not metric:
            continue
        ticks = t2c.get(cik) or []
        ident = ticks[0] if ticks else f"CIK{cik:010d}"
        safe = ident.replace("/", "_").replace(":", "_")
        path = os.path.join(GROUPED, safe + ".parquet")
        before = pq.read_metadata(path).num_rows if os.path.exists(path) else 0
        # --force exists because the skip is keyed on the LOCAL parquet. After a run
        # that updated local+CSV but not the R2 parquet, local already matches
        # upstream, so a plain re-run would skip exactly the companies whose R2 copy
        # needs repairing. A local-state check cannot detect remote drift.
        if len(metric) == before and not a.force:
            continue                       # identical fact count -> nothing new filed
        changed.append((ident, before, len(metric), max(odate)))
        if a.apply:
            tbl = pa.table({
                "metric": metric,
                "obs_date": pa.array(odate, type=pa.date32()),
                "value": vals,
                "vintage_date": pa.array(vint, type=pa.date32()),
            })
            pq.write_table(tbl, path)
            # BOTH artefacts, always. The first version of this wrote the parquet
            # LOCALLY and the CSV to R2, which left r2://clean_grouped/sec_edgar/
            # holding a copy older than the CSV derived from it. That is not a
            # cosmetic drift: the grouped parquet is the canonical store, so any
            # later rebuild-from-R2 would silently roll the served CSVs BACK to the
            # stale facts. A refresh has to move the store and the served object
            # together or not at all.
            buf = io.BytesIO()
            pq.write_table(tbl, buf)
            client.put_object(Bucket=BUCKET,
                              Key=f"clean_grouped/sec_edgar/{safe}.parquet",
                              Body=buf.getvalue())
            key = "series/" + urllib.parse.quote(f"sec_edgar:{ident}", safe="") + ".csv"
            client.put_object(Bucket=BUCKET, Key=key,
                              Body=csv_bytes(metric, odate, vals),
                              ContentType="text/csv")
        ok += 1
        if i % 50 == 0:
            print(f"  {i}/{len(todo)} probed, {ok} changed, {failed} failed", flush=True)

    print()
    print(f"companies probed : {len(todo):,}")
    print(f"companies CHANGED: {len(changed):,}"
          + ("  (dry run — nothing written)" if not a.apply else "  (parquet + CSV written)"))
    print(f"fetch failures   : {failed:,}{('  e.g. ' + str(errors[:4])) if errors else ''}")
    for ident, b, aft, latest in changed[:12]:
        print(f"   {ident:<12} {b:>8,} -> {aft:>8,} facts   newest obs {latest}")
    if not a.apply and changed:
        print("\nre-run with --apply to write parquet + CSV and upload to R2")
    return 0


if __name__ == "__main__":
    sys.exit(main())
