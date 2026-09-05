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
import re
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


def coverage_span(odate, vint):
    """(start, end) of REPORTED coverage for a company's facts.

    end = the latest period end among facts whose period had ENDED by the time the fact was
    filed (end <= filed). A reported period cannot end after its own filing, so this excludes,
    without any date threshold, both filer typos (VICR carried a fact dated 6016-06-30, PAMT
    3015-03-31, eleven companies 2201..2215) and forward-looking XBRL contexts (lease-maturity
    and remaining-performance-obligation schedules legitimately end 2027..2050 — NUE 2027-12-31,
    CIK0001518171 2053-03-31). Measured 2026-09-05: CIK0000005656 2201-08-31 -> 2017-04-11,
    ORCL 2199-12-31 -> 2026-03-05, AAPL unchanged (0 forward rows). The facts themselves stay
    exactly as filed — only the catalogue's coverage changes. Before this rule the span was
    max(obs_date), which copied the typo into series.end_date and from there into
    /v1/series/{id}.metadata.json (ledger: the 19-row impossible-date census, 11 sec_edgar).
    Falls back to max(obs_date) only when no fact carries a filed date at all.
    """
    if not odate:
        return None, None
    lo = min(odate)
    reported = [e for e, f in zip(odate, vint) if f is not None and e <= f]
    if reported:
        return lo, max(reported)
    # No fact has a filed date at or after its period end (measured 2026-09-05: never happens
    # in the 161 companies read - every row carries vintage_date). Fall back to the latest
    # period that has at least ENDED, never straight to max(obs_date), which is the typo.
    today = dt.date.today()
    ended = [e for e in odate if e <= today]
    return lo, (max(ended) if ended else max(odate))


def update_catalog(spans, apply_d1):
    """Move series.start_date/end_date with the data.

    Refreshing the parquet and the CSV but not the catalog leaves the METADATA lying
    about the data underneath it: after the first run, sec_edgar:BA served facts
    through 2026-07-21 while its catalog row still advertised 2026-04-15. The
    /v1/series/{id}.metadata.json endpoint reports exactly that field, so a user
    checking coverage before downloading is told the wrong answer — and anything
    keyed on end_date for freshness inherits the same error.

    Local catalog.db is updated when present (it is the curated source of truth and
    absent on a CI runner); D1 is updated whenever wrangler can authenticate, since
    D1 is what the worker actually reads. Neither is inferred from the other — a
    single diff shared across two stores that may disagree is what left an earlier
    licence fix inert (R107).
    """
    n_local = n_new = 0
    db = os.path.join(ROOT, "data", "catalog.db")
    if os.path.exists(db):
        import sqlite3
        con = sqlite3.connect(db, timeout=120)
        con.execute("PRAGMA busy_timeout=120000")
        # BEGIN IMMEDIATE with retries: the crawlers hold this database for hours and a
        # deferred transaction only discovers the lock at COMMIT, after every statement has
        # run (the respan needed three attempts on 2026-09-05).
        local_ok = True
        for attempt in range(12):
            try:
                con.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower() or attempt == 11:
                    local_ok = False
                    break
                time.sleep(10)
        if not local_ok:
            # The local catalogue is the curated copy, not what users read; a lock here must not
            # abort the D1 half after the R2 objects were already written (that is the hosted-but-
            # unlisted failure again). Say so loudly, leave the local rows for a --respan re-run.
            con.close()
            print(f"  LOCAL CATALOGUE NOT UPDATED: catalog.db stayed locked through 12 attempts - "
                  f"{len(spans)} span(s) still to apply locally (re-run --respan for these idents without --d1); "
                  f"continuing to D1", flush=True)
            spans_local = []
        else:
            spans_local = spans
        # UPSERT, not UPDATE. An UPDATE-only path silently does nothing for a company
        # that has no catalog row yet — and a NEW registrant filing for the first time
        # is exactly that case. Two such files (CIK0002084272, SMJF) were written to
        # R2 by earlier runs of this very tool and left uncatalogued: data hosted,
        # series invisible, undownloadable. That is the "merged but not served" failure
        # this repo keeps rediscovering, reintroduced here by me.
        for ident, lo, hi, title, cik in spans_local:
            sid = f"sec_edgar:{ident}"
            cur = con.execute("SELECT title FROM series WHERE series_id=?", (sid,))
            row = cur.fetchone()
            if row:
                con.execute("UPDATE series SET start_date=?, end_date=?, title=? WHERE series_id=?",
                            (str(lo), str(hi), title, sid))
                n_local += 1
            else:
                con.execute(
                    "INSERT INTO series (series_id, source_id, title, frequency, unit, "
                    "geography, category, license_id, start_date, end_date, metadata) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (sid, "sec_edgar", title, "Q", None, "US", "fundamentals",
                     "us-public-domain", str(lo), str(hi),
                     json.dumps({"cik": cik, "ticker": ident if not
                                 ident.startswith("CIK") else None})))
                # FTS is a standalone table and does not track `series`; skipping it
                # would leave the new company unsearchable even once catalogued. INSERT
                # only, no DELETE first: series_fts is fts5(series_id UNINDEXED, ...), so a
                # delete by id is a full scan of the index (13.5M rows here, 23.8M on D1 -
                # R492/R730), and inside this IMMEDIATE transaction it would hold the
                # crawlers off the database for its whole duration. A duplicate is
                # impossible on this path: the FTS row and the `series` row land in the same
                # transaction, and this branch runs only when the `series` row is absent.
                con.execute("INSERT INTO series_fts (series_id, title, geography) "
                            "VALUES (?,?,?)", (sid, title, "US"))
                n_new += 1
        if spans_local:
            try:
                con.commit()
            except sqlite3.OperationalError as e:
                # rollback-journal COMMIT needs the EXCLUSIVE lock (R734): a long reader wins
                con.rollback()
                n_local = n_new = 0
                print(f"  LOCAL CATALOGUE NOT UPDATED: COMMIT failed ({e}) - rolled back; {len(spans)} span(s) "
                      f"still to apply locally (re-run --respan for these idents without --d1); continuing to D1", flush=True)
            con.close()
    if n_new:
        print(f"   catalogued {n_new:,} NEW company/companies not previously listed",
              flush=True)
    n_d1 = 0
    d1_failed = False
    if apply_d1 and spans:
        tmp = os.path.join(ROOT, "data", "_sec_spans.sql")
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        rpath = os.path.join(ROOT, "data", f"_sec_edgar_catalog_receipt_{stamp}.json")
        sids = [f"sec_edgar:{ident}" for ident, _lo, _hi, _t, _c in spans]
        # READ D1 FIRST, by primary key (an index seek per id, IN-lists of 40): which of
        # today's companies already have a row, and under what title. That answer decides
        # which statements are emitted below, and it is what lets the FTS index be touched
        # by INSERT alone - see d1_catalog_statements.
        existing, rows_read = _d1_titles(sids)
        stmts, n_new_d1, n_title = d1_catalog_statements(spans, existing)
        assert_no_fts_predicate(stmts)
        print(f"  D1 pre-read: {len(existing):,} of {len(sids):,} ids already catalogued "
              f"(rows_read {rows_read:,}); {n_new_d1:,} new -> series + FTS INSERT; "
              f"{n_title:,} title change(s) -> series only (FTS keeps the old title: no reindex tool exists)",
              flush=True)
        receipt = {"spans": [list(map(str, s)) for s in spans], "existing_on_d1": len(existing),
                   "statements": len(stmts), "new_on_d1": n_new_d1, "title_changed": n_title,
                   "batches": [], "failed": False}
        for j in range(0, len(stmts), 400):
            io.open(tmp, "w", encoding="utf-8").write("\n".join(stmts[j:j + 400]))
            try:
                res = _d1_json(["--file", tmp])
            except Exception as e:                            # noqa: BLE001
                # Any batch, not only the first: a failure at 400+ used to leave n_d1 > 0
                # and the run green with half the day's companies uncatalogued.
                print(f"  D1 catalogue batch FAILED at statement {j}: {str(e)[-300:]}", flush=True)
                receipt["batches"].append({"from": j, "error": f"{type(e).__name__}: {str(e)[:300]}"})
                d1_failed = True
                break
            # The WHOLE meta is kept (R730): `changes` alone could not explain the +1 the
            # import endpoint reports per file, and rows_written was gone by then.
            receipt["batches"].append({"from": j, "n": len(stmts[j:j + 400]),
                                       "meta": [e.get("meta") for e in res]})
            n_d1 += len(stmts[j:j + 400])
        receipt["failed"] = d1_failed
        json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1, default=str)
        print(f"  D1 receipt: {rpath}", flush=True)
        if os.path.exists(tmp):
            os.remove(tmp)
    return n_local, n_d1, d1_failed


def _d1_titles(sids):
    """{series_id: title} for the ids that exist on D1 - primary-key IN-lists of 40, free."""
    out, rows_read = {}, 0
    for i in range(0, len(sids), 40):
        chunk = sids[i:i + 40]
        sql = ("SELECT series_id, title FROM series WHERE series_id IN ("
               + ",".join("'" + s.replace("'", "''") + "'" for s in chunk) + ")")
        for entry in _d1_json(["--command", sql]):
            rows_read += int((entry.get("meta") or {}).get("rows_read") or 0)
            for row in entry.get("results") or []:
                if "series_id" in row:
                    out[row["series_id"]] = row.get("title")
    return out, rows_read


def assert_no_fts_predicate(stmts):
    """Refuse any statement that predicates on series_fts.series_id.

    series_fts is fts5(series_id UNINDEXED, title, geography): a WHERE on its series_id is a
    full scan of the index (~23.8M rows, R492), and on 2026-09-05 11:28Z ONE such statement -
    `SELECT count(*) FROM series_fts WHERE series_id = 'sec_edgar:AAPL'` - did not finish
    inside D1's storage timeout (error 7429). The daily path emitted one per changed company
    (R730) and had never executed it on CI. This guard is code, not a comment: the statement
    list is checked before wrangler sees it.
    """
    for s in stmts:
        low = " ".join(s.lower().split())
        if "series_fts" in low and (" where " in low or " match " in low):
            raise RuntimeError("REFUSED: a statement predicates on series_fts (a full scan of "
                               "the FTS index, R492/R730): " + s[:160])


def d1_catalog_statements(spans, existing):
    """The D1 statements for one catalogue update - pure, so a test can assert their shape.

    `existing` maps series_id -> title for the spans' ids that already have a D1 row (from
    `_d1_titles`, a primary-key read). Rules:
      * an existing id gets ONE `UPDATE series ... WHERE series_id=` (PK seek); when its title
        changed the same UPDATE carries the title. Its FTS row is NOT touched - the only way
        to replace an FTS row is a DELETE by id, which is the full scan this file refuses -
        so a renamed company's FTS title stays stale (no reindex tool exists yet; open item).
      * a new id gets `INSERT OR IGNORE INTO series` + `INSERT INTO series_fts`. No DELETE
        first: both rows land in one import and this branch runs only for ids the pre-read
        did not find, so the duplicate the old DELETE guarded against cannot arise here.
    Returns (statements, n_new, n_title_changed).
    """
    def esc(s):
        return str(s).replace("'", "''")
    stmts, n_new, n_title = [], 0, 0
    for ident, lo, hi, title, _cik in spans:
        sid = f"sec_edgar:{esc(ident)}"
        if sid.replace("''", "'") in existing:
            old = existing[sid.replace("''", "'")]
            if old != title:
                n_title += 1
                stmts.append(f"UPDATE series SET start_date='{lo}', end_date='{hi}', "
                             f"title='{esc(title)}' WHERE series_id='{sid}';")
            else:
                stmts.append(f"UPDATE series SET start_date='{lo}', end_date='{hi}' "
                             f"WHERE series_id='{sid}';")
        else:
            n_new += 1
            stmts.append(
                f"INSERT OR IGNORE INTO series (series_id, source_id, title, frequency, "
                f"geography, category, license_id, start_date, end_date) VALUES "
                f"('{sid}','sec_edgar','{esc(title)}','Q','US','fundamentals',"
                f"'us-public-domain','{lo}','{hi}');")
            stmts.append(f"INSERT INTO series_fts (series_id, title, geography) "
                         f"VALUES ('{sid}','{esc(title)}','US');")
    return stmts, n_new, n_title


def audit(client):
    """Population audit, BOTH directions — the check that found what the run reports could not.

    A refresh reports what IT did. It cannot report what is wrong with the source as a
    whole, and the failure that matters here is invisible to any per-run counter: a
    company whose data is on R2 with no catalog row is hosted, paid for and
    undownloadable, and the run that created it printed nothing but success. Two such
    companies (CIK0002084272, SMJF) accumulated exactly that way before an audit of the
    population found them.

    So: enumerate the served objects, enumerate the catalog, and diff BOTH ways.
    `missing` (catalogued but no object) is the one people check; `orphaned` (object
    with no catalog row) is the one that actually happened.
    """
    import sqlite3
    db = os.path.join(ROOT, "data", "catalog.db")
    if not os.path.exists(db):
        print("no local catalog.db — audit needs it; skipping")
        return 0
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    cat = {r[0].split(":", 1)[1] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id='sec_edgar'")}
    served, tok = set(), None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": "clean_grouped/sec_edgar/", "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = client.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            k = o["Key"]
            if k.endswith(".parquet"):
                served.add(k[len("clean_grouped/sec_edgar/"):-len(".parquet")])
        if not r.get("IsTruncated"):
            break
        tok = r["NextContinuationToken"]
    missing = sorted(cat - served)
    orphan = sorted(served - cat)
    print(f"AUDIT  catalog={len(cat):,}  stored={len(served):,}  "
          f"catalogued-but-not-stored={len(missing):,}  "
          f"STORED-BUT-NOT-CATALOGUED={len(orphan):,}")
    for x in missing[:6]:
        print(f"   missing object : sec_edgar:{x}")
    for x in orphan[:6]:
        print(f"   uncatalogued   : {x}   <-- hosted and undownloadable")
    if orphan:
        print("   repair with:  --ciks <their CIKs> --apply --force --d1")
    return 1 if (missing or orphan) else 0


def prior_facts(client, path, prefer_r2=False):
    """What the store already holds for this company — mirror first, else R2. None if new.

    READ R2, NOT JUST THE LOCAL FILE. A CI runner has no local store, so a local-only lookup
    reports "new company" for all 17,322 of them and every merge below degenerates to a
    replace — which is the exact bug this function exists to prevent, reintroduced by
    environment.
    """
    import pyarrow.parquet as pq
    # prefer_r2: read the SERVED object first. The local mirror can be months behind R2 (ETD:
    # mirror 20,451 rows to 2026-04-22, served 21,003 to 2026-08-27 on 2026-09-05) and a span
    # or a merge computed from it describes a store nobody is served from (R383, R726). The
    # daily merge keeps mirror-first because its payload is the company's FULL history and the
    # union cannot lose served rows; anything that only READS the store must ask R2.
    t = None
    key = f"clean_grouped/sec_edgar/{os.path.basename(path)}"
    if prefer_r2 and client is not None:
        try:
            t = pq.read_table(io.BytesIO(client.get_object(Bucket=BUCKET, Key=key)["Body"].read()))
        except Exception:                                     # noqa: BLE001  (absent on R2)
            t = None
    if t is None and os.path.exists(path):
        t = pq.read_table(path)
    elif t is None and client is not None:
        try:
            t = pq.read_table(io.BytesIO(
                client.get_object(Bucket=BUCKET, Key=key)["Body"].read()))
        except Exception:                                     # noqa: BLE001  (absent = new)
            return None
    elif t is None:
        return None
    return {c: t.column(c).to_pylist() for c in ("metric", "obs_date", "value", "vintage_date")}


def merge_facts(prior, new):
    """Multiset union of the store's rows and this payload's. Never returns fewer than either.

    WHY MERGE AT ALL — a companyfacts payload is the full history OF ONE CIK, and a company can
    change CIK. Exxon re-registered in 2024: ticker XOM now resolves to CIK 2115436, whose
    payload is 274 facts from 2024-12-31. Writing that over the store keyed by TICKER deleted
    18 years and 20,629 facts of Exxon fundamentals, and it did so silently because the write
    path was `pq.write_table(tbl, path)` — a replace with no comparison to what was there.
    Seven companies in the catalogue have already had a CIK re-assigned (NVRI, CLBK, CBAT, XOM,
    GORO, XPRO, UROY), so this is a standing class, not one incident.

    WHY MULTISET AND NOT A DEDUP KEY. `parse_companyfacts` keeps end/val/filed and drops SEC's
    `start`, so one filing's 3-month and 9-month figures for the same period end collapse into
    indistinguishable rows — XOM has 20,629 rows but only 20,578 distinct 4-tuples. There is no
    key to dedup on, so the union takes max(count in store, count in payload) per distinct row.
    A restatement that genuinely retracts a fact is therefore KEPT: vintage_date makes this a
    point-in-time table, and a fact filed on a date stays true as of that date.
    """
    if not prior:
        return new
    import pandas as pd
    cols = ["metric", "obs_date", "value", "vintage_date"]
    kp = pd.DataFrame(prior)[cols].groupby(cols, dropna=False).size()
    kn = pd.DataFrame(dict(zip(cols, new)))[cols].groupby(cols, dropna=False).size()
    k = kp.align(kn, fill_value=0)
    k = k[0].combine(k[1], max).astype(int).sort_index()
    out = k.index.repeat(k.values).to_frame(index=False)
    merged = tuple(out[c].tolist() for c in cols)
    if len(merged[0]) < max(len(prior["metric"]), len(new[0])):
        raise AssertionError(                       # the one way this could lose a row
            f"union {len(merged[0])} < max(store {len(prior['metric'])}, payload {len(new[0])})")
    return merged


def csv_bytes(metric, odate, vals):
    """The served shape: series_id,obs_date,value — series_id IS the XBRL metric."""
    buf = io.StringIO()
    buf.write("series_id,obs_date,value\n")
    for m, o, v in zip(metric, odate, vals):
        buf.write(f"{m},{o.isoformat()},{v}\n")
    return buf.getvalue().encode("utf-8")


def stamp_freshness_d1(status: str, when_utc: str) -> None:
    """Upsert the D1 source_state row that /v1/sources reads for freshness.

    This refresher lives OUTSIDE the updater (sec_edgar is `live: null`, so
    AQUEDUCT_LIVE_ONLY never runs it) and nothing else ever wrote its
    source_state row — measured 2026-08-18: the workflow was green daily and
    35 companies refreshed that morning, yet /v1/sources showed
    freshness: null. core/sync_state_d1.py is upsert-only (ON CONFLICT DO
    UPDATE), so this row survives the daily state sync.

    Success rule: 'ok' when >=95% of the day's filers fetched (measured reality:
    1-3 transient HTTPErrors out of 37-612 filers EVERY day — 08-17: 3/612,
    08-18: 1/37 — and the --days window retries a failed CIK on the next runs,
    so zero-tolerance would pin the display at 'partial' forever while the
    refresh worked). A worse day stamps status + attempt but does NOT advance
    last_success_utc (R231's spirit: partial coverage must not look complete).
    """
    import subprocess
    ok = status == "ok"
    succ_insert = f"'{when_utc}'" if ok else "NULL"
    succ_update = f", last_success_utc='{when_utc}'" if ok else ""
    sql = ("INSERT INTO source_state (source_id, strategy, cadence, status, "
           "last_success_utc, last_attempt_utc) VALUES "
           f"('sec_edgar', 'edgar_delta', 'daily', '{status}', {succ_insert}, '{when_utc}') "
           f"ON CONFLICT(source_id) DO UPDATE SET status='{status}', cadence='daily', "
           f"last_attempt_utc='{when_utc}'{succ_update};")
    r = subprocess.run(
        [_wrangler_cmd(), "d1", "execute", "econ-catalog", "--remote",
         "--command", sql, "-y"],
        cwd=os.path.join(ROOT, "api", "worker"), capture_output=True, text=True,
        encoding="utf-8", errors="replace", env=_wrangler_env())
    if r.returncode != 0:
        # Fatal to the caller (R730 follow-up d): the 2026-09-04 failure was printed as a
        # truncated log path and the run stayed green while /v1/sources kept showing
        # last_updated 2026-09-03. A stamp that did not land is a red run.
        # The two streams are shown SEPARATELY, stderr first (R733): `(stderr + stdout)[-300:]`
        # ended every failed CI run's message with wrangler's informational banner "To execute
        # on your local development database, remove the --remote flag" from stdout, and the
        # actual error at the end of stderr was never seen - for eleven days.
        print(f"  freshness stamp FAILED (rc={r.returncode}) stderr: {(r.stderr or '')[-700:]!r} | "
              f"stdout tail: {(r.stdout or '')[-300:]!r}", flush=True)
        return False
    print(f"  freshness stamped: source_state sec_edgar {status} @ {when_utc}", flush=True)
    return True


def _wrangler_cmd():
    """The version-pinned local wrangler (never `npx wrangler`, which resolves whatever is on
    PATH — R218/R220 class)."""
    exe = os.path.join(ROOT, "api", "worker", "node_modules", ".bin", "wrangler.cmd")
    if not os.path.exists(exe):
        exe = os.path.join(ROOT, "api", "worker", "node_modules", ".bin", "wrangler")
    return exe


_WRANGLER_ENV = {"env": None, "mode": "inherited"}


def _wrangler_env():
    """The environment wrangler runs with. Inherited by default (on CI the workflow's
    CLOUDFLARE_API_TOKEN/ACCOUNT_ID secrets are the credential). If a D1 call answers 7403 -
    'not authorized to access this service' - the inherited token lacks D1 rights (locally,
    core.r2_util loads .env, whose CF token has R2 and Pages rights only) and the fallback is
    the machine's wrangler OAuth login: the same CLOUDFLARE_* variables removed. Decided once,
    printed once."""
    if _WRANGLER_ENV["env"] is None:
        _WRANGLER_ENV["env"] = dict(os.environ)
    return _WRANGLER_ENV["env"]


def _wrangler_env_fallback():
    stripped = {k: v for k, v in os.environ.items() if not k.startswith("CLOUDFLARE_")}
    _WRANGLER_ENV["env"] = stripped
    _WRANGLER_ENV["mode"] = "oauth (CLOUDFLARE_* stripped after 7403)"
    print(f"   wrangler auth: {_WRANGLER_ENV['mode']}", flush=True)


def _d1_json(args, timeout=600):
    """Run `wrangler d1 execute econ-catalog --remote --json <args>` and parse the JSON array.
    Retries a transient 'Authentication error [code: 10000]' twice (an OAuth-refresh race between
    two wrangler processes, seen 2026-09-05; a dead token fails all three times and raises);
    on 7403 switches once to the OAuth environment (see _wrangler_env)."""
    import subprocess
    last = None
    for attempt in range(4):
        r = subprocess.run([_wrangler_cmd(), "d1", "execute", "econ-catalog", "--remote", "--json", *args],
                           cwd=os.path.join(ROOT, "api", "worker"), capture_output=True, text=True,
                           encoding="utf-8", errors="replace", timeout=timeout, env=_wrangler_env())
        if r.returncode != 0 and "code: 7403" in (r.stdout or "") + (r.stderr or "") and _WRANGLER_ENV["mode"] == "inherited":
            _wrangler_env_fallback()
            continue
        if r.returncode == 0:
            lines = r.stdout.splitlines()
            start = next((i for i, ln in enumerate(lines) if ln.strip() == "["), None)
            if start is None:
                raise RuntimeError(f"no JSON array in wrangler output: {r.stdout[-600:]}")
            return json.loads("\n".join(lines[start:]))
        last = f"wrangler rc={r.returncode}: {(r.stderr or '')[-800:]} {(r.stdout or '')[-800:]}"
        if "code: 10000" in (r.stdout or "") + (r.stderr or "") and attempt < 2:
            print(f"   wrangler auth error 10000 (attempt {attempt + 1}/3) - retrying in 10 s", flush=True)
            time.sleep(10)
            continue
        break
    raise RuntimeError(last)


def _d1_dates(sids):
    """{series_id: (start, end)} from D1 by primary key, in IN-lists of 40 (an index seek per id;
    `--file` returns only a summary, so reads go through --command)."""
    out, rows_read = {}, 0
    for i in range(0, len(sids), 40):
        chunk = sids[i:i + 40]
        sql = ("SELECT series_id, start_date, end_date FROM series WHERE series_id IN ("
               + ",".join("'" + s.replace("'", "''") + "'" for s in chunk) + ")")
        for entry in _d1_json(["--command", sql]):
            rows_read += int((entry.get("meta") or {}).get("rows_read") or 0)
            for row in entry.get("results") or []:
                if "series_id" in row:
                    out[row["series_id"]] = (row.get("start_date"), row.get("end_date"))
    return out, rows_read


def _d1_sec_edgar_rows():
    """{series_id: (start, end)} for EVERY sec_edgar row on D1, by primary-key RANGE
    (`>= 'sec_edgar:' AND < 'sec_edgar;'`) - an index range read, measured 17,438 rows / 35 ms,
    never `WHERE source_id=` (R721/R723). D1, not the local catalogue, is the population: the CI
    refresher writes D1 only (no catalog.db on the runner), so D1 holds rows local never saw
    (17,437 vs 17,276 on 2026-09-05, R726)."""
    out = {}
    res = _d1_json(["--command", "SELECT series_id, start_date, end_date FROM series "
                                  "WHERE series_id >= 'sec_edgar:' AND series_id < 'sec_edgar;'"])
    for entry in res:
        for row in entry.get("results") or []:
            if "series_id" in row:
                out[row["series_id"]] = (row.get("start_date"), row.get("end_date"))
    return out


CONTROL_ID = "sec_edgar:AAPL"     # 0 forward/typo rows measured 2026-09-05: its span must not move


def respan(client, spec, apply=False, apply_d1=False, skip_local=False):
    """Recompute start/end coverage for named idents from the STORED parquets and write only the
    catalogue dates. Built for the 2026-09-05 census: 11 companies advertised end_dates of
    2201..6016 (filer typos copied by the old max(obs_date) span) and 130 more advertised
    forward-looking context ends (2027..2113) as coverage. Nothing but series.start_date /
    series.end_date changes: no facts, no CSV, no FTS statement (an FTS delete by id is a full
    scan of the 23.8M-row index, R492), no insert, no delete."""
    import sqlite3
    import urllib.request
    d1_all = None
    if spec in ("d1-scan", "all"):
        # Candidates from D1, the served population, not from a snapshot of the local catalogue
        # (R726: the 08-16 snapshot missed AERT, CRTD, PFIS, refreshed by CI in between).
        d1_all = _d1_sec_edgar_rows()
        today = dt.date.today().isoformat()
        if spec == "all":
            idents = sorted(s.split("sec_edgar:", 1)[1] for s in d1_all)
        else:
            idents = sorted(s.split("sec_edgar:", 1)[1] for s, (sd, ed) in d1_all.items()
                            if (ed and ed > today) or (sd and sd < "1500-01-01"))
        ctl_ident = CONTROL_ID.split("sec_edgar:", 1)[1]
        if ctl_ident in idents:
            # the external control must stay outside the write set; its span is unchanged under the
            # rule (0 forward/typo rows measured 2026-09-05), so leaving it out costs nothing
            idents = [i for i in idents if i != ctl_ident]
            print(f"respan: {CONTROL_ID} left out of the candidate set - it is the external control", flush=True)
        print(f"respan: D1 holds {len(d1_all):,} sec_edgar rows; candidates ({spec}, end_date > {today} or start < 1500): {len(idents):,}", flush=True)
    elif spec.startswith("@"):
        idents = [ln.strip() for ln in open(spec[1:], encoding="utf-8") if ln.strip() and not ln.startswith("#")]
    else:
        idents = [s for s in re.split(r"[,\s]+", spec) if s]
    idents = [s.split("sec_edgar:", 1)[1] if s.startswith("sec_edgar:") else s for s in idents]
    sids = [f"sec_edgar:{i}" for i in idents]
    if CONTROL_ID in sids:
        raise SystemExit(f"{CONTROL_ID} is the external control and is in the candidate set - pick another control")
    print(f"respan: {len(idents):,} ident(s)  mode={'APPLY' if apply else 'DRY-RUN'}  d1={'yes' if apply_d1 else 'no'}  store read: R2 first", flush=True)

    # store truth
    truth, missing = {}, []
    for ident in idents:
        safe = ident.replace("/", "_").replace(":", "_")
        path = os.path.join(GROUPED, safe + ".parquet")
        prior = prior_facts(client, path, prefer_r2=True)
        if not prior or not prior.get("obs_date"):
            missing.append(ident)
            continue
        lo, hi = coverage_span(prior["obs_date"], prior.get("vintage_date") or [None] * len(prior["obs_date"]))
        n_fwd = sum(1 for e, f in zip(prior["obs_date"], prior.get("vintage_date") or []) if f is not None and e > f)
        truth[f"sec_edgar:{ident}"] = (str(lo), str(hi), max(prior["obs_date"]), n_fwd, len(prior["obs_date"]))
    print(f"  store parquets read: {len(truth):,}; no store object: {len(missing)} {missing[:5]}")

    # catalogue state: local by PK, D1 by PK
    db = os.path.join(ROOT, "data", "catalog.db")
    local = {}
    if os.path.exists(db):
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        con.execute("PRAGMA busy_timeout=120000")
        for i in range(0, len(sids), 400):
            chunk = sids[i:i + 400]
            q = "SELECT series_id, start_date, end_date FROM series WHERE series_id IN (" + ",".join("?" * len(chunk)) + ")"
            for sid, sd, ed in con.execute(q, chunk):
                local[sid] = (sd, ed)
        con.close()
    d1, rr = _d1_dates(sids)
    ctl_before = _d1_dates([CONTROL_ID])[0].get(CONTROL_ID)
    print(f"  local rows: {len(local):,}   D1 rows: {len(d1):,} (rows_read {rr:,})   control {CONTROL_ID} before: {ctl_before}")
    if sids and not d1:
        raise SystemExit("D1 read returned 0 rows for a non-empty served id list - instrument broken (R338), refusing")
    if ctl_before is None:
        raise SystemExit(f"external control {CONTROL_ID} not found on D1 - the verify would be blind (R338), refusing")

    plan = []
    for sid in sids:
        if sid not in truth:
            continue
        lo, hi, raw_max, n_fwd, n = truth[sid]
        l = local.get(sid)
        d = d1.get(sid)
        need_l = l is not None and (l[0], l[1]) != (lo, hi)
        need_d = d is not None and (d[0], d[1]) != (lo, hi)
        if need_l or need_d:
            plan.append({"sid": sid, "lo": lo, "hi": hi, "raw_max": str(raw_max), "n_forward_or_typo": n_fwd,
                         "n": n, "local": l, "d1": d, "need_local": need_l, "need_d1": need_d})
    print(f"  PLAN: {len(plan)} row(s) (local {sum(p['need_local'] for p in plan)}, D1 {sum(p['need_d1'] for p in plan)}); "
          f"already equal: {len(truth) - len(plan)}")
    for p in sorted(plan, key=lambda p: (p["local"] or ("", ""))[1] or "", reverse=True)[:15]:
        print(f"   {p['sid'].split(':')[1]:16s} local={p['local']} d1={p['d1']} -> ({p['lo']}, {p['hi']})  raw_max={p['raw_max']} fwd/typo rows={p['n_forward_or_typo']}")
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    receipt = {"utc": stamp, "mode": "apply" if apply else "dry-run", "idents": len(idents), "truth": truth,
               "missing_store": missing, "plan": plan}
    rpath = os.path.join("D:/temp/claude" if os.path.isdir("D:/temp/claude") else ROOT, f"sec_edgar_respan_{stamp}.json")
    if not apply or not plan:
        json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1, default=str)
        print(f"  {'dry run - nothing written' if not apply else 'nothing to write'}; receipt {rpath}")
        return 0

    # local, by PK, BEGIN IMMEDIATE (two crawlers write this db)
    todo_l = [(p["lo"], p["hi"], p["sid"]) for p in plan if p["need_local"]]
    n_local = 0
    if todo_l and skip_local:
        # catalog.db is in rollback-journal mode ('delete'): a COMMIT needs the EXCLUSIVE lock
        # and waits for every reader, while its PENDING lock blocks every NEW reader. On
        # 2026-09-05 the 4,944-row local UPDATE sat 25 minutes in that state (a pytest full
        # scan of the catalogue held SHARED), freezing cbs_nl's writes behind it (R734). The
        # served state is D1; the local rows are re-run later with the same @file and no --d1.
        print(f"  local: SKIPPED by --skip-local ({len(todo_l)} row(s) still to apply locally - re-run this "
              f"--respan without --d1 when the crawlers and no long reader hold the catalogue)", flush=True)
        receipt["local_skipped"] = len(todo_l)
        todo_l = []
    if todo_l and os.path.exists(db):
        for attempt in range(12):
            con = sqlite3.connect(db, timeout=120, isolation_level=None)
            con.execute("PRAGMA busy_timeout=120000")
            try:
                con.execute("BEGIN IMMEDIATE")
                cur = con.executemany("UPDATE series SET start_date=?, end_date=? WHERE series_id=?", todo_l)
                n_local = cur.rowcount
                con.execute("COMMIT")
                con.close()
                break
            except sqlite3.OperationalError as e:
                print(f"   local attempt {attempt + 1}: {e} - retrying in 20 s", flush=True)
                try:
                    con.execute("ROLLBACK")
                except Exception:              # noqa: BLE001
                    pass
                con.close()
                time.sleep(20)
        else:
            raise SystemExit("local UPDATE never committed - D1 NOT touched")
    print(f"  local: UPDATE applied to {n_local} row(s) (planned {len(todo_l)})")
    receipt["local_updated"] = n_local

    def _dump():
        # The receipt is written after EVERY store transition, so a failure between the local
        # COMMIT and the D1 batch still leaves a record of what moved (R726 item 4).
        json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1, default=str)
    _dump()

    # D1, one --file batch of PK UPDATEs
    todo_d = [p for p in plan if p["need_d1"]]
    rc = 0
    if apply_d1 and todo_d:
        sqlp = os.path.join(os.path.dirname(rpath), f"sec_edgar_respan_{stamp}.sql")
        with open(sqlp, "w", encoding="utf-8") as fh:
            for p in todo_d:
                fh.write(f"UPDATE series SET start_date='{p['lo']}', end_date='{p['hi']}' WHERE series_id='{p['sid']}';\n")
        receipt["d1_sql"] = sqlp
        try:
            res = _d1_json(["--file", sqlp])
        except Exception as e:                                # noqa: BLE001
            receipt["d1_error"] = f"{type(e).__name__}: {str(e)[:300]}"
            _dump()
            raise
        changes = sum(int((e.get("meta") or {}).get("changes") or 0) for e in res)
        print(f"  D1: batch of {len(todo_d)} UPDATE(s): summed meta.changes={changes}; entries={len(res)}")
        receipt["d1_changes"] = changes
        # the WHOLE meta, not `changes` alone (R730): the import endpoint reports one change
        # more than the statement count and only rows_written/rows_read can bound it
        receipt["d1_meta"] = [e.get("meta") for e in res]
        _dump()
        after, rr2 = _d1_dates([p["sid"] for p in todo_d])
        bad = [(p["sid"], after.get(p["sid"])) for p in todo_d if after.get(p["sid"]) != (p["lo"], p["hi"])]
        print(f"  verify D1: {len(todo_d) - len(bad)}/{len(todo_d)} equal the store truth (rows_read {rr2:,})")
        for s, v in bad[:10]:
            print("   MISMATCH", s, v)
        ctl_after = _d1_dates([CONTROL_ID])[0].get(CONTROL_ID)
        ctl_ok = ctl_after == ctl_before
        print(f"  control {CONTROL_ID} on D1: before {ctl_before} after {ctl_after} -> {'unchanged' if ctl_ok else 'CHANGED - FAIL'}")
        probe = [p["sid"] for p in todo_d] + [CONTROL_ID]
        live = {}
        for s in probe:
            url = f"https://econdl-api.elkassabgi.workers.dev/v1/series/{urllib.parse.quote(s, safe='')}.metadata.json?v={int(time.time())}"
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 econdl-respan"}), timeout=60) as f:
                    m = json.loads(f.read())
                    live[s] = (m.get("start_date"), m.get("end_date"))
            except Exception as e:                            # noqa: BLE001
                live[s] = ("ERR", str(e)[:40])
        live_bad = [(p["sid"], live.get(p["sid"])) for p in todo_d if live.get(p["sid"]) != (p["lo"], p["hi"])]
        ctl_live_ok = live.get(CONTROL_ID) == ctl_before
        print(f"  verify LIVE metadata.json: {len(todo_d) - len(live_bad)}/{len(todo_d)} equal; control live {live.get(CONTROL_ID)} "
              f"(expected {ctl_before}) -> {'ok' if ctl_live_ok else 'MISMATCH'}")
        for s, v in live_bad[:10]:
            print("   LIVE MISMATCH", s, v)
        receipt.update({"d1_verify_bad": bad, "live_verify_bad": live_bad, "control": CONTROL_ID,
                        "control_before": ctl_before, "control_after_d1": ctl_after, "control_live": live.get(CONTROL_ID)})
        rc = 1 if (bad or live_bad or not ctl_ok or not ctl_live_ok) else 0
        _dump()
        if rc == 0:
            # /v1/sources shows `data_through` from D1's source_data_through, which CI stamps from
            # MAX(end_date) over the LOCAL catalogue at sync time (the stale coherence copy): it
            # read 2215-09-30 live on 2026-09-05 (R726). Re-stamp it from D1 itself, by PK, as the
            # newest coverage end that has actually arrived (<= today).
            rows = _d1_sec_edgar_rows()
            today = dt.date.today().isoformat()
            mx = max((ed for sd, ed in rows.values() if ed and ed <= today), default=None)
            if mx:
                _d1_json(["--command", f"UPDATE source_data_through SET data_through='{mx}' WHERE source_id='sec_edgar'"])
                back = _d1_json(["--command", "SELECT data_through FROM source_data_through WHERE source_id='sec_edgar'"])
                got = next((r.get("data_through") for e in back for r in (e.get("results") or []) if "data_through" in r), None)
                print(f"  source_data_through sec_edgar: stamped {mx} (D1 max end_date <= today over {len(rows):,} rows); read back {got}; "
                      f"/v1/sources shows it within its max-age=300 (no edge cache, measured R730); the next updater-daily sync "
                      f"recomputes it from the R2 coherence copy under the observed-only cap (core/sync_state_d1.py)")
                receipt["data_through_stamped"] = mx
                receipt["data_through_readback"] = got
                if got != mx:
                    rc = 1
    else:
        print("  D1: skipped (pass --d1) - the worker reads D1, so served metadata stays stale without it")
    _dump()
    print(f"  receipt {rpath}")
    return rc


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--apply", action="store_true",
                    help="write parquet + CSV + upload to R2 (default is a dry run)")
    ap.add_argument("--limit", type=int, default=0, help="cap companies (testing only)")
    ap.add_argument("--audit", action="store_true",
                    help="diff the served objects against the catalog BOTH ways and "
                         "exit; finds companies hosted with no catalog row")
    ap.add_argument("--ciks", default="",
                    help="refresh these CIKs explicitly (comma/space separated), "
                         "bypassing the daily-index window — for repairing companies "
                         "whose data fell behind without a recent filing")
    ap.add_argument("--d1", action="store_true",
                    help="also push start/end coverage to D1 (the store the worker "
                         "reads); needs CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID")
    ap.add_argument("--force", action="store_true",
                    help="rewrite even when the local fact count already matches "
                         "upstream (repairs an R2 copy that drifted from local)")
    ap.add_argument("--skip-local", action="store_true",
                    help="--respan only: write D1 but leave the local catalog.db rows for a later run without --d1 "
                         "(its rollback-journal COMMIT can block the crawlers for minutes behind a long reader, R734)")
    ap.add_argument("--dry-run", action="store_true",
                    help="explicit no-write run (already the default without --apply); named in ARGV so the "
                         "D1 cost guard can tell a free run from a charged one (R323)")
    ap.add_argument("--respan", default="",
                    help="recompute start/end coverage from the STORED parquets for these idents "
                         "(comma/space separated, or @file with one per line) and write ONLY the "
                         "catalogue dates — local by primary key and, with --d1, D1 by primary key "
                         "in one batch; no facts, CSVs or FTS rows are touched. Dry run unless --apply.")
    a = ap.parse_args()

    os.makedirs(GROUPED, exist_ok=True)
    if a.audit:
        from core import r2_util
        return audit(r2_util.client())
    if a.respan:
        from core import r2_util
        return respan(r2_util.client(), a.respan, apply=a.apply, apply_d1=a.d1, skip_local=a.skip_local)
    if a.ciks:
        # Targeted repair. The daily-index path answers "who filed recently"; it
        # cannot reach a company whose data fell behind for some OTHER reason. An
        # audit of all 17,274 companies found exactly two like that (our newest fact
        # 2018/2019, upstream's 2026-06-23) — invisible to a date-window scan because
        # they had not filed in the window, and unreachable without naming them.
        ciks = {int(c) for c in re.split(r"[,\s]+", a.ciks) if c.strip().isdigit()}
        scanned, missing = [f"explicit:{len(ciks)}"], []
        print(f"targeted refresh of {len(ciks):,} explicitly named CIK(s)", flush=True)
    else:
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

    # The client is needed for READS too, not only writes: merge_facts must see what the store
    # already holds, and on CI the local mirror does not exist.
    client = None
    try:
        from core import r2_util
        client = r2_util.client()
    except Exception as e:                                    # noqa: BLE001
        if a.apply:
            raise
        print(f"  (no R2 client: {type(e).__name__} — dry run will diff against the local "
              f"mirror only, so 'new company' here may just mean 'not mirrored')")

    ok = failed = 0
    n_with_baseline = 0
    spans = []
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
        prior = prior_facts(client, path)
        before = len(prior["metric"]) if prior else 0
        if before:
            n_with_baseline += 1
        try:
            metric, odate, vals, vint = merge_facts(prior, (metric, odate, vals, vint))
        except AssertionError as e:
            failed += 1
            errors.append(f"{ident}:merge:{e}")
            continue
        # The skip now compares the MERGED total against the store, not the payload against
        # the store: a payload that adds nothing leaves the union unchanged, and a payload
        # from a successor CIK adds rows without removing the predecessor's.
        if len(metric) == before and not a.force:
            continue                       # nothing new filed
        lo, hi = coverage_span(odate, vint)
        changed.append((ident, before, len(metric), hi))
        # Title carries every ticker SEC maps to this CIK, matching the convention
        # applied across the source — searching GOOG must find Alphabet even though
        # the series is keyed GOOGL.
        ent = data.get("entityName") or ident
        title = f"{ent} ({', '.join(ticks)})" if ticks else str(ent)
        spans.append((ident, lo, hi, title, cik))
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
    # This used to read "WRITTEN (no local baseline to diff)" on CI, because the baseline was
    # the LOCAL parquet and a runner has none — so every filer looked new and the count was
    # honest but meaningless. prior_facts() now falls back to the R2 object, so CI has a real
    # baseline and CHANGED means changed. A company with no baseline is genuinely first-seen.
    print(f"companies CHANGED: {len(changed):,}  (of {len(todo):,} probed, "
          f"{n_with_baseline:,} had a store baseline)"
          + ("  — dry run, nothing written" if not a.apply else "  (parquet + CSV written)"))
    if n_with_baseline < len(todo) - failed:
        print(f"   {len(todo) - failed - n_with_baseline:,} filer(s) had NO store baseline — "
              f"first appearance, or the company is stored under a different ident.")
    print(f"fetch failures   : {failed:,}{('  e.g. ' + str(errors[:4])) if errors else ''}")
    for ident, b, aft, latest in changed[:12]:
        print(f"   {ident:<12} {b:>8,} -> {aft:>8,} facts   newest obs {latest}")
    if a.apply and spans:
        nl, nd, d1_failed = update_catalog(spans, a.d1)
        print(f"catalog coverage updated: local rows={nl:,}  D1 statements={nd:,}"
              + ("" if a.d1 else "   (D1 SKIPPED — pass --d1; the worker reads D1, "
                                 "so served metadata stays stale without it)"))
        if a.d1 and (nd == 0 or d1_failed):
            # R726: from 2026-08-25 to 2026-09-04 every CI run printed "D1 span update FAILED at
            # 0" and exited 0 - 181 companies moved on R2 with no catalogue span and no
            # new-registrant row while the workflow stayed green. Data moved but the catalogue
            # did not: that is a FAILED refresh, and the step must say so. R730: a failure in
            # the second or later batch is the same failure for the companies behind it.
            print("FAIL: parquet + CSV were written for changed companies but the D1 catalogue "
                  f"batch did not complete ({nd:,} statement(s) landed, failed={d1_failed}) - "
                  "the served catalogue no longer matches the served data", flush=True)
            return 1
    if a.apply and a.d1:
        # Even a zero-span day is a completed freshness check (weekends have no
        # filings); the stamp is what keeps /v1/sources freshness non-null.
        ok_day = failed == 0 or failed * 20 <= len(todo)   # >=95% fetched
        if not stamp_freshness_d1("ok" if ok_day else "partial",
                                  dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")):
            print("FAIL: the freshness stamp did not land - /v1/sources would keep a stale "
                  "last_updated while this run reported success", flush=True)
            return 1
    if not a.apply and changed:
        print("\nre-run with --apply to write parquet + CSV and upload to R2")
    return 0


if __name__ == "__main__":
    sys.exit(main())
