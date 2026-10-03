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
import urllib.error
import urllib.parse
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core.titles import clean_title  # noqa: E402 - one rule for title line breaks
from core import sec_edgar_local  # noqa: E402 - the store-name rule, shared with the local check

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


def _quarter_listing(year: int, q: int):
    """The names of the daily form.*.idx files SEC lists for one quarter, or None when the listing cannot be read.
    SEC answers a day with no index (a weekend, a holiday) with 403, the same code as a refused request, so a failed
    fetch cannot say which it was; the quarter's own listing can (checked 2026-10-03: 2026-09-27, a Sunday, and
    2026-09-07, Labor Day, are absent from QTR3's listing; 2026-09-28 is present)."""
    try:
        d = json.loads(_get(f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{q}/index.json"))
        return {str(i.get("name", "")) for i in d.get("directory", {}).get("item", [])
                if str(i.get("name", "")).startswith("form.")}
    except Exception:                                         # noqa: BLE001 - None = cannot tell
        return None


def filers_since(days, today=None):
    """CIKs that filed a financial statement in the last `days` days, per EDGAR.

    `missing` holds a day either as "YYYY-MM-DD" (SEC lists no index for it: a weekend, a holiday, not yet posted)
    or as "YYYY-MM-DD:ERR<type>" (SEC lists it, or the listing could not be read, and the fetch failed). Only the
    second kind is an error; a run with one may not move the scan mark (may_advance). A fetch that failed on a day
    inside the overlap (the last WATERMARK_OVERLAP_DAYS days) is "YYYY-MM-DD:unread-in-overlap-<type>": the next
    window scans that day again, so it is shown but does not block the mark."""
    # UTC, like source_state.last_success_utc and the window computed from it: the workstation's local date is a
    # day behind UTC every evening (CI runs in UTC, so nothing changes there)
    today = today or dt.datetime.now(dt.timezone.utc).date()
    ciks, scanned, missing = set(), [], []
    listings = {}
    for back in range(days):
        day = today - dt.timedelta(days=back)
        q = (day.month - 1) // 3 + 1
        if (day.year, q) not in listings:
            listings[(day.year, q)] = _quarter_listing(day.year, q)
            time.sleep(SEC_MIN_INTERVAL)
        listed = listings[(day.year, q)]
        if listed is not None and f"form.{day:%Y%m%d}.idx" not in listed:
            # no index: NOT an error, but it IS recorded - a silently skipped day is indistinguishable from a day
            # with no filings. EXCEPT when no listing read so far names any later day and the day is older than the
            # overlap: then the listing may stop before it (stale or cached), and skipping it could lose it for good
            # (review AR-209 round 2). The later-day test spans quarters (the newest quarter is read first).
            later = any(n[5:13] > f"{day:%Y%m%d}" for L in listings.values() if L for n in L)
            if not later and day < today - dt.timedelta(days=WATERMARK_OVERLAP_DAYS - 1):
                missing.append(f"{day:%Y-%m-%d}:ERRunlisted")
            else:
                missing.append(f"{day:%Y-%m-%d}")
            continue
        url = (f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/"
               f"QTR{q}/form.{day:%Y%m%d}.idx")
        try:
            body = _get(url)
        except Exception as e:                                # noqa: BLE001
            if day >= today - dt.timedelta(days=WATERMARK_OVERLAP_DAYS - 1):
                # inside the overlap the next window scans this day again, so it does not block the mark: tagged so it
                # shows, without ":ERR" (a quarter's first days, a passing listing failure - AR-209 round 3)
                missing.append(f"{day:%Y-%m-%d}:unread-in-overlap-{type(e).__name__}")
            else:
                missing.append(f"{day:%Y-%m-%d}:ERR{type(e).__name__}")
            continue
        finally:
            time.sleep(SEC_MIN_INTERVAL)
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


def update_catalog(spans, apply_d1, last_updated=None):
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
        con = sqlite3.connect(db, timeout=120)  # plain-open: the catalogue (tests/catalog_db_legacy.txt); after T0 only under the writer lock (runtime guard)
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
            title = clean_title(title)
            sid = f"sec_edgar:{ident}"
            cur = con.execute("SELECT title FROM series WHERE series_id=?", (sid,))
            row = cur.fetchone()
            if row and last_updated is not None:
                # after T0 (the local refresher): /v1/series/<id>.metadata.json reads series.last_updated
                # first, and sec_edgar has no '_all' unit to fall back on once the 13F rows moved
                con.execute("UPDATE series SET start_date=?, end_date=?, title=?, last_updated=? "
                            "WHERE series_id=?", (str(lo), str(hi), title, last_updated, sid))
            elif row:
                con.execute("UPDATE series SET start_date=?, end_date=?, title=? WHERE series_id=?",
                            (str(lo), str(hi), title, sid))
                n_local += 1
            else:
                con.execute(
                    "INSERT INTO series (series_id, source_id, title, frequency, unit, "
                    "geography, category, license_id, start_date, end_date, metadata"
                    + (", last_updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)" if last_updated is not None
                       else ") VALUES (?,?,?,?,?,?,?,?,?,?,?)"),
                    (sid, "sec_edgar", title, "Q", None, "US", "fundamentals",
                     "us-public-domain", str(lo), str(hi),
                     json.dumps({"cik": cik, "ticker": ident if not
                                 ident.startswith("CIK") else None}))
                    + ((last_updated,) if last_updated is not None else ()))
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
            io.open(tmp, "w", encoding="utf-8", newline="\n").write("\n".join(stmts[j:j + 400]))
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
            # import endpoint reports per file, and rows_written was gone by then. Printed too
            # (R737 item e): on CI the receipt lands under gitignored data/ and is never uploaded.
            metas = [e.get("meta") for e in res]
            receipt["batches"].append({"from": j, "n": len(stmts[j:j + 400]), "meta": metas})
            print(f"  D1 batch from {j}: {len(stmts[j:j + 400])} statement(s); meta "
                  f"{[{k: m.get(k) for k in ('changes', 'rows_written', 'rows_read', 'duration')} for m in metas if m]}",
                  flush=True)
            n_d1 += len(stmts[j:j + 400])
        # THE CACHED TOTAL MUST MOVE WITH THE ROWS. `source_counts.n` is what the worker
        # serves as this source's browse total and what /v1/stats sums; only
        # core/sync_catalog_d1.py refreshed it, and only for sources its own push touched. So
        # every company inserted above left the advertised total behind - measured 2026-09-07 as
        # 17,437 advertised against 17,467 rows, exactly the 26 + 4 of the two catch-up receipts
        # from 2026-09-05.
        #
        # UNCONDITIONAL, not `if n_new_d1`: gating on new ids cannot repair drift a previous
        # failed run left, so the tool would never self-heal. The recount is an index seek -
        # measured rows_read 17,468 for n=17,467 - so gating buys nothing. And this tool only
        # ever APPENDS to `series`, so a recount after a partial failure still publishes a number
        # that is true of D1 at that instant; that is what separates it from a whole-source push,
        # where refreshing mid-push would publish a partial count.
        #
        # `--command`, never `--file`: the file path is the IMPORT endpoint, which blocked reads
        # for 112 minutes in R709. One statement has no business taking it.
        sc_sql = ("INSERT OR REPLACE INTO source_counts(source_id, n) "
                  "SELECT 'sec_edgar', COUNT(*) FROM series WHERE source_id = 'sec_edgar';")
        try:
            sc_res = _d1_json(["--command", sc_sql])
            back = _d1_json(["--command", "SELECT n FROM source_counts "
                                          "WHERE source_id = 'sec_edgar';"])
            now = (back[0].get("results") or [{}])[0].get("n") if back else None
            receipt["source_counts_meta"] = [e.get("meta") for e in sc_res]
            receipt["source_counts_after"] = now
            print(f"  source_counts refreshed: sec_edgar n = {now:,}"
                  if isinstance(now, int) else
                  f"  source_counts refreshed but the read-back returned {now!r}", flush=True)
        except Exception as e:                                # noqa: BLE001
            # LOUD, and it reddens the run. A silently stale total returns a 200 with a
            # plausible number, which is why nothing ever caught this one (R503).
            print(f"  source_counts refresh FAILED: {str(e)[-300:]}", flush=True)
            receipt["source_counts_error"] = f"{type(e).__name__}: {str(e)[:300]}"
            d1_failed = True

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
        title = clean_title(title)
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
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=120)  # plain-open: the catalogue, read-only
    con.execute("PRAGMA busy_timeout=120000")
    # primary-key RANGE, never `WHERE source_id=` (R721/R723/R737): the local catalogue has only
    # the PK autoindex, so a source_id predicate scans 13.5M rows under the crawlers' locks
    cat = {r[0].split(":", 1)[1] for r in con.execute(
        "SELECT series_id FROM series WHERE series_id >= 'sec_edgar:' AND series_id < 'sec_edgar;'")}
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
    ok = status == "ok"
    succ_insert = f"'{when_utc}'" if ok else "NULL"
    succ_update = f", last_success_utc='{when_utc}'" if ok else ""
    sql = ("INSERT INTO source_state (source_id, strategy, cadence, status, "
           "last_success_utc, last_attempt_utc) VALUES "
           f"('sec_edgar', 'edgar_delta', 'daily', '{status}', {succ_insert}, '{when_utc}') "
           f"ON CONFLICT(source_id) DO UPDATE SET status='{status}', cadence='daily', "
           f"last_attempt_utc='{when_utc}'{succ_update};")
    # Through _d1_json (R737 item d): the retrying runner (10000 transient x3, 7403 fallback) rather
    # than one bare subprocess.run - on a zero-change day this stamp is the run's only D1 write and
    # its failure exits 1, so a single transient must not make a weekend run red for no data reason.
    # Fatal to the caller (R730 follow-up d): the 2026-09-04 failure was printed as a truncated
    # log path and the run stayed green while /v1/sources kept showing last_updated 2026-09-03.
    # The failure message carries stderr FIRST and separately (R733).
    try:
        _d1_json(["--command", sql])
    except Exception as e:                                    # noqa: BLE001
        print(f"  freshness stamp FAILED: {str(e)[:900]}", flush=True)
        return False
    print(f"  freshness stamped: source_state sec_edgar {status} @ {when_utc}", flush=True)
    return True


def stamp_data_through_from_d1(source: str):
    """tools/stamp_source_data_through.stamp, imported lazily (this file is imported by tests from the
    repo root, where tools/ is not on sys.path)."""
    import importlib.util
    p = os.path.join(ROOT, "tools", "stamp_source_data_through.py")
    spec = importlib.util.spec_from_file_location("_stamp_source_data_through", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.stamp(source, apply=True)


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
        # stderr FIRST and labelled, then the API error's own notes parsed out of the JSON body
        # (R737 item e: the tail of stdout used to hide the cause behind wrangler's banner), then
        # a short stdout tail
        notes = []
        try:
            body = r.stdout or ""
            j = json.loads(body[body.index("{"):]) if "{" in body else {}
            notes = [n.get("text") for n in (j.get("error") or {}).get("notes", []) if n.get("text")]
            if not notes and (j.get("error") or {}).get("text"):
                notes = [j["error"]["text"]]
        except Exception:                                     # noqa: BLE001
            pass
        last = (f"wrangler rc={r.returncode} stderr={(r.stderr or '')[-600:]!r} error_notes={notes} "
                f"stdout_tail={(r.stdout or '')[-300:]!r}")
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


def respan(client, spec, apply=False, apply_d1=False, skip_local=False, local_chunk=400):
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
        safe = sec_edgar_local.store_name(ident)
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
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)  # plain-open: the catalogue, read-only
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
        # CHUNKED TRANSACTIONS (R734 rule 2). catalog.db is journal_mode=delete: one transaction of
        # 4,944 UPDATEs reached COMMIT, waited for a long reader to finish, and meanwhile its PENDING
        # lock froze the crawlers' writes and every new reader for 25 minutes. A few hundred rows per
        # IMMEDIATE transaction holds the locks for well under a second each; a long reader delays
        # one chunk, not the fleet. Each chunk keeps the 12-attempt retry.
        chunk = max(1, int(local_chunk))
        committed = 0
        for j in range(0, len(todo_l), chunk):
            part = todo_l[j:j + chunk]
            for attempt in range(12):
                con = sqlite3.connect(db, timeout=120, isolation_level=None)  # plain-open: the catalogue (--respan; refused after T0)
                con.execute("PRAGMA busy_timeout=120000")
                try:
                    con.execute("BEGIN IMMEDIATE")
                    cur = con.executemany("UPDATE series SET start_date=?, end_date=? WHERE series_id=?", part)
                    got = cur.rowcount
                    con.execute("COMMIT")
                    con.close()
                    n_local += got
                    committed += 1
                    break
                except sqlite3.OperationalError as e:
                    print(f"   local chunk {j // chunk + 1} attempt {attempt + 1}: {e} - retrying in 20 s", flush=True)
                    try:
                        con.execute("ROLLBACK")
                    except Exception:              # noqa: BLE001
                        pass
                    con.close()
                    time.sleep(20)
            else:
                receipt["local_updated"] = n_local
                receipt["local_chunks_committed"] = committed
                _dump_receipt = json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1, default=str)  # noqa: F841
                raise SystemExit(f"local UPDATE chunk {j // chunk + 1} never committed after {n_local} row(s) landed - "
                                 f"the rest of the local half is pending; D1 NOT touched")
            if committed % 5 == 0 or j + chunk >= len(todo_l):
                print(f"   local: {n_local:,}/{len(todo_l):,} row(s) committed in {committed} chunk(s) of {chunk}", flush=True)
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
        with open(sqlp, "w", encoding="utf-8", newline="\n") as fh:
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
            # /v1/sources shows `data_through` from D1's source_data_through. It is stamped from D1's
            # OWN rows (tools/stamp_source_data_through.py: PK-range MAX of ended periods, one upsert,
            # read back) because the updater sync's copy of the local catalogue is not this source's
            # truth and overwrote a correct stamp twice on 2026-09-05 (R730, R737); the sync now
            # skips sec_edgar (core.sync_state_d1.DATA_THROUGH_FROM_D1).
            mx, got, rr = stamp_data_through_from_d1("sec_edgar")
            print(f"  source_data_through sec_edgar: stamped {mx} from D1 (rows_read {rr:,}); read back {got} -> "
                  f"{'OK' if got == mx else 'MISMATCH'}; /v1/sources shows it within its max-age=300")
            receipt["data_through_stamped"] = mx
            receipt["data_through_readback"] = got
            if got != mx:
                rc = 1
                if got != mx:
                    rc = 1
    else:
        print("  D1: skipped (pass --d1) - the worker reads D1, so served metadata stays stale without it")
    _dump()
    print(f"  receipt {rpath}")
    return rc


# THE SCAN WINDOW AFTER T0 (2026-10-03; one of the proofs tools/selfhost/t0_ready.py SEC_EDGAR_OWED lists). Before
# T0 the window is a fixed --days (the CI workflow always passes it). After T0 a fixed window loses filings whenever
# the scheduled task does not run for longer than the window - a machine that was off for a week skips a week of
# filers for good. So, after T0 and without an explicit --days, the window reaches back to the day of the mark
# (source_state.last_success_utc) and the two days before it: L-2..today, WATERMARK_OVERLAP_DAYS days up to and
# including L.
#
# THE MARK MOVES ONLY FOR A SCAN THAT REACHED IT (review AR-209). It is set by _refresh_local on an ok day, and only
# when main() says the run may advance it: a daily-index scan whose window started on or before the old mark, with no
# --limit, and with no day whose index SEC lists but which could not be read. A --ciks repair, a --limit run and a
# --days window that stops short of the mark leave it alone - each would otherwise move it past days nobody
# scanned. The stamp is the time the window was computed from, not the end of the fetch.
WATERMARK_OVERLAP_DAYS = 3
# Beyond this the daily index is the wrong tool (one index fetch per day, then every filer of the whole gap): the run
# refuses and says to pass --days explicitly.
WATERMARK_MAX_DAYS = 120
PRE_T0_DEFAULT_DAYS = 3


def scan_days(explicit, cut_over: bool, last_success_utc, today: dt.date) -> tuple[int, str]:
    """(days, why) for the daily-index scan. An explicit --days always wins; before T0 the old default."""
    if explicit is not None:
        return explicit, "--days given"
    if not cut_over:
        return PRE_T0_DEFAULT_DAYS, "before T0: the fixed default"
    from core import cutover                                  # noqa: PLC0415
    if not last_success_utc:
        raise cutover.CutoverRefused("refused: no successful local refresh is recorded yet (source_state.last_success_"
                                     "utc for sec_edgar) - the first local run takes --days wide enough to reach "
                                     "the last CI scan (T0 step 6)")
    try:
        last = dt.date.fromisoformat(str(last_success_utc)[:10])
    except ValueError:
        raise cutover.CutoverRefused(f"refused: source_state.last_success_utc for sec_edgar is not a date: "
                                     f"{last_success_utc!r}") from None
    gap = (today - last).days
    if gap < 0:
        raise cutover.CutoverRefused(f"refused: the last successful local refresh ({last}) is after today UTC "
                                     f"({today}) - the clock or the state is wrong")
    days = gap + WATERMARK_OVERLAP_DAYS
    if days > WATERMARK_MAX_DAYS:
        raise cutover.CutoverRefused(f"refused: the last successful local refresh was {gap} days ago ({last}); a "
                                     f"scan of {days} days is past WATERMARK_MAX_DAYS ({WATERMARK_MAX_DAYS}) - pass "
                                     f"--days explicitly")
    return days, f"from {last - dt.timedelta(days=WATERMARK_OVERLAP_DAYS - 1)}: the mark {last} and the two days before it"


def _last_success_utc():
    """The mark: source_state.last_success_utc of the XBRL product. A row under the id with another strategy (a
    leftover 13F row, R1218) is not the mark."""
    from updater.state import StateStore                     # noqa: PLC0415
    st = StateStore()
    try:
        row = st.get_source("sec_edgar")
    finally:
        st.close()
    if not row or row.get("strategy") != "edgar_delta":
        return None
    return row.get("last_success_utc")


def _retry_path() -> str:
    from updater import config                                # noqa: PLC0415
    return os.path.join(config.STATE_DIR, "sec_edgar_retry_ciks.json")


# A company that fails (or is absent) in this many mark-moving runs IN A ROW is not transient: it leaves the retry
# list, with a printed line. Seven also covers a new filer whose facts reach companyfacts late (AR-209 round 3).
RETRY_MAX_RUNS = 7


def _load_retry() -> dict:
    """{cik: mark-moving runs it has failed in a row} for the companies whose fetch failed (or which were absent) in the
    run that last moved the mark. A missing file is empty; anything else (unreadable, malformed) raises - never read
    as empty. The first form of the file (a plain list) reads as count 0."""
    try:
        with open(_retry_path(), encoding="utf-8") as f:
            raw = json.load(f)["ciks"]
    except FileNotFoundError:
        return {}
    if isinstance(raw, list):
        return {int(c): 0 for c in raw}
    return {int(c): int(n) for c, n in raw.items()}


def _save_retry(counts: dict) -> None:
    import tempfile                                           # noqa: PLC0415
    from core.atomic import atomic_replace                    # noqa: PLC0415
    p = _retry_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(p), suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"ciks": {str(int(c)): int(n) for c, n in sorted(counts.items())}}, f)
    atomic_replace(tmp, p)


def is_absent(e: BaseException) -> bool:
    """A companyfacts 404: SEC holds no XBRL facts for the company (asset-backed 10-Ks are exempt) or has not posted
    them yet. Retried like a failure, but not a failed fetch for the 5% rule (AR-209 round 3)."""
    return isinstance(e, urllib.error.HTTPError) and e.code == 404


def next_retry(old: dict, failed_ciks, absent_ciks) -> tuple[dict, list]:
    """(the list to save, the companies dropped) when a run moves the mark: every company that failed or was absent
    this run, with its count of mark-moving runs in a row; past RETRY_MAX_RUNS it is dropped. A company that answered
    this run is not carried."""
    new = {int(c): old.get(int(c), 0) + 1 for c in list(failed_ciks) + list(absent_ciks)}
    drop = sorted(c for c, n in new.items() if n > RETRY_MAX_RUNS)
    return {c: n for c, n in new.items() if n <= RETRY_MAX_RUNS}, drop


def _dropped_path() -> str:
    from updater import config                                # noqa: PLC0415
    return os.path.join(config.STATE_DIR, "sec_edgar_retry_dropped.jsonl")


def carry_retry(failed_ciks, absent_ciks, why: dict, when: str) -> int:
    """On a mark move: count this run's failed and absent companies against the saved list, save what stays, and
    APPEND every dropped company to sec_edgar_retry_dropped.jsonl (date, CIK, runs, this run's error) - a scheduled
    task's printed lines are lost, so a later --ciks repair finds them there (AR-209 round 4). Returns the list size."""
    keep, drop = next_retry(_load_retry(), failed_ciks, absent_ciks)
    if drop:
        p = _dropped_path()
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "a", encoding="utf-8", newline="\n") as f:
            for c in drop:
                f.write(json.dumps({"utc": when, "cik": c, "runs": RETRY_MAX_RUNS + 1, "last_error": why.get(c, "")})
                        + "\n")
        print(f"  retry list: dropped {len(drop)} company(ies) after {RETRY_MAX_RUNS} runs in a row without facts "
              f"(appended to {p}): {', '.join(f'CIK{c:010d}' for c in drop[:10])}", flush=True)
    _save_retry(keep)
    print(f"  retry list: {len(keep):,} company(ies) carried to the next run", flush=True)
    return len(keep)


# A filer companyfacts has always answered for (Apple). When many companies answer 404, one fetch of it tells a
# broken endpoint from companies without facts (AR-209 round 4: a day of 404s for everyone was stamped ok).
CANARY_CIK = 320193


def companyfacts_url(cik: int) -> str:
    """ONE place for the companyfacts URL: the canary and the companies must break together (AR-209 round 5)."""
    return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"


def endpoint_answers() -> tuple[bool, str]:
    """(True, "") when companyfacts answers for CANARY_CIK with JSON; else (False, the error)."""
    try:
        json.loads(_get(companyfacts_url(CANARY_CIK), timeout=180))
        return True, ""
    except Exception as e:                                    # noqa: BLE001
        return False, f"{type(e).__name__}{getattr(e, 'code', '')}"


def may_advance(days: int, scan_day: dt.date, last_success_utc, limited: bool, missing) -> bool:
    """May this daily-index run move the mark? Only when its window started on or before the old mark (or there is
    no mark yet: the first run's explicit --days is the operator's statement), no --limit, and no day whose index
    SEC lists but which could not be read."""
    if limited or any(":ERR" in str(m) for m in missing):
        return False
    if not last_success_utc:
        return True
    try:
        last = dt.date.fromisoformat(str(last_success_utc)[:10])
    except ValueError:
        return False
    return scan_day - dt.timedelta(days=days - 1) <= last


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--days", type=int, default=None,
                    help=f"scan the EDGAR daily index this many days back. Default: {PRE_T0_DEFAULT_DAYS} before T0; "
                         "after T0, from the mark (the last OK daily scan) and the two days before it")
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
    ap.add_argument("--local-chunk", type=int, default=400,
                    help="--respan only: rows per local IMMEDIATE transaction (default 400; R734 - one big COMMIT held "
                         "the crawlers off the catalogue for 25 minutes behind a long reader)")
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
    ap.add_argument("--local-only", action="store_true",
                    help="the scheduled workstation run: before T0 it does nothing and exits 0 (the CI job "
                         "sec-edgar-daily refreshes sec_edgar then); after T0 it is the daily run, so --ciks, "
                         "--limit, --days, --d1, --audit, --respan and --force are refused")
    a = ap.parse_args()

    from core import cutover                                  # noqa: PLC0415
    if a.local_only:
        named = [f for f, v in (("--ciks", a.ciks), ("--limit", a.limit), ("--days", a.days is not None),
                                ("--d1", a.d1), ("--audit", a.audit), ("--respan", a.respan), ("--force", a.force))
                 if v]
        if named:
            # the scheduled run must be the one that may move the mark: a repair or a test run never is (may_advance)
            print(f"refused: --local-only is the daily run; it takes no {', '.join(named)}", flush=True)
            return 2
        if not cutover.is_cut_over():
            # before T0 the CI job writes R2 and D1; a second writer here would race it (the scheduled task may be
            # registered before T0 and start working at the flag, with nothing else to switch on)
            print(f"sec_edgar --local-only: not cut over ({cutover.FLAG_PATH} absent) - nothing to do; the CI "
                  f"job sec-edgar-daily refreshes sec_edgar until T0", flush=True)
            return 0
    if cutover.is_cut_over() and (a.d1 or a.audit or a.respan):
        # before anything else: these read or write D1 and R2, which are frozen after T0 (_refresh_local)
        raise cutover.CutoverRefused("refused: after T0 there is no D1, and --audit / --respan are not ported to "
                                     "the self-hosted store yet - run the daily refresh without them")
    os.makedirs(GROUPED, exist_ok=True)
    if a.audit:
        from core import r2_util
        return audit(r2_util.client())
    if a.respan:
        from core import r2_util
        return respan(r2_util.client(), a.respan, apply=a.apply, apply_d1=a.d1, skip_local=a.skip_local,
                      local_chunk=a.local_chunk)
    scan_started = dt.datetime.now(dt.timezone.utc)          # ONE clock reading: the window and the stamp
    advance = False                                           # only a daily scan that reached the mark moves it
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
        cut = cutover.is_cut_over()
        last = _last_success_utc() if cut else None           # before T0 no state is read
        days, why = scan_days(a.days, cut, last if a.days is None else None, scan_started.date())
        print(f"scanning EDGAR daily-index, last {days} day(s) ({why}) ...", flush=True)
        ciks, scanned, missing = filers_since(days, today=scan_started.date())
        print(f"  statement filings per day: {', '.join(scanned) or 'none'}")
        none_listed = [m for m in missing if ":" not in m]
        overlap = [m for m in missing if ":unread-in-overlap" in m]
        unread = [m for m in missing if ":ERR" in m]
        if none_listed:
            print(f"  no index listed by SEC (weekend/holiday/not yet posted): {', '.join(none_listed)}")
        if overlap:
            print(f"  index not read, scanned again next run (inside the overlap): {', '.join(overlap)}", flush=True)
        if unread:
            print(f"  INDEX NOT READ (listed by SEC, the fetch failed): {', '.join(unread)}", flush=True)
        retry = set(_load_retry()) if cut else set()
        if retry:
            # companies whose fetch failed in the run that last moved the mark: fetched again until they succeed,
            # so moving the mark never drops them (review AR-209 round 2)
            print(f"  + {len(retry):,} company(ies) whose fetch failed (or which were not found) when the mark last "
                  f"moved", flush=True)
            ciks |= retry
        print(f"  distinct CIKs to refresh: {len(ciks):,}", flush=True)
        advance = may_advance(days, scan_started.date(), last, bool(a.limit), missing)
        if cut:
            print(f"  this run {'MAY' if advance else 'may NOT'} move the scan mark (now {last})", flush=True)
    # After T0 an unread index is a failed run, not a quiet one: the mark cannot move, and a scheduled task that
    # stayed green would only find out at the WATERMARK_MAX_DAYS refusal (review AR-209 round 2). Before T0: as before.
    unread_after_t0 = cutover.is_cut_over() and any(":ERR" in str(m) for m in missing)
    if not ciks:
        print("nothing to do")
        return 1 if unread_after_t0 else 0

    t2c = ticker_map()
    todo = sorted(ciks)
    if a.limit:
        todo = todo[:a.limit]
        print(f"  LIMITED to {len(todo)} companies (testing)", flush=True)
    if cutover.is_cut_over():
        # the local store, under the lock; no R2, no D1
        rc = _refresh_local(a, todo, t2c, advance=advance, stamp_at=scan_started)
        return rc or (1 if unread_after_t0 else 0)

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
        url = companyfacts_url(cik)
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
        safe = sec_edgar_local.store_name(ident)
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
        title = clean_title(f"{ent} ({', '.join(ticks)})" if ticks else str(ent))
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
        # data_through from D1's own rows after EVERY run (R737): the updater sync no longer stamps
        # this source, so this is the only writer of its /v1/sources data_through.
        try:
            mx, got, rr = stamp_data_through_from_d1("sec_edgar")
        except Exception as e:                                # noqa: BLE001
            print(f"FAIL: data_through stamp from D1 failed: {str(e)[:600]}", flush=True)
            return 1
        print(f"  data_through stamped from D1: {mx} (rows_read {rr:,}); read back {got} -> {'OK' if got == mx else 'MISMATCH'}", flush=True)
        if got != mx:
            print("FAIL: data_through read back differs from what was stamped", flush=True)
            return 1
    if not a.apply and changed:
        print("\nre-run with --apply to write parquet + CSV and upload to R2")
    return 0


# ---- AFTER T0 (docs/ECON_SELF_HOSTING_PLAN.md: MOVE refresh_sec_edgar, design draft 2) ----------------------
# ONE STORE: the local one. Prior facts and the parquet go through the local files (the store IS them), the CSV
# into the self-hosted blob store, the catalogue into the build, the freshness into state.db (the origin copies
# build /v1/sources and /v1/last-updates from it). NO D1 and NO R2. The SEC fetch - many minutes on a busy day -
# runs WITHOUT the writer lock: each changed company is merged and STAGED beside the store; only the commit phase
# (move the parquets into place, the CSVs, the catalogue, the freshness stamp) holds the lock, taken with a bounded
# wait because the updater holds it for its whole run.

def _thirteen_f_blocker() -> "str | None":
    """Why the XBRL row may not be written yet, or None. Until the 13F product has its own key and its state
    rows have moved (feat/econ-13f-own-key), source_state('sec_edgar') is the 13F row, and writing the XBRL
    product's freshness there merges the two products (R1193, R1205)."""
    try:
        from updater import state_migrations as M                   # noqa: PLC0415
    except ImportError:
        return ("updater/state_migrations.py is missing - the 13F product's own key (feat/econ-13f-own-key) "
                "must be merged before the XBRL refresher writes source_state('sec_edgar')")
    import sqlite3                                                   # noqa: PLC0415
    from updater import config                                       # noqa: PLC0415
    con = sqlite3.connect(f"file:{config.STATE_DB}?mode=ro", uri=True)  # plain-open: the updater's state.db, read-only
    try:
        left = M.pending(con)
    finally:
        con.close()
    return None if left == 0 else f"{left} 13F state row(s) are still under sec_edgar - open the state store once"


def _waiting_writer_lock(max_wait_s: float = 1800.0, step_s: float = 30.0):
    """core.catalog_path.writer_lock, waited for (bounded): the updater holds it for its whole run."""
    import contextlib                                                # noqa: PLC0415
    from core import catalog_path, cutover                           # noqa: PLC0415

    @contextlib.contextmanager
    def held():
        end = time.monotonic() + max_wait_s
        while True:
            try:
                cm = catalog_path.writer_lock()
                cm.__enter__()
                break
            except cutover.CutoverRefused as e:
                if "another process holds" not in str(e) or time.monotonic() >= end:
                    raise
                print(f"  the writer lock is held by another process; waiting {step_s:.0f}s", flush=True)
                time.sleep(step_s)
        try:
            yield
        finally:
            cm.__exit__(None, None, None)
    return held()


def _refresh_local(a, todo, t2c, advance: bool = False, stamp_at=None) -> int:
    """The daily refresh after T0 (--days / --ciks). Dry run unless --apply. `advance`: main() decided that this run's
    scan reached the mark, so an ok day may move it (to `stamp_at`, the time the window was computed from)."""
    import shutil                                                    # noqa: PLC0415
    import tempfile                                                  # noqa: PLC0415
    from core import cutover                                         # noqa: PLC0415
    from updater import blob                                         # noqa: PLC0415
    if a.d1 or a.audit or a.respan:
        raise cutover.CutoverRefused("refused: after T0 there is no D1, and --audit / --respan are not ported to "
                                     "the self-hosted store yet - run the daily refresh without them")
    if a.apply:
        blob.refuse_unless_live_checkout("refresh_sec_edgar --apply")          # before anything is fetched
        why = _thirteen_f_blocker()
        if why:
            raise cutover.CutoverRefused(f"refused: {why}")
    from core import catalog_path                                    # noqa: PLC0415
    stage = tempfile.mkdtemp(prefix="sec_edgar_stage_", dir=os.path.dirname(os.path.dirname(GROUPED)))
    # `failed` = SEC fetches that failed (transient; up to 5% still makes an ok day, as before T0); `refused` =
    # companies the STORE refused (unreadable file, catalogued without a file, a merge that would shrink) -
    # never transient, so any one of them makes the day partial (R1235: they were counted as fetch failures,
    # and a company refused every day was stamped ok every day)
    staged, failed, refused, errors, n_with_baseline = [], 0, 0, [], 0
    failed_ciks = []                # carried into the next run when this one moves the mark (_save_retry)
    absent_ciks = []                # companyfacts answered 404: no XBRL facts (asset-backed 10-Ks are exempt) or not
                                    # posted yet. Retried like a failure, but NOT counted in the 5% rule: a filer that
                                    # never has facts would otherwise count as failed every day (AR-209 round 3)
    why = {}                        # cik -> this run's error, for the dropped-companies file (carry_retry)
    empty = 0                       # answered, but the parser found no facts (R1236: an all-empty day was "ok")
    cat = catalog_path.connect()                                     # read-only: spans, and "is it catalogued"
    try:
        for i, cik in enumerate(todo, 1):
            time.sleep(SEC_MIN_INTERVAL)
            url = companyfacts_url(cik)
            try:
                data = json.loads(_get(url, timeout=180))
            except Exception as e:                                   # noqa: BLE001
                why[cik] = f"{type(e).__name__}{getattr(e, 'code', '')}"
                if is_absent(e):
                    absent_ciks.append(cik)
                    continue
                failed += 1
                failed_ciks.append(cik)
                errors.append(f"CIK{cik:010d}:{type(e).__name__}")
                continue
            metric, odate, vals, vint = parse_companyfacts(data)
            if not metric:
                empty += 1
                continue
            ticks = t2c.get(cik) or []
            ident = ticks[0] if ticks else f"CIK{cik:010d}"
            safe = sec_edgar_local.store_name(ident)
            path = os.path.join(GROUPED, safe + ".parquet")
            row = cat.execute("SELECT start_date, end_date FROM series WHERE series_id=?",
                              (f"sec_edgar:{ident}",)).fetchone()
            try:
                digest = _file_digest(path)          # FIRST: a change after this is caught under the lock
                prior = prior_facts(None, path)      # the local store IS the store
            except Exception as e:                                   # noqa: BLE001
                refused += 1                         # an unreadable file is REFUSED, never read as "new"
                errors.append(f"{ident}:read:{type(e).__name__}")
                continue
            if prior is None and row is not None:
                refused += 1                         # catalogued but no store file: REFUSED, never "new" (R386)
                errors.append(f"{ident}:catalogued-but-no-store-file")
                continue
            before = len(prior["metric"]) if prior else 0
            if before:
                n_with_baseline += 1
            try:
                metric, odate, vals, vint = merge_facts(prior, (metric, odate, vals, vint))
            except AssertionError as e:
                refused += 1
                errors.append(f"{ident}:merge:{e}")
                continue
            lo, hi = coverage_span(odate, vint)
            if hi is None or str(hi) > sec_edgar_local.today_utc():
                # A span that ends after today UTC is a filer typo taken by coverage_span's fallback (no fact
                # has ended), or a filing EDGAR dated on the next business day. Written, it would make
                # core.sec_edgar_local refuse the WHOLE origin copy; refused here it costs one company one
                # day, and the day is partial so nobody reads it as whole (review AR-194).
                refused += 1
                errors.append(f"{ident}:forward-span:{hi}")
                continue
            # SKIP only when the facts AND the catalogue span are already right: a run that died after its store
            # write left the span behind, and the next run must catch it up (plan; R730)
            if len(metric) == before and not a.force and row is not None and \
                    (str(row[0]), str(row[1])) == (str(lo), str(hi)):
                continue
            ent = data.get("entityName") or ident
            title = clean_title(f"{ent} ({', '.join(ticks)})" if ticks else str(ent))
            tbl = pa.table({"metric": metric, "obs_date": pa.array(odate, type=pa.date32()), "value": vals,
                            "vintage_date": pa.array(vint, type=pa.date32())})
            sp = os.path.join(stage, safe + ".parquet")
            if a.apply:
                pq.write_table(tbl, sp)
            staged.append({"ident": ident, "safe": safe, "path": path, "staged": sp, "before": before,
                           "digest": digest,
                           "after": len(metric), "span": (ident, lo, hi, title, cik),
                           "csv": csv_bytes(metric, odate, vals) if a.apply else None})
            if i % 50 == 0:
                print(f"  {i}/{len(todo)} probed, {len(staged)} changed, {failed} failed, {refused} refused",
                      flush=True)

        print(f"\ncompanies probed : {len(todo):,}\ncompanies CHANGED: {len(staged):,}  "
              f"({n_with_baseline:,} had a store baseline)" + ("" if a.apply else "  - dry run, nothing written"))
        print(f"fetch failures   : {failed:,}\nnot found (404)  : {len(absent_ciks):,}\nstore refusals   : "
              f"{refused:,}\nparsed no facts  : {empty:,}{('  e.g. ' + str(errors[:4])) if errors else ''}")
        # more than 10 answers and EVERY one parsed to nothing is a schema break, not a quiet day (the econ-updater
        # rule for an all-empty window; R1236 measured 20 of 20 empty stamped ok). A 404 is not an answer with facts.
        answered = len(todo) - failed - len(absent_ciks)
        all_empty = answered > 10 and empty == answered
        if all_empty:
            print(f"STRUCTURAL: all {empty:,} answers parsed to no facts - the companyfacts shape changed?", flush=True)
        # Any 404: is it the companies, or is companyfacts broken (moved, a URL form change, a CDN fault)? Without
        # this a day of 404s for everyone was stamped ok and moved the mark (AR-209 round 4). One fetch of a filer it
        # has always answered for tells the two apart; on every day with a 404, so no threshold can miss a break
        # that leaves a few cached answers (AR-209 round 5).
        endpoint_broken = False
        if absent_ciks:
            time.sleep(SEC_MIN_INTERVAL)
            up, err = endpoint_answers()
            if not up:
                endpoint_broken = True
                print(f"STRUCTURAL: {len(absent_ciks):,} not found (404) and the canary CIK{CANARY_CIK:010d} also "
                      f"failed ({err}) - companyfacts is broken, not the companies", flush=True)
        if not a.apply:
            return 0
        when = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        store = blob.SelfhostBlob()
        with _waiting_writer_lock():
            written, skipped, missing = [], 0, []
            for s in staged:
                if _file_digest(s["path"]) != s["digest"]:
                    # the stored FILE changed since this company was merged (a row count can stay the same):
                    # never write a union of a stale read
                    print(f"  SKIPPED {s['ident']}: the store file changed after the merge read it")
                    skipped += 1
                    continue
                # THE CSV FIRST, then the parquet. The other order lost the CSV for good on a crash between the
                # two: the next run found the new facts already in the store and, when the span had not moved,
                # skipped the company. With the parquet last, a crash leaves the store behind, so the next run
                # merges again and writes both.
                key = "series/" + urllib.parse.quote(f"sec_edgar:{s['ident']}", safe="") + ".csv"
                store.put_atomic(key, s["csv"], plain=True)          # stored plain, as this tool always did (R1206)
                from core.atomic import atomic_replace               # noqa: PLC0415
                atomic_replace(s["staged"], s["path"])                # retries a reader's brief hold (WinError 5)
                written.append(s)
            spans = [s["span"] for s in written]
            if spans:
                update_catalog(spans, False, last_updated=when)
                missing = _catalogue_misses(spans, when)
                if missing:
                    print(f"FAIL: parquet + CSV written but the catalogue does not carry {len(missing)} span(s) "
                          f"(e.g. {missing[:3]}) - after T0 there is no D1 to fall back on", flush=True)
            # OK only when the day is whole: <=5% transient fetch failures (as before T0), and nothing refused,
            # skipped or missing from the catalogue. Anything else is partial, which NEVER sets last_success (the
            # econ rule; R1235 found skipped and refused days stamped ok).
            # The 5% is of the companies SEC has facts for: a 404 is neither a failure nor in the base.
            ok_day = (failed * 20 <= len(todo) - len(absent_ciks) and not refused and not skipped and not missing
                      and not all_empty and not endpoint_broken)
            # THE MARK (see WATERMARK_OVERLAP_DAYS): moved only by a run main() allowed to move it, on an ok day. The
            # companies whose fetch failed (a few every day) are written to the retry list FIRST and fetched by the
            # next run, so a failed company never falls out of the window when the mark moves past its filing day.
            # (Requiring zero failures instead would almost never move the mark: review AR-209 round 2, R1381.)
            mark = stamp_at.isoformat(timespec="seconds") if (ok_day and advance and stamp_at) else None
            if mark:
                carry_retry(failed_ciks, absent_ciks, why, mark)   # FIRST: a crash before the stamp keeps both old
            from updater.state import StateStore                     # noqa: PLC0415
            st = StateStore()
            try:
                st.upsert_source("sec_edgar", strategy="edgar_delta", cadence="daily",
                                 status="ok" if ok_day else "partial", last_attempt_utc=when,
                                 **({"last_success_utc": mark} if mark else {}))
            finally:
                st.close()
        print(f"written: {len(written):,} company(ies) (parquet + CSV + catalogue); freshness stamped "
              f"{'ok' if ok_day else 'partial'} at {when}; scan mark {'moved to ' + mark if mark else 'NOT moved'}")
        return 0 if ok_day and len(written) == len(staged) else 1
    finally:
        cat.close()
        shutil.rmtree(stage, ignore_errors=True)


def _file_digest(path: str) -> str | None:
    """sha256 of a stored file's bytes; None when it does not exist."""
    import hashlib                                                   # noqa: PLC0415
    try:
        with open(path, "rb") as f:
            return hashlib.file_digest(f, "sha256").hexdigest()
    except FileNotFoundError:
        return None


def _catalogue_misses(spans, last_updated=None) -> list:
    """The spans the catalogue does not carry as written (read back, read-only), and with last_updated
    when one was written."""
    from core import catalog_path                                    # noqa: PLC0415
    con = catalog_path.connect()
    try:
        out = []
        for ident, lo, hi, _title, _cik in spans:
            row = con.execute("SELECT start_date, end_date, last_updated FROM series WHERE series_id=?",
                              (f"sec_edgar:{ident}",)).fetchone()
            if row is None or (str(row[0]), str(row[1])) != (str(lo), str(hi)) or \
                    (last_updated is not None and row[2] != last_updated):
                out.append(ident)
        return out
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
