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
        con = sqlite3.connect(db)
        # UPSERT, not UPDATE. An UPDATE-only path silently does nothing for a company
        # that has no catalog row yet — and a NEW registrant filing for the first time
        # is exactly that case. Two such files (CIK0002084272, SMJF) were written to
        # R2 by earlier runs of this very tool and left uncatalogued: data hosted,
        # series invisible, undownloadable. That is the "merged but not served" failure
        # this repo keeps rediscovering, reintroduced here by me.
        for ident, lo, hi, title, cik in spans:
            sid = f"sec_edgar:{ident}"
            cur = con.execute("SELECT 1 FROM series WHERE series_id=?", (sid,))
            if cur.fetchone():
                con.execute("UPDATE series SET start_date=?, end_date=? WHERE series_id=?",
                            (str(lo), str(hi), sid))
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
                # would leave the new company unsearchable even once catalogued.
                try:
                    con.execute("INSERT INTO series_fts (series_id, title, geography) "
                                "VALUES (?,?,?)", (sid, title, "US"))
                except Exception:                             # noqa: BLE001
                    pass
                n_new += 1
        con.commit()
        con.close()
    if n_new:
        print(f"   catalogued {n_new:,} NEW company/companies not previously listed",
              flush=True)
    n_d1 = 0
    if apply_d1 and spans:
        import subprocess
        wdir = os.path.join(ROOT, "api", "worker")
        tmp = os.path.join(ROOT, "data", "_sec_spans.sql")
        def esc(s):
            return str(s).replace("'", "''")
        stmts = []
        for ident, lo, hi, title, cik in spans:
            sid = f"sec_edgar:{esc(ident)}"
            # INSERT OR IGNORE then UPDATE: covers both a company already listed and
            # one filing for the first time, without needing to read D1 first. An
            # UPDATE-only path leaves a brand-new registrant's data served but
            # uncatalogued and therefore unfindable.
            stmts.append(
                f"INSERT OR IGNORE INTO series (series_id, source_id, title, frequency, "
                f"geography, category, license_id, start_date, end_date) VALUES "
                f"('{sid}','sec_edgar','{esc(title)}','Q','US','fundamentals',"
                f"'us-public-domain','{lo}','{hi}');")
            stmts.append(f"INSERT OR IGNORE INTO series_fts (series_id, title, geography) "
                         f"VALUES ('{sid}','{esc(title)}','US');")
            stmts.append(f"UPDATE series SET start_date='{lo}', end_date='{hi}' "
                         f"WHERE series_id='{sid}';")
        for j in range(0, len(stmts), 400):
            io.open(tmp, "w", encoding="utf-8").write("\n".join(stmts[j:j + 400]))
            r = subprocess.run(
                ["npx", "wrangler", "d1", "execute", "econ-catalog", "--remote",
                 "--file", tmp, "-y"],
                cwd=wdir, capture_output=True, text=True,
                encoding="utf-8", errors="replace", shell=(os.name == "nt"))
            if r.returncode != 0:
                msg = (r.stderr or "") + (r.stdout or "") or "(no output)"
                print(f"  D1 span update FAILED at {j}: {msg[-300:]}", flush=True)
                break
            n_d1 += len(stmts[j:j + 400])
        if os.path.exists(tmp):
            os.remove(tmp)
    return n_local, n_d1


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


def prior_facts(client, path):
    """What the store already holds for this company — mirror first, else R2. None if new.

    READ R2, NOT JUST THE LOCAL FILE. A CI runner has no local store, so a local-only lookup
    reports "new company" for all 17,322 of them and every merge below degenerates to a
    replace — which is the exact bug this function exists to prevent, reintroduced by
    environment.
    """
    import pyarrow.parquet as pq
    if os.path.exists(path):
        t = pq.read_table(path)
    elif client is not None:
        key = f"clean_grouped/sec_edgar/{os.path.basename(path)}"
        try:
            t = pq.read_table(io.BytesIO(
                client.get_object(Bucket=BUCKET, Key=key)["Body"].read()))
        except Exception:                                     # noqa: BLE001  (absent = new)
            return None
    else:
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
    a = ap.parse_args()

    os.makedirs(GROUPED, exist_ok=True)
    if a.audit:
        from core import r2_util
        return audit(r2_util.client())
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
        changed.append((ident, before, len(metric), max(odate)))
        # Title carries every ticker SEC maps to this CIK, matching the convention
        # applied across the source — searching GOOG must find Alphabet even though
        # the series is keyed GOOGL.
        ent = data.get("entityName") or ident
        title = f"{ent} ({', '.join(ticks)})" if ticks else str(ent)
        spans.append((ident, min(odate), max(odate), title, cik))
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
        nl, nd = update_catalog(spans, a.d1)
        print(f"catalog coverage updated: local rows={nl:,}  D1 statements={nd:,}"
              + ("" if a.d1 else "   (D1 SKIPPED — pass --d1; the worker reads D1, "
                                 "so served metadata stays stale without it)"))
    if not a.apply and changed:
        print("\nre-run with --apply to write parquet + CSV and upload to R2")
    return 0


if __name__ == "__main__":
    sys.exit(main())
