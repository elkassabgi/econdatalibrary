#!/usr/bin/env python3
"""CBS Netherlands OData v3 ingest — full statistical catalog.

License: CC BY 4.0 (Statistics Netherlands open data)
Source: opendata.cbs.nl/OData (v3 REST API)
Catalog: https://opendata.cbs.nl/ODataCatalog/Tables?$format=json

Strategy:
  * List all tables from the OData catalog (~8000+ tables)
  * For each table: GET /OData/{tableId}/TypedDataSet paginated
  * Map Period dimension → obs_date; all other dims → series_key
  * One Parquet per table; fully resumable

Run: python jobs/ingest_cbs_nl.py
     python jobs/ingest_cbs_nl.py --only 83439NED,37230NED
"""
from __future__ import annotations
import datetime as dt, json, os, sys, time, urllib.parse
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT   = os.path.join(ROOT, "data", "clean_full", "cbs_nl")
CAT   = "https://opendata.cbs.nl/ODataCatalog/Tables"
BASE  = "https://opendata.cbs.nl/ODataFeed/odata"   # ODataApi doesn't support $skip; ODataFeed does
UA    = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
         "Accept": "application/json"}
PAGE  = 10000


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_json(url: str, retries: int = 4) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404, 500):  # 500 is permanent for CBS NL dead tables
                return None
            if r.status_code in (503, 429):
                log(f"  {r.status_code} throttle, sleeping 60s")
                time.sleep(60); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(8 * (attempt + 1))
    return None


def get_catalog() -> list[dict]:
    """Get all CBS tables from the OData catalog."""
    url = f"{CAT}?$format=json&$top=10000"
    result = []
    while url:
        data = get_json(url)
        if not data:
            break
        if isinstance(data, dict):
            result.extend(data.get("value", []))
            url = data.get("odata.nextLink") or data.get("@odata.nextLink")
        else:
            result.extend(data)
            url = None
        if url:
            time.sleep(0.5)
    return result


# A year outside this range is not a period anyone published — see the 4-digit branch below for
# the measured case. Wide on purpose: genuine long history and real projections (un_wpp 2101,
# bfs scenarios to 2150) must be untouched by a guard aimed at classification codes.
_YEAR_LO, _YEAR_HI = 1500, 2100


def _year_ok(y: int) -> bool:
    return _YEAR_LO <= y <= _YEAR_HI


def parse_cbs_period(s: str) -> dt.date | None:
    """Parse CBS period codes.
    Annual:      '2022JJ00' or '2022'
    Monthly:     '2022MM01'
    Quarterly:   '2022KW01'
    Half-year:   '2022HJ01'
    School year: '2000SJ00'   -> CBS titles this "2000/'01"
    Two school years: '2003X001' -> CBS titles this "2003/'04 - 2004/'05"
    Exact date:  '19990924'   -> CBS titles this "1999, vrijdag 24 september"

    Returning None here DISCARDS THE WHOLE ROW in ingest_table, values and all. The
    three formats below were missing, and because every period of an affected table
    is the same format, that silently discarded 100% of 23 tables — 71493ned alone
    fetched 144,000,000 rows over 60 hours and wrote zero observations, with the
    measure column populated the entire time. Anything added here must be verified
    against the table's own Perioden titles, not guessed from the code letters.
    """
    s = (s or "").strip()
    try:
        if len(s) == 4 and s.isdigit():
            # A BARE 4-DIGIT CODE IS NOT AUTOMATICALLY A YEAR. Table 70170NED has no Perioden
            # dimension at all; its axes are GeboorteperiodeEersteKind and Opleidingsniveau, and
            # the first is PERIOD-NAMED but is a birth-cohort classification whose keys are
            # COMPRESSED YEAR RANGES — read live from CBS on 2026-08-03:
            #     '8589' = "Geboorteperiode: 1985-1989"
            #     '9094' = "Geboorteperiode: 1990-1994"
            #     '9597' = "Geboorteperiode: 1995-1997"
            # Unbounded, '9597' became the year 9597 — exactly the worst obs_date in cbs_nl's
            # store, across four files.
            #
            # Out of range yields None, which discards the row, and for a table like this that
            # is the correct outcome rather than a loss: it is a cross-tabulation with no time
            # axis, so it has no time-series observations to contribute. Note the docstring
            # above warns that None discards the whole row — that warning is about MISSING
            # formats, where real periods were being thrown away. This is the opposite case:
            # refusing to invent a period that was never published.
            y = int(s)
            return dt.date(y, 12, 31) if _year_ok(y) else None
        # Exact date, YYYYMMDD. MUST precede the generic <year><code> branch, which
        # would read '19990924' as year 1999 + code '09' and fall through to None.
        if len(s) == 8 and s.isdigit():
            y = int(s[:4])
            return dt.date(y, int(s[4:6]), int(s[6:8])) if _year_ok(y) else None
        if len(s) >= 6 and s[:4].isdigit():
            yr = int(s[:4]); rest = s[4:].upper()
            if not _year_ok(yr):
                return None
            if rest[:2] == "JJ":          # annual
                return dt.date(yr, 12, 31)
            # Dutch academic year yr/yr+1, dated to its END — consistent with JJ
            # dating an annual period to its last day, and correct for these tables,
            # whose measures (graduates, enrolments) are realised when the year ends.
            if rest[:2] == "SJ":          # schooljaar
                return dt.date(yr + 1, 7, 31)
            # Span of TWO academic years starting at yr, so it ends with the year
            # beginning yr+1 -> July of yr+2.
            if rest[:2] == "X0":          # two-school-year span
                return dt.date(yr + 2, 7, 31)
            if rest[:2] == "KW":          # quarter
                q = int(rest[2:4]) if rest[2:4].isdigit() else 0
                return dt.date(yr, (q-1)*3+1, 1) if 1 <= q <= 4 else None
            if rest[:2] == "MM":          # month
                m = int(rest[2:4]) if rest[2:4].isdigit() else 0
                return dt.date(yr, m, 1) if 1 <= m <= 12 else None
            if rest[:2] == "HJ":          # half-year
                h = int(rest[2:3]) if rest[2:3].isdigit() else 1
                return dt.date(yr, 1 if h == 1 else 7, 1)
            if rest[:1] == "W" and rest[1:3].isdigit():  # week
                w = int(rest[1:3])
                return dt.date.fromisocalendar(yr, max(1, min(53, w)), 1)
        # ISO date
        if len(s) == 10 and s[4] == "-":
            return dt.date.fromisoformat(s[:10])
    except Exception:
        pass
    return None


def get_table_columns(table_id: str) -> list[str] | None:
    """Get column names from first row of TypedDataSet."""
    url = f"{BASE}/{table_id}/TypedDataSet?$top=1"  # $top = $top (OData)
    data = get_json(url)
    if not data:
        return None
    rows = data.get("value", []) if isinstance(data, dict) else data
    if not rows:
        return None
    return list(rows[0].keys())


PARTITION_MIN_ROWS = 3_000_000     # below this, deep-offset cost is not worth partitioning


def table_row_count(table_id: str) -> int | None:
    """Total TypedDataSet rows, or None if the endpoint won't say."""
    try:
        r = requests.get(f"{BASE}/{table_id}/TypedDataSet/$count", headers=UA, timeout=120)
        return int(r.text.strip()) if r.status_code == 200 and r.text.strip().isdigit() else None
    except Exception:
        return None


def period_keys(table_id: str, period_col: str = "Perioden") -> list[str]:
    """The table's period-dimension values (partition keys), oldest first.

    Takes the ACTUAL column name. This was hardcoded to "Perioden", so for a table whose
    time dimension is named otherwise — 84808NED/84809NED use `JaarVanImmigratie` — it
    requested a dimension that does not exist, got [], and partitioning silently declined,
    leaving a 23-57M-row table on the quadratic deep-$skip walk. Same hardcoding mistake
    as the period-column detector it was written to support.
    """
    data = get_json(f"{BASE}/{table_id}/{period_col}?$format=json")
    if not data:
        return []
    rows = data.get("value", []) if isinstance(data, dict) else data
    return [r.get("Key") for r in rows if r.get("Key")]


_PERIOD_EXACT = ("perioden", "periods", "jaar", "period", "datum", "t_period")


def _find_period_col(table_id: str, cols: list[str]) -> str | None:
    """The column carrying the observation period, or None if the table has none.

    Exact-name matching alone is not enough. CBS names the time dimension after what it
    measures: 84809NED's is `JaarVanImmigratie` ("year of immigration"), which is not
    equal to "jaar" and so went undetected — the table then fetched 38,500,000 of its
    57,139,992 rows and wrote ZERO observations, because an undated row is dropped.

    So: exact match first, then any column whose NAME suggests a year/period AND whose
    VALUES actually parse as CBS periods. The value check is what keeps `Leeftijd` (age)
    and `MinderDan10VanDeTijd_9` (a measure) out — both merely contain "tijd", neither
    parses as a period.
    """
    exact = next((c for c in cols if c.lower() in _PERIOD_EXACT), None)
    if exact:
        return exact
    named = [c for c in cols
             if any(w in c.lower() for w in ("jaar", "year", "period", "datum"))]
    if not named:
        return None
    probe = get_json(f"{BASE}/{table_id}/TypedDataSet?$top=25")
    rows = (probe.get("value", []) if isinstance(probe, dict) else probe) or []
    if not rows:
        return None
    for c in named:
        vals = [r.get(c) for r in rows if r.get(c) not in (None, "")]
        if not vals:
            continue
        ok = sum(1 for v in vals if parse_cbs_period(str(v).strip()) is not None)
        if ok >= max(1, int(0.8 * len(vals))):     # the column really holds periods
            log(f"  {table_id}: period column detected by value = {c!r}")
            return c
    return None


def ingest_table(table_id: str, title: str, out_dir: str) -> int:
    """Download all observations for one CBS table. Returns obs count.

    PARTITIONING: `$skip` on this API is O(offset) — measured on 71493ned,
    a 10,000-row page costs 2.8 s at $skip=0, 14.9 s at 40M and 46.1 s at 144M.
    Walking a large table with one growing offset is therefore QUADRATIC: 282.7M
    rows works out to ~14.8 days. Splitting on the Perioden dimension and filtering
    (`$filter=Perioden eq '...'`) keeps every offset shallow — the same table becomes
    22 partitions of ~12.8M rows, ~37 h, and each partition is independently
    resumable. Bigger pages do not help ($top is capped at 10,000 server-side) and
    neither does $select (payload halves, time does not).
    """
    out_path = os.path.join(out_dir, f"{table_id}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  skip {table_id} ({n:,} rows)")
        return n

    # Discover columns from first row
    cols = get_table_columns(table_id)
    if cols is None:
        return 0  # table unavailable

    # Identify period and value columns
    period_col = _find_period_col(table_id, cols)
    if period_col is None:
        # NO TIME COLUMN -> every row would be discarded, because the row loop does
        #   d = parse_cbs_period(row.get(period_col or "Perioden", ""))
        #   if d is None: continue
        # and "Perioden" is not present. Previously this crawled the whole table and
        # threw away 100% of it in silence: 84809NED (57,139,992 rows) reached 38.5M
        # fetched with ZERO observations written, and 84808NED (23,253,048 rows) the
        # same, over 18 hours. REFUSE to crawl instead — a table we cannot date is not
        # ingestible as a time series, and finding that out costs one metadata call,
        # not 59 million rows.
        log(f"  SKIP {table_id}: no period column in {len(cols)} columns "
            f"(cannot date observations) — not crawled")
        return 0
    # CBS TypedDataSet has numeric values in integer or decimal columns
    # Skip metadata/code columns (non-numeric) by checking name patterns
    skip_cols = {"ID", "StringValue", "ColorCode", "Status",
                 "odata.type", "odata.id"}
    if period_col:
        skip_cols.add(period_col)

    # Try to detect value column(s): all float/int columns that aren't period/dim
    # In CBS TypedDataSet the main value is often just an unnamed column or one numeric col
    # Let's just store all numeric values in long format.
    # Stream to disk in chunks — huge tables (85477NED: 40M+ source rows) cause
    # MemoryError if buffered entirely in Python lists.
    schema = pa.schema([("series_key", pa.string()),
                        ("obs_date",   pa.date32()),
                        ("value",      pa.float64())])
    tmp_path  = out_path + ".tmp"
    ckpt_path = os.path.join(out_dir, f"{table_id}.ckpt.json")
    if os.path.exists(tmp_path):
        os.remove(tmp_path)          # stale tmp from a crashed run (unclosed = unreadable)
    FLUSH_EVERY = 500_000            # obs buffered before flushing to a part file
    FLUSH_ROWS  = 2_000_000          # also checkpoint after this many source rows,
                                     # so sparse tables (few obs/row) still resume
                                     # close to where a reboot interrupted them

    def part_path(i: int) -> str:
        return os.path.join(out_dir, f"{table_id}.part{i}.parquet")

    all_keys, all_dates, all_vals = [], [], []
    fetch_error = False
    skip = 0
    parts = 0
    written = 0
    pidx = 0          # index into `partitions`; MUST be initialised here, not only in
                      # the checkpoint-resume branch — a table with no checkpoint (the
                      # normal case, and every table after a checkpoint reset) would
                      # otherwise hit UnboundLocalError on the first loop test.

    # Resume mid-table from checkpoint (reboot/crash during a huge download).
    # Flushes only happen at page boundaries, so resuming at the saved $skip
    # offset continues exactly where the flushed parts left off.
    if os.path.exists(ckpt_path):
        try:
            with open(ckpt_path) as f:
                ck = json.load(f)
            # Validate every part is a READABLE parquet, not just present —
            # a reboot can leave the in-progress part truncated/0-byte.
            for i in range(int(ck.get("parts", 0))):
                pq.read_metadata(part_path(i))  # raises if missing or corrupt
            skip, parts, written = int(ck["skip"]), int(ck["parts"]), int(ck["written"])
            pidx = int(ck.get("pidx", 0))
            log(f"  {table_id}: resuming at skip={skip:,} ({parts} parts, {written:,} obs already flushed)")
        except Exception:
            for i in range(1000):
                if os.path.exists(part_path(i)):
                    os.remove(part_path(i))
            os.remove(ckpt_path)
            skip = parts = written = pidx = 0

    # Partition plan. `partitions == [None]` reproduces the original single-stream
    # walk exactly; a list of period keys splits the table so no offset grows deep.
    partitions = [None]
    if period_col:
        total = table_row_count(table_id)
        if total and total >= PARTITION_MIN_ROWS:
            pk = period_keys(table_id, period_col)
            if len(pk) > 1:
                partitions = pk
                log(f"  {table_id}: {total:,} rows -> partitioning by {period_col} "
                    f"into {len(pk)} slices (avoids O(offset) deep-$skip cost)")

    last_ckpt_skip = skip
    while pidx < len(partitions):
        part_val = partitions[pidx]
        flt = ""
        if part_val is not None:
            flt = "&$filter=" + urllib.parse.quote(f"{period_col} eq '{part_val}'", safe="")
        url = (f"{BASE}/{table_id}/TypedDataSet"
               f"?$top={PAGE}&$skip={skip}{flt}")
        data = get_json(url)
        if not data:
            if skip > 0 or pidx > 0:
                fetch_error = True   # died mid-table, not a dead table
            break
        rows = data.get("value", []) if isinstance(data, dict) else data
        if not rows:
            # this partition is exhausted -> advance to the next, offset back to 0
            pidx += 1
            skip = 0
            last_ckpt_skip = 0
            with open(ckpt_path, "w") as f:
                json.dump({"skip": 0, "parts": parts, "written": written, "pidx": pidx}, f)
            continue

        for row in rows:
            period_raw = row.get(period_col or "Perioden", "")
            d = parse_cbs_period(str(period_raw).strip())
            if d is None:
                continue
            # Build key from all non-numeric / non-period columns
            dim_parts = []
            for col in cols:
                if col in skip_cols or col == period_col:
                    continue
                v = row.get(col)
                if v is None:
                    continue
                if isinstance(v, (int, float)):
                    continue  # numeric → candidate value
                if isinstance(v, str) and v.strip():
                    dim_parts.append(f"{col}={v.strip()}")
            series_key = ":".join(dim_parts) or table_id

            # All numeric values for this period+key
            for col in cols:
                if col in skip_cols or col == period_col:
                    continue
                v = row.get(col)
                if v is None:
                    continue
                if not isinstance(v, (int, float)):
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                all_keys.append(f"{series_key}:{col}")
                all_dates.append(d)
                all_vals.append(fv)

        skip += len(rows)
        rows_since_ckpt = skip - last_ckpt_skip
        if len(all_vals) >= FLUSH_EVERY or (rows_since_ckpt >= FLUSH_ROWS and all_vals):
            batch = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(batch, part_path(parts), compression="zstd")
            parts += 1
            written += len(all_vals)
            all_keys, all_dates, all_vals = [], [], []
            with open(ckpt_path, "w") as f:
                json.dump({"skip": skip, "parts": parts, "written": written, "pidx": pidx}, f)
            last_ckpt_skip = skip
            log(f"    {table_id}: flushed {written:,} obs (part {parts}, skip={skip:,})")
        elif rows_since_ckpt >= FLUSH_ROWS:
            # BUFFER EMPTY after a whole FLUSH_ROWS block. Say so, loudly. This is the
            # signature of every silent-loss bug this ingester has had — unparsed period
            # VALUES (SJ/X0/YYYYMMDD), then an undetected period COLUMN — and in each case
            # the job looked perfectly healthy from outside: process alive, log scrolling,
            # row counts climbing, and not one observation kept. 71493ned fetched
            # 144,000,000 rows that way; 84809NED 38,500,000. Nothing counted the discards,
            # so nothing could report them. Now the ratio is visible in the log the
            # supervisor already writes, and a sustained 0 is a defect, not a quiet table.
            log(f"    !! {table_id}: {skip:,} rows fetched, {written:,} obs written "
                f"— {rows_since_ckpt:,} rows in this block produced NOTHING "
                f"(period_col={period_col!r}) — check the parser before trusting this run")
            # buffer empty (sparse stretch) but many source rows scanned — persist
            # the skip offset so a reboot doesn't re-scan them (no part to write).
            with open(ckpt_path, "w") as f:
                json.dump({"skip": skip, "parts": parts, "written": written, "pidx": pidx}, f)
            last_ckpt_skip = skip
        if len(rows) < PAGE:
            # short page = end of THIS partition, not necessarily the table
            pidx += 1
            skip = 0
            last_ckpt_skip = 0
            with open(ckpt_path, "w") as f:
                json.dump({"skip": 0, "parts": parts, "written": written, "pidx": pidx}, f)
            if pidx >= len(partitions):
                break
            time.sleep(0.5)
            continue
        if skip % 50000 == 0:
            lbl = f" [{part_val}]" if part_val is not None else ""
            log(f"    {table_id}{lbl}: {skip:,} rows fetched...")
        time.sleep(0.5)

    if fetch_error:
        # Persist progress and keep the checkpoint so the next run resumes here
        if all_vals:
            batch = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(batch, part_path(parts), compression="zstd")
            parts += 1
            written += len(all_vals)
            with open(ckpt_path, "w") as f:
                json.dump({"skip": skip, "parts": parts, "written": written, "pidx": pidx}, f)
        log(f"  WARNING {table_id}: fetch failed at skip={skip:,}; "
            f"{written:,} obs checkpointed for resume next run")
        return 0

    if all_vals:
        batch = pa.table({
            "series_key": pa.array(all_keys,  pa.string()),
            "obs_date":   pa.array(all_dates, pa.date32()),
            "value":      pa.array(all_vals,  pa.float64()),
        })
        pq.write_table(batch, part_path(parts), compression="zstd")
        parts += 1
        written += len(all_vals)
        all_keys, all_dates, all_vals = [], [], []

    if parts == 0:
        # Fetched rows but kept nothing. ALWAYS report the ratio: this exact outcome —
        # a completed crawl that wrote zero observations — is what 23 tables did for
        # weeks under the SJ/X0/YYYYMMDD parser gap, and what 84808/84809NED did for
        # 18 hours under the undetected period column, without ever producing a single
        # line anyone could grep for.
        if skip > 0:
            log(f"  !! {table_id}: crawled {skip:,} rows and wrote ZERO observations "
                f"(period_col={period_col!r}) — this is a DEFECT, not an empty table")
        return 0

    # Concatenate part files into the final parquet (memory-bounded, one part at a time)
    writer = pq.ParquetWriter(tmp_path, schema, compression="zstd")
    for i in range(parts):
        writer.write_table(pq.read_table(part_path(i)))
    writer.close()
    os.replace(tmp_path, out_path)
    for i in range(parts):
        os.remove(part_path(i))
    if os.path.exists(ckpt_path):
        os.remove(ckpt_path)
    n = pq.read_metadata(out_path).num_rows
    log(f"  {table_id}: DONE {n:,} obs  [{title[:50]}]")
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    only_ids: set[str] = set()
    for a in sys.argv[1:]:
        if a.startswith("--only"):
            raw = a.split("=", 1)[-1] if "=" in a else ""
            only_ids = set(raw.split(","))
        elif not a.startswith("-"):
            only_ids.add(a)

    log("Fetching CBS Netherlands table catalog...")
    tables = get_catalog()
    log(f"Found {len(tables)} tables in catalog")

    if only_ids:
        tables = [t for t in tables if t.get("Identifier") in only_ids]
        log(f"Filtered to {len(tables)} tables")

    total = 0
    for i, tbl in enumerate(tables, 1):
        tid   = tbl.get("Identifier", "")
        title = tbl.get("Title", "") or tbl.get("ShortTitle", "")
        if not tid:
            continue
        log(f"[{i}/{len(tables)}] {tid}: {title[:60]}")
        total += ingest_table(tid, title, OUT)
        time.sleep(0.3)

    log(f"DONE: {total:,} total CBS Netherlands observations")


if __name__ == "__main__":
    main()
