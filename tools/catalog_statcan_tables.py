"""Catalogue statcan at TABLE grain, with titles from Statistics Canada's own cube metadata.

Pairs with tools/derive_statcan_tables.py: it imports that module's id builder and split
expression and reads the same _split_map.json the resolver reads, so the catalogue, the objects
and the resolver share ONE definition of a unit.

TITLES ARE ALREADY ON DISK — no network call, and nothing invented. The ingest writes a `.done`
sidecar per Product ID carrying StatCan's own `cubeTitleEn` plus cansimId, frequencyCode,
archived, subjectCode, start, end and license_id. All 8,207 sidecars have a title; the survey
found zero blanks. So a statcan unit reads

    statcan:10100001  "Federal public sector employment reconciliation of Treasury Board of
                       Canada Secretariat, Public Service Commission of Canada and Statistics
                       Canada statistical universes, as at December 31"

rather than the bare Product ID, which is the difference between a searchable catalogue and
8,207 opaque numbers.

DATES COME FROM THE DATA FOR SPLIT TABLES, not from the sidecar. The sidecar's start/end describe
the WHOLE cube; a part covers a slice of it, so each part's range is computed from its own rows.
An unsplit table can take the sidecar's range directly, which is why the common case costs
nothing.

LICENCE: statcan is CONFIRMED "redistributable_attribution / CLEARED - re-host OK (attribution)"
in DATABASE_LICENSES_VERBATIM.md, and the `statcan-open` licence row is reservable. Checked
before any row is written, because a catalogue row is an offer to serve.

--apply writes; without it this prints what it would do and changes nothing.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import datetime as dt
import shutil  # noqa: F401  (kept for callers; the db backup below uses sqlite's online backup API)
import sqlite3
import sys
import urllib.parse

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

from derive_statcan_tables import (SOURCE, STORE, MAX_ROWS_DEFAULT,   # noqa: E402
                                   csv_key, part_expr, unit_id)

LICENSE_ID = "statcan-open"
BATCH = 10_000
# StatCan frequencyCode -> the catalogue's single-letter frequency. Codes seen in the store:
# 18 (2,919), 12 (2,673), 6 (859), 9 (621), 13 (406), 16 (319), 15 (132), 11 (81). Anything not
# listed stays NULL rather than being guessed — a wrong frequency is worse than an absent one.
FREQ = {1: "A", 2: "A", 4: "Q", 6: "M", 7: "M", 9: "A", 11: "A", 12: "A", 13: "A",
        14: "D", 15: "W", 16: "D", 17: "Q", 18: "M", 19: "M", 20: "Q", 21: "A"}


def key_prefix(prefix: str = "series") -> str:
    """Every key of this source starts with this: '<prefix>/statcan%3A'."""
    return f"{prefix}/{urllib.parse.quote(SOURCE + ':', safe='')}"


def load_r2_keys(path: str) -> set:
    """The R2 listing under <prefix>/<source>%3A - one key per line, as list_objects_v2 returns
    it. utf-8-sig: a BOM-written listing would otherwise corrupt exactly one key, silently."""
    with open(path, encoding="utf-8-sig") as fh:
        return {ln.strip() for ln in fh if ln.strip()}


def load_listing_meta(path: str):
    """The listing's provenance sidecar (<path>.meta.json: bucket, prefix, listed_at_utc, count,
    tool) or None when the listing has no recorded instrument (R527)."""
    mp = path + ".meta.json"
    if not os.path.exists(mp):
        return None
    with open(mp, encoding="utf-8") as fh:
        return json.load(fh)


def listing_problems(keys: set, meta, prefix: str = "series", bucket: str = "econ-data",
                     max_age_hours: float = 24.0, now=None) -> list:
    """Why a listing must NOT be trusted to gate the catalogue. An empty or wrong listing would
    otherwise DISARM the completeness guard (every over-cap table 'has no object') and drop every
    row - a green zero-row run (R503/R508: every dangerous failure returned success)."""
    out = []
    if not keys:
        out.append("listing is EMPTY")
    pre = key_prefix(prefix)
    bad = sorted(k for k in keys if not k.startswith(pre))
    if bad:
        out.append(f"{len(bad):,} key(s) are not under {pre} (first: {bad[:3]}) - wrong bucket, "
                   f"prefix or source")
    if meta is None:
        out.append("listing has no provenance sidecar (<path>.meta.json) - produce it with "
                   "--list-r2-keys so bucket, prefix, time and count are recorded")
    else:
        if meta.get("count") != len(keys):
            out.append(f"sidecar count {meta.get('count')} != {len(keys):,} keys read")
        if meta.get("prefix") != pre:
            out.append(f"sidecar prefix {meta.get('prefix')!r} != {pre!r}")
        if meta.get("bucket") != bucket:
            out.append(f"sidecar bucket {meta.get('bucket')!r} != {bucket!r} - a listing of another "
                       f"store would gate the catalogue on the wrong objects")
        listed = None
        try:
            listed = dt.datetime.strptime(meta.get("listed_at_utc") or "", "%Y-%m-%dT%H:%M:%SZ")
            listed = listed.replace(tzinfo=dt.timezone.utc)
        except (TypeError, ValueError):
            out.append(f"sidecar listed_at_utc {meta.get('listed_at_utc')!r} is not an ISO UTC stamp")
        if listed is not None:
            now = now or dt.datetime.now(dt.timezone.utc)
            age_h = (now - listed).total_seconds() / 3600.0
            if age_h > max_age_hours:
                out.append(f"listing is {age_h:,.1f} h old (> {max_age_hours:g} h) - objects written "
                           f"since are invisible to the orphan gate; re-list")
            elif age_h < -0.25:  # two-sided: a future stamp would never expire
                out.append(f"listing is stamped {-age_h:,.1f} h in the FUTURE ({meta.get('listed_at_utc')}) "
                           f"- clock skew or an edited sidecar; re-list")
    return out


def unlisted_catalogue_rows(con, keys: set, prefix: str = "series") -> list:
    """F6: the population at risk is the CATALOGUE, not this run's emission. Every row of the
    source whose object is not in the listing is advertised-but-undeliverable (the namq_10_gdp
    shape) whether or not this run emitted it - e.g. 20 legacy `statcan:V...` ids."""
    ids = [r[0] for r in con.execute("SELECT series_id FROM series WHERE source_id=?", (SOURCE,))]
    return sorted(i for i in ids if object_key(i, prefix) not in keys)


def object_key(unit: str, prefix: str = "series") -> str:
    """The key the derive wrote this unit under: the SAME builder, so encoding cannot drift."""
    return csv_key(prefix, unit)


def pids_with_parts(keys: set, prefix: str = "series") -> set:
    """Table ids (RAW, unquoted - the split map and filenames are raw) that have at least one
    PART object in R2 ('#' is quoted as %23 by csv_key)."""
    pre = key_prefix(prefix)
    out = set()
    for k in keys:
        if k.startswith(pre) and "%23" in k:
            out.add(urllib.parse.unquote(k[len(pre):].split("%23", 1)[0]))
    return out


def refused_set(sum_obj, key):
    """(ids, provenance) from a derive summary's `refused` list. provenance is one of
    "full" | "partial" | "unreadable".

    A REFUSAL LIST IS EVIDENCE ONLY IF THE RUN THAT WROTE IT COVERED THE STORE. The derives write
    their summary unconditionally - `--dry-run`, `--only` and `--limit` runs included - and each
    cataloguer prints `--only <ids>` as the remedy for its own refusal, so following that
    instruction is precisely what leaves a scoped record behind (R843 addendum).

    Both directions matter, and they fail differently:
      * an EMPTY list from a scoped run makes "not seen by the derive" an assertion nobody
        checked - R219's single confident cause;
      * a NON-EMPTY list from a scoped run is worse: it can mark a table "correctly NOT
        catalogued" that a full run would have split without trouble.

    "unreadable" is kept distinct from "partial" so the operator is told WHICH it was; collapsing
    them is the fail-quiet shape of R503. A caller must treat anything but "full" as UNKNOWN -
    never as empty.
    """
    if not isinstance(sum_obj, dict):
        return set(), "unreadable"
    lst = sum_obj.get("refused")
    if not isinstance(lst, list):
        return set(), "unreadable"
    # `refused_scope` is the list's own provenance; `scope` describes the CAP and is accepted
    # only for back-compatibility with summaries written before the list had its own key.
    scope = sum_obj.get("refused_scope") or ("full" if sum_obj.get("scope") == "full" else None)
    ids = {r.get(key) for r in lst if isinstance(r, dict) and r.get(key) is not None}
    return ids, ("full" if scope == "full" else "partial")


def summary_coverage(sum_obj, n_store_now):
    """One line saying what the summary actually covers - the cheapest guard of all.

    `considered: 11` against a store of 2,442 makes the scope error self-evident with no tag to
    interpret. Printed unconditionally wherever the summary is read.
    """
    if not isinstance(sum_obj, dict):
        return "summary: UNREADABLE"
    # NOT `a or b or c`: a legitimate `processed: 0` is falsy and would fall through
    # to `considered`, reporting a run that processed NOTHING as having covered
    # everything - the fail-open this whole line exists to prevent.
    con = None
    for _k in ("processed", "processed_tables", "considered"):
        if sum_obj.get(_k) is not None:
            con = sum_obj[_k]
            break
    store = sum_obj.get("store_files") or sum_obj.get("store_shards")
    bits = ["scope=%s" % (sum_obj.get("scope") or "UNRECORDED"),
            "refused_scope=%s" % (sum_obj.get("refused_scope") or "UNRECORDED")]
    if con is not None:
        bits.append("covered %s of %s at the time" % (f"{con:,}", f"{store:,}" if store else "?"))
    bits.append("store holds %s now" % f"{n_store_now:,}")
    return "summary: " + ", ".join(bits)

def classify_absent(absent: dict, keys: set, refused: set, prefix: str = "series") -> tuple:
    """Split the over-cap tables that have NO split-map entry into three sets, by what R2 holds
    AND what the derive recorded (never assert a cause that was not checked - R219):

      acceptable    no object of the table exists (no whole-table key, no part key) AND the
                    derive RECORDED refusing it (logs/statcan_tables_summary.json 'refused'):
                    nothing was written, nothing can be advertised, so NOT cataloguing it is
                    exactly right;
      unrefused     no object exists but the derive did NOT refuse it - a table ingested or grown
                    past the cap after the derive ran. Real data, never derived: refuse, never
                    silently omit;
      unacceptable  an object exists - a whole-table object written above the cap (an id that
                    may point at an undeliverable object) or parts with no dim to name them (a
                    lost or stale map). Those refuse, as before.

    Without a listing the guard below refuses on any absent table, and it did: at the cap the
    derive really ran with (3,000,000) the absent set is exactly the 5 tables the derive REFUSED
    for having no usable splitter.
    """
    with_parts = pids_with_parts(keys, prefix)
    acceptable, unrefused, unacceptable = {}, {}, {}
    for pid, n in absent.items():
        exists = object_key(unit_id(pid), prefix) in keys or pid in with_parts
        if exists:
            unacceptable[pid] = n
        elif pid in refused:
            acceptable[pid] = n
        else:
            unrefused[pid] = n
    return acceptable, unrefused, unacceptable


def filter_rows_by_listing(rows: list, keys: set, prefix: str = "series") -> tuple:
    """Keep only rows whose object exists in R2. A catalogue row with no object is an id that
    404s - the exact shape that made namq_10_gdp advertised-but-undeliverable."""
    kept, dropped = [], []
    for r in rows:
        (kept if object_key(r[0], prefix) in keys else dropped).append(r)
    return kept, dropped


def orphan_keys(rows: list, keys: set, prefix: str = "series") -> set:
    """The direction that hurts (R501): objects in R2 with NO catalogue row - data held while
    the catalogue says it does not exist (the R2-side twin of series.ts's unlisted series). A
    dim value that DISAPPEARS on re-ingest leaves its object orphaned; without this it is
    invisible, while a value that APPEARS is emitted, dropped and named."""
    return keys - {object_key(r[0], prefix) for r in rows}


def expectation_problems(kept: int, dropped: int, expect_kept, expect_dropped) -> list:
    """State the predicted counts BEFORE the run and STOP if the run disagrees (R566). A kept
    fraction floor cannot see one unexpected drop; an exact expectation can."""
    out = []
    if expect_kept is not None and kept != expect_kept:
        out.append(f"kept {kept:,} != expected {expect_kept:,}")
    if expect_dropped is not None and dropped != expect_dropped:
        out.append(f"dropped {dropped:,} != expected {expect_dropped:,}")
    return out


def write_r2_listing(path: str, prefix: str = "series", bucket: str = "econ-data") -> int:
    """Produce the listing WITH its provenance sidecar - the instrument (R527). list_objects_v2
    under <prefix>/statcan%3A: ~1 class-A call per 1,000 keys, no D1."""
    from core import r2_util  # noqa: E402  (boto3 only when listing)
    s3 = r2_util.client()
    pre = key_prefix(prefix)
    n = 0
    started = dt.datetime.now(dt.timezone.utc)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre):
            for o in page.get("Contents", []):
                fh.write(o["Key"] + "\n")
                n += 1
    meta = {"bucket": bucket, "prefix": pre, "count": n,
            "listed_at_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tool": "tools/catalog_statcan_tables.py --list-r2-keys"}
    with open(path + ".meta.json", "w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=1)
    return n


def _discard(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def backup_sqlite(src_path: str, dst_path: str, pages: int = 4096, sleep: float = 0.05,
                  stall_steps: int = 20, max_restarts: int = 50) -> str:
    """A CONSISTENT copy of a live SQLite db via the online backup API, then `PRAGMA quick_check`
    on the copy. A `shutil.copyfile` of a db in journal_mode=delete while a writer holds a
    transaction is a torn image with the same byte count (R578): size equality verifies nothing.

    INCREMENTAL (R579): `pages=-1` copies the whole db inside ONE backup step holding the read
    lock for the entire copy - measured ~27 MB/s, so ~7 min for 11.9 GB, longer than the 180 s
    patience every other tool here has, and the failure lands on the OTHER job. With `pages`
    the lock is released between steps; a write by another connection RESTARTS the copy, so a
    writer that never pauses livelocks it - `db_holders()` is checked first and the rebuild
    refuses while any other process holds the file.

    STALL SIGNALS (R580/R581/R583 - each round found a case the previous guard missed):
      (a) `remaining` UNCHANGED for `stall_steps` consecutive steps: at the shipped pages=4096
          a restarted copy completes its full chunk from the beginning every time, so under a
          livelock `remaining` never moves - and never RISES either, so a restart-only detector
          is inert (R583);
      (b) an UPWARD jump is a restart: reset the no-progress counter (one ordinary commit
          restarts the copy once and the next attempt runs to completion - R581's false abort)
          and count it; more than `max_restarts` restarts is a livelock at a small page size.
    The copy is written to `<dst>.partial` and renamed only after quick_check == 'ok', so no
    failure path leaves a torn copy under the authoritative name (R583).
    Returns the quick_check result ('ok' or the first problem)."""
    tmp_path = dst_path + ".partial"
    _discard(tmp_path)
    st = {"last": None, "no_dec": 0, "restarts": 0}

    def _abort(remaining, total, why):
        holders = db_holders(src_path)
        raise RuntimeError(
            f"backup stalled: {why} ({remaining:,} of {total:,} pages remaining, {st['restarts']} "
            f"restart(s) by foreign writes); holders now: "
            f"{holders or '[] (a transient writer - none holds the file at this instant)'}")

    def progress(status, remaining, total):
        last = st["last"]
        st["last"] = remaining
        if last is None:
            return
        if remaining > last:                               # restart by a foreign write
            st["restarts"] += 1
            st["no_dec"] = 0
            if st["restarts"] > max_restarts:
                _abort(remaining, total, f"more than {max_restarts} restarts")
        elif remaining < last:
            st["no_dec"] = 0
        else:                                              # no progress this step
            st["no_dec"] += 1
            if st["no_dec"] >= stall_steps:
                _abort(remaining, total, f"no progress for {stall_steps} consecutive steps - a "
                                         f"writer restarts the copy before it can advance")

    src = sqlite3.connect(src_path, timeout=180.0)
    dst = sqlite3.connect(tmp_path)
    try:
        src.backup(dst, pages=pages, sleep=sleep, progress=progress)
        qc = dst.execute("PRAGMA quick_check").fetchone()[0]
    except BaseException:
        dst.close(); src.close()
        _discard(tmp_path)
        raise
    dst.close(); src.close()
    if qc != "ok":
        _discard(tmp_path)
        return qc
    os.replace(tmp_path, dst_path)
    return qc


def db_holders(db_path: str, pids=None) -> list:
    """(pid, name) of OTHER processes holding `db_path` (or its -journal/-wal) open, PLUS
    (pid, 'UNINSPECTABLE:<name>') for any Python process whose handles could not be read
    (R580: "could not look" must never read as "does not hold" - R503). `pids` restricts the
    scan (tests); handle enumeration on Windows is slow per process."""
    import psutil
    target = os.path.normcase(os.path.abspath(db_path))
    out = []
    # Only Python processes open catalog.db here (every tool in this repo is Python; wrangler and
    # the crawlers' shells never touch it). Scanning every process's handles on Windows takes
    # minutes and can stall on system processes - a full scan is not an instrument.
    py = {"python.exe", "python3.exe", "pythonw.exe", "python", "python3"}
    for p in psutil.process_iter(["pid", "name"]):
        if p.info["pid"] == os.getpid() or (p.info["name"] or "").lower() not in py:
            continue
        if pids is not None and p.info["pid"] not in pids:
            continue
        try:
            for f in p.open_files():
                fp = os.path.normcase(f.path)
                if fp == target or fp.startswith(target + "-"):
                    out.append((p.info["pid"], p.info["name"]))
                    break
        except psutil.NoSuchProcess:
            continue
        except (psutil.Error, OSError):
            out.append((p.info["pid"], f"UNINSPECTABLE:{p.info['name']}"))
    return out


def fts_off_range_count(con) -> int:
    """R481 shape in the second place a row lives: EVERY series_fts entry with no `series` row
    (an orphan of any spelling - 'statcanX', a bare legacy 'V2132579', anything) would survive a
    range reconcile invisibly. Anti-join against `series`, one scan of series_fts: measured
    5.7 s on the 11.9 GB catalogue (reviewer, R580) - cheap enough to run every time."""
    lo, hi = source_range()
    # In-range orphans are exactly what the range reconcile removes; only the ones OUTSIDE the
    # range would survive it, so those are the refusal.
    return con.execute("SELECT count(*) FROM series_fts f WHERE NOT EXISTS "
                       "(SELECT 1 FROM series s WHERE s.series_id = f.series_id) "
                       "AND NOT (f.series_id >= ? AND f.series_id < ?)", (lo, hi)).fetchone()[0]


def rebuild_fts(con) -> int:
    """`series_fts` is a STANDALONE fts5 table with its own content store and no triggers on
    `series`, so `INSERT INTO series_fts(series_fts) VALUES('rebuild')` re-indexes the FTS's own
    copy and never reads `series` (R575: 466,341 rows would have been reachable by exact id only,
    under a printed "rebuilt"). The repo's idiom (core/catalog.py, core/broaden_catalog.py,
    core/apply_title_wave.py): delete, re-insert from `series`, ASSERT the counts agree INSIDE the
    transaction, and ROLLBACK on any mismatch (R578: a refusal must not ship its own damage)."""
    if con.in_transaction:  # commit the caller's finished work so a rollback here undoes ONLY ours
        con.commit()
    con.execute("DELETE FROM series_fts")
    con.execute("INSERT INTO series_fts(series_id, title, geography) "
                "SELECT series_id, title, geography FROM series")
    n_s = con.execute("SELECT count(*) FROM series").fetchone()[0]
    n_f = con.execute("SELECT count(*) FROM series_fts").fetchone()[0]
    n_src = con.execute("SELECT count(*) FROM series_fts WHERE series_id LIKE ?",
                        (SOURCE + ":%",)).fetchone()[0]
    n_src_series = con.execute("SELECT count(*) FROM series WHERE source_id=?", (SOURCE,)).fetchone()[0]
    if n_f != n_s or n_src != n_src_series:
        con.rollback()
        print(f"REFUSING (rolled back): series_fts {n_f:,} != series {n_s:,}, or {SOURCE} {n_src:,} != "
              f"{n_src_series:,} after the rebuild - the index is unchanged")
        return 1
    con.commit()
    print(f"series_fts rebuilt from series: {n_f:,} rows (series {n_s:,}; {SOURCE} in fts {n_src:,}, "
          f"in series {n_src_series:,})")
    return 0


FTS_IDS_PER_STMT = 500  # series_fts(series_id UNINDEXED): EVERY statement is a full scan, so
#                         raise predicate arity, never add statements (CLAUDE.md, R492, R542, R576)


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def fts_has(con, ids: list) -> list:
    """Which of `ids` are present in series_fts - the second place a catalogue row lives (R481).
    ONE `IN (...)` statement per 500 ids: a per-id probe is a full scan of ~13.5M rows each."""
    out = []
    for ch in _chunks(list(ids), FTS_IDS_PER_STMT):
        marks = ",".join("?" * len(ch))
        out.extend(r[0] for r in con.execute(
            f"SELECT series_id FROM series_fts WHERE series_id IN ({marks})", ch))
    return [i for i in ids if i in set(out)]


def fts_delete_ids(con, ids: list) -> None:
    for ch in _chunks(list(ids), FTS_IDS_PER_STMT):
        marks = ",".join("?" * len(ch))
        con.execute(f"DELETE FROM series_fts WHERE series_id IN ({marks})", ch)


def source_range(prefix_id: str = SOURCE + ":") -> tuple:
    """Half-open series_id range covering one source: ['statcan:', 'statcan;')."""
    return prefix_id, prefix_id[:-1] + chr(ord(prefix_id[-1]) + 1)


def reconcile_fts_source(con) -> int:
    """Source-scoped FTS reconcile (the D1 sync's own idiom, sync_catalog_d1.py:407): ONE range
    DELETE over the source's ids, one INSERT from `series`, one range COUNT to verify - three
    scans of series_fts, regardless of how many rows the source has. Never a whole-catalogue
    delete (R576: a full rebuild holds an exclusive lock on the 11.9 GB db for its duration)."""
    lo, hi = source_range()
    n_src = con.execute("SELECT count(*) FROM series WHERE source_id=?", (SOURCE,)).fetchone()[0]
    # A source row whose id lacks the 'statcan:' prefix would be missed by the range DELETE and
    # re-added by the INSERT - a duplicate FTS row on every run. Refuse before touching anything.
    off_range = con.execute("SELECT count(*) FROM series WHERE source_id=? AND NOT "
                            "(series_id >= ? AND series_id < ?)", (SOURCE, lo, hi)).fetchone()[0]
    if off_range:
        print(f"REFUSING: {off_range:,} {SOURCE} row(s) have an id outside [{lo!r}, {hi!r}) - the "
              f"range reconcile cannot cover them")
        return 1
    stale = fts_off_range_count(con)
    if stale:
        print(f"REFUSING: {stale:,} series_fts entr(y/ies) OUTSIDE [{lo!r}, {hi!r}) have NO series row "
              f"- a CATALOGUE-WIDE condition (orphans of any source or spelling), which a range "
              f"reconcile would leave in place invisibly (R481/R580/R581). Find them with: "
              f"SELECT f.series_id FROM series_fts f WHERE NOT EXISTS (SELECT 1 FROM series s WHERE "
              f"s.series_id = f.series_id)")
        return 1
    if con.in_transaction:  # commit the caller's finished work so a rollback here undoes ONLY ours
        con.commit()
    con.execute("DELETE FROM series_fts WHERE series_id >= ? AND series_id < ?", (lo, hi))
    con.execute("INSERT INTO series_fts(series_id, title, geography) "
                "SELECT series_id, title, geography FROM series WHERE source_id=?", (SOURCE,))
    n_fts = con.execute("SELECT count(*) FROM series_fts WHERE series_id >= ? AND series_id < ?",
                        (lo, hi)).fetchone()[0]
    if n_fts != n_src:
        con.rollback()
        print(f"REFUSING (rolled back): series_fts would hold {n_fts:,} {SOURCE} rows but series "
              f"holds {n_src:,} - the index is unchanged")
        return 1
    con.commit()
    print(f"series_fts reconciled for {SOURCE}: {n_fts:,} in the index, {n_src:,} in series")
    return 0


def dump_rows(con, ids: list, path: str) -> int:
    """Full `series` rows for `ids` to a TSV; returns the number of rows written (verified by
    re-reading the file). The one population this tool cannot regenerate is exactly what a purge
    deletes, so it is backed up first (R565 rule 2)."""
    cols = [r[1] for r in con.execute("PRAGMA table_info(series)")]
    rows = []
    for i in ids:
        rows.extend(con.execute(f"SELECT {', '.join(cols)} FROM series WHERE series_id=?", (i,)))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\t".join(cols) + "\n")
        for r in rows:
            fh.write("\t".join("" if v is None else str(v).replace("\t", " ").replace("\n", " ")
                               for v in r) + "\n")
    with open(path, encoding="utf-8") as fh:
        return sum(1 for _ in fh) - 1


def audit_catalogue(con, keys: set, prefix: str, purge: bool, expect_unlisted=None) -> int:
    """F6 gate over the population at risk. Returns the exit code. A purge removes the rows from
    BOTH places they live (series and series_fts, R481) after dumping the full rows to a file
    whose count is verified first."""
    bad = unlisted_catalogue_rows(con, keys, prefix)
    n_all = con.execute("SELECT count(*) FROM series WHERE source_id=?", (SOURCE,)).fetchone()[0]
    print(f"catalogue audit: {n_all:,} {SOURCE} row(s) in the catalogue; {len(bad):,} have NO object "
          f"in the listing" + (f"; first: {bad[:8]}" if bad else ""))
    if expect_unlisted is not None and len(bad) != expect_unlisted:
        print(f"REFUSING: {len(bad):,} unlisted row(s) found, --expect-unlisted {expect_unlisted} - the "
              f"population moved; re-measure before purging (R500)")
        return 1
    if not bad:
        return 0
    ap_ = os.path.join(ROOT, "logs", "_catalog_statcan_unlisted_ids.txt")
    with open(ap_, "w", encoding="utf-8") as fh:
        for i in bad:
            fh.write(i + "\n")
    if not purge:
        print(f"REFUSING: {len(bad):,} advertised id(s) have no object (list: {ap_}) - the "
              f"namq_10_gdp shape. Re-run with --purge-unlisted to delete them from the LOCAL "
              f"catalogue (nothing here touches D1).")
        return 1
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dump = os.path.join(ROOT, "logs", f"_catalog_statcan_purged_rows_{stamp}.tsv")
    dumped = dump_rows(con, bad, dump)
    if dumped != len(bad):
        print(f"REFUSING purge: dumped {dumped:,} row(s) != {len(bad):,} to delete ({dump})")
        return 1
    print(f"backup of the {dumped:,} row(s) about to be deleted: {dump}")
    in_fts = fts_has(con, bad)
    con.executemany("DELETE FROM series WHERE series_id=? AND source_id=?", [(i, SOURCE) for i in bad])
    fts_delete_ids(con, bad)
    con.commit()
    left = unlisted_catalogue_rows(con, keys, prefix)
    left_fts = fts_has(con, bad)
    print(f"purged {len(bad):,} unlisted row(s) from series ({len(in_fts):,} of them were also in "
          f"series_fts); remaining: series {len(left):,}, series_fts {len(left_fts):,}")
    return 0 if not left and not left_fts else 1


def sidecars() -> dict:
    """{productId: sidecar dict} — StatCan's own cube metadata, written at ingest time."""
    out = {}
    for f in glob.glob(os.path.join(STORE, "*.done")):
        try:
            j = json.load(open(f, encoding="utf-8"))
        except (OSError, ValueError):
            continue
        pid = str(j.get("productId") or os.path.splitext(os.path.basename(f))[0])
        out[pid] = j
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-rows", type=int, default=None,
                    help="must match the value the derive ran with, or the completeness guard "
                         "below compares against the wrong set of oversized tables. Default: "
                         "whatever the derive RECORDED in its summary; falls back to "
                         f"{MAX_ROWS_DEFAULT:,} only when the summary predates that record.")
    ap.add_argument("--r2-keys", metavar="PATH",
                    help="a listing of the R2 keys under series/<source>%%3A (one per line). With "
                         "it, (1) an over-cap table with no split entry AND no object in R2 is "
                         "accepted as refused-by-the-derive, nothing to advertise, instead of "
                         "blocking the whole catalogue, and (2) every emitted row must have an "
                         "object or it is dropped and NAMED - an id with no object is a 404.")
    ap.add_argument("--prefix", default="series",
                    help="the derive's --prefix: the object-key prefix (default series)")
    ap.add_argument("--bucket", default="econ-data")
    ap.add_argument("--list-r2-keys", metavar="PATH",
                    help="write the R2 listing under <prefix>/statcan%%3A to PATH, with its "
                         "provenance sidecar PATH.meta.json, and exit")
    ap.add_argument("--max-orphans", type=int, default=0,
                    help="refuse when more than this many R2 objects would have NO catalogue "
                         "row (default 0: data held while the catalogue denies it is the "
                         "failure that hurts, R501)")
    ap.add_argument("--min-kept-fraction", type=float, default=0.99,
                    help="refuse when fewer than this fraction of emitted rows have an object "
                         "(an empty or wrong listing would otherwise drop every row and "
                         "return success)")
    ap.add_argument("--max-listing-age-hours", type=float, default=24.0,
                    help="refuse a listing older than this (objects written since would be "
                         "invisible to the orphan gate)")
    ap.add_argument("--audit-catalogue", action="store_true",
                    help="no scan: compare the EXISTING catalogue rows of the source against "
                         "--r2-keys and report every advertised id with no object, then exit")
    ap.add_argument("--purge-unlisted", action="store_true",
                    help="with --apply or --audit-catalogue: DELETE catalogue rows of the source "
                         "whose object is not in the listing (an explicit operator decision; "
                         "the run refuses on survivors without it)")
    ap.add_argument("--reconcile-fts", action="store_true",
                    help="no scan: source-scoped series_fts reconcile (range DELETE + INSERT from "
                         "series + range COUNT = 3 scans) and exit - the standalone repair for R575")
    ap.add_argument("--rebuild-fts", action="store_true",
                    help="no scan: rebuild the WHOLE series_fts from series (guarded: needs "
                         "--i-understand-full-rebuild and takes a db backup first)")
    ap.add_argument("--i-understand-full-rebuild", action="store_true")
    ap.add_argument("--expect-unlisted", type=int, default=None,
                    help="with --audit-catalogue/--purge-unlisted: refuse unless exactly this many "
                         "catalogue rows have no object (a purge on an unmeasured population is R500)")
    ap.add_argument("--expect-kept", type=int, default=None,
                    help="refuse unless exactly this many rows survive the listing gate")
    ap.add_argument("--expect-dropped", type=int, default=None,
                    help="refuse unless exactly this many rows are dropped by the listing gate")
    a = ap.parse_args()

    # THE CAP IS THE DERIVE'S, NOT OURS (R833). Reading it from the derive's own summary
    # removes the inference that made a 500,000-vs-3,000,000 mismatch look like a frozen
    # pipeline: 367 tables the derive had correctly written whole reported as 'no split-map
    # entry', and I escalated a multi-day re-derive that was never needed.
    # `_sum` is initialised HERE, not only inside the try: it is reused far below for the
    # refusal list, and an unreadable summary would otherwise raise NameError there.
    recorded, rec_scope, _sum = None, None, None
    try:
        _sum = json.load(open(os.path.join(ROOT, "logs",
                                           "statcan_tables_summary.json"),
                              encoding="utf-8"))
        recorded = _sum.get("max_rows")
        # A CAP IS ONLY EVIDENCE IF THE RUN THAT SET IT COVERED THE STORE. The summary is
        # written unconditionally, including by --dry-run/--only/--limit, so a one-table
        # dry run at another cap would otherwise be adopted as fact and reconstitute the
        # very refusal R832 records - this time with a provenance line attached.
        rec_scope = _sum.get("scope") or ("dry_run" if _sum.get("dry_run") else None)
        if recorded is not None and rec_scope not in (None, "full"):
            print(f"IGNORING the recorded cap {recorded!r}: the derive run that wrote it\n"
                  f"  was scoped {rec_scope!r}, not a full-store run, so it is not\n"
                  f"  evidence about this store's cap. Pass --max-rows explicitly.")
            recorded = None
        if recorded is not None and not isinstance(recorded, int):
            # a JSON string/float/list would crash int() with a traceback; refuse in the
            # file's own fail-closed style instead.
            print(f"IGNORING the recorded cap {recorded!r}: not an integer.")
            recorded = None
        if isinstance(recorded, bool) or (isinstance(recorded, int) and recorded <= 0):
            print(f"IGNORING the recorded cap {recorded!r}: a cap must be a positive "
                  f"integer.")
            recorded = None
    except Exception as e:                                     # noqa: BLE001
        print(f"derive summary unreadable ({type(e).__name__}) - cannot confirm the cap")
    if a.max_rows is None:
        if recorded is None:
            # NEVER SILENT. A summary written before max_rows was recorded cannot confirm
            # the cap, and a default that merely LOOKS shared is exactly the trap.
            a.max_rows = MAX_ROWS_DEFAULT
            print(f"WARNING: the derive summary does NOT record max_rows, so the cap it ran"
                  f" with is UNKNOWN.\n  Falling back to {MAX_ROWS_DEFAULT:,}. If the derive"
                  f" used another value every 'no split-map entry'\n  below is an artifact -"
                  f" re-run the derive, or pass --max-rows explicitly.")
        else:
            a.max_rows = int(recorded)
            print(f"cap {a.max_rows:,} adopted from the derive's recorded max_rows")
    elif recorded is not None and int(recorded) != int(a.max_rows):
        # FAIL CLOSED. A disagreement here silently changes which tables are checked for a
        # split entry, which is the entire completeness guard.
        print(f"REFUSING: --max-rows {a.max_rows:,} disagrees with the cap the derive"
              f" recorded ({int(recorded):,}).\n  The completeness guard would compare"
              f" against the wrong set of oversized tables.\n  Pass the recorded value, or"
              f" re-run the derive at the one you want.")
        return 1
    if a.list_r2_keys:
        n = write_r2_listing(a.list_r2_keys, a.prefix, a.bucket)
        print(f"{n:,} key(s) listed under {key_prefix(a.prefix)} -> {a.list_r2_keys} "
              f"(+ .meta.json)")
        return 0
    if a.reconcile_fts:
        con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
        try:
            return reconcile_fts_source(con)
        finally:
            con.close()
    if a.rebuild_fts:
        # Whole-catalogue destructive write: guarded by an explicit flag AND a file backup of the
        # db first (repo convention: data/catalog.db.pre_*), with the counts previewed (R576).
        dbp = os.path.join(ROOT, "data", "catalog.db")
        if not a.i_understand_full_rebuild:
            print("REFUSING --rebuild-fts: it deletes and re-inserts the WHOLE series_fts (~13.5M rows) "
                  "under an exclusive lock, and the consistent backup taken first READ-LOCKS the db "
                  "for minutes (writers with the usual 180 s patience fail - R579). Use --reconcile-fts "
                  "(source-scoped) unless the whole index is wrong; if it is, stop every other job that "
                  "uses catalog.db, then pass --i-understand-full-rebuild.")
            return 1
        holders = db_holders(dbp)
        if holders:
            print(f"REFUSING --rebuild-fts: {len(holders)} other process(es) hold {dbp} open: {holders[:6]} "
                  f"- the backup would block their writes for minutes (R579). Wait or stop them first.")
            return 1
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bak = dbp + f".pre_fts_rebuild_{stamp}"
        print(f"backing up {dbp} ({os.path.getsize(dbp)/1e9:.2f} GB) -> {bak} (sqlite online backup)")
        qc = backup_sqlite(dbp, bak)
        if qc != "ok":
            print(f"REFUSING: backup quick_check = {qc!r}")
            return 1
        print(f"backup verified: quick_check ok, {os.path.getsize(bak)/1e9:.2f} GB")
        con = sqlite3.connect(dbp, timeout=180.0)
        try:
            return rebuild_fts(con)
        finally:
            con.close()
    if (a.expect_kept is not None or a.expect_dropped is not None or a.audit_catalogue
            or a.purge_unlisted) and not a.r2_keys:
        print("REFUSING: --expect-kept/--expect-dropped/--audit-catalogue/--purge-unlisted need "
              "--r2-keys; without the listing they would evaluate to nothing (fail closed).")
        return 1
    if a.audit_catalogue:
        keys = load_r2_keys(a.r2_keys)
        problems = listing_problems(keys, load_listing_meta(a.r2_keys), a.prefix, a.bucket,
                                    a.max_listing_age_hours)
        if problems:
            print(f"REFUSING: the listing {a.r2_keys} cannot gate the catalogue:")
            for pr in problems:
                print(f"   {pr}")
            return 1
        con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
        try:
            return audit_catalogue(con, keys, a.prefix, a.purge_unlisted, a.expect_unlisted)
        finally:
            con.close()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    lic = con.execute("select reservable from license where license_id=?",
                      (LICENSE_ID,)).fetchone()
    if not lic or not lic[0]:
        print(f"licence {LICENSE_ID!r} missing or not reservable — refusing to create rows")
        return 1

    files = sorted(f.replace("\\", "/") for f in
                   glob.glob(os.path.join(STORE, "**", "*.parquet"), recursive=True)
                   if not f.endswith("__series.parquet"))
    if not files:
        print(f"no parquet under {STORE}")
        return 1
    try:
        smap = json.load(open(os.path.join(STORE, "_split_map.json"), encoding="utf-8"))
    except (OSError, ValueError) as e:
        print(f"cannot read the split map ({e!r}) — run the derive first")
        return 1

    # THE MAP MUST COVER EVERY OVERSIZED TABLE, or this emits ONE id for a table whose objects
    # were written as N parts: the whole-table id 404s and every part stays invisible. State the
    # discrepancy and list the causes rather than asserting one (R219).
    big = {os.path.splitext(os.path.basename(f))[0]: pq.ParquetFile(f).metadata.num_rows
           for f in files}
    big = {k: v for k, v in big.items() if v > a.max_rows}
    absent = {k: v for k, v in big.items() if k not in smap}
    # ONE FILE, ONE TRUST DECISION. This used to re-open the summary and derive a policy
    # different from the one made 124 lines above for the cap: there a scoped run's cap is
    # refused, here its refusal list was adopted. `_sum` is already parsed; reuse it.
    ref, ref_prov = refused_set(_sum, "table")
    print("  " + summary_coverage(_sum, len(files)))
    if ref_prov != "full":
        # NOT EMPTY - UNKNOWN. An empty set would send every absent table to `unrefused`,
        # which is loud and safe; but a scoped list can also carry FALSE refusals, and
        # `classify_absent` would file those as "nothing written, correctly NOT catalogued"
        # and drop them silently. Neither direction may be trusted, so the list is not used.
        print(f"  the derive's refusal list is {ref_prov.upper()}, not a store-wide record, "
              f"so it is NOT used to excuse any table")
        ref = set()
    keys = None
    if a.r2_keys:
        keys = load_r2_keys(a.r2_keys)
        problems = listing_problems(keys, load_listing_meta(a.r2_keys), a.prefix, a.bucket,
                                    a.max_listing_age_hours)
        if problems:
            print(f"REFUSING: the listing {a.r2_keys} cannot gate the catalogue:")
            for pr in problems:
                print(f"   {pr}")
            return 1
        print(f"listing: {len(keys):,} key(s) under {key_prefix(a.prefix)} ({a.r2_keys})")
    has_object = set()
    if keys is not None and absent:
        acceptable, unrefused, absent = classify_absent(absent, keys, ref, a.prefix)
        has_object = set(absent)  # every survivor of classify_absent has an object in R2
        if acceptable:
            print(f"{len(acceptable):,} over-cap table(s) have no split entry, no object in R2, "
                  f"AND are in the derive's refused list (logs/statcan_tables_summary.json) - "
                  f"nothing written, correctly NOT catalogued:")
            for k, v in sorted(acceptable.items(), key=lambda kv: -kv[1]):
                print(f"   {k:16s} {v:>14,} rows")
        absent.update(unrefused)
    if absent:
        print(f"REFUSING: {len(big):,} table(s) exceed {a.max_rows:,} rows but {len(absent):,} "
              f"have no split-map entry. Missing:")
        for k, v in sorted(absent.items(), key=lambda kv: -kv[1])[:20]:
            why = ("REFUSED by the derive — no splitter found" if k in ref
                   else "not seen by the derive — new, grown, or the derive is still running")
            if k in has_object:
                why = ("an OBJECT EXISTS in R2 for this table (whole above the cap, or parts with "
                       "no dim in the map) - " + why)
            print(f"   {k:16s} {v:>14,} rows   {why}")
        if len(absent) > 20:
            print(f"   … and {len(absent) - 20:,} more")
        return 1

    meta_cubes = sidecars()
    print(f"{len(files):,} table(s); split map {len(smap):,}; sidecars {len(meta_cubes):,}")

    spill = os.path.join(ROOT, "logs", "_duckspill", f"pid{os.getpid()}")
    os.makedirs(spill, exist_ok=True)
    base_meta = {
        "citation_short": "Statistics Canada.",
        "citation_long": ("Statistics Canada. Reproduced and distributed on an 'as is' basis "
                          "with the permission of Statistics Canada. Compiled and redistributed "
                          "by the Elkassabgi Data Library."),
        "description_processing": (
            "Retrieved from Statistics Canada's Web Data Service and stored as zstd Parquet, one "
            "file per Product ID. Served at TABLE grain because the source averages 10.8 "
            "observations per series across 5.26 billion series; large tables are split on one "
            "of their own dimension columns or on the coordinate hierarchy."),
    }

    rows, untitled = [], 0
    for i, f in enumerate(files, 1):
        pid = os.path.splitext(os.path.basename(f))[0]
        sc = meta_cubes.get(pid) or {}
        title = (sc.get("title") or "").strip()
        if not title:
            untitled += 1
            title = pid                                        # never invented
        freq = FREQ.get(sc.get("frequencyCode"))
        meta = dict(base_meta)
        for k, src in (("cansim_id", "cansimId"), ("archived", "archived"),
                       ("subject_code", "subjectCode")):
            if sc.get(src) not in (None, "", []):
                meta[k] = sc[src]
        meta_json = json.dumps(meta, ensure_ascii=False)

        entry = smap.get(pid)
        if not entry:
            # Unsplit: the sidecar's own start/end describe the whole cube, so no scan needed.
            d0, d1 = sc.get("start"), sc.get("end")
            if d0 and d1:
                rows.append((unit_id(pid), SOURCE, title, freq, None, "Canada", None,
                             LICENSE_ID, d0, d1, meta_json))
            else:
                q = duckdb.connect()
                q.execute(f"SET temp_directory='{spill}'")
                q.execute("SET enable_progress_bar=false")
                try:
                    d0, d1, n = q.execute(
                        f"select min(obs_date)::VARCHAR, max(obs_date)::VARCHAR, count(*) "
                        f"from read_parquet('{f}') "
                        f"where value is not null and obs_date is not null").fetchone()
                    if n:
                        rows.append((unit_id(pid), SOURCE, title, freq, None, "Canada", None,
                                     LICENSE_ID, d0, d1, meta_json))
                except Exception as e:                          # noqa: BLE001
                    print(f"  {pid}: FAILED {type(e).__name__} {str(e)[:60]}")
                finally:
                    q.close()
        else:
            # Split: each part covers a slice, so the sidecar's whole-cube range would be wrong
            # for every one of them. Compute each part's own range from its rows.
            dim = entry["dim"]
            q = duckdb.connect()
            q.execute("SET memory_limit='6GB'")
            q.execute(f"SET temp_directory='{spill}'")
            q.execute("SET preserve_insertion_order=false")
            q.execute("SET enable_progress_bar=false")
            try:
                got = q.execute(f"""
                    select {part_expr(dim)} p, min(obs_date)::VARCHAR, max(obs_date)::VARCHAR
                    from read_parquet('{f}') where value is not null and obs_date is not null
                    group by 1 order by 1""").fetchall()
                for p, d0, d1 in got:
                    if p is None or p == "":
                        continue
                    rows.append((unit_id(pid, p), SOURCE, f"{title} — {dim} {p}", freq, None,
                                 "Canada", None, LICENSE_ID, d0, d1, meta_json))
            except Exception as e:                              # noqa: BLE001
                print(f"  {pid}: SPLIT SCAN FAILED {type(e).__name__} {str(e)[:60]}")
            finally:
                q.close()
        if i % 500 == 0 or i == len(files):
            print(f"  [{i}/{len(files)}] {len(rows):,} unit(s)", flush=True)

    if keys is not None:
        emitted = len(rows)
        rows, dropped = filter_rows_by_listing(rows, keys, a.prefix)
        if dropped:
            dp = os.path.join(ROOT, "logs", "_catalog_statcan_dropped_ids.txt")
            with open(dp, "w", encoding="utf-8") as fh:
                for r in dropped:
                    fh.write(r[0] + "\n")
            print(f"{len(dropped):,} row(s) DROPPED - no object in R2 for the id (list: {dp}); "
                  f"first: {[r[0] for r in dropped[:8]]}")
        orphans = orphan_keys(rows, keys, a.prefix)
        op = os.path.join(ROOT, "logs", "_catalog_statcan_orphan_keys.txt")
        with open(op, "w", encoding="utf-8") as fh:
            for k in sorted(orphans):
                fh.write(k + "\n")
        kept_frac = len(rows) / emitted if emitted else 0.0
        print(f"listing gate [this run's EMISSION vs the listing]: emitted {emitted:,}; kept {len(rows):,} "
              f"({kept_frac:.4%}); dropped {len(dropped):,}; listing {len(keys):,}; "
              f"ORPHANS (object with no emitted row) {len(orphans):,} - rows already IN the catalogue "
              f"with no object are checked by the catalogue audit after the write (R584)"
              + (f"  first: {sorted(orphans)[:5]}" if orphans else ""))
        if len(orphans) > a.max_orphans:
            print(f"REFUSING: {len(orphans):,} object(s) in R2 would have NO catalogue row "
                  f"(> --max-orphans {a.max_orphans}); list: {op}. Data held while the "
                  f"catalogue says it does not exist is the failure that hurts (R501).")
            return 1
        if kept_frac < a.min_kept_fraction:
            print(f"REFUSING: only {kept_frac:.2%} of emitted rows have an object "
                  f"(< --min-kept-fraction {a.min_kept_fraction}) - the listing or the store "
                  f"moved; stop, do not apply.")
            return 1
        bad = expectation_problems(len(rows), len(dropped), a.expect_kept, a.expect_dropped)
        if bad:
            print("REFUSING: the run disagrees with the counts predicted before it (R566): "
                  + "; ".join(bad) + " - the listing or the store moved; stop, do not apply.")
            return 1

    print(f"\nrows to write: {len(rows):,}   tables with no published title: {untitled:,}")
    for r in rows[:4]:
        print(f"   {r[0][:60]}\n      {r[2][:100]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    con.execute(
        "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,terms_url)"
        " VALUES(?,?,?,?,?,?)",
        (SOURCE, "Statistics Canada", "https://www150.statcan.gc.ca/", LICENSE_ID,
         "Source: Statistics Canada. Reproduced and distributed on an 'as is' basis with the "
         "permission of Statistics Canada.",
         "https://www.statcan.gc.ca/en/reference/licence"))
    for i in range(0, len(rows), BATCH):
        con.executemany(
            """INSERT OR REPLACE INTO series
               (series_id,source_id,title,frequency,unit,geography,category,license_id,
                start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            rows[i:i + BATCH])
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (SOURCE,)).fetchone()[0]
    print(f"\ncatalogue rows for {SOURCE}: {n:,}")
    if keys is not None:
        # F6: the population at risk is the CATALOGUE, not this run's emission. Audit it before the
        # FTS index is rebuilt, so an unlisted id (e.g. a legacy V-id) never reaches the index.
        rc = audit_catalogue(con, keys, a.prefix, a.purge_unlisted, a.expect_unlisted)
        if rc:
            print("catalogue audit REFUSED after the write - series_fts NOT rebuilt; resolve with "
                  "--audit-catalogue --purge-unlisted (explicit) and re-run")
            return rc
    rc = reconcile_fts_source(con)  # source-scoped, 3 scans; never a whole-catalogue delete
    if rc:
        return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
