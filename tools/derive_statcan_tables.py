"""Derive statcan CSVs at TABLE grain and put them in R2.

statcan is the largest source in the library: 8,207 files, 175.3 GB, 56,845,453,642 observations
across ~5,258,059,229 series — 10.81 observations per series. Series grain is arithmetically
impossible twice over. It would mean five BILLION CSVs averaging eleven rows each, and the
catalogue alone would not fit: D1 sits at ~6.9 GB of a 10 GB hard ceiling with 10.9M rows total,
so five billion rows is off by three orders of magnitude (#45).

THE UNIT IS THE TABLE, and it is the publisher's own unit. Every file is one StatCan Product ID
— 10100001.parquet, 11100058.parquet — which is exactly what Statistics Canada names, cites and
versions. 8,207 units is the same order as istat's 14,258 and ilostat's 3,225, both of which
came out of the identical measurement (9.2 and 10.0 obs per series).

SPLITTING USES REAL COLUMNS, as ilostat's does, because statcan carries them: geo, uom,
coordinate, status alongside series_key/obs_date/value. No regex over the key is needed. The
largest single table is 962,150,400 rows, so splitting is not optional — and a table that large
needs a PAIR of columns, which is why the pair search is here from the start rather than added
after it refused something (R219).

CARDINALITY IS NOT BALANCE: every candidate split is checked by measuring the LARGEST resulting
part, never by counting distinct values.

Not yet run at scale. --dry-run reports the split decisions without contacting R2, which is how
the splitter should be validated on the giant tables before a multi-day derive is committed to.
"""
from __future__ import annotations

import argparse
import csv
import glob
import gzip
import io
import json
import os
import queue
import sys
import threading
import time
import urllib.parse

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import r2_util                                        # noqa: E402

SOURCE = "statcan"
STORE = os.path.join(ROOT, "data", "clean_full", SOURCE)
HEADER = ["series_id", "obs_date", "value"]
MAX_ROWS_DEFAULT = 500_000
# Real dimension columns, coarsest-looking first. `status` is a per-observation quality flag and
# is NOT a splitter -- splitting on it would scatter one series across parts, the same defect
# that made insee_sdmx unusable (10.8M rows under 817 keys, all built from observation
# attributes). obs_date is excluded for the usual reason: splitting a time series by time makes
# every part useless alone.
DIM_COLS = ("uom", "geo", "coordinate")


def choose_split(con, path: str, n_rows: int, max_rows: int):
    """(column-or-pair, parts) needing a split; (None, 1) if it fits; ("", 0) if refused."""
    if n_rows <= max_rows:
        return None, 1
    cols = [c for c in DIM_COLS if c in set(pq.read_schema(path).names)]
    card = {}
    for c in cols:
        try:
            card[c] = con.execute(
                f'select count(distinct "{c}") from read_parquet(\'{path}\')').fetchone()[0]
        except Exception:                                       # noqa: BLE001
            continue

    def largest(expr: str):
        return con.execute(
            f"select max(n) from (select count(*) n from read_parquet('{path}') "
            f"where value is not null and obs_date is not null group by {expr})").fetchone()[0]

    for c, d in sorted((c, d) for d, c in card.items() if 2 <= c <= 2000):
        try:
            if largest(f'"{d}"') <= max_rows:
                return d, c
        except Exception:                                       # noqa: BLE001
            continue
    # HIERARCHICAL TRUNCATION OF `coordinate`, which is where statcan's giants actually divide.
    # Measured on 12100152 (427,009,412 rows): uom has ONE distinct value, status ONE, geo 14
    # (largest group 135,795,854 — far too coarse), and coordinate 17,732,442 (largest group 429
    # — far too fine to be a unit). There is nothing in between, which is why single columns and
    # pairs both refused it.
    #
    # But a coordinate is a dot-separated dimension tuple — '59.9.37.1.85.3.100.1400' — so
    # truncating it to k segments walks the publisher's own hierarchy from coarse to fine. Same
    # technique the istat derive uses for nested ISTAT territory codes and derive_census_tables
    # uses for HS commodity codes, applied to the column statcan actually nests.
    # ONE SCAN, THEN ROLL UP — not one scan per truncation level. The obvious loop issues a
    # count(distinct) and a group-by for each k, so seven levels is ~14 full passes over the
    # table; on 12100152 (427M rows) that ran 78 CPU-MINUTES without finishing, and the derive
    # has 8,207 tables to get through. Counting each FULL coordinate once gives a 17.7M-row
    # summary, and every truncation level is then a cheap group-by over that summary rather than
    # over the raw rows. Same answers, one pass.
    if "coordinate" in card:
        try:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _coord AS
                SELECT "coordinate" AS c, count(*) AS n
                FROM read_parquet('{path}')
                WHERE value IS NOT NULL AND obs_date IS NOT NULL
                GROUP BY 1""")
            ladder = []
            for k in range(1, 9):
                trunc = ("array_to_string(array_slice(string_split(c, '.'), 1, "
                         f"{k}), '.')")
                parts, biggest = con.execute(f"""
                    SELECT count(*), max(s) FROM (
                      SELECT {trunc} AS p, sum(n) AS s FROM _coord GROUP BY 1)""").fetchone()
                ladder.append((k, parts or 0, biggest or 0))
                if parts and 2 <= parts <= 20_000 and biggest and biggest <= max_rows:
                    return f"coordinate:{k}", parts
            # NO LEVEL SATISFIED BOTH BOUNDS — say so WITH the ladder, because "refused" alone
            # hides that this is a parameter choice and not a property of the data. Measured on
            # 12100152 (427,009,412 rows):
            #     k=4   6,514 parts   largest 2,172,577   <- fits the part cap, over the row bound
            #     k=5 317,350 parts   largest   339,545   <- fits the row bound, absurd part count
            # There is no k that fits both at max_rows=500,000, and the honest resolution is to
            # raise --max-rows for this source (k=4 becomes legal at 3,000,000, giving 6,514
            # parts of ~65 MB) rather than emit a third of a million objects for one table.
            print(f"      coordinate ladder (no level fits parts<=20,000 AND rows<={max_rows:,}):",
                  flush=True)
            for k, parts, biggest in ladder:
                fits = ("parts ok" if 2 <= parts <= 20_000 else "parts too many") + ", " + \
                       ("rows ok" if biggest <= max_rows else "rows too big")
                print(f"        k={k}  {parts:>12,} parts  largest {biggest:>14,}   {fits}",
                      flush=True)
        except Exception:                                       # noqa: BLE001
            pass

    usable = sorted((c, d) for d, c in card.items() if c >= 2)
    for prod, d1, d2 in sorted(
            (c1 * c2, n1, n2) for i, (c1, n1) in enumerate(usable)
            for c2, n2 in usable[i + 1:] if 2 <= c1 * c2 <= 20_000):
        try:
            if largest(f'"{d1}", "{d2}"') > max_rows:
                continue
            # '~' joins the two values in the part label, so it must be absent from both
            # vocabularies or the label cannot be split back apart. Checked, not assumed.
            if con.execute(f"select count(*) from read_parquet('{path}') "
                           f"where \"{d1}\" like '%~%' or \"{d2}\" like '%~%'").fetchone()[0]:
                continue
            return f"{d1}+{d2}", prod
        except Exception:                                       # noqa: BLE001
            continue
    return "", 0


def part_expr(dim: str) -> str:
    """ONE definition of the part label, imported by the catalogue and the resolver.

    Three shapes, and the catalogue MUST use this function rather than reimplement any of them —
    a split expression rebuilt elsewhere drifts into ids no object answers to:
        "geo"              a single column
        "uom+geo"          a pair, joined with '~' (verified absent from both vocabularies)
        "coordinate:3"     the first 3 dot-segments of the coordinate hierarchy
    """
    if dim.startswith("coordinate:"):
        k = int(dim.split(":", 1)[1])
        return (f"array_to_string(array_slice(string_split(\"coordinate\", '.'), 1, {k}), '.')")
    if "+" in dim:
        d1, d2 = dim.split("+", 1)
        return f"\"{d1}\" || '~' || \"{d2}\""
    return f'"{dim}"'


def unit_id(pid: str, part: str | None = None) -> str:
    return f"{SOURCE}:{pid}" + (f"#{part}" if part else "")


def csv_key(prefix: str, sid: str) -> str:
    return f"{prefix}/{urllib.parse.quote(sid, safe='')}.csv"


def _rows_csv(rows) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    for sid, d, v in rows:
        w.writerow([sid, d, v])
    return buf.getvalue().encode("utf-8")


def merged_refused(summary_path: str, key: str, examined, refused, full_run: bool):
    """(list, scope) for the summary's `refused` field, merging a scoped run into the record.

    A `--only` run knows the truth for the stems it examined and NOTHING about the rest, so
    replacing the whole list throws away a previous full run's verdicts -- which is what the
    cataloguers print as their own remediation command, so the guard destroys its own evidence
    every time an operator follows it (R843 addendum).

    Returns `scope` = "full" when the merged list is still a statement about the whole store:
    either this run was full, or a previous full-scoped record existed and we updated only the
    stems this run looked at. Otherwise "partial" -- and a reader must then treat the list as
    UNKNOWN rather than as empty.

    Deliberately NOT keyed off the summary's `scope` field, because that one describes the CAP's
    provenance and the two have opposite risk asymmetries: an unknown cap must never be adopted
    (R833), while a merged refusal list from a previous full run is sound.

    Fails SAFE, never silently: if the old summary cannot be read or carries no scope, the merge
    is skipped and the scope returned is "partial", so a reader is told it does not know.
    """
    mine = [{key: st, "rows": nr} for st, nr in refused]
    if full_run:
        return mine, "full"
    try:
        old = json.load(open(summary_path, encoding="utf-8"))
        prev = old.get("refused")
        prev_scope = old.get("refused_scope") or ("full" if old.get("scope") == "full" else None)
        if not isinstance(prev, list) or prev_scope != "full":
            return mine, "partial"
        kept = [r for r in prev if isinstance(r, dict) and r.get(key) not in set(examined)]
        return kept + mine, "full"
    except (OSError, ValueError, TypeError, AttributeError):
        return mine, "partial"

# What pinned_split returns for a cube that has NO public ids yet (not in the map, not catalogued):
# there is nothing to keep, so the caller decides its split with choose_split as a first derive does.
CHOOSE = "choose"


def pinned_split(pid: str, n_rows: int, pinned: dict, max_rows: int, served_whole: bool,
                 served_parts: bool = False):
    """The split a refreshed cube is SERVED under - what --pin-split reports against.

    Why this exists (2026-09-23). A cube's part ids are its split's VALUES (`statcan:<pid>#<value>`)
    and choose_split() decides the split from the data. Re-deciding it on a refreshed cube can pick
    another column - or split a cube that was served whole - and rename EVERY public id at once.

    `served_whole` / `served_parts`: whether the catalogue holds `statcan:<pid>` itself / any
    `statcan:<pid>#...` - the facts, read from the catalogue, never inferred from the map's silence.

    A map entry's `parts` is the count choose_split measured - DISTINCT split values when the split
    was chosen - not the parts a derive emits: a value whose rows are all NULL emits no part, so
    35100172 records 13 and is catalogued with 12 (round-4 review measured 8 such cubes, each gap
    exactly its all-NULL groups). Never use it as a completeness count.

    Returns choose_split's shape: (dim, n) for a recorded split; (None, 1) for a cube served whole
    that still fits the cap; ("", 0) - refused by name - for a whole cube that has outgrown the cap
    (catalog_statcan_tables.py refuses the WHOLE catalogue when an over-cap cube has no split: one
    rule, both tools) and for a cube served as PARTS whose split is not recorded (choosing one now
    would rename every served id - round-2 review); (CHOOSE, 0) only for a cube with no ids at all.
    """
    entry = pinned.get(pid)
    if entry and entry.get("dim"):
        return entry["dim"], int(entry.get("parts") or 0)
    if served_parts:
        return "", 0
    if served_whole:
        return (None, 1) if n_rows <= max_rows else ("", 0)
    return CHOOSE, 0


def pinned_columns_present(dim: str, schema_names) -> bool:
    """A recorded split must still name columns the refreshed cube has."""
    if dim.startswith("coordinate:"):
        cols = ["coordinate"]
    elif "+" in dim:
        cols = dim.split("+", 1)
    else:
        cols = [dim]
    return all(c in set(schema_names) for c in cols)


def part_diff(emitted: set, catalogued: set) -> dict:
    """{new: ids emitted but not catalogued, vanished: catalogued ids no longer emitted}.

    A NEW id has an object and no catalogue row, so users cannot reach it until the catalogue is
    synced. A VANISHED id (its dimension value was relabelled or dropped) keeps its old object and
    its catalogue row, so users get STALE data until the catalogue retires it. Neither is fixed
    here: this tool writes objects, the catalogue is tools/catalog_statcan_tables.py plus the D1
    sync. It reports them so the catalogue step knows exactly what to do."""
    return {"new": sorted(emitted - catalogued), "vanished": sorted(catalogued - emitted)}


def write_map_atomic(path: str, obj: dict) -> None:
    """Write the split map to a temp file and os.replace it in (round-4 review, probe P1).

    Opening the map itself with "w" EMPTIES it first, so a kill between the truncate and the last
    byte left invalid JSON on disk - and while the map is unreadable the resolver raises for every
    `#` part id in the catalogue. os.replace is atomic on one volume: a reader sees the old map or
    the new one, never half of either. A reader holding the file open makes Windows refuse the
    replace (a sharing violation, PermissionError), so the replace is retried for ~25 s before it
    gives up - and on giving up the temp file is removed and the old map is untouched."""
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, indent=1, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        for attempt in range(10):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.5 * (attempt + 1))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def catalogued_ids(catalog_db: str, pid: str) -> set:
    """Every catalogued id of one cube, by a PRIMARY-KEY RANGE read of the local catalogue (no scan;
    DECIDE LOCALLY). 'statcan:<pid>' and 'statcan:<pid>#...' both sort inside the range because the
    product id is a fixed 8 digits."""
    import sqlite3                                              # noqa: PLC0415
    lo = unit_id(pid)
    hi = lo + "$"          # '$' (0x24) sorts just after '#' (0x23): the cube and its parts only
    # timeout=180: the catalogue is rollback-journal, so a concurrent writer blocks readers (R715)
    con = sqlite3.connect(f"file:{catalog_db}?mode=ro", uri=True, timeout=180)
    try:
        return {r[0] for r in con.execute(
            "SELECT series_id FROM series WHERE series_id >= ? AND series_id < ?", (lo, hi))}
    finally:
        con.close()


def run_scope(a) -> str:
    """Was this run evidence about the WHOLE store, or only part of it?

    The summary is written unconditionally - by dry runs and by scoped runs too - and it carries
    the `max_rows` the cataloguer adopts as fact. So a one-table dry run at another cap would
    stamp the whole 8,207-table store's provenance, reconstituting R832's refusal with a
    confident provenance line attached, which is worse than the shared-constant guess it replaced.

    `--dry-run` is checked FIRST: a dry run is not evidence whatever else was passed.
    `--limit` defaults to 0 and `--only` to "" - both falsy - so an unused flag reads as `full`,
    and an EXPLICIT `--limit 0` or `--only ""` also reads as full, which is correct: neither
    restricts anything.

    Named rather than inlined in the summary dict so it can be tested without running a derive
    over a store (R840 - a branch nothing calls is a branch nothing tests).
    """
    if getattr(a, "dry_run", False):
        return "dry_run"
    # (--pin-split sets dry_run, so a pinned report is scoped "dry_run" above: never evidence of
    # the store's cap - R832/R833.)
    if getattr(a, "only", None):
        return "only"
    if getattr(a, "limit", None):
        return "limit"
    return "full"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--largest-first", action="store_true",
                    help="process the biggest tables first — use with --dry-run to validate the "
                         "splitter where it actually has to work")
    ap.add_argument("--memory-limit", default="8GB")
    ap.add_argument("--max-rows", type=int, default=None,
                    help=f"row cap per served object (default {MAX_ROWS_DEFAULT:,}; REQUIRED with "
                         f"--pin-split unless the derive summary records the store's cap)")
    ap.add_argument("--only", default="")
    ap.add_argument("--pin-split", action="store_true",
                    help="REPORT ONLY (implies --dry-run): which served ids a refreshed cube keeps, "
                         "adds and strands, under the split it is already served with")
    ap.add_argument("--parts-report", metavar="PATH",
                    help="write {pid: {new, vanished}} part ids against the local catalogue")
    ap.add_argument("--catalog-db", default=os.path.join(ROOT, "data", "catalog.db"))
    ap.add_argument("--rekey", action="store_true",
                    help="RE-CHOOSE every split from the data, as the first derive did. This renames "
                         "served part ids, so it is a decision and not a refresh; without it a "
                         "writing run keeps every catalogued cube's recorded split")
    a = ap.parse_args()
    # --pin-split WRITES NOTHING (round-2 review). The orchestrator already re-derives a refreshed
    # cube's CATALOGUED parts, under the recorded split by construction, byte-identical to this tool
    # (the reviewer's 12/12 on 14100026 and 10100001). What it cannot do is name the parts a refresh
    # CREATES or STRANDS - which is this mode's whole job. Writing objects here only raced the
    # orchestrator, could skip-existing its way to a green "refresh" that changed nothing, and put
    # objects under ids no catalogue row reaches.
    if a.pin_split:
        a.dry_run = True
    if not a.dry_run and not a.bucket:
        ap.error("--bucket is required unless --dry-run")
    if a.pin_split and a.rekey:
        ap.error("--pin-split reports against the recorded splits; --rekey discards them")
    # A WRITING RUN KEEPS SERVED IDS (round-4 review, probe P2). Pinning used to protect only the
    # report: a real `--only` run re-chose the split of a cube already served as parts, wrote a
    # whole-cube object, dropped its map entry and exited 0. Now every writing run pins, and only an
    # explicit --rekey re-chooses (a full re-derive campaign; the retired RELAUNCH_GUARD argv needs
    # it added if it is ever relaunched).
    pin_writes = not a.dry_run and not a.rekey
    _smap_path = os.path.join(STORE, "_split_map.json")
    # THE MAP AS IT WAS WHEN THIS RUN STARTED, kept whole by every map write (see there). A pinned
    # report and EVERY writing run fail closed on an unreadable map (round-4 review, probe P5: a
    # full run started from {} and dropped a refused cube's entry). Only a --rekey run may start
    # without a map at all (a first derive), and only a plain --dry-run tolerates a corrupt one.
    try:
        base_map = json.load(open(_smap_path, encoding="utf-8"))
    except FileNotFoundError as e:
        if a.pin_split or pin_writes:
            raise SystemExit(f"REFUSING{' --pin-split' if a.pin_split else ''}: {_smap_path} does not "
                             f"exist; pinning needs the recorded splits (--rekey to choose them "
                             f"afresh)") from e
        base_map = {}
    except (OSError, ValueError) as e:
        if a.pin_split or not a.dry_run:
            raise SystemExit(f"REFUSING{' --pin-split' if a.pin_split else ''}: {_smap_path} is "
                             f"unreadable ({e!r}); a run that writes or pins cannot keep the other "
                             f"cubes' splits without it") from e
        base_map = {}
    if (a.pin_split or pin_writes) and (not isinstance(base_map, dict) or not base_map):
        # FAIL CLOSED: pinning against an empty map would silently re-decide every split - exactly
        # the id churn pinning exists to prevent.
        raise SystemExit(f"REFUSING{' --pin-split' if a.pin_split else ''}: {_smap_path} holds no "
                         f"split decisions")
    pinned = base_map if (a.pin_split or pin_writes) else {}
    if a.pin_split or not a.dry_run:
        # THE CAP MUST BE THE STORE'S, in the report AND in every mode that writes (round-4 review,
        # probe P3: a real --only run without --max-rows split at the 500,000 default while the store
        # was built at 3,000,000). The production derive ran at 3,000,000 (the smallest split cube
        # has 3,003,000 rows). A cap is evidence only from a FULL run (R833; catalog_statcan_tables.py
        # applies the same rule): a dry or scoped run stamps its own max_rows (round-2 review, P2).
        try:
            _sum = json.load(open(os.path.join(ROOT, "logs", "statcan_tables_summary.json"),
                                  encoding="utf-8"))
            _recorded = _sum.get("max_rows") if _sum.get("scope") == "full" else None
        except (OSError, ValueError, AttributeError):
            _recorded = None
        _mode = "--pin-split" if a.pin_split else "a writing run"
        if _recorded is not None and a.max_rows is not None and int(_recorded) != a.max_rows:
            raise SystemExit(f"REFUSING {_mode}: --max-rows {a.max_rows:,} disagrees with the "
                             f"store's recorded cap {int(_recorded):,}")
        if a.max_rows is None:
            if _recorded is None:
                raise SystemExit(f"REFUSING {_mode} without --max-rows: the derive summary does "
                                 "not record the cap the store was built at, and a guessed cap "
                                 "refuses cubes that fit (the production derive ran at 3,000,000)")
            a.max_rows = int(_recorded)
    if a.max_rows is None:
        a.max_rows = MAX_ROWS_DEFAULT
    if (a.parts_report or a.pin_split or pin_writes) and not os.path.exists(a.catalog_db):
        raise SystemExit(f"REFUSING: no catalogue at {a.catalog_db} (needed to know which cubes are "
                         f"served whole or as parts - by --pin-split, by --parts-report, and by any "
                         f"writing run without --rekey)")
    parts_report: dict = {}
    if not isinstance(base_map, dict):
        base_map = {}

    files = sorted(f.replace("\\", "/") for f in
                   glob.glob(os.path.join(STORE, "**", "*.parquet"), recursive=True)
                   if not f.endswith("__series.parquet"))
    n_store = len(files)          # BEFORE --only filters it. NOTE --limit narrows nothing here: it breaks
                                 # the LOOP, which is why `processed` exists below
    n_done = 0                    # what the loop ACTUALLY processed; --limit breaks it
    _examined_stems = []          # and WHICH ones - the merge drops only these
    if a.only:
        want = {s.strip() for s in a.only.split(",") if s.strip()}
        files = [f for f in files if os.path.splitext(os.path.basename(f))[0] in want]
        got = {os.path.splitext(os.path.basename(f))[0] for f in files}
        if got != want:
            raise SystemExit(f"--only names {len(want)}; {len(got)} exist. "
                             f"Missing: {', '.join(sorted(want - got))}")
    if not files:
        raise SystemExit(f"no parquet under {STORE} — refusing to report an empty derive")
    if a.largest_first:
        # SORT BY BYTES, NOT ROWS. Row count means opening 8,207 parquet footers, and a
        # 962M-row file's footer carries thousands of row-group entries — that sort alone burned
        # 40 CPU-MINUTES here without finishing. File size is free from the directory entry and
        # ranks the giants identically for this purpose. The distribution makes the point: the
        # largest table is 4,075 MB and the MEDIAN is 0.03 MB, so a handful of tables dominate
        # and everything else is trivial.
        files = [f for _n, f in sorted(((os.path.getsize(f), f) for f in files), reverse=True)]
    print(f"{len(files):,} table(s); splitting any over {a.max_rows:,} rows"
          f"{' — LARGEST FIRST' if a.largest_first else ''}", flush=True)

    # THE CATALOGUE'S IDS FOR EVERY CUBE IN SCOPE, read BEFORE anything is written (round-1
    # review): read inside the loop, a locked catalogue raised with PUTs already queued to daemon
    # workers, and those were lost with no summary. Fails closed here instead.
    cat_ids: dict = {}
    if a.pin_split or a.parts_report or pin_writes:
        try:
            for f in files:
                stem = os.path.splitext(os.path.basename(f))[0]
                cat_ids[stem] = catalogued_ids(a.catalog_db, stem)
        except Exception as e:                                  # noqa: BLE001
            raise SystemExit(f"REFUSING: the catalogue at {a.catalog_db} could not be read "
                             f"({type(e).__name__}: {e}); nothing was written") from e

    # PER-PROCESS SPILL DIRECTORY. Every tool here used a shared logs/_duckspill, which is fine
    # until two of them spill at the same moment — then one deletes the other's temp storage and
    # both die: "IO Error: Failed to delete file duckdb_temp_storage_DEFAULT-0.tmp: The system
    # cannot find the file specified", and the sibling exits 139. Observed exactly that running
    # this probe alongside a measurement on the same table. DuckDB names its temp file after the
    # DATABASE, not the process, so the collision is silent until it isn't.
    spill = os.path.join(ROOT, "logs", "_duckspill", f"pid{os.getpid()}")
    os.makedirs(spill, exist_ok=True)

    existing = set()
    s3 = None
    if not a.dry_run:
        s3 = r2_util.client(write=True)
        if a.skip_existing:
            pref = f"{a.prefix}/{urllib.parse.quote(SOURCE + ':', safe='')}"
            tok = None
            while True:
                kw = {"Bucket": a.bucket, "Prefix": pref, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                r = s3.list_objects_v2(**kw)
                existing.update(o["Key"] for o in r.get("Contents", []))
                if not r.get("IsTruncated"):
                    break
                tok = r["NextContinuationToken"]
            print(f"skip-existing: {len(existing):,} already in R2", flush=True)

    # maxsize 64, not 1000: bodies are whole unit CSVs (up to ~100 MB raw). A
    # 1000-slot queue is an unbounded-in-BYTES buffer — with the census giants
    # producing units faster than 16 uploaders drained them, the 2026-08-19
    # relaunch died of MemoryError at giant 10/8207 while the box also hosted a
    # 63 GB imts finalize. 64 compressed bodies (~10-20 MB each after the gzip
    # below) bound the buffer to ~1 GB and give backpressure instead of death.
    q: queue.Queue = queue.Queue(maxsize=64)
    counts = {"put": 0, "skip": 0, "err": 0}
    lock = threading.Lock()
    STOP = object()
    written: set = set()          # keys whose PUT SUCCEEDED - what the parts report may call written
    emitted: dict = {}            # pid -> ids this run produced a body for
    status: dict = {}             # pid -> ok | refused | scan_failed
    pin_refused: set = set()      # refused because its SERVED ids cannot be reproduced

    def worker():
        while True:
            item = q.get()
            if item is STOP:
                q.task_done()
                return
            key, body = item
            try:
                # GZIP AT REST (Ahmed 2026-08-18: "bring back statcan compressed").
                # This tool has its OWN uploader predating the fleet gzip writers —
                # the first statcan campaign uploaded 1.37 TB uncompressed because
                # of exactly this gap. Compression normally happens at ENQUEUE
                # (flush) so the queue buffers small bodies; the magic-byte check
                # keeps this path safe for both compressed and raw producers.
                if body[:2] != b"\x1f\x8b":
                    body = r2_util.gzip_bytes(body)
                s3.put_object(Bucket=a.bucket, Key=key, Body=body, ContentType="text/csv",
                              ContentEncoding="gzip")
                with lock:
                    written.add(key)
                    counts["put"] += 1
                    if counts["put"] % 500 == 0:
                        print(f"  put {counts['put']:,}", flush=True)
            except Exception as e:                              # noqa: BLE001
                with lock:
                    counts["err"] += 1
                    if counts["err"] <= 5:
                        print(f"  PUT FAILED {key}: {str(e)[:90]}", flush=True)
            finally:
                q.task_done()

    threads = []
    if not a.dry_run:
        for _ in range(a.workers):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)

    t0 = time.time()
    n_units = 0
    dropped_total = 0
    refused = []
    split_map: dict = {}
    for i, f in enumerate(files, 1):
        pid = os.path.splitext(os.path.basename(f))[0]
        # RECORDED HERE, AT THE TOP, because "examined" means "this run looked at it and
        # formed a verdict" - which includes REFUSED and SCAN FAILED. Recording it at the
        # BOTTOM put it after two `continue`s, so a refused stem was in `refused` and not
        # in `examined`; the merge then KEPT the previous entry for that stem and added
        # the new one, duplicating it and double-counting `refused_rows`.
        _examined_stems.append(pid)
        con = duckdb.connect()
        con.execute(f"SET memory_limit='{a.memory_limit}'")
        con.execute(f"SET temp_directory='{spill}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET enable_progress_bar=false")
        n_rows = pq.ParquetFile(f).metadata.num_rows
        pin_refusal = None
        if a.pin_split or pin_writes:
            _parts = any("#" in s for s in cat_ids.get(pid, set()))
            dim, n_parts = pinned_split(pid, n_rows, pinned, a.max_rows,
                                        served_whole=unit_id(pid) in cat_ids.get(pid, set()),
                                        served_parts=_parts)
            if dim == CHOOSE:
                # no public ids yet: nothing to keep, so decide it as a first derive does
                dim, n_parts = choose_split(con, f, n_rows, a.max_rows)
            elif dim == "" and _parts:
                pin_refusal = ("served as parts but no split is recorded in the map - choosing one "
                               "now would rename every served part (--rekey is that decision)")
            elif dim == "":
                pin_refusal = (f"served whole and now {n_rows:,} rows, over the {a.max_rows:,} cap - "
                               f"splitting it re-keys its public id, a decision and not a derive")
            elif dim and not pinned_columns_present(dim, pq.read_schema(f).names):
                pin_refusal = (f"its recorded split {dim!r} names a column the refreshed cube "
                               f"no longer has")
                dim, n_parts = "", 0
        else:
            dim, n_parts = choose_split(con, f, n_rows, a.max_rows)
        emitted_ids: set = emitted.setdefault(pid, set())
        status[pid] = "ok"
        if dim:
            split_map[pid] = {"dim": dim, "parts": n_parts, "rows": n_rows}
            # PERSIST THE DECISION IMMEDIATELY, not at the end of an 8,207-table run. Choosing a
            # split is the expensive half — ~7 minutes on a 427M-row cube — and a map written
            # only on a clean finish is lost entirely if the run is interrupted, which for a
            # multi-day job is the likely case, not the unlikely one. Same reasoning as
            # stat_slovenia's sweep offset: state that exists to survive a kill must be written
            # before the kill. --skip-existing then makes a restart cheap in BOTH halves.
            #
            # A SCOPED RUN WRITES THE WHOLE MAP (2026-09-23). This used to dump `split_map` - THIS
            # run's entries only - over the file, so an `--only` run truncated the 593-cube map to
            # its own cubes mid-run, and the end-of-run merge below then re-read that truncated file
            # and wrote it back: every other split flow lost its entry, and the resolver raises
            # "never split" for each of their catalogued part ids. `base_map` is the map as it was
            # when this run started (read fail-closed above for scoped runs).
            if not a.dry_run:
                try:
                    # a REFUSED cube keeps its previous entry: nothing new was written for it,
                    # and its catalogued parts still resolve through that entry
                    seen = set(_examined_stems) - {st for st, _n in refused}
                    write_map_atomic(os.path.join(STORE, "_split_map.json"),
                                     {**{k: v for k, v in base_map.items() if k not in seen},
                                      **split_map})
                except OSError:
                    pass    # the old map is intact (atomic write); the end-of-run write retries
        if dim == "":
            refused.append((pid, n_rows))
            status[pid] = "refused"
            if pin_refusal:
                pin_refused.add(pid)
            why = pin_refusal or f"{n_rows:,} rows and no column pair divides it below {a.max_rows:,}"
            print(f"  [{i}/{len(files)}] {pid}: REFUSED — {why}; NOT emitted", flush=True)
            con.close()
            continue
        if a.dry_run and a.parts_report:
            # what a real run WOULD emit, from the same predicate and the same part expression
            expr = part_expr(dim) if dim else "''"
            for (p,) in con.execute(f"SELECT DISTINCT {expr} FROM read_parquet('{f}') "
                                    f"WHERE value IS NOT NULL AND obs_date IS NOT NULL").fetchall():
                if dim and p in (None, ""):
                    continue                      # a NULL split value has no part id (see flush)
                emitted_ids.add(unit_id(pid, p or None))
        if a.dry_run:
            if dim or i <= 5 or i % 500 == 0:
                print(f"  [{i}/{len(files)}] {pid}: {n_rows:>12,} rows -> "
                      f"{('split by ' + dim + f' ({n_parts:,} parts)') if dim else 'whole'}"
                      f"   {time.time()-t0:,.0f}s", flush=True)
            con.close()
            n_units += n_parts if dim else 1
            n_done = i
            if a.limit and i >= a.limit:
                break
            continue

        if dim:
            sel = f"{part_expr(dim)} AS part, series_key, obs_date, value"
            order = "part, series_key, obs_date, value"
        else:
            sel = "'' AS part, series_key, obs_date, value"
            order = "series_key, obs_date, value"
        try:
            cur = con.execute(f"""
                SELECT {sel} FROM read_parquet('{f}')
                WHERE value IS NOT NULL AND obs_date IS NOT NULL
                ORDER BY {order}""")
        except Exception as e:                                  # noqa: BLE001
            print(f"  [{i}/{len(files)}] {pid}: SCAN FAILED {type(e).__name__} "
                  f"{str(e)[:70]}", flush=True)
            status[pid] = "scan_failed"
            con.close()
            continue

        cur_part, rows, last, dropped = None, [], None, 0

        def flush(part):
            nonlocal n_units
            if not rows:
                return
            if dim and part in (None, ""):
                # A NULL value of the split column has NO part id: `unit_id(pid, None)` is the
                # WHOLE-cube id, so these rows were written as `statcan:<pid>` holding only the
                # NULL slice, while the resolver serves that id as the entire cube - two different
                # bodies for one id (round-2 review, probe P6). The cataloguer skips NULL parts, so
                # the object was an orphan; it is simply not written.
                with lock:
                    counts["null_part_rows"] = counts.get("null_part_rows", 0) + len(rows)
                return
            sid = unit_id(pid, part or None)
            n_units += 1
            emitted_ids.add(sid)
            key = csv_key(a.prefix, sid)
            if key in existing:
                with lock:
                    counts["skip"] += 1
                return
            # Compress BEFORE enqueueing: the queue then buffers ~10-20 MB gzip
            # bodies instead of ~100 MB raw CSVs (measured 5.4-11x on statcan).
            # Same deterministic bytes as the worker path (mtime=0).
            q.put((key, r2_util.gzip_bytes(_rows_csv(rows))))

        while True:
            batch = cur.fetchmany(200_000)
            if not batch:
                break
            for part, k, d, v in batch:
                if part != cur_part:
                    flush(cur_part)
                    cur_part, rows, last = part, [], None
                if (k, d) == last:
                    dropped += 1
                    rows[-1] = (k, d.isoformat(), v)
                    continue
                last = (k, d)
                rows.append((k, d.isoformat(), v))
        flush(cur_part)
        con.close()
        dropped_total += dropped
        if i % 100 == 0 or i == len(files) or dim:
            print(f"  [{i}/{len(files)}] {pid}{' split by ' + dim if dim else ''}: "
                  f"{n_units:,} units so far, {dropped_total:,} dup rows collapsed, "
                  f"{time.time()-t0:,.0f}s", flush=True)
        # WHAT THIS RUN ACTUALLY LOOKED AT. `--limit` breaks this loop; it does NOT filter
        # `files`, so `considered` (len(files)) would report the whole store for a run that
        # stopped after five. Against `store_files` that renders as 100% coverage of a 0.2%
        # run -- a summary contradicting its own `scope` tag, with the wrong half readable.
        n_done = i
        if a.limit and i >= a.limit:
            break

    if not a.dry_run:
        for _ in threads:
            q.put(STOP)
        q.join()

    dt = time.time() - t0
    print(f"\nunits: {n_units:,}   put {counts['put']:,}   skipped {counts['skip']:,}   "
          f"errors {counts['err']:,}   {dt:,.0f}s")
    print(f"duplicate (series_key, obs_date) rows collapsed: {dropped_total:,}")
    if refused:
        print(f"REFUSED (too large, no usable splitter) — {len(refused)}:")
        for st, nr in refused:
            print(f"   {st:24s} {nr:>14,} rows")

    smap = os.path.join(STORE, "_split_map.json")
    if a.dry_run:
        print(f"(dry run: split map NOT written; this run chose {len(split_map)} split(s))")
    else:
        out_map = split_map
        # MERGE ON EVERY SCOPED RUN, not only --only (round-1 review): a --limit or --pin-split
        # run examined part of the store too, and replacing the map with its entries dropped 592
        # of 593 in the reviewer's probe.
        # A PINNED full run merges too (round-5 review, minor 1): it keeps every split, so a map
        # entry for a cube with no store file this run is still the truth, not a stale decision to
        # drop. Only a --rekey full run rebuilds the map from its own decisions.
        if run_scope(a) != "full" or pin_writes:
            try:
                out_map = json.load(open(smap, encoding="utf-8"))
            except (OSError, ValueError) as _e:
                # FAIL CLOSED ON A SCOPED RUN (R503). Starting from {} here writes a
                # map holding ONLY this run's stems, and every other split flow then
                # has no entry - the resolver raises "never split" for every part id
                # already catalogued. A transient read error must stop the run, not
                # silently narrow the map to what one --only run happened to touch.
                raise SystemExit(
                    f"REFUSING: --only was given but the existing split map at {smap} "
                    f"could not be read ({type(_e).__name__}: {_e}). Merging is "
                    f"impossible, and writing a fresh map would orphan every other "
                    f"split flow. Restore the map (a .20260907.bak sits beside it) "
                    f"and re-run.") from _e
            # the cubes this run EXAMINED, not every file in scope: `--limit` breaks the loop
            # without narrowing `files`, so popping `files` erased the whole map on a limited run
            # ...and a REFUSED cube keeps its previous entry: this run wrote nothing for it, so its
            # catalogued parts still resolve through that entry (round-2 review, probe P1)
            for st in set(_examined_stems) - {st for st, _n in refused}:
                out_map.pop(st, None)
            out_map.update(split_map)
        else:
            # A FULL run rebuilds the map from its own decisions - but a cube it REFUSED wrote
            # nothing, so its served parts still resolve through the entry it had. Dropping it
            # here left every such part id "never split" (round-3 review, probe P9); the mid-run
            # write already kept it, so the two writes disagreed.
            out_map = dict(split_map)
            for st, _n in refused:
                if st in base_map and st not in out_map:
                    out_map[st] = base_map[st]
        write_map_atomic(smap, out_map)
        print(f"split map ({len(out_map):,} table(s)) -> {smap}")

    # Every terminal disposition gets a key (R219).
    # MERGE, DO NOT REPLACE, when this run looked at only part of the store. The
    # cataloguers print `--only <ids>` as their own fix; without this, following that
    # instruction erases every other stem's verdict (R843 addendum).
    # WHAT THE RUN PROCESSED, not what it globbed. `--only` filters `files`, but
    # `--limit` does NOT - it breaks the loop - so using `files` here would drop
    # every stem in the store from the previous record while having examined five.
    _examined = _examined_stems
    summary = os.path.join(ROOT, "logs", "statcan_tables_summary.json")
    # A PINNED REPORT LEAVES THE SUMMARY ALONE (round-3 review, probe P7). The summary is the only
    # structured record of the last FULL run - its cap (which the cataloguer adopts) and its refused
    # giants - and a routine refresh report overwrote it with scope=dry_run, erasing both. A report
    # decides no split and writes no object, so it has nothing to record there.
    if a.pin_split:
        _ref_list = None
    else:
        _ref_list, _ref_scope = merged_refused(summary, "table", _examined, refused,
                                               run_scope(a) == "full")
    if _ref_list is not None:
        json.dump({"considered": len(files), "units": n_units, "put": counts["put"],
               "skipped": counts["skip"], "errors": counts["err"],
               "refused": _ref_list,
               "refused_scope": _ref_scope,
               # SUMS THE MERGED LIST, not just this run's - `refused` above is merged,
               # and two keys describing one set must not disagree (R219).
               "refused_rows": sum(r.get("rows", 0) for r in _ref_list),
               "duplicates_collapsed": dropped_total, "seconds": round(dt),
               # THE PARAMETER THE CATALOGUER MUST MATCH, RECORDED WHERE IT CAN READ IT
               # (R833). Persisted nowhere before: not here, and not in _split_map.json,
               # whose entries are {dim, parts, rows}. It was recoverable only by
               # inference from min(split rows), which bounds the cap from ABOVE and not
               # below - so a cataloguer run at the wrong cap read as a frozen pipeline.
               "max_rows": int(a.max_rows),
               # THE SCOPE OF THE RUN THAT SET max_rows. A --dry-run, --only or --limit
               # run is not evidence about the whole store's cap, and the cataloguer
               # refuses to adopt one that says so (R833 follow-up): without this, a
               # one-table dry run at another cap stamps the 8,207-table store.
               "scope": run_scope(a),
               "store_files": n_store,
               # THE LENGTH OF WHAT WAS EXAMINED, not the loop index:
               # trailing refusals never reached the index assignment.
               "processed": len(_examined_stems),
               "dry_run": bool(a.dry_run)}, open(summary, "w"), indent=1)
        print(f"summary -> {summary}")
    else:
        print(f"summary NOT touched (a pinned report records no run): {summary}")
    if a.parts_report:
        # BUILT AFTER q.join(), so "new" means WRITTEN (a successful PUT - or, in a dry run, what a
        # real run would write), never merely queued; every EXAMINED cube gets an entry with its
        # status, so a refused or failed cube is listed rather than silently absent (round-1 review).
        for pid in dict.fromkeys(_examined_stems):
            ids = emitted.get(pid, set())
            ok = ids if a.dry_run else {s for s in ids
                                        if csv_key(a.prefix, s) in written
                                        or csv_key(a.prefix, s) in existing}
            # a cube not judged ok emits nothing to compare: listing all its catalogued ids as
            # "vanished" would invite retiring every served id it has (round-3 review)
            entry = (part_diff(ok, cat_ids.get(pid, set())) if status.get(pid) == "ok"
                     else {"new": [], "vanished": []})
            entry["status"] = status.get(pid, "not_reached")
            entry["unwritten"] = sorted(ids - ok)
            parts_report[pid] = entry
        n_new = sum(len(v["new"]) for v in parts_report.values())
        n_gone = sum(len(v["vanished"]) for v in parts_report.values())
        with open(a.parts_report, "w", encoding="utf-8") as fh:
            json.dump({"dry_run": bool(a.dry_run), "pin_split": bool(a.pin_split),
                       "catalog_db": a.catalog_db, "cubes": parts_report}, fh, indent=1)
        print(f"parts vs the catalogue: {n_new:,} new id(s) (objects with no catalogue row), "
              f"{n_gone:,} vanished id(s) (catalogued, now STALE) -> {a.parts_report}")
    if counts.get("null_part_rows"):
        print(f"rows under a NULL split value, not written (no part id): {counts['null_part_rows']:,}")
    # SAY FAILURE IN THE EXIT CODE (round-2 review, probe P5): every PUT failing used to exit 0.
    # A pinned report also fails when any cube could not be judged, since that is its whole job.
    # WHAT FAILS THE RUN (round-4 review, probe P4). Exit 1 means something that should have been
    # written was not: a PUT error, a scan that failed, a cube whose SERVED ids could not be
    # reproduced (pin_refused) - and, in a report, any cube it could not judge, since judging is
    # its whole job. A cube choose_split REFUSES on a --rekey or first derive (no column divides it
    # under the cap) is a known structural limit: the live summary names five that every full run
    # refuses (37100234, 37100277, 98100023, 98100174, 98100206). It is printed and recorded in the
    # summary, and does NOT fail the run - or a guarded job would never earn its done-sentinel
    # (tools/run_guarded_job.ps1 writes it only on exit 0) and would relaunch a ~7.8-day derive
    # for ever.
    bad = [p for p, s in status.items()
           if s not in ("ok", "refused") or p in pin_refused or (a.pin_split and s == "refused")]
    if counts["err"] or bad:
        print(f"EXIT 1: {counts['err']:,} PUT error(s)"
              + (f"; {len(bad)} cube(s) not ok: {', '.join(bad[:10])}" if bad else ""))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
