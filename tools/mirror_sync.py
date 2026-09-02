"""Sync exactly the files a footer_diff run classified as BEHIND or R2-ONLY. Never the AHEAD ones.

WHY THIS EXISTS AS A SEPARATE TOOL. The obvious move — "the mirror is stale, copy the source
down" — is the one that destroys data, because divergence is not uniform. Three sources are
currently AHEAD of the store on some files while behind on others, and the last time a blind
`aws s3 sync`-shaped operation ran against ilostat it overwrote 41 ahead files and took 967,043
rows with it (ledger R388). So the copy list is never computed here: it is read from a
footer_diff JSON, which classified every file in both directions by parquet footer, and the
`ahead` list is printed as a MERGE queue and skipped.

It also refuses to run against a stale classification, in two places. The whole JSON is refused
past --max-age-hours (default 6), which is a cheap outer bound and nothing more. The real check
is per file: footer_diff records the R2 row count it saw for every behind file, and each object
is compared against that count once it is downloaded - free, because the bytes are already
here. A file whose footer disagrees is left alone and named in the run's output; the answer
there is to re-run footer_diff, not to copy from a snapshot of the past.

    python tools/footer_diff.py --all --json data/_probe/fleet_diff.json
    python tools/mirror_sync.py --from-json data/_probe/fleet_diff.json --apply
"""
from __future__ import annotations

import argparse
import threading
import time
import concurrent.futures
import json
import os
import uuid
import sys

import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

BUCKET = "econ-data"


MIRROR_SYNC_WORKERS = 16
# EVERY comparison queues here, not just the whole-row ones. `memory_limit` bounds DuckDB's
# buffer pool and not the process, and both paths exceed it on the real mirror: whole-row on
# baci_hs96 (266,028,708 rows) peaked at 20.9 GB RSS with 25.83 GB spilled, and the KEYED path
# on cbs_nl/37731 (1,056,918,900 rows) peaked at 22.0 GB with 93.2 GB - worse on both axes, and
# it was the ungated one. Sixteen of those is 352 GB against 331.4 GB free, and 200 keyed files
# hold 100M+ rows (165 in statcan alone), so it is one source away, not hypothetical (R628).
# Downloads still run MIRROR_SYNC_WORKERS wide; only the comparison queues.
#
# EIGHT, measured rather than guessed. Peak RSS tracks the CAP, not the file: the same file at
# memory_limit 4/8/12/20 GB peaked at 4.4/8.5/12.3/12.6 GB, so the envelope is
# COMPARE_SLOTS x (DUCK_MEM_GB + ~2) and 8 slots is 176 GB of the 331.4 GB free, with the
# crawlers measured under 1 GB. DUCK_MEM_GB stays at 20 rather than shrinking to buy slots -
# spill runs the other way (0.00 GB at cap 20, 9.97 GB at cap 4). Sixteen is the only setting
# that does not fit (R637).
COMPARE_SLOTS = 8
_compare_gate = threading.Semaphore(COMPARE_SLOTS)
DUCK_MEM_GB = 20          # per connection; 16 workers x 20 GB = 320 GB on the 384 GB box (R605)
DUCK_THREADS = 4


class IdentityCheckFailed(Exception):
    """The identity check itself could not run (a date string that will not cast, a missing
    column in the incoming file): the local copy is KEPT and the failure is recorded - never
    try_cast (a NULL from a failed cast would match every NULL, R605)."""


def lost_identities(local_path: str, new_path: str):
    """(count, description) of identities the LOCAL copy holds that the incoming copy lacks.

    A SECOND line of defence behind footer_diff's classification. footer_diff answers "is this
    file behind?" by row count and max date; it cannot see a file that grows overall while
    dropping individual observations. Measured 2026-09-01 while repairing 1,384 files: 228
    ilostat files were BEHIND (R2 ahead on rows AND dates) and still lacked identities the
    local copy held — CCF_XPPP_CUR_RT_A gained a full year to 2026-01-01 while dropping four
    countries' 1990-91 values.

    duckdb ANTI JOIN, not Python sets: it streams and spills, so there is no size cap and no
    population exempt from the check. An earlier version capped at 3M rows and the ten LARGEST
    files were therefore replaced unchecked, after which the local copy was gone and the
    question could never be answered (R550).

    NEVER guess the date column positionally. `cols[1]` is gleif's `LegalName` and defillama's
    `name`; comparing on those made every RENAME look like a lost observation and refused three
    files with zero identities actually lost (R551). With no time axis, identity is the key.

    COPY-AWARE, not a set test (R571). cbs_nl's period parser makes (series_key, obs_date)
    NON-unique (R573: 74 of 374 publish files carry duplicate pairs), and `EXCEPT` is a set
    operation, so an incoming file that drops some COPIES of a duplicated pair passed the old
    check with 0. The count is now sum over identities of max(local_copies - incoming_copies, 0):
    identical to the set count when keys are unique, larger when copies are lost.

    NULL-SAFE (R595). A `LEFT JOIN ... USING` compares with `=`, and NULL never equals NULL, so
    every local row with a NULL key or date counted as lost - three real fdic files compared
    UNEQUAL TO THEMSELVES (history.parquet: 14,058). The join is `IS NOT DISTINCT FROM` on both
    columns. Temporal columns are compared as DATE on both sides (a DATE vs TIMESTAMP drift made
    every row "lost"). When NEITHER side has a duplicate identity the set `EXCEPT` is used (it
    gives the same answer there; no per-identity aggregation). R605: the DATE cast runs in UTC
    (a TIMESTAMP WITH TIME ZONE cast in the host's zone made 3 of 3 rows "lost" here and 0 on a
    UTC runner) and the date column's type is read from BOTH files (the incoming one is the one
    that drifts). The connection is capped and spills under the repo's logs/_duckspill, sized so
    MIRROR_SYNC_WORKERS callers cannot exhaust the box (an unsized cap spilled 36 GB under the
    CWD on area_16; the earlier "~50x cheaper" was never measured and is withdrawn).
    """
    import duckdb
    import shutil
    # THE GATE IS TAKEN BEFORE THE CONNECTION OPENS, not around the query. A DuckDB connection
    # reserves its buffer pool, so gating only the comparison would still let sixteen of them
    # exist at once - the reviewer's point that the old gate at the query bounded comparisons
    # and never connections (R628). Downloads are unaffected; they hold no connection.
    with _compare_gate:
        return _lost_identities_gated(local_path, new_path)


def _lost_identities_gated(local_path: str, new_path: str):
    import duckdb
    import shutil
    q = duckdb.connect()
    q.execute("SET TimeZone='UTC'")
    q.execute(f"SET memory_limit='{DUCK_MEM_GB}GB'")
    q.execute(f"SET threads={DUCK_THREADS}")
    # ONE spill directory PER CONNECTION (R612): DuckDB names spill files by block size with no
    # instance id, so two instances sharing a directory open each other's files and the process
    # segfaults with no traceback (measured: N = 2, 8, 16 connections all exit 139).
    spill = os.path.join(ROOT, "logs", "_duckspill", f"mirror_sync_{os.getpid()}_{uuid.uuid4().hex[:8]}")
    os.makedirs(spill, exist_ok=True)
    q.execute(f"SET temp_directory='{spill.replace(os.sep, '/')}'")
    try:
        return _lost_identities(q, local_path, new_path)
    finally:
        q.close()
        shutil.rmtree(spill, ignore_errors=True)


def _lost_identities(q, local_path: str, new_path: str):
    lp = str(local_path).replace(os.sep, "/")
    rp = str(new_path).replace(os.sep, "/")
    desc = q.execute(f"describe select * from read_parquet('{lp}')").fetchall()
    desc_r = q.execute(f"describe select * from read_parquet('{rp}')").fetchall()
    cols = [r[0] for r in desc]
    types = {r[0]: str(r[1]).upper() for r in desc}
    types_r = {r[0]: str(r[1]).upper() for r in desc_r}
    kc = next((c for c in cols if c.lower() in ("series_key", "series_id", "key")), None)
    dc = next((c for c in cols if c.lower() in ("obs_date", "date", "time_period")), None)
    # ONLY THE COLUMNS THE IDENTITY USES MUST EXIST ON BOTH SIDES. Requiring every column was
    # right for the whole-row path, where every column IS the identity, and wrong for a keyed
    # file: retiring or renaming an unrelated column - a routine publisher change that loses no
    # observation - refused the file PERMANENTLY, because nothing clears the condition and the
    # same schema arrives on every later pass. The refusal was then subtracted from the pulled
    # count without a word (R624).
    needed = [c for c in (kc, dc) if c] if kc else list(cols)
    missing = [c for c in needed if c not in types_r]
    if missing:
        raise IdentityCheckFailed(
            f"the incoming file is missing {len(missing)} column(s) the identity needs: "
            f"{missing[:6]}")
    if kc is None:
        # NO NAMED KEY COLUMN -> THE IDENTITY IS THE WHOLE ROW. The previous code took
        # `cols[0]`, untested, which on the real mirror meant comparing 89,207,221 cepii_baci
        # trade flows on `year` alone, cftc on a float measure, and edgar_pointers on `cik`
        # (28.3 rows per value). Every row of a real edgar_pointers shard could be replaced and
        # the check still returned 0 lost - after which the local copy was gone and the ledger
        # was deleted for having nothing to report (R617, and R550 before it). 1,598 files and
        # 827,032,326 rows across 12 sources took that path. A full-row multiset comparison
        # cannot miss a replaced row, and duckdb streams and spills it like any other.
        return _lost_rows_whole(q, lp, rp, cols, types, types_r)
    kq = kc.replace('"', '""')
    if dc is None:
        ident = f'"{kq}"::VARCHAR'
        cond = "l.k IS NOT DISTINCT FROM r.k"
        mode = (f"key-only on {kc!r}, copy-aware, NULL-safe "
                f"(this schema has no date column)")
    else:
        dq = dc.replace('"', '""')
        temporal = any(t in (types.get(dc, "") + " " + types_r.get(dc, "")) for t in ("DATE", "TIMESTAMP"))
        dexpr = f'"{dq}"::DATE::VARCHAR' if temporal else f'"{dq}"::VARCHAR'
        ident = f'"{kq}"::VARCHAR, {dexpr}'
        cond = "l.k IS NOT DISTINCT FROM r.k AND l.d IS NOT DISTINCT FROM r.d"
        mode = (f"({kc}, {dc}), copy-aware, NULL-safe"
                + (", temporal as DATE in UTC" if temporal else ""))
    dups = lambda path: q.execute(  # noqa: E731
        f"select count(*) - count(distinct ({ident})) from read_parquet('{path}')").fetchone()[0]
    if dups(lp) == 0 and dups(rp) == 0:
        # unique identities on both sides: the set difference IS the copy-aware count
        n = q.execute(f"select count(*) from (select {ident} from read_parquet('{lp}') "
                      f"except select {ident} from read_parquet('{rp}'))").fetchone()[0]
        return int(n), mode + " (unique identities: set path)"
    # grouped per-identity counts, named k[, d] on both sides
    if dc is None:
        left = f"select \"{kq}\"::VARCHAR k, count(*) c from read_parquet('{lp}') group by 1"
        right = f"select \"{kq}\"::VARCHAR k, count(*) c from read_parquet('{rp}') group by 1"
    else:
        left = f"select \"{kq}\"::VARCHAR k, {dexpr} d, count(*) c from read_parquet('{lp}') group by 1, 2"
        right = f"select \"{kq}\"::VARCHAR k, {dexpr} d, count(*) c from read_parquet('{rp}') group by 1, 2"
    n = q.execute(f"select coalesce(sum(greatest(l.c - coalesce(r.c, 0), 0)), 0) "
                  f"from ({left}) l left join ({right}) r on {cond}").fetchone()[0]
    return int(n), mode


_INTEGER = ("HUGEINT", "BIGINT", "INTEGER", "SMALLINT", "TINYINT",
            "UHUGEINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT")
_FLOATING = ("DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC")
_CONTAINER = ("[]", "STRUCT", "MAP", "UNION", "LIST")


def _scalar(typ: str) -> str:
    """The type name with any container syntax stripped away, or '' for a container.

    Matching a SUBSTRING of the rendered type is what made `INTEGER[]` take the numeric branch
    and `STRUCT(REAL_GDP VARCHAR)` take it on a FIELD NAME (R624). A container is compared as
    text, whole, which is exactly right: its rendering is faithful and its parts are not
    separately castable anyway."""
    t = (typ or "").upper().strip()
    if any(m in t for m in _CONTAINER):
        return ""
    return t.split("(")[0].strip()


def _norm(col: str, typ: str, typ_other: str = "") -> str:
    """One column as comparable text.

    INTEGERS COMPARE AS INTEGERS, when BOTH sides are integers. Routing them through DOUBLE
    loses everything past 2^53: BIGINT 9007199254740993 and ...992 compared EQUAL. Measured
    headroom on the real files is 9x, so nothing is exposed today, and exactness is free
    (R624). When the publisher has CHANGED the column from an integer to a float, both sides go
    through DOUBLE instead - otherwise 1 and 1.0 would read as a lost row - and above 2^53 that
    trade is unavoidable without inventing a loss on every such column.

    FLOATS go through DOUBLE with a +0.0, so -0.0 and 0.0 are one value and DOUBLE 1.0 matches
    an integer 1 written as a float (R617 MINOR).

    TEMPORAL COLUMNS KEEP THEIR TIME. The keyed path casts ONE named time axis to DATE, on
    purpose; doing that to every column in whole-row mode discards the time of day that IS the
    grain of 409 files and 103,679,078 rows - two filings at 09:30 and 16:00 replaced by two
    different filings the same day compared EQUAL (R624). TIMESTAMP still reconciles a DATE
    against a TIMESTAMP without throwing anything away.

    BLOBS compare as hex: rendered to VARCHAR, a 0x00 byte and the four characters \x00 are
    the same string."""
    q = col.replace('"', '""')
    t = _scalar(typ)
    o = _scalar(typ_other) if typ_other else t
    if not t or not o:
        return f'"{q}"::VARCHAR'                      # container: compare its rendering whole
    if t in _INTEGER and o in _INTEGER:
        return f'"{q}"::VARCHAR'                      # exact, no float round-trip
    if t in _FLOATING or o in _FLOATING:
        return f'(CAST("{q}" AS DOUBLE) + 0.0)::VARCHAR'
    if t == "TIME" or o == "TIME":
        # DuckDB has no TIME -> TIMESTAMP cast, so the old rule produced a permanent CHECK
        # FAILED on any TIME column. Text is faithful for a wall-clock time (R628).
        return f'"{q}"::VARCHAR'
    if (t.startswith("DATE") or t.startswith("TIMESTAMP")
            or o.startswith("DATE") or o.startswith("TIMESTAMP")):
        # BOTH sides, like the keyed path. Consulting only this side made the comparison
        # ORDER-DEPENDENT: a local VARCHAR date against an incoming TIMESTAMP counted 1 lost,
        # and the same pair with the roles swapped counted 0 (R628).
        return f'"{q}"::TIMESTAMP::VARCHAR'
    if t == "BLOB":
        return f'hex("{q}")'
    return f'"{q}"::VARCHAR'


def _lost_rows_whole(q, lp: str, rp: str, cols, types, types_r):
    """Copy-aware count of WHOLE ROWS the local file holds that the incoming file lacks."""
    return _lost_rows_whole_locked(q, lp, rp, cols, types, types_r)


def _lost_rows_whole_locked(q, lp: str, rp: str, cols, types, types_r):
    def nrm(c):
        # BOTH types, because the reconciliation depends on the PAIR: two integers compare
        # exactly, an integer against a float must go through DOUBLE (R624).
        return _norm(c, types.get(c) or types_r.get(c, ""), types_r.get(c) or types.get(c, ""))
    exprs = ", ".join(nrm(c) for c in cols)
    sel = ", ".join(f"{nrm(c)} c{i}" for i, c in enumerate(cols))
    on = " AND ".join(f"l.c{i} IS NOT DISTINCT FROM r.c{i}" for i in range(len(cols)))
    mode = f"WHOLE ROW over {len(cols)} column(s) (no key column in this schema), copy-aware, NULL-safe"
    dl = q.execute(f"select count(*) - count(distinct ({exprs})) from read_parquet('{lp}')").fetchone()[0]
    dr = q.execute(f"select count(*) - count(distinct ({exprs})) from read_parquet('{rp}')").fetchone()[0]
    if dl == 0 and dr == 0:
        n = q.execute(f"select count(*) from (select {exprs} from read_parquet('{lp}') "
                      f"except select {exprs} from read_parquet('{rp}'))").fetchone()[0]
        return int(n), mode + " (unique rows: set path)"
    grp = ", ".join(str(i + 1) for i in range(len(cols)))
    left = f"select {sel}, count(*) c from read_parquet('{lp}') group by {grp}"
    right = f"select {sel}, count(*) c from read_parquet('{rp}') group by {grp}"
    n = q.execute(f"select coalesce(sum(greatest(l.c - coalesce(r.c, 0), 0)), 0) "
                  f"from ({left}) l left join ({right}) r on {on}").fetchone()[0]
    return int(n), mode


def reap_dead_spill() -> int:
    """Remove logs/_duckspill/* directories whose owning pid is gone (R612: 71.8 GB of orphaned
    spill from dead processes). DuckDB temp files carry no data; a live pid's directory is kept."""
    import glob
    import re
    import shutil
    try:
        import psutil
    except ImportError:
        return 0
    freed = 0
    for d in glob.glob(os.path.join(ROOT, "logs", "_duckspill", "*")):
        m = re.match(r"^(?:pid|mirror_sync_)(\d+)", os.path.basename(d))
        if not m or psutil.pid_exists(int(m.group(1))):
            continue
        freed += sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(d) for f in fs)
        shutil.rmtree(d, ignore_errors=True)
    return freed


def sync_source(s3, rec, apply: bool):
    src, root = rec["source"], rec["root"]
    names = [n for n, _l, _r in rec["behind"]] + list(rec["r2_only"])
    # The recorded counts are no longer consulted: the check re-classifies the bytes in hand
    # with footer_diff's own decision function, which uses BOTH axes and both sides (R631).
    ahead = [n for n, _l, _r in rec["ahead"]]
    if not names:
        return 0, ahead
    d = os.path.join(ROOT, "data", root, src)
    if not apply:
        return len(names), ahead
    os.makedirs(d, exist_ok=True)
    freed = reap_dead_spill()
    if freed:
        print(f"   reaped {freed / 1e9:.1f} GB of DuckDB spill left by dead processes")
    fail = []
    stale_files = []
    weak_identity = []
    unreadable_local = []
    one_axis = []
    check_failed = []
    withdrawals = []
    whole_row = []      # files with no key column: compared on every column (R617)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    ledger = os.path.join(ROOT, "logs", f"_mirror_sync_withdrawals_{stamp}_{src}.tsv")
    os.makedirs(os.path.dirname(ledger), exist_ok=True)
    led = open(ledger, "a", encoding="utf-8")
    led.write("source\tfile\trows_lost\tmode\toutcome\n"); led.flush()
    lock = threading.Lock()

    def record(n, lost, mode, outcome):
        with lock:
            led.write(f"{src}\t{n}\t{lost}\t{mode}\t{outcome}\n"); led.flush(); os.fsync(led.fileno())

    def one(n):
        # `n` is a RELATIVE PATH, which for bea and eia contains a directory component.
        try:
            dest = os.path.join(d, *(n + ".parquet").split("/"))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            # Download BESIDE the target, check, then replace. Downloading straight onto
            # `dest` destroys the local copy before anything has looked at it, which is also
            # what makes the loss unverifiable afterwards (R550). Unique temp name so two
            # runs cannot share it, and the temp is cleaned on every path.
            tmp = f"{dest}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
            try:
                s3.download_file(BUCKET, f"{root}/{src}/{n}.parquet", tmp)
                # RE-CLASSIFY THE BYTES IN HAND, with the producer's own decision function.
                #
                # The first version of this compared the object's row count against the count
                # footer_diff recorded and admitted anything that had merely GROWN. Row count
                # is one of TWO axes classify() uses, and the other inverts independently:
                # local(100 rows, 2021-06) against R2(120 rows, 2019) is 'ahead' - the merge
                # queue, which must never be overwritten - and "more rows than recorded"
                # admits it. Measured end to end: four observations through 2021 destroyed and
                # logged as "replaced (publisher ahead)", which is R388's shape arriving
                # through the guard built to prevent it (R631).
                #
                # classify() on the local file and the downloaded one costs two footer reads,
                # one of which was already happening, and needs nothing from the JSON. It also
                # catches a LOCAL file changed by a derive since the sweep, and an r2_only file
                # that turns out to exist locally - which is itself a stale classification.
                local_is_junk = False
                junk_note = None
                weak_pct = None
                if os.path.exists(dest):
                    from tools.footer_diff import classify, file_meta
                    # AN UNREADABLE LOCAL FILE IS NOT A FILE TO PROTECT - but only when it is
                    # unreadable because its BYTES ARE BAD. Five failures reach this except and
                    # exactly one of them justifies replacing (R641):
                    #   corrupt parquet            ArrowInvalid      -> replace
                    #   a write still in progress  ArrowInvalid      -> WAIT: same exception,
                    #                                                  same message, told apart
                    #                                                  only by how recently the
                    #                                                  file was touched
                    #   vanished between listing and now  FileNotFoundError -> nothing to lose
                    #   locked by another writer   OSError           -> someone is using it
                    #   a directory where a file belongs  PermissionError -> structural
                    try:
                        lm = file_meta(dest)
                    except FileNotFoundError:
                        lm = None
                        local_is_junk = True        # there is no local copy to protect
                    except Exception as e:                          # noqa: BLE001
                        msg = " ".join(str(e).split())[:100]
                        # IS IT A PARQUET FAULT? Ask the LIBRARY, not the class name. On
                        # pyarrow 23 the MRO is ArrowInvalid < ValueError < ArrowException, so
                        # a name match on "ArrowException" is already dead, and a version bump
                        # routing a bad footer through ArrowIOError or ArrowCapacityError would
                        # stop matching entirely - failing safe, but silently disabling the
                        # repair and mislabelling a rotten file as locked (R645).
                        try:
                            import pyarrow as _pa
                            corrupt = isinstance(e, _pa.lib.ArrowException)
                        except Exception:                           # noqa: BLE001
                            corrupt = e.__class__.__name__.startswith("Arrow")
                        # IS SOMEONE WRITING IT RIGHT NOW? mtime is a guess; an exclusive-open
                        # probe is an answer. os.rename(p, p) raises PermissionError on Windows
                        # while another process holds the file open and succeeds when it does
                        # not - one syscall, indifferent to how slow the writer is (R645).
                        # POSIX renames regardless, so mtime stays as the fallback there.
                        recent_s = 600
                        try:
                            os.rename(dest, dest)
                            being_written = False
                        except OSError:
                            being_written = True
                        if not being_written:
                            try:
                                being_written = (time.time() - os.path.getmtime(dest)) <= recent_s
                            except OSError:
                                being_written = True
                        age = None
                        try:
                            age = time.time() - os.path.getmtime(dest)
                        except OSError:
                            pass
                        if corrupt and not being_written:
                            lm = None
                            local_is_junk = True
                            # HELD, NOT APPENDED. The summary line for this list says the files
                            # "have been replaced", and a refused os.replace left that sentence
                            # printed over bytes still on disk - R641's headline defect
                            # returning on the failure path (R652).
                            junk_note = (n, repr(e)[:60])
                            record(n, "", "", f"LOCAL COPY CORRUPT ({int(age or 0)}s old, no "
                                              f"writer holds it) - replacing it: {msg}")
                        else:
                            why = ("is being written right now" if corrupt else
                                   "cannot be opened - it may be locked, or not a file")
                            check_failed.append((n, f"local {why}: {e!r}"[:80]))
                            record(n, "", "", f"LOCAL COPY {why.upper()} - local kept: {msg}")
                            return
                    if lm is not None:
                        try:
                            rm = file_meta(tmp)
                        except Exception as e:                      # noqa: BLE001
                            check_failed.append((n, f"incoming unreadable: {e!r}"[:80]))
                            record(n, "", "", "INCOMING FILE UNREADABLE - local kept: "
                                              f"{' '.join(str(e).split())[:100]}")
                            return
                        verdict = classify(lm, rm)
                        # WHICH AXIS DECIDED. classify() uses row count and max observation
                        # date, and the date vanishes for 1,984 of the 55,906 mirror files -
                        # 1,980 with no recognised date column and 4 whose statistics are
                        # absent. For those the verdict IS the row-count comparison this check
                        # was built to replace, so it is counted and printed rather than
                        # passing as if both axes had spoken. They are disproportionately the
                        # keyless files, where the whole-row identity check is the real guard
                        # (R637).
                        if lm[1] is None or rm[1] is None:
                            one_axis.append(n)
                    else:
                        verdict = "behind"          # nothing readable to compare against
                    if verdict != "behind":
                        stale_files.append((n, verdict))
                        record(n, "", "", f"STALE CLASSIFICATION - local kept: on the bytes in "
                                          f"hand this file is '{verdict}', not 'behind' - the "
                                          f"sweep no longer describes it")
                        return
                lost, mode, weak = 0, "", ""
                # A LOCAL FILE THAT COULD NOT BE READ CANNOT BE COMPARED EITHER. Skipping the
                # classify step and then running the identity check on the same corrupt bytes
                # is what made the previous version announce a replacement and perform none:
                # DuckDB raised on the same file, the run recorded "CHECK FAILED - not
                # replaced", and returned before os.replace (R641).
                if os.path.exists(dest) and not local_is_junk:
                    try:
                        lost, mode = lost_identities(dest, tmp)
                    except (IdentityCheckFailed, Exception) as e:      # noqa: BLE001
                        # R605: the CHECK failed, not the download - keep the local copy, say so.
                        # IdentityCheckFailed is named first so the intent is visible: it is
                        # RAISED now (R617 MEDIUM 3 - it was defined, never raised, never caught).
                        check_failed.append((n, repr(e)[:80]))
                        record(n, "", "", f"CHECK FAILED - not replaced: {' '.join(str(e).split())[:100]}")
                        return
                    if mode.startswith("WHOLE ROW"):
                        with lock:
                            whole_row.append(n)
                    # ONE DESCRIPTION OF THE WEAKNESS, NOT THREE OVERLAPPING ONES. A missing
                    # date axis, a key-only identity and a loss are not alternatives: a keyed,
                    # dateless file that loses a row is all three at once. Reported separately
                    # they produced five printed lines and four ledger lines for ONE file -
                    # three of them saying "replaced" and one saying "kept" - so an operator
                    # summing the summary counted five files where there was one (R650).
                    #
                    # THIS DISCLOSES RATHER THAN REFUSING, because no remedy offered so far is
                    # safe. Refusing is a permanent outage: the condition is a property of the
                    # file, so a footer_diff re-run re-derives the same verdict for ever (R647).
                    # Giving it the WHOLE-ROW identity re-opens R551 - gleif is exactly this
                    # shape, a key and no date, and whole-row counts a RENAME as a loss
                    # ("comparing on (key, LegalName) reported 6,817 losses with 0 LEIs gone").
                    # And no discriminator survives its own control: "a registry has one row per
                    # key" dies on gleif itself (3,416,994 rows, 3,416,756 distinct LEIs), and a
                    # type rule dies on census/pseo__earnings, which stores earnings as strings.
                    # Separating a NAME from a VALUE needs to know which column is a measure,
                    # which is the schema hint R551 forbids inventing (R649).
                    #
                    # What each caveat means:
                    #   no date axis   classify() decided on the row count alone, and R631
                    #                  showed "more rows" coexists with an inversion;
                    #   key-only       a restatement under an unchanged key is invisible to
                    #                  both signals;
                    #   rows lost      with no date axis this may be a withdrawal OR an
                    #                  attribute change, and nothing here can tell them apart.
                    #
                    # AND THE LOSS IS A FRACTION, not a bare count: "1 row" and "3,400,000 rows"
                    # printed the same shape, where 0.0000% reads as attribute churn and 100%
                    # reads as wholesale replacement. lm[0] is the local row count already read
                    # from the footer above, so this costs no I/O.
                    # THE CAVEATS SAY WHAT THEY MEAN, in the ledger, in full. This file is the
                    # durable record - the printed summary scrolls away and the person reading
                    # the TSV months later has only this column - so each caveat carries its
                    # consequence, not a label that has to be looked up (R550, R649).
                    if n in one_axis:
                        caveats = ["no date axis, so the verdict is the row count alone"]
                        # THE IDENTITY'S OWN WORDS, not the absence of another
                        # mode. `one_axis` fires on two causes - no recognised date column, OR
                        # the column present with no row-group statistics (1,980 and 4 files
                        # respectively) - and on the second the identity IS (key, date). Gated
                        # on "not WHOLE ROW" this row read `(series_key, obs_date), copy-aware`
                        # in its mode column and "key-only identity" in its outcome column, in
                        # the file whose comment calls it the durable record (R652).
                        if mode.startswith("key-only on "):
                            caveats.append("key-only identity, so a restatement under an "
                                           "unchanged key is invisible")
                        if lost:
                            local_rows = lm[0] if lm else 0
                            weak_pct = (100.0 * lost / local_rows) if local_rows else None
                            pct = (f"{weak_pct:.4f}%" if weak_pct is not None
                                   else "share unknown")
                            caveats.append(f"{lost:,} of {local_rows:,} local rows lost ({pct}), "
                                           f"and with no date axis that may be a withdrawal OR "
                                           f"an attribute change")
                        # A one-axis file whose identity is WHOLE-ROW and which loses nothing is
                        # already covered by the one_axis line in the summary; it is not a
                        # second population. Only a file with a SECOND caveat is weak.
                        if len(caveats) > 1:
                            weak = "; ".join(caveats)
                    if lost:
                        # footer_diff already established R2 is ahead here, so identities that
                        # vanish are the publisher withdrawing them, not breakage. Follow it —
                        # refusing would keep users on an older vintage to preserve superseded
                        # rows — but never silently: the INTENT is recorded before the replace
                        # and the OUTCOME after it (R612: 'replaced' written first lied when the
                        # replace then failed).
                        record(n, lost, mode, "replacing (publisher ahead) - intent")
                if weak:
                    # INTENT, BEFORE THE REPLACE. Written afterwards it is a lie whenever
                    # os.replace raises - and a held handle does exactly that, which is why the
                    # open-handle probe above exists. The two disclosure lines this replaces
                    # both said "replaced" at decision time: forcing a PermissionError produced
                    # a ledger reading "WEAK IDENTITY - replaced", "ONE-AXIS VERDICT WITH A LOSS
                    # - replaced", then "replace FAILED - local kept", with the file untouched.
                    # R612's third recurrence in this file (R650).
                    record(n, lost, mode, "WEAK COMPARISON - about to replace: " + weak)
                try:
                    os.replace(tmp, dest)
                except OSError as e:
                    check_failed.append((n, f"replace failed: {e!r}"[:80]))
                    record(n, "", "", f"replace FAILED - local kept: {' '.join(str(e).split())[:100]}")
                    return
                # THE ACT HAS HAPPENED. Every list a completed-tense summary line
                # counts is appended HERE and nowhere earlier, so no wording of those lines can
                # outrun `os.replace`. Ordering, not phrasing: round 14 fixed the two ledger
                # lines and left the three printed ones counting lists filled at decision time,
                # which printed "100 were REPLACED" over 50 replacements and "50 file(s) lost
                # 50 ROWS ... followed the publisher" over 0 (R612's fourth recurrence, R652).
                with lock:
                    if lost:
                        withdrawals.append((n, lost, mode))
                    if weak:
                        weak_identity.append((n, lost, weak, weak_pct))
                    if junk_note is not None:
                        unreadable_local.append(junk_note)
                if lost:
                    record(n, lost, mode, "replaced (publisher ahead)")
                if weak:
                    record(n, lost, mode, "WEAK COMPARISON - replaced: " + weak)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
        except Exception as e:                                     # noqa: BLE001
            # A download that failed is a file NOT synced, and the run must be able to say
            # which ones afterwards: printed truncated to three and never written down, they
            # were unrecoverable from the log (R617 MEDIUM 4).
            fail.append((n, repr(e)[:60]))
            record(n, "", "", f"DOWNLOAD FAILED - local kept: {' '.join(str(e).split())[:100]}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MIRROR_SYNC_WORKERS) as ex:
        list(ex.map(one, names))
    led.close()
    if fail:
        print(f"   {src}: {len(fail)} download(s) FAILED {fail[:3]}")
    if unreadable_local:
        print(f"   {src}: {len(unreadable_local)} local file(s) were UNREADABLE and have been "
              f"replaced - they held nothing that could be lost, and refusing would have "
              f"preserved the corruption: {unreadable_local[:3]}")
    if one_axis:
        print(f"   {src}: {len(one_axis)} file(s) were classified on the ROW COUNT ALONE - no "
              f"usable observation-date column on one side or the other, so the date axis said "
              f"nothing; the identity check is the guard for those. e.g. {one_axis[:3]}")
    if weak_identity:
        # NESTED, NOT ADDITIVE. Every file here is one of the one_axis files named above, and a
        # file that also lost rows is counted again in the withdrawals line below. Three lists
        # that overlap by construction were printed as three independent counts, which reads as
        # three populations (R650). One line, one count, said to be a subset.
        # A SHARE, NOT A COUNT. "Worst single loss 2 rows" was 100% of that file,
        # and this line fires on every run that syncs one of the 690 keyed dateless files - an
        # always-firing warning needs a magnitude to be read at all (R650 rule 4, R652).
        lossy = [w for w in weak_identity if w[1]]
        if lossy:
            worst = max(lossy, key=lambda w: (w[3] if w[3] is not None else -1.0, w[1]))
            share = (f"{worst[3]:.4f}% of it" if worst[3] is not None else "share unknown")
            worst_txt = f"Worst single loss {worst[1]:,} rows, {share} ({worst[0]})."
        else:
            worst_txt = "None of them lost a row; the risk is a value restated in place."
        print(f"   {src}: of those, {len(weak_identity)} were REPLACED on a WEAK COMPARISON - a "
              f"missing date axis plus an identity or a loss that cannot be interpreted without "
              f"one. {worst_txt} Refusing them is permanent (R647) and whole-row would count a "
              f"rename as a loss (R551), so each is in {ledger} with exactly what was and was "
              f"not checked: {[w[0] for w in weak_identity][:3]}")
    if stale_files:
        print(f"   {src}: {len(stale_files)} file(s) no longer classify as 'behind' on the "
              f"bytes in hand - the sweep does not describe them any more, local copies kept; "
              f"re-run footer_diff. e.g. {stale_files[:3]}")
    if whole_row:
        print(f"   {src}: {len(whole_row)} file(s) have no key column and were compared on "
              f"EVERY column (whole-row, copy-aware) - the only identity that cannot miss a "
              f"replaced row; e.g. {whole_row[:3]}")
    if check_failed:
        print(f"   {src}: {len(check_failed)} file(s) CHECK FAILED - local copies kept, recorded in {ledger}: {check_failed[:3]}")
    if withdrawals:
        tot = sum(w[1] for w in withdrawals)
        print(f"   {src}: {len(withdrawals)} file(s) lost {tot:,} ROWS (copy-aware; a replaced value "
              f"is not counted) that the incoming copy lacks — followed the publisher, every "
              f"tuple in {ledger} (written before each replace); e.g. {[(w[0], w[1], w[2]) for w in withdrawals[:3]]}")
    if not (withdrawals or check_failed or fail or stale_files or weak_identity
            or unreadable_local):
        try:
            os.remove(ledger)   # nothing to keep
        except OSError:
            pass
    elif fail:
        print(f"   {src}: every failure above is recorded in {ledger}")
    # STALE-refused files are not pulled either. The count subtracted `fail` and
    # `check_failed` and forgot the third not-pulled outcome, so a correctly refused file
    # was reported as synced - verbatim R612, one outcome later (R628).
    return len(names) - len(fail) - len(check_failed) - len(stale_files), ahead


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-json", required=True, help="a footer_diff --all output")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--source", action="append", default=[], help="limit to these sources")
    ap.add_argument("--max-age-hours", type=float, default=6.0,
                    help="refuse a classification older than this (0 disables the check)")
    a = ap.parse_args()

    # THE STALENESS REFUSAL THE DOCSTRING PROMISED. It did not exist: main() read the JSON and
    # never looked at its age or at R2, and the file on disk was 30 hours old (R617 MAJOR 2).
    # A classification ages badly in both directions - a file that was BEHIND may since have
    # been repaired, and one that was AHEAD may since have been superseded - and acting on a
    # stale one is how a copy runs against the wrong side of a divergence (R388).
    age_h = (time.time() - os.path.getmtime(a.from_json)) / 3600.0
    if a.max_age_hours > 0 and age_h > a.max_age_hours:
        print(f"REFUSED: {a.from_json} is {age_h:.1f} h old (limit {a.max_age_hours:.1f} h). "
              f"A classification this old no longer describes R2. Re-run:\n"
              f"    python tools/footer_diff.py --all --json {a.from_json}\n"
              f"or pass --max-age-hours to accept it deliberately.")
        return 2
    print(f"classification: {a.from_json} ({age_h:.1f} h old)")

    d = json.load(open(a.from_json, encoding="utf-8"))
    recs = d["sources"] if "sources" in d else [d]
    if a.source:
        recs = [r for r in recs if r["source"] in set(a.source)]
    todo = [r for r in recs if r["behind"] or r["r2_only"]]
    print(f"MODE: {'APPLY' if a.apply else 'REPORT ONLY'}   "
          f"{len(todo)} source(s) with files to pull\n")

    # The merge queue is built from EVERY record, not just the ones with something to pull.
    # Scoping it to `todo` hid eia's 30 ahead files entirely, because eia has nothing behind —
    # a report that goes quiet about the most divergent source in the fleet, which is the exact
    # shape of hole this session has spent the day closing.
    merge_queue = [(r["source"], [x[0] for x in r["ahead"]]) for r in recs if r["ahead"]]
    total = 0
    for r in sorted(todo, key=lambda r: -(len(r["behind"]) + len(r["r2_only"]))):
        n, ahead = sync_source(s3, r, a.apply) if a.apply else (
            len(r["behind"]) + len(r["r2_only"]), [x[0] for x in r["ahead"]])
        total += n
        note = f"   ({len(ahead)} AHEAD file(s) LEFT ALONE — merge queue)" if ahead else ""
        print(f"  {r['source']:22s} {len(r['behind']):>4} behind + {len(r['r2_only']):>4} "
              f"R2-only = {n:>4} pulled{note}")

    print(f"\n{total:,} file(s) {'pulled' if a.apply else 'would be pulled'}")
    if merge_queue:
        print("\nNOT COPIED — local is AHEAD of the store on these; copying either way loses "
              "rows, so they need a merge decision per file:")
        for src, names in merge_queue:
            print(f"   {src:22s} {len(names):>3}: {', '.join(names[:8])}"
                  + (" ..." if len(names) > 8 else ""))
    # Sources whose whole store is missing locally cannot be compared OR repaired from here.
    for src in d.get("unchecked", []):
        print(f"   UNCHECKED {src}: no local parquets at all — footer_diff could not compare it")
    return 0


if __name__ == "__main__":
    from core import r2_util
    s3 = r2_util.client()
    sys.exit(main())
