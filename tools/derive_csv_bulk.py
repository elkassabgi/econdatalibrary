"""Bulk per-series CSV derive for ONE tidy source — a streaming group-by instead of N scans.

WHY THIS EXISTS ALONGSIDE core/derive_csv.py. That tool resolves each series independently,
which is right for incremental work and quadratic for a backfill: every call runs a predicate
scan over the whole source parquet. Measured on cepii_gravity (93 MB, 69,666,545 rows,
1,143,250 series): 63 ms/series, i.e. 17.4 h serial, and 16 threads did not help because they
contend on the same file. Its 991,707 missing objects were not going to land that way.

WHAT THIS DOES INSTEAD. DuckDB streams the parquet ONCE in `ORDER BY series_key, obs_date`
(spilling to disk, so memory stays flat regardless of source size), and rows for a series are
flushed the moment the key changes. One pass, no per-series scan.

BYTE-EXACTNESS IS THE WHOLE CONTRACT, so it is verified rather than asserted: --verify N
compares this path's output against core.derive_csv._series_csv_bytes for N series sampled
across the FULL key range, and refuses to run unless every one matches exactly. The sample is
random, not the first N — a prefix sample cannot detect a divergence that only appears later,
which is the same reason a 5-key listing once nearly certified a 13%-complete derive (R167).

Usage:
  python tools/derive_csv_bulk.py --source cepii_gravity --verify 300 --dry-run
  python tools/derive_csv_bulk.py --source cepii_gravity --bucket econ-data --skip-existing
"""
from __future__ import annotations
import argparse
import csv
import gzip
import io
import os
import queue
import random
import sys
import threading
import urllib.parse

# Same resolution contract as updater/config.py: ECONDL_ROOT wins, else the repo this file
# lives in. The env override is what lets tests point the tool at a fixture tree instead of
# the production store — without it, a test of this tool IS a run against production.
_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.environ.get("ECONDL_ROOT") or _REPO
sys.path.insert(0, _REPO)
sys.path.insert(0, os.path.join(_REPO, "clients", "python"))


HEADER = ["series_id", "obs_date", "value"]


def csv_key(prefix: str, source: str, series_key: str) -> str:
    """The R2 key for one series' derived CSV. THE single definition of this layout.

    tools/verify_derive_parity.py imports it rather than re-deriving it: if the writer and
    the checker each spelled the encoding out, a drift in one could still show clean parity
    by being wrong the same way in both, which is precisely the failure a parity check is
    supposed to catch.
    """
    return f"{prefix}/{urllib.parse.quote(f'{source}:{series_key}', safe='')}.csv"


def csv_key_prefix(prefix: str, source: str) -> str:
    """The listing prefix covering every series of one source."""
    return f"{prefix}/{urllib.parse.quote(source + ':', safe='')}"


def _retry(fn, what, tries=8):
    """Call fn() with patient backoff. R2 answers ServiceUnavailable / SlowDown under load —
    'Reduce your concurrent request rate for the same object' killed a run in the
    skip-existing LISTING, which had no retry at all while the PUT path did. A resume step
    that dies on a throttle is worse than no resume: it forces a re-run that throttles harder.
    """
    import time as _t
    from core.cutover import CutoverRefused                  # noqa: PLC0415
    last = None
    for attempt in range(tries):
        try:
            return fn()
        except (ValueError, CutoverRefused):
            # a REFUSAL (not gzip, a bad endpoint, the post-T0 single-writer rule) gives the same answer on
            # every try: 7 sleeps (~2 min) per key were spent on it (R1220 finding 4)
            raise
        except Exception as e:                               # noqa: BLE001
            last = e
            if attempt == tries - 1:
                break
            wait = min(60, 2 ** attempt)
            print(f"  {what} retry {attempt+1}/{tries} in {wait}s ({str(e)[:70]})", flush=True)
            _t.sleep(wait)
    raise last


def _csv_bytes(short_id: str, rows) -> bytes:
    """CSV for one series. Mirrors core.derive_csv._series_csv_bytes: same header, the SOURCE
    PREFIX STRIPPED from the id column, and lineterminator='\\n' so bytes match the Worker."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    for d, v in rows:
        w.writerow([short_id, d, v])
    return buf.getvalue().encode("utf-8")


def _stream_one(con, path):
    """Yield (series_key, [(obs_date, value), ...]) in key order, one series at a time."""
    cur = con.execute(
        "SELECT series_key, obs_date, value FROM read_parquet(?) "
        "ORDER BY series_key, obs_date", [path])
    key = None
    acc = []
    while True:
        batch = cur.fetchmany(100_000)
        if not batch:
            break
        for k, d, v in batch:
            if k != key:
                if key is not None:
                    yield key, acc
                key, acc = k, []
            acc.append((d, v))
    if key is not None:
        yield key, acc


def _stream(con, paths, qualify=False):
    """Same, over a SHARDED source: one sorted scan PER SHARD, in sequence.

    Sharded stores are the norm above a certain size - noaa is 417 country-prefix shards over
    549,412,914 rows - and a single ORDER BY across all of them would sort the whole corpus to
    produce output that is already grouped. Per-shard sorting is bounded by the largest shard
    instead of by the source.

    WITH qualify=True the emitted id becomes `<shard>:<series_key>`, which is what a source
    whose CATALOGUE ids carry the shard needs - fed_board:H15:RIFSPFF_N.B, fhfa:annual_cbsa:01.
    Deriving those under a bare key would write every CSV to the wrong R2 object, so the
    catalogue would list 142,028 series whose downloads all 404. The disjointness check below
    is then unnecessary (the shard is IN the id, so two shards cannot collide) and is skipped.

    THE PRECONDITION, when qualify is False, IS THAT SHARDS DO NOT SHARE A series_key, and it
    is CHECKED, not assumed:
    if two shards held the same key, each would flush its own CSV and the second would silently
    overwrite the first with a partial history. The check is a running set of keys already
    emitted, which costs one string per series and turns a silent truncation into a loud stop.
    """
    if isinstance(paths, str):
        paths = [paths]
    emitted: set[str] = set()
    for i, p in enumerate(paths, 1):
        shard = os.path.splitext(os.path.basename(p))[0]
        if len(paths) > 1:
            print(f"  shard {i}/{len(paths)}: {os.path.basename(p)}", flush=True)
        for k, rows in _stream_one(con, p):
            if len(paths) > 1 and not qualify:
                if k in emitted:
                    raise SystemExit(
                        f"REFUSING to continue: series_key {k!r} appears in more than one shard "
                        f"({os.path.basename(p)} and an earlier one). Per-shard streaming would "
                        f"write this series twice and keep only the last, partial history. "
                        f"Derive this source with a single sorted scan instead.")
                emitted.add(k)
            yield (f"{shard}:{k}" if qualify else k), rows


# The window a clear holds the state store: pull, clear, push. Measured on the desktop over six passes on
# 2026-09-20..21 (logs/local_heavy_*.log): pull-state 1 min 36 s to 2 min 01 s, push-state 5 min 37 s to
# 6 min 51 s - and push_state's CAS is a HEAD compare followed by an unconditional PUT after VACUUM INTO +
# zstd, so the whole push is race window (R1102 rule 3). Asked for with a 3x margin.
CLEAR_WINDOW_MIN = 30
LOCAL_MIN_HOURS = 20          # tools/run_local_heavy.ps1 `$MinHours` default; pinned by a test


def _writers_quiet(manual: str, window_min: int = CLEAR_WINDOW_MIN) -> bool:
    """True only when NO other state writer can start inside the next `window_min` minutes (R1102).

    The local lock alone covered neither of the other two writers:
      - the cloud updater workflows (tools/ci_writer_gate.py). A queued updater-heavy run read BLOCKED
        while this tool, checking only the lock, would have pulled and pushed. The gate's --until-block
        answer is the free time left; less than the window is a refusal, and so is "cannot tell";
      - a desktop pass that STARTS mid-envelope. The guard ticks every 5 min and starts a pass the
        moment one is due and CI is clear - which is exactly the moment this check passes. So a clear
        runs only while the last pass is recent enough that the next is not due within the window.
    """
    import subprocess
    gate = os.path.join(_REPO, "tools", "ci_writer_gate.py")
    try:
        p = subprocess.run([sys.executable, gate, "--until-block"], cwd=_REPO, capture_output=True,
                           text=True, encoding="utf-8", errors="replace", timeout=300)
        out = (p.stdout or "").strip()
        free = int(out.splitlines()[-1]) if p.returncode == 0 and out else None
    except Exception as e:  # noqa: BLE001 - cannot tell is never clear
        out, free = f"{type(e).__name__}: {e}", None
    if free is None or free < window_min:
        print(f"NOT clearing: the CI writer gate does not show {window_min} free minutes "
              f"({'cannot tell: ' + out[-200:] if free is None else f'{free} min before a cloud writer may run'}). "
              f"A pull/push now can make a cloud run lose its whole bookkeeping at its push (R5, R1102). "
              f"The debt stands. Retry later: {manual}", flush=True)
        return False
    stamp = os.path.join(ROOT, "logs", "local_heavy.last_success")
    import datetime as _dt
    since_h = None
    try:
        with open(stamp, encoding="ascii") as f:
            last = _dt.datetime.fromisoformat(f.readline().strip().replace("Z", "+00:00"))
        if last.tzinfo is None:
            last = last.replace(tzinfo=_dt.timezone.utc)
        since_h = (_dt.datetime.now(_dt.timezone.utc) - last).total_seconds() / 3600.0
    except (OSError, ValueError):
        pass                                   # the runner reads an absent/unreadable stamp as DUE
    if since_h is None or since_h + window_min / 60.0 >= LOCAL_MIN_HOURS:
        print(f"NOT clearing: a desktop heavy pass is due within {window_min} min "
              f"({'no readable ' + stamp if since_h is None else f'last stamped pass {since_h:.1f} h ago, cadence {LOCAL_MIN_HOURS} h'}), "
              f"and the guard starts one the moment CI is clear. Run this after a desktop pass whose "
              f"log says 'cadence stamped' - a pass that logs 'cadence NOT stamped' leaves the next "
              f"one due ~10 min later (R1102 rule 2). The debt stands: {manual}", flush=True)
        return False
    return True


def _durable_clear(source: str) -> bool:
    """Clear the full_rederive_owed row THROUGH the pull→push protocol (R529).

    The first cut cleared only the local state.db — and every heavy pass
    wholesale-replaces that file on pull (R340), so a row that had been pushed
    RESURRECTED after the campaign's clear (debt un-clearable through the
    documented path, ATTENTION forever), while a row never pushed was deleted
    mid-campaign by the pass's pull (the evaporation the row exists to prevent).
    Any state transition that must outlive this process goes pull → change →
    push, like every other write in the store's lifecycle.

    REFUSED while the heavy runner's lock is live: a pull now would wholesale-
    replace state the pass is still writing. The debt then STANDS — the honest
    state — and the printed command re-runs the clear after the pass.

    ALSO REFUSED unless no OTHER state writer can start inside the envelope (_writers_quiet,
    R1102): the cloud updater workflows, and a desktop pass that comes due mid-envelope. Both
    are checked before the pull and again before the push.
    """
    import subprocess
    manual = f"py tools/derive_csv_bulk.py --source {source} --clear-owed-only"
    lock = os.path.join(ROOT, "logs", "local_heavy.lock")
    if os.path.exists(lock):
        import time as _t
        age_h = (_t.time() - os.path.getmtime(lock)) / 3600.0
        print(f"NOT clearing full_rederive_owed for {source}: local_heavy.lock is present "
              f"({age_h:.1f}h old) — a heavy pass may be mid-run, and pulling state now "
              f"would wholesale-replace what it is writing (R340/R529). The debt stands. "
              f"After the pass finishes: {manual}", flush=True)
        return False
    if not _writers_quiet(manual):
        return False

    def _run(*args):
        p = subprocess.run([sys.executable, "-m", "updater.run", *args], cwd=_REPO,
                           capture_output=True, text=True, encoding="utf-8",
                           errors="replace", timeout=1800)
        for ln in (p.stdout or "").strip().splitlines()[-2:]:
            print("   ", ln, flush=True)
        return p.returncode

    if _run("--pull-state") != 0:
        print(f"NOT clearing full_rederive_owed for {source}: pull-state failed, and a "
              f"clear applied to a stale copy dies at the next pull. The debt stands. "
              f"Retry: {manual}", flush=True)
        return False
    from updater.state import StateStore
    st = StateStore()
    if not any(r["source_id"] == source for r in st.full_rederives_owed()):
        # Nothing to clear (fresh authoritative pull says so) — pushing an unchanged
        # 11 GB store would spend minutes and a CAS slot to delete nothing.
        print(f"no full_rederive_owed row for {source} in the authoritative store — "
              f"nothing to clear", flush=True)
        return True
    st.clear_full_rederive_owed(source)
    # Re-check the lock between the clear and the push (verifier's residual): a pass
    # STARTING in this window pulls state that still holds the row and would destroy
    # the clear while we print success. Seconds wide, but free to close.
    if os.path.exists(lock):
        print(f"full_rederive_owed cleared LOCALLY for {source} but a heavy pass "
              f"acquired the lock mid-envelope — NOT pushing over its run. The clear "
              f"will not survive its pull; after the pass: {manual}", flush=True)
        return False
    if not _writers_quiet(manual, window_min=CLEAR_WINDOW_MIN // 2):
        # The pull and clear took part of the window; the push needs the rest (R1102).
        print(f"full_rederive_owed cleared LOCALLY for {source} but NOT pushed: this clear will "
              f"not survive the next pull.", flush=True)
        return False
    if _run("--push-state") != 0:
        print(f"full_rederive_owed cleared LOCALLY for {source} but push-state failed "
              f"(a writer likely raced us) — this clear will NOT survive the next pull. "
              f"Re-run: {manual}", flush=True)
        return False
    print(f"full_rederive_owed cleared for {source} and pushed to the authoritative "
          f"store", flush=True)
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--skip-newer-than", default=None,
                    help="resume a changed-everything campaign: skip keys whose R2 "
                         "LastModified >= this ISO timestamp (the campaign's start). "
                         "--skip-existing would skip the stale objects being replaced.")
    ap.add_argument("--failed-keys-file", default=None,
                    help="append every failed PUT's key+reason here (default: "
                         "<source>_failed_puts.tsv beside the store)")
    ap.add_argument("--only-catalogued", action="store_true",
                    help="write ONLY series with a catalog.db row. Without this the stream "
                         "writes every series_key in the parquet — R364 measured 1,927 objects "
                         "for a 1,749-row catalogue on whr, and wid would mint 393,196 "
                         "uncatalogued objects (2,858,393 store keys vs 2,465,197 catalogued). "
                         "Cataloguing those is a D1-headroom decision, not a derive side effect.")
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--qualify-with-shard", action="store_true",
                    help="emit ids as <source>:<shard>:<series_key> — required for sources "
                         "whose catalogue ids carry the store file (fed_board, fhfa)")
    ap.add_argument("--verify", type=int, default=300,
                    help="byte-compare this many RANDOM series against the resolver first")
    ap.add_argument("--allow-stale-mirror", action="store_true",
                    help="override the R383/R530 mirror preflight - ONLY for sources "
                         "whose run_location is local (their local store IS the truth); "
                         "for a cloud source this rewrites served CSVs to a stale vintage")
    ap.add_argument("--clear-owed-only", action="store_true",
                    help="run ONLY the durable full_rederive_owed clear (pull->clear->push) "
                         "for --source and exit - for finishing a campaign whose stamp was "
                         "deferred by a live heavy pass, AFTER verifying the campaign")
    a = ap.parse_args()

    if a.clear_owed_only:
        # --dry-run must stay dry HERE TOO (verifier's finding, the R503 class: this
        # early-return ran before --dry-run was ever consulted, so rehearsing the
        # command performed a real remote delete of the debt marker).
        if a.dry_run:
            print(f"(dry run) would pull-state, clear full_rederive_owed for "
                  f"{a.source}, and push-state — nothing done")
            return 0
        return 0 if _durable_clear(a.source) else 1

    import duckdb
    # WALK, AND RESOLVE THE ROOT. A flat os.listdir under clean_full returns [] for two real
    # layouts, and "no parquet in ..." is indistinguishable from an empty store. usda keeps
    # data/clean_full/usda/<theme>/part_NNN.parquet (60 files) and bea
    # data/clean_full/bea/<Dataset>/<Table>.parquet (592); sec_edgar is not under clean_full at
    # all. The same assumption blinded the R383 preflight to its two nested sources (R390);
    # this was the copy left unfixed in that sweep, and it surfaced the moment usda needed the
    # bulk path — the per-series route was measured at 337 objects in 90 minutes, i.e. 314
    # hours for its 69,704 series.
    src_dir = None
    for _root in ("clean_full", "clean_grouped"):
        cand = os.path.join(ROOT, "data", _root, a.source)
        if os.path.isdir(cand) and any(f.endswith(".parquet")
                                       for _dp, _dn, fs in os.walk(cand) for f in fs):
            src_dir = cand
            break
    if src_dir is None:
        print(f"no parquet for {a.source} under data/clean_full or data/clean_grouped")
        return 2

    # THE STORE WAS RESOLVED BY SOURCE ID; THE REGISTRY MAY NAME A DIFFERENT ONE (R1050).
    # The loop above lands on data/<root>/<source_id>, which is right for 276 of the 278
    # registry entries. For the other two it is the WRONG PRODUCT, because sec_edgar's pair
    # have their directory names inverted:
    #
    #     source_id=sec_edgar_13f    out_dir=edgar_13f   (relational 13F/insider, nothing served; was `sec_edgar` until 2026-09-24)
    #     source_id=sec_edgar_xbrl   out_dir=sec_edgar   (the 17,467 served company-facts CSVs)
    #
    # So `--source sec_edgar` resolves clean_grouped/sec_edgar — the OTHER entry's store. On
    # 2026-09-17 a health-gate remedy line named exactly that command for exactly that source
    # and I was one step from running it; what stopped it was a review, not the tool. The tool
    # should say so itself, and say it BEFORE the mirror preflight, whose "MIRROR BEHIND R2"
    # reads as "just sync the mirror" and leads to --allow-stale-mirror on a source where that
    # is the R386 failure verbatim.
    #
    # Refuse rather than guess: only the operator knows which product was meant, and for this
    # id neither answer is a campaign the tool can actually run (the served files carry no
    # series_key column, so the uniform-long filter writes zero objects; the 13F store has no
    # catalogued series at all).
    # Since 2026-09-24 the 13F entry is `sec_edgar_13f`, so no registry entry is named `sec_edgar` and the
    # check below would find nothing and PROCEED on clean_grouped/sec_edgar. That store is the catalogue's
    # XBRL product, whose CSVs tools/refresh_sec_edgar.py derives; its files carry no series_key. Refuse.
    if a.source == "sec_edgar":
        print("REFUSING: --source sec_edgar is the XBRL company-facts product (registry entry "
              "sec_edgar_xbrl). Its CSVs are written by tools/refresh_sec_edgar.py, not by this tool "
              "(R275/R1050); the 13F product (sec_edgar_13f, store clean_full/edgar_13f) is wide tables with no series CSVs.")
        return 2
    try:
        import yaml                                                   # noqa: PLC0415
        from updater import config as _cfg                            # noqa: PLC0415
        _entries = yaml.safe_load(open(_cfg.REGISTRY, encoding="utf-8")) or []
        if isinstance(_entries, dict):
            _entries = _entries.get("sources") or list(_entries.values())
        _e = next((e for e in _entries if e.get("source_id") == a.source), None)
        _declared = (_e or {}).get("out_dir", a.source)
    except Exception as _e:                                           # noqa: BLE001
        # Cannot read the registry -> cannot prove the store is the right one. Say which,
        # and do not let an unreadable registry read like agreement (R338's direction).
        print(f"WARNING: could not check --source {a.source} against the registry's out_dir "
              f"({type(_e).__name__}: {str(_e)[:80]}) — proceeding on the id-resolved store "
              f"{src_dir}")
        _declared = a.source
    if _declared != a.source:
        print(f"REFUSING: --source {a.source} resolved the store {src_dir}, but the registry "
              f"entry named {a.source!r} writes {_declared!r}. Those are different products "
              f"sharing one id (R275/R1050), so this command would derive from a store the "
              f"named entry does not own. Decide which product you mean and address it by the "
              f"id whose out_dir IS that directory.")
        return 2

    # THE R383/R530 MIRROR PREFLIGHT — ported here the day its absence was re-committed.
    # This tool derives from the LOCAL tree; for a CLOUD source (run_location: cloud) R2
    # is the authoritative store and the local dir is a scratch mirror, so a campaign
    # from a behind-mirror rewrites every served CSV to a stale vintage while --verify
    # passes 300/300 (this-path vs resolver over the SAME stale files — R383's hollow
    # pass, produced live on norgesbank: local 3,768,215 rows/08-06 vs R2 3,805,628/
    # 08-28, 35,135 CSVs rewritten to the vintage they already had). core/derive_csv.py
    # gained this refusal after R383; the bulk tool — built precisely so big campaigns
    # stop using that path — never did. Content-compared (row count + max obs date,
    # never LastModified/md5), sample-scaled, and any read error refuses toward "cannot
    # prove it is safe".
    if not a.dry_run and not a.allow_stale_mirror:
        from core.derive_csv import _mirror_behind_store
        behind = _mirror_behind_store([a.source])
        if behind:
            for src, detail in behind:
                print(f"MIRROR BEHIND R2 : {detail}")
            print(f"REFUSING: the local mirror is behind the authoritative store — a "
                  f"campaign from it would rewrite served CSVs to a stale vintage while "
                  f"--verify passes against the same stale files (R383/R530). Sync "
                  f"{a.source}'s parquets from R2 first, or pass --allow-stale-mirror "
                  f"if local is genuinely authoritative for this source "
                  f"(run_location: local).")
            return 2
    paths = sorted(os.path.join(dp, f)
                   for dp, _dn, fs in os.walk(src_dir) for f in fs
                   if f.endswith(".parquet") and not f.endswith("__series.parquet"))

    # LICENCE EXCLUSIONS ARE STRUCTURAL, NOT A FLAG. tools/catalog_complete.py refuses to
    # catalogue files whose licence is not the directory's source licence, and until now this
    # tool did not know about that list. data/clean_full/vdem/ holds vparty.parquet beside
    # vdem.parquet: V-Dem publishes CC BY-SA 4.0 for "The V-Dem Dataset", V-Party is a separate
    # publication whose own page states no licence, and its 682,659 keys do not overlap vdem's
    # so the duplicate-shard guard below cannot fire either. Running this tool on vdem without
    # --only-catalogued would therefore mint 682,659 unlicensed CSVs onto R2 - R364 verbatim,
    # whose stated remedy is to separate them BEFORE any whole-directory tool runs. Honouring
    # the same list here makes the protection a property of the CODE rather than of remembering
    # a flag.
    # NO SILENT FALLBACK. This was `except Exception: SOURCE_FILE_EXCLUSIONS = {}`, which turns
    # a broken import into ZERO exclusions - a fallback that absorbs 100% of the protection it
    # guards, and the failure would be invisible (R419). An import error here means the licence
    # list is unavailable, and deriving a whole directory without it is exactly what publishes
    # V-Party. Refuse instead.
    try:
        from tools.catalog_complete import SOURCE_FILE_EXCLUSIONS
    except Exception as _e:                                  # noqa: BLE001
        print(f"REFUSING: cannot import the licence exclusion list ({_e}). A whole-directory "
              f"derive without it can publish files this source is not licensed to serve.")
        return 2
    _excl = set(SOURCE_FILE_EXCLUSIONS.get(a.source, ()))
    if _excl:
        _skipped = [p for p in paths if os.path.basename(p) in _excl]
        paths = [p for p in paths if os.path.basename(p) not in _excl]
        # Say what was dropped: a coverage limit nobody prints reads as full coverage.
        print(f"  {a.source}: EXCLUDING {len(_skipped)} file(s) whose licence is not this "
              f"source's: {[os.path.basename(p) for p in _skipped]} - see "
              f"DATABASE_LICENSES_VERBATIM.md")
        if not paths:
            print(f"  {a.source}: every parquet was excluded; nothing to derive.")
            return 2
    # wid ONLY: exclude the superseded legacy monolith when the per-country shards exist —
    # the same targeted skip the resolver applies (_resolve._resolve_generic_long). wid.parquet
    # (1.93M series to 2024) sits beside 412 shards (2.86M series to 2025); streaming both
    # emits duplicate dates with contradictory values, which is the corruption R384 nearly
    # published at 2.4M-object scale. NOT generalised: sources with a same-named file
    # beside shards include bea, stats_nz, vdem and wid, and only wid's is proven
    # superseded. The --verify gate would catch a divergence anyway, since the resolver now
    # excludes the monolith — this keeps the two readers defined identically rather than
    # relying on the gate to notice they are not.
    if a.source == "wid":
        mono = os.path.join(src_dir, "wid.parquet")
        rest = [f for f in paths if os.path.abspath(f) != os.path.abspath(mono)]
        if rest and len(rest) != len(paths):
            paths = rest
    if not paths:
        print(f"no parquet in {src_dir}")
        return 2
    print(f"source files: {len(paths)} parquet(s) under "
          f"{os.path.relpath(src_dir, ROOT).replace(os.sep, '/')}", flush=True)

    # ONLY UNIFORM-LONG FILES ARE THE SERVING STORE. A store dir may hold native/raw parquets
    # BESIDE the tidy projection that serves (cepii_baci: baci_hs17/hs96.parquet are the raw
    # vintages with year/exporter/importer columns; cepii_baci_pairs.parquet is what serves).
    # Feeding a raw file into the DISTINCT-series_key scan is a BinderException at best and a
    # wrong id universe at worst. Skip by SCHEMA, and say so — a silent skip would read as
    # "covered everything" (the no-silent-caps rule).
    import duckdb as _duck
    _need = {"series_key", "obs_date", "value"}
    uniform, skipped = [], []
    for p in paths:
        cols = {r[0] for r in _duck.connect().execute(
            "SELECT name FROM parquet_schema(?)", [p]).fetchall()}
        (uniform if _need <= cols else skipped).append(p)
    if skipped:
        print(f"skipping {len(skipped)} non-uniform parquet(s) (missing "
              f"{sorted(_need)} columns) — native/raw files are not the serving store: "
              + ", ".join(os.path.basename(s) for s in skipped), flush=True)
    if not uniform:
        print(f"no uniform-long parquet (series_key/obs_date/value) in {src_dir} — "
              f"nothing here can serve")
        return 2
    paths = uniform
    # `path` is what the VERIFY step and the distinct-key count read. DuckDB's read_parquet
    # takes a list as happily as a string, so a sharded source is verified across all of its
    # shards rather than only the first - sampling one shard of 417 would certify a format
    # that the other 416 might not share.
    path = paths if len(paths) > 1 else paths[0]
    if len(paths) > 1:
        print(f"{len(paths)} shards in {src_dir}", flush=True)
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    # ---- byte-exactness gate -------------------------------------------------------
    if a.verify:
        from core.derive_csv import _series_csv_bytes
        if a.qualify_with_shard:
            # The id carries the shard, so a bare DISTINCT over the whole source cannot build
            # one: the same key may exist in two shards as two different series. Sample per
            # shard instead, which also spreads the check along the axis where a schema
            # divergence would actually appear.
            per = max(1, -(-a.verify // len(paths)))
            keys, grouped = [], {}
            for pth in paths:
                shard = os.path.splitext(os.path.basename(pth))[0]
                got = 0
                for k, rows in _stream_one(con, pth):
                    qk = f"{shard}:{k}"
                    keys.append(qk)
                    grouped[qk] = rows
                    got += 1
                    if got >= per:
                        break
            sample = keys
            print(f"verify sample: {len(sample):,} series, up to {per} from each of "
                  f"{len(paths)} shard(s)", flush=True)
        else:
            keys = [r[0] for r in con.execute(
                "SELECT DISTINCT series_key FROM read_parquet(?) ORDER BY series_key",
                [path]).fetchall()]
            print(f"{len(keys):,} distinct series in the parquet", flush=True)
            rnd = random.Random(20260730)
            sample = rnd.sample(keys, min(a.verify, len(keys)))
        # ONE scan for the whole sample. A per-key query would re-scan all 69.6M rows each
        # time — 300 full passes to check 300 series, which is the very cost this tool exists
        # to remove, reintroduced inside its own test.
        want = set(sample)
        if not a.qualify_with_shard:
            grouped = {k: [] for k in sample}
        cur = None if a.qualify_with_shard else con.execute(
            "SELECT series_key, obs_date, value FROM read_parquet(?) "
            "WHERE series_key IN (SELECT UNNEST(?)) ORDER BY series_key, obs_date",
            [path, sample])
        while cur is not None:
            batch = cur.fetchmany(50_000)
            if not batch:
                break
            for k, d, v in batch:
                if k in want:
                    grouped[k].append((d, v))
        bad = 0
        for k in sample:
            mine = _csv_bytes(k, grouped[k])
            theirs = _series_csv_bytes(f"{a.source}:{k}")
            if mine != theirs:
                bad += 1
                if bad <= 3:
                    print(f"  MISMATCH {k}\n    mine  : {mine[:120]!r}\n"
                          f"    theirs: {theirs[:120]!r}")
        print(f"verify: {len(sample) - bad}/{len(sample)} byte-identical", flush=True)
        if bad:
            print("REFUSING to run — output would not match the served contract.")
            return 1

    existing = set()
    store = None
    if not a.dry_run:
        if not a.bucket:
            ap.error("--bucket is required unless --dry-run")
        # THE BLOB STORE, not a bare R2 client (plan step 1): R2 before T0, the self-hosted store after it
        # (AQUEDUCT_BACKEND=selfhost), the same keys and bytes either way; after T0 an R2 client is refused.
        from updater import blob as _blob                                 # noqa: PLC0415
        store = _blob.csv_store(a.bucket)             # R2, or the self-hosted store after T0 - never local files
        if a.skip_existing or a.skip_newer_than:
            lp = csv_key_prefix(a.prefix, a.source)
            import datetime as _dt
            cutoff = None
            if a.skip_newer_than:
                cutoff = _dt.datetime.fromisoformat(a.skip_newer_than.replace("Z", "+00:00"))
                if cutoff.tzinfo is None:
                    cutoff = cutoff.replace(tzinfo=_dt.timezone.utc)
            for key, modified in _retry(lambda: store.list_modified(lp), "LIST"):
                if cutoff is not None:
                    # RESUME semantics (ported from core/derive_csv.py's
                    # --skip-newer-than, built after a noaa re-derive died in the
                    # 2026-08-03 reboot): skip only what THIS campaign already wrote.
                    # --skip-existing is wrong for a changed-everything run — it would
                    # skip exactly the stale objects being replaced.
                    if modified >= cutoff:
                        existing.add(key)
                else:
                    existing.add(key)
            mode = ("newer than %s" % a.skip_newer_than) if cutoff else "already in the store"
            print(f"skip: {len(existing):,} {mode}", flush=True)

    # ---- producer (single sorted scan) -> bounded queue -> PUT workers --------------
    q: queue.Queue = queue.Queue(maxsize=4000)
    counts = {"put": 0, "skip": 0, "err": 0}
    lock = threading.Lock()
    STOP = object()
    failed_log = None
    if not a.dry_run:
        _flog = a.failed_keys_file or os.path.join(
            ROOT, "data", "%s_failed_puts.tsv" % a.source)
        failed_log = open(_flog, "a", encoding="utf-8")
        print(f"failed PUTs will be recorded in {_flog}", flush=True)

    def worker():
        while True:
            item = q.get()
            if item is STOP:
                q.task_done()
                return
            key, body = item
            try:
                # BORN GZIPPED, per the 2026-08-18 fleet policy (copied from
                # blob.put_atomic's branch — updater/blob.py:449-457): mtime=0 keeps the
                # bytes deterministic, ContentEncoding is the marker the worker's reader
                # decompresses on. The 2026-08-31 noaa-derive review flagged that this tool
                # still PUT plain — a 3.1M-object rewrite is the one free chance to comply,
                # and rewriting plain would re-entrench the exception. The --verify gate
                # compares PRE-compression bytes, so it is unaffected.
                # Since plan step 1 through the blob store: put_atomic gzips the PLAIN body itself
                # (series_csv_put_args - ContentEncoding gzip, mtime=0) and records the CSV's own md5,
                # which this private put never did, so the skip-identical rule can recognise it later.
                _retry(lambda: store.put_atomic(key, body), "PUT")
                with lock:
                    counts["put"] += 1
                    if counts["put"] % 25_000 == 0:
                        print(f"  put {counts['put']:,} (skip {counts['skip']:,})", flush=True)
            except Exception as e:                           # noqa: BLE001
                with lock:
                    counts["err"] += 1
                    if counts["err"] <= 5:
                        print(f"  PUT FAILED {key}: {str(e)[:90]}", flush=True)
                    # EVERY failed key lands in the file, not just the first five as
                    # console lines: at 3.1M PUTs a 0.01% failure rate is ~300 silently
                    # stale objects nobody can enumerate afterwards (review change).
                    if failed_log is not None:
                        failed_log.write("%s\t%s\n" % (key, str(e)[:160]))
                        failed_log.flush()
            finally:
                q.task_done()

    threads = []
    if not a.dry_run:
        for _ in range(a.workers):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)

    catalogued = None
    if a.only_catalogued:
        import sqlite3
        _con = sqlite3.connect(
            f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro", uri=True)
        _con.execute("PRAGMA busy_timeout = 180000")
        catalogued = {r[0] for r in _con.execute(
            "select series_id from series where source_id=?", (a.source,))}
        _con.close()
        print(f"only-catalogued: {len(catalogued):,} id(s) eligible; store keys outside the "
              f"catalogue are counted and skipped, not silently dropped", flush=True)

    seen = 0
    uncat = 0
    for k, rows in _stream(con, paths, qualify=a.qualify_with_shard):
        seen += 1
        if catalogued is not None and f"{a.source}:{k}" not in catalogued:
            uncat += 1
            continue
        key = csv_key(a.prefix, a.source, k)
        if key in existing:
            counts["skip"] += 1
            continue
        if a.dry_run:
            if seen <= 3:
                print(f"  would PUT {key} ({len(_csv_bytes(k, rows))} B)")
            continue
        q.put((key, _csv_bytes(k, rows)))

    if not a.dry_run:
        q.join()
        for _ in threads:
            q.put(STOP)
        q.join()
    if uncat:
        print(f"only-catalogued: SKIPPED {uncat:,} store series with no catalogue row "
              f"(they are real data — cataloguing them is a separate decision)", flush=True)
    print(f"done: {seen:,} series streamed, put {counts['put']:,}, "
          f"skipped {counts['skip']:,}, errors {counts['err']:,}")
    if failed_log is not None:
        failed_log.close()
    # CLEAR THE FULL-REDERIVE DEBT — only on a campaign that PROVABLY TOUCHED the stale
    # population (R529, both conditions from the review that FAILED the first cut):
    #   * zero errors AND put > 0 — a stream that rewrote nothing certifies nothing;
    #   * never under --skip-existing — that flag skips every exists-but-stale object,
    #     which is precisely the population the debt names, so err==0 is vacuous there
    #     (the first cut stamped "complete" on a campaign that wrote zero bytes);
    # and the clear goes through pull→clear→push (_durable_clear), because a clear
    # written to the local file alone dies at the next wholesale pull (R340).
    # A partial campaign (errors, a kill) leaves the debt standing: the honest state.
    # A --skip-newer-than cutoff EARLIER than the debt was noted means the campaign
    # skipped objects written before the debt existed — i.e. the stale population —
    # while still PUTting some genuinely-missing keys, so put>0 alone would stamp a
    # debt the run never repaid (verifier's residual). Refuse toward the debt
    # standing; the local noted_utc may be stale, but a wrong refusal only defers
    # the stamp to --clear-owed-only after verification.
    _cutoff_predates_debt = False
    if not a.dry_run and a.skip_newer_than:
        try:
            from updater.state import StateStore as _SS
            _owe = {r["source_id"]: r for r in _SS().full_rederives_owed()}.get(a.source)
            if _owe and str(_owe.get("noted_utc") or "") > a.skip_newer_than:
                _cutoff_predates_debt = True
        except Exception:                                    # noqa: BLE001
            _cutoff_predates_debt = True   # unreadable state: refuse toward standing
    if (not a.dry_run and counts["err"] == 0 and counts["put"] > 0
            and not a.skip_existing and not _cutoff_predates_debt):
        try:
            _durable_clear(a.source)
        except Exception as e:                               # noqa: BLE001
            print(f"WARNING: durable clear of full_rederive_owed for {a.source} raised "
                  f"{e!r} — the debt stands; after verifying the campaign, run "
                  f"tools/derive_csv_bulk.py --source {a.source} --clear-owed-only",
                  flush=True)
    elif not a.dry_run:
        why = ("errors > 0" if counts["err"] else
               "--skip-existing skips the stale objects a debt names" if a.skip_existing
               else "the --skip-newer-than cutoff predates the debt — the stale "
                    "population was skipped" if _cutoff_predates_debt
               else "0 PUTs — nothing was rewritten")
        print(f"full_rederive_owed NOT cleared ({why}); if a debt row exists it stands, "
              f"which is the honest state", flush=True)
    return 1 if counts["err"] else 0


if __name__ == "__main__":
    sys.exit(main())
