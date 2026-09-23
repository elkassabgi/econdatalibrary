"""S5 bulk fetcher — KSH (Hungarian Central Statistical Office) STADAT tables. No key.

~1,642 STADAT tables fan IN to one parquet per THEME (theme = table_id[:3]); 26 files on R2,
schema (series_key, obs_date, value). series_key = "KSH:{table_id}:{row_label}:{col_label}",
built by jobs.ingest_ksh_stadat.make_key via parse_table — imported here so keys match disk
byte-for-byte (duplication invariant).

Manifest = https://www.ksh.hu/stadat_files/toc.json: every table carries `updatedAt` and
`correctedAt` (a real per-table vintage; verified 1,642/1,642 populated, max updatedAt = today).
The CSVs expose NO Last-Modified/ETag, so an rba-style conditional GET is impossible — the
toc.json vintage IS the gate. Token = "{updatedAt}|{correctedAt}"; correctedAt catches silent
revisions that updatedAt misses (same rationale as faostat's DateUpdate|rows|size triple).

Because many tables share one theme parquet, changed tables are grouped BY THEME and each theme
is merged once (dedup + never-shrink), and a theme's tables only have their vintages advanced
after that theme's merge succeeds — so a failed merge can never strand a table as "done".

Store I/O via blob (R36); sidecar on the store. Downloads run across a small pool (R40).

HONEST-STATUS: toc.json failure -> TransientError. Per-table fetch failure / WAF page -> transient_unit.
A table that parses to zero rows -> empty_unit (KSH has genuinely empty tables; its vintage still
advances so we don't refetch it every tick — a content change moves the token and we re-examine).
Cursors emitted for merged series (R41).
"""
from __future__ import annotations
import datetime as dt
import hashlib
import json
import os
import threading
import time
from collections import defaultdict
from itertools import zip_longest
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import NOT_HOSTED_NOTE, ROTATION_NOTE, Result
from ._common import Deadline, Tally, finalize
from ._common import cancellable_pool
from jobs import ingest_ksh_stadat as ig   # reuse catalog + THE table parser / key builder

SOURCE = "ksh_stadat"
DEDUP = ("series_key", "obs_date")
SIDECAR = "_bulk_vintages.json"       # {table_id: "updatedAt|correctedAt"}
# www.ksh.hu is SLOW and refuses load: run 30136135069 spent 32 minutes on connect-timeouts
# (60s each) against /stadat_files/*/en/*.csv at 5 workers x 400 tables and never finished.
# Keep concurrency low. (R40b)
#
# THE BUDGET BOUNDS A PASS, NOT THE CAP (review R1121). That run had no wave budget; now the waves
# stop at KSH_BUDGET_MIN and book the rest deferred, so the cap only has to be large enough never
# to be the binding limit. At 60 it was: toc.json showed 413 of 1,642 tables updated within 30 days
# (~96 a week) against one pass per ~7 days, so 60 a pass could never catch up, and 849 tables were
# owed. Measured 2026-09-23 from the desktop with the cap lifted to 400 and NO pacing: the WAF let
# ~56 back-to-back requests through, then blocked for ~18 min (rejections at queue positions 56/57,
# 28 s in, and 112/113, 18.9 min in), so the 30-min budget reached 120 tables in two bursts
# (2,223 s). Requests are now paced (_pace).
MAX_WORKERS = 2
STOP_GRACE_MIN = 5   # budget + this = the last moment a request or back-off sleep may start (update())
PACE_S = ig.RATE     # seconds between request STARTS across workers - the job's own pace (_pace)
MERGE_MARGIN_MIN = 5  # after the stop time: theme merges + saves. Measured R1127: 28 theme GETs 5.7 s,
                      # the largest merge (mun.parquet) 0.25 s; PUT time not measured - generous on purpose
OWED_ATTENTION_DAYS = 45   # a table owed longer than this (first missed release) turns ROTATING -> ATTENTION
MAX_PER_RUN = int(os.environ.get("KSH_MAX_PER_RUN", "400"))
# Tables submitted per deadline check. The pool is given a whole wave at once, so
# the wave size — not the loop — is what actually bounds the fetch.
TABLE_WAVE = int(os.environ.get("KSH_TABLE_WAVE", "10"))


def _vintage(entry) -> str:
    return f"{entry.get('updatedAt', '')}|{entry.get('correctedAt', '')}"


def _table_id(entry):
    for k in ("id", "tableId", "table_id", "code"):
        v = entry.get(k)
        if v:
            return str(v)
    return None


def _catalog(raise_transient: bool):
    try:
        cat = ig.load_catalog()
    except Exception as e:
        if raise_transient:
            raise TransientError(f"ksh_stadat: toc.json fetch failed: {e}")
        return None
    if not cat:
        if raise_transient:
            raise TransientError("ksh_stadat: toc.json returned no tables")
        return None
    return cat


def current_vintage(unit) -> str | None:
    cat = _catalog(raise_transient=False)
    if not cat:
        return None
    h = hashlib.sha256()
    for e in sorted(cat, key=lambda x: str(_table_id(x) or "")):
        tid = _table_id(e)
        if tid:
            h.update(f"{tid}={_vintage(e)};".encode())
    return f"ksh:{h.hexdigest()[:16]}"


def _load_sidecar(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save_sidecar(out_dir, data) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, sort_keys=True).encode("utf-8"))


def _holds_table(out_dir, tid):
    """Does the store hold any row of table `tid` (keys 'KSH:<tid>:...')? Reads the theme parquet AND
    every '_'-prefixed side file: five served tables live only in _migrated_from_ksh_unparsed.parquet
    (review R1118). True / False when READ, None when any file could not be (never read as False)."""
    import pyarrow.compute as _pc
    try:
        names = [n for n in blob.list_parquets(out_dir)
                 if n == f"{tid[:3].lower()}.parquet" or n.startswith("_")]
        for n in names:
            keys = blob.read_table(os.path.join(out_dir, n), columns=["series_key"]).column("series_key")
            if _pc.any(_pc.starts_with(keys, f"KSH:{tid}:")).as_py():
                return True
        return False
    except Exception:                                        # noqa: BLE001
        return None


NODATA = "_no_data_tables.json"   # {tid: vintage} - tables with nothing to store at that vintage


def _load_nodata(out_dir) -> dict:
    """Tables whose last fetch had NOTHING TO STORE (parsed empty, or a link-only 404) at a vintage.
    Kept apart from the vintage sidecar (tools/clear_ksh_mojibake_vintages.py reads that one as
    {tid: vintage}). Without it such a table re-entered the queue on every pass - the todo check also
    requires the theme parquet, which an all-empty theme never gets (review R1118: ido0001..0016
    took 16 of the 60 slots on every pass)."""
    try:
        raw = blob.read_bytes(os.path.join(out_dir, NODATA))
        d = json.loads(raw.decode("utf-8")) if raw else {}
        return d if isinstance(d, dict) else {}
    except Exception:                                        # noqa: BLE001
        return {}


def _when(s):
    """An ISO timestamp from toc.json ('2026-09-10T00:00:00Z' / '+00:00') as an aware datetime, or None."""
    try:
        d = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)


def _first_missed(entry, stored_updated, now):
    """The FIRST KSH release of this table after the copy we store - how long a reader has been denied
    a newer one (review R1127). From toc.json's own calendar (`updateDates`, past and future) plus its
    `updatedAt`; only releases already out (<= now) count. None when none is found."""
    stored = _when(stored_updated)
    out = [d for d in (_when(x) for x in list(entry.get("updateDates") or []) + [entry.get("updatedAt")])
           if d is not None and d <= now and (stored is None or d > stored)]
    return min(out) if out else None


_PACE_LOCK = threading.Lock()
_LAST_START = [0.0]


def _pace():
    """At most one request START per PACE_S across the worker threads (review R1123). The fetcher
    sent requests back to back: KSH's WAF let ~56 through, then blocked for ~18 min (dry run
    2026-09-23: rejections at queue positions 56/57 and 112/113). The job's own main() waits
    ig.RATE (1.2 s) between requests, and CI run 35022271104 fetched 60 tables in ~4.5 min with no
    WAF line.

    MEASURED, NOT A CURE: paced, the desktop still met the WAF ~64 s in (ksh_dryrun2, 2026-09-23:
    110 tables in a 25-min pass, 16 back-off lines). The allowance looks COUNT-based (~55 requests,
    then an ~18-min block), so pacing buys little; it stays because it is the job's own pace and
    costs ~2 min a pass. Throughput is set by the WAF, and the backlog needs more passes, not a
    faster one."""
    with _PACE_LOCK:
        wait = _LAST_START[0] + PACE_S - time.time()
        if wait > 0:
            time.sleep(wait)
        _LAST_START[0] = time.time()


def _fetch_table(tid):
    """Thread task -> (tid, rows|None). None marks a transport/WAF failure (transient)."""
    theme = tid[:3].lower()
    url = f"{ig.BASE}/{theme}/en/{tid}.csv"
    _pace()
    try:
        raw = ig.get_bytes(url)          # returns None on WAF page / failure / 404
    except Exception:
        return tid, None
    if not raw:
        # A 404 is not the WAF: the table has no CSV at this URL (gdp0049, 2026-09-23: listed in
        # toc.json as "Financial accounts (available at the related links)", a link-only stub).
        st = getattr(ig, "LAST_STATUS", {}).get(url)
        return tid, ("absent" if st == 404 else "deadline" if st == "deadline" else None)
    try:
        # DECODE EXACTLY AS THE INGESTER DOES (ingest_ksh_stadat.py:664-666): strict
        # utf-8-sig first, cp1250 fallback. This line used to read
        # `decode("utf-8", errors="replace")` — R333's two-parsers class verbatim:
        # KSH serves some tables cp1250-encoded, `replace` minted U+FFFD into the
        # series keys, and 1,931 catalogued series (whose CLEAN accents came from the
        # ingester's correct decode) became unservable — the resolver's exact-equality
        # match can never hit a mojibake-only store (WU-3 of the 2026-08-31 grain
        # sweep, the one unit where users receive EMPTY data).
        try:
            txt = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            txt = raw.decode("cp1250", errors="replace")
        rows, _err = ig.parse_table(tid, txt)
    except Exception:
        return tid, None
    return tid, rows or []


def _budget_min() -> float:
    """Minutes this pass may keep starting waves (review R1127 (5)). The orchestrator runs a unit under
    SIGALRM (setitimer ITIMER_REAL, armed around the unit), and the gate can admit ksh with a window
    as small as ~13 min on its cost estimate - smaller than KSH_BUDGET_MIN + STOP_GRACE_MIN - so a
    fixed budget could be killed with nothing merged. Size it from the time the alarm actually leaves,
    minus the grace and a margin for the merges and saves, capped at KSH_BUDGET_MIN (25). 0 or less
    means start nothing (Deadline(0) is spent at once). No alarm (a desktop run) -> the cap."""
    cap = float(os.environ.get("KSH_BUDGET_MIN", "25"))
    try:
        import signal
        left_s = signal.getitimer(signal.ITIMER_REAL)[0]
    except (AttributeError, ValueError, OSError):
        left_s = 0.0
    if left_s > 0:
        return max(0.0, min(cap, left_s / 60.0 - STOP_GRACE_MIN - MERGE_MARGIN_MIN))
    return cap


def update(unit, since) -> Result:
    # THE CLOCK STARTS HERE (review R1123). The budget and the stop time used to start after the todo
    # scan, which sent one R2 HEAD per table (809 HEADs = 167.7 s from the desktop), so on the
    # orchestrator's clock a pass was ~40 min old before its first merge. Both now count from entry.
    budget_min = _budget_min()
    dl = Deadline(minutes=budget_min)
    budget_min = dl.budget_min          # the EFFECTIVE budget (a desktop override replaces it)
    # NO NEW REQUEST AND NO BACK-OFF SLEEP PAST budget + STOP_GRACE_MIN: one URL's WAF ladder is 33 min,
    # and every fetched table merges only after the last wave - a kill at 45 min would lose them all.
    # 25 + 5 = 30 min leaves the merges and the saves ~14 min under the 45-min kill.
    ig.STOP_AT[0] = time.time() + (budget_min + STOP_GRACE_MIN) * 60
    try:
        return _update(dl, budget_min, since)
    finally:
        ig.STOP_AT[0] = None             # the ingester's own main() keeps the full ladder


def _update(dl, budget_min, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    cat = _catalog(raise_transient=True)
    sidecar = _load_sidecar(out_dir)
    nodata = _load_nodata(out_dir)
    nodata_before = dict(nodata)
    stubs: list[str] = []

    present = set(blob.list_parquets(out_dir))      # ONE listing, not one HEAD per table (R1123)
    todo = []
    missed: dict = {}                  # owed table -> its FIRST missed release (datetime)
    now = dt.datetime.now(dt.timezone.utc)
    for e in cat:
        tid = _table_id(e)
        if not tid:
            continue
        cur_v = _vintage(e)
        if sidecar.get(tid) == cur_v and (f"{tid[:3].lower()}.parquet" in present
                                          or nodata.get(tid) == cur_v):
            continue
        todo.append((tid, cur_v))
        if tid in sidecar:
            missed[tid] = _first_missed(e, str(sidecar[tid]).split("|")[0], now)
    # OWED AND NEVER-FETCHED TAKE TURNS (reviews R1121, R1123). Sorted by id, the first 60 owed tables
    # were always in themes a..k and 802 tables in kor..tur were never fetched by the updater (166 with
    # nothing stored). Never-fetched-first fixed that and froze the other side: simulated on KSH's 2026
    # release calendar at 120 tables a pass, 0 of the 840 maintained tables refreshed in the first six
    # passes, and the headline series (price indices, industrial production, external trade) went
    # 99 days owed. So they take turns 1:1, owed first. The owed side goes by FIRST MISSED RELEASE
    # (review R1127): the stored updatedAt said how old OUR copy is, not how long a reader has been
    # denied KSH's newer one - simulated on KSH's 2026 calendar at the measured ~110 tables a pass, the
    # headline tables' worst wait was 49 days by stored updatedAt and 23 by first missed release.
    # Measured capacity is the desktop's (ksh_dryrun2); a GitHub runner's is not measured.
    owed = sorted((tv for tv in todo if tv[0] in sidecar),
                  key=lambda tv: (missed.get(tv[0]) or now, tv[0]))
    never = sorted(tv for tv in todo if tv[0] not in sidecar)
    todo = [tv for pair in zip_longest(owed, never) for tv in pair if tv is not None]

    tally = Tally()
    capped = len(todo) > MAX_PER_RUN
    batch = todo[:MAX_PER_RUN]
    # OWED, NOT SILENT (review R1118): tables past the per-run cap are booked deferred, so a pass
    # that reached part of the owed tables reads partial (deferral-only: ROTATING), never ok.
    for tid, _v in todo[MAX_PER_RUN:]:
        tally.deferred_unit(f"{tid} (per-run cap {MAX_PER_RUN})")

    # fetch+parse concurrently, accumulating rows per THEME (many tables -> one parquet)
    by_theme = defaultdict(list)           # theme -> [(key, date, val), ...]
    theme_tables = defaultdict(list)       # theme -> [(tid, vintage), ...] pending vintage bump
    # FETCH IN WAVES UNDER A SELF-IMPOSED BUDGET, so whatever came back is merged and its vintages
    # recorded - which is what lets a backlog drain instead of resetting.
    fetched = 0
    answered = 0                           # tables KSH actually answered (data, empty or 404)
    waf_cut = 0                            # tables cut at the stop time inside a WAF/throttle back-off
    if batch:
        with cancellable_pool(MAX_WORKERS) as ex:
            for wave_start in range(0, len(batch), TABLE_WAVE):
                if dl.spent():
                    print(f"[{SOURCE}] budget of {budget_min:.0f} min spent after "
                          f"{dl.elapsed_min():.1f} min — {fetched}/{len(batch)} tables "
                          f"fetched, {len(batch) - fetched} left for the next tick (their "
                          f"vintages stay unbumped, so they are retried)", flush=True)
                    for tid, _v in batch[wave_start:]:
                        tally.deferred_unit(f"{tid} (budget {budget_min:.0f} min)")   # R1118
                    break
                wave = batch[wave_start:wave_start + TABLE_WAVE]
                futs = {ex.submit(_fetch_table, tid): (tid, v) for tid, v in wave}
                for fut in as_completed(futs):
                    tid, cur_v = futs[fut]
                    _t, rows = fut.result()
                    if rows == "deadline":
                        # Every stop-time cut follows a WAF page, a throttle status or an error
                        # (get_bytes only sleeps after one), so it is NAMED as the WAF's, not the
                        # budget's (review R1123).
                        waf_cut += 1
                        tally.deferred_unit(f"{tid} (WAF/throttle back-off cut at the stop time)")
                        continue
                    if rows == "absent":
                        answered += 1
                        held = _holds_table(out_dir, tid)
                        if held is False:
                            # NEVER STORED and no CSV: a link-only table (gdp0049). Its vintage is
                            # recorded so it is fetched again only when KSH changes it; not tallied -
                            # it is neither a failure nor data. It had been booked a WAF failure on
                            # every run, keeping ksh_stadat partial (daily run 35783253243).
                            print(f"[{SOURCE}] {tid}: no CSV (HTTP 404) and nothing of it stored - a "
                                  f"link-only table; skipped until KSH updates it", flush=True)
                            sidecar[tid] = cur_v
                            nodata[tid] = cur_v
                            stubs.append(tid)
                            continue
                        # we SERVE it (or cannot tell): its CSV disappearing is a real break
                        tally.structural_unit(f"{tid}: CSV now answers HTTP 404"
                                              + (" although we store it" if held else
                                                 " (could not read the store to tell if we hold it)"))
                        continue
                    if rows is None:
                        # NAMED. `_fetch_table` returns None for a transport or WAF failure;
                        # the table id was in scope all along and never passed, so five weeks
                        # of "1/60 transient-failed" never said WHICH of the 60 (R669).
                        tally.transient_unit(f"{tid}: transport/WAF failure fetching the table")
                        continue
                    answered += 1
                    theme = tid[:3].lower()
                    if not rows:
                        # genuinely empty table: advance its vintage so we don't refetch every tick
                        tally.empty_unit()
                        sidecar[tid] = cur_v
                        nodata[tid] = cur_v          # its theme file may never exist (ido*, R1118)
                        continue
                    by_theme[theme].extend(rows)
                    theme_tables[theme].append((tid, cur_v))
                    tally.added_unit(len(rows))
                fetched += len(wave)
    if waf_cut and answered < TABLE_WAVE:
        # A pass the WAF stopped before one wave's worth of answers is not a rotation making
        # progress: keep it in ATTENTION rather than ROTATING (review R1123).
        tally.transient_unit(f"KSH's WAF blocked this pass: {answered} table(s) answered, "
                             f"{waf_cut} cut at the stop time")

    cursors: dict[str, str] = {}
    maxd = None
    published = 0
    for theme, rows in by_theme.items():
        keys = [r[0] for r in rows]
        dates = [r[1] for r in rows]
        vals = [r[2] for r in rows]
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        path = os.path.join(out_dir, f"{theme}.parquet")
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError as e:
            # isolate to this theme; its tables keep their OLD vintages so they retry next tick.
            # The exception carries the reason the merge refused and used to be discarded.
            tally.transient_unit(f"{theme}: merge refused — {str(e)[:160]}")
            continue
        published += n
        for k, d in zip(keys, dates):
            iso = d.isoformat()
            if k not in cursors or iso > cursors[k]:
                cursors[k] = iso
        if md and (maxd is None or str(md) > str(maxd)):
            maxd = md
        for tid, cur_v in theme_tables[theme]:
            sidecar[tid] = cur_v            # advance ONLY after the theme merged cleanly
        # SAVED AFTER EVERY THEME (review R1123): a kill during the merges used to lose the whole
        # pass's vintages, and the next pass re-walked the same tables (the R190 stall).
        _save_sidecar(out_dir, sidecar)

    _save_sidecar(out_dir, sidecar)
    if nodata != nodata_before:
        blob.write_bytes_atomic(os.path.join(out_dir, NODATA),
                                json.dumps(nodata, sort_keys=True).encode("utf-8"))

    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))

    # HOW FAR BEHIND THE ROTATION IS, measured and bounded (review R1127): ROTATING could not tell a
    # draining backlog from one that never drains (a WAF that allows one burst a pass answers ~55
    # tables, and headline tables then wait 155-190 days, every pass still ROTATING). The table owed
    # longest, by its first missed release, is named in the rotation note; past OWED_ATTENTION_DAYS it
    # is booked as a failure so the source reads ATTENTION.
    still = [(missed[tid], tid) for tid, cur_v in todo
             if missed.get(tid) and sidecar.get(tid) != cur_v]
    oldest = min(still) if still else None
    owed_days = (now - oldest[0]).days if oldest else 0
    if oldest and owed_days > OWED_ATTENTION_DAYS:
        tally.transient_unit(f"rotation behind: {oldest[1]} has waited {owed_days} days for KSH's "
                             f"{oldest[0].date()} release (limit {OWED_ATTENTION_DAYS})")

    res = finalize(tally, published, maxd or (since or None), source=SOURCE,
                   series_cursors=cursors)
    if capped:
        res.new_vintage = None
    if stubs:
        # Named in the result on EVERY pass that found one, deferral passes included (review R1121:
        # while the backlog stands every pass is capped, so an ok-only note was never written). It
        # goes in last_error and the runs table; the digest prints no error for ok rows. Health reads
        # a partial pass as a pure deferral only when its error is the deferral note, so this tail
        # carries the NOT_HOSTED_NOTE prefix that health strips like a csv coverage note (R1116).
        res.error = (f"{res.error}; " if res.error else "") + (
            f"{NOT_HOSTED_NOTE} {len(stubs)} table(s) - no CSV at KSH (HTTP 404) and nothing stored "
            f"[{', '.join(stubs[:5])}]")
    if len(todo) > answered:
        # WHERE THE ROTATION STANDS, on every pass that leaves work owed (review R1123 (c)). A
        # stripped tail like the not-hosted note, so a pure deferral pass stays ROTATING.
        # No "; " inside: health drops a note SEGMENT by its prefix (R1127).
        never_left = sum(1 for tid, _v in todo if tid not in sidecar)
        res.error = (f"{res.error}; " if res.error else "") + (
            f"{ROTATION_NOTE} {len(todo)} table(s) were owed at the start of this pass, "
            f"{answered} answered, {never_left} never fetched by the updater remain"
            + (f", longest wait {owed_days} days ({oldest[1]}, KSH release {oldest[0].date()})"
               if oldest else ""))
    return res
