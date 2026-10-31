"""Shared machinery for the UNCTADstat successor family (#70) — one implementation,
thin per-source modules.

The publisher's own documented data API (contract proven live 2026-08-07): vintage is
KEYLESS — `GET /api/reportMetadata/{DS}` returns {version, lastUpdated}, the
publisher's own statement of the current release — and observations are keyed
(ClientId/ClientSecret via CI secrets or .env) through
`POST https://unctadstat-user-api.unctad.org/{DS}/cur/Facts`.

The metadata address has NO language segment: `.../reportMetadata/{DS}/en` answers 404 since
2026-10-29 (every unctad probe failed in updater-daily 41054519780). The address is built in ONE
place, jobs/ingest_unctad_ds.report_metadata_url; tests/test_unctad_metadata_url.py pins it.

Whole-dataset snapshot: re-pull ONLY when the vintage token moves, MERGE with dedup
(series_key, obs_date) so a revision updates in place and never-shrink holds. ALL parse
and key logic lives in jobs/ingest_unctad_ds.py and is called, never copied — the
insee_bdm/eurostat parity lesson (R-ledger 2026-08-08): a fetcher that re-types its
ingester's logic drifts silently.

HONEST-STATUS: metadata endpoint unreachable -> transient (no vintage claim). Facts
unreachable/HTTP-4xx -> transient_unit, data kept, retried by a later pass. A 200 that parses
ZERO rows, or a layout the generic job refuses -> structural_unit and the vintage is NOT
recorded, so a contract break resurfaces every run instead of being sealed in.

Per-source module (see unctad_trademerchtotal.py):

    from ._unctad import make
    current_vintage, update = make("US.TradeMerchTotal", "unctad_trademerchtotal")
"""
from __future__ import annotations

import os

import pyarrow as pa

from ... import blob, config, merge
from ..base import Result
from ._common import Tally, finalize

DEDUP = ("series_key", "obs_date")


def _job():
    # jobs/ is imported lazily so a broken jobs tree cannot break fetcher discovery.
    import importlib
    return importlib.import_module("jobs.ingest_unctad_ds")


def _token(meta: dict) -> "str | None":
    """The release token: the publisher's own {version, lastUpdated}. ONE function for the probe and
    for the token update() stamps, so the two can never be spelled differently. None when the
    metadata carries neither: "None|None" on both sides would match for ever and seal the unit
    (review R1154); None makes the strategy fetch and update() stamp nothing."""
    v, lu = meta.get("version"), meta.get("lastUpdated")
    if v is None and lu is None:
        return None
    return f"{v}|{lu}"


def make(ds: str, source: str, window: bool = False):
    """Return (current_vintage, update) bound to one UNCTADstat dataset.

    window=True opts ONE source into the WINDOWED update (see _windowed below). Every module that passes
    nothing gets exactly the update it had before."""

    def current_vintage(unit):
        j = _job()
        return _token(j.report_metadata(ds))

    if window:
        return current_vintage, _windowed(ds, source)

    def update(unit, since) -> Result:
        j = _job()
        out_dir = config.source_dir(source)
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{source}.parquet")
        before = blob.row_count(path)
        tally = Tally()

        try:
            cid, key = j.creds()
            meta = j.report_metadata(ds)
            rows_k, rows_d, rows_v = j.pull_rows(ds, cid, key, meta)
        except j.UnsupportedLayout as e:
            # NAMED. This used to discard the exception, so every failure of any unctad source
            # rendered as a bare "1/1 sub-unit(s) ..." and the reason was gone. The label is
            # what finalize renders through _named into unit_state.last_error, the run note and
            # the digest.
            tally.structural_unit(f"{ds}: unsupported layout — {str(e)[:160]}")
            return finalize(tally, before, None, source=source, merged_rows=0)
        except Exception as e:                           # noqa: BLE001 — network/contract
            # Credentials travel as ClientId/ClientSecret HEADERS, not URL parameters
            # (jobs/ingest_unctad_ds.py), so the exception text carries no secret. Truncated
            # because a label is a signpost, not a stack trace.
            tally.transient_unit(f"{ds}: {type(e).__name__} — {str(e)[:160]}")
            return finalize(tally, before, None, source=source, merged_rows=0)

        if not rows_k:
            # A reachable API that yields zero parseable observations is a CONTRACT
            # change, not an empty release — do not record the vintage. Named for the same
            # reason as the handlers above: a bare count cannot be investigated.
            tally.structural_unit(f"{ds}: 200 but zero parseable observations")
            return finalize(tally, before, None, source=source, merged_rows=0)

        tbl = pa.table({"series_key": pa.array(rows_k, pa.string()),
                        "obs_date": pa.array(rows_d, pa.date32()),
                        "value": pa.array(rows_v, pa.float64())})
        merge.merge_and_write(path, tbl, mode="merge", dedup_keys=list(DEDUP))
        after = blob.row_count(path)
        tally.added_unit(max(0, after - before))
        cursors = {}
        for k, d in zip(rows_k, rows_d):
            if k not in cursors or d > cursors[k]:
                cursors[k] = d
        # finalize's THIRD positional is last_obs (a date); cursors go in the KEYWORD —
        # the dict passed positionally bound into state.db's last_obs_date column
        # ("type 'dict' is not supported"), caught by the first real orchestrator run.
        res = finalize(tally, after, max(rows_d), source=source,
                       series_cursors={k: v.isoformat() for k, v in cursors.items()})
        # THE RELEASE TOKEN, NOT finalize's "date-tail" PLACEHOLDER. finalize() stamps new_vintage=
        # "date-tail" on every Result (it is shared with the date-tail fetchers), and
        # bulk_snapshot_if_changed only fills in the probed token when a fetcher returns None - so
        # every unctad unit stored "date-tail", the probe never matched, and every tick re-pulled the
        # whole dataset: unctad_oceantrade spent ~47 min a week to report no_change, and ~16 h of
        # no_change runs in 30 days went to units with this placeholder (faostat already overrides it
        # the same way). The token is the one read BEFORE the pull, so a release that lands during
        # the pull reads as changed next tick - a re-pull, never a skipped release. Only on a clean
        # ok/no_change; the orchestrator also keeps the old vintage for anything it demotes.
        if res.status in ("ok", "no_change") and _token(meta) is not None:
            res.new_vintage = _token(meta)
        return res

    return current_vintage, update


# ================================================================================================
# THE WINDOWED UPDATE (opt-in, make(..., window=True); PLAN R10.3 step 4, 2026-10-30)
#
# Opted in: unctad_nonplasticsubststradebypartner ONLY. Every other unctad module calls make(ds, source)
# and runs the update above, unchanged (tests/test_unctad_window_cap.py pins that differentially).
#
# WHY (measured, receipts in the PR): its stored vintage is NULL, so every due turn is forced to pull. Its
# 2026-10-08 desktop turn re-walked release 10009 from nothing and was killed by the ps1 hard stop at
# 8,326 s while still FETCHING chunks (first spill 00:30:22Z, last 02:48:27Z, kill booked 02:48:33Z), before
# any merge. Whether the store already holds 10009 is not known. On main the pull never reads the window
# end, one request may take ~6 x 900 s plus retry sleeps, a kill after the merge leaves a debt nothing
# records, and a revision with no new row reads no_change and is sealed.
#
#   * STOP: the NETWORK phase ends at orchestrate._RUN_DEADLINE_TS minus AQUEDUCT_UNCTAD_ASSEMBLY_RESERVE_MIN.
#     Before every network request of the walk (never before a chunk re-read from the spill) the network
#     time left must be at least max(AQUEDUCT_UNCTAD_MARGIN_MIN floor, 2 x the longest request this pass);
#     every attempt is clamped to the network time left and every retry sleep counts against it, so a
#     request's WHOLE retry ladder ends before the reserve (jobs.ingest_unctad_ds.Window). ALL measures are
#     walked first; the reserve is checked again before the parse and merge. A stop books the rest as
#     deferred (finalize's ROTATING sentence), merges nothing, reports merged_rows=0, keeps every chunk and
#     every size-cap answer on disk, and stamps no vintage, so a later pass resumes the walk. A stopped
#     pass's run row still reads about its window's minutes, so the orchestrator's admission reserve keeps
#     treating this unit as large (PLAN DESKTOP STALL); this module does not change admission.
#     The floor is CHOSEN (5 min; the largest gap between two spill writes of the 10-08 turn was 42 s). The
#     ASSEMBLY RESERVE is MEASURED on three sibling turns (ASSEMBLY_RESERVE_MIN_DEFAULT). The phase timing
#     lines (walk per measure, parse, shape+table, merge, read-back + cursors) re-measure it on this source.
#   * CHANGE SIGNAL (c): merge_and_write(report_changed_keys=True), per-call cap = the table's own rows;
#     Result.changed_keys is the derive set; a revision with no added row is revised_unit (ok, so the CSV
#     step runs). A refused report runs the plain merge and every key is handed over.
#   * OWED MARK (b)(j)(n): data/clean_full/<source>/_unctad_owed.json through blob, the imf_imts_direct form
#     ("merging" before the merge - fails closed -, "owed" after it, "handed" with a token settled from
#     unit_state.last_error). An ABSENT mark beside an existing store is owed ONCE (main left no mark).
#     ROUTE B: a structural refusal while something is owed returns partial WITH every stored key.
#   * SPILLS (k): gated on the full release token version|lastUpdated, so a re-issue under the same version
#     wipes the chunks AND the size-cap answers (_capped.tsv, same directory) at the walk's start. A legacy
#     bare-version token is accepted only when the release's lastUpdated is earlier than every spilled
#     chunk (jobs.ingest_unctad_ds._spill_token_matches). The spills are NOT cleared after a merge: the
#     vintage is stamped later (orchestrator + state push), and a lost CAS or a kill before the push must not
#     turn a later pass into a full network re-walk. Disk is held until the next release, as on main.
#   * KEY SHAPE (l): the number of key parts is pinned to the store (a sidecar <store>.keyshape.json, seeded
#     once from the footer statistics); a release whose keys have another shape is STRUCTURAL (Route B when
#     something is owed, never green): the text names it a re-grain, an operator re-key (R22 clean re-pull),
#     never a merge.
# STATED RESIDUALS: every measure's CSV text is held in memory before the parse (about 2.7 GB of spills for
# this source; the 36.5 GB and 116 GB spill sets of other giants cannot use this order unchanged);
# report_metadata (one GET, a line this change does not touch) and the dimension GETs are
# bounded per read, not in total, against a body that drips; the parse and the merge cannot be interrupted,
# so the assembly reserve is the only bound on them; a lastUpdated move inside a multi-turn walk restarts the
# walk (no recorded case: 0 of 2 recorded token transitions kept the version); the other 95 unctad modules
# keep main's version-only spill gate, so a same-version re-issue there still reuses old chunks (AR-411 S8lu);
# the key-shape seed reads the footer min/max (2 keys per row group): measured {5} for this store and a full
# census of its 100,233,168 keys gives {5} too, but in another module a code containing '.' or a mixed store
# could make the seed a subset; outside the orchestrator (no state.db) a kill after the return is not
# covered, as in census and wid.
# ================================================================================================

import json as _json
import math as _math
import secrets as _secrets
import sqlite3 as _sqlite3
import sys as _sys
import time as _time

from ...errors import DefinitiveError, TransientError

MARGIN_FLOOR_MIN_DEFAULT = 5.0        # CHOSEN (largest spill gap measured on the 10-08 turn: 42 s)
# MEASURED, n=3 replay-only desktop turns of sibling giants (file mtimes vs runs/unit_state, UTC; see the
# PR): gstptradematrix 10-08 (76,643,660 rows) last walk start 00:20:57 -> unit end 00:29:20 = 8.4 min;
# creativegoodsvalue 10-04 (75,560,775) last network request 13:03:32 -> end 13:12:09 = 8.6 min;
# criticalmineralstradebypart 10-05 (109,956,508) last request 14:03:51 -> end 14:27:52 = 24.0 min (that end
# includes the orchestrator's CSV mapping). Scaled by rows to this store (100,233,168): 11.0 / 11.4 / 21.9
# min. ON MAIN each interval holds the LAST measure's re-read and parse plus the merge; THIS update parses
# EVERY measure after the walk, and this store has 2 measures (M0100 50,312,430 rows, M2200 49,920,738; a
# DuckDB census of the desktop store copy), so the bound is 2 x 21.9 = 43.8 min (as if each interval were
# all parse). Reserve = 60 min, about 1.4 x that bound. NOT measured: the change report and the key-shape
# pass this update adds; the phase timing lines below measure them.
ASSEMBLY_RESERVE_MIN_DEFAULT = 60.0
OWED_FILE = "_unctad_owed.json"
_MERGING, _OWED, _HANDED = "merging", "owed", "handed"
_FORMS = (_MERGING, _OWED, _HANDED)
_FENCE_NOTE = "csv phase exceeded its"     # orchestrate's csv-fence note: the hand-over was NOT derived
_DEBT_LABEL = "owed keys of an earlier merge (mark kept; a pass that merges hands them over)"


def _utc(ts) -> str:
    import datetime as _dt
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _window_end_ts():
    """The pass's run ceiling as an epoch timestamp, or None. Read through sys.modules (as wid, census and
    imf_imts_direct do), so this module never imports the orchestrator."""
    orch = _sys.modules.get("updater.orchestrate")
    ts = getattr(orch, "_RUN_DEADLINE_TS", None) if orch is not None else None
    if isinstance(ts, bool) or not isinstance(ts, (int, float)) or not _math.isfinite(ts):
        return None
    return float(ts)


def _env_num(source: str, name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        v = float(raw)
    except (TypeError, ValueError):
        v = float("nan")
    if not (_math.isfinite(v) and v >= 0):
        print(f"[{source}] {name}={raw!r} is not a finite number >= 0 - using {default:g}", flush=True)
        return default
    return v


def _owed_path(out_dir: str) -> str:
    return os.path.join(out_dir, OWED_FILE)


def _load_owed(out_dir: str, ds: str, source: str):
    """{} (nothing owed), a mark, or None when ABSENT (the caller decides). FAILS CLOSED: a read that
    raises -> TransientError; present content that is not a mark -> DefinitiveError naming its recovery."""
    try:
        raw = blob.read_bytes(_owed_path(out_dir))
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"{source}: owed mark {OWED_FILE} unreadable: {e!r}") from e
    if raw is None:
        return None
    try:
        obj = _json.loads(raw.decode("utf-8"))
        ok = isinstance(obj, dict) and (obj == {} or (
            obj.get("ds") == ds and obj.get("form") in _FORMS and isinstance(obj.get("utc"), str)
            and (obj["form"] != _HANDED or (isinstance(obj.get("token"), str) and bool(obj["token"])))))
    except (ValueError, UnicodeDecodeError):
        ok = False
    if not ok:
        raise DefinitiveError(
            f"{source}: owed mark {OWED_FILE} is present but is not a mark; existing data kept. RECOVERY "
            f'(operator): overwrite it through blob with {{"ds": "{ds}", "form": "owed", "utc": "<now>"}} - '
            f"the source's next merging pass then hands every stored key to the CSV step and settles the mark. An explicit "
            f"{{}} instead would drop the keys it owes.")
    return obj


def _save_owed(out_dir: str, mark: dict, source: str) -> bool:
    try:
        blob.write_bytes_atomic(_owed_path(out_dir),
                                _json.dumps(mark, indent=1, sort_keys=True).encode("utf-8"))
        return True
    except Exception as e:                                   # noqa: BLE001
        print(f"[{source}] owed mark NOT saved ({e!r})", flush=True)
        return False


def _handed_recorded(source: str, token: str):
    """(recorded, why) - the imf_imts_direct rule: settled only when unit_state.last_error carries the
    token without the csv-fence note; anything unreadable counts as NOT recorded; no state.db at all means
    the update ran outside the orchestrator, and the hand-over went to its caller."""
    if not os.path.exists(config.STATE_DB):
        return True, "no state.db: not run under the orchestrator, so the hand-over went to its caller"
    try:
        con = _sqlite3.connect(f"file:{config.STATE_DB}?mode=ro", uri=True, timeout=5)
        try:
            rows = con.execute("SELECT last_error FROM unit_state WHERE source_id=?", (source,)).fetchall()
        finally:
            con.close()
    except Exception as e:                                   # noqa: BLE001
        return False, f"state not readable ({type(e).__name__})"
    for (err,) in rows:
        if err and token in err:
            if _FENCE_NOTE in err:
                return False, "recorded, but the csv phase was abandoned at its fence"
            return True, "recorded"
    return False, "not recorded (a kill before the orchestrator's CSV step?)"


def _key_parts(k: str) -> int:
    return str(k).split("|", 1)[0].count(".") + 1


def _store_shape(path: str):
    """The stored keys' part counts: from <store>.keyshape.json, else (seeded once) from the footer
    statistics of the series_key column. None when it cannot be told (then nothing is refused on shape)."""
    try:
        raw = blob.read_bytes(path + ".keyshape.json")
        if raw is not None:
            v = _json.loads(raw.decode("utf-8")).get("key_parts")
            if isinstance(v, list) and v and all(isinstance(x, int) and not isinstance(x, bool) for x in v):
                return sorted(set(v))
    except Exception:                                        # noqa: BLE001
        pass
    try:
        md = blob.read_metadata(path)
        col = md.schema.to_arrow_schema().get_field_index("series_key")
        shapes = set()
        for i in range(md.num_row_groups):
            st = md.row_group(i).column(col).statistics
            if st is None or st.min is None or st.max is None:
                return None
            for v in (st.min, st.max):
                shapes.add(_key_parts(v.decode() if isinstance(v, bytes) else v))
        return sorted(shapes) or None
    except Exception:                                        # noqa: BLE001
        return None


def _fold(keys, dates) -> dict:
    cursors = {}
    for k, d in zip(keys, dates):
        if k not in cursors or d > cursors[k]:
            cursors[k] = d
    return {k: v.isoformat() for k, v in cursors.items()}


def _windowed(ds: str, source: str):
    def update(unit, since) -> Result:
        j = _job()
        say = lambda m: print(f"[{source}] {m}", flush=True)          # noqa: E731
        out_dir = config.source_dir(source)
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{source}.parquet")
        before = blob.row_count(path) if blob.exists(path) else 0
        tally = Tally()
        end_ts = _window_end_ts()
        floor_s = _env_num(source, "AQUEDUCT_UNCTAD_MARGIN_MIN", MARGIN_FLOOR_MIN_DEFAULT) * 60
        reserve_s = _env_num(source, "AQUEDUCT_UNCTAD_ASSEMBLY_RESERVE_MIN", ASSEMBLY_RESERVE_MIN_DEFAULT) * 60
        win = j.Window(end_ts, floor_s=floor_s, say=say, reserve_s=reserve_s)
        t_pass = _time.time()
        say(f"windowed pull of {ds}; run ceiling {_utc(end_ts) if end_ts is not None else 'none'}; margin "
            f"floor {floor_s / 60:g} min (chosen), assembly reserve {reserve_s / 60:g} min (measured on three "
            f"sibling turns, scaled; see ASSEMBLY_RESERVE_MIN_DEFAULT); store "
            f"{before:,} rows")

        # a kill's leftovers are never reusable: a chunk's .tmp (its spill), an owed-mark .tmp (local mirror)
        sd = j._spill_dir(ds)
        for d, pred in ((sd, lambda f: f.endswith(".tmp")),
                        (out_dir, lambda f: f.startswith(OWED_FILE + ".") and f.endswith(".tmp"))):
            if os.path.isdir(d):
                for f in os.listdir(d):
                    if pred(f):
                        try:
                            os.remove(os.path.join(d, f))
                        except OSError:
                            pass

        debt = {"on": False}
        owed = _load_owed(out_dir, ds, source)
        if owed is None:
            if before > 0:
                owed = {"ds": ds, "form": _OWED, "utc": _utc(_time.time())}
                say(f"no owed mark beside a store of {before:,} rows (the first pass after the switch from "
                    f"main, or a lost mark) - every stored key goes to the CSV step once")
            else:
                owed = {}
        if owed.get("form") == _HANDED:
            recorded, how = _handed_recorded(source, owed["token"])
            if recorded:
                say(f"owed mark settled: {owed['token']} was recorded by the orchestrator after its CSV step")
                _save_owed(out_dir, {}, source)
                owed = {}
            else:
                debt["on"] = True
                say(f"owed mark {owed['token']} {how} - every stored key is handed over again")
        elif owed:
            debt["on"] = True
            say(f"owed mark '{owed['form']}' ({owed['utc']}): an earlier merge's keys were never handed "
                f"over - every stored key goes to the CSV step")

        def _fin(**kw):
            if debt["on"]:
                tally.deferred_unit(_DEBT_LABEL)
            return finalize(tally, before, None, source=source, **kw)

        def _handed(res):
            token = f"hand-over {_utc(_time.time())}-{_secrets.token_hex(6)}"
            if _save_owed(out_dir, {"ds": ds, "form": _HANDED, "utc": _utc(_time.time()), "token": token},
                          source):
                say(f"owed mark -> handed ({token}); settled by the first later pass that finds it recorded")
            res.error = f"{token}: {res.error or ''}"
            return res

        def _hand_over_all(why: str):
            if not blob.exists(path):
                _save_owed(out_dir, {}, source)
                raise DefinitiveError(f"{why}; owed mark RELEASED: the store holds no object, so nothing "
                                      f"can be derived")
            try:
                t = blob.read_table(path, columns=["series_key", "obs_date"])
            except Exception as e:                           # noqa: BLE001
                raise DefinitiveError(f"{why}; owed mark kept - the store could not be read to hand its "
                                      f"keys over ({e!r})") from e
            cur = _fold(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist())
            say(f"{why} - ROUTE B: {len(cur):,} owed key(s) handed over anyway")
            mx = max(cur.values()) if cur else None
            return _handed(Result(
                status="partial", obs=t.num_rows, last_obs_date=mx, new_vintage="date-tail",
                series_cursors=cur, changed_keys=dict(cur),
                error=f"{why}; {len(cur):,} key(s) owed by an earlier merge handed over to the CSV step"))

        def _refuse(label: str):
            tally.structural_unit(label)
            try:
                return _fin(merged_rows=0)                   # raises DefinitiveError (structural)
            except DefinitiveError as e:
                if not debt["on"]:
                    raise                                    # nothing owed: as before
                return _hand_over_all(str(e))

        def before_assembly():
            left = win.left()
            if left is not None and left < reserve_s:
                raise j.WindowStop(f"{left / 60:.1f} min left, under the {reserve_s / 60:g}-min assembly "
                                   f"reserve before the parse and merge")
            say(f"walk timing: every measure walked in {_time.time() - t_pass:.1f}s; "
                f"{win.requests:,} request(s), longest {win.longest:.1f}s")

        try:
            win.before_request("the release metadata")
            cid, key = j.creds()
            meta = j.report_metadata(ds)
            tok = _token(meta)
            rows_k, rows_d, rows_v = j.pull_rows(
                ds, cid, key, meta, progress=j.log, window=win,
                spill_token=tok or f"{meta.get('version')}", before_assembly=before_assembly)
        except j.WindowStop as e:
            tally.deferred_unit(f"{ds}: stopped before the window end - {str(e)[:200]}")
            say(f"STOP before the window end: {e}; every chunk and size-cap answer stays on disk for a later "
                f"pass ({_time.time() - t_pass:.1f}s into the pass)")
            return _fin(merged_rows=0)
        except j.UnsupportedLayout as e:
            return _refuse(f"{ds}: unsupported layout — {str(e)[:160]}")
        except Exception as e:                               # noqa: BLE001 — network/contract
            tally.transient_unit(f"{ds}: {type(e).__name__} — {str(e)[:160]}")
            return _fin(merged_rows=0)

        if not rows_k:
            return _refuse(f"{ds}: 200 but zero parseable observations")
        t_asm = _time.time()
        shape_new = sorted({_key_parts(k) for k in rows_k})
        if before > 0:
            shape_store = _store_shape(path)
            if shape_store is not None and shape_new != shape_store:
                return _refuse(f"{ds}: the release's keys have {shape_new} part(s), the store's {shape_store} - "
                               f"a changed key shape is an operator re-key (R22 clean re-pull), REFUSED, nothing "
                               f"merged")
        tbl = pa.table({"series_key": pa.array(rows_k, pa.string()),
                        "obs_date": pa.array(rows_d, pa.date32()),
                        "value": pa.array(rows_v, pa.float64())})
        t_tbl = _time.time()
        # TWO-PHASE MARK, phase 1, FAIL CLOSED: if it is not stored, nothing is merged (the chunks stay).
        if not _save_owed(out_dir, {"ds": ds, "form": _OWED if debt["on"] else _MERGING,
                                    "utc": _utc(_time.time())}, source):
            tally.transient_unit("merge: pre-merge owed mark not saved - not merged, chunks kept")
            return _fin(merged_rows=0)
        try:
            try:
                _n, _last, changed = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=list(DEDUP),
                                                           report_changed_keys=True,
                                                           changed_keys_cap=max(tbl.num_rows, 1))
            except ValueError as e:
                if not str(e).startswith("report_changed_keys refused"):
                    raise
                say(f"change report refused ({e}) - plain merge, every key handed over")
                merge.merge_and_write(path, tbl, mode="merge", dedup_keys=list(DEDUP))
                changed = None
        except DefinitiveError as e:
            _save_owed(out_dir, owed, source)                # nothing was published
            if not debt["on"]:
                raise
            return _hand_over_all(f"{ds}: merge refused ({e})")
        t_merged = _time.time()
        owes = debt["on"] or changed is None or bool(changed)
        mark_ok = _save_owed(out_dir, {"ds": ds, "form": _OWED, "utc": _utc(_time.time())} if owes else {},
                             source)
        after = blob.row_count(path)
        added = max(0, after - before)
        if added:
            tally.added_unit(added, ds)
        elif changed is None or changed:
            tally.revised_unit(ds)                           # served values changed, no row added
        else:
            tally.added_unit(0, ds)
        cursors = _fold(rows_k, rows_d)
        ck = dict(cursors) if (debt["on"] or changed is None) else dict(changed)
        try:
            blob.write_bytes_atomic(path + ".keyshape.json",
                                    _json.dumps({"ds": ds, "key_parts": shape_new}).encode("utf-8"))
        except Exception as e:                               # noqa: BLE001
            say(f"key-shape sidecar not saved ({e!r}); a later pass reads the footer statistics")
        # THE SPILLS ARE KEPT after the merge (review AR-411 O2): the vintage is stamped only later, by the
        # orchestrator and the state push, so a lost CAS or a kill before the push would otherwise make the
        # a later pass re-walk the whole release over the network. A re-issue cannot reuse them: the walk wipes
        # chunks AND size-cap answers at its start under any other full release token.
        if not mark_ok:
            say("post-merge owed mark not saved - the 'merging' mark stays (an over-report, never a loss)")
        say(f"assembly timing: shape+table {t_tbl - t_asm:.1f}s, merge {t_merged - t_tbl:.1f}s, read-back+"
            f"cursors {_time.time() - t_merged:.1f}s; store {after:,} rows; "
            f"{len(changed) if changed is not None else 'unknown'} key(s) changed"
            + (f", {len(ck):,} handed over (owed mark)" if debt["on"] else ""))
        mx = max(rows_d)
        try:
            res = finalize(tally, after, mx, source=source, series_cursors=cursors)
        except DefinitiveError as e:
            if not owes:
                raise
            say(f"{e} - ROUTE B: {len(ck):,} changed/owed key(s) handed over anyway")
            res = Result(status="partial", obs=after, last_obs_date=mx, new_vintage="date-tail",
                         series_cursors=cursors,
                         error=f"{e}; {len(ck):,} changed/owed key(s) handed over to the CSV step")
        res.changed_keys = ck
        if res.status in ("ok", "no_change") and tok is not None:
            res.new_vintage = tok
        if debt["on"] and res.status == "no_change":
            res.status = "ok"
            res.error = f"{res.error}; {len(ck):,} key(s) carried from an earlier merge's owed mark"
        return _handed(res) if owes else res

    return update
