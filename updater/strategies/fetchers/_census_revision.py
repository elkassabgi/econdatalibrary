"""census intltrade: the ANNUAL REVISION RE-PULL (ledger R1819, review AR-442 5d; design review AR-444).

WHY. census.py's tail (_time_windows) walks forward from each flow's newest stored month and
re-requests only that boundary month, so a month older than the boundary is never asked again. The
Census Bureau restates complete prior years: in imports/hitech the national TOTAL served today differs
from the store in exactly the 36 months 2023-01..2025-12 and agrees in the other 158 (AR-442 indep.log),
and imports/sitc 2024-01 differs in its TOTAL and in its row set (AR-441 R0b). 15,214,299 of the
40,908,842 intltrade rows (24 tables) lie in 2023-2025 (AR-442 stale_size.log). The revision is MEASURED
in those two tables only; for the other 22 it is inferred. The cause ("an annual revision of the prior
three complete years") fits the data but is NOT verified against the publisher's own text.

WHAT THIS DOES, per FAMILY (intltrade/imports, intltrade/exports - every flow of one direction moves
together, because the stored TOTALs of the import tables agree in 200/200 months and a per-table
re-pull would break that agreement, AR-442 5b):
  1. DUE once a cycle (cycle = the UTC year; window = the three complete years before it) from
     START_MMDD (a CHOSEN date, not the publisher's - see START_MMDD_DEFAULT), or when the SENTINEL
     probe (January of last year, one small flow per family, at most every PROBE_DAYS) finds that
     the publisher no longer serves what the store holds.
  2. FETCH, resumable, one PIECE = (flow, month), only for months the store already holds rows for
     (a refresh, never a backward backfill). Same request as the tail (pins, split, predicates) and
     the same known-series filter (R278). Each complete month is STAGED as its own parquet OUTSIDE
     data/clean_full (AR-444 a: anything under clean_full/<src> is counted as served by
     tools/series_census.py and make_servable). The piece set is recomputed from the flows the tail
     sees on every pass, so a flow the publisher retires mid-campaign drops out (AR-444 b).
     A month that answers no rows while the store holds rows for it is NOT staged as empty: it is a
     HOLD, and its stored rows stay as they are.
  3. VERIFY: once every piece is staged or held, the first piece is fetched again and compared with
     its staged copy. A difference means the publisher moved during the campaign: a new ATTEMPT
     re-stages everything. Attempts are capped per cycle (AR-444 c).
  4. PUBLISH, per flow: the staged months are merged with merge_and_write(mode="merge") - revised
     rows are overwritten, NOTHING is deleted. Store rows in a re-pulled month whose key the publisher
     no longer serves are HELD: counted, sampled and named in every run note, never removed (deleting
     served rows is the owner's decision, AR-442 5c, R278). They are old-vintage rows next to revised
     ones, so those months' detail rows may no longer sum to the revised TOTAL (R1817) - the note says
     so. A flow whose staged rows match a stored row on every key column except a LABEL column is
     not merged, because the merge would ADD a second row instead of overwriting; it is held and
     named. A label is every dedup-key column that is not a declared dimension code (DIMENSION_CODES,
     read from the real schemas: *_LDESC, *_SDESC, HITECH_DESC, CTY_NAME, DIST_NAME, UNIT_QY1,
     LAST_UPDATE - R1824). LAST_UPDATE stays in _dedup_for's key (it is '0' on every stored row of
     all 24 tables and the key without labels is unique, AR-444 redkey_all.log); the TAIL would still
     duplicate a row whose LAST_UPDATE or label the publisher changes at the boundary month - a
     pre-existing property this module does not change. Every published flow's note carries
     changed / added / held counts per year, and a flow that would ADD more rows under served ids
     than the growth bound (ADDED_CAP_*) is held, not merged (R1824 D2). Published tables go through census's owed-mark path, so their
     table CSVs are re-derived. After the last flow, the sentinel month must equal the store; if it
     does not, that is reported as a DEFECT and no new attempt is started (AR-444 c).

BUDGET. The pieces run AFTER the tail, under census's own stop check (budget + window end). A
separate per-pass cap, BUDGET_MIN_DEFAULT, gates only the START of a new piece; a piece already
started is finished unless the stop check fires (AR-444 d - a month slower than the cap would
otherwise never be staged). A stop inside a piece stages nothing for that month; a later pass asks
it again.

OFF BY DEFAULT (AR-444 e). Merging this code must not arm a write over ~15.2M served rows ahead of the
R1760 repair sequence. It runs only from the date set by a reviewed one-line commit of
OPERATOR_ACTIVATED_ON, or by ENV_ACTIVATE on the workstation.

STATED RESIDUALS. The publish of one family can span passes if a pass stops part-way; until it ends,
the family's tables are of two vintages (a later pass finishes the publish after its forward tail and before any new revision fetch). A flow
whose piece fails on PIECE_FAIL_CAP consecutive passes is HELD by name and the family goes on without it.
census's cadence is monthly, so the probe can run only as often as census is attempted. Real piece
times are not measured (exports/naics was ~537 s for one month split 7 ways).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time

import pyarrow as pa

from ... import blob, config, merge
from ...errors import DefinitiveError, TransientError

STATE_FILE = "_census_revision.json"
ENV_ACTIVATE = "CENSUS_REVISION_FROM"
# Set by a reviewed one-line commit, after the R1760 repair plan has fixed the order. None = off.
OPERATOR_ACTIVATED_ON: dt.date | None = None

YEARS = 3
# CHOSEN, not measured: AR-442 found the store's 2026-03 history ingested before the 2026-04 release
# (published ~June), the release an annual revision would normally ride on. The probe covers a later one.
START_MMDD_DEFAULT = "07-01"
PROBE_DAYS_DEFAULT = 7
BUDGET_MIN_DEFAULT = 30.0
MAX_ATTEMPTS_PER_CYCLE = 3
PIECE_FAIL_CAP = 3          # consecutive failing passes of one piece before its flow is held
SAMPLE_KEYS = 5
# One small, unsplit flow per family. An explicit list, following census.py's own precedent.
SENTINEL = {"imports": "intltrade/imports/hitech", "exports": "intltrade/exports/hitech"}
# THE DIMENSION CODES of the 24 intltrade tables - an ALLOW-LIST, read from the real store schemas
# (review AR-444 schema.log: the dedup-key columns of all 24 tables). Every other dedup-key column is a
# LABEL: *_LDESC, *_SDESC, HITECH_DESC, CTY_NAME, DIST_NAME, UNIT_QY1, LAST_UPDATE. A suffix rule missed
# HITECH_DESC and UNIT_QY1 (R1824), and HITECH is the sentinel table, so the rule is inverted: anything
# not declared a code is a label. An unknown new column therefore errs toward HOLDING a flow (a label
# conflict), never toward adding a duplicate row.
DIMENSION_CODES = frozenset({
    "DF", "SUMMARY_LVL", "SUMMARY_LVL2", "COMM_LVL", "DISTRICT", "CTY_CODE", "STATE", "MONTH",
    "WORLD", "E_ENDUSE", "I_ENDUSE", "HITECH", "E_COMMODITY", "I_COMMODITY", "NAICS", "SITC", "USDA",
    "SERIES_KEY", "OBS_DATE", "TIME"})
# A revised year may ADD rows under series the flow already publishes (the intltrade dedup key is wider
# than series_key: hitech 2025 has 12 fetch-only key-months, AR-442). Bounded per flow: more than
# max(ADDED_CAP_MIN_ROWS, ADDED_CAP_PCT % of the stored rows in the re-pulled months) is HELD, not merged.
ADDED_CAP_MIN_ROWS_DEFAULT = 50
ADDED_CAP_PCT_DEFAULT = 1.0


def _env_float(name, default):
    try:
        v = float(os.environ.get(name, "") or default)
        return v if v >= 0 else default
    except ValueError:
        return default


def activated_on(environ=None):
    env = os.environ if environ is None else environ
    raw = (env.get(ENV_ACTIVATE) or "").strip()
    if raw:
        try:
            return dt.date.fromisoformat(raw)
        except ValueError:
            print(f"[census] WARNING: {ENV_ACTIVATE}={raw!r} is not YYYY-MM-DD - annual revision OFF",
                  flush=True)
            return None
    return OPERATOR_ACTIVATED_ON


def start_date(year: int) -> dt.date:
    raw = (os.environ.get("CENSUS_REVISION_START_MMDD") or START_MMDD_DEFAULT).strip()
    try:
        m, d = (int(x) for x in raw.split("-"))
        return dt.date(year, m, d)
    except (ValueError, TypeError):
        m, d = (int(x) for x in START_MMDD_DEFAULT.split("-"))
        return dt.date(year, m, d)


def staging_root() -> str:
    """OUTSIDE data/clean_full (AR-444 a), still a store path (a /data/ segment) so blob routes it."""
    return os.path.join(os.path.dirname(config.DATA_ROOT), "census_revision_staging")


def family_of(flow: str):
    p = flow.split("/")
    return p[1] if len(p) >= 3 and p[0] == "intltrade" else None


def window_months(cycle: int) -> list[str]:
    return [f"{y:04d}-{m:02d}" for y in range(cycle - YEARS, cycle) for m in range(1, 13)]


def is_label(col: str) -> bool:
    """True for every dedup-key column that is not a declared dimension code (DIMENSION_CODES)."""
    return col.upper() not in DIMENSION_CODES


# ---- state ------------------------------------------------------------------------------------------

def load_state(out_dir) -> dict:
    """FAILS CLOSED: absent -> {}; unreadable -> TransientError; malformed -> DefinitiveError."""
    path = os.path.join(out_dir, STATE_FILE)
    try:
        raw = blob.read_bytes(path)
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"census: revision state {STATE_FILE} unreadable: {e!r}") from e
    if raw is None:
        return {"families": {}}
    try:
        obj = json.loads(raw.decode("utf-8"))
        ok = isinstance(obj, dict) and isinstance(obj.get("families"), dict)
    except (ValueError, UnicodeDecodeError):
        ok = False
    if not ok:
        raise DefinitiveError(f"census: revision state {STATE_FILE} is present but malformed; "
                              f"nothing re-pulled, existing data kept")
    return obj


def save_state(out_dir, st) -> bool:
    try:
        blob.write_bytes_atomic(os.path.join(out_dir, STATE_FILE),
                                json.dumps(st, indent=1, sort_keys=True).encode("utf-8"))
        return True
    except Exception as e:                                   # noqa: BLE001
        print(f"[census] revision state NOT saved: {e!r}", flush=True)
        return False


def _new_campaign(cycle, attempt, today, why):
    print(f"[census] annual revision: campaign cycle {cycle} attempt {attempt} started ({why})",
          flush=True)
    return {"cycle": cycle, "attempt": attempt, "phase": "fetch", "started": today.isoformat(),
            "why": why, "staged": {}, "holds": {}, "published": [], "held_rows": {},
            "label_conflicts": {}, "first_piece": None, "completed": None, "last_probe": None,
            "drift_after_publish": False, "stuck": None}


def _stage_path(fam, fs, flow, month):
    from .census import _store_name
    return os.path.join(staging_root(), fam, f"{fs['cycle']}-a{fs['attempt']}",
                        _store_name(flow), f"{month}.parquet")


# ---- reading the store ------------------------------------------------------------------------------

def stored_months(path, months) -> dict:
    """{YYYY-MM: rows} for the months in `months` that the store holds rows for."""
    want = set(months)
    try:
        od = blob.read_table(path, columns=["obs_date"]).column("obs_date").to_pylist()
    except Exception:                                        # noqa: BLE001
        return {}
    out: dict = {}
    for d in od:
        if d is None:
            continue
        k = f"{d.year:04d}-{d.month:02d}"
        if k in want:
            out[k] = out.get(k, 0) + 1
    return out


def _month_bounds(month):
    y, m = int(month[:4]), int(month[5:7])
    lo = dt.date(y, m, 1)
    hi = dt.date(y + (m == 12), 1 if m == 12 else m + 1, 1)
    return lo, hi


def _store_rows_in(path, months, cols):
    """The store rows dated in `months`, as a table with `cols` (missing columns -> null)."""
    import pyarrow.compute as pc
    names = blob.read_schema(path).names
    tbl = blob.read_table(path, columns=[c for c in cols if c in names])
    mask = None
    for mo in months:
        lo, hi = _month_bounds(mo)
        m1 = pc.and_(pc.greater_equal(tbl.column("obs_date"), pa.scalar(lo, pa.date32())),
                     pc.less(tbl.column("obs_date"), pa.scalar(hi, pa.date32())))
        mask = m1 if mask is None else pc.or_(mask, m1)
    return tbl.filter(mask) if mask is not None else tbl.slice(0, 0)


def _rowmap(tbl, keys):
    cols = list(tbl.column_names)
    k = [c for c in keys if c in cols]
    v = [c for c in cols if c not in k]
    kc = [[None if x is None else str(x) for x in tbl.column(c).to_pylist()] for c in k]
    vc = [[None if x is None else str(x) for x in tbl.column(c).to_pylist()] for c in v]
    n = tbl.num_rows
    return {tuple(col[i] for col in kc): tuple(col[i] for col in vc) for i in range(n)}, k, v


def differs(fetched, reference, keys) -> list[str]:
    """Reasons `fetched` disagrees with `reference` on the keys `fetched` carries. Reference-only keys
    are NOT a difference here (they are the held set)."""
    fm, k, v = _rowmap(fetched, keys)
    cols = k + v
    ref = reference.select([c for c in cols if c in reference.column_names])
    rm, _k, _v = _rowmap(ref, keys)
    missing = [kk for kk in fm if kk not in rm]
    changed = [kk for kk, vv in fm.items() if kk in rm and rm[kk] != vv]
    out = []
    if missing:
        out.append(f"{len(missing)} fetched key(s) absent from the reference")
    if changed:
        out.append(f"{len(changed)} row(s) with different values")
    return out


# ---- one month from the API -------------------------------------------------------------------------

def fetch_month(ctx, flow, path, month):
    """('ok', table) | ('empty', None) | ('stop', None) | ('fail', label) | ('refused', label).
    Same request as the tail. 'refused' is a DefinitiveError (an HTTP refusal, a broken shape): the
    tail books those as structural and so does the caller; 'fail' is a TransientError."""
    from . import census as cs
    p0 = time.time()
    dim_cols, data_cols, shapes, pins = cs._dims_from_store(path)
    if not dim_cols:
        return "refused", f"{flow} {month}: no dimensions recoverable from the stored series_key"
    get_cols = [c for c in data_cols if c.lower() not in cs._IMPLICIT]
    gcol = cs._geo_col(data_cols)
    levels = cs._store_geo_levels(path, gcol) if gcol else set()
    preds = cs._predicates_for(flow, levels)
    pins, _dropped = cs._request_pins(pins, preds)
    reqs = cs._split_requests(flow, path, dim_cols, pins)
    # The store re-reads above are part of what one piece costs: timed into the stop margin.
    ctx.note_req(time.time() - p0)
    parts = []
    for pred in preds:
        for rp in reqs:
            if ctx.stop_now():
                return "stop", None
            r0 = time.time()
            try:
                part = cs._fetch(ctx.sess, flow, get_cols, month, ctx.key, pred, rp,
                                 req_end_ts=ctx.req_end_ts)
            except cs._WindowStop:
                ctx.force_stop()
                return "stop", None
            except TransientError as e:
                return "fail", f"{flow} revision time={month} for={pred} - {cs._why(e)}"
            except DefinitiveError as e:
                return "refused", f"{flow} revision time={month} for={pred} - {cs._why(e)}"
            finally:
                ctx.note_req(time.time() - r0)
            time.sleep(cs.RATE)
            if part and len(part) >= 2:
                parts.append(part)
    lo, _hi = _month_bounds(month)
    known = cs._store_keys(path)
    cols = {c: [] for c in data_cols}
    keys, dates = [], []
    for part in parts:
        hidx = {c: i for i, c in enumerate(part[0])}
        ti = {c.lower(): i for i, c in enumerate(part[0])}.get("time")
        if ti is None:
            return "fail", f"{flow} revision time={month}: response lacks time"
        for row in part[1:]:
            d = ctx.J.parse_obs_date(row[ti])
            if d is None or (d.year, d.month) != (lo.year, lo.month):
                continue
            sk = cs._key_for(ctx.J, row, hidx, dim_cols, shapes, f"timeseries/{flow}")
            if sk is None or (known and sk not in known):
                continue                                     # never mints a series (R278)
            keys.append(sk)
            dates.append(d)
            for c in data_cols:
                i = hidx.get(c)
                cols[c].append(row[i] if i is not None and i < len(row) else None)
    if not keys:
        return "empty", None
    arrays = {"series_key": pa.array(keys, pa.string()), "obs_date": pa.array(dates, pa.date32())}
    for c in data_cols:
        arrays[c] = pa.array([None if v is None else str(v) for v in cols[c]], pa.string())
    return "ok", pa.table(arrays)


# ---- the pass -----------------------------------------------------------------------------------------

class Outcome:
    def __init__(self):
        self.cursors: dict = {}      # table id -> max obs_date iso (published tables)
        self.deferred: list = []
        self.transient: list = []
        self.structural: list = []   # refusals (DefinitiveError), booked as the tail books them
        self.notes: list = []
        self.published: list = []


def _plan(fs, fam_flows, path_of):
    """[(flow, month, stored_rows)] in campaign order: sentinel flow first, months ascending.
    Recomputed every pass from the flows the tail sees (AR-444 b)."""
    out = []
    for flow in fam_flows:
        sm = stored_months(path_of(flow), window_months(fs["cycle"]))
        out += [(flow, mo, sm[mo]) for mo in sorted(sm)]
    return out


def run(ctx, flows, out_dir, today) -> Outcome:
    from . import census as cs
    oc = Outcome()
    act = activated_on()
    if act is None or today < act:
        print(f"[census] annual revision OFF (not activated: {ENV_ACTIVATE} / "
              f"_census_revision.OPERATOR_ACTIVATED_ON)", flush=True)
        return oc
    try:
        st = load_state(out_dir)
    except TransientError as e:
        oc.transient.append(str(e))
        return oc
    budget_s = _env_float("CENSUS_REVISION_BUDGET_MIN", BUDGET_MIN_DEFAULT) * 60
    probe_days = _env_float("CENSUS_REVISION_PROBE_DAYS", PROBE_DAYS_DEFAULT)
    t0 = time.time()

    def path_of(flow):
        return os.path.join(out_dir, f"{cs._store_name(flow)}.parquet")

    def may_start():
        return (time.time() - t0) < budget_s and not ctx.stop_now()

    families: dict = {}
    for f in flows:
        fam = family_of(f)
        if fam:
            families.setdefault(fam, []).append(f)
    cycle = today.year
    for fam in sorted(families):
        sent = SENTINEL.get(fam)
        fam_flows = sorted(families[fam], key=lambda f: (f != sent, f))
        fs = st["families"].get(fam)
        # ---- due? ---------------------------------------------------------------------------------
        if fs is None or fs.get("cycle") != cycle:
            if today < start_date(cycle):
                continue
            fs = st["families"][fam] = _new_campaign(cycle, 1, today, "annual cycle")
            save_state(out_dir, st)
        elif fs["phase"] == "done":
            _note_holds(oc, fam, fs)
            if fs.get("drift_after_publish"):
                oc.notes.append(f"{fam}: DEFECT - the sentinel still differed from the store right after "
                                f"publish (cycle {cycle} attempt {fs['attempt']}); no new attempt started")
                continue
            lp = fs.get("last_probe")
            if lp and (today - dt.date.fromisoformat(lp)).days < probe_days:
                continue
            if not (sent in fam_flows and may_start()):
                continue
            drift = _sentinel_drift(ctx, sent, path_of(sent), cycle)
            if drift is None:
                continue                                     # could not probe: try again later
            fs["last_probe"] = today.isoformat()
            if drift and fs["attempt"] >= MAX_ATTEMPTS_PER_CYCLE:
                oc.notes.append(f"{fam}: the publisher differs from the store again ({'; '.join(drift)}) "
                                f"but cycle {cycle} has used {fs['attempt']} attempts - owner check")
            elif drift:
                fs = st["families"][fam] = _new_campaign(cycle, fs["attempt"] + 1, today,
                                                         "sentinel drift: " + "; ".join(drift))
            save_state(out_dir, st)
            if fs["phase"] == "done":
                continue
        if fs.get("stuck"):
            oc.notes.append(f"{fam}: annual revision STUCK - {fs['stuck']}")
            continue
        # ---- fetch --------------------------------------------------------------------------------
        # A FLOW WHOSE PIECE FAILS ON PIECE_FAIL_CAP CONSECUTIVE PASSES is held by name and the family
        # goes on without it (review AR-444 phase 3: one refused flow otherwise kept the whole family in
        # "fetch" for ever, R1694's 29 refused flows being the precedent).
        failed_flows = fs.setdefault("failed_flows", {})
        live = [f for f in fam_flows if f not in failed_flows]
        if fs["phase"] == "fetch":
            plan = _plan(fs, live, path_of)
            if plan and fs.get("first_piece") is None:
                fs["first_piece"] = [plan[0][0], plan[0][1]]
            todo = [(f, mo, n) for f, mo, n in plan
                    if mo not in fs["staged"].get(f, []) and mo not in fs["holds"].get(f, {})]
            fails = fs.setdefault("fails", {})
            for flow, mo, n in todo:
                if flow in failed_flows:
                    continue
                if not may_start():
                    break
                kind, val = fetch_month(ctx, flow, path_of(flow), mo)
                if kind == "stop":
                    break
                if kind in ("fail", "refused"):
                    (oc.structural if kind == "refused" else oc.transient).append(val)
                    pk = f"{flow}|{mo}"
                    fails[pk] = fails.get(pk, 0) + 1
                    if fails[pk] >= PIECE_FAIL_CAP:
                        failed_flows[flow] = f"{mo} failed on {fails[pk]} consecutive passes: {val}"
                        print(f"[census] {flow}: annual revision HOLDS this flow - {failed_flows[flow]}",
                              flush=True)
                    save_state(out_dir, st)
                    continue
                fails.pop(f"{flow}|{mo}", None)
                if kind == "empty":
                    fs["holds"].setdefault(flow, {})[mo] = (f"publisher answered no rows; the store "
                                                            f"holds {n} - kept, not re-pulled")
                else:
                    blob.write_table_atomic(_stage_path(fam, fs, flow, mo), val)
                    fs["staged"].setdefault(flow, []).append(mo)
                save_state(out_dir, st)
            live = [f for f in fam_flows if f not in failed_flows]
            left = sum(1 for f, mo, _n in todo if f in live and mo not in fs["staged"].get(f, [])
                       and mo not in fs["holds"].get(f, {}))
            if left:
                oc.deferred.append(f"{fam}: annual revision cycle {cycle} attempt {fs['attempt']} - "
                                   f"{left} of {len(plan)} month piece(s) not yet fetched")
                continue
            # ---- verify ---------------------------------------------------------------------------
            if not may_start():
                oc.deferred.append(f"{fam}: annual revision - all pieces staged, verify not yet run")
                continue
            # The FIRST STAGED piece in campaign order (the planned first piece unless it was held).
            fp = next(([f, mo] for f, mo, _n in plan if f in live and mo in fs["staged"].get(f, [])),
                      None)
            if fp is None:
                oc.notes.append(f"{fam}: annual revision verify SKIPPED - no piece was staged "
                                f"(every piece held)")
            if fp:
                kind, val = fetch_month(ctx, fp[0], path_of(fp[0]), fp[1])
                if kind == "stop":
                    oc.deferred.append(f"{fam}: annual revision - verify stopped")
                    continue
                if kind != "ok":
                    oc.transient.append(f"{fam}: annual revision verify re-fetch {kind}: {val or ''}")
                    continue
                staged = blob.read_table(_stage_path(fam, fs, fp[0], fp[1]))
                dk = cs._dedup_for(fp[0], [c for c in staged.column_names
                                           if c not in ("series_key", "obs_date")])
                why = differs(val, staged, dk) or differs(staged, val, dk)
                if why:
                    if fs["attempt"] >= MAX_ATTEMPTS_PER_CYCLE:
                        fs["stuck"] = (f"the publisher moved during attempt {fs['attempt']} "
                                       f"({'; '.join(why)}) and the cycle's attempts are used up")
                    else:
                        st["families"][fam] = _new_campaign(
                            cycle, fs["attempt"] + 1, today,
                            f"verify: {fp[0]} {fp[1]} moved during the campaign ({'; '.join(why)})")
                    save_state(out_dir, st)
                    oc.deferred.append(f"{fam}: annual revision restarted - the publisher moved "
                                       f"during the campaign")
                    continue
            fs["phase"] = "publish"
            save_state(out_dir, st)
        # ---- publish ------------------------------------------------------------------------------
        if fs["phase"] == "publish":
            live = [f for f in fam_flows if f not in failed_flows]
            _publish(ctx, cs, fam, fs, live, path_of, oc, out_dir, st, today)
            if fs["phase"] == "publish":
                oc.deferred.append(f"{fam}: annual revision publish part-way - "
                                   f"{len([f for f in fs['published'] if f in live])} of {len(live)} "
                                   f"flow(s) done; the family is two vintages until it finishes")
                continue
            sm = sorted(fs["staged"].get(sent, [])) if sent in live else []
            if sm and sent not in fs.get("label_conflicts", {}) and sent not in fs.get("growth_held", {}):
                drift = _sentinel_drift(ctx, sent, path_of(sent), cycle, month=sm[0])
                fs["drift_after_publish"] = bool(drift)
                if drift:
                    oc.notes.append(f"{fam}: DEFECT - sentinel differs from the store right after "
                                    f"publish ({'; '.join(drift)})")
            fs["last_probe"] = today.isoformat()
            save_state(out_dir, st)
            _note_holds(oc, fam, fs)
    return oc


def _sentinel_drift(ctx, flow, path, cycle, month=None):
    """Reasons the publisher's sentinel month differs from the store; [] when equal; None if unknown."""
    from . import census as cs
    month = month or f"{cycle - 1:04d}-01"
    if not stored_months(path, [month]):
        return None
    kind, val = fetch_month(ctx, flow, path, month)
    if kind != "ok":
        return None
    store = _store_rows_in(path, [month], val.column_names)
    dk = cs._dedup_for(flow, [c for c in val.column_names if c not in ("series_key", "obs_date")])
    return differs(val, store, dk)


def _publish(ctx, cs, fam, fs, fam_flows, path_of, oc, out_dir, st, today):
    for flow in fam_flows:
        if flow in fs["published"]:
            continue
        months = sorted(fs["staged"].get(flow, []))
        held_months = sorted(fs["holds"].get(flow, {}))
        if not months:
            fs["published"].append(flow)
            save_state(out_dir, st)
            continue
        if ctx.stop_now():
            return
        path = path_of(flow)
        c0 = time.time()                 # the comparison below is timed into the stop margin too
        tid = cs._store_name(flow)
        new = pa.concat_tables([blob.read_table(_stage_path(fam, fs, flow, mo)) for mo in months])
        data_cols = [c for c in new.column_names if c not in ("series_key", "obs_date")]
        dk = cs._dedup_for(flow, data_cols)
        old = _store_rows_in(path, months + held_months, new.column_names)
        newfull = _rowmap(new, dk)[0]
        oldfull = _rowmap(old.select([c for c in new.column_names if c in old.column_names]), dk)[0]
        newk = set(newfull)
        oldmap = oldfull
        store_only = [k for k in oldmap if k not in newk]
        added = [k for k in newk if k not in oldmap]
        changed = [k for k in newk if k in oldmap and oldmap[k] != newfull[k]]
        # obs_date is the second key part (dk starts series_key, obs_date): count by YEAR.
        by_year: dict = {}
        for name, ks in (("changed", changed), ("added", added), ("held", store_only)):
            for k in ks:
                y = str(k[1])[:4]
                by_year.setdefault(y, {"changed": 0, "added": 0, "held": 0})[name] += 1
        counts = " ".join(f"{y} changed={v['changed']} added={v['added']} held={v['held']}"
                          for y, v in sorted(by_year.items())) or "no difference"
        fs.setdefault("counts", {})[flow] = by_year
        ctx.note_merge(time.time() - c0)
        # LABEL CONFLICT: same row on every non-label key column, different label -> a merge would ADD.
        nolab = [c for c in dk if not is_label(c)]
        if len(nolab) < len(dk):
            nl_new = {}
            for k in newk:
                nl_new.setdefault(tuple(v for c, v in zip(dk, k) if not is_label(c)), set()).add(k)
            conflicts = sum(1 for k in oldmap
                            if k not in newk
                            and tuple(v for c, v in zip(dk, k) if not is_label(c)) in nl_new)
        else:
            conflicts = 0
        if conflicts:
            fs["label_conflicts"][flow] = conflicts
            fs["published"].append(flow)
            save_state(out_dir, st)
            oc.notes.append(f"{flow}: annual revision NOT merged - {conflicts} stored row(s) differ from "
                            f"the publisher only in a label column, so a merge would add a second row; "
                            f"held for a re-key decision ({counts})")
            continue
        # THE GROWTH BOUND (R1824 D2): rows the publisher serves that the store lacks are ADDED by the
        # merge under ids already served. Counted always; over the bound the flow is held, not merged.
        cap_min = _env_float("CENSUS_REVISION_ADDED_CAP_MIN_ROWS", ADDED_CAP_MIN_ROWS_DEFAULT)
        cap_pct = _env_float("CENSUS_REVISION_ADDED_CAP_PCT", ADDED_CAP_PCT_DEFAULT)
        allowed = max(cap_min, cap_pct / 100.0 * len(oldmap))
        if len(added) > allowed:
            fs.setdefault("growth_held", {})[flow] = {"added": len(added), "allowed": allowed,
                                                     "stored": len(oldmap)}
            fs["published"].append(flow)
            save_state(out_dir, st)
            oc.notes.append(f"{flow}: annual revision NOT merged - it would ADD {len(added):,} row(s) under "
                            f"served ids against {len(oldmap):,} stored in the re-pulled months (bound "
                            f"{allowed:,.0f}); held for approval ({counts})")
            continue
        if not ctx.mark_merging(tid):
            oc.transient.append(f"{flow}: annual revision pre-merge owed mark not saved - not merged")
            return
        m0 = time.time()
        try:
            n, md = merge.merge_and_write(path, new, mode="merge", dedup_keys=dk)
        except (DefinitiveError, ValueError) as e:
            ctx.mark_cancel(tid)
            oc.transient.append(f"{flow}: annual revision merge refused ({e!r})")
            return
        finally:
            ctx.note_merge(time.time() - m0)
        ctx.mark_owed(tid)
        if store_only:
            fs["held_rows"][flow] = {"count": len(store_only),
                                     "sample": ["|".join("" if x is None else x for x in k)
                                                for k in sorted(store_only, key=str)[:SAMPLE_KEYS]]}
        fs["published"].append(flow)
        oc.published.append(flow)
        iso = md if isinstance(md, str) else (md.isoformat() if md else None)
        if iso and oc.cursors.get(tid, "") < iso:
            oc.cursors[tid] = iso
        save_state(out_dir, st)
        oc.notes.append(f"{flow}: annual revision published {len(months)} month(s): {counts}")
        print(f"[census] {flow}: annual revision published {new.num_rows:,} row(s) over "
              f"{len(months)} month(s); {counts}", flush=True)
    fs["phase"] = "done"
    fs["completed"] = fs.get("completed") or today.isoformat()


def _note_holds(oc, fam, fs):
    held = fs.get("held_rows") or {}
    months = sum(len(v) for v in (fs.get("holds") or {}).values())
    n = sum(v["count"] for v in held.values())
    if n:
        oc.notes.append(f"{fam}: annual revision cycle {fs['cycle']} HELD {n:,} store-only row(s) in "
                        f"re-pulled months across {len(held)} flow(s) - NOT deleted (owner decision); "
                        f"they are the old vintage beside revised rows, so those months' details may not "
                        f"sum to the revised TOTAL")
    if months:
        oc.notes.append(f"{fam}: {months} stored month(s) answered no rows on re-pull - kept as stored")
    for kind, what in (("failed_flows", "failed on consecutive passes"),
                       ("growth_held", "would add rows over the bound"),
                       ("label_conflicts", "differ only in a label column")):
        if fs.get(kind):
            oc.notes.append(f"{fam}: annual revision HELD {len(fs[kind])} flow(s) unmerged that {what}: "
                            f"{sorted(fs[kind])[:6]} - owner decision")
