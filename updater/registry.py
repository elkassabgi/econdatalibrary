"""Registry loader + validator + Unit materialization.

The registry is the authoritative assignment of one strategy per source. The
validator FAILS LOUDLY (used as a CI gate) if coverage is incomplete or any source
lacks a valid strategy — that's what guarantees no source can silently go unmanaged.

Entries may carry an optional `live: true` flag — the rollout perimeter lives in
DATA, never in a Python source list (the whack-a-mole pattern reborn). A live
source with no runnable adapter is a RUN FAILURE in the orchestrator, not a skip
(UPDATER_BUILD_PLAN.md §1.3/§5.3).
"""
from __future__ import annotations
import os

import yaml

from . import config
from .strategies.base import Unit

VALID_STRATEGIES = {
    "overwrite_if_changed", "extend_by_date", "sdmx_delta",
    "giant_changed_units", "bulk_snapshot_if_changed", "manual_vintage",
}


def load(path: str | None = None) -> dict:
    path = path or config.REGISTRY
    if not os.path.exists(path):
        raise SystemExit(f"registry not found at {path} — run `python -m updater.gen_registry`")
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


CSV_GRAINS = {"series", "flow", "table"}
# `csv_bulk` (optional): how tools/derive_csv_bulk.py must be run to derive this source's catalogue ids.
# The one value: "qualify_with_shard" (fed_board) - with --qualify-with-shard --only-catalogued.
CSV_BULK = {"qualify_with_shard"}
# `csv_owed_repay` (optional, fed_board): "fresh" = the CSV phase derives the source's PAYABLE csv_desktop_owed
# ids first and clears every id it derived (orchestrate._owed_repay_ids / _pay_owed_by_put, DESIGN_v2 §2-§3).
CSV_OWED_REPAY = {"fresh"}
# `csv_ids` (optional, fhfa): "shard_qualified" = every catalogue id is '<src>:<shard>:<key>' and the fetcher's
# changed keys are '<shard>:<key>' - the property csv_owed_repay's payable rule reads - for a source the bulk
# tool CANNOT derive (fhfa is served wide: derive_csv_bulk exits 2), so it must not declare csv_bulk.
CSV_IDS = {"shard_qualified"}


def csv_misses_from_utc(v):
    """`csv_misses_from` as a full UTC stamp in state.now_utc()'s format ('YYYY-MM-DDTHH:MM:SS+00:00'), or None.

    Accepted: a STRING holding an ISO date 'YYYY-MM-DD' (= its 00:00:00 UTC) or exactly that full stamp. A
    bare YAML date (`csv_misses_from: 2026-11-10`, unquoted) loads as datetime.date and is refused: compared
    with a stored stamp it raises TypeError (AR-458 r2 (b)2). The hold compares at FULL precision (AR-461 F2a):
    with a date, a row stamped ON that date is not legacy, so a date must be a day AFTER the deploy."""
    import datetime as _dt
    if not isinstance(v, str):
        return None
    try:
        if len(v) == 10:
            return (v + "T00:00:00+00:00") if _dt.date.fromisoformat(v).isoformat() == v else None
        if len(v) == 25 and v.endswith("+00:00"):
            return v if _dt.datetime.fromisoformat(v).isoformat(timespec="seconds") == v else None
    except ValueError:
        return None
    return None
# Who writes a source's served CSVs. Absent = the orchestrator's CSV phase (every source but one).
# "lane" = a continuous job of the source's own (jobs/statcan_lane.py), which holds the region's
# writer lock; the orchestrator then derives nothing for it (orchestrate._served_by_lane).
SERVED_BY = {"lane"}
# `desktop_lane` (optional): "skip" = the scheduled desktop pass does not attempt this source (only an explicit
# request runs it, orchestrate._giant_lane_skip); "last" = it is ordered after every other unit
# (orchestrate.order_units). Needs `desktop_lane_exit` (see validate).
DESKTOP_LANES = {"skip", "last"}
"""`csv_grain` (optional registry field; default `series`). `flow` declares that one catalogue
id serves a WHOLE store file (the resolver reads the file, not a keyed slice), as eurostat's
`eurostat:<dataset>` ids do. `table` declares that one catalogue id serves every store key under its
table prefix inside a SHARED store file (the resolver's prefix arm: dst's `dst:DST:<TABLE>` reads the
`DST:<TABLE>:...` keys of its subject parquet), so neither the series-grain bulk campaign (one object per
store key - R498; dst measured 2026-10-19: 959,014 store keys against 2,264 table ids) nor the whole-file flow path is right for it.
`table` is read by exactly two things: the health gate's remedy text and tools/derive_csv_bulk.py's campaign
refusal; the orchestrator compares only `== "flow"`, so `table` derives exactly as `series` there
(tests/test_flow_grain_remedy_hazard.py pins it). Every `econdl._resolve._FLOW_GRAIN` member declares it
(same test). Things that key on `flow`: the health gate's remedy text for a
`full_rederive_owed` row (the series-grain bulk tool writes 0 objects for such a source, R882), the bulk
tool's campaign refusal, and the cloud derive's in-memory row ceiling (a whole-file CSV can be 8M+ rows). Pinned to the
resolver by `tests/test_csv_grain_field.py`, so the registry cannot claim a grain the resolver
does not have (the R875 drift class: three grain registries already exist)."""


_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _paused_run_members(root: str | None) -> dict:
    """{workflow file: set of source ids it runs} for the two runs outside the registry's own flags - the
    updater-heavy matrix (its ALL='[...]' list) and sec-edgar-daily.yml - the same reading as catalog/gen_site.py
    load_wiring. A missing file contributes nothing (a test registry, a checkout without workflows)."""
    import json as _json                                                 # noqa: PLC0415
    import re as _re                                                     # noqa: PLC0415
    root = _REPO if root is None else root
    out = {}
    heavy = os.path.join(root, ".github", "workflows", "updater-heavy.yml")
    if os.path.isfile(heavy):
        m = _re.search(r"ALL='(\[[^']*\])'", open(heavy, encoding="utf-8").read())
        try:
            out["updater-heavy.yml"] = set(_json.loads(m.group(1))) if m else set()
        except ValueError:
            out["updater-heavy.yml"] = set()
    sec = os.path.join(root, ".github", "workflows", "sec-edgar-daily.yml")
    if os.path.isfile(sec):
        out["sec-edgar-daily.yml"] = set(_re.findall(r"sec_edgar(?:_xbrl)?", open(sec, encoding="utf-8").read()))
    return out


def validate(reg: dict, expected_count: int | None = None, workflows_root: str | None = None) -> list[str]:
    """Return a list of problems (empty = valid). `workflows_root` is where .github/workflows is read for the
    paused check (default: this checkout)."""
    problems = []
    sources = reg.get("sources", [])
    seen = set()
    for e in sources:
        sid = e.get("source_id")
        if not sid:
            problems.append(f"entry missing source_id: {e}")
            continue
        if sid in seen:
            problems.append(f"duplicate source_id: {sid}")
        seen.add(sid)
        if e.get("strategy") not in VALID_STRATEGIES:
            problems.append(f"{sid}: invalid/missing strategy {e.get('strategy')!r}")
        if not e.get("cadence"):
            problems.append(f"{sid}: missing cadence")
        if "live" in e and not isinstance(e.get("live"), bool):
            # the live tier gates run-failure semantics — a typo like live: "yes"
            # must not silently widen or narrow the perimeter
            problems.append(f"{sid}: live flag must be boolean, got {e.get('live')!r}")
        if "csv_grain" in e and e.get("csv_grain") not in CSV_GRAINS:
            # csv_grain selects which derive tool the health gate names for an owed
            # re-derive and which ids the cloud derive may materialise in memory. An
            # unknown value must not silently default to series (R882: the series-grain
            # remedy on a flow-grain source writes 0 objects and leaves the debt standing).
            problems.append(f"{sid}: csv_grain must be one of {sorted(CSV_GRAINS)}, "
                            f"got {e.get('csv_grain')!r}")
        if "csv_merge_served" in e and not isinstance(e.get("csv_merge_served"), bool):
            # csv_merge_served: true merges each CSV upload with the served object (R1136). A typo
            # like "yes" must not silently switch it off - the derive reads `is True`.
            problems.append(f"{sid}: csv_merge_served must be boolean, got {e.get('csv_merge_served')!r}")
        if "csv_derive_workers" in e:
            # csv_derive_workers: N runs the CSV phase (and the retry drain) of THIS source with N threads
            # and an R2 connection pool of N + 8 (updater/derive.py _put_store; fed_board 24, DESIGN_v2
            # §1b). An int in 1..64 only: a string or a bool must not silently fall back to 8, and 64 is the
            # most threads any derive here has run (bea's bulk tool).
            w = e.get("csv_derive_workers")
            if not isinstance(w, int) or isinstance(w, bool) or not 1 <= w <= 64:
                problems.append(f"{sid}: csv_derive_workers must be an int in 1..64, got {w!r}")
        if "csv_bulk" in e and e.get("csv_bulk") not in CSV_BULK:
            # csv_bulk: qualify_with_shard declares that tools/derive_csv_bulk.py with --qualify-with-shard
            # --only-catalogued derives this source's catalogue ids exactly (fed_board: AR-458 J1, 12,746 of
            # 12,746 equal to the resolver). Read by health's remedy text for a full_rederive_owed row
            # (fed_board's cursor-cap backstop), so a typo must not fall through to another source's remedy.
            problems.append(f"{sid}: csv_bulk must be one of {sorted(CSV_BULK)}, got {e.get('csv_bulk')!r}")
        if "csv_misses" in e and e.get("csv_misses") != "desktop_owed":
            # csv_misses: desktop_owed books a series-grain source's missed CSV ids (budget-deferred,
            # failed, crash, fence trip) PER ID in csv_desktop_owed; changed keys that cannot be mapped
            # at all go to one full_rederive_owed row listing them (R1131, R1137, R1149). The only
            # value; a typo must not silently fall back to the retry queue that cannot drain on r2.
            problems.append(f"{sid}: csv_misses must be 'desktop_owed', got {e.get('csv_misses')!r}")
        if "csv_misses_from" in e:
            # csv_misses_from: '<ISO date or UTC stamp>' HOLDS csv_misses routing OFF while csv_retry_queue holds a
            # row of this source enqueued before it (orchestrate._csv_misses_holds, DESIGN_v2 §7). QUOTED: a bare
            # YAML date is a datetime.date, and the run-time hold would refuse it anyway (it fails closed).
            if csv_misses_from_utc(e.get("csv_misses_from")) is None:
                problems.append(f"{sid}: csv_misses_from must be a QUOTED ISO date 'YYYY-MM-DD' or UTC stamp "
                                f"'YYYY-MM-DDTHH:MM:SS+00:00', got {e.get('csv_misses_from')!r}")
            if e.get("csv_misses") != "desktop_owed":
                problems.append(f"{sid}: csv_misses_from holds csv_misses routing, so it needs csv_misses: desktop_owed")
        if "csv_owed_repay" in e:
            # csv_owed_repay: fresh derives the payable owed ids first. Its payable rule reads a catalogue id as
            # '<src>:<shard>:<key>' against changed keys '<shard>:<key>' - exactly what csv_bulk: qualify_with_shard
            # declares - and it pays csv_misses debts, so both are required.
            if e.get("csv_owed_repay") not in CSV_OWED_REPAY:
                problems.append(f"{sid}: csv_owed_repay must be one of {sorted(CSV_OWED_REPAY)}, "
                                f"got {e.get('csv_owed_repay')!r}")
            if e.get("csv_misses") != "desktop_owed" or (
                    e.get("csv_bulk") != "qualify_with_shard" and e.get("csv_ids") != "shard_qualified"):
                problems.append(f"{sid}: csv_owed_repay needs csv_misses: desktop_owed and "
                                f"csv_bulk: qualify_with_shard or csv_ids: shard_qualified")
        if "csv_ids" in e:
            if e.get("csv_ids") not in CSV_IDS:
                problems.append(f"{sid}: csv_ids must be one of {sorted(CSV_IDS)}, got {e.get('csv_ids')!r}")
            if "csv_bulk" in e:
                # The two describe different tools' reach: csv_bulk says the bulk tool derives the ids, csv_ids
                # says it cannot. Both on one entry would let health name the wrong remedy.
                problems.append(f"{sid}: csv_ids is for a source the bulk tool cannot derive; drop csv_bulk")
        if "csv_desktop_exclude" in e:
            # Catalogue ids unserved BY DECISION: never booked as a desktop debt (no derive will
            # ever pay it). Each must be a full catalogue id of THIS source.
            ex = e.get("csv_desktop_exclude")
            if not isinstance(ex, list) or not all(isinstance(x, str) and x.startswith(f"{sid}:")
                                                   for x in ex):
                problems.append(f"{sid}: csv_desktop_exclude must be a list of '{sid}:<id>' "
                                f"strings, got {ex!r}")
        if "paused" in e:
            # `paused: "<reason>"` marks a source taken OFF the run on purpose (it was wired and has run): the
            # site says "paused - <reason>" instead of "live" or "not yet wired" (catalog/gen_site.py
            # refresh_rows). The reason is shown to users, so it must be a sentence, and NO run may still pick a
            # paused source up (AR-345 finding 5): not the cloud daily (live), not the workstation update pass
            # (run_location: local, tools/_list_local_sources.py), not the heavy matrix, not sec-edgar-daily.
            pz = e.get("paused")
            if not isinstance(pz, str) or not pz.strip():
                problems.append(f"{sid}: paused must be a non-empty reason string, got {pz!r}")
            if e.get("live"):
                problems.append(f"{sid}: paused and live: true together - a paused source is live: false")
            if e.get("run_location") == "local":
                problems.append(f"{sid}: paused and run_location: local together - the workstation update pass "
                                f"still runs it")
            for wf, run in _paused_run_members(workflows_root).items():
                if sid in run:
                    problems.append(f"{sid}: paused, but {wf} still runs it")
        if "served_by" in e and e.get("served_by") not in SERVED_BY:
            # `served_by: lane` takes the orchestrator's CSV phase and retry drain OFF a source whose
            # own continuous job serves it (statcan, 2026-09-23). A typo must not silently put a
            # second writer back on the served objects.
            problems.append(f"{sid}: served_by must be one of {sorted(SERVED_BY)}, "
                            f"got {e.get('served_by')!r}")
        if "rotating" in e:
            # `rotating: true` declares a budget ROTATOR whose passes end `partial` by design and so never
            # stamp a success (R231): the public "late" rule (core/freshness_vocab.py) judges it on its
            # LATEST pass instead - not late while that pass refreshed it and ran within the window.
            # A declaration that can hide staleness, so it is narrow: boolean true only, never on a
            # paused source, and tests/test_freshness_vocab.py requires the fetcher to rotate.
            if e.get("rotating") is not True:
                problems.append(f"{sid}: rotating must be true (or absent), got {e.get('rotating')!r}")
            if "paused" in e:
                problems.append(f"{sid}: rotating and paused together - a paused source does not run")
        if "window_stop" in e:
            # `window_stop: true` declares that this source's fetcher stops at the pass's run ceiling on its
            # own (it reads orchestrate._RUN_DEADLINE_TS) and still hands its work over. The start gate
            # then admits it with the reserve the fetcher declares (window_admission_reserve_min()), not
            # the 90-min worst case. Boolean true only: a typo like "yes" must not silently shrink a
            # reserve. tests/test_admit_window_stoppable.py pins the declared set against the fetchers.
            if e.get("window_stop") is not True:
                problems.append(f"{sid}: window_stop must be true (or absent), got {e.get('window_stop')!r}")
        if "desktop_lane" in e:
            # A MITIGATION for windowless UNCTAD giants that held the desktop pass to its hard stop (review AR-467):
            #   skip - the SCHEDULED desktop pass does not attempt the unit (orchestrate._giant_lane_skip); an
            #          explicit request still does (run_local_heavy.ps1 -Only <sid>, or updater.run --source <sid>
            #          without --scheduled). For units MEASURED as unable to finish a pass: their kills wrote no
            #          spill chunk, so a turn is a lost pass with zero progress.
            #   last - the unit is ORDERED after every other unit (orchestrate.order_units), so it is admitted only
            #          after every other unit has run or been judged. For a unit still making durable progress.
            # Fail-closed: exactly one of these two strings, only on a run_location: local source, never with
            # window_stop (a fetcher that stops at the run ceiling bounds itself), and ALWAYS with
            # `desktop_lane_exit`: a sentence naming the owner and the condition that removes the flag - a
            # protection that outlives what it protects becomes the outage (orchestrate.py FIRSTPASS_DIRS note).
            # tests/test_giant_lane.py pins the flagged set and every refusal here.
            dx = e.get("desktop_lane_exit")
            if not isinstance(dx, str) or not dx.strip():
                problems.append(f"{sid}: desktop_lane needs desktop_lane_exit - a non-empty sentence naming the "
                                f"owner and the condition that removes the flag, got {dx!r}")
            if e.get("desktop_lane") not in DESKTOP_LANES:
                problems.append(f"{sid}: desktop_lane must be one of {sorted(DESKTOP_LANES)}, "
                                f"got {e.get('desktop_lane')!r}")
            if e.get("run_location") != "local":
                problems.append(f"{sid}: desktop_lane needs run_location: local (only the desktop pass reads it)")
            if e.get("window_stop") is True:
                problems.append(f"{sid}: desktop_lane and window_stop together - a window-stoppable fetcher "
                                f"bounds itself and belongs in the scheduled pass")
        elif "desktop_lane_exit" in e:
            problems.append(f"{sid}: desktop_lane_exit without desktop_lane - remove both together")
    if expected_count is not None and len(sources) != expected_count:
        problems.append(f"expected {expected_count} sources, found {len(sources)}")
    return problems


def to_units(entry: dict) -> list[Unit]:
    """Materialize an entry into refresh Units. A source with an explicit `units`
    list (e.g. a multi-provider entry -> boc/snb/...) yields one Unit each; otherwise a
    single implicit `_all` unit covering the source dir.

    Per-flow giants (giant_changed_units: eurostat, oecd, ...) keep yielding the
    single `_all` source-dir Unit here: their thousands of per-flow units are not
    known at registry-load time (selecting them requires a *live* catalog download +
    diff). The strategy itself enumerates and addresses each changed flow at run
    time via `flow_unit()` below — which builds a per-flow Unit from the same source
    entry, fully backward-compatibly (no existing caller passes `flow_*`)."""
    sid = entry["source_id"]
    strat = entry["strategy"]
    cadence = entry.get("cadence", "monthly")
    base_cfg = {"matrix": entry.get("matrix", {}), "script": entry.get("script"),
                "refresh_cost": entry.get("refresh_cost"),
                "out_dir": entry.get("out_dir", sid),
                # live tier flag rides in unit config so the orchestrator can apply
                # the no-silent-skip rule without re-reading the registry entry
                "live": bool(entry.get("live", False)),
                # WHERE this source may execute. Rides in unit config for the same reason
                # `live` does: the orchestrator must be able to enforce it without re-reading
                # the registry. It was previously a registry key that NOTHING read - only two
                # local tools - so the routing was inert. 13 sources carry it; 12 were
                # incidentally kept out of CI by live:false, and the one that was live,
                # `ons_uk`, destroyed the runner. A routing label no code consults is a
                # comment, not a routing decision - and it only looks like it works for as
                # long as something else happens to be blocking the traffic.
                "run_location": entry.get("run_location") or "any",
                # The start gate's window-stop declaration (see validate): `is True` only.
                "window_stop": entry.get("window_stop") is True,
                # The scheduled pass's skip declaration (see validate): the validated value, else None.
                "desktop_lane": entry.get("desktop_lane") if entry.get("desktop_lane") in DESKTOP_LANES else None,
                "desktop_lane_exit": entry.get("desktop_lane_exit") if entry.get("desktop_lane") in DESKTOP_LANES
                else None}
    units_spec = entry.get("units")
    if units_spec:
        out = []
        for u in units_spec:
            out_paths = [os.path.join(config.DATA_ROOT, p) for p in u.get("out_paths", [])]
            out.append(Unit(source_id=sid, unit_id=u["unit_id"], strategy=u.get("strategy", strat),
                            cadence=u.get("cadence", cadence), out_paths=out_paths,
                            config={**base_cfg, **u.get("config", {})}))
        return out
    out_dir = os.path.join(config.DATA_ROOT, entry.get("out_dir", sid))
    return [Unit(source_id=sid, unit_id="_all", strategy=strat, cadence=cadence,
                 out_paths=[out_dir], config=base_cfg)]


def flow_unit(parent: Unit, flow_id: str, out_filename: str, *, flow_cfg: dict | None = None) -> Unit:
    """Materialize ONE per-flow sub-Unit of a giant source's `_all` Unit, at run time.

    A giant (eurostat/oecd) owns a directory of thousands of per-flow parquets. After
    the strategy diffs the upstream catalog it calls this to address a single CHANGED
    flow as its own Unit — `unit_id = flow_id`, `out_paths = [<source_dir>/<file>]` —
    so per-flow merge/never-shrink/state operate on exactly one file. Backward
    compatible: nothing in to_units()/all_units() emits these; only the strategy does,
    so the orchestrator's one-`_all`-unit-per-giant contract is unchanged."""
    out_dir = (parent.out_paths or [None])[0]
    if out_dir is None:
        out_dir = os.path.join(config.DATA_ROOT, (parent.config or {}).get("out_dir", parent.source_id))
    out_path = os.path.join(out_dir, out_filename)
    cfg = dict(parent.config or {})
    if flow_cfg:
        cfg.update(flow_cfg)
    return Unit(source_id=parent.source_id, unit_id=flow_id, strategy=parent.strategy,
                cadence=parent.cadence, out_paths=[out_path], config=cfg)


def all_units(path: str | None = None) -> list[Unit]:
    reg = load(path)
    units = []
    for e in reg.get("sources", []):
        units.extend(to_units(e))
    return units
