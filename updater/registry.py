"""Registry loader + validator + Unit materialization.

The registry is the authoritative assignment of one strategy per source. The
validator FAILS LOUDLY (used as a CI gate) if coverage is incomplete or any source
lacks a valid strategy — that's what guarantees no source can silently go unmanaged.
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


def validate(reg: dict, expected_count: int | None = None) -> list[str]:
    """Return a list of problems (empty = valid)."""
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
    if expected_count is not None and len(sources) != expected_count:
        problems.append(f"expected {expected_count} sources, found {len(sources)}")
    return problems


def to_units(entry: dict) -> list[Unit]:
    """Materialize an entry into refresh Units. A source with an explicit `units`
    list (e.g. central_banks -> boc/snb/...) yields one Unit each; otherwise a
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
                "out_dir": entry.get("out_dir", sid)}
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
