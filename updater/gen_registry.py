"""Generate registry.yaml from UPDATE_CAPABILITY_MATRIX.json.

Assigns each source a DEFAULT strategy via a (mechanism, incremental, cost,
detection) decision table, carrying the matrix's force-refresh / detection /
incremental notes forward as config seed. The defaults are a starting point —
each strategy assignment is reviewed and pinned as its adapter is implemented and
verified (Phase 3/4). The validator (registry.py) guarantees full coverage.

Run:  python -m updater.gen_registry            # writes updater/registry.yaml
      python -m updater.gen_registry --print    # preview distribution only
"""
from __future__ import annotations
import json
import os
import sys

import yaml

from . import config

# canonical strategy ids (S-number in comments)
S1 = "overwrite_if_changed"      # S1
S2 = "extend_by_date"            # S2
S3 = "sdmx_delta"                # S3
S4 = "giant_changed_units"       # S4
S5 = "bulk_snapshot_if_changed"  # S5
S6 = "manual_vintage"            # S6

_SDMX_HINT = ("sdmx", "updatedafter", "startperiod")
_DATE_HINT = ("since=", "startperiod", "observation_start", "record_date",
              "datainicial", "mindate", "/{start}", "observation_start", "cosd")
_BULK_HINT = ("bulk", "zip", ".csv", "file-date", "last-modified", "content-length",
              "manifest", "filerows", "filesize", "full-pull-only", "snapshot")
_MANUAL_HINT = ("403", "waf", "hardcoded", "offset ceiling", "manual", "browser header",
                "credential", "eherkenning", "blocked")


def assign_strategy(p: dict) -> tuple[str, str]:
    """Return (strategy_id, reason)."""
    incr = (p.get("supports_incremental") or "").lower()
    cost = (p.get("refresh_cost") or "").lower()
    sid = (p.get("source_id") or "").lower()
    ndd = (p.get("new_data_detection") or "").lower()
    ipath = (p.get("incremental_path") or "").lower()
    blk = (p.get("keys_or_blockers") or "").lower()
    risks = (p.get("risks") or "").lower()

    if any(h in blk for h in _MANUAL_HINT) or "hardcoded" in ndd or "hardcoded" in ipath:
        return S6, "blocked/hardcoded vintage -> manual alert"
    if cost == "giant":
        return S4, "giant -> changed-unit refresh"
    if "sdmx" in sid or any(h in ipath for h in _SDMX_HINT) or "sdmx" in ndd:
        return S3, "SDMX delta (updatedAfter/startPeriod)"
    if incr in ("yes", "partial") or any(h in ipath for h in _DATE_HINT):
        return S2, "native date filter -> since=last_obs"
    if cost in ("large", "medium") and any(h in ndd for h in _BULK_HINT):
        return S5, "bulk file -> snapshot-if-changed"
    return S1, "overwrite-if-changed (vintage-gated)"


def build() -> dict:
    from collections import defaultdict
    matrix = json.load(open(config.MATRIX_JSON, encoding="utf-8"))
    by_src = defaultdict(list)
    for p in matrix.get("profiles", []):
        by_src[p.get("source_id")].append(p)

    cls_path = os.path.join(config.ROOT, "updater", "_classifications.json")
    cls = json.load(open(cls_path, encoding="utf-8")) if os.path.exists(cls_path) else {}

    entries = []
    for sid, ps in by_src.items():
        c = cls.get(sid)
        if c:  # authoritative agent classification
            strat, reason = c["strategy"], c.get("strategy_reason", "")
            cadence = c.get("cadence") or "monthly"
            adapter = {k: c.get(k) for k in ("vintage_signal", "since_param", "out_paths_note",
                                             "rate_note", "key_env", "confidence", "open_question")}
        else:  # heuristic fallback (should not happen once classified)
            strat, reason = assign_strategy(ps[0])
            cadence = ps[0].get("cadence") or "monthly"
            adapter = {}
        entries.append({
            "source_id": sid,
            "scripts": [p.get("script") for p in ps],
            "strategy": strat,
            "strategy_reason": reason,
            "cadence": cadence,
            "refresh_cost": ps[0].get("refresh_cost"),
            "out_dir": sid,            # relative to DATA_ROOT; refined per-source where source!=dir
            "review": True,            # flips to False once the adapter is implemented+verified
            "adapter": adapter,
            "matrix": {
                "refresh_mechanism": [p.get("refresh_mechanism") for p in ps],
                "force_refresh_procedure": [p.get("force_refresh_procedure") for p in ps],
                "incremental_path": [p.get("incremental_path") for p in ps],
                "keys_or_blockers": [p.get("keys_or_blockers") for p in ps],
                "storage_layout": [p.get("storage_layout") for p in ps],
            },
        })
    entries.sort(key=lambda e: e["source_id"])
    return {"version": 1, "generated_from": "matrix + classifications", "sources": entries}


def main():
    reg = build()
    from collections import Counter
    dist = Counter(e["strategy"] for e in reg["sources"])
    print(f"sources: {len(reg['sources'])}")
    for k, v in sorted(dist.items()):
        print(f"  {k}: {v}")
    if "--print" in sys.argv:
        return
    with open(config.REGISTRY, "w", encoding="utf-8") as f:
        yaml.safe_dump(reg, f, sort_keys=False, allow_unicode=True, width=120)
    print(f"wrote {config.REGISTRY}")


if __name__ == "__main__":
    main()
