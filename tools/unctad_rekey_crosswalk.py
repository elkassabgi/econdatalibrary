"""Value-verify a legacy unctad_* source against a candidate modern successor.

WHY THIS EXISTS. The 38 legacy `unctad_*` sources (127,413 series) carry relay-era
title-acronym ids (`tabbapotta`, `gasbtoia`) that no longer exist upstream. Ahmed
authorised re-keying them (2026-08-09). Reading the titles suggests most are not
re-keys at all but DUPLICATES of sources we already serve under UNCTAD's current
`US.*` codes — and several candidate pairs have suspiciously close series counts
(gasbtoia 6,776 vs goodsandservtradeopennessbpm6 6,672).

R403 is the reason this file is not a title-matcher. On fao_gt a topic-and-id match
looked conclusive and the VALUES refuted it: the two sides were different bases and
merging them would have published a series that never existed. A close count is a
weaker signal than that one was. So the verdict here is computed from observations
only; titles pick the candidate, values decide.

METHOD. Both stores are {series_key, obs_date, value}. For every legacy series we
look for modern series that agree on the periods they share:

  - at least MIN_SHARED shared dates (default 3) — two points can coincide by luck
  - agreement on >= FLOOR of them (default 0.90) within RTOL relative tolerance
  - the best candidate must beat the runner-up by MARGIN (default 0.05), else the
    legacy series is AMBIGUOUS and is reported, never guessed

Verdicts, per legacy series: MATCHED / AMBIGUOUS / UNMATCHED / NO-OVERLAP.
The source-level verdict is deliberately conservative — see classify().

This tool only REPORTS. It writes nothing to the catalogue, the store or D1; the
re-key itself is a separate, explicitly-allowlisted step.

Usage:
  python tools/unctad_rekey_crosswalk.py --legacy unctad_gasbtoia \
      --candidate unctad_goodsandservtradeopennessbpm6
  python tools/unctad_rekey_crosswalk.py --pairs pairs.json --json out.json
"""
from __future__ import annotations

import argparse
import collections
import json
import math
import os
import sys

import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STORE = os.path.join(ROOT, "data", "clean_full")

MIN_SHARED = 3
FLOOR = 0.90
MARGIN = 0.05
RTOL = 1e-6


def _period(dates: list) -> list:
    """Map dates to PERIOD keys, not days.

    MEASURED 2026-08-09, and the reason this function exists: the relay-era stores
    stamp an annual observation at the period START (2005-01-01) while the modern
    UNCTAD stores stamp the same observation at the period END (2005-12-31). Keying
    on the raw date makes every cross-era comparison return NO-OVERLAP — the first
    run of this tool duly declared 6,776 of 6,776 series "REFUTED (different data)"
    when the two sides had simply never been given a chance to line up. Only the
    self-comparison control exposed it.

    Frequency is inferred from the median gap inside the series rather than assumed,
    so quarterly and monthly sources collapse to their own period, not to the year.
    """
    if len(dates) < 2:
        return [d.year if hasattr(d, "year") else str(d) for d in dates]
    ords = sorted(d.toordinal() for d in dates if hasattr(d, "toordinal"))
    if len(ords) < 2:
        return [str(d) for d in dates]
    gaps = sorted(b - a for a, b in zip(ords, ords[1:]))
    med = gaps[len(gaps) // 2] or 365
    out = []
    for d in dates:
        if not hasattr(d, "year"):
            out.append(str(d))
        elif med >= 200:                      # annual
            out.append(("Y", d.year))
        elif med >= 60:                       # quarterly
            out.append(("Q", d.year, (d.month - 1) // 3))
        elif med >= 20:                       # monthly
            out.append(("M", d.year, d.month))
        else:
            out.append(("D", d.toordinal()))
    return out


def load(source_id: str) -> dict[str, dict]:
    """series_key -> {period: value} for every parquet shard in the store dir.

    Reads EVERY shard (R364: a store dir can hold more than one file, and taking
    just the first silently halves the evidence).
    """
    d = os.path.join(STORE, source_id)
    if not os.path.isdir(d):
        raise SystemExit(f"no store dir: {d}")
    shards = sorted(f for f in os.listdir(d) if f.endswith(".parquet"))
    if not shards:
        raise SystemExit(f"no parquet in {d}")
    raw: dict[str, list] = collections.defaultdict(list)
    for shard in shards:
        t = pq.read_table(os.path.join(d, shard), columns=["series_key", "obs_date", "value"])
        for k, dt, v in zip(t.column("series_key").to_pylist(),
                            t.column("obs_date").to_pylist(),
                            t.column("value").to_pylist()):
            if v is None or dt is None:
                continue
            try:
                raw[k].append((dt, float(v)))
            except (TypeError, ValueError):
                continue
    out: dict[str, dict] = {}
    for k, obs in raw.items():
        periods = _period([d for d, _ in obs])
        out[k] = {p: v for p, (_, v) in zip(periods, obs)}
    return out


def agree(a: float, b: float) -> bool:
    if a == b:
        return True
    scale = max(abs(a), abs(b))
    return scale > 0 and abs(a - b) <= RTOL * scale


def score(lhs: dict[str, float], rhs: dict[str, float]) -> tuple[int, float]:
    """(shared points, fraction agreeing). Only periods present on BOTH sides."""
    shared = lhs.keys() & rhs.keys()
    if not shared:
        return 0, 0.0
    ok = sum(1 for d in shared if agree(lhs[d], rhs[d]))
    return len(shared), ok / len(shared)


def crosswalk(legacy: dict, cand: dict) -> dict:
    """Index candidates by value-fingerprint so this is not O(n*m) over 24k series.

    Two series can only agree everywhere they overlap if they agree at the earliest
    shared date, so bucketing on (date, rounded value) pairs prunes almost all of
    the cross product without being able to drop a true match.
    """
    index: dict[tuple, list[str]] = collections.defaultdict(list)
    for key, obs in cand.items():
        for dt, v in obs.items():
            index[(dt, round(v, 9))].append(key)

    verdicts = {"MATCHED": [], "AMBIGUOUS": [], "UNMATCHED": [], "NO-OVERLAP": [],
                "TOO-SHORT": []}
    mapping: dict[str, str] = {}
    for lkey, lobs in legacy.items():
        if len(lobs) < MIN_SHARED:
            verdicts["TOO-SHORT"].append(lkey)
            continue
        pool = collections.Counter()
        for dt, v in lobs.items():
            for ckey in index.get((dt, round(v, 9)), ()):
                pool[ckey] += 1
        if not pool:
            verdicts["NO-OVERLAP"].append(lkey)
            continue
        scored = []
        for ckey, _hits in pool.most_common(50):
            n, frac = score(lobs, cand[ckey])
            if n >= MIN_SHARED and frac >= FLOOR:
                scored.append((frac, n, ckey))
        if not scored:
            verdicts["UNMATCHED"].append(lkey)
            continue
        scored.sort(reverse=True)
        best = scored[0]
        if len(scored) > 1 and (best[0] - scored[1][0]) < MARGIN:
            verdicts["AMBIGUOUS"].append(lkey)
            continue
        verdicts["MATCHED"].append(lkey)
        mapping[lkey] = best[2]
    return {"verdicts": verdicts, "mapping": mapping}


def classify(n_legacy: int, v: dict) -> str:
    """Source-level verdict.

    Two DIFFERENT questions are being answered and they need different bars, which
    the control run made obvious — comparing a source with ITSELF returned 328
    AMBIGUOUS, because a source legitimately contains series whose value vectors are
    identical. Ambiguity is fatal for a re-key (you cannot choose which target to
    rewrite to) but harmless for a retirement (the observations demonstrably exist
    on the other side either way).

      covered  = MATCHED + AMBIGUOUS   -> is retiring the legacy id lossless?
      keyable  = MATCHED               -> can each legacy id be rewritten uniquely?

    TOO-SHORT series (fewer than MIN_SHARED points) can never be judged by values;
    they are excluded from the denominator and reported separately rather than
    silently counted as failures.
    """
    m, amb = len(v["MATCHED"]), len(v["AMBIGUOUS"])
    judged = n_legacy - len(v["TOO-SHORT"])
    covered = m + amb
    if judged <= 0:
        return "UNJUDGEABLE (every series too short to value-verify)"
    if covered == judged:
        tail = "" if amb == 0 else f", {amb} only non-uniquely"
        return f"DUPLICATE (lossless to retire{tail}; {m}/{judged} uniquely re-keyable)"
    if covered == 0:
        return "REFUTED (different data — do NOT re-key)"
    return (f"PARTIAL ({covered}/{judged} covered — legacy holds "
            f"{judged - covered} series the candidate does not; KEEP)")


def run(legacy_id: str, cand_id: str) -> dict:
    legacy, cand = load(legacy_id), load(cand_id)
    res = crosswalk(legacy, cand)
    v = res["verdicts"]
    verdict = classify(len(legacy), v)
    print(f"\n{legacy_id}  ({len(legacy):,} series)  vs  {cand_id}  ({len(cand):,} series)")
    for bucket in ("MATCHED", "AMBIGUOUS", "UNMATCHED", "NO-OVERLAP", "TOO-SHORT"):
        print(f"  {bucket:<11}{len(v[bucket]):>7,}")
    print(f"  => {verdict}")
    for k in v["UNMATCHED"][:3]:
        print(f"     unmatched sample: {k[:100]}")
    return {"legacy": legacy_id, "candidate": cand_id, "n_legacy": len(legacy),
            "n_candidate": len(cand),
            "counts": {k: len(x) for k, x in v.items()}, "verdict": verdict}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--legacy")
    ap.add_argument("--candidate")
    ap.add_argument("--pairs", help="JSON [[legacy, candidate], ...]")
    ap.add_argument("--json", help="write results here")
    a = ap.parse_args()

    pairs = json.load(open(a.pairs, encoding="utf-8")) if a.pairs else [[a.legacy, a.candidate]]
    results = []
    for lg, cd in pairs:
        try:
            results.append(run(lg, cd))
        except SystemExit as e:
            print(f"\n{lg} vs {cd}: SKIPPED — {e}")
            results.append({"legacy": lg, "candidate": cd, "verdict": f"SKIPPED: {e}"})
    if a.json:
        json.dump(results, open(a.json, "w", encoding="utf-8"), indent=1)
        print(f"\nwrote {a.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
