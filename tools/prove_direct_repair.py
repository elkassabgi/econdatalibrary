"""Can source X be repaired in place from IMF direct, keeping every live series id?

The imf_commodity migration needed three facts before it was safe: which upstream
code means which of ours, which date convention the source stores, and what fraction
of live ids would survive. Getting any of them wrong is silent — a wrong code pairing
swaps two real series under ids people cite, a wrong date convention duplicates every
observation, and a wrong key format doubles the source instead of extending it.

Doing that by hand per source does not scale to the nine IMF datasets that have a
direct flow, so this derives all three instead of assuming them:

  1. MATCH SERIES BY VALUE, not by name. Upstream and our store are bucketed by
     (obs count, first date, last date) and matched on agreement of the actual
     numbers. Codes never enter into it, so a renamed vocabulary is no obstacle.
  2. DERIVE THE CODE MAP from the matched pairs — read off which upstream attribute
     value co-occurs with which component of our key.
  3. REPORT ID SURVIVAL: how many live ids the repair would preserve, how many new
     ids it would mint, and which of ours upstream no longer carries.

Both date conventions are tried and the better-scoring one wins, because that is
exactly the kind of detail that is invisible until it has already corrupted a merge.

Usage:
  python tools/prove_direct_repair.py --source imf_pctot --agency IMF.RES --flow CTOT
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import io
import json
import os
import sys
import xml.etree.ElementTree as ET

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow.parquet as pq                                  # noqa: E402
from jobs.ingest_imf_direct import http_get, BASE, NON_IDENTITY  # noqa: E402


def per_start(p):
    p = (p or "").strip()
    try:
        if len(p) == 4:
            return dt.date(int(p), 1, 1)
        if "-M" in p:
            y, m = p.split("-M"); return dt.date(int(y), int(m), 1)
        if "-Q" in p:
            y, q = p.split("-Q"); return dt.date(int(y), (int(q) - 1) * 3 + 1, 1)
        if len(p) == 7 and "-" in p:
            y, m = p.split("-"); return dt.date(int(y), int(m), 1)
        if len(p) == 10:
            return dt.date(int(p[:4]), int(p[5:7]), 1)
    except (ValueError, TypeError):
        return None
    return None


def _eom(y, m):
    return (dt.date(y + (m == 12), (m % 12) + 1, 1) - dt.timedelta(days=1))


def per_end(p):
    p = (p or "").strip()
    try:
        if len(p) == 4:
            return dt.date(int(p), 12, 31)
        if "-M" in p:
            y, m = p.split("-M"); return _eom(int(y), int(m))
        if "-Q" in p:
            y, q = p.split("-Q"); return _eom(int(y), int(q) * 3)
        if len(p) == 7 and "-" in p:
            y, m = p.split("-"); return _eom(int(y), int(m))
        if len(p) == 10:
            return dt.date(int(p[:4]), int(p[5:7]), int(p[8:10]))
    except (ValueError, TypeError):
        return None
    return None


def load_upstream(agency, flow, conv):
    raw = http_get(f"{BASE}/data/{agency},{flow}/all")
    root = ET.fromstring(raw)
    out = {}
    for s in (e for e in root.iter() if e.tag.split("}")[-1] == "Series"):
        obs = {}
        for o in s:
            if o.tag.split("}")[-1] != "Obs":
                continue
            v = o.attrib.get("OBS_VALUE")
            d = conv(o.attrib.get("TIME_PERIOD", ""))
            if v in (None, "", "NaN") or d is None:
                continue
            try:
                obs[d] = float(v)
            except ValueError:
                pass
        if obs:
            dims = tuple(sorted((k, v) for k, v in s.attrib.items()
                                if k not in NON_IDENTITY))
            out[dims] = obs
    return out, len(raw)


def load_ours(source_id):
    p = os.path.join(ROOT, "data", "clean_full", source_id, f"{source_id}.parquet")
    t = pq.read_table(p)
    out = collections.defaultdict(dict)
    for k, d, v in zip(t["series_key"].to_pylist(), t["obs_date"].to_pylist(),
                       t["value"].to_pylist()):
        out[k][d] = v
    return dict(out)


def bucket(obs):
    """Index on the FIRST observation date only.

    An earlier version keyed on (count, first, last) and matched nothing on a source
    whose series counts were identical — because the whole premise of the repair is
    that upstream is FRESHER, so the count and the last date MUST differ. Bucketing
    on them excludes exactly the pairs we are looking for. The start of history is
    the part that stays put.
    """
    return min(obs)


def agreement(a, b):
    shared = set(a) & set(b)
    if not shared:
        return 0.0, 0
    ok = sum(1 for d in shared
             if abs(a[d] - b[d]) <= max(1e-9, 1e-3 * max(abs(a[d]), abs(b[d]))))
    return ok / len(shared), len(shared)


def match(up, ours, min_rate=0.90):
    """Pair upstream series to ours by value agreement, bucketed for tractability."""
    idx = collections.defaultdict(list)
    for k, obs in ours.items():
        idx[bucket(obs)].append(k)
    pairs, unmatched = {}, []
    used = set()
    for dims, obs in up.items():
        b = bucket(obs)
        cands = list(idx.get(b, []))
        if not cands:
            # History can also be revised BACKWARDS (IMF extends a series earlier).
            # Fall back to any series whose start is within a year, then let value
            # agreement decide — never fall back on recency, which is what differs
            # by construction.
            cands = [k for bb, ks in idx.items()
                     if abs((bb - b).days) <= 366 for k in ks]
        best, best_r = None, 0.0
        for k in cands:
            if k in used:
                continue
            r, n = agreement(obs, ours[k])
            if n and r > best_r:
                best, best_r = k, r
        if best and best_r >= min_rate:
            pairs[dims] = (best, best_r)
            used.add(best)
        else:
            unmatched.append(dims)
    return pairs, unmatched


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", required=True)
    ap.add_argument("--agency", required=True)
    ap.add_argument("--flow", required=True)
    ap.add_argument("--emit", help="write the derived map as JSON for a fetcher to "
                                   "consume, so nobody retypes it by hand")
    a = ap.parse_args()

    ours = load_ours(a.source)
    print(f"{a.source}: {len(ours):,} series published")

    best = None
    for name, conv in (("period-START", per_start), ("period-END", per_end)):
        up, nbytes = load_upstream(a.agency, a.flow, conv)
        pairs, un = match(up, ours)
        print(f"  {name:<13} upstream {len(up):,} series ({nbytes:,} bytes) "
              f"-> matched {len(pairs):,}")
        if best is None or len(pairs) > len(best[2]):
            best = (name, up, pairs, un)
    name, up, pairs, un = best
    print(f"\nDATE CONVENTION: {name}  (chosen by match count, not assumed)")

    if not pairs:
        print("\nNO SERIES MATCHED — this flow is not the same data. Do not repair.")
        return 1

    # Derive the code map: which upstream attribute value goes with which slot.
    arity = len(next(iter(pairs.values()))[0].split(":", 1)[1].split("."))
    slots = collections.defaultdict(lambda: collections.defaultdict(collections.Counter))
    for dims, (key, _r) in pairs.items():
        parts = key.split(":", 1)[1].split(".")
        if len(parts) != arity:
            continue
        for dim, val in dims:
            for i, ourval in enumerate(parts):
                slots[dim][i][ourval] += 1

    print(f"\nDERIVED CODE MAP (our key has {arity} components)")
    # Slots are assigned as a BIJECTION, best-purity first. Choosing each dimension's
    # best slot independently lets two dimensions claim the same one: on imf_pctot
    # that put WGT_TYPE in slot 0 alongside FREQUENCY (both scored 100% because
    # WGT_TYPE's four values happen to partition by frequency) and left slot 3
    # unexplained. A confident-looking map with a duplicated slot is worse than no
    # map — it is the exact input a later migration would trust.
    scored = []
    for dim in sorted(slots):
        for i in slots[dim]:
            byup = collections.defaultdict(collections.Counter)
            for dims, (key, _r) in pairs.items():
                dv = dict(dims).get(dim)
                pp = key.split(":", 1)[1].split(".")
                if dv is not None and len(pp) == arity:
                    byup[dv][pp[i]] += 1
            tot = sum(sum(c.values()) for c in byup.values())
            if not tot:
                continue
            purity = sum(c.most_common(1)[0][1] for c in byup.values()) / tot
            m = {k: c.most_common(1)[0][0] for k, c in byup.items()}
            # Purity alone rewards COLLAPSE. On imf_hpdd it ranked COUNTRY -> slot 0
            # at "100%" because that slot holds the frequency, so all 191 countries
            # map to 'A' — perfectly consistent and completely wrong. A real
            # dimension correspondence is close to INJECTIVE, so weight purity by how
            # many distinct targets the map actually reaches. 191->1 scores 0.005;
            # 191->191 scores 1.0.
            inj = len(set(m.values())) / max(len(m), 1)
            scored.append((purity * inj, dim, i, m))
    scored.sort(key=lambda x: -x[0])
    taken_dim, taken_slot, assign = set(), set(), {}
    for sc, dim, i, m in scored:
        if dim in taken_dim or i in taken_slot:
            continue
        taken_dim.add(dim); taken_slot.add(i); assign[dim] = (i, sc, m)

    for dim in sorted(slots):
        bi, bscore, bmap = assign.get(dim, (None, 0.0, {}))
        if bi is None:
            print(f"  {dim:<22} -> UNASSIGNED (no free slot explains it)")
            continue
        ident = all(k == v for k, v in bmap.items())
        print(f"  {dim:<22} -> slot {bi}  purity {100 * bscore:5.1f}%"
              f"{'  (identity)' if ident else ''}")
        if not ident:
            for k in sorted(bmap)[:8]:
                print(f"        {k:<22} -> {bmap[k]}")
            if len(bmap) > 8:
                print(f"        ... {len(bmap) - 8} more")

    matched_keys = {k for k, _ in pairs.values()}
    surv = 100.0 * len(matched_keys) / max(len(ours), 1)
    print(f"\nID SURVIVAL")
    print(f"  live ids preserved      : {len(matched_keys):,} of {len(ours):,} "
          f"({surv:.1f}%)")
    print(f"  upstream series unmatched (would mint NEW ids): {len(un):,}")
    print(f"  our ids upstream no longer carries            : "
          f"{len(ours) - len(matched_keys):,}  (never-shrink keeps their history)")
    newest_up = max((d for o in up.values() for d in o), default=None)
    newest_ours = max((d for o in ours.values() for d in o), default=None)
    print(f"  newest obs  upstream {newest_up}   ours {newest_ours}")
    print()
    print("VERDICT: " + ("REPAIRABLE IN PLACE — ids survive, gains freshness"
                        if surv >= 95 else
                        f"NOT a clean repair — only {surv:.1f}% of ids survive; "
                        "a parallel _direct source is the honest option"))

    if a.emit:
        if surv < 95:
            print(f"\nREFUSING to emit a config for a {surv:.1f}% repair — a fetcher "
                  f"built on this would re-key most of the source.")
            return 1
        prefix = next(iter(matched_keys)).split(":", 1)[0]
        cfg = {
            "source_id": a.source, "agency": a.agency, "flow": a.flow,
            "key_prefix": prefix, "arity": arity,
            "date_convention": "start" if name == "period-START" else "end",
            "slots": {dim: assign[dim][0] for dim in assign},
            "code_maps": {dim: assign[dim][2] for dim in assign},
            "derived_from": {"matched_series": len(matched_keys),
                             "our_series": len(ours),
                             "id_survival_pct": round(surv, 2)},
        }
        io.open(a.emit, "w", encoding="utf-8").write(json.dumps(cfg, indent=1,
                                                                sort_keys=True))
        print(f"\nwrote {a.emit}  ({len(cfg['slots'])} dims mapped)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
