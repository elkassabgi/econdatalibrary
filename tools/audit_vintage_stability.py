"""Does every vintage-gated fetcher return a token that can actually MATCH next run?

THE DEFECT THIS HUNTS (ledger R164, found on fed_board). A change-detection gate compares a
token to the one stored last run. If the upstream endpoint mints a NEW token on every request,
the comparison can never succeed: the source re-downloads, re-parses and re-uploads everything,
every run, forever — while the sidecar fills in, the logs read normally and the status stays
green. There is no per-run check that can see this. A cache that never hits is indistinguishable
from a cache that always hits, from the inside.

federalreserve.gov's Output.aspx was the first: generated per request, so Last-Modified advanced
on every call (03:17:40 / 03:18:01 / 03:18:21 across three HEADs 20s apart), no ETag, no
Content-Length, no Range. data.bis.org was the second and subtler one — several origin replicas
holding the same bytes with DIFFERENT mtimes, so the ETag and Last-Modified FLAP between requests
(Last-Modified even moved BACKWARDS five hours) while Content-Length stayed put.

THE TEST is the production signal itself: call current_vintage() twice, minutes apart, and
compare. Upstream data does not change in that window, so a token that moves is a gate that
cannot hit.

  STABLE  identical across both calls        -> the gate works
  MOVING  differs                            -> DEFECT unless listed in EXPECTED_MOVERS
  NONE    None both times                    -> no cheap vintage; falls back to cadence. Safe
                                                and documented behaviour, not a defect.
  ERROR   raised                             -> needs a look

A MOVING token is only a defect if the CONTENT did not change. Before adding anything to
EXPECTED_MOVERS, fetch the body twice and compare hashes — that is the discriminating test, and
it is what separated defillama (real change) from bis (replica flap) on 2026-07-29.

Usage:  python tools/audit_vintage_stability.py [--gap 180]
Exit 1 if any unexpected MOVING or ERROR — safe to wire into CI.
"""
from __future__ import annotations
import argparse
import importlib
import os
import pkgutil
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
os.environ.setdefault("AQUEDUCT_BACKEND", "local")

# Sources whose token legitimately moves because the DATA legitimately moves. Each needs a
# recorded body-hash comparison, not an assumption.
EXPECTED_MOVERS = {
    "defillama": (
        "api.llama.fi/protocols is live DeFi TVL — the body genuinely changes every few "
        "minutes. Verified 2026-07-29: the ETag's size half moved 0x818f58 -> 0x818c87 "
        "(8,491,352 -> 8,490,119 bytes), i.e. real content change, and two GETs 20s apart "
        "were byte-identical with an identical ETag. The gate is working, not flapping."
    ),
}


def discover() -> list:
    """Every fetcher module exposing current_vintage — discovered, not listed, so a fetcher
    added tomorrow is swept without anyone remembering to append it here (R159)."""
    import updater.strategies.fetchers as pkg
    out = []
    for m in pkgutil.iter_modules(pkg.__path__):
        if m.name.startswith("_"):
            continue
        try:
            mod = importlib.import_module(f"updater.strategies.fetchers.{m.name}")
        except Exception:                                    # noqa: BLE001
            continue
        if callable(getattr(mod, "current_vintage", None)):
            out.append(m.name)
    return sorted(out)


def probe(name):
    try:
        mod = importlib.import_module(f"updater.strategies.fetchers.{name}")
        t = time.time()
        return ("OK", mod.current_vintage(None), time.time() - t)
    except Exception as e:                                   # noqa: BLE001
        return ("ERR", f"{type(e).__name__}: {e}"[:160], 0.0)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gap", type=int, default=180,
                    help="seconds between the two probes (default 180)")
    args = ap.parse_args()

    mods = discover()
    print(f"round 1: {len(mods)} vintage-gated fetchers", flush=True)
    r1 = {}
    for n in mods:
        r1[n] = probe(n)
        print(f"  {n:22s} {str(r1[n][1])[:66]}", flush=True)

    print(f"\nsleeping {args.gap}s...", flush=True)
    time.sleep(args.gap)

    print("\nround 2 + verdicts", flush=True)
    rows = []
    for n in mods:
        k2, v2, el2 = probe(n)
        k1, v1, el1 = r1[n]
        if k1 == "ERR" or k2 == "ERR":
            verdict, detail = "ERROR", str(v1 if k1 == "ERR" else v2)
        elif v1 is None and v2 is None:
            verdict, detail = "NONE", "no cheap vintage; cadence-gated"
        elif v1 == v2:
            verdict, detail = "STABLE", f"{str(v1)[:50]} ({el1:.1f}s/{el2:.1f}s)"
        elif n in EXPECTED_MOVERS:
            verdict, detail = "MOVES-OK", EXPECTED_MOVERS[n][:60]
        else:
            verdict, detail = "MOVING", f"{str(v1)[:38]} -> {str(v2)[:38]}"
        rows.append((verdict, n, detail))
        print(f"  {verdict:8s} {n:22s} {detail}", flush=True)

    print("\n===== SUMMARY =====")
    for v in ("MOVING", "ERROR", "MOVES-OK", "NONE", "STABLE"):
        hit = [r for r in rows if r[0] == v]
        print(f"{v:8s} {len(hit):3d}   {' '.join(r[1] for r in hit)}")
    bad = [r for r in rows if r[0] in ("MOVING", "ERROR")]
    print(f"\nDEFECTS (must be 0): {len(bad)}")
    for _, n, d in bad:
        print(f"    {n}: {d}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
