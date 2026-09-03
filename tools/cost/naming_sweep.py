"""How many failure call sites still refuse to say WHICH sub-unit failed?

Two regression gates exist for exactly this defect - tests/test_cso_failure_naming.py and
tests/test_stat_slovenia_failure_naming.py - and their reasoning is the same in both:

    stat_slovenia: 1/85 sub-unit(s) returned 200 but parsed 0 rows ... existing data kept

    "and that is the entire record. One table out of 85 broke and nothing anywhere says WHICH.
     hagstofa, on the same tick, reported its two by name ... which is what let those two be
     probed against the live API in minutes."

Each gate was written for ONE source. The mechanism is fleet-wide - `Tally.transient_unit(label)`
and friends append to `*_ids` and `finalize` renders them through `_named` - so every unlabelled
call site is a failure that can be counted but never investigated.

Live example, measured 2026-09-03: ksh_stadat has reported `1/60 sub-unit(s) transient-failed;
will retry` on five consecutive runs spanning 2026-07-31 to 09-01 - an identical count for five
weeks, which by R669's rule is a constant rather than a measurement - and BOTH its
`transient_unit()` call sites pass no label, so nobody can say which of the 60.

Static: parses the source, counts calls to the four labelling methods with and without an
argument. Reads nothing but code.
"""
import ast
import os
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FETCHERS = os.path.join(ROOT, "updater", "strategies", "fetchers")
METHODS = ("transient_unit", "structural_unit", "no_time_unit", "deferred_unit")


def main() -> int:
    unlabelled = defaultdict(list)
    labelled = defaultdict(int)

    for dp, dirnames, filenames in os.walk(FETCHERS):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for fn in sorted(filenames):
            if not fn.endswith(".py"):
                continue
            path = os.path.join(dp, fn)
            try:
                tree = ast.parse(open(path, encoding="utf-8").read())
            except (SyntaxError, UnicodeDecodeError):
                continue
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr in METHODS):
                    continue
                rel = os.path.relpath(path, ROOT)
                if node.args or node.keywords:
                    labelled[node.func.attr] += 1
                else:
                    unlabelled[node.func.attr].append((rel, node.lineno))

    print(f"{'method':<20}{'labelled':>10}{'UNLABELLED':>12}")
    tot_l = tot_u = 0
    for m in METHODS:
        l, u = labelled.get(m, 0), len(unlabelled.get(m, []))
        tot_l += l
        tot_u += u
        print(f"{m:<20}{l:>10}{u:>12}")
    print(f"{'TOTAL':<20}{tot_l:>10}{tot_u:>12}")
    print()

    files = defaultdict(int)
    for m, sites in unlabelled.items():
        for rel, _ in sites:
            files[rel] += 1
    print(f"{len(files)} fetcher(s) have at least one unlabelled failure call:")
    for rel, n in sorted(files.items(), key=lambda kv: -kv[1])[:18]:
        print(f"    {n:>3}  {rel}")
    if len(files) > 18:
        print(f"    ... and {len(files) - 18} more")
    print()
    print("NOT ALL OF THESE ARE DEFECTS. Two bare `deferred_unit()` calls are deliberate —")
    print("stat_estonia.py and unsdg.py both count deferrals without holding an identifier,")
    print("and deferral is not a failure (R303): naming every deferred sub-unit would bury")
    print("the ones that actually broke. Read each site; do not apply a blanket rule.")
    print()
    print("Every other one can report a COUNT and never an identity.")
    print()
    print("Progress is ratcheted by tests/test_failure_naming_ratchet.py, which fails if this")
    print("count RISES and demands the baseline be lowered when it falls. Sources whose")
    print("failures are CURRENT were done first, on 2026-09-03: the sixteen the cloud health")
    print("gate fails on, then the worst remaining live files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
