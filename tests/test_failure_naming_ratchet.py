"""No NEW failure call site may report a count without an identity.

THE DEFECT, which already has two regression gates and neither generalises. Both
tests/test_cso_failure_naming.py and tests/test_stat_slovenia_failure_naming.py were written for
the same problem, one source at a time, and stat_slovenia's records the reasoning exactly:

    stat_slovenia: 1/85 sub-unit(s) returned 200 but parsed 0 rows ... existing data kept

    "and that is the entire record. One table out of 85 broke and nothing anywhere says WHICH.
     hagstofa, on the same tick, reported its two by name ... which is what let those two be
     probed against the live API in minutes."

`Tally.transient_unit(label)` and its siblings append to `*_ids`, and `finalize` renders them
through `_named`. The mechanism is fleet-wide; the discipline is not.

MEASURED 2026-09-03: 241 unlabelled call sites across 76 fetchers against 175 labelled ones —
58% of all failure reporting can produce a count and never an identity. Even stat_slovenia's own
`transient_unit()` calls are still bare; the gate it has protects its STRUCTURAL path only.

A live cost of it: ksh_stadat has reported `1/60 sub-unit(s) transient-failed; will retry` on
five consecutive runs from 2026-07-31 to 2026-09-01. An identical count for five weeks is a
constant, not a measurement (R669) — and both its call sites pass no label, so which of the 60 is
unanswerable from our own logs. (Both were labelled on 2026-09-03; it is kept here
as the worked example precisely because it shows what the fix looks like.)

WHY A RATCHET AND NOT A FIX. Labelling 241 sites is a large mechanical change that deserves its
own review; a ratchet stops the bleeding today at no risk. The baseline below is the measured
count, and the test fails if it RISES. Lower it whenever sites are fixed — that is the point.
"""
from __future__ import annotations

import ast
import os
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FETCHERS = os.path.join(ROOT, "updater", "strategies", "fetchers")
METHODS = ("transient_unit", "structural_unit", "no_time_unit", "deferred_unit")

# Measured 2026-09-03: 241 at first sweep, lowered to 225 the same day after the
# sixteen sites in the currently-FAILING sources were labelled.
# RATCHET: this may only ever go DOWN.
BASELINE_UNLABELLED = 225


def _unlabelled() -> list[tuple[str, int, str]]:
    out = []
    for dirpath, dirnames, filenames in os.walk(FETCHERS):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for name in sorted(filenames):
            if not name.endswith(".py"):
                continue
            path = os.path.join(dirpath, name)
            try:
                tree = ast.parse(open(path, encoding="utf-8").read())
            except (SyntaxError, UnicodeDecodeError):
                continue
            for node in ast.walk(tree):
                if (isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr in METHODS
                        and not node.args and not node.keywords):
                    out.append((os.path.relpath(path, ROOT), node.lineno, node.func.attr))
    return out


def test_unlabelled_failure_sites_do_not_increase() -> None:
    sites = _unlabelled()
    n = len(sites)
    assert n <= BASELINE_UNLABELLED, (
        f"{n} unlabelled failure call sites, up from the {BASELINE_UNLABELLED} baseline.\n"
        "A bare transient_unit()/structural_unit() can report a COUNT and never an identity, so "
        "the run says '1/60 sub-unit(s) transient-failed' and nobody can say which of the 60.\n"
        "Pass the sub-unit's id: tally.transient_unit(f\"{flow}: what went wrong\"). It is "
        "appended to *_ids and rendered by _named, capped at 20 with the elision stated.\n"
        "New sites (not in the baseline):\n  "
        + "\n  ".join(f"{f}:{ln} {m}()" for f, ln, m in sites[-8:])
    )


def test_the_baseline_is_not_stale_by_a_wide_margin() -> None:
    """If sites get fixed, LOWER the baseline — otherwise the ratchet stops ratcheting.

    Deliberately loose (20) so ordinary work does not trip it, and deliberately present so a
    campaign that fixes 100 sites cannot leave a baseline that permits 100 new ones.
    """
    n = len(_unlabelled())
    assert n >= BASELINE_UNLABELLED - 20, (
        f"only {n} unlabelled sites remain against a baseline of {BASELINE_UNLABELLED}. "
        f"Lower BASELINE_UNLABELLED to {n} so the ratchet keeps its grip."
    )


def test_the_detector_finds_both_shapes() -> None:
    """A guard that matches nothing passes vacuously (R501, R503)."""
    sites = _unlabelled()
    assert sites, "the detector found no unlabelled sites at all — it has stopped matching"
    kinds = {m for _, _, m in sites}
    assert "transient_unit" in kinds, "expected bare transient_unit() calls to be detected"

    by_file = defaultdict(int)
    for f, _, _ in sites:
        by_file[f] += 1
    # NOT anchored on a named source. The first version asserted ksh_stadat was detected,
    # because it was the docstring's worked example — and labelling ksh_stadat, which is
    # exactly what this ratchet exists to encourage, then broke the test. An anti-vacuity
    # check must not require any particular offender to stay broken.
    assert len(by_file) > 1, (
        f"unlabelled sites found in only {len(by_file)} file(s) — the detector is probably "
        f"matching one accidental pattern rather than the fleet"
    )
