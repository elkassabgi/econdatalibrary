"""One line, first thing, naming which store the tool is about to operate on.

WHY. On 2026-08-03 I relaunched the eurostat re-key's dry run without AQUEDUCT_BACKEND=r2, so a
migration safety measurement for 2.4 billion rows read the LOCAL SCRATCH MIRROR instead of the R2
store the migration targets (R296, and R36 for why those are different things). It would have
looked entirely correct: the mirror holds the same 7,754 files, so the file count, the progress
lines and the shape of every number were plausible. The only reason it was caught is that
rekey_eurostat.py prints its backend and resolved path on line one, unasked.

So this is that habit, factored out instead of copy-pasted into each tool — a second copy of a
convention is a second thing to drift (R159). Tools that can address more than one store call
banner() before doing anything, and an environment mistake becomes visible in the first line of
output rather than invisible in all of it.

Deliberately NOT a guard. It does not refuse, prompt, or require a flag: `local` is a legitimate
target for plenty of work, and a banner that blocks would just get bypassed. It states the choice
and leaves the judgement where it belongs.
"""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import config                                        # noqa: E402


def banner(what: str, path: str | None = None, n: int | None = None) -> None:
    """Print the target store. `what` names the thing (source id, catalog, ...)."""
    bits = [f"{what}:"]
    if n is not None:
        bits.append(f"{n:,} file(s)")
    if path:
        bits.append(f"under {path}")
    bits.append(f"(backend={config.BACKEND})")
    print("  ".join(bits), flush=True)
    if config.BACKEND != "r2":
        print("  NOTE: backend is not r2 — this is the local scratch mirror, NOT the served "
              "store. Set AQUEDUCT_BACKEND=r2 to address production.", flush=True)
