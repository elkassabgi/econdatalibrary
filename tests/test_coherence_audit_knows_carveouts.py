"""A licence carve-out must not be reported as catalogue drift.

`tools/audit_serving_coherence.py` compares each source's local catalogue count against the live
`total` from `/v1/catalog`. Three sources carry `SERIES_CARVEOUTS`, so their live total is
DELIBERATELY lower - the carved series are not served. `api/worker/src/sql.ts` states it plainly:
"It must not be `source_counts`, which counts carved rows and so advertised 692 for worldbank where
262 are reachable."

On its first run (2026-09-22) the auditor reported 6 drifts. Three were exactly the carve-out
sources - worldbank 692 vs 262, worldbank_esg 5,473 vs 5,295, worldbank_wdi 1,486 vs 1,484 - which
is 610 series withheld by licence, reported as defects. A permanent false positive is how the three
REAL drifts beside them (fhfa, fed_board, sec_edgar; 104 series) get ignored.

Offline: parses the two TypeScript sources, opens no connection and probes nothing.
"""
from __future__ import annotations

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

TOOL = os.path.join(ROOT, "tools", "audit_serving_coherence.py")
DENYLIST = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")


def _carveout_keys(text):
    """The same parse the tool performs: comments stripped, then top-level keys."""
    start = re.search(r"SERIES_CARVEOUTS\b[^=]*=\s*\{", text)
    if not start:
        return set()
    body = text[start.end():]
    end = re.search(r"^\s*\};", body, re.M)
    body = body[: end.start()] if end else body
    body = "\n".join(re.sub(r"//.*$", "", ln) for ln in body.splitlines())
    return set(re.findall(r"^\s*([a-z0-9_]+)\s*:", body, re.M))


def test_the_carveout_sources_are_found_in_the_worker_source():
    keys = _carveout_keys(open(DENYLIST, encoding="utf-8").read())
    assert keys, "SERIES_CARVEOUTS parsed to nothing - the audit would call every carve-out a drift"
    assert "worldbank" in keys, "the worldbank carve-out is the one the worker's own comment cites"


def test_comment_stripping_is_what_makes_the_parse_work():
    """The block is preceded by a comment that names the carved indicators. Without stripping
    comments, a lazy parse picks them up - the R0.4 trap the tool's own SUPPORTED_SOURCES parser
    documents."""
    fake = (
        "/* worldbank: CPI is IMF-sourced, so it is gated.\n"
        " * decoy: \"NOT_A_SOURCE\"\n */\n"
        "export const SERIES_CARVEOUTS: Readonly<Record<string, readonly string[]>> = {\n"
        "  // commented_out: [\"X\"],\n"
        "  realsource: [\"A.B\", \"C.D\"],\n"
        "};\n")
    keys = _carveout_keys(fake)
    assert keys == {"realsource"}, f"comment stripping failed: parsed {keys}"


def test_an_empty_parse_refuses_rather_than_reporting():
    """An empty parse means 'I could not look', not 'nothing is carved' (R261, R503).

    Pinned on the ZERO-SOURCES branch specifically. A first version searched a 2,000-character
    window for "PARSE FAILED" and "sys.exit" and passed while that branch printed a warning and
    carried on - because the window also contained the *other* refusal, the missing-literal one.
    A mutation check caught it. Two guards that differ only in their message need two assertions.
    """
    src = open(TOOL, encoding="utf-8").read()
    i = src.index("if not carved:")
    branch = src[i:i + 400]
    assert "sys.exit" in branch, (
        "the ZERO-sources branch must exit; a warning here lets every carve-out read as drift")
    assert "refusing to report drift" in branch, "the refusal must say what it is refusing to do"


def test_the_audit_separates_carveouts_from_drift():
    """Pin the behaviour, not the wording: a carve-out must not land in the drift list."""
    src = open(TOOL, encoding="utf-8").read()
    assert "carveouts.append" in src, "carve-outs are not collected separately"
    assert "is_carved = src in carved and lv < n" in src, (
        "the carve-out test must require the live count to be LOWER - a carve-out cannot make the "
        "live total exceed the catalogue, and treating that as a carve-out would hide real drift")
    block = src[src.index("is_carved ="):]
    assert "elif lv != n:" in block, "drift must be the else-branch of the carve-out test"
