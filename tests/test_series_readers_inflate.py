"""Anything that GETs a `series/*.csv` object must inflate before it reads the bytes.

WHY THIS IS A TEST AND NOT A NOTE. Commit d866c43d3 (2026-08-18) shipped gzip-at-rest and
recorded the follow-up in its own message: "one-shot maintenance tools that read series objects
directly (repair_stale_csvs, audit_csv_staleness, probe_csv_freshness, resync_and_repair) get
the same magic-byte inflate before their next use." Two weeks later three of the four were
unfixed, and `rebuild_cso_retired_from_csv.py` was raising UnicodeDecodeError on every key
because cso's objects are gzipped.

A follow-up noted in a commit message is not a follow-up done.

WHAT IT COSTS TO GET WRONG. These are not passive readers:

  * `probe_csv_freshness.py` runs DAILY (updater-daily.yml) and prints "SERVED BYTES DISAGREE
    ... users are downloading superseded data". Comparing compressed bytes against a freshly
    built CSV marks every gzipped object stale.
  * `repair_stale_csvs.py --apply` and `resync_and_repair.py --apply` turn that same false
    verdict into a mass re-derive.

The serving contract is the DECOMPRESSED text - `api/worker/src/series.ts:260` keys on
`obj.httpMetadata?.contentEncoding` and the worker inflates before it does anything - so
equality must be judged on inflated bytes.
"""
from __future__ import annotations

import ast
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS = os.path.join(ROOT, "tools")

# A file builds a series-CSV key if it mentions the prefix with the encoded id.
_SERIES_KEY = re.compile(r'["\']series/["\']\s*\+|f["\']series/\{')
# The inflate can be spelled a few ways; all of them name the magic bytes.
_INFLATE = re.compile(r'\\x1f\\x8b|1f8b')


# Files that build a series key for WRITING but whose get_object reads something else.
# Listed explicitly, with what it actually reads, because a detector that guesses which key a
# get_object receives is a detector that will quietly stop detecting. Verified 2026-09-03.
READS_SOMETHING_ELSE = {
    "make_servable.py":            "clean_full/<src>/<src>.parquet (line 128)",
    "flowgrain_ons_uk.py":         "clean_full/<src>/ parquet (PREFIX, line 36)",
    "flowgrain_insee_melodi.py":   "clean_full/<src>/ parquet",
    "refresh_sec_edgar.py":        "clean_grouped/sec_edgar/*.parquet (line 320)",
}


def _gets_an_object(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and node.func.attr == "get_object":
            return True
    return False


def test_every_series_csv_reader_inflates():
    offenders = []
    checked = []
    for name in sorted(os.listdir(TOOLS)):
        if not name.endswith(".py"):
            continue
        path = os.path.join(TOOLS, name)
        with open(path, encoding="utf-8", errors="replace") as fh:
            src = fh.read()
        if not _SERIES_KEY.search(src):
            continue
        try:
            tree = ast.parse(src, filename=name)
        except SyntaxError:                    # not this test's business
            continue
        if not _gets_an_object(tree):
            continue                           # a writer, not a reader
        if name in READS_SOMETHING_ELSE:
            continue                           # verified to read parquet, not a series CSV
        checked.append(name)
        if not _INFLATE.search(src):
            offenders.append(name)

    assert checked, (
        "found no series-CSV readers at all - the detector is broken, which would make this "
        "test pass forever while proving nothing")
    assert not offenders, (
        "these GET a series/*.csv object and never inflate it, so every gzipped object reads "
        "as corrupt or stale: " + ", ".join(offenders)
        + f"  (checked: {', '.join(checked)})")


def test_the_exclusion_list_has_not_gone_stale():
    """An allow-list that names files which no longer exist is an allow-list nobody maintains.

    And if one of these ever starts reading a series CSV, its entry here would silence this
    test for exactly the file that needs it - so the entry must keep naming what it reads.
    """
    for name in READS_SOMETHING_ELSE:
        assert os.path.exists(os.path.join(TOOLS, name)), (
            f"{name} is excluded from the inflate check but no longer exists - remove the entry")
