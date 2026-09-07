"""Every tool that changes D1 `series` row counts must refresh `source_counts`.

WHY. `source_counts` is what the worker serves as a source's browse `total`
(`sql.ts::BROWSE_SOURCE_COUNT_CACHED`) and what `/v1/stats` sums. It exists because a live
`COUNT(*)` per page view caused the 2026-08-15 cost incident. It has **no foreign key** to the
rows it counts, so a writer that forgets it fails in the quietest possible way: a 200 with a
plausible number.

MEASURED 2026-09-07: `tools/refresh_sec_edgar.py` had been INSERTing into `series` and never
refreshing the total. D1 held 17,467 `sec_edgar` rows while the cache said 17,437 - 30 series
advertised away, dated by two receipts (26 + 4 on 2026-09-05). Nothing failed. Nothing could.

THIS IS A RATCHET, NOT A CLEAN BILL. Three tools in the same shape are still unfixed and are
listed below with what each does. The test's job is to stop the set from GROWING, and to force a
deliberate edit here when one is fixed - an allowlist nobody has to update is an allowlist that
quietly absorbs the next instance.

The rule it enforces is `protocols.md` item 7, which until today also claimed `source_counts` has
"exactly one writer". It does not, and believing so is what let the drift sit.
"""
from __future__ import annotations

import io
import os
import re

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)

# Anything matching this changes the number of rows in `series`.
_WRITES = re.compile(
    r"(INSERT\s+(OR\s+\w+\s+)?INTO\s+series\s*\(|DELETE\s+FROM\s+series\s)", re.I)

# ...but ONLY when the rows go to D1. The first version of this test matched the write alone
# and flagged 30+ files: almost all of them are cataloguers writing the LOCAL sqlite
# `data/catalog.db`, where `source_counts` is not their business - the sync refreshes it when
# it pushes those rows. A regex over one half of the property is not the property (R839).
#
# A file is a D1 writer when it also reaches D1: `wrangler d1 execute`, the repo's `_d1`
# runners, `--remote`, or a database name. Deliberately generous - a false positive here costs
# one line in KNOWN_UNFIXED, a false negative costs a silently wrong browse total.
_D1 = re.compile(
    r"(wrangler[\"'\s,\]]+d1|_d1_json|_d1\(|execute_remote|--remote|econ-catalog)", re.I)

# KNOWN UNFIXED, with what each one does. Removing an entry here is how a fix is recorded.
KNOWN_UNFIXED = {
    "tools/migrate_noaa_shard.py":
        "INSERT OR REPLACE INTO series on econ-catalog-climate and a paged DELETE FROM series "
        "on the primary. The most likely origin of noaa's historically recorded cache drift "
        "(3,138,201 cached vs 3,138,159 actual) - and it writes to TWO databases, so its fix "
        "must refresh the right one for the right source.",
    "core/export_d1_sources.py":
        "INSERT OR REPLACE INTO series for a whole source; emits SQL rather than executing it, "
        "so the refresh belongs in the emitted file, after the inserts.",
    "core/export_d1_new_series.py":
        "emits `INSERT OR REPLACE INTO series` for whole newly-titled sources into "
        "dist/d1/newseries/part_*.sql, which are imported into econ-catalog. Rows that were "
        "not there before, so the total moves and nothing refreshes it.",
}

# MATCHED BUT LOCAL. The scan cannot tell statically whether a `series` write is destined for D1
# or for the local sqlite `catalog.db`, and several files legitimately do one while mentioning the
# other. Rather than narrow the regex until it silently drops a real writer, each is classified
# here with the evidence. A NEW file must be put in one list or the other, which is the point.
KNOWN_LOCAL_WRITE = {
    "core/catalog.py":
        "the LOCAL sqlite catalogue itself; its only D1 mention is one docstring sentence "
        "('moving to production = pointing this same SQL at wrangler d1 execute --remote').",
    "core/export_d1_i18n_delta.py":
        "line 113 writes an IN-MEMORY sqlite probe (`mem.execute(... VALUES (?, '{}')`). What it "
        "emits for D1 are title UPDATEs, which do not change row counts.",
    "tools/sync_source_rows_d1_to_local.py":
        "reads D1 and writes the LOCAL copy (line 164, sqlite `?` placeholders on `con`). It "
        "exists precisely because D1 runs ahead of local for CI-written sources - the gap "
        "measured in R849.",
}

# Handled. Kept explicit so a regression in one of them fails here rather than in production.
KNOWN_HANDLED = {
    "core/sync_catalog_d1.py",
    "tools/refresh_sec_edgar.py",
    "tools/delist_source_rows.py",
    "tools/retire_source.py",
    "tools/delist_timeless_tables.py",
}


def _scan():
    """{relative path: writes_series} for every tool and core module."""
    out = {}
    for sub in ("tools", "core"):
        d = os.path.join(_ROOT, sub)
        for name in sorted(os.listdir(d)):
            if not name.endswith(".py"):
                continue
            rel = f"{sub}/{name}"
            try:
                src = io.open(os.path.join(d, name), encoding="utf-8").read()
            except OSError:
                continue
            if _WRITES.search(src) and _D1.search(src):
                out[rel] = "source_counts" in src
    return out


def test_the_scanner_does_not_flag_local_sqlite_cataloguers():
    """The discriminator is D1, not the word `series`. `tools/catalog_fhfa.py` writes
    `INSERT INTO series (...)` into the LOCAL catalog.db and has no business refreshing a D1
    cache; the first version of this test flagged it and thirty of its siblings."""
    found = _scan()
    for local_only in ("tools/catalog_fhfa.py", "tools/catalog_cepii_baci.py",
                       "tools/_cat_bea.py"):
        assert local_only not in found, (
            f"{local_only} writes the local catalogue, not D1 - the scanner is too broad again")


def test_no_new_writer_forgets_the_cached_total():
    """The ratchet. A NEW tool that changes `series` row counts must reference `source_counts`."""
    found = _scan()
    offenders = sorted(p for p, ok in found.items() if not ok)
    new = [p for p in offenders if p not in KNOWN_UNFIXED and p not in KNOWN_LOCAL_WRITE]
    assert not new, (
        "these change D1 `series` row counts and never touch `source_counts`, so the browse "
        "total they invalidate stays stale and returns a plausible wrong number:\n  "
        + "\n  ".join(new)
        + "\n\nRefresh it in the same operation - the canonical statement is in "
          "core/sync_catalog_d1.py:277-279 - or add the file to KNOWN_UNFIXED here with what it "
          "does and why it is deferred.")


def test_the_known_unfixed_list_does_not_go_stale():
    """When one is fixed, this fails until the entry is removed. An allowlist nobody has to
    update is an allowlist that quietly absorbs the next instance."""
    found = _scan()
    fixed = sorted(p for p in KNOWN_UNFIXED if found.get(p) is True)
    assert not fixed, (
        "these now reference `source_counts` and must be removed from KNOWN_UNFIXED:\n  "
        + "\n  ".join(fixed))
    gone = sorted(p for p in list(KNOWN_UNFIXED) + list(KNOWN_LOCAL_WRITE)
                  if p not in found)
    assert not gone, (
        "these no longer write `series` at all (renamed, deleted, or did the scan narrow?) "
        "- drop them from KNOWN_UNFIXED / KNOWN_LOCAL_WRITE:\n  " + "\n  ".join(gone))
    both = sorted(set(KNOWN_UNFIXED) & set(KNOWN_LOCAL_WRITE))
    assert not both, ("a file cannot be both a D1 writer and a local-only writer: "
                      + str(both))


def test_the_handled_ones_stay_handled():
    """A regression in a tool that already got this right must fail here, not in production."""
    found = _scan()
    broken = sorted(p for p in KNOWN_HANDLED if found.get(p) is False)
    assert not broken, (
        "these used to refresh `source_counts` alongside their `series` writes and no longer "
        "do:\n  " + "\n  ".join(broken))


def test_the_sec_edgar_refresher_is_in_the_handled_set():
    """The instance that produced the measurement - pinned by name so a revert is loud."""
    found = _scan()
    assert found.get("tools/refresh_sec_edgar.py") is True, (
        "refresh_sec_edgar.py INSERTs into `series` on a daily CI schedule; without the recount "
        "it advertised 17,437 against 17,467 rows for two days")
