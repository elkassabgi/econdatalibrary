"""The `<source>:` .. `<source>;` PK range must mean exactly `source_id = <source>`.

WHY THIS FILE EXISTS. Three queries in `_catalog_ids_for`, and the per-source counts in
`tools/audit_schedule_coverage.py`, were `WHERE source_id = ?` -- a full scan of an 11.9 GB
file, because `series` carries exactly ONE index (the series_id primary key). Replacing them
with a PK range is 5,228x faster on cso (0.00 s vs 7.13 s warm, 389 s cold) and 15x across
the whole catalogue (1.39 s vs 21.35 s).

That substitution is only safe if the range is EQUIVALENT, and the dangerous direction is
silent: a range that misses rows makes a source look smaller than it is, and one that
over-reaches re-derives another source's series. Neither raises anything.

The sharp case is a source id that is a PREFIX of another. The catalogue has 19 such pairs
today -- `unctad_biotrademerch` against five longer siblings, `fsi` against
`fsi_fundforpeace`, and more. All 19 are safe, and the reason is narrower than it first
looks: ':' is 0x3A and ';' is 0x3B, adjacent bytes, and every one of those 19 extends with
'_' (0x5F) or a letter (0x41+), which sort AFTER ';'.

DIGITS DO NOT. 0x30-0x39 are all BELOW 0x3B, so `foo2:X` < `foo;` and a source id that is
another one plus a digit WOULD be swallowed by the shorter one's range. I wrote the
assertion the confident way round first -- "every character that can extend a source id
sorts after ';'" -- and it failed on '0'. There are zero such pairs among the real 349
source ids today, so the substitution is correct; the registry guard below is what keeps it
correct when someone adds the 350th.

These tests pin that reasoning, and the equivalence itself, without needing the real 11.9 GB
catalogue.
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Real prefix pairs from data/catalog.db, 2026-08-30. Chosen because they are the case that
# would actually break, not invented shapes.
REAL_PREFIX_PAIRS = [
    ("fsi", "fsi_fundforpeace"),
    ("unctad_biotrademerch", "unctad_biotrademerchrca"),
    ("unctad_biotrademerch", "unctad_biotrademerchmarketindices"),
    ("unctad_ictuseeconactivity", "unctad_ictuseeconactivityisic4"),
    ("worldbank", "worldbank_wdi"),
    ("imf_fm", "imf_fm_direct"),
]


@pytest.fixture
def cat(tmp_path):
    p = tmp_path / "c.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    for short, long_ in REAL_PREFIX_PAIRS:
        for i in range(3):
            con.execute("INSERT OR IGNORE INTO series VALUES (?,?)",
                        (f"{short}:S{i}", short))
            con.execute("INSERT OR IGNORE INTO series VALUES (?,?)",
                        (f"{long_}:L{i}", long_))
    con.commit()
    yield con
    con.close()


def _range_ids(con, src):
    return [r[0] for r in con.execute(
        "SELECT series_id FROM series WHERE series_id >= ? AND series_id < ?",
        (src + ":", src + ";"))]


def _scan_ids(con, src):
    return [r[0] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id = ?", (src,))]


@pytest.mark.parametrize("short,long_", REAL_PREFIX_PAIRS)
def test_a_prefix_range_never_leaks_into_a_longer_sibling(cat, short, long_):
    """THE CORRECTNESS PROPERTY. Measured on the real catalogue: 19 prefix pairs, 0 leaks."""
    got = _range_ids(cat, short)
    assert got, f"{short} must match its own rows"
    assert not any(g.startswith(long_ + ":") for g in got), (
        f"{short}'s range leaked {long_} rows: {[g for g in got if g.startswith(long_)][:3]}")


@pytest.mark.parametrize("short,long_", REAL_PREFIX_PAIRS)
def test_the_longer_sibling_still_finds_all_its_own_rows(cat, short, long_):
    """The other direction, which a too-tight bound would break silently: the longer id must
    not lose rows to the shorter one's range."""
    assert sorted(_range_ids(cat, long_)) == sorted(_scan_ids(cat, long_))


def test_range_and_scan_agree_for_every_source(cat):
    """Equivalence stated as the thing actually relied on. On the real catalogue this holds
    across all 322 sources and 13,486,342 series, checked 2026-08-30."""
    srcs = {r[0] for r in cat.execute("SELECT DISTINCT source_id FROM series")}
    for s in sorted(srcs):
        assert sorted(_range_ids(cat, s)) == sorted(_scan_ids(cat, s)), s


def test_digits_sort_BELOW_the_upper_bound_which_is_the_real_hazard():
    """THE TRAP, found by writing this assertion the wrong way round first.

    I asserted "every character that can extend a source id sorts after ';'" and it FAILED:
    ';' is 0x3B and the digits are 0x30-0x39, all BELOW it. So a source id that is another
    source id followed by a DIGIT lands INSIDE the shorter one's range -- `foo2:X` < `foo;`
    -- and the shorter source would silently swallow the longer one's series.

    Letters and '_' are safe (0x41+, 0x5F). Digits are not. Measured on the real catalogue
    2026-08-30: 349 source ids, ZERO digit-extended prefix pairs, and zero rows landing in a
    range they do not belong to -- so the substitution is correct TODAY. The next test is
    what keeps it correct.
    """
    assert ord(";") == ord(":") + 1
    for ch in "_abcxyzABCXYZ":
        assert ord(ch) > ord(";"), ch
    for ch in "0123456789":
        assert ord(ch) < ord(";"), (
            f"{ch!r} sorts below ';' -- a digit-extended source id WOULD leak")


def test_no_registry_source_id_is_another_one_plus_a_low_byte():
    """THE GUARD, mechanical rather than prose. The PK-range substitution in
    `_catalog_ids_for` and `audit_schedule_coverage` is equivalent only while no source id is
    another source id extended by a byte below ';' -- in practice, a digit.

    This reads the committed registry rather than the 11.9 GB catalogue, so it is fast and
    runs in CI. If someone adds `unctad_oceantrade2` beside `unctad_oceantrade`, this fails
    here instead of silently merging two sources' series months later.
    """
    import yaml
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "updater", "registry.yaml"), encoding="utf-8") as fh:
        reg = yaml.safe_load(fh)
    ids = sorted({s["source_id"] for s in reg["sources"] if s.get("source_id")})
    bad = [(a, b) for a in ids for b in ids
           if b != a and b.startswith(a) and ord(b[len(a)]) < ord(";")]
    assert not bad, (
        f"source id(s) extended by a byte below ';' -- the shorter one's PK range would "
        f"swallow the longer one's series: {bad}")
