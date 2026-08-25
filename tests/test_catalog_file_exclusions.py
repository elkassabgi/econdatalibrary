"""A parquet must not inherit a licence just because of the directory it sits in.

`tools/catalog_complete.py` copies one licence onto every key it finds, and it finds keys by
globbing a source's whole directory. One directory meaning one licence is a convention, not a
guarantee — and vdem is where it broke: `data/clean_full/vdem/` holds `vdem.parquet` and
`vparty.parquet`, V-Dem publishes CC BY-SA 4.0 for "The V-Dem Dataset" on two official surfaces,
and V-Party is a separate publication whose own page carries no licence language at all.
Cataloguing the directory would have stamped 2,218,990 V-Party observations with a grant nobody
gave — the same shape as the FAO incident the tool already documents, arriving through the
filesystem instead of through the rows.

These tests pin the exclusion and, more importantly, pin that it is ANNOUNCED. A coverage limit
nobody prints reads as full coverage.
"""
from __future__ import annotations

import re

import os

import pytest

from tools import catalog_complete as C

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LICENCES = os.path.join(ROOT, "DATABASE_LICENSES_VERBATIM.md")


def test_vparty_is_excluded_from_vdems_catalogue():
    assert "vparty.parquet" in C.SOURCE_FILE_EXCLUSIONS.get("vdem", ())


def test_the_exclusion_filter_keeps_the_cleared_file_and_drops_the_other():
    files = ["/x/data/clean_full/vdem/vdem.parquet", "/x/data/clean_full/vdem/vparty.parquet"]
    ex = C.SOURCE_FILE_EXCLUSIONS["vdem"]
    kept = [f for f in files if os.path.basename(f) not in ex]
    assert kept == ["/x/data/clean_full/vdem/vdem.parquet"]


def test_sources_without_an_entry_are_unaffected():
    """The exclusion must be opt-in — a typo in the map cannot silently drop another source."""
    assert C.SOURCE_FILE_EXCLUSIONS.get("cbs_nl", ()) == ()
    assert C.SOURCE_FILE_EXCLUSIONS.get("faostat", ()) == ()


def test_every_excluded_file_is_explained_in_the_licence_audit():
    """An exclusion is a licence claim, so it must be traceable to the canonical document.

    Otherwise the map becomes a place where data quietly disappears for reasons nobody recorded.
    """
    text = open(LICENCES, encoding="utf-8").read()
    for source, files in C.SOURCE_FILE_EXCLUSIONS.items():
        assert source in text, f"{source} has an exclusion but no mention in the licence audit"
        for fname in files:
            stem = fname.replace(".parquet", "")
            assert stem in text or stem.replace("_", "-") in text, (
                f"{source}/{fname} is excluded from cataloguing but the licence audit never "
                f"explains why — record the reason before excluding data")


def test_the_exclusion_is_announced_not_silent():
    src = open(os.path.join(ROOT, "tools", "catalog_complete.py"), encoding="utf-8").read()
    i = src.index("SOURCE_FILE_EXCLUSIONS.get(source")
    window = src[i:i + 900]
    assert "EXCLUDING" in window, "the tool must print what it dropped"
    assert "return 0" in window, "excluding every file must stop rather than insert nothing quietly"


def test_the_map_is_documented_where_it_is_defined():
    """R469's habit: the reason lives beside the rule, not in a commit message."""
    src = open(os.path.join(ROOT, "tools", "catalog_complete.py"), encoding="utf-8").read()
    i = src.index("SOURCE_FILE_EXCLUSIONS = {")
    preamble = src[max(0, i - 1600):i]
    assert "vparty" in preamble.lower(), "the measured case must be named where the map is defined"
    assert "licence" in preamble.lower() or "license" in preamble.lower()


@pytest.mark.parametrize("bad", ["vdem.parquet"])
def test_the_cleared_file_is_never_itself_excluded(bad):
    """A guard against the obvious editing accident: excluding the source's own dataset."""
    assert bad not in C.SOURCE_FILE_EXCLUSIONS.get("vdem", ())

def test_the_bulk_derive_honours_the_same_exclusions():
    """The exclusion must be a property of the CODE, not of remembering a flag.

    tools/derive_csv_bulk.py walks a source directory. vdem's directory holds vparty.parquet,
    whose 682,659 keys do not overlap vdem's, so the duplicate-shard guard cannot fire. Without
    honouring the exclusion list, a run without --only-catalogued would mint 682,659 unlicensed
    CSVs onto R2 - R364 verbatim. Verified live: the dry run reports EXCLUDING vparty.parquet
    and streams 783,100 series, not 1,465,759."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src = open(os.path.join(root, "tools", "derive_csv_bulk.py"), encoding="utf-8").read()
    assert "SOURCE_FILE_EXCLUSIONS" in src, (
        "derive_csv_bulk does not know the licence exclusions — a whole-directory run on vdem "
        "would publish V-Party")
    # BEHAVIOURAL, not positional. This was `window = src[i:i+900]` and it went red when a
    # comment was added above the announcement, pushing the gap to 1,044 characters while both
    # facts it checks stayed true. A guard a comment can break invites someone to delete the
    # comment, and its failure says nothing about the property being protected.
    assert re.search(r"_skipped\s*=\s*\[", src), (
        "derive_csv_bulk no longer computes the list of files it skipped, so it cannot report "
        "them - the exclusion would apply silently")
    m = re.search(r"print\(([^)]*EXCLUDING[^)]*)\)", src, re.S)
    assert m, "the exclusion is applied but never announced - a silent skip is R364's shape"
    assert "_skipped" in m.group(1), (
        "the EXCLUDING message does not reference the skipped files, so it cannot say WHICH "
        "files were left out")
