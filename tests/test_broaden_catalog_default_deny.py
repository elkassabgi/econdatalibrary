"""broaden_catalog must not re-catalogue a source that was retired (R819, R226).

WHAT WENT WRONG. A fleet-wide `core/broaden_catalog.py --dry-run` on 2026-09-06 proposed KEEP
for 24 uncatalogued store directories holding 308,555 series, and every one was retired or
reserved: the 22 legacy `imf_*` ids from the 2026-08-07 retirement wave, `ksh` (withdrawn
2026-08-02 after being re-served, R226), and `unctad_cpa` (reserved to Ahmed).

The two gates that existed could not see any of them:
  * `UNHOSTABLE` (gen_denylist.LEGACY_KEEP) was frozen at the 2026-07-22/23 purge; and
  * `not_reservable` joins `source` to `license`, but `tools/retire_source.py` DELETES the
    `source` row, so a retired id produces no row for the join — retiring a source disarms
    the guard meant to stop it being resurrected.

These tests pin the inverted, default-deny rule. They are deliberately TWO-SIDED, because a
one-sided assertion here cannot fail in the direction that matters: a gate that refuses
everything would pass a "retired ids are refused" test, and a gate that allows everything —
which is the bug — would pass a "served ids are allowed" test. Both must hold at once.
"""
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.broaden_catalog import _allowed_to_catalog, _served_ids   # noqa: E402

# Retired or reserved: none of these may ever be catalogued by this tool without --allow-new.
# ksh: withdrawn 2026-08-02 (R226). imf_*: the 2026-08-07 retirement wave.
# unctad_cpa: one of the 38 UNCTAD legacy ids reserved to Ahmed (#46).
RETIRED = ("ksh", "imf_hpdd", "imf_pctot", "imf_psbsfad", "imf_gfse", "unctad_cpa")

# Served today. If one of these is ever genuinely retired, this list is what must be edited —
# deliberately, which is the entire point of the gate.
SERVED_CONTROLS = ("damodaran", "bea", "cbs_nl")


def test_retired_ids_are_refused():
    """The bug, stated as a test: an uncatalogued retired id must NOT be catalogueable."""
    served = _served_ids()
    for src in RETIRED:
        assert not _allowed_to_catalog(src, cataloged=set(), served=served, allow_new=set()), (
            f"{src} is retired/reserved but the gate would catalogue it — this is R819"
        )


def test_served_ids_are_allowed():
    """The other side: the gate must not simply refuse everything.

    Without this, a `return False` would satisfy the test above and silently break the tool.
    """
    served = _served_ids()
    for src in SERVED_CONTROLS:
        assert _allowed_to_catalog(src, cataloged=set(), served=served, allow_new=set()), (
            f"{src} is served by the deployed worker but the gate refuses it"
        )


def test_allow_new_is_the_only_door_for_an_unserved_id():
    """A genuinely new source is legitimately absent from util.ts, so it must be NAMED.

    Checklist B catalogues BEFORE util.ts is edited, so this path is the normal one for a new
    source — it just has to be deliberate.
    """
    served = _served_ids()
    newsrc = "a_source_that_does_not_exist_anywhere"
    assert not _allowed_to_catalog(newsrc, set(), served, allow_new=set())
    assert _allowed_to_catalog(newsrc, set(), served, allow_new={newsrc})
    # naming a DIFFERENT id must not open the door
    assert not _allowed_to_catalog(newsrc, set(), served, allow_new={"something_else"})


def test_allow_new_does_not_resurrect_a_retired_id_by_accident():
    """--allow-new is an explicit override; it must require the exact id, not a prefix."""
    served = _served_ids()
    assert not _allowed_to_catalog("ksh", set(), served, allow_new={"ksh_stadat"})
    assert not _allowed_to_catalog("imf_hpdd", set(), served, allow_new={"imf"})


def test_served_reader_strips_comments():
    """util.ts names retired ids INSIDE comments; harvesting them would re-open the hole.

    This is the R137 shape and the reason the gate reuses audit_schedule_coverage's reader
    rather than scanning for quoted strings.
    """
    served = _served_ids()
    assert len(served) > 200, f"implausibly small SUPPORTED_SOURCES ({len(served)}) — parse broke"
    for src in ("ksh", "zillow", "owid"):
        assert src not in served, (
            f"{src} appears in the parsed SUPPORTED_SOURCES; it is retired/gated and is named "
            f"only in a util.ts comment — the comment stripping has regressed"
        )


def test_gate_is_wired_into_the_scan_loop():
    """A predicate nobody calls is not a gate (R109: 'wired' functions that are never called)."""
    src = open(os.path.join(ROOT, "core", "broaden_catalog.py"), encoding="utf-8").read()
    assert "_allowed_to_catalog(d, cataloged, served, allow_new)" in src, (
        "the default-deny predicate is defined but not called from the store-dir loop"
    )
    assert "refused_unlisted" in src, "refusals must be collected and reported, never silent"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
