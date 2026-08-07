"""ecb sweeps 540 sorted files under a 35-minute budget; without a bookmark, most are frozen.

MEASURED 2026-08-07 across four consecutive real runs. The deferred set was always a SUFFIX
(280, 349, 338, 307 of 540) and the best prefix ever reached was 260 of 540, so sorted
indices 260-539 had NEVER been fetched: 280 files across 107 agency__flow groups, including
ECB__EXR (euro reference rates), ECB__ICP (HICP), ECB__YC (yield curves), ECB__FM, ECB__STS
and 64 ESTAT__QSA files. ECB__EXR__D sat at newest obs 2026-07-31 while every run reported
`partial` with ZERO failures — a silent outage wearing a reassuring status.
"""
from __future__ import annotations

import inspect

from updater.strategies.fetchers import ecb
from updater.strategies.fetchers._common import rotate_after


class TestRotationReachesTheTail:
    def test_the_first_deferred_file_leads_the_next_run(self):
        """The exact production shape: 540 sorted names, budget stops at index 260."""
        files = [f"ECB__F{i:04d}.parquet" for i in range(540)]
        out = rotate_after(files, files[259])
        assert out[0] == files[260], out[0]
        assert out[:3] == files[260:263]

    def test_every_file_survives_the_rotation(self):
        files = [f"F{i}.parquet" for i in range(540)]
        assert sorted(rotate_after(files, "F259.parquet")) == sorted(files)

    def test_unknown_or_empty_bookmark_starts_at_the_top(self):
        files = ["A.parquet", "B.parquet", "C.parquet"]
        assert rotate_after(files, "") == files
        assert rotate_after(files, "DELETED.parquet") == files

    def test_wrapping_covers_the_original_prefix_again(self):
        """A full pass must eventually return to the head, or the PREFIX becomes the new tail."""
        files = [f"F{i}.parquet" for i in range(10)]
        assert rotate_after(files, "F8.parquet") == \
            ["F9.parquet"] + [f"F{i}.parquet" for i in range(9)]


class TestWiring:
    def test_update_rotates_its_file_list(self):
        assert "rotate_after(pfiles, load_rotation(out_dir))" in inspect.getsource(ecb.update)

    def test_bookmark_saved_after_the_deferral_branch(self):
        src = inspect.getsource(ecb.update)
        assert src.index("save_rotation(out_dir, fn)") > src.index("tally.deferred_unit"), \
            "saving before the deferral check would bookmark a file that was NOT worked on"


class TestSsbRotation:
    """ssb has the same defect: 186 sorted grp_* files under a 40-minute budget.

    MEASURED 2026-08-07 — a run of 2,401 s (exactly the budget) deferred 135 sub-units
    starting at Fbu03, a table inside grp_Fb, which is sorted index 53 of 186, then grp_Fe
    (54), grp_Fi (55), grp_Fj (56) and "+129 more". Roughly 53 of 186 groups were ever
    reached; the other ~71% had never been fetched.
    """

    def test_update_rotates_its_group_list(self):
        from updater.strategies.fetchers import ssb
        assert "rotate_after(pfiles, load_rotation(out_dir))" in inspect.getsource(ssb.update)

    def test_bookmark_saved_after_the_deferral_branch(self):
        from updater.strategies.fetchers import ssb
        src = inspect.getsource(ssb.update)
        assert src.index("save_rotation(out_dir, fn)") > src.index("group deferred")

    def test_the_deferred_group_leads_the_next_run(self):
        """Production shape: 186 sorted groups, budget stops after index 53."""
        groups = [f"grp_{c}{i:02d}.parquet" for c in "ABCDEF" for i in range(31)]
        out = rotate_after(groups, groups[53])
        assert out[0] == groups[54]
        assert sorted(out) == sorted(groups)
