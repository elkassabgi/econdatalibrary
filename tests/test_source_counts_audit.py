"""`/v1/catalog`'s `total` comes from a cache nothing validated. These pin the validator.

WHY THIS FILE EXISTS. On 2026-09-04 `noaa`'s `source_counts` row read 3,138,201 against a true
3,138,159 — served to users for at least a day, while the static catalogue page published the
correct figure, so the site and the API disagreed about the same source. There were 31
`tools/audit_*.py` and none covered that table. `audit_d1_source_counts.py` is the answer, and an
adversarial review found EIGHT defects in it within the hour. Two of them lived in this comparison
and both were found by running the tool, not by reading it — which is exactly the kind of fault a
test is for.

The two, restated as the properties below:

  1. ABSENT AND ZERO ARE THE SAME THING. Comparing `cv != tv` made `None != 0` a mismatch, so the
     first run after the source set was widened reported 27 not-yet-ingested sources
     (central_banks, cftc, gii, pxweb ...) as findings of +0 and buried the four real ones. The
     fix must NOT also swallow the case that matters: a source with rows and NO cache row is the
     `vdem` shape — ECONLIB_COMPLETION_PLAN.md:77, a live COUNT(*) of 783,100 rows per page view.

  2. THE TWO MODES KEY DIFFERENTLY. Remote truth is per (database, source): a source present in
     BOTH catalogue databases is the state a botched shard migration leaves, and keying by
     source_id alone would let the second silently overwrite the first — reporting agreement for
     the one case the audit most needs to catch. Local truth is per source, so the cache is summed
     across databases instead.

R702 is the reason the fleet is two databases in every one of these cases: counting only
`econ-catalog` understates by the whole climate shard.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.audit_d1_source_counts import diff_counts  # noqa: E402

PRIMARY = "econ-catalog"
CLIMATE = "econ-catalog-climate"


class AbsentAndZero(unittest.TestCase):
    def test_no_cache_row_and_no_rows_is_not_a_mismatch(self):
        """A registered but never-ingested source is not a finding."""
        bad = diff_counts(cache={}, truth={"cftc": 0}, homes={}, remote=False)
        self.assertEqual(bad, [], "absent cache over zero rows must not be reported")

    def test_cache_row_of_zero_and_no_rows_is_not_a_mismatch(self):
        bad = diff_counts(cache={(PRIMARY, "cftc"): 0}, truth={"cftc": 0},
                          homes={"cftc": [PRIMARY]}, remote=False)
        self.assertEqual(bad, [])

    def test_absent_cache_WITH_rows_is_still_reported(self):
        """The expensive class must survive the fix that silenced the noise.

        No cache row means the worker falls through to a live COUNT(*) for that source on every
        page view. Silencing this to quiet the 27 false positives would have removed the only
        reason the check earns its place.
        """
        bad = diff_counts(cache={}, truth={"vdem": 783_100}, homes={}, remote=False)
        self.assertEqual(len(bad), 1)
        self.assertEqual(bad[0][1], "vdem")
        self.assertIsNone(bad[0][2])
        self.assertEqual(bad[0][3], 783_100)

    def test_cache_zero_with_rows_is_reported(self):
        """statcan after the aborted push: total:0 served beside a non-empty page (R709)."""
        bad = diff_counts(cache={(PRIMARY, "statcan"): 0}, truth={"statcan": 466_341},
                          homes={"statcan": [PRIMARY]}, remote=False)
        self.assertEqual([(b[1], b[2], b[3]) for b in bad], [("statcan", 0, 466_341)])

    def test_the_noaa_drift_itself(self):
        bad = diff_counts(cache={(CLIMATE, "noaa"): 3_138_201}, truth={"noaa": 3_138_159},
                          homes={"noaa": [CLIMATE]}, remote=False)
        self.assertEqual(len(bad), 1)
        self.assertEqual(bad[0][2] - bad[0][3], 42)


class ModeKeying(unittest.TestCase):
    def test_remote_mode_keys_by_database_and_does_not_collapse_a_split_source(self):
        """A source in BOTH databases must be compared per database, not merged.

        Keyed by source_id alone the climate entry would overwrite the primary one and this would
        report agreement — for precisely the state a botched shard migration leaves behind.
        """
        cache = {(PRIMARY, "noaa"): 10, (CLIMATE, "noaa"): 3_138_159}
        truth = {(PRIMARY, "noaa"): 0, (CLIMATE, "noaa"): 3_138_159}
        bad = diff_counts(cache, truth, homes={}, remote=True)
        self.assertEqual(len(bad), 1, "the primary's stray 10 rows must be reported")
        self.assertEqual((bad[0][0], bad[0][2], bad[0][3]), (PRIMARY, 10, 0))

    def test_remote_mode_clean_fleet_reports_nothing(self):
        cache = {(PRIMARY, "bea"): 913_230, (CLIMATE, "noaa"): 3_138_159}
        truth = {(PRIMARY, "bea"): 913_230, (CLIMATE, "noaa"): 3_138_159}
        self.assertEqual(diff_counts(cache, truth, homes={}, remote=True), [])

    def test_local_mode_sums_the_cache_across_databases(self):
        """Local truth is per source, so the cache must be summed rather than picked from one."""
        cache = {(PRIMARY, "noaa"): 10, (CLIMATE, "noaa"): 3_138_159}
        bad = diff_counts(cache, truth={"noaa": 3_138_169},
                          homes={"noaa": [PRIMARY, CLIMATE]}, remote=False)
        self.assertEqual(bad, [], "10 + 3,138,159 equals the local count, so nothing is wrong")


class ExitCodeTrichotomy(unittest.TestCase):
    """0 = agree, 1 = a cached count disagrees, 2 = could not look.

    `_wrangler()` used to raise SystemExit when no binary was found. SystemExit derives from
    BaseException, so main()'s `except Exception` never caught it and the process exited 1 — the
    code meaning "at least one cached count disagrees". A could-not-look reporting as a finding is
    R704 inverted, and it is a live CI path: wrangler is installed by the `npm ci` inside the
    "Sync freshness to D1" step, which is skipped whenever the state push fails.
    """

    def test_missing_wrangler_raises_a_catchable_exception(self):
        import tools.audit_d1_source_counts as mod
        real_exists, real_which = os.path.exists, mod.shutil.which
        mod.os.path.exists = lambda p: False
        mod.shutil.which = lambda p: None
        try:
            with self.assertRaises(Exception) as ctx:
                mod._wrangler()
            self.assertNotIsInstance(ctx.exception, SystemExit,
                                     "SystemExit escapes `except Exception` and exits 1, which "
                                     "this tool defines as a FINDING rather than a failure")
        finally:
            mod.os.path.exists, mod.shutil.which = real_exists, real_which


if __name__ == "__main__":
    unittest.main()
