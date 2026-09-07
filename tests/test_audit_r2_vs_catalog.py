"""R2 vs the catalogue: the leg the store audit cannot see.

WHY. `tools/audit_store_vs_catalog.py` compares the CATALOGUE against the LOCAL PARQUET STORE.
Neither is what a user receives, and it cannot see R2 at all - which is how it came to print
"hosted but not catalogued" about data that had never been published (R834), the mirror of its
ORPHAN-means-404 error a day earlier (R825). Three quantities, three answers: what we HOLD, what
we HOST, what we LIST.

Pinned here, all from both sides:

  agree                       R2 objects == catalogue rows
  objects with no row         published but unlisted
  rows with no object         listed ids that 404 - the one that reaches a user as an error
  truncated at --max          NEVER reported as a total
  a prefix that will not list UNCHECKED, never clean (R390)
"""
from __future__ import annotations

import importlib.util
import io
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(os.path.dirname(_HERE), "tools", "audit_r2_vs_catalog.py")


def _load():
    spec = importlib.util.spec_from_file_location("_r2_audit_under_test", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class FakeS3:
    """Pages like list_objects_v2 does: 1,000 keys per page, a continuation token until the end."""

    def __init__(self, counts, fail=()):
        self.counts, self.fail = counts, set(fail)

    def list_objects_v2(self, Bucket, Prefix, MaxKeys, ContinuationToken=None):  # noqa: N803
        for src, n in self.counts.items():
            if f"{src}%3A" in Prefix:
                if src in self.fail:
                    raise RuntimeError("simulated listing failure")
                seen = int(ContinuationToken or 0)
                page = min(MaxKeys, max(0, n - seen))
                out = {"KeyCount": page}
                if seen + page < n:
                    out["NextContinuationToken"] = str(seen + page)
                return out
        return {"KeyCount": 0}


def _run(m, s3, cat, argv):
    m.catalogue_counts = lambda: dict(cat)
    import types
    fake_core = types.ModuleType("core")
    fake_r2 = types.ModuleType("core.r2_util")
    fake_r2.client = lambda: s3
    fake_core.r2_util = fake_r2
    sys.modules["core"], sys.modules["core.r2_util"] = fake_core, fake_r2
    old = sys.argv
    sys.argv = ["audit_r2_vs_catalog"] + argv
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        rc = m.main()
    finally:
        sys.stdout = real
        sys.argv = old
        sys.modules.pop("core", None)
        sys.modules.pop("core.r2_util", None)
    return rc, buf.getvalue()


def test_agreement_is_reported_as_agreement():
    m = _load()
    rc, out = _run(m, FakeS3({"abs": 18}), {"abs": 18}, ["abs"])
    assert rc == 0
    assert "agree" in out and "+0" in out, out
    assert "1 source(s) where R2 and the catalogue agree; 0 where they do not." in out, out


def test_objects_with_no_catalogue_row_are_named():
    """Published but unlisted - hosted data nobody can find."""
    m = _load()
    # 3,135,873 exceeds the DEFAULT --max of 2,000,000, and the tool correctly refuses to call
    # a truncated count a total - so this case must raise the cap, which also proves --max is
    # honoured in both directions.
    _rc, out = _run(m, FakeS3({"noaa": 3_135_873}), {"noaa": 0},
                    ["noaa", "--max", "5000000"])
    assert "OBJECTS WITH NO CATALOGUE ROW" in out and "published, unlisted" in out, out
    assert "+3,135,873" in out, out


def test_catalogue_rows_with_no_object_are_named():
    """The direction that reaches a user as a 404."""
    m = _load()
    _rc, out = _run(m, FakeS3({"x": 10}), {"x": 25}, ["x"])
    # NOT 404. api/worker/src/series.ts pins the honest-status tree: an id absent from the
    # CATALOGUE is 404 not_found, but a catalogued id whose OBJECT is absent is
    # 502 data_unavailable - "loud + actionable, never an empty 200". Two different states, and
    # calling the second a 404 is a served-system claim made from a local measurement (R825).
    assert "CATALOGUE ROWS WITH NO OBJECT" in out, out
    assert "502 data_unavailable" in out, out
    assert "404" not in out, out
    assert "-15" in out, out


def test_truncation_is_never_reported_as_a_total():
    """A bounded count that reads as complete is the defect --max exists to avoid."""
    m = _load()
    _rc, out = _run(m, FakeS3({"big": 50_000}), {"big": 1}, ["big", "--max", "3000"])
    assert "STOPPED at --max" in out and "not a total" in out, out
    # and it must NOT have produced a difference or a verdict for that row
    assert "agree" not in out.split("STOPPED")[0].split("big")[-1], out


def test_a_prefix_that_will_not_list_is_UNCHECKED_not_clean():
    """R390 - a guard that cannot evaluate must say so; a dropped source is indistinguishable
    from a clean one."""
    m = _load()
    _rc, out = _run(m, FakeS3({"bad": 5}, fail=["bad"]), {"bad": 5}, ["bad"])
    assert "UNCHECKED" in out and "LIST FAILED" in out, out
    assert "agree" not in out.split("\n")[1], out
    # a failed listing must not be counted as agreement
    assert "0 source(s) where R2 and the catalogue agree" in out, out


def test_paging_counts_every_page():
    """The count must survive continuation tokens - an off-by-one page is a silent undercount."""
    m = _load()
    # THE ASSERTIONS HERE WERE DECORATION, AND A MUTANT PROVED IT. With paging broken the
    # tool returns page one only (1,000 of 2,501) - but "2,501" STILL appears, in the
    # CATALOGUE column, and "agree" STILL appears, inside the summary line "0 source(s) ...
    # agree". Both original assertions passed against the broken pager. A guard is defeated
    # where it READS (R525), so read the row itself.
    _rc, out = _run(m, FakeS3({"p": 2_501}), {"p": 2_501}, ["p"])
    assert "1 source(s) where R2 and the catalogue agree; 0 where they do not." in out, out
    assert "CATALOGUE ROWS WITH NO OBJECT" not in out, out
    row = [ln for ln in out.splitlines() if ln.strip().startswith("p ")][0]
    assert row.split() == ["p", "2,501", "2,501", "+0", "agree"], row


def test_refuses_with_no_sources_named():
    m = _load()
    rc, out = _run(m, FakeS3({}), {}, [])
    assert rc == 2 and "name at least one source" in out, out

def test_a_truncated_source_is_disclosed_in_the_SUMMARY_not_just_its_row():
    """The fleet run printed "315 agree; 4 do not" over 322 sources. 315 + 4 = 319 — two prefixes
    that stopped at --max and one that would not list were each named on their own line and then
    vanished from the arithmetic.

    That is the failure --max exists to prevent, one level up: a bounded pass reading as full
    coverage. `agree + disagree` is NOT the number of sources asked about, and the summary must
    say so."""
    m = _load()
    _rc, out = _run(m, FakeS3({"big": 50_000, "ok": 5}), {"big": 1, "ok": 5},
                    ["big", "ok", "--max", "3000"])
    assert "NOT MEASURED: 1 stopped at --max" in out, out
    assert "1 of 2 sources were actually compared" in out, out
    assert "re-run with a larger --max" in out, out


def test_an_unlistable_source_is_disclosed_in_the_SUMMARY_too():
    """Same rule for the other way a source goes unmeasured."""
    m = _load()
    _rc, out = _run(m, FakeS3({"bad": 5, "ok": 5}, fail=["bad"]), {"bad": 5, "ok": 5},
                    ["bad", "ok"])
    assert "NOT MEASURED:" in out and "1 could not be listed" in out, out
    assert "1 of 2 sources were actually compared" in out, out
    assert "listing failed: RuntimeError" in out, out


def test_a_clean_run_prints_no_NOT_MEASURED_line():
    """The disclosure must not fire when nothing was missed, or it becomes noise people learn to
    skip — which is how a real one gets missed."""
    m = _load()
    _rc, out = _run(m, FakeS3({"a": 5, "b": 7}), {"a": 5, "b": 7}, ["a", "b"])
    assert "NOT MEASURED" not in out, out
    assert "2 source(s) where R2 and the catalogue agree; 0 where they do not." in out, out
