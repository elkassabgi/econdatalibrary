"""Discriminating half of the catalog_coverage guard: prove each check can FAIL.

Run: py -3.14 tools/mutate_coverage_guard.py    (exit 0 = every scenario behaved)

tests/test_catalog_coverage_sync.py only ever demonstrates that the CURRENT, correct string
passes. That is the half of a guard's behaviour which proves nothing: "the three holders agree"
passes just as well when the extractor returns None for all three. R414 -- a guard ships with a
discriminating pair, one case it must block and one it must let through.

It lives in the repo because the test file cites it. A harness cited in shipped code and kept
in a scratch directory is unreproducible for the next reader, which review flagged.

Every scenario runs on temp copies; the repo is never touched.

Scenarios 6 and 7 exist because the first two versions of the guard were defeated:
  * the extractor stopped at a `;` INSIDE the old string literal, returned EMPTY, and the
    no-count and caveat checks then passed vacuously (R413's cannot-fail comparator);
  * a DECOY `const COVERAGE` planted in a /* */ comment was read in preference to the real
    one, so the guard passed while the deployed value was the original bug;
  * requiring only the word "absence" accepted the literal INVERSION of the caveat.
"""
import importlib.util
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
TEST = os.path.join(os.path.dirname(HERE), "tests", "test_catalog_coverage_sync.py")

TRUE = ("mixed grain: some sources are catalogued per series, others per table or flow — "
        "absence from this catalogue does not mean a series is unavailable")
STALE = "series-level for 33 sources; source-level for the rest"
UNIFORM = "series-level for every served source"
INVERTED = "series-level throughout; absence from this catalogue means a series is unavailable"

CHECKS = ("agree", "contract", "no_count", "caveat")


def _load(catalog_ts, devserver, contract):
    spec = importlib.util.spec_from_file_location("cov_guard_%d" % _load.n, TEST)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _load.n += 1
    mod.CATALOG_TS, mod.DEVSERVER, mod.CONTRACT = catalog_ts, devserver, contract
    return mod


_load.n = 0


def scenario(name, expect_fail, ts_body=None, ts_value=None, py_value=None, md_value=None):
    """expect_fail: the check that MUST reject this, or None if it must be accepted."""
    ts_value = TRUE if ts_value is None else ts_value
    py_value = ts_value if py_value is None else py_value
    md_value = ts_value if md_value is None else md_value
    body = ts_body if ts_body is not None else 'const COVERAGE = "%s";\n' % ts_value

    d = tempfile.mkdtemp()
    ts, py, md = (os.path.join(d, f) for f in ("catalog.ts", "devserver.py", "CONTRACT.md"))
    open(ts, "w", encoding="utf-8").write(body)
    open(py, "w", encoding="utf-8").write('_CATALOG_COVERAGE = "%s"\n' % py_value)
    open(md, "w", encoding="utf-8").write('x `"catalog_coverage":"%s"` y\n' % md_value)

    mod = _load(ts, py, md)
    fns = {
        "agree": mod.test_worker_and_devserver_agree,
        "contract": mod.test_contract_documents_the_deployed_string,
        "no_count": mod.test_string_embeds_no_count,
        "caveat": mod.test_string_keeps_the_absence_caveat,
    }
    failed = set()
    for key, fn in fns.items():
        try:
            fn()
        except Exception:
            failed.add(key)
    shutil.rmtree(d, ignore_errors=True)

    ok = (expect_fail in failed) if expect_fail else (not failed)
    print("  [%s] %-46s failed=%s"
          % ("PASS" if ok else "MISS", name, sorted(failed) or "none"))
    return ok


def main():
    print("Discriminating pair for the catalog_coverage guard:\n")
    r = [
        # ---- must be ACCEPTED -------------------------------------------------------
        scenario("the real, correct string", None),
        scenario("same value written as a `+` concatenation", None,
                 ts_body='const COVERAGE =\n  "%s" +\n  "%s";\n'
                         % (TRUE[:40], TRUE[40:])),

        # ---- must be REFUSED, each by its own check ---------------------------------
        scenario("dev shim drifts from the worker", "agree", py_value=UNIFORM),
        scenario("contract documents a different string", "contract",
                 md_value="something else entirely"),
        scenario("string embeds a count (the 33-source rot)", "no_count", ts_value=STALE),
        scenario("claims uniform coverage (the false repair)", "caveat", ts_value=UNIFORM),
        scenario("drops the absence caveat entirely", "caveat",
                 ts_value="series and table grain"),

        # ---- the two bypasses adversarial review found ------------------------------
        scenario("DECOY: honest value hidden in a /* */ comment", "no_count",
                 ts_body='/* const COVERAGE = "%s"; */\nconst COVERAGE = "%s";\n'
                         % (TRUE, STALE)),
        scenario("DECOY: honest value hidden in a // comment", "no_count",
                 ts_body='// const COVERAGE = "%s";\nconst COVERAGE = "%s";\n'
                         % (TRUE, STALE)),
        scenario("INVERTED caveat (says absence DOES mean gone)", "caveat",
                 ts_value=INVERTED),
        scenario("weasel: 'series-level throughout' + the word absence", "caveat",
                 ts_value="series-level throughout; absence is not nonexistence"),
    ]
    print("\n%d/%d scenarios behaved as required." % (sum(r), len(r)))
    return 0 if all(r) else 1


if __name__ == "__main__":
    sys.exit(main())
