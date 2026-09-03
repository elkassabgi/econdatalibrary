"""Run, in five minutes, everything CI actually exercises — so a push can be checked before it.

WHY THIS EXISTS. On 2026-09-03 I pushed nine times with `main` red and did not know, because
after each change I ran the test I had just written instead of the repo's own command. Ledger
R685. The rule that came out of it — "run the repo's CI command before pushing" — is only
followable if that is practical, and it is not: `pytest tests/ -q` takes **57 minutes** on this
workstation.

It is slow for a reason that does not apply to CI. Two files are data-bound:

    tests/test_eurostat_value_dimension.py    globs 7,638 .tsv.gz / 10.0 GB from data/raw/eurostat
    tests/test_series_carveout_coverage.py    reads the 11.9 GB catalog.db

and BOTH SKIP when their data is absent:

    test_eurostat_value_dimension.py:100  pytest.skip("raw eurostat mirror not present")
    test_series_carveout_coverage.py:86   skipif(not os.path.exists(CATALOG))

The GitHub runner has neither, so on CI they contribute nothing. Deselecting them locally
reproduces CI's SELECTION at 4m54s instead of 57m — measured, both numbers.

IT DOES NOT REPRODUCE CI'S ENVIRONMENT, and that distinction cost forty red runs. CI pins Python
3.11; this workstation runs 3.14. `tests/test_blob_skip_identical.py` built a fixture with
`gzip.compress(data, mtime=0)` while production uses `r2_util.gzip_bytes`, and those two agree
only on 3.14 — 3.11 leaves zlib's build platform in the gzip header's OS byte where 3.14 forces
255. This tool printed PASS for forty pushes while `tests` was red on every one of them. A PASS
here means "the tests CI selects pass ON THIS INTERPRETER", which is weaker than it reads, so the
banner below prints both versions on every run.

    full        1,945 collected   1 failed, 1944 passed   3,445 s
    this tool   1,940 collected   1,928 passed, 12 desel    293 s

WORTH SAYING PLAINLY: those two files therefore protect NOTHING on CI. Their guarantee exists
only when someone runs them on a machine holding the data, which is this one, and rarely. That is
a real gap and it is not this tool's job to hide it — run `--full` before anything that touches
eurostat parsing or the carve-out.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Deselected by default: data-bound, and skipped on CI because the data is not there.
DATA_BOUND = [
    ("tests/test_eurostat_value_dimension.py", "globs 7,638 .tsv.gz / 10.0 GB"),
    ("tests/test_series_carveout_coverage.py", "reads the 11.9 GB catalog.db"),
]


def _ci_python() -> str:
    """The version CI pins, read from the workflow rather than remembered."""
    wf = os.path.join(ROOT, ".github", "workflows", "tests.yml")
    try:
        with open(wf, encoding="utf-8") as fh:
            m = re.search(r"python-version:\s*['\"]?([0-9.]+)", fh.read())
        return m.group(1) if m else "unknown"
    except OSError:
        return "unknown"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--full", action="store_true",
                    help="run everything, including the data-bound files (~57 minutes)")
    ap.add_argument("rest", nargs="*", help="extra args passed straight to pytest")
    a = ap.parse_args()

    cmd = [sys.executable, "-m", "pytest", "tests/", "-q"]
    if not a.full:
        for path, _ in DATA_BOUND:
            cmd += ["--deselect", path]
    cmd += a.rest

    if a.full:
        print("running the FULL suite, including the data-bound files (~57 min)\n", flush=True)
    else:
        print("running CI's real coverage; deselected because CI skips them anyway:", flush=True)
        for path, why in DATA_BOUND:
            print(f"    {path}  — {why}", flush=True)
        print("  (use --full before touching eurostat parsing or the carve-out)\n", flush=True)

    t0 = time.time()
    rc = subprocess.run(cmd, cwd=ROOT).returncode
    local = f"{sys.version_info.major}.{sys.version_info.minor}"
    ci = _ci_python()
    if ci != "unknown" and not ci.startswith(local):
        print(f"\nNOTE: this ran on Python {local}; CI pins {ci}. A PASS here does NOT cover "
              f"version-sensitive behaviour —\n      forty red runs hid behind exactly that gap "
              f"(gzip header bytes differ between 3.11 and 3.14).", flush=True)
    print(f"\n{'PASS' if rc == 0 else 'FAIL'} in {time.time() - t0:.0f}s "
          f"(exit {rc}) — {' '.join(cmd[2:])}")
    if rc != 0:
        print("\nDo not push. R685: nine pushes went out with main red because only the newly "
              "written test was run.")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
