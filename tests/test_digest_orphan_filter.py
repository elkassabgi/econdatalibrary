"""The digest's orphan filter must work when the workflow's own command is used.

WHAT WENT WRONG. `send_digest.py` scopes its verdict to sources the registry still manages, and
its own comment explains why:

    "Those leftovers kept their last status forever, so the digest reported de-registered
     sources as failing every single day - norgesbank and unsdg were both counted as `partial`
     while being entirely unmanaged (no registry entry, hence never re-run and never able to
     recover)."

The lookup was `from . import registry` — a RELATIVE import — and
`.github/workflows/updater-daily.yml:451` runs the module as `python updater/send_digest.py`, a
SCRIPT. `__package__` is then empty, the relative import raises ImportError, the surrounding
`except Exception` swallows it, `managed` becomes None, and the filter never runs. The fix has
therefore never been in effect on a single scheduled digest.

Evidence it was live: `fred_releases` was removed from registry.yaml in July, cannot be
scheduled, was last attempted 71 days ago — and appeared in the attention list, while the
"unmanaged leftover state row(s)" line the code prints for orphans did not appear at all.

WHY THIS TEST INVOKES A SUBPROCESS. Importing the module would resolve the package correctly and
prove nothing; the defect lives entirely in HOW CI invokes it. R249 and R684: call what
production calls, and verify by reverting the fix and watching the test fail. That reversion was
run — with `from . import registry` restored as the only lookup, this test fails.
"""
from __future__ import annotations

import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, "updater", "send_digest.py")
STATE = os.path.join(ROOT, "data", "_aqueduct", "state.db")


def _run_as_the_workflow_does() -> str:
    """Exactly `.github/workflows/updater-daily.yml` line 451, minus the API key."""
    env = dict(os.environ)
    env.pop("RESEND_API_KEY", None)          # no key -> prints and skips sending
    env["RUN_STATUS"] = "failure"
    p = subprocess.run([sys.executable, SCRIPT], cwd=ROOT, env=env,
                       capture_output=True, text=True, timeout=300)
    return (p.stdout or "") + (p.stderr or "")


@pytest.mark.skipif(not os.path.exists(STATE), reason="needs the local state store")
def test_orphans_are_separated_when_run_as_a_script() -> None:
    out = _run_as_the_workflow_does()
    assert "[digest]" in out, f"the digest did not run:\n{out[-800:]}"
    assert "unmanaged leftover state row(s)" in out, (
        "the orphan filter did not run. It is disabled whenever the registry lookup fails, and "
        "a RELATIVE import fails under the workflow's own `python updater/send_digest.py`. "
        "De-registered sources are then reported as failing every morning.\n\n" + out[-1200:]
    )


@pytest.mark.skipif(not os.path.exists(STATE), reason="needs the local state store")
def test_a_known_deregistered_source_is_not_listed_as_needing_attention() -> None:
    """fred_releases is the worked example: removed from the registry, so unschedulable.

    Anchored on the ORPHAN LINE rather than on fred_releases staying broken — if it is ever
    re-registered this must not turn into a false failure, so the assertion is that whatever
    orphans exist are named in the orphan line and not in the attention rows.
    """
    out = _run_as_the_workflow_does()
    orphan_line = next((l for l in out.splitlines()
                        if "unmanaged leftover state row(s)" in l), "")
    assert orphan_line, "no orphan line to check against"

    named = orphan_line.split(":")[-1]
    orphans = {s.strip().rstrip(")") for s in named.split(",") if s.strip()}
    assert orphans, f"orphan line named nobody: {orphan_line!r}"

    attention = [l for l in out.splitlines() if l.lstrip().startswith("!!")]
    for line in attention:
        who = line.split()[1]
        assert who not in orphans, (
            f"{who} is listed as needing attention AND as an unmanaged leftover — the filter "
            f"ran but did not exclude it from the counts"
        )
