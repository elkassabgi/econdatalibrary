"""An instrument cited in shipped code must ship with it (R808 rule 3).

`tools/guard_heartbeat.py` justifies its `RELAUNCH_WINDOW_S` constant with measured cadences and
names the instrument that produced them. Review R808 finding (5) was that the comment cited
`scratchpad/measure_relaunch_cadence.py`, "which does not exist in the repo or in git" — a
per-session temp path a reader cannot open. The tool was added to `tools/`, but the citation was
never redirected, so the PR that claimed R808 "answered" still shipped the dangling reference.

The number in that comment is the whole reason the window is 330 s rather than a guess, so the
citation is load-bearing documentation, not decoration: without a reachable instrument the next
session either trusts the figure blindly or re-derives it, and the last time a cadence was carried
across without measuring it was wrong by a factor of ~25 (gus_dbw "every ~53 min" against a
measured 22.2 h, R804 #4).

Scoped deliberately to this one file. A fleet-wide sweep for the same shape finds five shipped
modules citing `scratchpad/` paths, two of them hardcoding another session's temp directory — that
is a real and separate defect (the R330 dead-path class) and belongs to its own change, not to a
guard-heartbeat PR.
"""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET = os.path.join(ROOT, "tools", "guard_heartbeat.py")

# A citation is a bare relative path to a .py/.ps1 the reader is told to consult.
_CITED = re.compile(r"(?<![\w/.-])((?:[A-Za-z_][\w.-]*/)+[A-Za-z_][\w.-]*\.(?:py|ps1))")

# Paths that are runtime data or documented external locations, not instruments to open.
_NOT_AN_INSTRUMENT = ("logs/", "data/", "_aqueduct/", "http://", "https://")


def _citations(text):
    out = set()
    for m in _CITED.finditer(text):
        p = m.group(1)
        if any(p.startswith(x) or x in p for x in _NOT_AN_INSTRUMENT):
            continue
        out.add(p)
    return out


def test_guard_heartbeat_cites_no_per_session_scratchpad_path():
    """No citation may point into a scratchpad — that is the defect, stated exactly.

    An earlier draft of this test asserted that EVERY cited path exists, and it failed on
    `jobs/ingest_x.py` — an illustrative placeholder in a docstring, not a citation. A check
    that cries wolf on its first run gets disabled, so it is narrowed to the class R808 is
    about: a path under `scratchpad/`, which is per-session by construction and can never be
    opened by a later reader.
    """
    src = open(TARGET, encoding="utf-8").read()
    cited = _citations(src)
    assert cited, "found no cited path at all — the regex has stopped matching, not the file"
    ephemeral = sorted(p for p in cited if "scratchpad/" in p)
    assert not ephemeral, (
        "guard_heartbeat.py cites per-session scratchpad path(s): "
        + ", ".join(ephemeral)
        + " — R808 rule 3: an instrument cited in shipped code must ship with it."
    )


def test_the_measured_cadence_instrument_is_named_and_present():
    """The specific citation R808 was opened about, pinned by name.

    The general test above passes if the citation is deleted entirely; this one fails in that
    case, so the pair distinguishes "fixed" from "removed the evidence".
    """
    src = open(TARGET, encoding="utf-8").read()
    assert "measure_relaunch_cadence.py" in src, (
        "the cadence figures in RELAUNCH_WINDOW_S's comment no longer name their instrument"
    )
    assert "scratchpad/measure_relaunch_cadence.py" not in src, (
        "the citation still points at a per-session scratchpad path (R808 finding 5)"
    )
    assert os.path.exists(os.path.join(ROOT, "tools", "measure_relaunch_cadence.py")), (
        "tools/measure_relaunch_cadence.py is cited but absent from the repo"
    )
