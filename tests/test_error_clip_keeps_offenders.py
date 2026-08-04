"""The offender list must survive being stored (orchestrate._clip_err).

Two fixes existed that cancelled each other out. finalize() names the sub-units that failed —
"7/1096 sub-unit(s) ... [a.px, b.px, ...]" — precisely so a finding is actionable without a
bisect. The orchestrator then stored `str(e)[:300]`. For a source whose unit ids are paths the
message prefix alone is ~135 characters, so the list was cut off mid-token:

    ... [kosningar/sveitastjorn/svf_urslit/KOS03190.px, kosningar/.../KOS03190a.px,
    manntal/2011/1manntalfjolsk/CEN01560.px, manntal/2011/1manntalf

Four of seven, the fourth unusable, and nothing saying the rest were dropped — so it reads as
the complete list. Both the CI log line and the stored state row showed that same truncation.
"""
from updater.orchestrate import _clip_err


HAGSTOFA = (
    "hagstofa: 7/1096 sub-unit(s) returned 200 but parsed 0 rows from a non-trivial body "
    "(schema/structural break); existing data kept ["
    + ", ".join([
        "kosningar/sveitastjorn/svf_urslit/KOS03190.px",
        "kosningar/sveitastjorn/svf_urslit/KOS03190a.px",
        "manntal/2011/1manntalfjolsk/CEN01560.px",
        "manntal/2011/1manntalfjolsk/CEN01570.px",
        "manntal/2011/1manntalfjolsk/CEN01580.px",
        "manntal/2011/1manntalfjolsk/CEN01590.px",
        "manntal/2011/1manntalfjolsk/CEN01600.px",
    ]) + "]"
)


def test_a_real_offender_list_survives_intact():
    """THE regression: every one of the seven ids is still readable after storage."""
    out = _clip_err(HAGSTOFA)
    for px in ("KOS03190.px", "KOS03190a.px", "CEN01560.px", "CEN01570.px",
               "CEN01580.px", "CEN01590.px", "CEN01600.px"):
        assert px in out, f"{px} was clipped away"
    assert "truncated" not in out, "this message fits and must not be marked truncated"
    assert len(HAGSTOFA) > 300, "guard: the old 300-char cap really did cut this message"


def test_short_errors_are_returned_unchanged():
    for s in ("", "boom", "worldbank_wdi: 3/700 sub-unit(s) transient-failed; will retry"):
        assert _clip_err(s) == s


def test_over_long_errors_ANNOUNCE_the_truncation():
    """A silent clip reads as completeness — that is the whole defect. It must say so."""
    huge = "src: failed [" + ", ".join(f"unit_{i:04d}/path/to/table.px" for i in range(400)) + "]"
    out = _clip_err(huge)
    assert len(out) < len(huge)
    assert "truncated" in out
    assert "more chars" in out


def test_truncation_lands_on_a_whole_element():
    """Cutting mid-id produces a token that looks real and is not — worse than omitting it."""
    huge = "src: failed [" + ", ".join(f"aaa/bbb/unit_{i:04d}.px" for i in range(400)) + "]"
    out = _clip_err(huge)
    body = out.split("…")[0].rstrip()
    assert not body.endswith(","), "should not end on a dangling separator"
    last = body.rsplit(", ", 1)[-1]
    assert last.endswith(".px"), f"truncated mid-token: {last!r}"


def test_non_string_input_is_tolerated():
    """Callers pass exception objects, not strings."""
    assert _clip_err(ValueError("nope")) == "nope"
