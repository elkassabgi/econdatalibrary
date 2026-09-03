"""`_named` must bound the RENDERED LENGTH, not only the label count.

Its docstring reasoned the count cap out loud, and the reasoning had a premise:

    "Twenty path-shaped ids run ~900 characters, comfortably inside 1400 with the message
     prefix; beyond that the orchestrator's clip takes over and says so."

That held while a label was an ID. On 2026-09-03 roughly forty call sites were labelled with the
REASON as well — an exception type and message, truncated per site — which is the point of them,
and which took twenty labels to ~3,200 characters. Measured before the fix:

     5 failed sub-units      828 chars   ok
    10                     1,637        past the orchestrator's 1,400 clip
    20                     3,195        far past it

Nothing breaks at 3,200 — `unit_state.last_error` and `runs.note` store it, and `gen_runbook`
announces its truncation — but a 3,200-character cell in the morning email is unreadable, and an
unreadable alert is the failure the labels exist to prevent.

BOTH LIMITS NOW APPLY. The count still binds first when labels are short, which is what keeps
wid's 12 and hagstofa's 7 rendering complete — the property the cap was raised from 6 to 20 for
on 2026-08-04, and which a character budget must not quietly undo.
"""
from __future__ import annotations

from updater.strategies.fetchers._common import _named

CLIP = 1400          # orchestrate._clip_err, the binding downstream limit


def test_short_ids_still_render_complete() -> None:
    """The wid/hagstofa case: 12 path-shaped ids, all of them, no elision."""
    ids = [f"1_natturufar/2_vedurfar/UMH{1000 + i}.px" for i in range(12)]
    out = _named(ids)
    assert out.count("UMH") == 12, out
    assert "more" not in out, f"elided a set that fits: {out}"
    assert len(out) < CLIP


def test_long_labels_are_bounded_by_characters_not_only_count() -> None:
    """THE REGRESSION. Twenty 150-char labels must not render 3,200 characters."""
    ids = ["x" * 150 for _ in range(20)]
    out = _named(ids)
    assert len(out) < CLIP, (
        f"{len(out)} characters — past the {CLIP}-char clip. The count cap alone does not "
        f"bound length once labels carry reasons as well as ids."
    )
    assert "more" in out, "an elision must be stated, not silent"


def test_the_elision_counts_what_it_dropped() -> None:
    ids = ["y" * 150 for _ in range(20)]
    out = _named(ids)
    kept = out.count("y" * 150)
    assert f"+{20 - kept} more" in out, f"kept {kept} but the note says: {out[-24:]}"


def test_a_single_oversized_label_is_still_shown() -> None:
    """One truncated label is more use than none, so the budget never returns an empty list."""
    out = _named(["z" * 5000])
    assert out.startswith(" [z"), out[:40]
    assert "more" not in out, "there was nothing else to omit"


def test_empty_stays_empty() -> None:
    assert _named([]) == ""
    assert _named(None) == ""
