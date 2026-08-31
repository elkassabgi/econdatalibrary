"""The §5.7 no-cursors debt must PERSIST until a completed campaign clears it.

Before 2026-08-31 the debt evaporated: the no-cursors branch demoted the run and returned a
note, nothing was queued (ids unknown), the fetcher's vintage sidecar was already written, so
the next run skipped as unchanged and reported clean — which is how noaa served 3,138,159
CSVs one restatement behind with every instrument green.

Both directions per R414: the marker must be written by the branch's caller-side hook and
survive; and ONLY the campaign stamp clears it — an unrelated source's stamp must not.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402


def test_marker_persists_and_only_the_right_stamp_clears(tmp_path):
    st = StateStore(path=str(tmp_path / "state.db"))

    st.note_full_rederive_owed("noaa", vintage="noaa:abc123", note="csv coherence unmet…")
    owed = st.full_rederives_owed()
    assert [r["source_id"] for r in owed] == ["noaa"]
    assert owed[0]["vintage"] == "noaa:abc123"

    # Re-noting updates in place (one row per source, never a growing pile).
    st.note_full_rederive_owed("noaa", vintage="noaa:def456", note="again")
    owed = st.full_rederives_owed()
    assert len(owed) == 1 and owed[0]["vintage"] == "noaa:def456"

    # An unrelated clear must not touch it.
    st.clear_full_rederive_owed("norgesbank")
    assert len(st.full_rederives_owed()) == 1

    st.clear_full_rederive_owed("noaa")
    assert st.full_rederives_owed() == []


def test_the_note_constant_is_shared_not_retyped():
    """The branch returns the note built from _NO_CURSORS_NOTE and the caller matches on the
    SAME constant — R142's marker rule. If either side re-types the string, this fails."""
    src = open(os.path.join(os.path.dirname(orchestrate.__file__), "orchestrate.py"),
               encoding="utf-8").read()
    assert src.count("_NO_CURSORS_NOTE") >= 3, (
        "expected the shared constant at: definition, the branch's return, and the "
        "caller's startswith match"
    )
    # And the literal must appear exactly once — in the constant's definition.
    assert src.count('"csv coherence unmet: fetcher reported no series_cursors"') == 1


def test_no_cursors_branch_returns_the_shared_note():
    """Drive the actual branch: obs merged, no cursors, catalogued > 0."""
    class _Unit:
        source_id = "testsrc"
        unit_id = "_all"
        key = "testsrc/_all"

    class _Res:
        obs = 42
        series_cursors = None
        new_vintage = "v1"

    import unittest.mock as mock
    with mock.patch.object(orchestrate, "_catalog_series_count", return_value=10,
                           create=True):
        failed, note, deferred, reasons = orchestrate._derive_changed_csvs(
            _Unit(), _Res(), blob=None)
    assert failed == [] and deferred == [] and reasons == {}
    assert note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)
