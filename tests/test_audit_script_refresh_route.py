"""The coverage audit may count a guard-run script as a refresh route only on EVIDENCE.

This module exists because of a specific past error: matrix membership was once counted as
"scheduled", and two sources in the matrix turned out to be structurally incapable of running
("PENDING — no adapter built"), so coverage was overstated. cbs_nl and gus_dbw now genuinely
refresh via scripts the guard relaunches rather than via fetcher modules (R475), and the
tempting fix was a registry field declaring that. A declaration is exactly what failed before.

So the route counts only when the mechanism has left an artifact that cannot exist unless it
actually operated. These tests pin that the check is existence-based and that a source with no
artifact stays stranded — the honest answer for a mechanism that has never run.
"""
from __future__ import annotations

import os

import pytest

from tools import audit_schedule_coverage as A


def test_a_source_with_no_evidence_artifact_is_not_counted(tmp_path, monkeypatch):
    monkeypatch.setattr(A, "ROOT", str(tmp_path))
    monkeypatch.setattr(A, "SCRIPT_REFRESH_EVIDENCE", {"demo": os.path.join("d", "state.json")})
    assert A._script_refresh_proven("demo") is None


def test_a_source_whose_mechanism_has_run_is_counted(tmp_path, monkeypatch):
    monkeypatch.setattr(A, "ROOT", str(tmp_path))
    rel = os.path.join("d", "state.json")
    monkeypatch.setattr(A, "SCRIPT_REFRESH_EVIDENCE", {"demo": rel})
    os.makedirs(tmp_path / "d")
    (tmp_path / "d" / "state.json").write_text("{}", encoding="utf-8")
    assert A._script_refresh_proven("demo") == rel


def test_an_unlisted_source_never_qualifies(tmp_path, monkeypatch):
    """No source gets this route by accident — it is an explicit, per-source allowance."""
    monkeypatch.setattr(A, "ROOT", str(tmp_path))
    monkeypatch.setattr(A, "SCRIPT_REFRESH_EVIDENCE", {})
    assert A._script_refresh_proven("anything") is None


@pytest.mark.parametrize("sid", ["cbs_nl", "gus_dbw"])
def test_the_evidence_files_are_written_by_refresh_not_by_backfill(sid):
    """If the backfill wrote these, existence would prove nothing.

    cbs_nl's manifest is written by record_modified, called only from the freshness gate;
    gus_dbw's state is written by mark_refreshed, called only after an area is rebuilt.
    """
    rel = A.SCRIPT_REFRESH_EVIDENCE[sid]
    assert rel, "every listed source needs an artifact path"
    name = os.path.basename(rel)
    root = os.path.dirname(os.path.dirname(os.path.abspath(A.__file__)))
    if sid == "cbs_nl":
        src = open(os.path.join(root, "jobs", "ingest_cbs_nl.py"), encoding="utf-8").read()
        assert "MODIFIED_FILE = " in src and name.strip('"') in src
        assert "def record_modified(" in src
    else:
        src = open(os.path.join(root, "jobs", "gus_dbw_refresh.py"), encoding="utf-8").read()
        assert "STATE_FILE = " in src and name.strip('"') in src
        assert "def mark_refreshed(" in src


def test_the_real_audit_still_refuses_a_source_that_has_not_proven_itself():
    """A live check against the repo: whatever the current state, the classifier must agree
    with the artifact on disk rather than with the registry."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(A.__file__)))
    for sid, rel in A.SCRIPT_REFRESH_EVIDENCE.items():
        on_disk = os.path.exists(os.path.join(root, rel))
        assert (A._script_refresh_proven(sid) is not None) == on_disk, sid
