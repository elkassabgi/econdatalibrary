"""tools/stamp_source_data_through.py - the second publisher of source_data_through - honours the gate.

core/sync_state_d1.py withholds gated sources from source_data_through; this tool stamps the sources
that sync leaves out (DATA_THROUGH_FROM_D1) straight into D1, so without its own check a gated source
in that set would still be published (review of PR #36). Nothing here touches D1: the refusal fires
before the first wrangler call, and a wrangler call is replaced with a failure so a leak cannot pass.
"""
from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

import stamp_source_data_through as stamp_tool  # noqa: E402
from core import gen_denylist  # noqa: E402


@pytest.fixture
def no_d1(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("D1 must not be reached")
    monkeypatch.setattr(stamp_tool, "d1_json", boom)


def test_a_gated_source_is_refused_before_d1(monkeypatch, no_d1):
    monkeypatch.setattr(gen_denylist, "committed_gate", lambda: {"zz_gated_fixture"})
    with pytest.raises(SystemExit, match="gated"):
        stamp_tool.stamp("ZZ_GATED_FIXTURE", apply=True)


def test_a_non_gated_source_reaches_d1(monkeypatch, no_d1):
    """Control: the refusal is specific - a non-gated source goes on to D1 (here, the planted failure)."""
    monkeypatch.setattr(gen_denylist, "committed_gate", lambda: {"zz_gated_fixture"})
    with pytest.raises(AssertionError, match="must not be reached"):
        stamp_tool.stamp("zz_hosted_fixture", apply=True)


def test_an_absent_gate_refuses(monkeypatch, tmp_path, no_d1):
    monkeypatch.setattr(gen_denylist, "OUT", str(tmp_path / "no_such_denylist.ts"))
    with pytest.raises(SystemExit, match="absent"):
        stamp_tool.stamp("zz_hosted_fixture", apply=True)
