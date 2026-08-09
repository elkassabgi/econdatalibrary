"""A sliced IMF pull must RESUME after the unit deadline kills it — and must never
mix slices from two releases.

WHY. The orchestrator's unit deadline is a SIGALRM (orchestrate.py:158) that interrupts
a pull mid-flight on CI. `_pull_sliced` has no try/finally, so the .sliceNN.parquet
files it had already finished survived the kill — and were then ignored, because the
next run started again at slice 0. A flow that needs longer than the 45-minute limit
could therefore never converge: killed, restarted from zero, killed again. The five
imf_gfs*_direct sources (570,092 catalogued series) are exactly that shape, which is
why they sit at live:false.

Reusing finished slices fixes convergence but introduces a correctness risk that is
worse than the bug: slices left by an OLDER IMF release must not be assembled together
with new ones, or the published dataset is a vintage that never existed. The reuse is
therefore gated on a token — the flow version — and a mismatch wipes the set.

These tests pin both halves. They monkeypatch the network so they run anywhere.
"""
from __future__ import annotations

import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from jobs import ingest_imf_direct as ing  # noqa: E402


def _slice_paths(base: str) -> list[str]:
    d, name = os.path.dirname(base), os.path.basename(base)
    return sorted(p for p in os.listdir(d) if p.startswith(name + ".slice"))


@pytest.fixture()
def stub(monkeypatch, tmp_path):
    """Two decade windows, and a _pull_streamed that records what it was asked to fetch."""
    monkeypatch.setattr(ing, "_slice_windows", lambda first=1950: [("2000", "2009"),
                                                                   ("2010", "2019")])
    fetched: list[str] = []

    def fake_pull(url, flow, agency, source_id, part, min_obs):
        fetched.append(part)
        pq.write_table(pa.table({"series_key": ["k"], "obs_date": [None], "value": [1.0]}),
                       part)
        return 1

    monkeypatch.setattr(ing, "_pull_streamed", fake_pull)
    return fetched


def test_finished_slices_are_reused_when_the_vintage_matches(stub, tmp_path):
    """The convergence fix: a second run must not re-fetch what the first finished."""
    base = str(tmp_path / "src.parquet")

    n1 = ing._pull_sliced("GFS_SOO", "IMF.STA", "src", base, 0, resume_token="v1")
    assert n1 == 2, "first run should pull both windows"
    assert len(stub) == 2

    # The first run consumed its slices on success. Simulate the kill instead: put one
    # finished slice back and re-run under the SAME token.
    pq.write_table(pa.table({"series_key": ["k"], "obs_date": [None], "value": [1.0]}),
                   f"{base}.slice00.parquet")
    stub.clear()

    n2 = ing._pull_sliced("GFS_SOO", "IMF.STA", "src", base, 0, resume_token="v1")
    assert n2 == 2, "resumed run must still assemble every window"
    assert len(stub) == 1, (
        f"slice00 was already on disk under a matching token, so only slice01 should have "
        f"been fetched — but {len(stub)} slice(s) were: {stub}")
    assert stub[0].endswith("slice01.parquet")


def test_slices_from_a_superseded_release_are_discarded(stub, tmp_path):
    """The correctness guard: never assemble a dataset out of two IMF vintages."""
    base = str(tmp_path / "src.parquet")
    ing._pull_sliced("GFS_SOO", "IMF.STA", "src", base, 0, resume_token="v1")

    # A slice survives from the v1 release; upstream has since published v2.
    pq.write_table(pa.table({"series_key": ["OLD"], "obs_date": [None], "value": [9.9]}),
                   f"{base}.slice00.parquet")
    stub.clear()

    ing._pull_sliced("GFS_SOO", "IMF.STA", "src", base, 0, resume_token="v2")
    assert len(stub) == 2, (
        f"the token moved v1 -> v2, so the stale slice must be discarded and BOTH windows "
        f"re-fetched; only {len(stub)} were: {stub}")


def test_no_token_never_reuses(stub, tmp_path):
    """resume_token=None (a caller that cannot vouch for the vintage) must not resume."""
    base = str(tmp_path / "src.parquet")
    ing._pull_sliced("GFS_SOO", "IMF.STA", "src", base, 0, resume_token=None)
    pq.write_table(pa.table({"series_key": ["k"], "obs_date": [None], "value": [1.0]}),
                   f"{base}.slice00.parquet")
    stub.clear()
    ing._pull_sliced("GFS_SOO", "IMF.STA", "src", base, 0, resume_token=None)
    assert len(stub) == 2, "without a token there is nothing to vouch for; re-fetch everything"


def test_a_truncated_slice_is_refetched_not_trusted(stub, tmp_path):
    """The kill can land mid-write. A slice that will not open must be pulled again."""
    base = str(tmp_path / "src.parquet")
    ing._pull_sliced("GFS_SOO", "IMF.STA", "src", base, 0, resume_token="v1")
    with open(f"{base}.slice00.parquet", "wb") as fh:
        fh.write(b"PAR1 truncated garbage")          # unreadable by pyarrow
    stub.clear()
    ing._pull_sliced("GFS_SOO", "IMF.STA", "src", base, 0, resume_token="v1")
    assert len(stub) == 2, "an unreadable slice must be re-fetched, never counted as done"
