"""An IMF direct flow is pulled on every run - an unchanged dataflow version is NOT "no change".

Measured 2026-09-23 (NUMBERS.md): EER's version read 6.0.0 on 2026-08-05 and on 2026-09-23 while
IMF appended 2026-M07 and M08 (179,736 -> 179,956 obs). A gate that skipped the pull on an
unchanged version would have frozen every imf_*_direct source at its first pull; it never fired in
CI only because its sidecar never reached R2. Both places that could skip are pinned here: run()
itself, and the strategy's detect_change(), which skips when the stored vintage equals the probe.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.bulk_snapshot_if_changed import BulkSnapshotIfChanged  # noqa: E402
from updater.strategies.base import Unit  # noqa: E402
from updater.strategies.fetchers import _imf_direct as M  # noqa: E402


def _table(months):
    keys = ["EER:ARE.M.true.NEER" for _ in months]
    return pa.table({"series_key": keys,
                     "obs_date": pa.array([dt.date(2026, m, 28) for m in months], pa.date32()),
                     "value": [float(m) for m in months]})


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(M.config, "source_dir", lambda sid: str(tmp_path))
    pq.write_table(_table([4, 5, 6]), tmp_path / "imf_eer_direct.parquet")
    # the sidecar the old gate trusted, claiming the version we are about to read
    (tmp_path / "_version.json").write_text(json.dumps({"version": "6.0.0"}))
    monkeypatch.setattr(M, "_flow_version", lambda flow: "6.0.0")
    pulled = []

    def _pull(flow, agency, sid, out_path=None, min_obs=0, resume_token=None):
        pulled.append(resume_token)
        pq.write_table(_table([4, 5, 6, 7, 8]), out_path)      # IMF appended M07, M08
        return 5
    monkeypatch.setattr(M.ing, "pull", _pull)
    return tmp_path, pulled


def test_an_unchanged_version_still_pulls_and_lands_the_appended_months(store):
    d, pulled = store
    res = M.run("EER", "IMF.STA", "imf_eer_direct")
    assert pulled == ["6.0.0"], "the pull must run although the version did not move"
    assert res.status == "ok" and res.last_obs_date == dt.date(2026, 8, 28), (res.status, res.last_obs_date)
    assert pq.read_metadata(d / "imf_eer_direct.parquet").num_rows == 5


def test_an_unreachable_dataflow_catalogue_does_not_stop_the_pull(store, monkeypatch):
    """The version only keys the resume now; without it the pull starts afresh (token None)."""
    d, pulled = store

    def _down(flow):
        raise OSError("catalogue timed out")
    monkeypatch.setattr(M, "_flow_version", _down)
    res = M.run("EER", "IMF.STA", "imf_eer_direct")
    assert pulled == [None] and res.status == "ok"


def test_the_strategy_cannot_skip_on_the_version_either(store, monkeypatch):
    """detect_change() skips when the STORED vintage equals the probe. The stored vintage is what
    the run returned: finalize's "date-tail", never a flow version - so it cannot."""
    res = M.run("EER", "IMF.STA", "imf_eer_direct")
    assert res.new_vintage == "date-tail"
    s = BulkSnapshotIfChanged()

    class _F:
        @staticmethod
        def current_vintage(unit):
            return "EER:6.0.0"
    monkeypatch.setattr("updater.strategies.bulk_snapshot_if_changed.get_fetcher", lambda sid: _F)
    unit = Unit("imf_eer_direct", "_all", "bulk_snapshot_if_changed", cadence="weekly")
    assert s.detect_change(unit, {"upstream_vintage": res.new_vintage}) is not None
    # negative control: IF a run had stored the flow version, the strategy WOULD skip
    assert s.detect_change(unit, {"upstream_vintage": "EER:6.0.0"}) is None
