"""ilostat: the unit's last_obs_date carries the newest OBSERVED date this pass published (2026-11-16).

Health read ilostat's newest_obs only from the series cursors, which stop at CURSOR_CAP after ~3 of the
500-670 indicators a pass publishes, so the reading froze at 2026-08-01 and the gate read RED-DATA at
107 days while ILO's monthly unemployment table held 2026M10. update() passed `since` (never set) to
finalize. Hermetic: the ILO downloader is faked; a tmp store under the LOCAL backend.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater.strategies.fetchers import ilostat as I  # noqa: E402

TODAY = dt.date.today()
PAST = (TODAY - dt.timedelta(days=40)).isoformat()
OLDER = (TODAY - dt.timedelta(days=400)).isoformat()
FUTURE = (TODAY + dt.timedelta(days=900)).isoformat()


def _wire(monkeypatch, tmp_path, toc, broken=()):
    """toc: [(id, last.update, max obs_date)]; `broken` ids parse to 0 rows although records exist."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(I.config, "source_dir", lambda s: str(tmp_path))
    rows = [{"id": i, "last.update": u, "n.records": 5} for i, u, _ in toc]
    mx = {i: m for i, _, m in toc}
    monkeypatch.setattr(I.ig, "download_toc", lambda: None)
    monkeypatch.setattr(I.ig, "read_toc", lambda: rows)

    def _process(ds):
        iid = ds["id"]
        if iid in broken:
            return iid, 0, 0, 5, "empty", None, None, 0, 0
        d = mx[iid]
        pq.write_table(pa.table({"series_key": [f"{iid}|x"], "obs_date": pa.array([d]).cast(pa.date32()),
                                 "value": [1.0]}), str(tmp_path / f"{iid}.parquet"))
        return iid, 1, 1, 1, "ok", d, dt.date.fromisoformat(d), 10, 0.1     # real process_one: a date
    monkeypatch.setattr(I.ig, "process_one", _process)


def test_last_obs_is_the_newest_observed_published_date(tmp_path, monkeypatch):
    _wire(monkeypatch, tmp_path, [("EMP_A", "u1", OLDER), ("UNE_M", "u1", PAST), ("POP_A", "u1", FUTURE)])
    res = I.update(None, None)
    assert res.last_obs_date == PAST, res.last_obs_date          # the projection (FUTURE) is left out


def test_negative_control_the_old_code_path_would_report_since(tmp_path, monkeypatch):
    """Planted positive for the receipt: with the helper disabled, last_obs is `since` again (None here),
    which is exactly what froze health's reading. If this test passes with the helper patched out, the
    first test is the one that can fail."""
    _wire(monkeypatch, tmp_path, [("UNE_M", "u1", PAST)])
    monkeypatch.setattr(I, "_observed_newest", lambda changed, today=None: None)
    res = I.update(None, None)
    assert res.last_obs_date is None, res.last_obs_date


def test_a_stored_since_does_not_win_over_the_published_date(tmp_path, monkeypatch):
    """AR-491 condition 1: in production `since` is the stored last_obs_date from the first pass on
    (orchestrate passes sane_since of it). `since or _observed_newest(...)` would freeze the reading at
    its first value for good - every other test calls update(None, None) and cannot see that order."""
    _wire(monkeypatch, tmp_path, [("UNE_M", "u1", PAST)])
    res = I.update(None, OLDER)
    assert res.last_obs_date == PAST, res.last_obs_date


def test_nothing_published_passes_since_through(tmp_path, monkeypatch):
    _wire(monkeypatch, tmp_path, [("UNE_M", "u1", PAST)])
    I.update(None, None)                                          # publishes; stamps the sidecar
    res = I.update(None, "2026-01-01")                            # nothing moved upstream
    assert res.changed_keys == {} and res.last_obs_date == "2026-01-01", (res.changed_keys, res.last_obs_date)


def test_the_structural_catch_keeps_the_observed_date(tmp_path, monkeypatch):
    _wire(monkeypatch, tmp_path, [("UNE_M", "u1", PAST), ("BAD_A", "u1", PAST)], broken={"BAD_A"})
    res = I.update(None, None)
    assert res.status == "partial" and res.last_obs_date == PAST, (res.status, res.last_obs_date)


@pytest.mark.parametrize("changed,expected", [
    ({}, None),
    ({"A": None}, None),
    ({"A": FUTURE}, None),                                        # all projections: no observed date
    ({"A": OLDER, "B": PAST, "C": FUTURE}, PAST),
    ({"A": TODAY.isoformat()}, TODAY.isoformat()),                # today counts as observed
])
def test_observed_newest(changed, expected):
    assert I._observed_newest(changed, today=TODAY) == expected
