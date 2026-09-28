"""unctad: a clean pull stores the publisher's release token, so an unchanged release is skipped.

finalize() stamps new_vintage="date-tail" on every Result, and bulk_snapshot_if_changed fills in the
probed token only when a fetcher returns None - so every unctad unit stored "date-tail", the probe
never matched, and every tick re-pulled the whole dataset. Hermetic: the job module is faked; the
store is a tmp dir under the LOCAL backend.
"""
import datetime as dt
import os
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import _unctad as U  # noqa: E402
from updater.strategies.bulk_snapshot_if_changed import BulkSnapshotIfChanged  # noqa: E402

META = {"version": 10001, "lastUpdated": "2026-06-05T15:25:52"}
TOKEN = "10001|2026-06-05T15:25:52"


def _job(pull):
    j = types.SimpleNamespace(
        report_metadata=lambda ds: dict(META), creds=lambda: ("c", "k"),
        UnsupportedLayout=type("UnsupportedLayout", (RuntimeError,), {}))
    j.pull_rows = pull
    return j


def _rows(ds, cid, key, meta):
    return ["A.B", "A.C"], [dt.date(2024, 12, 31)] * 2, [1.0, 2.0]


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(U.config, "source_dir", lambda s: str(tmp_path / s))
    return tmp_path


def test_a_clean_pull_stores_the_release_token_the_probe_reads(store, monkeypatch):
    monkeypatch.setattr(U, "_job", lambda: _job(_rows))
    current_vintage, update = U.make("US.X", "unctad_x")
    first = update(None, None)
    assert first.status == "ok" and first.new_vintage == TOKEN == current_vintage(None)
    again = update(None, None)                                     # same rows: no_change, same token
    assert again.status == "no_change" and again.new_vintage == TOKEN


def test_a_failed_pull_does_not_claim_the_release(store, monkeypatch):
    def boom(*a):
        raise ConnectionError("facts unreachable")
    monkeypatch.setattr(U, "_job", lambda: _job(boom))
    _, update = U.make("US.X", "unctad_x")
    res = update(None, None)
    assert res.status != "ok" and res.new_vintage != TOKEN


def test_a_pull_that_finalizes_partial_keeps_the_placeholder(store, monkeypatch):
    """The guard on the success path: finalize can grade a merged pull below ok; that must not
    claim the release, or the unit would skip a release it never finished."""
    monkeypatch.setattr(U, "_job", lambda: _job(_rows))
    real = U.finalize

    def partial(*a, **k):
        r = real(*a, **k)
        r.status = "partial"
        return r
    monkeypatch.setattr(U, "finalize", partial)
    _, update = U.make("US.X", "unctad_x")
    res = update(None, None)
    assert res.status == "partial" and res.new_vintage != TOKEN


def test_a_release_that_lands_during_the_pull_reads_as_changed_next_tick(store, monkeypatch):
    """The stamped token is the one read BEFORE the pull (review R1154 A): a later read would claim a
    release whose data this pull never saw."""
    calls = {"n": 0}
    later = {"version": 10002, "lastUpdated": "2026-09-23T12:00:00"}

    def meta(ds):
        calls["n"] += 1
        return dict(META) if calls["n"] == 1 else dict(later)
    j = _job(_rows)
    j.report_metadata = meta
    monkeypatch.setattr(U, "_job", lambda: j)
    _, update = U.make("US.X", "unctad_x")
    res = update(None, None)
    assert res.new_vintage == TOKEN, res.new_vintage
    assert U._token(later) != TOKEN


def test_metadata_without_a_release_never_seals_the_unit(store, monkeypatch):
    """"None|None" on both sides would match for ever (review R1154 C)."""
    j = _job(_rows)
    j.report_metadata = lambda ds: {}
    monkeypatch.setattr(U, "_job", lambda: j)
    current_vintage, update = U.make("US.X", "unctad_x")
    assert current_vintage(None) is None
    res = update(None, None)
    # finalize's placeholder stays: None would let the orchestrator store the strategy's "force"
    assert res.status == "ok" and res.new_vintage == "date-tail", res.new_vintage
    assert U._token({"version": 7}) == "7|None", "one half present is still a token"


def test_the_strategy_skips_an_unchanged_release_and_fetches_the_placeholder(monkeypatch):
    """The gate itself: the stored token skips; the old "date-tail" (every unit today) fetches once."""
    fetcher = types.SimpleNamespace(current_vintage=lambda unit: TOKEN)
    monkeypatch.setattr("updater.strategies.bulk_snapshot_if_changed.get_fetcher", lambda sid: fetcher)
    unit = types.SimpleNamespace(source_id="unctad_x")
    s = BulkSnapshotIfChanged()
    assert s.detect_change(unit, {"upstream_vintage": TOKEN}) is None
    assert s.detect_change(unit, {"upstream_vintage": "date-tail"}) == TOKEN
    assert s.detect_change(unit, {"upstream_vintage": "10002|2026-09-01T00:00:00"}) == TOKEN


def test_every_unctad_module_binds_through_make():
    """The fix lives in make(); a module that defined its own update would keep the placeholder."""
    d = os.path.join(ROOT, "updater", "strategies", "fetchers")
    mods = [f for f in os.listdir(d) if f.startswith("unctad_") and f.endswith(".py")]
    src = {f: open(os.path.join(d, f), encoding="utf-8").read() for f in mods}
    assert len(mods) >= 90, len(mods)                                      # 97 on 2026-09-23
    own = [f for f, s in src.items() if "def update" in s or "make(" not in s]
    assert own == [], own
