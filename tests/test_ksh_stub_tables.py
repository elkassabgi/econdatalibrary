"""ksh_stadat: a table whose CSV answers HTTP 404 is not the WAF (2026-09-23).

gdp0049 is listed in toc.json ("Financial accounts (available at the related links)", no reference
period, updated 2021) but has no CSV - its URL answers 404 (measured live). get_bytes returned None for a
404 and for a WAF/transport failure alike, so the fetcher booked it "transport/WAF failure" on every run
and ksh_stadat read partial ("1/60 sub-unit(s) transient-failed [gdp0049: ...]"). The real update() runs;
KSH's HTTP is faked at get_bytes.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import ksh_stadat as K  # noqa: E402

STUB = {"id": "gdp0049", "updatedAt": "2021-04-06T00:00:00Z", "correctedAt": None}
URL = f"{K.ig.BASE}/gdp/en/gdp0049.csv"


def _run(tmp_path, monkeypatch, status=404, stored=("KSH:gdp0001:x",)):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: [dict(STUB)])
    if stored is not None:
        pq.write_table(pa.table({"series_key": list(stored), "obs_date": pa.array([dt.date(2024, 12, 31)] * len(stored)),
                                 "value": [1.0] * len(stored)}), str(tmp_path / "gdp.parquet"))
    asked = []

    def _get(url):
        asked.append(url)
        K.ig.LAST_STATUS[url] = status
        return None
    monkeypatch.setattr(K.ig, "get_bytes", _get)
    try:
        res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    except K.DefinitiveError as e:
        res = types.SimpleNamespace(status="structural", error=str(e))
    side = json.loads((tmp_path / K.SIDECAR).read_text()) if (tmp_path / K.SIDECAR).exists() else {}
    return res, side, asked


def test_a_never_stored_table_with_no_csv_is_skipped_until_ksh_updates_it(tmp_path, monkeypatch, capsys):
    res, side, asked = _run(tmp_path, monkeypatch)
    assert res.status != "partial" and "transport/WAF" not in (res.error or ""), res.error
    assert side.get("gdp0049") == "2021-04-06T00:00:00Z|None", side
    assert "link-only table" in capsys.readouterr().out
    res, side, asked = _run(tmp_path, monkeypatch)
    assert asked == [], "its vintage is recorded: not fetched again until KSH changes it"


def test_a_stored_table_whose_csv_404s_is_a_break(tmp_path, monkeypatch):
    res, side, asked = _run(tmp_path, monkeypatch, stored=("KSH:gdp0049:x",))
    assert res.status == "structural" and "gdp0049: CSV now answers HTTP 404 although we store it" in res.error
    assert "gdp0049" not in side


def test_the_waf_is_still_transient(tmp_path, monkeypatch):
    res, side, asked = _run(tmp_path, monkeypatch, status=200)       # a WAF page: 200, no CSV body
    assert res.status == "partial" and "gdp0049: transport/WAF failure" in (res.error or ""), res.error
    assert "gdp0049" not in side


def test_an_unreadable_store_is_not_read_as_not_stored(tmp_path, monkeypatch):
    (tmp_path / "gdp.parquet").write_bytes(b"not a parquet")
    res, side, asked = _run(tmp_path, monkeypatch, stored=None)
    assert res.status == "structural" and "could not read the store" in res.error, res.error


def test_the_ingester_records_the_final_status(monkeypatch):
    class _R:
        status_code, content = 404, b"<!DOCTYPE html>"
    monkeypatch.setattr(K.ig.requests, "get", lambda url, headers=None, timeout=None: _R())
    assert K.ig.get_bytes("https://x/y.csv") is None and K.ig.LAST_STATUS["https://x/y.csv"] == 404


# ---- review R1118 -------------------------------------------------------------------------------
def test_a_table_stored_only_in_a_side_file_is_a_break_not_a_stub(tmp_path, monkeypatch):
    """Five served tables live only in _migrated_from_ksh_unparsed.parquet."""
    pq.write_table(pa.table({"series_key": ["KSH:gdp0049:x"], "obs_date": pa.array([dt.date(2024, 12, 31)]),
                             "value": [1.0]}), str(tmp_path / "_migrated_from_ksh_unparsed.parquet"))
    res, side, asked = _run(tmp_path, monkeypatch)
    assert res.status == "structural" and "although we store it" in res.error, res.error
    assert "gdp0049" not in side


@pytest.mark.parametrize("status", [403, 429, 503, 500])
def test_a_final_throttle_or_server_error_stays_transient(tmp_path, monkeypatch, status):
    res, side, asked = _run(tmp_path, monkeypatch, status=status)
    assert res.status == "partial" and "gdp0049: transport/WAF failure" in (res.error or ""), res.error
    assert "gdp0049" not in side


def test_a_fetch_that_raises_stays_transient(tmp_path, monkeypatch):
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, None))
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: [dict(STUB)])
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert res.status == "partial" and "gdp0049" in (res.error or "")


def test_the_skip_is_named_in_the_result_of_a_clean_pass(tmp_path, monkeypatch):
    res, side, asked = _run(tmp_path, monkeypatch)
    assert "not hosted - no CSV at KSH (HTTP 404) and nothing stored [gdp0049]" in (res.error or ""), res.error


def _cat(n):
    return [{"id": f"gdp{i:04d}", "updatedAt": "2026-09-10T00:00:00Z", "correctedAt": None} for i in range(1, n + 1)]


def test_tables_past_the_per_run_cap_are_booked_deferred(tmp_path, monkeypatch):
    from updater.health import _deferral_only
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: _cat(3))
    monkeypatch.setattr(K, "MAX_PER_RUN", 1)
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, []))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert res.status == "partial" and "2 deferred" in (res.error or ""), res.error
    assert _deferral_only([{"status": res.status, "last_error": res.error}]), res.error


def test_a_table_with_nothing_to_store_is_not_refetched_every_pass(tmp_path, monkeypatch):
    """ido0001..0016 parse empty and their theme file never exists: they took 16 of 60 slots every pass."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: [
        {"id": "ido0001", "updatedAt": "2026-01-01T00:00:00Z", "correctedAt": None}])
    asked = []
    monkeypatch.setattr(K, "_fetch_table", lambda tid: asked.append(tid) or (tid, []))
    K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert asked == ["ido0001"], asked


def test_tables_past_the_budget_stop_are_booked_deferred(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: _cat(3))
    monkeypatch.setattr(K, "TABLE_WAVE", 1)
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, []))

    class _DL:
        n = 0

        def __init__(self, minutes=None):
            pass

        def spent(self):
            _DL.n += 1
            return _DL.n > 1                                  # one wave, then the budget is spent

        def elapsed_min(self):
            return 30.0
    monkeypatch.setattr(K, "Deadline", _DL)
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert res.status == "partial" and "2 deferred" in (res.error or ""), res.error
