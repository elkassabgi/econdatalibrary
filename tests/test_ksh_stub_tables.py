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
    assert "not hosted note: 1 table(s) - no CSV at KSH (HTTP 404) and nothing stored [gdp0049]" \
        in (res.error or ""), res.error


def test_the_skip_is_named_on_a_deferral_pass_and_it_stays_a_pure_deferral(tmp_path, monkeypatch):
    """R1121: while the backlog stands EVERY pass is capped, so a note written only on ok/no_change
    passes was never written. It is written on the deferral pass too, and health still reads that
    pass as a pure deferral (ROTATING), with or without the orchestrator's csv coverage tail."""
    from updater.health import _deferral_only
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    later = [{"id": t, "updatedAt": "2026-09-10T00:00:00Z", "correctedAt": None} for t in ("tur0001", "tur0002")]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: [dict(STUB)] + later)
    monkeypatch.setattr(K, "MAX_PER_RUN", 2)
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, "absent" if tid == "gdp0049" else []))
    monkeypatch.setattr(K, "_holds_table", lambda out_dir, tid: False)
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert res.status == "partial" and "1 deferred" in res.error, res.error
    assert "not hosted note: 1 table(s)" in res.error and "gdp0049" in res.error, res.error
    assert _deferral_only([{"status": "partial", "last_error": res.error}]), res.error
    tail = res.error + "; csv coverage note: 3 changed keys are outside the catalogue"
    assert _deferral_only([{"status": "partial", "last_error": tail}]), tail


def test_health_strips_only_the_named_note_tail():
    """Negative controls: the strip is the exact prefix, and a real failure beside it still demotes."""
    from updater.health import _deferral_only
    base = "3 sub-unit(s) attempted, none failed; 5 deferred by budget and taken next tick"
    ok = f"{base}; {K.NOT_HOSTED_NOTE} 1 table(s) - x [gdp0049]"
    assert _deferral_only([{"status": "partial", "last_error": ok}])
    assert not _deferral_only([{"status": "partial", "last_error": f"{base}; not hosted: 1 table(s)"}])
    assert not _deferral_only([{"status": "partial", "last_error":
                                f"1/3 sub-unit(s) transient-failed [x]; {K.NOT_HOSTED_NOTE} 1 table(s)"}])


def test_owed_and_never_fetched_tables_take_turns(tmp_path, monkeypatch):
    """R1121: sorted by id, the capped queue was always spent on themes a..k (monthly updates), and
    802 tables in kor..tur were never fetched by the updater. R1123: never-fetched-FIRST froze the 840
    maintained tables instead (headline series 99 days owed in simulation). So they take turns, owed
    first; the owed side goes oldest stored updatedAt first, the never-fetched side by id."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    cat = [{"id": t, "updatedAt": "2026-09-20T00:00:00Z", "correctedAt": None}
           for t in ("aaa0001", "aaa0002", "aaa0003", "tur0001", "tur0002")]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)
    (tmp_path / K.SIDECAR).write_text(json.dumps({
        "aaa0001": "2026-08-01T00:00:00Z|None", "aaa0002": "2026-03-01T00:00:00Z|None",
        "aaa0003": "2026-06-01T00:00:00Z|None"}))
    monkeypatch.setattr(K, "MAX_PER_RUN", 3)
    monkeypatch.setattr(K, "TABLE_WAVE", 1)
    asked = []
    monkeypatch.setattr(K, "_fetch_table", lambda tid: asked.append(tid) or (tid, []))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert asked == ["aaa0002", "tur0001", "aaa0003"], asked
    assert "tur0002 (per-run cap" in res.error and "aaa0001 (per-run cap" in res.error, res.error


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


# ---- no back-off sleep past the pass's stop time (R1121 follow-up) ------------------------------
class _WafPage:
    status_code, content = 200, b"<html><head><title>Request Rejected</title></head></html>"


def test_get_bytes_gives_up_instead_of_sleeping_past_the_stop_time(monkeypatch):
    """One URL's WAF ladder is 60+120+300+600+900 s = 33 min; begun just inside the budget it would
    carry the pass past the orchestrator's 45-minute kill, losing every table fetched so far."""
    slept = []
    monkeypatch.setattr(K.ig.requests, "get", lambda url, headers=None, timeout=None: _WafPage())
    monkeypatch.setattr(K.ig.time, "sleep", slept.append)
    monkeypatch.setattr(K.ig, "STOP_AT", [K.ig.time.time() + 30])
    assert K.ig.get_bytes("https://x/a.csv") is None
    assert K.ig.LAST_STATUS["https://x/a.csv"] == "deadline" and slept == [], slept


def test_negative_control_no_stop_time_keeps_the_full_ladder(monkeypatch):
    slept = []
    monkeypatch.setattr(K.ig.requests, "get", lambda url, headers=None, timeout=None: _WafPage())
    monkeypatch.setattr(K.ig.time, "sleep", slept.append)
    monkeypatch.setattr(K.ig, "STOP_AT", [None])
    assert K.ig.get_bytes("https://x/b.csv") is None
    assert slept == K.ig.WAF_SLEEPS and K.ig.LAST_STATUS["https://x/b.csv"] == 200


def test_a_table_cut_at_the_stop_time_is_deferred_and_the_stop_is_cleared(tmp_path, monkeypatch):
    from updater.health import _deferral_only
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: _cat(2))
    seen = []

    def _get(url):
        seen.append(K.ig.STOP_AT[0])
        if url.endswith("gdp0001.csv"):
            K.ig.LAST_STATUS[url] = 200
            return b"csv"
        K.ig.LAST_STATUS[url] = "deadline"
        return None
    monkeypatch.setattr(K.ig, "get_bytes", _get)
    monkeypatch.setattr(K.ig, "parse_table", lambda tid, txt: ([(f"KSH:{tid}:r:c", dt.date(2025, 12, 31), 1.0)], None))
    monkeypatch.setattr(K, "TABLE_WAVE", 1)                 # one wave's worth answered: not WAF-blocked
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert res.status == "partial" and "1 deferred" in res.error, res.error
    assert "gdp0002 (WAF/throttle back-off cut at the stop time)" in res.error, res.error
    assert _deferral_only([{"status": "partial", "last_error": res.error}]), res.error
    budget = float(os.environ.get("KSH_BUDGET_MIN", "25"))
    assert seen and all(s is not None for s in seen)
    assert abs(seen[0] - K.time.time() - (budget + K.STOP_GRACE_MIN) * 60) < 60
    assert K.ig.STOP_AT[0] is None, "the ingester's own main() keeps the full ladder"
    side = json.loads((tmp_path / K.SIDECAR).read_text())
    assert "gdp0001" in side and "gdp0002" not in side, "the cut table is not recorded as fetched"


# ---- review R1123 -------------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _no_pace(monkeypatch):
    monkeypatch.setattr(K, "PACE_S", 0.0)


def _plain(monkeypatch, tmp_path, cat):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)


def test_a_pass_the_waf_stops_before_one_wave_stays_in_attention(tmp_path, monkeypatch):
    from updater.health import _deferral_only
    _plain(monkeypatch, tmp_path, _cat(3))
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, "deadline"))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert "KSH's WAF blocked this pass: 0 table(s) answered, 3 cut at the stop time" in res.error, res.error
    assert not _deferral_only([{"status": res.status, "last_error": res.error}]), "ATTENTION, not ROTATING"


def test_the_rotation_note_names_what_is_owed_and_health_strips_it(tmp_path, monkeypatch):
    from updater.health import _deferral_only
    _plain(monkeypatch, tmp_path, _cat(3))
    monkeypatch.setattr(K, "MAX_PER_RUN", 1)
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, []))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert "rotation note: 3 table(s) were owed at the start of this pass, 1 answered; 2 never fetched" \
        in res.error, res.error
    assert _deferral_only([{"status": "partial", "last_error": res.error}]), res.error


def test_no_rotation_note_when_nothing_is_left_owed(tmp_path, monkeypatch):
    _plain(monkeypatch, tmp_path, _cat(2))
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, []))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert "rotation note" not in (res.error or ""), res.error


def test_the_stop_time_is_set_before_any_work_and_cleared_on_a_raise(tmp_path, monkeypatch):
    """R1123 (a): the clock starts at entry, and STOP_AT is cleared in a finally."""
    seen = []

    def _boom(raise_transient):
        seen.append(K.ig.STOP_AT[0])
        raise K.TransientError("toc.json down")
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", _boom)
    with pytest.raises(K.TransientError):
        K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert seen and seen[0] is not None, "set before the catalogue read"
    assert K.ig.STOP_AT[0] is None, "cleared although update() raised"


def test_the_todo_scan_lists_the_store_once_and_heads_nothing(tmp_path, monkeypatch):
    """R1123 (a): one HEAD per table was 167.7 s for 809 tables before the clock started."""
    _plain(monkeypatch, tmp_path, _cat(3))
    (tmp_path / K.SIDECAR).write_text(json.dumps({"gdp0001": "2026-09-10T00:00:00Z|None",
                                                  "gdp0002": "2026-09-10T00:00:00Z|None"}))
    pq.write_table(pa.table({"series_key": ["KSH:gdp0001:x"], "obs_date": pa.array([dt.date(2024, 12, 31)]),
                             "value": [1.0]}), str(tmp_path / "gdp.parquet"))

    def _no_head(path):
        raise AssertionError(f"per-table HEAD {path}")
    monkeypatch.setattr(K.blob, "exists", _no_head)
    asked = []
    monkeypatch.setattr(K, "_fetch_table", lambda tid: asked.append(tid) or (tid, []))
    K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert asked == ["gdp0003"], "the two current tables with a theme file were skipped without a HEAD"


def test_every_table_request_is_paced(monkeypatch):
    paced = []
    monkeypatch.setattr(K, "_pace", lambda: paced.append(1))
    monkeypatch.setattr(K.ig, "get_bytes", lambda url: None)
    K._fetch_table("gdp0001")
    assert paced == [1]


def test_the_sidecar_is_saved_after_each_theme_so_a_kill_keeps_the_merged_ones(tmp_path, monkeypatch):
    _plain(monkeypatch, tmp_path, [{"id": t, "updatedAt": "2026-09-10T00:00:00Z", "correctedAt": None}
                                   for t in ("aaa0001", "bbb0001")])
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, [(f"KSH:{tid}:r:c", dt.date(2025, 12, 31), 1.0)]))
    real = K.merge.merge_and_write
    calls = []

    class _Killed(BaseException):
        pass

    def _merge(path, tbl, **kw):
        calls.append(path)
        if len(calls) == 2:
            raise _Killed("the 45-minute SIGALRM")
        return real(path, tbl, **kw)
    monkeypatch.setattr(K.merge, "merge_and_write", _merge)
    with pytest.raises(_Killed):
        K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    side = json.loads((tmp_path / K.SIDECAR).read_text())
    assert len(side) == 1, "the first theme's table is recorded although the pass was killed"


@pytest.mark.parametrize("answer", ["throttle", "raise"])
def test_get_bytes_starts_no_request_and_no_sleep_past_the_stop_time(monkeypatch, answer):
    """R1123 M1/M2: the 403/429/503 back-off and the error back-off must stop at the stop time too."""
    asked, slept = [], []

    def _get(url, headers=None, timeout=None):
        asked.append(url)
        if answer == "raise":
            raise K.ig.requests.ConnectionError("connect timeout")
        return types.SimpleNamespace(status_code=503, content=b"")
    monkeypatch.setattr(K.ig.requests, "get", _get)
    monkeypatch.setattr(K.ig.time, "sleep", slept.append)
    monkeypatch.setattr(K.ig, "STOP_AT", [K.ig.time.time() + 3])
    assert K.ig.get_bytes("https://x/c.csv") is None
    assert len(asked) == 1 and slept == [] and K.ig.LAST_STATUS["https://x/c.csv"] == "deadline", (asked, slept)
    monkeypatch.setattr(K.ig, "STOP_AT", [K.ig.time.time() - 1])
    asked.clear()
    assert K.ig.get_bytes("https://x/d.csv") is None and asked == [], "no request once past the stop time"


def test_requests_are_paced_across_workers(monkeypatch):
    monkeypatch.setattr(K, "PACE_S", 1.2)
    now = [100.0]
    slept = []
    monkeypatch.setattr(K.time, "time", lambda: now[0])
    monkeypatch.setattr(K.time, "sleep", lambda s: slept.append(round(s, 3)) or now.__setitem__(0, now[0] + s))
    monkeypatch.setattr(K, "_LAST_START", [0.0])
    K._pace()
    K._pace()
    now[0] += 5
    K._pace()
    assert slept == [1.2], slept


def test_the_cap_default_is_the_budget_bound_400(monkeypatch):
    monkeypatch.delenv("KSH_MAX_PER_RUN", raising=False)
    import importlib
    import updater.strategies.fetchers.ksh_stadat as mod
    assert importlib.reload(mod).MAX_PER_RUN == 400
    importlib.reload(mod)
