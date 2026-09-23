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
    cal = ["2026-03-15T00:00:00Z", "2026-06-15T00:00:00Z", "2026-08-15T00:00:00Z", "2026-09-20T00:00:00Z"]
    cat = [{"id": t, "updatedAt": "2026-09-20T00:00:00Z", "correctedAt": None, "updateDates": cal}
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
            self.budget_min = minutes

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


@pytest.fixture
def real_backfill():
    """Opt out of _backfill_default: the test drives the real loader with no file present."""


@pytest.fixture(autouse=True)
def _backfill_default(request, monkeypatch):
    """A missing backfill file now makes a pass ATTENTION (R1138). Tests about other behaviour run as if
    the seed had been written (an empty map) unless the store dir holds a real file."""
    if "real_backfill" in request.fixturenames:
        return
    real = K._load_backfill
    monkeypatch.setattr(K, "_load_backfill", lambda out_dir: real(out_dir)
                        if os.path.exists(os.path.join(out_dir, K.BACKFILL)) else {})


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
    assert "rotation note: 3 table(s) were owed at the start of this pass, 1 answered, 2 with nothing stored" \
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


# ---- review R1127 -------------------------------------------------------------------------------
BASE = "3 sub-unit(s) attempted, none failed; 5 deferred by budget and taken next tick [x]"


@pytest.mark.parametrize("csv_err", [
    "csv_derive failed 3/10 series [ksh_stadat:KSH:gdp0001]",
    "csv coherence unmet: 9 changed series_keys have no catalog mapping for ksh_stadat",
    "csv_derive crashed (0 of 3 changed series queued): UnitTimeout('x')",
])
def test_a_csv_failure_joined_after_the_rotation_and_not_hosted_tails_is_attention(csv_err):
    """R1127: health cut at the first non-failure tail and dropped everything after it - the
    orchestrator joins its csv verdict AFTER the fetcher's error with '; '."""
    from updater.health import _deferral_only
    fetcher = (f"{BASE}; {K.NOT_HOSTED_NOTE} 1 table(s) - no CSV [gdp0049]; "
               f"{K.ROTATION_NOTE} 849 table(s) were owed at the start of this pass, 105 answered, "
               f"744 never fetched by the updater remain")
    err = "; ".join((fetcher, csv_err))                     # the orchestrator's own join
    assert not _deferral_only([{"status": "partial", "last_error": err}]), err
    assert _deferral_only([{"status": "partial", "last_error": fetcher}]), "negative control"


def test_the_subset_coverage_note_and_the_fence_note_still_read_as_non_failures():
    from updater import orchestrate
    from updater.health import _deferral_only
    note, demote = orchestrate._classify_zero_mapped("abs", "subset", 18, 0, 500, 60000)
    assert not demote and "; " not in note, note
    assert _deferral_only([{"status": "partial", "last_error": f"{BASE}; {note}"}])


def test_no_non_failure_note_in_the_code_contains_a_segment_separator():
    """Health drops a note SEGMENT by prefix; a '; ' inside a note would leave its tail as a segment
    that is not a note, and a healthy deferral pass would read ATTENTION (or, worse, a failure would
    be read as a note's tail). Every literal note text in the orchestrator and this fetcher is checked."""
    import ast
    from updater.strategies import base
    from updater.strategies.base import NON_FAILURE_NOTES
    # An f-string that OPENS with a note constant ({ROTATION_NOTE} ..., {NOT_HOSTED_NOTE} ...) is a note
    # too: the fetcher builds both of its notes that way, and the first cut of this walk saw 0 of them
    # (review R1134). Such a head is resolved to the constant's text before the prefix test.
    heads = {n: getattr(base, n) for n in ("ROTATION_NOTE", "NOT_HOSTED_NOTE")}
    found, by_head = 0, 0
    for rel in ("updater/orchestrate.py", "updater/strategies/fetchers/ksh_stadat.py"):
        tree = ast.parse(open(os.path.join(ROOT, rel), encoding="utf-8").read())
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                parts = [node.value]
            elif isinstance(node, ast.JoinedStr):
                parts = []
                for i, v in enumerate(node.values):
                    if isinstance(v, ast.Constant):
                        parts.append(v.value)
                    elif (i == 0 and isinstance(v, ast.FormattedValue) and isinstance(v.value, ast.Name)
                          and v.value.id in heads):
                        parts.append(heads[v.value.id])
                        by_head += 1
            else:
                parts = []
            text = "".join(parts)
            if text.startswith(NON_FAILURE_NOTES):
                found += 1
                assert "; " not in text, (rel, text[:120])
    assert found >= 5 and by_head >= 2, (found, by_head)


def test_owed_tables_go_by_first_missed_release_not_by_the_age_of_our_copy(tmp_path, monkeypatch):
    """An annual table stored in January first missed a release in August; a monthly table stored in
    March has been denied its April release since April - it goes first (R1127)."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    cat = [{"id": "ann0001", "updatedAt": "2026-08-10T00:00:00Z", "correctedAt": None,
            "updateDates": ["2026-08-10T00:00:00Z"]},
           {"id": "mon0001", "updatedAt": "2026-09-10T00:00:00Z", "correctedAt": None,
            "updateDates": [f"2026-{m:02d}-10T00:00:00Z" for m in range(1, 10)]}]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)
    (tmp_path / K.SIDECAR).write_text(json.dumps({"ann0001": "2026-01-05T00:00:00Z|None",
                                                  "mon0001": "2026-03-10T00:00:00Z|None"}))
    monkeypatch.setattr(K, "MAX_PER_RUN", 1)
    asked = []
    monkeypatch.setattr(K, "_fetch_table", lambda tid: asked.append(tid) or (tid, []))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert asked == ["mon0001"], asked
    assert "longest wait" in res.error and "ann0001" in res.error, res.error


def test_a_table_owed_past_the_limit_turns_the_pass_to_attention(tmp_path, monkeypatch):
    from updater.health import _deferral_only
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    now = dt.datetime.now(dt.timezone.utc)
    old = (now - dt.timedelta(days=K.OWED_ATTENTION_DAYS + 5)).isoformat()
    young = (now - dt.timedelta(days=K.OWED_ATTENTION_DAYS - 5)).isoformat()
    cat = [{"id": "gdp0001", "updatedAt": young, "correctedAt": None, "updateDates": [old, young]},
           {"id": "gdp0002", "updatedAt": young, "correctedAt": None, "updateDates": [young]}]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)
    (tmp_path / K.SIDECAR).write_text(json.dumps({"gdp0001": "2025-01-01T00:00:00Z|None",
                                                  "gdp0002": "2025-01-01T00:00:00Z|None"}))
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, "deadline" if tid == "gdp0001" else []))
    monkeypatch.setattr(K, "TABLE_WAVE", 1)                 # one table answered: not the WAF floor
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert f"rotation behind: gdp0001 has waited {K.OWED_ATTENTION_DAYS + 5} days" in res.error, res.error
    assert not _deferral_only([{"status": res.status, "last_error": res.error}])
    cat[0]["updateDates"] = [young]
    (tmp_path / K.SIDECAR).write_text(json.dumps({"gdp0001": "2025-01-01T00:00:00Z|None",
                                                  "gdp0002": "2025-01-01T00:00:00Z|None"}))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert "rotation behind" not in res.error and _deferral_only(
        [{"status": res.status, "last_error": res.error}]), res.error


@pytest.mark.parametrize("answered,attention", [(9, True), (10, False)])
def test_the_waf_blocked_floor_is_one_wave(tmp_path, monkeypatch, answered, attention):
    from updater.health import _deferral_only
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: _cat(answered + 1))
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, "deadline" if tid == f"gdp{answered + 1:04d}" else []))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert (not _deferral_only([{"status": res.status, "last_error": res.error}])) is attention, res.error


def test_the_budget_is_sized_from_the_alarm_and_can_start_nothing(monkeypatch):
    import signal
    monkeypatch.delenv("KSH_BUDGET_MIN", raising=False)
    monkeypatch.setattr(signal, "ITIMER_REAL", 0, raising=False)
    monkeypatch.setattr(signal, "getitimer", lambda which: (20 * 60.0, 0.0), raising=False)
    assert K._budget_min() == pytest.approx(20 - K.STOP_GRACE_MIN - K.MERGE_MARGIN_MIN)
    monkeypatch.setattr(signal, "getitimer", lambda which: (8 * 60.0, 0.0), raising=False)
    assert K._budget_min() == 0.0, "under the grace + margin: start nothing"
    monkeypatch.setattr(signal, "getitimer", lambda which: (3600.0, 0.0), raising=False)
    assert K._budget_min() == 25.0
    monkeypatch.setattr(signal, "getitimer", lambda which: (0.0, 0.0), raising=False)
    assert K._budget_min() == 25.0, "no alarm (desktop): the cap"


def test_a_zero_budget_starts_no_request(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: _cat(3))
    monkeypatch.setattr(K, "_budget_min", lambda: 0.0)
    asked = []
    monkeypatch.setattr(K, "_fetch_table", lambda tid: asked.append(tid) or (tid, []))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert asked == [] and "3 deferred" in res.error, res.error


def test_first_missed_counts_only_releases_already_out():
    now = dt.datetime(2026, 9, 23, tzinfo=dt.timezone.utc)
    e = {"updatedAt": "2026-09-10T00:00:00Z",
         "updateDates": ["2026-08-10T00:00:00Z", "2026-09-10T00:00:00Z", "2026-10-10T00:00:00Z"]}
    assert K._first_missed(e, "2026-07-01T00:00:00Z", now) == dt.datetime(2026, 8, 10, tzinfo=dt.timezone.utc)
    assert K._first_missed(e, "2026-09-10T00:00:00Z", now) is None, "the October date is KSH's calendar, not out"


def test_a_desktop_budget_override_moves_the_stop_time_too(tmp_path, monkeypatch):
    """AQUEDUCT_BUDGET_MIN_OVERRIDE replaces the Deadline's budget on the workstation; the stop time must
    follow it, or a 600-min desktop pass would stop starting requests at 30 min."""
    seen = []

    def _cat_spy(raise_transient):
        seen.append(K.ig.STOP_AT[0] - K.time.time())
        return _cat(1)
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", _cat_spy)
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, []))
    monkeypatch.setattr(K, "_budget_min", lambda: 25.0)
    monkeypatch.setenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", "600")
    K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert seen and abs(seen[0] - (600 + K.STOP_GRACE_MIN) * 60) < 30, seen


def test_the_grace_and_the_pace_are_pinned(monkeypatch):
    monkeypatch.undo()                                      # the autouse no-pace patch
    assert K.STOP_GRACE_MIN == 5 and K.PACE_S == K.ig.RATE == 1.2 and K.MERGE_MARGIN_MIN == 5
    assert K.OWED_ATTENTION_DAYS == 45


# ---- review R1134: the never-fetched stored tables, and the limit outside the Tally ------------------
def _ago(days):
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _store(tmp_path, tids):
    pq.write_table(pa.table({"series_key": [f"KSH:{t}:x" for t in tids],
                             "obs_date": pa.array([dt.date(2024, 12, 31)] * len(tids)),
                             "value": [1.0] * len(tids)}), str(tmp_path / f"{tids[0][:3]}.parquet"))


def test_a_stored_table_the_updater_never_fetched_is_measured_from_its_backfill_vintage(tmp_path, monkeypatch):
    from updater.health import _deferral_only
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    old, now_ = _ago(K.OWED_ATTENTION_DAYS + 20), _ago(1)
    cat = [{"id": "tur0064", "updatedAt": now_, "correctedAt": None, "updateDates": [old, now_]},
           {"id": "tur0001", "updatedAt": "2025-06-01T00:00:00Z", "correctedAt": None},
           {"id": "tur0099", "updatedAt": now_, "correctedAt": None}]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)
    _store(tmp_path, ["tur0064", "tur0001"])
    (tmp_path / K.SIDECAR).write_text(json.dumps({"kkr0049": "2026-01-01T00:00:00Z|None"}))
    (tmp_path / K.BACKFILL).write_text(json.dumps({"tur0064": "2025-06-01T00:00:00Z|None",
                                                   "tur0001": "2025-06-01T00:00:00Z|None"}))
    asked = []
    monkeypatch.setattr(K, "_fetch_table", lambda tid: asked.append(tid) or (tid, "deadline"))
    monkeypatch.setattr(K, "TABLE_WAVE", 1)
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert "tur0001" not in asked, "unchanged since the backfill: current, not fetched"
    assert asked[0] == "tur0064", "a stored owed table goes on the owed side, first"
    assert f"rotation behind: tur0064 has waited {K.OWED_ATTENTION_DAYS + 20} days" in res.error, res.error
    assert "1 with nothing stored remain" in res.error, res.error              # tur0099
    assert not _deferral_only([{"status": res.status, "last_error": res.error}])


def test_negative_control_no_backfill_file_measures_nothing_and_says_so(tmp_path, monkeypatch, capsys, real_backfill):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    old, now_ = _ago(K.OWED_ATTENTION_DAYS + 20), _ago(1)
    cat = [{"id": "tur0064", "updatedAt": now_, "correctedAt": None, "updateDates": [old, now_]}]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)
    _store(tmp_path, ["tur0064"])
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, "deadline"))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert "rotation behind" not in res.error and "backfill vintages unavailable" in res.error, res.error
    assert f"{K.BACKFILL} is absent" in capsys.readouterr().out


@pytest.mark.parametrize("state", ["absent", "unreadable"])
def test_a_missing_backfill_file_makes_the_pass_attention_not_rotating(tmp_path, monkeypatch, state, real_backfill):
    """R1138: without the file 739-775 served tables go unmeasured; a clause in the rotation note (which
    health strips) left the source ROTATING."""
    from updater.health import _deferral_only
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: _cat(3))
    (tmp_path / K.SIDECAR).write_text(json.dumps({"gdp0001": "x|None"}))
    if state == "unreadable":
        real = K.blob.read_bytes
        monkeypatch.setattr(K.blob, "read_bytes", lambda p: (_ for _ in ()).throw(OSError("R2 down"))
                            if p.endswith(K.BACKFILL) else real(p))
    monkeypatch.setattr(K, "MAX_PER_RUN", 1)
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, []))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert "backfill vintages unavailable" in res.error and not _deferral_only(
        [{"status": res.status, "last_error": res.error}]), res.error
    (tmp_path / K.BACKFILL).write_text("{}")
    if state == "unreadable":
        monkeypatch.setattr(K.blob, "read_bytes", real)
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert "backfill vintages unavailable" not in (res.error or ""), "negative control: a present file"


def test_a_correctedat_change_on_a_backfill_table_is_owed(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    cat = [{"id": "tur0064", "updatedAt": "2025-06-01T00:00:00Z", "correctedAt": "2026-09-01T00:00:00Z"}]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)
    _store(tmp_path, ["tur0064"])
    (tmp_path / K.SIDECAR).write_text(json.dumps({"kkr0049": "x|None"}))
    (tmp_path / K.BACKFILL).write_text(json.dumps({"tur0064": "2025-06-01T00:00:00Z|None"}))
    asked = []
    monkeypatch.setattr(K, "_fetch_table", lambda tid: asked.append(tid) or (tid, []))
    K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert asked == ["tur0064"], "a correction alone changes the vintage: fetched"


def test_an_unchanged_backfill_table_without_its_theme_file_is_fetched(tmp_path, monkeypatch):
    """R1138: the backfill 'current' skip needs the theme parquet, like the sidecar path."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    cat = [{"id": "tur0064", "updatedAt": "2025-06-01T00:00:00Z", "correctedAt": None}]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)
    (tmp_path / K.SIDECAR).write_text(json.dumps({"kkr0049": "x|None"}))
    (tmp_path / K.BACKFILL).write_text(json.dumps({"tur0064": "2025-06-01T00:00:00Z|None"}))
    asked = []
    monkeypatch.setattr(K, "_fetch_table", lambda tid: asked.append(tid) or (tid, []))
    K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert asked == ["tur0064"], asked
    _store(tmp_path, ["tur0064"])
    asked.clear()
    K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert asked == [], "negative control: with its theme file it is current"


@pytest.mark.parametrize("days,behind", [(45, False), (46, True)])
def test_the_limit_is_strictly_past_45_days(tmp_path, monkeypatch, days, behind):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    now = dt.datetime.now(dt.timezone.utc)
    first = (now - dt.timedelta(days=days, hours=1)).isoformat()
    cat = [{"id": "gdp0001", "updatedAt": first, "correctedAt": None, "updateDates": [first]}]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)
    (tmp_path / K.SIDECAR).write_text(json.dumps({"gdp0001": "2025-01-01T00:00:00Z|None"}))
    (tmp_path / K.BACKFILL).write_text("{}")
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, "deadline"))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert ("rotation behind" in res.error) is behind, res.error


def test_an_overdue_table_does_not_hide_a_structural_break(tmp_path, monkeypatch):
    """12 tables that all parse empty raise DefinitiveError; one overdue owed table must not turn that
    into '1/13 transient-failed; will retry' (review R1134 finding 2)."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    old, now_ = _ago(K.OWED_ATTENTION_DAYS + 5), _ago(1)
    cat = _cat(12) + [{"id": "kkr0049", "updatedAt": now_, "correctedAt": None, "updateDates": [old, now_]}]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)
    (tmp_path / K.SIDECAR).write_text(json.dumps({"kkr0049": "2025-01-01T00:00:00Z|None"}))
    # kkr0049 is cut at the stop time (deferred) and stays owed past the limit
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, "deadline" if tid == "kkr0049" else []))
    order = []
    real = K.finalize
    monkeypatch.setattr(K, "finalize", lambda t, *a, **k: order.append((t.attempted, t.empty)) or real(t, *a, **k))
    with pytest.raises(K.DefinitiveError, match="all 12 attempted"):
        K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert order == [(12, 12)], order


def test_the_note_names_the_OLDER_of_two_owed_tables(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path))
    a, b, now_ = _ago(30), _ago(10), _ago(1)
    cat = [{"id": "gdp0001", "updatedAt": now_, "correctedAt": None, "updateDates": [b, now_]},
           {"id": "gdp0002", "updatedAt": now_, "correctedAt": None, "updateDates": [a, now_]}]
    monkeypatch.setattr(K, "_catalog", lambda raise_transient: cat)
    (tmp_path / K.SIDECAR).write_text(json.dumps({"gdp0001": "2025-01-01T00:00:00Z|None",
                                                  "gdp0002": "2025-01-01T00:00:00Z|None"}))
    monkeypatch.setattr(K, "_fetch_table", lambda tid: (tid, "deadline"))
    res = K.update(types.SimpleNamespace(config={}, key="ksh_stadat/_all"), None)
    assert "longest wait 30 days (gdp0002" in res.error, res.error


# ---- the one-time seed tool --------------------------------------------------------------------------
def _seed_env(tmp_path, monkeypatch):
    import importlib
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path / "store"))
    (tmp_path / "store").mkdir()
    snap = [{"id": t, "updatedAt": "2025-06-12T00:00:00Z", "correctedAt": None}
            for t in ("kkr0049", "szo0065", "szo0066", "mez0112")]
    (tmp_path / "cat.json").write_text(json.dumps(snap))
    s = tmp_path / "store"
    _store(s, ["kkr0049"])
    _store(s, ["szo0065"])                                    # szo0066 in the snapshot, no rows
    pq.write_table(pa.table({"series_key": ["KSH:mez0112:y"], "obs_date": pa.array([dt.date(2024, 1, 1)]),
                             "value": [1.0]}), str(s / "_migrated_from_ksh_unparsed.parquet"))
    (s / K.SIDECAR).write_text(json.dumps({"kkr0049": "2026-01-01T00:00:00Z|None"}))
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    return importlib.import_module("seed_ksh_backfill_vintages"), s


def test_the_seed_holds_only_stored_never_fetched_tables_and_is_one_time(tmp_path, monkeypatch):
    tool, s = _seed_env(tmp_path, monkeypatch)
    assert tool.main(["--catalog", str(tmp_path / "cat.json"), "--apply"]) == 0
    seed = json.loads((s / K.BACKFILL).read_text())
    # mez0112's rows are only in a side file (a retired source): its vintage is unknown, so NOT seeded (R1138)
    assert seed == {"szo0065": "2025-06-12T00:00:00Z|None"}, seed
    assert tool.main(["--catalog", str(tmp_path / "cat.json"), "--apply"]) == 2, "one-time: refuses to replace"


def test_the_seed_refuses_when_its_plant_does_not_read(tmp_path, monkeypatch):
    tool, s = _seed_env(tmp_path, monkeypatch)
    assert tool.main(["--catalog", str(tmp_path / "cat.json"), "--plant", "zzz0001", "--apply"]) == 2
    assert not (s / K.BACKFILL).exists()


def test_the_seed_fails_when_the_read_back_differs(tmp_path, monkeypatch):
    tool, s = _seed_env(tmp_path, monkeypatch)
    real = K.blob.read_bytes
    monkeypatch.setattr(tool.blob, "read_bytes", lambda p: b'{"x": 1}' if p.endswith(K.BACKFILL)
                        and (s / K.BACKFILL).exists() else real(p))
    assert tool.main(["--catalog", str(tmp_path / "cat.json"), "--apply"]) == 1
