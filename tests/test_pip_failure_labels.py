"""pip names WHICH poverty line failed, and why.

WHY. From 2026-11-05 pip's stored note read "1/2 sub-unit(s) transient-failed; will retry"
for six runs in a row. pip.update() tallies one sub-unit per poverty line that fails and ONE
added_unit for the merged table, so "1/2" says exactly one of the ten lines failed - but not
which one, and not the cause. The reason was known at the raise site (_get_rows) and dropped.

These tests drive the real update() with requests.get replaced by a canned responder, so no
request leaves the machine. They pin PROPERTIES: the line's povline and label appear in the
note, the cause appears with it, and the counts are unchanged.
"""
from __future__ import annotations

import pytest

from updater.strategies.fetchers import _common
from updater.strategies.fetchers import pip

TIMEOUT_ENVELOPE = {"ok": [False], "error": ["Request exceeded timeout of 180 seconds"]}


class _Resp:
    def __init__(self, status, payload=None, raw=b""):
        self.status_code = status
        self._payload = payload
        self.content = raw

    def json(self):
        if self._payload is None:
            raise ValueError("no JSON")
        return self._payload


def _record(pline):
    return {"country_code": "AAA", "reporting_year": 2020, "headcount": 0.1 + pline / 100.0}


@pytest.fixture
def run(monkeypatch, tmp_path):
    """run(responder) -> Result. responder(pline) returns a _Resp or raises."""
    calls = []
    monkeypatch.setattr(pip.time, "sleep", lambda s: None)
    monkeypatch.setattr(pip, "_current_release_for_ppp", lambda *a, **k: "20260324_2017_01_02_PROD")
    monkeypatch.setattr(pip.config, "source_dir", lambda src: str(tmp_path / src))
    monkeypatch.setattr(pip.blob, "row_count", lambda path: 0)
    monkeypatch.setattr(pip.merge, "merge_and_write",
                        lambda path, tbl, **kw: (tbl.num_rows, None))

    def go(responder):
        def fake_get(url, params=None, headers=None, timeout=None):
            pl = float(params["povline"])
            calls.append(pl)
            return responder(pl)
        monkeypatch.setattr(pip.requests, "get", fake_get)
        return pip.update(unit=None, since=None)

    go.calls = calls
    return go


def test_server_timeout_on_one_line_names_that_line_and_the_cause(run):
    """The inferred production shape: 21.70 answers PIP's 180 s timeout envelope every try."""
    res = run(lambda pl: _Resp(200, TIMEOUT_ENVELOPE) if pl == 21.70 else _Resp(200, [_record(pl)]))
    assert res.status == "partial"
    assert res.error.startswith("1/2 sub-unit(s) transient-failed; will retry")
    assert "povline 21.70 (2170)" in res.error
    assert "Request exceeded timeout of 180 seconds" in res.error
    assert "x4" in res.error                       # retries exhausted, stated
    for other in ("povline 1.00", "povline 15.00"):
        assert other not in res.error              # only the failing line is named
    assert run.calls.count(21.70) == 4            # retry count unchanged


@pytest.mark.parametrize("status", [429, 503])
def test_http_status_is_named(run, status):
    res = run(lambda pl: _Resp(status) if pl == 3.20 else _Resp(200, [_record(pl)]))
    assert res.status == "partial"
    assert f"povline 3.20 (320): HTTP {status} x4" in res.error


def test_client_timeout_is_named(run):
    def responder(pl):
        if pl == 1.90:
            raise pip.requests.Timeout("read timed out")
        return _Resp(200, [_record(pl)])
    res = run(responder)
    assert "povline 1.90 (190): Timeout" in res.error


def test_two_failing_lines_are_both_named(run):
    res = run(lambda pl: _Resp(502) if pl in (10.00, 21.70) else _Resp(200, [_record(pl)]))
    assert res.error.startswith("2/3 sub-unit(s) transient-failed")
    assert "povline 10.00 (1000): HTTP 502" in res.error
    assert "povline 21.70 (2170): HTTP 502" in res.error


def test_structural_break_names_the_line(run):
    """A hard 4xx raises DefinitiveError; its message must name the line too."""
    with pytest.raises(_common.DefinitiveError) as ei:
        run(lambda pl: _Resp(404) if pl == 6.85 else _Resp(200, [_record(pl)]))
    assert "povline 6.85 (685): HTTP 404" in str(ei.value)


def test_unparseable_records_name_the_line(run):
    bad = [{"country_code": "AAA", "reporting_year": 2020, "headcount": "n/a"}]
    with pytest.raises(_common.DefinitiveError) as ei:
        run(lambda pl: _Resp(200, bad) if pl == 5.50 else _Resp(200, [_record(pl)]))
    assert "povline 5.50 (550): HTTP 200 with 1 record(s), 0 parsed" in str(ei.value)


def test_all_ok_note_is_unchanged(run):
    """Negative control: a clean run carries no failure label at all."""
    res = run(lambda pl: _Resp(200, [_record(pl)]))
    assert res.status == "ok"
    assert res.error == "+10 new rows"
    assert "povline" not in res.error


def test_versions_catalog_failure_is_labelled(monkeypatch, tmp_path):
    monkeypatch.setattr(pip.config, "source_dir", lambda src: str(tmp_path / src))
    monkeypatch.setattr(pip, "_current_release_for_ppp", lambda *a, **k: None)
    monkeypatch.setattr(pip.blob, "row_count", lambda path: 0)
    res = pip.update(unit=None, since=None)
    assert res.status == "partial"
    assert "versions catalog" in res.error and "2017-PPP" in res.error
