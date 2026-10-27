"""A connection reset while census.gov streams a body must cost ONE flow, never the whole source.

Run 3437 (2026-10-27, the post-merge proof pass of the pin fix) died after 1,933 s with
`UNEXPECTED:ChunkedEncodingError(ProtocolError("Connection broken: ConnectionResetError(10054 ...`,
the same note as run 542 (2026-08-06). requests raises ChunkedEncodingError for a reset mid-body; it
is not a requests.ConnectionError, so _fetch did not retry it, update() did not catch it, and the
orchestrator booked the whole unit transient_fail with NO Result - no cursors, no CSV derive for the
EITS flows that pass had already merged.
"""
from __future__ import annotations

import os
import sys
import types

import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import census as cs  # noqa: E402
from tests.test_census_pins_retired import _rows, _setup  # noqa: E402

_RESET = 'Connection broken: ConnectionResetError(10054, "forcibly closed by the remote host")'


def _flaky(api, flow_suffix, exc, failures):
    """Make api.get raise `exc` for the first `failures` calls on one flow (all calls if None)."""
    real, seen = api.get, {"n": 0}

    def get(url, params=None, timeout=None):
        if url.endswith(flow_suffix) and (failures is None or seen["n"] < failures):
            seen["n"] += 1
            raise exc(_RESET)
        return real(url, params=params, timeout=timeout)
    api.get = get
    return seen


@pytest.mark.parametrize("exc", [requests.exceptions.ChunkedEncodingError,
                                 requests.exceptions.ContentDecodingError])
def test_a_broken_stream_is_retried_and_the_retry_merges(tmp_path, monkeypatch, capsys, exc):
    api = _setup(tmp_path, monkeypatch)
    seen = _flaky(api, "eits/marts", exc, failures=1)
    res = cs.update(types.SimpleNamespace(source_id="census", unit_id="_all"), None)
    assert seen["n"] == 1
    assert res.status in ("ok", "no_change"), res
    assert sorted(r["cell_value"] for r in _rows(tmp_path, "eits__marts")
                  if r["obs_date"].isoformat() == "2026-03-01") == ["12", "22"]
    assert "eits/marts: RECEIPT window>=2026-02-01 returned=4 skipped_new=0 stored=4 MATCH" \
        in capsys.readouterr().out


def test_a_stream_that_keeps_breaking_fails_that_flow_only(tmp_path, monkeypatch, capsys):
    """Every attempt breaks on the BETA flow: it is a named transient sub-unit, and marts - which
    merged in the same pass - keeps its cursor, so its CSV is derived (the Result survives)."""
    api = _setup(tmp_path, monkeypatch)
    seen = _flaky(api, "intltrade/exports/sitcexport", requests.exceptions.ChunkedEncodingError,
                  failures=None)
    res = cs.update(types.SimpleNamespace(source_id="census", unit_id="_all"), None)
    assert seen["n"] == cs.MAX_ATTEMPTS
    assert res.status == "partial", res
    assert "1/2 sub-unit(s) transient-failed" in (res.error or ""), res.error
    assert "sitcexport" in res.error and "ChunkedEncodingError" in res.error
    assert (res.series_cursors or {}).get("eits__marts") == "2026-03-01", res.series_cursors
    out = capsys.readouterr().out
    assert "[census] SUB-UNIT FAILED - intltrade/exports/sitcexport time=2025-12" in out
    assert "TransientError" in out


def test_the_retry_tuple_names_both_stream_errors():
    assert requests.exceptions.ChunkedEncodingError in cs._RETRYABLE
    assert requests.exceptions.ContentDecodingError in cs._RETRYABLE
    # the reason they must be listed: neither is a ConnectionError
    assert not issubclass(requests.exceptions.ChunkedEncodingError, requests.ConnectionError)
