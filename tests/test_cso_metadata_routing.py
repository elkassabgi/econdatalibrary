"""cso: a matrix absent from the Search listing routes via its OWN metadata (R61 class).

Pinned 2026-08-05: SIH13/SIA208 exist upstream with full metadata (probed live) yet are
absent from CSO's Search API, so the Search-built subject map retried them forever —
up to 222 matrices eating ~45% of every run's budget. ReadMetadata's extension.subject
carries the exact {code, value} _subject_key needs.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload


def test_metadata_subject_routes_like_subject_key(monkeypatch):
    from updater.strategies.fetchers import cso as C
    payload = {"result": {"label": "At Risk of Poverty Rate Threshold",
                          "extension": {"subject": {"code": 106,
                                                    "value": "Survey on Income and "
                                                             "Living Conditions (SILC)"}}}}
    monkeypatch.setattr(C.requests, "post", lambda *a, **k: _Resp(200, payload))
    got = C._subject_from_metadata("SIH13")
    want = C._subject_key({"SbjCode": 106,
                           "SbjValue": "Survey on Income and Living Conditions (SILC)"})
    assert got == want and got.startswith("106_"), (got, want)


def test_metadata_probe_failure_is_none(monkeypatch):
    from updater.strategies.fetchers import cso as C
    monkeypatch.setattr(C.requests, "post", lambda *a, **k: _Resp(500, {}))
    assert C._subject_from_metadata("SIH13") is None
    monkeypatch.setattr(C.requests, "post", lambda *a, **k: _Resp(200, {"result": None}))
    assert C._subject_from_metadata("SIH13") is None
