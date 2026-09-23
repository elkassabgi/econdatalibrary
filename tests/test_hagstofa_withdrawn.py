"""hagstofa: a stored table the publisher WITHDREW is kept frozen (quiet), not a structural break
re-fired every run - but only when its folder listing, read, no longer lists it.

Measured live 2026-09-23: Atvinnuvegir/sjavarutvegur/utf/SJA04901.px (exported marine products by
categories and species, 1999-2024; 6,952 stored rows) answered HTTP 400, and the folder listing held
SJA04902..SJA04907 only. The 2026-09-15 edition of SJA04903 carries Species x Country x Product
category. The real _get_meta and _listed_in_folder run; only HTTP is faked.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import hagstofa as H  # noqa: E402

PATH = "sjavarutvegur/utf/SJA04901.px"
# The live listing's shape, 2026-09-23 (two of its six entries).
LISTING = [{"id": "SJA04902.px", "type": "t", "text": "Exported marine products by product categories 1979-1998",
            "updated": "2024-09-24T09:00:06"},
           {"id": "SJA04903.px", "type": "t", "text": "Exported marine products by categories and countries 1999-2025",
            "updated": "2026-09-15T09:00:22"}]


class _R:
    def __init__(self, status, body=None, bad_json=False):
        self.status_code, self._body, self._bad = status, body, bad_json

    def json(self):
        if self._bad:
            raise ValueError("not json")
        return self._body


class _Sess:
    def __init__(self, folder):
        self.folder, self.asked = folder, []

    def get(self, url, timeout=None):
        self.asked.append(url)
        if url.endswith("SJA04901.px/"):
            return _R(400, {"error": "Bad request"})
        if url.endswith("/sjavarutvegur/utf/"):
            return self.folder() if callable(self.folder) else self.folder
        raise AssertionError(f"unexpected GET {url}")


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(H.time, "sleep", lambda s: None)


def _fetch(sess, since=dt.date(2024, 12, 31)):
    return H._fetch_table(sess, "Atvinnuvegir", PATH, "ICE:Atvinnuvegir:sjavarutvegur:utf:SJA04901.px", since)


def test_a_stored_table_withdrawn_from_its_folder_is_kept_frozen():
    sess = _Sess(_R(200, LISTING))
    assert _fetch(sess) == ([], "quiet")
    assert sess.asked[-1].endswith("/Atvinnuvegir/sjavarutvegur/utf/"), sess.asked


def test_a_400_on_a_table_its_folder_still_lists_is_a_break():
    """Negative control: PxWeb also answers 400 mid-republication."""
    listed = LISTING + [{"id": "SJA04901.px", "type": "t", "text": "x", "updated": "2026-09-15T09:00:22"}]
    assert _fetch(_Sess(_R(200, listed))) == ([], "structural")


@pytest.mark.parametrize("folder", [_R(500, LISTING), _R(429, LISTING), _R(404, LISTING), _R(200, []),
                                    _R(200, {"a": 1}),
                                    _R(200, bad_json=True)])
def test_a_listing_that_cannot_be_read_is_not_evidence_of_withdrawal(folder):
    assert _fetch(_Sess(folder)) == ([], "structural")


def test_a_listing_that_raises_is_not_evidence_of_withdrawal():
    def _boom():
        raise H.requests.ConnectionError("dropped")
    assert _fetch(_Sess(_boom)) == ([], "structural")


def test_a_never_stored_table_that_400s_is_absent_without_asking_the_folder():
    sess = _Sess(_R(200, LISTING))
    assert _fetch(sess, since=None) == ([], "empty")
    assert len(sess.asked) == 1, sess.asked
