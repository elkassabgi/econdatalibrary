"""worldbank_esg must tell an ARCHIVED indicator from a quiet year.

Measured 2026-08-07: 13 of our 71 indicators had been deleted by the World Bank, 9 of them
catalogued and served. Each returned HTTP 200 carrying a message envelope, which is a
ONE-element list, so `_fetch_window`'s `len(j) < 2` fallthrough returned [] and the caller
booked `empty_unit` — "a quiet window is normal for annual data". The source reported healthy
while that data could never update again.
"""
from __future__ import annotations

import pytest

from updater.errors import TransientError
from updater.strategies.fetchers import worldbank_esg as W


class TestMessageEnvelopeIsNotAnEmptyWindow:
    """A complaint from the API must never be mistaken for 'no observations this window'."""

    def test_message_envelope_raises_instead_of_returning_empty(self, monkeypatch):
        monkeypatch.setattr(W.ig, "get_json", lambda url: [
            {"message": [{"id": "120", "key": "Invalid value",
                          "value": "The provided parameter value is not valid"}]}])
        with pytest.raises(TransientError):
            W._fetch_window("EN.ATM.CO2E.PC", "2020:2026")

    def test_a_genuinely_short_envelope_is_still_an_empty_window(self, monkeypatch):
        """No `message` key -> the old lenient behaviour stands; this must not become a
        blanket 'any short response is an error'."""
        monkeypatch.setattr(W.ig, "get_json", lambda url: [{"page": 1, "total": 0}])
        assert W._fetch_window("X", "2020:2026") == []

    def test_normal_envelope_returns_rows(self, monkeypatch):
        monkeypatch.setattr(W.ig, "get_json",
                            lambda url: [{"page": 1}, [{"value": 1}, {"value": 2}]])
        assert len(W._fetch_window("X", "2020:2026")) == 2


class TestPublishedIndicatorListing:
    """Retirement is decided against a STRUCTURED fact, never a formatted message — the id
    for a deleted indicator is 175 without `source=` and 120 with it, and 120 also means an
    ordinary bad parameter, so the message cannot carry a permanent verdict."""

    def test_returns_the_ids_across_pages(self, monkeypatch):
        pages = {1: [{"pages": 2}, [{"id": "A"}, {"id": "B"}]],
                 2: [{"pages": 2}, [{"id": "C"}]]}
        monkeypatch.setattr(W.ig, "get_json",
                            lambda url: pages[int(url.rsplit("page=", 1)[1])])
        assert W._published_indicators() == {"A", "B", "C"}

    def test_unreadable_listing_returns_None_not_empty(self, monkeypatch):
        """THE FAIL-SAFE. An empty set would mean 'every indicator we hold is archived' and
        would retire the entire source in one run; None means 'do not touch the stored set'."""
        def boom(url):
            raise RuntimeError("WB down")
        monkeypatch.setattr(W.ig, "get_json", boom)
        assert W._published_indicators() is None

    def test_malformed_listing_returns_None_not_empty(self, monkeypatch):
        monkeypatch.setattr(W.ig, "get_json", lambda url: {"unexpected": "shape"})
        assert W._published_indicators() is None


def test_retired_sidecar_is_blob_routed(tmp_path):
    """A local write is scratch on a CI runner, so the retirement would be re-learned every
    run and never actually shorten the work list (the R36 class)."""
    import inspect
    src = inspect.getsource(W)
    assert "blob.write_bytes_atomic(\n                _retired_path(out_dir)" in src \
        or "blob.write_bytes_atomic(" in src
    assert "_retired_path" in src
    # and it must be READ through blob too, not open()
    assert "blob.read_bytes(_retired_path(out_dir))" in src


class TestNewlyPublishedIndicators:
    """The work list came from the files we ALREADY hold, so an indicator the World Bank
    ADDS could never be fetched — measured 2026-08-07: 22 of the publisher's 80 source-75
    indicators had no file here and nothing in the loop could create one."""

    def test_a_new_indicator_gets_the_full_history_not_a_tail(self, tmp_path):
        """A non-existent store file means 'we hold nothing', which wants 1960:now — not the
        short default window, which would ingest a new indicator already truncated."""
        w = W._window(str(tmp_path / "NEVER_SEEN.parquet"), None)
        assert w.startswith("1960:"), w

    def test_an_existing_file_still_gets_a_tail(self, tmp_path, monkeypatch):
        """The new branch must not turn every refresh into a full re-pull."""
        monkeypatch.setattr(W.blob, "exists", lambda p: True)
        monkeypatch.setattr(W, "_stored_max_year", lambda p: 2024)
        w = W._window(str(tmp_path / "HELD.parquet"), None)
        assert not w.startswith("1960:"), w
        assert w.startswith(str(2024 - W.LOOKBACK_YEARS)), w
