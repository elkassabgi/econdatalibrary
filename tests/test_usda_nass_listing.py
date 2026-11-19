"""usda: the NASS datasets listing after the 2026-10 site move, and the vintage gate.

From 2026-10-02 every usda pass ended `transient_fail` for seven weeks. NASS moved its site:
"https://www.nass.usda.gov/datasets/" 301s to www-origin.nass.usda.gov/datasets, which is a 404,
while "https://www.nass.usda.gov/datasets" (no slash) is a 200 that links each dump as
/sites/default/files/datasets/<fname>. The fetcher built downloads as PAGE + fname, which is a
404 too. Failures stay TransientError (a DefinitiveError is booked `partial`, which would turn
the red SLA amber and pollute the admission-reserve history), but the message now says the
publisher moved the page and names the URL.

Second defect, same module: finalize() stamps new_vintage="date-tail", so the stored vintage
never matched the probe and every cadence re-ingested the whole database. update() now stamps
the listing hash on a clean run.

tests/fixtures/usda/nass_datasets_20261119.html is the REAL page, saved 2026-11-18/19 (43,780
bytes, the live Content-Length that day). No test here touches the network or starts a server:
the HTTP layer is a fake session, the ingest a fake module, the store a temp dir.
"""
import os
import sys
import types

import pytest
import requests

from updater import config
from updater.errors import DefinitiveError, TransientError
from updater.strategies.fetchers import usda

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE = os.path.join(ROOT, "tests", "fixtures", "usda", "nass_datasets_20261119.html")
VINTAGE = "20261118"
FILES = "https://www.nass.usda.gov/sites/default/files/datasets/"


def _page() -> str:
    with open(FIXTURE, encoding="utf-8") as fh:
        return fh.read()


class _Resp:
    def __init__(self, status=200, text="", body=b""):
        self.status_code = status
        self.text = text
        self.body = body
        self.headers = {"Content-Length": str(len(body))} if body else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error", response=self)

    def iter_content(self, chunk_size=1):
        yield self.body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Sess:
    """GET answers from a {url: _Resp | Exception} map; `default` for any other url."""

    def __init__(self, routes=None, default=None):
        self.routes, self.default, self.urls = dict(routes or {}), default, []

    def get(self, url, **kw):
        self.urls.append(url)
        r = self.routes.get(url, self.default)
        if isinstance(r, BaseException):
            raise r
        if r is None:
            raise AssertionError(f"unexpected GET {url}")
        return r


def _listing_sess(text=None, status=200):
    return _Sess({usda.PAGE: _Resp(status, _page() if text is None else text)})


# ---------------------------------------------------------------- listing -----------------

def test_lists_from_the_no_slash_url():
    s = _listing_sess()
    usda._listing(s)
    assert s.urls == ["https://www.nass.usda.gov/datasets"]


def test_saved_page_yields_all_five_sectors_at_one_vintage():
    got = usda._listing(_listing_sess())
    assert sorted(got) == sorted(usda.SECTORS)
    for sector, (fname, date, _url) in got.items():
        assert fname == f"qs.{sector}_{VINTAGE}.txt.gz"
        assert date == VINTAGE


def test_undated_census_files_are_ignored():
    page = _page()
    assert "qs.census2017.txt.gz" in page and "qs.census2022.txt.gz" in page   # control
    got = usda._listing(_listing_sess(page))
    assert not any("census" in f for f, _d, _u in got.values())


def test_download_urls_are_the_pages_own_hrefs_joined_to_the_page_constant():
    got = usda._listing(_listing_sess())
    for _sector, (fname, _date, url) in got.items():
        assert url == FILES + fname
        # The old PAGE + fname form is a 404 since the move; it must not come back.
        assert url not in (usda.PAGE + fname, usda.PAGE + "/" + fname)
        assert "www-origin" not in url


def test_relative_href_joins_to_the_site_not_a_redirect_host():
    page = '<a href="sites/default/files/datasets/qs.crops_20261118.txt.gz">x</a>'
    got = usda._listing(_listing_sess(page))
    assert got["crops"][2] == "https://www.nass.usda.gov/sites/default/files/datasets/qs.crops_20261118.txt.gz"


def test_a_sector_listed_twice_keeps_the_newest_date():
    page = ('<a href="/d/qs.crops_20261218.txt.gz">new</a>'
            '<a href="/d/qs.crops_20261118.txt.gz">old</a>')
    got = usda._listing(_listing_sess(page))
    assert got["crops"][:2] == ("qs.crops_20261218.txt.gz", "20261218")


def test_dump_names_in_text_but_not_in_hrefs_give_nothing():
    # The old parser scanned the whole text; a name without a link gives no download URL.
    with pytest.raises(TransientError, match="links no"):
        usda._listing(_listing_sess("<p>qs.crops_20261118.txt.gz</p>"))


@pytest.mark.parametrize("status", [404, 410])
def test_gone_page_is_transient_and_says_it_moved(status):
    with pytest.raises(TransientError) as ei:
        usda._listing(_listing_sess("Not Found", status))
    assert not isinstance(ei.value, DefinitiveError)
    msg = str(ei.value)
    assert f"HTTP {status}" in msg and "MOVED OR REMOVED" in msg and usda.PAGE in msg


def test_page_with_no_dumps_is_transient_and_says_so():
    with pytest.raises(TransientError, match="links no") as ei:
        usda._listing(_listing_sess("<html><a href='/x.pdf'>x</a></html>"))
    assert usda.PAGE in str(ei.value)


@pytest.mark.parametrize("exc", [requests.Timeout("read timed out"),
                                 requests.ConnectionError("connection reset")])
def test_network_errors_stay_transient(exc):
    with pytest.raises(TransientError, match="unreachable"):
        usda._listing(_Sess({usda.PAGE: exc}))


@pytest.mark.parametrize("status", [403, 429, 500, 503])
def test_other_http_errors_stay_transient(status):
    with pytest.raises(TransientError, match="unreachable"):
        usda._listing(_listing_sess("", status))


# ---------------------------------------------------------------- probe -------------------

def test_probe_returns_the_hash_on_the_saved_page(monkeypatch):
    page = _page()
    monkeypatch.setattr(usda.requests, "Session", lambda: _listing_sess(page))
    v = usda.current_vintage(None)
    assert v is not None and v.startswith("usda:") and len(v) == len("usda:") + 16
    # It moves with a datestamp.
    monkeypatch.setattr(usda.requests, "Session",
                        lambda: _listing_sess(page.replace("crops_20261118", "crops_20261218")))
    assert usda.current_vintage(None) not in (None, v)


def test_probe_is_none_when_the_page_is_gone(monkeypatch):
    monkeypatch.setattr(usda.requests, "Session", lambda: _listing_sess("", 404))
    assert usda.current_vintage(None) is None


def test_probe_is_none_when_a_sector_is_missing(monkeypatch):
    page = _page().replace("qs.economics_20261118", "qs.economix_20261118")
    monkeypatch.setattr(usda.requests, "Session", lambda: _listing_sess(page))
    assert usda.current_vintage(None) is None


# ---------------------------------------------------------------- update() ----------------

@pytest.fixture
def rig(tmp_path, monkeypatch):
    """update() with every side effect inside tmp_path: raw dir, store dir, ingest, publish."""
    root = tmp_path / "root"
    out = tmp_path / "store"
    raw = root / "data" / "raw" / "usda"
    monkeypatch.setattr(usda, "_root", lambda: str(root))
    monkeypatch.setattr(config, "source_dir", lambda s: str(out))
    monkeypatch.setattr(usda.blob, "publish_file", lambda p: False)
    monkeypatch.setattr(usda, "_table_cursors", lambda o: {})
    monkeypatch.setattr(sys, "path", list(sys.path))
    calls = []
    fake = types.ModuleType("jobs.ingest_usda")
    fake.main = lambda: calls.append(sorted(os.listdir(raw)))
    monkeypatch.setitem(sys.modules, "jobs.ingest_usda", fake)
    try:
        import jobs
        monkeypatch.setattr(jobs, "ingest_usda", fake, raising=False)
    except ImportError:
        pass

    def run(routes):
        sess = _Sess(dict({usda.PAGE: _Resp(200, _page())}, **routes),
                     default=_Resp(200, body=b"gz"))
        monkeypatch.setattr(usda.requests, "Session", lambda: sess)
        return usda.update(None, None), sess

    return types.SimpleNamespace(run=run, raw=raw, out=out, calls=calls)


def _expected_vintage():
    return usda._vintage_of(usda._listing(_listing_sess()))


def test_clean_run_downloads_from_hrefs_and_stamps_the_listing_hash(rig):
    res, sess = rig.run({})
    assert sorted(sess.urls[1:]) == sorted(FILES + f"qs.{s}_{VINTAGE}.txt.gz" for s in usda.SECTORS)
    assert res.status in ("ok", "no_change")
    assert res.new_vintage == _expected_vintage() and res.new_vintage != "date-tail"
    assert rig.calls and len(rig.calls[0]) == 5


def test_one_file_404_is_a_transient_unit_and_a_partial(rig):
    bad = FILES + f"qs.crops_{VINTAGE}.txt.gz"
    res, _ = rig.run({bad: _Resp(404, "Not Found")})
    assert res.status == "partial"
    assert "crops" in (res.error or "")
    assert res.new_vintage is None                # the gate must NOT advance on a partial
    assert rig.calls                              # the other four still ingest


def test_missing_sector_refuses_as_transient(rig):
    page = _page().replace("qs.economics_20261118", "qs.economix_20261118")
    with pytest.raises(TransientError, match="no longer lists economics") as ei:
        rig.run({usda.PAGE: _Resp(200, page)})
    assert not isinstance(ei.value, DefinitiveError)
    assert not rig.calls


def test_already_downloaded_sector_keeps_its_sentinel(rig):
    rig.raw.mkdir(parents=True)
    (rig.raw / f"qs.crops_{VINTAGE}.txt.gz").write_bytes(b"x" * 1_000_001)
    for s in ("crops", "economics"):
        (rig.out / s).mkdir(parents=True)
        (rig.out / s / "_complete").write_text("")
    res, sess = rig.run({})
    assert FILES + f"qs.crops_{VINTAGE}.txt.gz" not in sess.urls         # not re-downloaded
    assert (rig.out / "crops" / "_complete").exists()                     # not re-ingested
    assert not (rig.out / "economics" / "_complete").exists()             # new dump: cleared
    assert res.new_vintage == _expected_vintage()
