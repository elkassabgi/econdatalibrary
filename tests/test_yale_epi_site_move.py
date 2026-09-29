"""yale_epi after Yale's 2026 site redesign: downloads found from the home page's /<year>/downloads link, the
country vocabulary read from the edition's indicator zip, a retired historic URL (HTML page) skipped, and a
workbook never parsed without the vocabulary (it would fork the published numeric ids into alpha-3 ones).
Hermetic: HTTP is faked; the store is a tmp dir under the LOCAL backend; the merge is the real one."""
from __future__ import annotations

import io
import os
import sys
import zipfile

import pytest

openpyxl = pytest.importorskip("openpyxl")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import yale_epi as Y  # noqa: E402
from updater.errors import DefinitiveError  # noqa: E402

FILES = "https://epi.yale.edu/sites/default/files/2026-09/"
XLSX = FILES + "epi2026results2026-07-07.xlsx"
ZIP = FILES + "epi2026_indicators_na_2026-08-31.zip"
HTML = b"\n<!DOCTYPE html>\n<html lang='en'><body>" + b"x" * 2000 + b"</body></html>"


def _xlsx():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "data"
    ws.append(["iso", "country", "EPI.new", "AGR.new"])
    ws.append(["AFG", "Afghanistan", 30.5, 12.0])
    ws.append(["PLW", "Palau", 55.0, None])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("readme.txt", "x")
        z.writestr("BCA_ind_na.csv", "code,iso,country,BCA.ind.1996\n4,AFG,Afghanistan,1\n585,PLW,Palau,2\n")
    return buf.getvalue()


class _Resp:
    def __init__(self, status, content, ctype):
        self.status_code, self.content, self.headers = status, content, {"content-type": ctype}
        self.text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content


def _site(routes):
    def get(url, **kw):
        if url in routes:
            return routes[url]
        return _Resp(404, HTML, "text/html; charset=UTF-8")
    return get


def _routes(**over):
    r = {
        Y.HOME: _Resp(200, b'<a href="/2026/downloads">Downloads</a>', "text/html"),
        "https://epi.yale.edu/2026/downloads": _Resp(
            200, f'<a href="/sites/default/files/2026-09/epi2026results2026-07-07.xlsx">r</a>'
                 f'<a href="/sites/default/files/2026-09/epi2026_indicators_na_2026-08-31.zip">z</a>'.encode(),
            "text/html"),
        XLSX: _Resp(200, _xlsx(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        ZIP: _Resp(200, _zip(), "application/zip"),
        Y.KNOWN_URLS[0][0]: _Resp(200, HTML, "text/html; charset=UTF-8"),       # the retired 2024 CSV
    }
    r.update(over)
    return r


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(Y.config, "source_dir", lambda s: str(tmp_path / s))
    return tmp_path


def _keys(tmp_path):
    import pyarrow.parquet as pq
    return set(pq.read_table(str(tmp_path / "yale_epi" / "yale_epi.parquet")).column("series_key").to_pylist())


def test_the_year_scoped_downloads_page_is_found_from_the_home_page(store, monkeypatch):
    monkeypatch.setattr(Y.requests, "get", _site(_routes()))
    results, vocab = Y._scan()
    assert results == [(XLSX, 2026)] and vocab == [(ZIP, 2026)]


RETIRED = "https://epi.yale.edu/downloads/epi2024results.csv"     # Yale's dead URL: an UNPINNED floor, retired


def _retired_floor(monkeypatch):
    """The scenario these tests were written for: an unpinned floor URL answering an HTML page (a pinned archive
    answering HTML is a break instead - tests/test_yale_epi_dataverse_2024.py)."""
    monkeypatch.setattr(Y, "KNOWN_URLS", [(RETIRED, 2024)])
    return {RETIRED: _Resp(200, HTML, "text/html; charset=UTF-8")}


def test_the_edition_parses_into_the_published_numeric_ids_and_the_retired_csv_is_skipped(store, monkeypatch):
    monkeypatch.setattr(Y.requests, "get", _site(_routes(**_retired_floor(monkeypatch))))
    res = Y.update(None, None)
    assert res.status == "ok", (res.status, res.error)
    assert _keys(store) == {"EPI:EPI.new:4", "EPI:AGR.new:4", "EPI:EPI.new:585"}


def test_without_a_vocabulary_the_workbook_is_refused_not_forked(store, monkeypatch):
    routes = _routes()
    routes["https://epi.yale.edu/2026/downloads"] = _Resp(
        200, b'<a href="/sites/default/files/2026-09/epi2026results2026-07-07.xlsx">r</a>', "text/html")
    monkeypatch.setattr(Y.requests, "get", _site(routes))
    with pytest.raises(DefinitiveError, match="forked alpha-3"):
        Y.update(None, None)
    assert not (store / "yale_epi" / "yale_epi.parquet").exists(), "nothing merged"


def test_a_current_file_answering_html_is_a_structural_break(store, monkeypatch):
    monkeypatch.setattr(Y.requests, "get", _site(_routes(**{XLSX: _Resp(200, HTML, "text/html")})))
    with pytest.raises(DefinitiveError, match="an HTML page"):
        Y.update(None, None)


# --- round 2 (review R1287) ---------------------------------------------------------------------------
CSV2024 = Y.KNOWN_URLS[0][0]


def test_a_known_url_that_is_also_discovered_and_answers_html_is_a_break(store, monkeypatch):
    # A floor URL of Yale's own shape (the 2024 floor is now a Dataverse URL, which no downloads page links).
    yale_csv = "https://epi.yale.edu/downloads/epi2024results.csv"
    monkeypatch.setattr(Y, "KNOWN_URLS", [(yale_csv, 2024)])
    routes = _routes(**{yale_csv: _Resp(200, HTML, "text/html; charset=UTF-8")})
    routes["https://epi.yale.edu/2026/downloads"] = _Resp(
        200, (f'<a href="/sites/default/files/2026-09/epi2026results2026-07-07.xlsx">r</a>'
              f'<a href="/sites/default/files/2026-09/epi2026_indicators_na_2026-08-31.zip">z</a>'
              f'<a href="{yale_csv}">old</a>').encode(), "text/html")
    monkeypatch.setattr(Y.requests, "get", _site(routes))
    with pytest.raises(DefinitiveError, match="an HTML page"):
        Y.update(None, None)


def test_a_floor_url_with_a_real_body_is_parsed(store, monkeypatch):
    body = b"code,iso,country,EPI.old\n4,AFG,Afghanistan,20.0\n"
    import hashlib
    monkeypatch.setitem(Y.PINNED_MD5, CSV2024, hashlib.md5(body + b"\n" * 600).hexdigest())
    monkeypatch.setattr(Y.requests, "get",
                        _site(_routes(**{CSV2024: _Resp(200, body + b"\n" * 600, "text/csv")})))
    res = Y.update(None, None)
    assert res.status == "ok" and "EPI:EPI.old:4" in _keys(store), (res.status, sorted(_keys(store)))


def test_an_absolute_year_page_link_with_a_trailing_slash_is_followed(store, monkeypatch):
    routes = _routes(**{Y.HOME: _Resp(200, b'<a href="https://epi.yale.edu/2026/downloads/">D</a>', "text/html")})
    routes["https://epi.yale.edu/2026/downloads/"] = routes["https://epi.yale.edu/2026/downloads"]
    monkeypatch.setattr(Y.requests, "get", _site(routes))
    assert Y._scan()[0] == [(XLSX, 2026)]


def test_an_alpha3_only_csv_without_a_vocabulary_is_refused_not_forked(store, monkeypatch):
    csv_url = FILES + "epi2026results.csv"
    routes = _routes()
    routes["https://epi.yale.edu/2026/downloads"] = _Resp(
        200, b'<a href="/sites/default/files/2026-09/epi2026results.csv">r</a>', "text/html")
    routes[csv_url] = _Resp(200, b"iso,country,EPI.new\nAFG,Afghanistan,30.5\n" + b"\n" * 600, "text/csv")
    monkeypatch.setattr(Y.requests, "get", _site(routes))
    with pytest.raises(DefinitiveError, match="forked alpha-3"):
        Y.update(None, None)
    assert not (store / "yale_epi" / "yale_epi.parquet").exists()


def test_a_vocabulary_zip_blip_is_transient_not_a_break(store, monkeypatch):
    monkeypatch.setattr(Y.requests, "get", _site(_routes(**{ZIP: _Resp(503, b"busy", "text/plain"),
                                                            **_retired_floor(monkeypatch)})))
    res = Y.update(None, None)
    assert res.status == "partial" and "vocabulary zip was unavailable" in (res.error or ""), (res.status, res.error)


def test_a_redesign_that_lists_nothing_is_still_loud(store, monkeypatch):
    monkeypatch.setattr(Y.requests, "get", _site(_routes(**{Y.HOME: _Resp(200, b"<html>no links</html>",
                                                                          "text/html")})))
    with pytest.raises(DefinitiveError, match="listed no epiYYYYresults"):
        Y.update(None, None)
