"""yale_epi's 2024 edition from the publisher's Dataverse archive (review R1299): the pinned file is taken only with
the md5 the dataset lists, and its country map only SUPPLEMENTS the current edition's indicator-zip map - an older
map keying the newer workbook would drop every country it does not know (R1287). Hermetic: HTTP faked, LOCAL store."""
from __future__ import annotations

import hashlib
import os
import sys

import pytest

pytest.importorskip("openpyxl")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import yale_epi as Y  # noqa: E402

from tests.test_yale_epi_site_move import HTML, ZIP, _Resp, _keys, _routes, _site  # noqa: E402,F401

ARCH = Y.DATAVERSE_2024
PAD = b"\n" * 600


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(Y.config, "source_dir", lambda s: str(tmp_path / s))
    return tmp_path


def _archive(monkeypatch, body):
    """Serve `body` at the pinned archive URL, pinned to its own md5 (the real pin is the publisher's)."""
    monkeypatch.setitem(Y.PINNED_MD5, ARCH, hashlib.md5(body).hexdigest())
    return _Resp(200, body, "text/comma-separated-values")


def test_the_2024_floor_is_the_publishers_dataverse_file_pinned_by_md5():
    assert Y.KNOWN_URLS == [(ARCH, 2024)]
    assert ARCH.startswith("https://dataverse.harvard.edu/api/access/datafile/14094607")
    assert Y.PINNED_MD5[ARCH] == "688e7ee38f02d7698e3f299a40ef3fc0"


def test_the_archive_restores_the_countries_under_code_100(store, monkeypatch):
    body = b"code,iso,country,EPI.new\n4,AFG,Afghanistan,20.0\n156,CHN,China,35.0\n" + PAD
    monkeypatch.setattr(Y.requests, "get", _site(_routes(**{ARCH: _archive(monkeypatch, body)})))
    res = Y.update(None, None)
    assert res.status == "ok", (res.status, res.error)
    assert {"EPI:EPI.new:4", "EPI:EPI.new:156"} <= _keys(store)


def test_other_bytes_at_the_pinned_url_are_a_break_not_merged(store, monkeypatch):
    body = b"code,iso,country,EPI.new\n4,AFG,Afghanistan,20.0\n" + PAD           # the real pin stays in place
    monkeypatch.setattr(Y.requests, "get", _site(_routes(**{ARCH: _Resp(200, body, "text/csv")})))
    with pytest.raises(DefinitiveError, match="pinned archive file answered with other bytes"):
        Y.update(None, None)


def test_the_archive_map_supplements_the_zip_map_never_replaces_it(store, monkeypatch):
    """The 2024 CSV knows AFG only, and under a WRONG code; the 2026 zip knows AFG=4 and PLW=585. The workbook's PLW
    must survive (it would be dropped under the CSV map alone) and AFG must key as the zip says."""
    body = b"code,iso,country,EPI.old\n999,AFG,Afghanistan,20.0\n" + PAD
    monkeypatch.setattr(Y.requests, "get", _site(_routes(**{ARCH: _archive(monkeypatch, body)})))
    res = Y.update(None, None)
    assert res.status == "ok", (res.status, res.error)
    keys = _keys(store)
    assert {"EPI:EPI.new:4", "EPI:AGR.new:4", "EPI:EPI.new:585"} <= keys, sorted(keys)
    assert not any(k.startswith("EPI:EPI.new:999") or k.endswith(":AFG") for k in keys), sorted(keys)


@pytest.mark.parametrize("answer", [_Resp(404, HTML, "text/html"), _Resp(200, HTML, "text/html; charset=UTF-8")],
                         ids=["404", "200-html"])
def test_the_pinned_archive_answering_404_or_html_is_a_break_not_retired(store, monkeypatch, answer):
    """Review R1300: the retired-floor branch ("its data is already held") is for Yale's dead URL. A DOI-backed,
    md5-pinned file answering 404 or an HTML page would otherwise read ok and, on an empty store, serve 2026 only."""
    monkeypatch.setattr(Y.requests, "get", _site(_routes(**{ARCH: answer})))
    with pytest.raises(DefinitiveError, match="2024"):
        Y.update(None, None)


def test_a_country_only_the_archive_knows_is_added_to_the_zip_map(store, monkeypatch):
    """The supplement: the zip knows AFG and PLW; the archive also knows ALB (8). The workbook's ALB row is keyed 8."""
    import io
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["iso", "country", "EPI.new"])
    ws.append(["AFG", "Afghanistan", 30.5])
    ws.append(["ALB", "Albania", 44.0])
    buf = io.BytesIO()
    wb.save(buf)
    from tests.test_yale_epi_site_move import XLSX
    body = b"code,iso,country,EPI.old\n8,ALB,Albania,40.0\n" + PAD
    monkeypatch.setattr(Y.requests, "get", _site(_routes(**{ARCH: _archive(monkeypatch, body),
                                                            XLSX: _Resp(200, buf.getvalue(), "application/xlsx")})))
    res = Y.update(None, None)
    assert res.status == "ok", (res.status, res.error)
    assert {"EPI:EPI.new:4", "EPI:EPI.new:8", "EPI:EPI.old:8"} <= _keys(store), sorted(_keys(store))


def test_a_listed_zip_that_fails_does_not_fall_back_to_the_archive_map(store, monkeypatch):
    body = b"code,iso,country,EPI.old\n4,AFG,Afghanistan,20.0\n" + PAD
    monkeypatch.setattr(Y.requests, "get", _site(_routes(**{ARCH: _archive(monkeypatch, body),
                                                            ZIP: _Resp(503, b"busy", "text/plain")})))
    res = Y.update(None, None)
    assert res.status == "partial" and "vocabulary zip was unavailable" in (res.error or ""), (res.status, res.error)
    assert "EPI:EPI.new:585" not in _keys(store) and "EPI:EPI.old:4" in _keys(store)   # workbook NOT parsed


def test_with_no_zip_listed_the_archive_map_keys_the_workbook(store, monkeypatch):
    body = b"code,iso,country,EPI.old\n4,AFG,Afghanistan,20.0\n585,PLW,Palau,1.0\n" + PAD
    routes = _routes(**{ARCH: _archive(monkeypatch, body)})
    routes["https://epi.yale.edu/2026/downloads"] = _Resp(
        200, b'<a href="/sites/default/files/2026-09/epi2026results2026-07-07.xlsx">r</a>', "text/html")
    monkeypatch.setattr(Y.requests, "get", _site(routes))
    res = Y.update(None, None)
    assert res.status == "ok", (res.status, res.error)
    assert {"EPI:EPI.new:4", "EPI:EPI.new:585"} <= _keys(store)
