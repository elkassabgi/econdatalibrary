"""undp_hdr discovers the NEWEST HDR edition on the downloads page, and stays loud (fix of 2026-11-25).

Before the fix update() took the first URL that answered a HEAD from a fixed list [HDR25, HDR23-24], so an HDR 2026
file under any name could never be fetched. These tests pin the rules in the fetcher's docstring. Hermetic:
`requests` is faked (any unplanned URL fails the test, so no request leaves the machine), the clock is pinned (a
test that reads the real clock fails), and the store is a LOCAL temp directory.

Expected URLs are literal strings, never built from the module under test. The downloads-page fixture is built from
the page's data anchors VERBATIM (one raw-bytes GET, 2026-11-25T06:13:16Z, no JavaScript run); the HDR26 anchor
added to it is synthetic and says so."""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.structures import CaseInsensitiveDict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import undp_hdr as H  # noqa: E402

_REAL_UTC_NOW = H._utc_now

PAGE = "https://hdr.undp.org/data-center/documentation-and-downloads"
U25 = "https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_complete_time_series.csv"
U26 = "https://hdr.undp.org/sites/default/files/2026_HDR/HDR26_Composite_indices_complete_time_series.csv"
U2324 = "https://hdr.undp.org/sites/default/files/2023-24_HDR/HDR23-24_Composite_indices_complete_time_series.csv"
MPI_OCT26 = "https://hdr.undp.org/sites/default/files/publications/additional-files/2026-10/2026_gMPI_Table1and2.xlsx"

# The page's data anchors, VERBATIM (2026-11-25T06:13:16Z): the composite CSV, its metadata, the annex tables,
# the 12 October 2026 MPI update, and two non-file links.
ANCHORS_2026_11_25 = (
    '<a href="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_complete_time_series.csv">',
    '<a href="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_metadata.xlsx">',
    '<a href="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Statistical_Annex_GDI_Table.xlsx">',
    '<a href="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Statistical_Annex_GII_Table.xlsx">',
    '<a href="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Statistical_Annex_HDI_Table.xlsx">',
    '<a href="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Statistical_Annex_HDI_Trends_Table.xlsx">',
    '<a href="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Statistical_Annex_IHDI_Table.xlsx">',
    '<a href="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Statistical_Annex_PHDI_Table.xlsx">',
    '<a href="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Statistical_Annex_Tables_1-7.xlsx">',
    '<a href="https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_calculating_indices.xlsx" target="_blank">',
    '<a href="https://hdr.undp.org/sites/default/files/publications/additional-files/2026-10/2026_gMPI_Table1and2.xlsx">',
    '<a href="/data-center/composite-indices" data-drupal-link-system-path="node/13">',
    '<a href="https://hdr.undp.org/mpi-statistical-programmes">',
)
SYNTHETIC_HDR26 = '<a href="/sites/default/files/2026_HDR/HDR26_Composite_indices_complete_time_series.csv">'  # relative
SYNTHETIC_2324 = f'<a href="{U2324}">'
FOREIGN = '<a href="https://mirror.example.org/sites/default/files/HDR27_Composite_indices_complete_time_series.csv">'
JS_RENDERED = (b'<!DOCTYPE html><html><head><title>Documentation and downloads</title></head><body>'
               b'<div id="app"></div><script src="/themes/hdro/app.js"></script></body></html>')

UTC = dt.timezone.utc
NOV_2026 = dt.datetime(2026, 11, 25, 6, 0, tzinfo=UTC)
JAN_2027 = dt.datetime(2027, 1, 15, 6, 0, tzinfo=UTC)
APR_2027 = dt.datetime(2027, 4, 30, 6, 0, tzinfo=UTC)
DUE_2027 = dt.datetime(2027, 6, 20, 6, 0, tzinfo=UTC)

COUNTRIES = [("CHE", "Switzerland"), ("DEU", "Germany"), ("USA", "United States"),
             ("FRA", "France"), ("JPN", "Japan"), ("BRA", "Brazil")]
COLS = ("hdi", "le")
PLANT_KEY, PLANT_DATE, PLANT_VALUE = "hdi:CHE", dt.date(2024, 12, 31), 0.977


# ---------------------------------------------------------------- fixtures and fakes

@pytest.fixture(autouse=True)
def _no_real_clock_and_a_fresh_run(monkeypatch):
    def _forbidden(*a, **k):
        raise AssertionError("a test read the real clock; pin it with _pin()")
    monkeypatch.setattr(H, "_utc_now", _forbidden)
    H._SELECTION.clear()
    yield
    H._SELECTION.clear()


def _pin(monkeypatch, when):
    monkeypatch.setattr(H, "_utc_now", lambda *a, **k: when)


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path / s))
    return tmp_path / "undp_hdr" / "undp_hdr.parquet"


class _Resp:
    def __init__(self, status, body=b"", ctype="text/html; charset=UTF-8", headers=None):
        self.status_code = status
        self.content = body
        self.headers = CaseInsensitiveDict({"Content-Type": ctype} if ctype else {})
        self.headers.update(headers or {})

    @property
    def text(self):
        return self.content.decode("utf-8", errors="replace")


NOT_FOUND = _Resp(404, b"<html><body>Page not found</body></html>")
HTML_200 = _Resp(200, b"<!DOCTYPE html><html><body>Page not found" + b" " * 3000 + b"</body></html>")


def _page(*anchors):
    body = ("<!DOCTYPE html><html><head><title>Documentation and downloads</title></head><body><ul>"
            + "".join(f"<li>{a}download</a></li>" for a in anchors) + "</ul></body></html>")
    return _Resp(200, body.encode())


TODAY = _page(*ANCHORS_2026_11_25)                           # links HDR25 only
WITH_26 = _page(*ANCHORS_2026_11_25, SYNTHETIC_HDR26, FOREIGN)


def _csv_resp(body):
    return _Resp(200, body, "text/csv", {"ETag": '"abc-1"', "Last-Modified": "Mon, 05 May 2025 22:39:12 GMT"})


class Site:
    def __init__(self, routes):
        self.routes = dict(routes)
        self.gets, self.heads = [], []

    def _answer(self, url):
        if url not in self.routes:
            raise AssertionError(f"unplanned request: {url}")
        a = self.routes[url]
        if isinstance(a, BaseException):
            raise a
        return a

    def get(self, url, headers=None, timeout=None, allow_redirects=True):
        self.gets.append(url)
        return self._answer(url)

    def head(self, url, headers=None, timeout=None, allow_redirects=True):
        self.heads.append(url)
        a = self._answer(url)
        h = CaseInsensitiveDict(a.headers)
        if a.content:
            h.setdefault("Content-Length", str(len(a.content)))
        return _Resp(a.status_code, b"", None, h)


def _serve(monkeypatch, routes):
    site = Site(routes)
    monkeypatch.setattr(H.requests, "get", site.get)
    monkeypatch.setattr(H.requests, "head", site.head)
    return site


def _v(i, yr, col, bump):
    return round(0.3 + i * 0.05 + (yr - 1990) * 0.004 + col * 10.0 + bump, 4)


def _edition(last_year, bump=0.0, plant=False, rows=COUNTRIES, drop_second=(), rank=True, extra_cols=()):
    """A wide composite CSV (the HDR layout): iso3,country,hdicode,region,hdi_rank_<Y>,hdi_1990..,le_1990..
    `drop_second` lists the iso3 codes whose le column is blank; returns (bytes, {(key, date): value})."""
    years = range(1990, last_year + 1)
    header = ["iso3", "country", "hdicode", "region"] + ([f"hdi_rank_{last_year}"] if rank else []) \
        + [f"{c}_{y}" for c in COLS for y in years] + list(extra_cols)
    lines, cells = [",".join(header)], {}
    for i, (iso, name) in enumerate(rows):
        row = [iso, name, "Very High", "ECA"] + ([str(i + 1)] if rank else [])
        for ci, c in enumerate(COLS):
            for y in years:
                if ci == 1 and iso in drop_second:
                    row.append("")
                    continue
                v = _v(i, y, ci, bump)
                if plant and iso == "CHE" and c == "hdi" and y == PLANT_DATE.year:
                    v = PLANT_VALUE
                row.append(repr(v))
                cells[(f"{c}:{iso}", dt.date(y, 12, 31))] = v
        row += ["1.5"] * len(extra_cols)
        lines.append(",".join(row))
    body = ("\n".join(lines) + "\n").encode()
    assert len(body) > 1000
    return body, cells


def _seed(path, cells, extra=None):
    """Write a store directly (not through the code under test). `extra` adds {(key, date): value} cells."""
    allc = dict(cells)
    allc.update(extra or {})
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.table({"series_key": pa.array([k for k, _d in allc], pa.string()),
                             "obs_date": pa.array([d for _k, d in allc], pa.date32()),
                             "value": pa.array(list(allc.values()), pa.float64())}), str(path))


def _cells(path):
    t = pq.read_table(str(path))
    return {(k, d): v for k, d, v in zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                                          t.column("value").to_pylist())}


def _opening(exc_info) -> str:
    return str(exc_info.value)[:72]


def _stuck_alarm_counts_it_failed(exc_info) -> bool:
    from updater import health
    return health._stuck_class("partial", str(exc_info.value)) == "fail"


B25, C25 = _edition(2023)                                   # HDR25: data to 2023 (today's store)
B26, C26 = _edition(2024, bump=0.01, plant=True)            # HDR26: data to 2024, history revised
B2324, _C2324 = _edition(2022)                              # HDR23-24: data to 2022


# ---------------------------------------------------------------- the selector

def test_edition_year_is_the_last_two_digit_group_of_an_exact_basename():
    assert H.edition_year(U25) == 2025
    assert H.edition_year(U26) == 2026
    assert H.edition_year(U2324) == 2024
    assert H.edition_year(MPI_OCT26) is None
    assert H.edition_year("https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_metadata.xlsx") is None
    assert H.edition_year("https://hdr.undp.org/sites/default/files/x/HDR2027_Composite_indices_complete_time_series.csv") is None


def test_the_verbatim_page_yields_only_hdr25_and_the_relative_hdr26_link_resolves():
    assert H.composite_links(TODAY.text) == [(2025, U25)]
    assert H.composite_links(WITH_26.text) == [(2026, U26), (2025, U25)]   # FOREIGN is another host: left out


# ---------------------------------------------------------------- planted positive and negative control

def test_a_discovered_hdr26_is_fetched_and_its_value_lands(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25)
    site = _serve(monkeypatch, {PAGE: WITH_26, U26: _csv_resp(B26), U25: _csv_resp(B25)})
    res = H.update(None, "2023-12-31")
    assert res.status == "ok", (res.status, res.error)
    assert site.gets == [PAGE, U26]                          # HDR25, MPI and the foreign link never asked for
    cells = _cells(store)
    assert cells[(PLANT_KEY, PLANT_DATE)] == PLANT_VALUE      # a value only HDR26 holds
    assert cells[("le:DEU", dt.date(2010, 12, 31))] == _v(1, 2010, 1, 0.01)   # the revision won
    assert not any(k.startswith("hdi_rank") for k, _d in cells)               # the rank column is a label
    assert PLANT_KEY in res.changed_keys and len(res.changed_keys) == 12
    assert "edition 2026 (HDR26_Composite_indices_complete_time_series.csv)" in res.error


def test_control_todays_page_links_only_hdr25_so_hdr25_is_fetched(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25)
    site = _serve(monkeypatch, {PAGE: TODAY, U25: _csv_resp(B25)})
    res = H.update(None, "2023-12-31")
    assert site.gets == [PAGE, U25]
    assert res.status == "no_change", (res.status, res.error)
    assert res.changed_keys == {}
    assert "edition 2025 (HDR25_Composite_indices_complete_time_series.csv)" in res.error


def test_gii_rank_and_gdi_group_stay_series_and_a_far_future_column_is_dropped(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    body, _c = _edition(2023, extra_cols=("gii_rank_2023", "gdi_group_2023", "hdi_2028"))
    _serve(monkeypatch, {PAGE: TODAY, U25: _csv_resp(body)})
    H.update(None, None)
    keys = {k for k, _d in _cells(store)}
    assert "gii_rank:CHE" in keys and "gdi_group:CHE" in keys
    assert not any(d.year == 2028 for _k, d in _cells(store))   # beyond run year + 1


# ---------------------------------------------------------------- a revision is ok and the CSV phase derives it

@pytest.fixture
def catalog(tmp_path, monkeypatch):
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    for (k, _d) in C25:
        con.execute("INSERT OR IGNORE INTO series VALUES (?,?)", (f"undp_hdr:{k}", "undp_hdr"))
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    return p


def test_a_one_value_revision_reads_ok_and_the_csv_phase_derives_that_series(store, catalog, monkeypatch):
    """Review item 2 (R549): same rows, one changed value. finalize books such a pass no_change unless the
    revision is counted, and _should_derive_csvs('no_change') derives nothing."""
    from updater import derive, orchestrate
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25)
    body = B25.replace(repr(_v(0, 2010, 0, 0.0)).encode(), b"0.5555", 1)    # hdi:CHE 2010 only
    assert body != B25
    _serve(monkeypatch, {PAGE: TODAY, U25: _csv_resp(body)})
    res = H.update(None, "2023-12-31")
    assert res.status == "ok", (res.status, res.error)
    assert "revised stored values without adding rows" in res.error
    assert list(res.changed_keys) == ["hdi:CHE"]
    assert orchestrate._should_derive_csvs(res.status, res.changed_keys) is True

    asked = []

    def _fake_derive(ids, blob, **kw):
        asked.append(list(ids))
        return {"put": 0, "failed": [], "put_ids": [], "deferred_ids": []}
    monkeypatch.setattr(derive, "derive_and_put", _fake_derive)
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)

    class _Unit:
        source_id, unit_id, key = "undp_hdr", "_all", "undp_hdr/_all"

    orchestrate._derive_changed_csvs(_Unit(), res, blob=object())
    assert asked == [["undp_hdr:hdi:CHE"]]


# ---------------------------------------------------------------- a transient on the CSV stops, nothing merged

@pytest.mark.parametrize("answer", [_Resp(503), _Resp(500), _Resp(429), requests.Timeout("t"),
                                    requests.ConnectionError("c")],
                         ids=["503", "500", "429", "timeout", "connection-error"])
def test_a_transient_newest_csv_stops_and_never_takes_hdr25(store, monkeypatch, answer):
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25)
    before = store.read_bytes()
    site = _serve(monkeypatch, {PAGE: WITH_26, U26: answer, U25: _csv_resp(B25)})
    res = H.update(None, "2023-12-31")
    assert res.status == "partial", (res.status, res.error)
    assert "2026 edition" in res.error and "nothing merged" in res.error
    assert site.gets == [PAGE, U26]                          # HDR25 never asked for
    assert store.read_bytes() == before


def test_a_transient_probe_head_returns_none(monkeypatch):
    _pin(monkeypatch, NOV_2026)
    site = _serve(monkeypatch, {PAGE: TODAY, U25: _Resp(503)})
    assert H.current_vintage(None) is None
    assert site.heads == [U25]


# ---------------------------------------------------------------- any page failure is a loud pinned fallback

@pytest.mark.parametrize("page, why", [
    (requests.Timeout("t"), "the downloads page did not answer (Timeout)"),
    (requests.ConnectionError("c"), "the downloads page did not answer (ConnectionError)"),
    (requests.exceptions.ChunkedEncodingError("x"), "the downloads page did not answer (ChunkedEncodingError)"),
    (requests.TooManyRedirects("x"), "the downloads page did not answer (TooManyRedirects)"),
    (_Resp(503), "the downloads page answered HTTP 503"),
    (_Resp(403, b"<html>challenge</html>"), "the downloads page answered HTTP 403"),
    (NOT_FOUND, "the downloads page answered HTTP 404"),
    (_Resp(200, JS_RENDERED), "the downloads page holds no HDR composite time-series CSV link"),
    (_page(ANCHORS_2026_11_25[1], ANCHORS_2026_11_25[10]), "the downloads page holds no HDR composite"),
    (_page(SYNTHETIC_2324), "the downloads page links only editions up to 2024, older than the pinned 2025"),
], ids=["timeout", "connection", "chunked", "redirects", "503", "403-challenge", "404", "js-rendered",
        "only-distractors", "only-older"])
def test_any_page_failure_takes_the_pinned_edition_and_is_partial_reason_first(store, monkeypatch, page, why):
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25)
    site = _serve(monkeypatch, {PAGE: page, U25: _csv_resp(B25), U2324: _csv_resp(B2324)})
    res = H.update(None, "2023-12-31")
    assert res.status == "partial", (res.status, res.error)
    assert res.error.startswith("discovery failed: " + why), res.error
    assert site.gets == [PAGE, U25]
    assert res.changed_keys == {}


def test_the_pinned_edition_is_still_refused_when_older_than_the_store(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, C26)                                         # the store holds HDR26 (hdi to 2024)
    before = store.read_bytes()
    _serve(monkeypatch, {PAGE: _Resp(503), U25: _csv_resp(B25)})
    with pytest.raises(DefinitiveError, match=r"the store's 2024\)") as e:
        H.update(None, "2024-12-31")
    assert _opening(e).startswith("undp_hdr: 2025 edition is older than the store"), _opening(e)
    assert store.read_bytes() == before


# ---------------------------------------------------------------- body check, gone, parse errors

@pytest.mark.parametrize("answer", [
    HTML_200,
    _Resp(200, b"<!DOCTYPE html><html><body>" + b"x" * 3000 + b"</body></html>", "text/csv"),
    _Resp(200, b"iso3,country,hdi_2024\nCHE,Switzerland,0.9\n", "text/csv"),
], ids=["html-content-type", "html-body-as-text-csv", "tiny-body"])
def test_a_body_that_is_not_a_csv_is_refused_and_hdr25_is_not_taken(store, monkeypatch, answer):
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25)
    before = store.read_bytes()
    site = _serve(monkeypatch, {PAGE: WITH_26, U26: answer, U25: _csv_resp(B25)})
    with pytest.raises(DefinitiveError, match="is not a CSV") as e:
        H.update(None, "2023-12-31")
    assert _opening(e).startswith("undp_hdr: the 2026 composite CSV (HDR26_"), _opening(e)
    assert _stuck_alarm_counts_it_failed(e)
    assert U25 not in site.gets
    assert store.read_bytes() == before


def test_a_linked_csv_that_answers_404_is_refused_not_replaced_by_hdr25(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25)
    site = _serve(monkeypatch, {PAGE: WITH_26, U26: NOT_FOUND, U25: _csv_resp(B25)})
    with pytest.raises(DefinitiveError, match="answers HTTP 404") as e:
        H.update(None, "2023-12-31")
    assert _opening(e).startswith("undp_hdr: the 2026 composite CSV (HDR26_"), _opening(e)
    assert U25 not in site.gets


def test_a_parse_error_is_refused_with_its_name(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    huge = b'iso3,country,hdicode,region,hdi_2024\nCHE,"' + b"x" * 200_000 + b'",x,y,0.9\n'
    _serve(monkeypatch, {PAGE: WITH_26, U26: _csv_resp(huge)})
    with pytest.raises(DefinitiveError, match="parse error Error: field larger than field limit") as e:
        H.update(None, None)
    assert _opening(e).startswith("undp_hdr: the 2026 composite CSV (HDR26_"), _opening(e)
    assert "parsed 0 rows" not in str(e.value)


def test_a_csv_with_no_indicator_columns_parses_0_rows_and_is_structural(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    body = ("iso3,country,hdicode,region,notes\n"
            + "".join(f"C{i:02d},Country {i:02d},Very High,ECA,none\n" for i in range(40))).encode()
    _serve(monkeypatch, {PAGE: WITH_26, U26: _csv_resp(body)})
    with pytest.raises(DefinitiveError, match="parsed 0 rows"):
        H.update(None, None)


# ---------------------------------------------------------------- refusals: older than held (hdi:<ISO3> years)

def test_an_hdr26_link_carrying_older_data_is_refused_on_an_hdr25_store(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25)
    before = store.read_bytes()
    _serve(monkeypatch, {PAGE: WITH_26, U26: _csv_resp(B2324)})          # data to 2022 under an HDR26 name
    with pytest.raises(DefinitiveError, match=r"its hdi year 2022 < the store's 2023\)") as e:
        H.update(None, "2023-12-31")
    assert _opening(e).startswith("undp_hdr: 2026 edition is older than the store"), _opening(e)
    assert _stuck_alarm_counts_it_failed(e)
    assert store.read_bytes() == before


def test_an_edition_older_than_since_is_refused(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _serve(monkeypatch, {PAGE: TODAY, U25: _csv_resp(B25)})
    with pytest.raises(DefinitiveError, match=r"since 2024-12-31\)") as e:
        H.update(None, "2024-12-31")
    assert _opening(e).startswith("undp_hdr: 2025 edition is older than since"), _opening(e)
    assert not store.exists()


def test_only_the_hdi_iso3_family_decides_the_store_year(store, monkeypatch):
    """An aggregate and a name key with a LATER year, and another family ending later, must not make the store
    read newer than its hdi:<ISO3> family; an EQUAL hdi year passes."""
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25, extra={("hdi:ZZA.VHHD", dt.date(2024, 12, 31)): 0.9,
                             ("hdi:Afghanistan", dt.date(2024, 12, 31)): 0.4,
                             ("abr:CHE", dt.date(2024, 12, 31)): 2.0})
    _serve(monkeypatch, {PAGE: TODAY, U25: _csv_resp(B25)})
    res = H.update(None, "2023-12-31")
    assert res.status == "no_change", (res.status, res.error)


# ---------------------------------------------------------------- the key floor: current keys of every family

# 20 ISO3 rows + 5 ZZ aggregate rows x (hdi, le) = 50 CURRENT keys (2023). The store also holds 20 annex NAME keys
# that end 2022 (as the served hdi:<Name> series do). A denominator over ALL keys, or over ISO3 keys only, fails.
ISO3_ROWS = [(f"{chr(65 + i // 26)}{chr(65 + i % 26)}X", f"Country {i:02d}") for i in range(20)]
ZZ_ROWS = [("ZZA.VHHD", "Very high"), ("ZZB.HHD", "High"), ("ZZC.MHD", "Medium"), ("ZZD.LHD", "Low"),
           ("ZZE.AS", "Arab States")]
NAME_KEYS = {(f"hdi:Name Country {i:02d}", dt.date(y, 12, 31)): 0.5 for i in range(20) for y in range(1990, 2023)}


def _floor_store(store):
    _b, cells = _edition(2023, rows=ISO3_ROWS + ZZ_ROWS)
    _seed(store, cells, extra=NAME_KEYS)


def test_a_94_percent_key_share_is_refused(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _floor_store(store)
    before = store.read_bytes()
    body, _c = _edition(2023, rows=ISO3_ROWS + ZZ_ROWS, drop_second=("ZZA.VHHD", "ZZB.HHD", "ZZC.MHD"))  # 47/50
    _serve(monkeypatch, {PAGE: TODAY, U25: _csv_resp(body)})
    with pytest.raises(DefinitiveError, match=r"94\.0% of the 50 current stored keys") as e:
        H.update(None, "2023-12-31")
    assert _opening(e).startswith("undp_hdr: 2025 edition is an id fork"), _opening(e)
    assert store.read_bytes() == before


def test_a_96_percent_key_share_is_merged(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _floor_store(store)
    body, _c = _edition(2023, rows=ISO3_ROWS + ZZ_ROWS, drop_second=("ZZA.VHHD", "ZZB.HHD"))             # 48/50
    _serve(monkeypatch, {PAGE: TODAY, U25: _csv_resp(body)})
    res = H.update(None, "2023-12-31")
    assert res.status == "no_change", (res.status, res.error)


# ---------------------------------------------------------------- the data gap (hdi year below run year - 3)

@pytest.mark.parametrize("when", [NOV_2026, JAN_2027, APR_2027, DUE_2027],
                         ids=["2026-11-25", "2027-01-15", "2027-04-30", "2027-06-20"])
@pytest.mark.parametrize("ends", [2023, 2024], ids=["store-ends-2023", "store-ends-2024"])
def test_the_gap_fires_only_below_run_year_minus_3(store, monkeypatch, when, ends):
    _pin(monkeypatch, when)
    if ends == 2023:
        _seed(store, C25)
        _serve(monkeypatch, {PAGE: TODAY, U25: _csv_resp(B25)})
    else:
        _seed(store, C26)
        _serve(monkeypatch, {PAGE: WITH_26, U26: _csv_resp(B26)})
    res = H.update(None, f"{ends}-12-31")
    gap = ends < when.year - 3
    assert (res.status == "partial") is gap, (when, ends, res.status, res.error)
    assert res.error.startswith("newest data gap:") is gap, res.error
    if not gap:
        assert res.status == "no_change", (res.status, res.error)


def test_a_gap_partial_with_no_change_derives_no_csv(store, monkeypatch):
    from updater import derive, orchestrate
    _pin(monkeypatch, DUE_2027)
    _seed(store, C25)
    _serve(monkeypatch, {PAGE: TODAY, U25: _csv_resp(B25)})
    res = H.update(None, "2023-12-31")
    assert res.status == "partial" and res.changed_keys == {}, (res.status, res.changed_keys)

    def _no_derive(*a, **k):
        raise AssertionError("the CSV phase derived an undp series on a pass that changed nothing")
    monkeypatch.setattr(derive, "derive_and_put", _no_derive)

    class _Unit:
        source_id, unit_id, key = "undp_hdr", "_all", "undp_hdr/_all"

    assert orchestrate._derive_changed_csvs(_Unit(), res, blob=None) == ([], None, [], {})


def test_the_clock_is_asked_for_utc():
    seen = []

    class _Clock:
        @staticmethod
        def now(tz=None):
            seen.append(tz)
            return NOV_2026 if tz is not None else NOV_2026.replace(tzinfo=None)

    assert _REAL_UTC_NOW(clock=_Clock) == NOV_2026
    assert seen == [dt.timezone.utc]


def test_a_naive_clock_is_refused():
    with pytest.raises(ValueError, match="timezone-aware"):
        H._run_year(dt.datetime(2026, 12, 31, 23, 30))


# ---------------------------------------------------------------- one discovery per run; probe and fetch agree

@pytest.mark.parametrize("page, url, year", [
    (WITH_26, U26, 2026),
    (TODAY, U25, 2025),
    (_Resp(200, JS_RENDERED), U25, 2025),
], ids=["hdr26-linked", "today-hdr25", "js-rendered-pinned"])
def test_probe_and_fetch_share_one_discovery(store, monkeypatch, page, url, year):
    _pin(monkeypatch, NOV_2026)
    site = _serve(monkeypatch, {PAGE: page, U26: _csv_resp(B26), U25: _csv_resp(B25)})
    token = H.current_vintage(None)
    H.update(None, None)
    assert token.split(":", 1)[0] == str(year)
    assert site.gets == [PAGE, url]                          # the page is read ONCE in the run
    assert site.heads == [url]


def test_no_stamp_probe_and_no_fetcher_token():
    assert getattr(H, "STAMP_PROBE", False) is False


# ---------------------------------------------------------------- N1(a): one discovery line per run

def _discovery_lines(capsys):
    return [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("[undp_hdr] discovery:")]


def test_one_discovery_line_per_run_names_status_bytes_links_choice(store, monkeypatch, capsys):
    _pin(monkeypatch, NOV_2026)
    _serve(monkeypatch, {PAGE: TODAY, U25: _csv_resp(B25)})
    H.current_vintage(None)
    H.update(None, None)
    assert _discovery_lines(capsys) == [
        f"[undp_hdr] discovery: page HTTP 200, {len(TODAY.content):,} bytes, 1 composite link(s); chose {U25}; "
        "fallback: none"]


@pytest.mark.parametrize("page, head", [
    (_Resp(403, b"<html>challenge</html>"), "page HTTP 403, 22 bytes, 0 composite link(s)"),
    (requests.Timeout("t"), "page no answer, 0 bytes, 0 composite link(s)"),
], ids=["403", "timeout"])
def test_the_discovery_line_carries_the_fallback_reason(store, monkeypatch, capsys, page, head):
    _pin(monkeypatch, NOV_2026)
    _serve(monkeypatch, {PAGE: page, U25: _csv_resp(B25)})
    H.update(None, None)
    lines = _discovery_lines(capsys)
    assert len(lines) == 1, lines
    assert lines[0].startswith(f"[undp_hdr] discovery: {head}; chose {U25}; fallback: discovery failed: "), lines[0]


# ---------------------------------------------------------------- the ingester goes through the fetcher

def test_the_ingester_runs_the_fetcher_and_takes_hdr26(store, monkeypatch):
    from jobs import ingest_undp_hdr as J
    _pin(monkeypatch, NOV_2026)
    site = _serve(monkeypatch, {PAGE: WITH_26, U26: _csv_resp(B26)})
    res = J.main()
    assert res.status == "ok", (res.status, res.error)
    assert site.gets == [PAGE, U26]
    assert _cells(store)[(PLANT_KEY, PLANT_DATE)] == PLANT_VALUE


def test_the_ingester_writes_nothing_on_an_existing_store_even_when_hdr26_is_linked(store, monkeypatch, capsys):
    """N2: main() writes ONLY on a cold start. An existing HDR25 store and a page that links HDR26: no request is
    made, the store bytes are unchanged, and the forced-run command is printed."""
    from jobs import ingest_undp_hdr as J
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25)
    before = store.read_bytes()
    site = _serve(monkeypatch, {PAGE: WITH_26, U26: _csv_resp(B26), U25: _csv_resp(B25)})
    assert J.main() is None
    assert store.read_bytes() == before
    assert site.gets == [] and site.heads == []
    assert "python -m updater.run --source undp_hdr --force" in capsys.readouterr().out


def test_the_ingester_writes_nothing_on_a_2023_store_when_the_server_has_only_23_24(store, monkeypatch):
    from jobs import ingest_undp_hdr as J
    _pin(monkeypatch, NOV_2026)
    _seed(store, C25)
    before = store.read_bytes()
    site = _serve(monkeypatch, {PAGE: _page(SYNTHETIC_2324), U2324: _csv_resp(B2324), U25: NOT_FOUND})
    assert J.main() is None
    assert site.gets == []
    assert store.read_bytes() == before


def test_a_cold_start_with_only_23_24_served_writes_nothing(store, monkeypatch):
    """No store, a page that links HDR23-24 only, HDR25 gone: the pinned HDR25 answers 404, so the run is refused
    and no store is written; HDR23-24 is never fetched."""
    from jobs import ingest_undp_hdr as J
    _pin(monkeypatch, NOV_2026)
    site = _serve(monkeypatch, {PAGE: _page(SYNTHETIC_2324), U2324: _csv_resp(B2324), U25: NOT_FOUND})
    with pytest.raises(DefinitiveError, match="answers HTTP 404"):
        J.main()
    assert U2324 not in site.gets
    assert not store.exists()
