"""fsi_fundforpeace: the year comes from the file, discovery is one loud call, and every year is decided before the
merge (fix of 2026-11-25).

Hermetic: `requests` is faked (any unplanned URL fails the test), the clock is pinned (a real-clock read fails),
the store is a LOCAL temp directory, and the XLSX files are built here with openpyxl (no real FSI file was
downloaded). The excel-page fixture is the page's 18 data anchors VERBATIM (one raw GET, 2026-11-25T06:55:42Z);
the distractors added to it are synthetic and say so. The fixture header uses the 13 indicator column names the
catalogue copy of 2026-11-16 holds; their ORDER, and the Year and Rank columns, are not measured."""
from __future__ import annotations

import datetime as dt
import io
import os
import sqlite3
import sys

import openpyxl
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.structures import CaseInsensitiveDict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import fsi_fundforpeace as F  # noqa: E402

PAGE = "https://fragilestatesindex.org/excel/"
REAL = {   # the 18 links of the excel page, verbatim (2026-11-25T06:55:42Z)
    2023: "https://fragilestatesindex.org/wp-content/uploads/2023/06/FSI-2023-DOWNLOAD.xlsx",
    2022: "https://fragilestatesindex.org/wp-content/uploads/2022/07/fsi-2022-download.xlsx",
    2021: "https://fragilestatesindex.org/wp-content/uploads/2021/05/fsi-2021.xlsx",
    2020: "https://fragilestatesindex.org/wp-content/uploads/2020/05/fsi-2020.xlsx",
    2019: "https://fragilestatesindex.org/wp-content/uploads/2019/04/fsi-2019.xlsx",
    2018: "https://fragilestatesindex.org/wp-content/uploads/2018/04/fsi-2018.xlsx",
    **{y: f"https://fragilestatesindex.org/wp-content/uploads/data/fsi-{y}.xlsx" for y in range(2006, 2018)},
}
ANCHORS = {y: f'<a href="{u}">' for y, u in REAL.items()}   # the page writes each as exactly this anchor
U2024_REL = "/wp-content/uploads/2025/02/FSI-2024-DOWNLOAD.xlsx"                       # synthetic, relative
U2024 = "https://fragilestatesindex.org/wp-content/uploads/2025/02/FSI-2024-DOWNLOAD.xlsx"
U2024_FAR = "https://fragilestatesindex.org/wp-content/uploads/2026/02/FSI-2024-DOWNLOAD.xlsx"  # folder = year + 2
FOREIGN = "https://mirror.example.org/wp-content/uploads/2024/05/fsi-2024.xlsx"         # synthetic, other host
PROFILES = "https://fragilestatesindex.org/wp-content/uploads/2023/06/FSI-2023-Country-Profiles.xlsx"  # synthetic
DUP2023 = "https://fragilestatesindex.org/wp-content/uploads/2023/07/fsi-2023.xlsx"     # synthetic duplicate year

UTC = dt.timezone.utc
NOV_2026 = dt.datetime(2026, 11, 25, 6, 0, tzinfo=UTC)
JUL_2024 = dt.datetime(2024, 7, 15, 6, 0, tzinfo=UTC)

INDICATORS = ["Total", "S1: Demographic Pressures", "S2: Refugees and IDPs", "C3: Group Grievance",
              "E3: Human Flight and Brain Drain", "E2: Economic Inequality", "E1: Economy",
              "P1: State Legitimacy", "P2: Public Services", "P3: Human Rights", "C1: Security Apparatus",
              "C2: Factionalized Elites", "X1: External Intervention"]
COUNTRIES = [f"Country {i:02d}" for i in range(1, 13)]


# ---------------------------------------------------------------- fixtures and fakes

@pytest.fixture(autouse=True)
def _no_real_clock_and_a_fresh_run(monkeypatch):
    def _forbidden(*a, **k):
        raise AssertionError("a test read the real clock; pin it with _pin()")
    monkeypatch.setattr(F, "_utc_now", _forbidden)
    F._SELECTION.clear()
    yield
    F._SELECTION.clear()


def _pin(monkeypatch, when):
    monkeypatch.setattr(F, "_utc_now", lambda *a, **k: when)


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(F.config, "source_dir", lambda s: str(tmp_path / s))
    return tmp_path / "fsi_fundforpeace" / "fsi_fundforpeace.parquet"


class _Resp:
    def __init__(self, status, body=b"", ctype="text/html; charset=UTF-8"):
        self.status_code = status
        self.content = body
        self.headers = CaseInsensitiveDict({"Content-Type": ctype})

    @property
    def text(self):
        return self.content.decode("utf-8", errors="replace")


class Site:
    def __init__(self, routes):
        self.routes = dict(routes)
        self.gets = []

    def get(self, url, headers=None, timeout=None, allow_redirects=True):
        self.gets.append(url)
        if url not in self.routes:
            raise AssertionError(f"unplanned request: {url}")
        a = self.routes[url]
        if isinstance(a, BaseException):
            raise a
        return a


def _serve(monkeypatch, routes):
    site = Site(routes)
    monkeypatch.setattr(F.requests, "get", site.get)
    return site


def _page(*anchors):
    body = ("<!DOCTYPE html><html><head><title>Excel</title></head><body><ul>"
            + "".join(f"<li>{a}download</a></li>" for a in anchors) + "</ul></body></html>")
    return _Resp(200, body.encode())


def _v(i, j, year, bump=0.0):
    return round(1.0 + (i * 0.37 + j * 0.53 + (year - 2006) * 0.11) % 9 + bump, 2)


_CACHE = {}


def _xlsx(year, countries=tuple(COUNTRIES), year_cell="int", no_year=False, cols=tuple(INDICATORS), bump=0.0,
          plant=None):
    """An FSI-shaped workbook: Country, Year, Rank, indicators. year_cell: 'int', 'datetime', 'str', 'mixed'."""
    ck = (year, countries, year_cell, no_year, cols, bump, plant)
    if ck in _CACHE:
        return _CACHE[ck]
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Country"] + ([] if no_year else ["Year"]) + ["Rank"] + list(cols))
    for i, c in enumerate(countries):
        yc = {"int": year, "datetime": dt.datetime(year, 1, 1), "str": str(year),
              "mixed": year if i else year - 1, "bad-one": year if i else "n/a"}[year_cell]
        vals = [_v(i, j, year, bump) for j in range(len(cols))]
        if plant and plant[0] == c:
            vals[cols.index(plant[1])] = plant[2]
        ws.append([c] + ([] if no_year else [yc]) + [f"{i + 1}st"] + vals)
    bio = io.BytesIO()
    wb.save(bio)
    body = bio.getvalue()
    assert len(body) >= 5000, len(body)
    _CACHE[ck] = body
    return body


def _cells(year, countries=tuple(COUNTRIES), cols=tuple(INDICATORS), bump=0.0):
    return {(f"FSI_FP:{c}:{ctry}", dt.date(year, 12, 31)): _v(i, j, year, bump)
            for i, ctry in enumerate(countries) for j, c in enumerate(cols)}


def _ok(body):
    return _Resp(200, body, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def _files(years, **over):
    routes = {REAL[y]: _ok(_xlsx(y)) for y in years}
    routes.update(over)
    return routes


def _seed(path, years, extra=None, countries=tuple(COUNTRIES)):
    cells = {}
    for y in years:
        cells.update(_cells(y, countries))
    cells.update(extra or {})
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.table({"series_key": pa.array([k for k, _d in cells], pa.string()),
                             "obs_date": pa.array([d for _k, d in cells], pa.date32()),
                             "value": pa.array(list(cells.values()), pa.float64())}), str(path))


def _stored(path):
    t = pq.read_table(str(path))
    return {(k, d): v for k, d, v in zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                                          t.column("value").to_pylist())}


def _stuck_fail(exc_info) -> bool:
    from updater import health
    return health._stuck_class("partial", str(exc_info.value)) == "fail"


SMALL = _page(ANCHORS[2023], ANCHORS[2022], ANCHORS[2021])
ALL = _page(*(ANCHORS[y] for y in sorted(REAL, reverse=True)))


# ---------------------------------------------------------------- names, folders, discovery

def test_name_and_folder_years():
    assert F.name_year(U2024) == 2024 and F.folder_year(U2024) == 2025
    assert F.name_year(REAL[2022]) == 2022 and F.folder_year(REAL[2022]) == 2022
    assert F.name_year(REAL[2010]) == 2010 and F.folder_year(REAL[2010]) is None
    assert F.name_year(PROFILES) is None


FIXTURE = os.path.join(ROOT, "tests", "fixtures", "fsi", "excel_anchors_20261125.html")
PADDED = '<a href=" https://fragilestatesindex.org/wp-content/uploads/2023/06/FSI-2023-DOWNLOAD.xlsx ">'


def test_the_committed_page_fixture_holds_36_anchors_one_padded_and_yields_the_18_years():
    html = open(FIXTURE, encoding="utf-8").read()
    xlsx = [h for h in F.hrefs(html) if h.lower().endswith(".xlsx")]
    assert len(xlsx) == 36 and html.count(PADDED) == 1
    assert xlsx.count(REAL[2023]) == 2                       # the padded href is read, padding stripped
    kept, left_out, dup = F.year_links(html)
    assert kept == REAL and left_out == [] and dup == {}


def test_a_padded_href_alone_is_found():
    kept, _l, _d = F.year_links(_page(PADDED).text)
    assert kept == {2023: REAL[2023]}


def test_the_committed_page_runs_end_to_end(store, monkeypatch):
    _pin(monkeypatch, JUL_2024)
    _seed(store, list(REAL))
    page = _Resp(200, open(FIXTURE, "rb").read())
    site = _serve(monkeypatch, {PAGE: page, **_files(list(REAL))})
    res = F.update(None, "2023-12-31")
    assert res.status == "no_change", (res.status, res.error)
    assert sorted(site.gets[1:]) == sorted(REAL.values())    # each year fetched once


def test_the_verbatim_page_keeps_all_18_years_and_drops_the_distractors():
    kept, left_out, dup = F.year_links(ALL.text)
    assert kept == REAL and left_out == [] and dup == {}
    kept, left_out, dup = F.year_links(_page(ANCHORS[2023], f'<a href="{U2024_REL}">', f'<a href="{FOREIGN}">',
                                             f'<a href="{PROFILES}">').text)
    assert kept == {2024: U2024, 2023: REAL[2023]}                 # the relative link resolves; host pinned
    assert sorted(left_out) == sorted([FOREIGN, PROFILES]) and dup == {}


# ---------------------------------------------------------------- the year label: the file's Year column

def test_an_fsi_2024_file_under_a_2025_folder_lands_on_2024(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, [2022, 2023])
    page = _page(f'<a href="{U2024_REL}">', ANCHORS[2023], ANCHORS[2022], f'<a href="{FOREIGN}">',
                 f'<a href="{PROFILES}">')
    site = _serve(monkeypatch, _files([2023, 2022], **{U2024: _ok(_xlsx(2024, plant=("Country 01", "Total", 77.7)))}))
    site.routes[PAGE] = page
    res = F.update(None, "2023-12-31")
    st = _stored(store)
    assert st[("FSI_FP:Total:Country 01", dt.date(2024, 12, 31))] == 77.7
    assert not any(d.year == 2025 for _k, d in st)
    assert FOREIGN not in site.gets and PROFILES not in site.gets
    assert res.status == "ok", (res.status, res.error)        # the gap is a note, never a demotion (R2209)
    assert res.error.startswith("newest data gap: the newest FSI year held is 2024; 2025 and 2026 are overdue"), \
        res.error


@pytest.mark.parametrize("url, body, why", [
    (U2024_FAR, _xlsx(2024), "the year is 2024 but the upload folder is 2026"),
    (U2024_FAR, _xlsx(2024, no_year=True), "the year is 2024 but the upload folder is 2026"),
    (U2024, _xlsx(2023), "the Year column says 2023 but the file name says 2024"),
    (U2024, _xlsx(2024, year_cell="mixed"), "the Year column holds [2023, 2024]"),
    (U2024, _xlsx(2024, year_cell="bad-one"), "the Year column holds [2024] and 1 unreadable cell(s)"),
], ids=["folder-year-plus-2", "no-year-column-folder-plus-2", "name-vs-column", "two-years",
        "r1-a-row-with-values-and-an-unreadable-year"])
def test_a_year_disagreement_refuses_and_merges_nothing(store, monkeypatch, url, body, why):
    _pin(monkeypatch, NOV_2026)
    _seed(store, [2022, 2023])
    before = store.read_bytes()
    _serve(monkeypatch, {PAGE: _page(f'<a href="{url}">', ANCHORS[2023], ANCHORS[2022]), url: _ok(body),
                         **_files([2023, 2022])})
    with pytest.raises(DefinitiveError) as e:
        F.update(None, "2023-12-31")
    assert why in str(e.value), str(e.value)
    assert str(e.value).startswith("fsi_fundforpeace: 2024: "), str(e.value)[:72]
    assert "(3/3 sub-unit(s) REFUSED by the fetcher" in str(e.value) and _stuck_fail(e)
    assert store.read_bytes() == before


def test_a_file_without_a_year_column_is_labelled_from_its_name_with_a_note(store, monkeypatch):
    _pin(monkeypatch, JUL_2024)
    _seed(store, [2022, 2023])
    _serve(monkeypatch, {PAGE: _page(f'<a href="{U2024_REL}">', ANCHORS[2023], ANCHORS[2022]),
                         U2024: _ok(_xlsx(2024, no_year=True)), **_files([2023, 2022])})
    res = F.update(None, "2023-12-31")
    assert res.status == "ok", (res.status, res.error)
    assert res.error.startswith("year(s) 2024: no Year column, labelled from the file name"), res.error
    assert ("FSI_FP:Total:Country 01", dt.date(2024, 12, 31)) in _stored(store)


def _with_footer(year):
    """Reviewer probe P3: an FSI file with a footer note row (no indicator value)."""
    wb = openpyxl.load_workbook(io.BytesIO(_xlsx(year)))
    ws = wb.active
    ws.append(["Source: The Fund for Peace, fragilestatesindex.org"])
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


def test_p3_a_footer_note_row_refuses_nothing(store, monkeypatch):
    _pin(monkeypatch, JUL_2024)
    _seed(store, [2022, 2023])
    _serve(monkeypatch, {PAGE: _page(ANCHORS[2023], ANCHORS[2022]),
                         REAL[2023]: _ok(_with_footer(2023)), REAL[2022]: _ok(_xlsx(2022))})
    res = F.update(None, "2023-12-31")
    assert res.status == "no_change", (res.status, res.error)


def test_change_from_previous_year_is_skipped_and_the_stored_series_stay(store, monkeypatch):
    _pin(monkeypatch, JUL_2024)
    chg = {(f"FSI_FP:Change from Previous Year:Country {i:02d}", dt.date(2020, 12, 31)): 0.5 for i in (1, 2)}
    _seed(store, [2020, 2021], extra=chg)
    cols2020 = tuple(INDICATORS) + ("Change from Previous Year",)
    _serve(monkeypatch, {PAGE: _page(ANCHORS[2021], ANCHORS[2020]),
                         REAL[2021]: _ok(_xlsx(2021)), REAL[2020]: _ok(_xlsx(2020, cols=cols2020))})
    res = F.update(None, "2021-12-31")
    assert res.status in ("ok", "no_change"), (res.status, res.error)
    st = _stored(store)
    assert {k for k in st if k[0].startswith("FSI_FP:Change from Previous Year:")} == set(chg)   # kept, none new
    assert all(st[k] == v for k, v in chg.items())


@pytest.mark.parametrize("cell", ["datetime", "str"])
def test_a_datetime_or_text_year_cell_is_read(store, monkeypatch, cell):
    _pin(monkeypatch, JUL_2024)
    _serve(monkeypatch, {PAGE: _page(ANCHORS[2023]), REAL[2023]: _ok(_xlsx(2023, year_cell=cell))})
    res = F.update(None, None)
    assert res.status == "ok", (res.status, res.error)
    assert ("FSI_FP:Total:Country 01", dt.date(2023, 12, 31)) in _stored(store)


# ---------------------------------------------------------------- every verdict before the merge

def test_one_structural_year_among_good_years_merges_nothing(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, [2021, 2022])                                   # 2023 would be new rows if merged
    before = store.read_bytes()
    html = _Resp(200, b"<!DOCTYPE html><html>" + b"x" * 6000 + b"</html>")
    _serve(monkeypatch, {PAGE: SMALL, **_files([2023, 2021], **{REAL[2022]: html})})
    with pytest.raises(DefinitiveError, match=r"2022: HTTP 200, 6,\d{3} bytes, not an XLSX") as e:
        F.update(None, "2022-12-31")
    assert str(e.value).startswith("fsi_fundforpeace: 2022: "), str(e.value)[:72]
    assert store.read_bytes() == before


def test_a_newest_file_with_other_indicator_columns_is_refused(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, [2022])
    before = store.read_bytes()
    cols = tuple(INDICATORS[:-1]) + ("X1: External Intervention (revised)",)
    _serve(monkeypatch, {PAGE: _page(ANCHORS[2023], ANCHORS[2022]),
                         REAL[2023]: _ok(_xlsx(2023, cols=cols)), REAL[2022]: _ok(_xlsx(2022))})
    with pytest.raises(DefinitiveError, match=r"2023: its indicator columns differ from 2022's"):
        F.update(None, "2022-12-31")
    assert store.read_bytes() == before


def test_p2_two_transient_newest_years_do_not_make_a_structural_refusal(store, monkeypatch):
    """Reviewer probe P2: the real files differ ('Change from Previous Year' in 2019/2020). With 2023 and 2022
    transient, the column rule must not compare 2021 with 2020: it checks only the newest LISTED year."""
    _pin(monkeypatch, NOV_2026)
    _seed(store, [2020, 2021, 2022, 2023])
    cols2020 = tuple(INDICATORS) + ("Change from Previous Year",)
    _serve(monkeypatch, {PAGE: _page(ANCHORS[2023], ANCHORS[2022], ANCHORS[2021], ANCHORS[2020]),
                         REAL[2023]: requests.Timeout("t"), REAL[2022]: requests.Timeout("t"),
                         REAL[2021]: _ok(_xlsx(2021)), REAL[2020]: _ok(_xlsx(2020, cols=cols2020))})
    res = F.update(None, "2023-12-31")
    assert res.status == "partial" and "2/3 sub-unit(s) transient-failed" in res.error, (res.status, res.error)


def test_p2b_older_years_with_other_columns_are_not_compared(store, monkeypatch):
    """P2 with a column the skip list does not remove (synthetic): the 2020 file carries one more indicator."""
    _pin(monkeypatch, NOV_2026)
    _seed(store, [2020, 2021, 2022, 2023])
    cols2020 = tuple(INDICATORS) + ("Q9: Synthetic Extra Indicator",)
    _serve(monkeypatch, {PAGE: _page(ANCHORS[2023], ANCHORS[2022], ANCHORS[2021], ANCHORS[2020]),
                         REAL[2023]: requests.Timeout("t"), REAL[2022]: requests.Timeout("t"),
                         REAL[2021]: _ok(_xlsx(2021)), REAL[2020]: _ok(_xlsx(2020, cols=cols2020))})
    res = F.update(None, "2023-12-31")
    assert res.status == "partial", (res.status, res.error)


def test_two_links_for_one_year_refuse_before_any_file(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    site = _serve(monkeypatch, {PAGE: _page(ANCHORS[2023], f'<a href="{DUP2023}">')})
    with pytest.raises(DefinitiveError, match="the excel page links two files for one year: 2023"):
        F.update(None, None)
    assert site.gets == [PAGE]


# ---------------------------------------------------------------- loud partials: fallback, 404, gap

@pytest.mark.parametrize("answer, why", [
    (_Resp(503), "the excel page answered HTTP 503"),
    (_Resp(403, b"<html>challenge</html>"), "the excel page answered HTTP 403"),
    (requests.Timeout("t"), "the excel page did not answer (Timeout)"),
    (requests.exceptions.ChunkedEncodingError("c"), "the excel page did not answer (ChunkedEncodingError)"),
    (requests.TooManyRedirects("r"), "the excel page did not answer (TooManyRedirects)"),
    (_page(f'<a href="{FOREIGN}">', f'<a href="{PROFILES}">'), "the excel page holds no fsi-<YYYY>"),
    (_page('<a href="http://[broken/x/fsi-2024.xlsx">', ANCHORS[2023]),
     "the excel page holds a link that will not parse (ValueError"),
], ids=["503", "403", "timeout", "chunked", "redirects", "only-distractors", "p4-malformed-href"])
def test_a_page_failure_uses_the_fallback_and_is_partial_reason_first(store, monkeypatch, answer, why):
    _pin(monkeypatch, JUL_2024)                                   # no gap at this clock: the fallback alone
    _seed(store, list(REAL))
    site = _serve(monkeypatch, {PAGE: answer, **_files(list(REAL))})
    res = F.update(None, "2023-12-31")
    assert res.status == "partial", (res.status, res.error)
    assert res.error.startswith("discovery failed: " + why), res.error
    assert sorted(site.gets[1:]) == sorted(REAL.values())
    assert res.changed_keys == {}


def test_a_404_on_a_listed_year_is_partial_naming_it_and_the_others_merge(store, monkeypatch):
    _pin(monkeypatch, JUL_2024)
    _seed(store, [2021, 2022])
    _serve(monkeypatch, {PAGE: SMALL, **_files([2023, 2021], **{REAL[2022]: _Resp(404, b"<html>404</html>")})})
    res = F.update(None, "2022-12-31")
    assert res.status == "partial", (res.status, res.error)
    assert res.error.startswith("year(s) 2022 listed but the file answers 404 (fsi-2022-download.xlsx)"), res.error
    assert ("FSI_FP:Total:Country 01", dt.date(2023, 12, 31)) in _stored(store)


@pytest.mark.parametrize("when, gap", [
    (dt.datetime(2024, 7, 31, 23, 0, tzinfo=UTC), None),
    (dt.datetime(2025, 7, 31, 23, 0, tzinfo=UTC), "2024 is overdue"),
    (dt.datetime(2025, 8, 1, 0, 30, tzinfo=UTC), "2024 and 2025 are overdue"),
    (NOV_2026, "2024, 2025 and 2026 are overdue"),
], ids=["2024-07-31", "2025-07-31", "2025-08-01", "2026-11-25"])
def test_the_gap_rule_is_a_note_never_a_demotion(store, monkeypatch, when, gap):
    _pin(monkeypatch, when)
    _seed(store, [2021, 2022, 2023])
    _serve(monkeypatch, {PAGE: SMALL, **_files([2023, 2022, 2021])})
    res = F.update(None, "2023-12-31")
    assert res.status == "no_change", (res.status, res.error)       # R2209: never partial for the gap
    if gap is None:
        assert "newest data gap" not in res.error, res.error
    else:
        assert res.error.startswith(f"newest data gap: the newest FSI year held is 2023; {gap}"), res.error
        assert "no per-year XLSX after 2023" in res.error and "fsi-2024-rankings" in res.error


def test_a_transient_year_is_partial_and_the_others_merge(store, monkeypatch):
    _pin(monkeypatch, JUL_2024)
    _seed(store, [2021, 2022])
    _serve(monkeypatch, {PAGE: SMALL, **_files([2023, 2021], **{REAL[2022]: requests.Timeout("t")})})
    res = F.update(None, "2022-12-31")
    assert res.status == "partial" and "1/2 sub-unit(s) transient-failed" in res.error, res.error
    assert ("FSI_FP:Total:Country 01", dt.date(2023, 12, 31)) in _stored(store)


# ---------------------------------------------------------------- revisions reach the CSV phase (R549)

@pytest.fixture
def catalog(tmp_path, monkeypatch):
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    for (k, _d) in _cells(2023):
        con.execute("INSERT OR IGNORE INTO series VALUES (?,?)", (f"fsi_fundforpeace:{k}", "fsi_fundforpeace"))
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    return p


def test_a_one_value_revision_reads_ok_and_the_csv_phase_derives_it(store, catalog, monkeypatch):
    from updater import derive, orchestrate
    _pin(monkeypatch, JUL_2024)
    _seed(store, [2022, 2023])
    revised = _xlsx(2023, plant=("Country 03", "E1: Economy", 9.87))
    _serve(monkeypatch, {PAGE: _page(ANCHORS[2023], ANCHORS[2022]), REAL[2023]: _ok(revised),
                         REAL[2022]: _ok(_xlsx(2022))})
    res = F.update(None, "2023-12-31")
    assert res.status == "ok", (res.status, res.error)
    assert "revised stored values without adding rows" in res.error
    assert list(res.changed_keys) == ["FSI_FP:E1: Economy:Country 03"]
    assert _stored(store)[("FSI_FP:E1: Economy:Country 03", dt.date(2023, 12, 31))] == 9.87

    asked = []

    def _fake_derive(ids, blob, **kw):
        asked.append(list(ids))
        return {"put": 0, "failed": [], "put_ids": [], "deferred_ids": []}
    monkeypatch.setattr(derive, "derive_and_put", _fake_derive)
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)

    class _Unit:
        source_id, unit_id, key = "fsi_fundforpeace", "_all", "fsi_fundforpeace/_all"

    orchestrate._derive_changed_csvs(_Unit(), res, blob=object())
    assert asked == [["fsi_fundforpeace:FSI_FP:E1: Economy:Country 03"]]


def test_an_unchanged_pass_is_no_change_with_an_empty_changed_set(store, monkeypatch):
    _pin(monkeypatch, JUL_2024)
    _seed(store, [2022, 2023])
    _serve(monkeypatch, {PAGE: _page(ANCHORS[2023], ANCHORS[2022]), **_files([2023, 2022])})
    res = F.update(None, "2023-12-31")
    assert res.status == "no_change" and res.changed_keys == {}, (res.status, res.error)


# ---------------------------------------------------------------- the key floor (current keys only)

FLOOR = tuple(f"Floor {i:02d}" for i in range(1, 51))        # 50 countries x 13 columns = 650 current keys
ORPHANS = {(f"FSI_FP:Change from Previous Year:Floor {i:02d}", dt.date(2020, 12, 31)): 1.0 for i in range(1, 41)}


def _floor(store, monkeypatch, keep):
    _pin(monkeypatch, JUL_2024)
    _seed(store, [2023], extra=ORPHANS, countries=FLOOR)       # + 40 keys that end 2020 (not current)
    body = _xlsx(2023, countries=FLOOR[:keep])
    _serve(monkeypatch, {PAGE: _page(ANCHORS[2023]), REAL[2023]: _ok(body)})


def test_a_94_percent_key_share_is_refused(store, monkeypatch):
    _floor(store, monkeypatch, keep=47)
    before = store.read_bytes()
    with pytest.raises(DefinitiveError, match=r"94\.0% of the 650 stored keys current in 2023") as e:
        F.update(None, "2023-12-31")
    assert str(e.value).startswith("fsi_fundforpeace: an id fork"), str(e.value)[:72]
    assert store.read_bytes() == before


def test_a_96_percent_key_share_is_merged(store, monkeypatch):
    _floor(store, monkeypatch, keep=48)                         # 624/650; over all 690 keys it would be 90.4%
    res = F.update(None, "2023-12-31")
    assert res.status == "no_change", (res.status, res.error)


def test_p1_a_new_year_that_renames_a_fifth_of_the_countries_is_refused(store, monkeypatch):
    """Reviewer probe P1 (R2210): 2022+2023 seeded with 50 countries; a NEW 2024 file renames 10. Across all
    years read the renamed countries are still found (in 2022 and 2023), so only the NEWEST file's keys can see
    the fork: 40 of 50 = 80%."""
    _pin(monkeypatch, NOV_2026)
    _seed(store, [2022, 2023], countries=FLOOR)
    before = store.read_bytes()
    renamed = tuple(FLOOR[:40]) + tuple(f"Renamed {i:02d}" for i in range(1, 11))
    _serve(monkeypatch, {PAGE: _page(f'<a href="{U2024_REL}">', ANCHORS[2023], ANCHORS[2022]),
                         U2024: _ok(_xlsx(2024, countries=renamed)),
                         REAL[2023]: _ok(_xlsx(2023, countries=FLOOR)), REAL[2022]: _ok(_xlsx(2022, countries=FLOOR))})
    with pytest.raises(DefinitiveError, match=r"an id fork: the 2024 file holds 80\.0% of the 650 stored keys"):
        F.update(None, "2023-12-31")
    assert store.read_bytes() == before


# ---------------------------------------------------------------- one discovery per run, one line

def test_probe_and_fetch_share_one_discovery_and_print_one_line(store, monkeypatch, capsys):
    _pin(monkeypatch, JUL_2024)
    site = _serve(monkeypatch, {PAGE: SMALL, **_files([2023, 2022, 2021])})
    tok = F.current_vintage(None)
    F.update(None, None)
    assert tok.startswith("urls:")
    assert site.gets.count(PAGE) == 1
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("[fsi_fundforpeace] discovery:")]
    assert lines == [f"[fsi_fundforpeace] discovery: page HTTP 200, {len(SMALL.content):,} bytes, 3 year link(s) "
                     "kept, 0 left out; newest year 2023; fallback: none"]


def test_no_stamp_probe():
    assert getattr(F, "STAMP_PROBE", False) is False


def test_the_clock_must_be_utc_aware():
    with pytest.raises(ValueError, match="timezone-aware"):
        F._as_utc(dt.datetime(2026, 11, 25))


# ---------------------------------------------------------------- the ingester: cold start only

def test_the_ingester_writes_nothing_on_an_existing_store(store, monkeypatch, capsys):
    from jobs import ingest_fsi_fundforpeace as J
    _pin(monkeypatch, JUL_2024)
    _seed(store, [2022, 2023])
    before = store.read_bytes()
    site = _serve(monkeypatch, {PAGE: _page(f'<a href="{U2024_REL}">', ANCHORS[2023]),
                                U2024: _ok(_xlsx(2024)), **_files([2023])})
    assert J.main() is None
    assert site.gets == [] and store.read_bytes() == before
    out = capsys.readouterr().out
    assert "gh workflow run updater-daily.yml -f source=fsi_fundforpeace -f force=true" in out
    assert "updater.run" not in out                                  # never a local run (R406)


def test_the_ingester_cold_start_runs_the_fetcher(store, monkeypatch):
    from jobs import ingest_fsi_fundforpeace as J
    _pin(monkeypatch, JUL_2024)
    _serve(monkeypatch, {PAGE: SMALL, **_files([2023, 2022, 2021])})
    res = J.main()
    assert res.status == "ok", (res.status, res.error)
    assert ("FSI_FP:Total:Country 01", dt.date(2021, 12, 31)) in _stored(store)
