"""kof_globalization fetches the NEWEST year-stamped edition, and stays loud (fix of 2026-11-25).

Before the fix update() walked a fixed list whose first entry was the 2025 file, so no run could fetch a newer
edition and every due run booked no_change. These tests pin the rules in the fetcher's docstring. Hermetic:
`requests` is faked (any unplanned URL fails the test, so no request leaves the machine), the clock is pinned
(a test that reads the real clock fails), and the store is a LOCAL temp directory.

Expected URLs are written out as literal strings, never built from the module's template."""
from __future__ import annotations

import datetime as dt
import io
import os
import sys

import pytest

openpyxl = pytest.importorskip("openpyxl")
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import requests  # noqa: E402
from requests.structures import CaseInsensitiveDict  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import kof_globalization as K  # noqa: E402

_REAL_UTC_NOW = K._utc_now

U = {
    2024: "https://ethz.ch/content/dam/ethz/special-interest/dual/kof-dam/documents/Globalization/2024/KOFGI_2024_public.xlsx",
    2025: "https://ethz.ch/content/dam/ethz/special-interest/dual/kof-dam/documents/Globalization/2025/KOFGI_2025_public.xlsx",
    2026: "https://ethz.ch/content/dam/ethz/special-interest/dual/kof-dam/documents/Globalization/2026/KOFGI_2026_public.xlsx",
    2027: "https://ethz.ch/content/dam/ethz/special-interest/dual/kof-dam/documents/Globalization/2027/KOFGI_2027_public.xlsx",
    2028: "https://ethz.ch/content/dam/ethz/special-interest/dual/kof-dam/documents/Globalization/2028/KOFGI_2028_public.xlsx",
    2029: "https://ethz.ch/content/dam/ethz/special-interest/dual/kof-dam/documents/Globalization/2029/KOFGI_2029_public.xlsx",
    2030: "https://ethz.ch/content/dam/ethz/special-interest/dual/kof-dam/documents/Globalization/2030/KOFGI_2030_public.xlsx",
}
UTC = dt.timezone.utc
NOV_2026 = dt.datetime(2026, 11, 25, 5, 0, tzinfo=UTC)
EVE_2026 = dt.datetime(2026, 12, 31, 23, 30, tzinfo=UTC)
NEW_2027 = dt.datetime(2027, 1, 1, 0, 30, tzinfo=UTC)
MID_2029 = dt.datetime(2029, 6, 1, 0, 0, tzinfo=UTC)

COUNTRIES = [("CHE", "Switzerland"), ("DEU", "Germany"), ("USA", "United States"),
             ("FRA", "France"), ("JPN", "Japan"), ("BRA", "Brazil")]
HEADER = ("code", "country", "year", "KOFGI", "KOFEcGI")
PLANT_KEY, PLANT_DATE, PLANT_VALUE = "KOF:KOFGI:CHE", dt.date(2024, 12, 31), 77.7
XLSX_CT = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


# ---------------------------------------------------------------- fixtures and fakes

@pytest.fixture(autouse=True)
def _no_real_clock(monkeypatch):
    def _forbidden(*a, **k):
        raise AssertionError("a test read the real clock; pin it with _pin()")
    monkeypatch.setattr(K, "_utc_now", _forbidden)


def _pin(monkeypatch, when):
    monkeypatch.setattr(K, "_utc_now", lambda *a, **k: when)


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(K.config, "source_dir", lambda s: str(tmp_path / s))
    return tmp_path / "kof_globalization" / "kof_globalization.parquet"


class _Resp:
    def __init__(self, status, body=b"", ctype="text/html;charset=utf-8", headers=None):
        self.status_code = status
        self.content = body
        self.headers = CaseInsensitiveDict({"Content-Type": ctype} if ctype else {})
        self.headers.update(headers or {})


NOT_FOUND = _Resp(404, b"<html><body>404 Not Found</body></html>")
SOFT_404 = _Resp(200, b"<!DOCTYPE html><html><body>Page not found" + b" " * 6000 + b"</body></html>")


def _ok(body):
    return _Resp(200, body, XLSX_CT, {"Last-Modified": "Fri, 18 Dec 2026 10:00:00 GMT"})


class Site:
    """Every literal URL in U answers 404 unless overridden; any other URL fails the test (no network)."""

    def __init__(self, **by_year):
        self.routes = {u: NOT_FOUND for u in U.values()}
        for y, a in by_year.items():
            self.routes[U[int(y.lstrip("y"))]] = a
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


def _serve(monkeypatch, site):
    monkeypatch.setattr(K.requests, "get", site.get)
    monkeypatch.setattr(K.requests, "head", site.head)
    return site


def _v(i, yr, col, bump):
    return round(10.0 + i * 7.0 + (yr - 1970) * 0.9 + col * 3.0 + bump, 3)


def _edition_rows(last_year, bump=0.0, plant=False):
    rows = []
    for i, (iso, name) in enumerate(COUNTRIES):
        for yr in range(1970, last_year + 1):
            a, b = _v(i, yr, 0, bump), _v(i, yr, 1, bump)
            if plant and iso == "CHE" and yr == PLANT_DATE.year:
                a = PLANT_VALUE
            rows.append((iso, name, yr, a, b))
    return rows


def _xlsx(rows, header=HEADER):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "data"
    ws.append(list(header))
    for r in rows:
        ws.append(list(r))
    buf = io.BytesIO()
    wb.save(buf)
    body = buf.getvalue()
    assert body.startswith(b"PK\x03\x04") and len(body) > 5000, len(body)
    return body


def _seed(path, rows):
    """Write a store directly (not through the code under test): the keys the published file dedups on."""
    keys, dates, vals = [], [], []
    for iso, _name, yr, a, b in rows:
        for col, v in (("KOFGI", a), ("KOFECGI", b)):
            keys.append(f"KOF:{col}:{iso}")
            dates.append(dt.date(yr, 12, 31))
            vals.append(float(v))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.table({"series_key": pa.array(keys, pa.string()),
                             "obs_date": pa.array(dates, pa.date32()),
                             "value": pa.array(vals, pa.float64())}), str(path))


def _cells(path):
    t = pq.read_table(str(path))
    return {(k, d): v for k, d, v in zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                                          t.column("value").to_pylist())}


ED_2025 = _edition_rows(2023)                         # the 2025 edition: newest year 2023
ED_2026 = _edition_rows(2024, bump=0.25, plant=True)  # the 2026 edition: newest year 2024, history revised


# ---------------------------------------------------------------- the year selector and the clock

@pytest.mark.parametrize("when, expected", [
    (EVE_2026, [U[2027], U[2026], U[2025], U[2024]]),
    (NEW_2027, [U[2028], U[2027], U[2026], U[2025], U[2024]]),
    (MID_2029, [U[2030], U[2029], U[2028], U[2027], U[2026], U[2025], U[2024]]),
    # the same instants written in other zones: the year is the UTC year
    (dt.datetime(2026, 12, 31, 18, 30, tzinfo=dt.timezone(dt.timedelta(hours=-6))),     # = 2027-01-01T00:30Z
     [U[2028], U[2027], U[2026], U[2025], U[2024]]),
    (dt.datetime(2027, 1, 1, 5, 30, tzinfo=dt.timezone(dt.timedelta(hours=6))),         # = 2026-12-31T23:30Z
     [U[2027], U[2026], U[2025], U[2024]]),
], ids=["2026-12-31T23:30Z", "2027-01-01T00:30Z", "2029-06-01", "00:30Z-written-as-UTC-6", "23:30Z-written-as-UTC+6"])
def test_the_fetch_walks_utc_run_year_plus_one_down_to_2024(store, monkeypatch, when, expected):
    _pin(monkeypatch, when)
    site = _serve(monkeypatch, Site())                 # every year 404
    with pytest.raises(DefinitiveError, match="no year-stamped edition answers"):
        K.update(None, None)
    assert site.gets == expected
    site.heads.clear()
    assert K.current_vintage(None) is None
    assert site.heads == expected                     # the probe walks the SAME years
    assert not store.exists()


def test_the_clock_is_asked_for_utc():
    seen = []

    class _Clock:
        @staticmethod
        def now(tz=None):
            seen.append(tz)
            return EVE_2026 if tz is not None else EVE_2026.replace(tzinfo=None)

    assert _REAL_UTC_NOW(clock=_Clock) == EVE_2026
    assert seen == [dt.timezone.utc]


def test_a_naive_clock_is_refused():
    with pytest.raises(ValueError, match="timezone-aware"):
        K.edition_years(dt.datetime(2026, 12, 31, 23, 30))


# ---------------------------------------------------------------- planted positive and negative control

def test_a_2026_edition_is_fetched_and_its_values_land(store, monkeypatch):
    _pin(monkeypatch, EVE_2026)
    _seed(store, ED_2025)
    site = _serve(monkeypatch, Site(y2026=_ok(_xlsx(ED_2026)), y2025=_ok(_xlsx(ED_2025))))
    res = K.update(None, "2023-12-31")
    assert res.status == "ok", (res.status, res.error)
    assert site.gets == [U[2027], U[2026]]            # 2025 is never asked for
    cells = _cells(store)
    assert cells[(PLANT_KEY, PLANT_DATE)] == PLANT_VALUE   # a value only the 2026 edition holds
    assert cells[("KOF:KOFGI:DEU", dt.date(2023, 12, 31))] == _v(1, 2023, 0, 0.25)  # the revision won
    assert "edition 2026 (KOFGI_2026_public.xlsx)" in res.error
    assert PLANT_KEY in res.changed_keys and len(res.changed_keys) == 12   # every key revised or extended


def test_control_only_2025_answers_so_2025_is_fetched(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, ED_2025)
    site = _serve(monkeypatch, Site(y2025=_ok(_xlsx(ED_2025))))
    res = K.update(None, "2023-12-31")
    assert site.gets == [U[2027], U[2026], U[2025]]   # the newer years WERE asked for
    assert res.status == "no_change", (res.status, res.error)
    assert "edition 2025 (KOFGI_2025_public.xlsx)" in res.error
    assert (PLANT_KEY, PLANT_DATE) not in _cells(store)
    assert res.changed_keys == {}


def test_an_empty_store_takes_the_newest_edition(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _serve(monkeypatch, Site(y2025=_ok(_xlsx(ED_2025))))
    res = K.update(None, None)
    assert res.status == "ok", (res.status, res.error)
    assert len(_cells(store)) == len(ED_2025) * 2


# ---------------------------------------------------------------- a transient stops the walk, nothing merged

@pytest.mark.parametrize("answer", [_Resp(503), _Resp(429), _Resp(500), requests.Timeout("read timed out"),
                                    requests.ConnectionError("reset")],
                         ids=["503", "429", "500", "timeout", "connection-error"])
def test_a_transient_newest_edition_never_lets_2025_overwrite_the_store(store, monkeypatch, answer):
    _pin(monkeypatch, EVE_2026)
    _seed(store, ED_2026)                              # the store already holds the 2026 edition
    before = store.read_bytes()
    site = _serve(monkeypatch, Site(y2026=answer, y2025=_ok(_xlsx(ED_2025))))
    res = K.update(None, "2024-12-31")
    assert res.status == "partial", (res.status, res.error)
    assert "2026 edition" in res.error and "walk stopped, nothing merged" in res.error
    assert U[2025] not in site.gets
    assert store.read_bytes() == before


def test_a_transient_on_a_not_yet_published_year_also_stops(store, monkeypatch):
    _pin(monkeypatch, EVE_2026)
    _seed(store, ED_2025)
    before = store.read_bytes()
    site = _serve(monkeypatch, Site(y2027=_Resp(503), y2026=_ok(_xlsx(ED_2026))))
    res = K.update(None, "2023-12-31")
    assert res.status == "partial", (res.status, res.error)
    assert site.gets == [U[2027]]
    assert store.read_bytes() == before


def test_a_transient_probe_returns_none_not_an_older_year(monkeypatch):
    _pin(monkeypatch, EVE_2026)
    site = _serve(monkeypatch, Site(y2026=_Resp(503), y2025=_ok(_xlsx(ED_2025))))
    assert K.current_vintage(None) is None
    assert site.heads == [U[2027], U[2026]]


# ---------------------------------------------------------------- soft-404, parse failures

def test_a_soft_404_for_one_year_is_not_an_edition(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, ED_2025)
    site = _serve(monkeypatch, Site(y2026=SOFT_404, y2025=_ok(_xlsx(ED_2025))))
    res = K.update(None, "2023-12-31")
    assert site.gets == [U[2027], U[2026], U[2025]]
    assert "edition 2025" in res.error
    assert K.current_vintage(None).startswith("2025:")


def _opening(exc_info) -> str:
    """What the digest shows of a stored error (send_digest: err=...[:72])."""
    return str(exc_info.value)[:72]


def _stuck_alarm_counts_it_failed(exc_info) -> bool:
    from updater import health
    return health._stuck_class("partial", str(exc_info.value)) == "fail"


def test_a_soft_404_for_every_year_is_structural(store, monkeypatch):
    _pin(monkeypatch, EVE_2026)
    _serve(monkeypatch, Site(**{f"y{y}": SOFT_404 for y in (2027, 2026, 2025, 2024)}))
    with pytest.raises(DefinitiveError, match="no year-stamped edition answers for 2027..2024.*not an xlsx") as e:
        K.update(None, None)
    assert _opening(e).startswith("kof_globalization: no year-stamped edition answers for 2027..2024"), _opening(e)
    assert "parsed 0 rows" not in str(e.value)
    assert _stuck_alarm_counts_it_failed(e)
    assert K.current_vintage(None) is None
    assert not store.exists()


def test_a_newest_edition_with_a_changed_layout_is_structural_not_2025(store, monkeypatch):
    _pin(monkeypatch, EVE_2026)
    _seed(store, ED_2025)
    before = store.read_bytes()
    relaid = _xlsx(ED_2026, header=("land", "name", "jahr", "KOFGI", "KOFEcGI"))   # no code/year header
    site = _serve(monkeypatch, Site(y2026=_ok(relaid), y2025=_ok(_xlsx(ED_2025))))
    with pytest.raises(DefinitiveError, match="2026 edition: the xlsx parsed 0 rows"):
        K.update(None, "2023-12-31")
    assert U[2025] not in site.gets
    assert store.read_bytes() == before


def test_an_unparseable_xlsx_body_is_structural_and_named(store, monkeypatch):
    _pin(monkeypatch, EVE_2026)
    broken = b"PK\x03\x04" + b"\x00" * 6000
    site = _serve(monkeypatch, Site(y2026=_ok(broken), y2025=_ok(_xlsx(ED_2025))))
    with pytest.raises(DefinitiveError, match="2026 edition: parse error BadZipFile"):
        K.update(None, None)
    assert U[2025] not in site.gets


def test_a_row_shorter_than_its_year_column_is_skipped_not_an_indexerror(monkeypatch):
    rows = [("code", "country", "kofgi", "year"), ("CHE", "Switzerland", 50.0, 2020), ("DEU",),
            ("DEU", "Germany", 51.0, 2020), ("USA", "United States", 52.0, 2020), ("FRA", "France", 53.0, 2020)]

    class _Ws:
        def iter_rows(self, values_only=True):
            return iter(rows)

    class _Wb:
        sheetnames = ["data"]

        def __getitem__(self, name):
            return _Ws()

    monkeypatch.setattr(openpyxl, "load_workbook", lambda *a, **k: _Wb())
    keys, dates, vals = K._parse_kof_xlsx(b"PK\x03\x04")
    assert keys == ["KOF:KOFGI:CHE", "KOF:KOFGI:DEU", "KOF:KOFGI:USA", "KOF:KOFGI:FRA"]


# ---------------------------------------------------------------- refusals: older than held, id fork

def test_an_edition_older_than_the_store_is_refused(store, monkeypatch):
    _pin(monkeypatch, dt.datetime(2027, 3, 1, tzinfo=UTC))
    _seed(store, ED_2026)                              # newest stored year 2024
    before = store.read_bytes()
    _serve(monkeypatch, Site(y2025=_ok(_xlsx(ED_2025))))   # the 2026 URL is gone; 2025 (to 2023) answers
    with pytest.raises(DefinitiveError, match=r"the store's 2024\)") as e:
        K.update(None, None)
    assert _opening(e).startswith("kof_globalization: 2025 edition is older than the store"), _opening(e)
    assert "parsed 0 rows" not in str(e.value)
    assert _stuck_alarm_counts_it_failed(e)
    assert store.read_bytes() == before


def test_an_edition_older_than_since_is_refused(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _serve(monkeypatch, Site(y2025=_ok(_xlsx(ED_2025))))
    with pytest.raises(DefinitiveError, match=r"since 2024-12-31\)") as e:
        K.update(None, "2024-12-31")
    assert _opening(e).startswith("kof_globalization: 2025 edition is older than since"), _opening(e)
    assert "parsed 0 rows" not in str(e.value)
    assert _stuck_alarm_counts_it_failed(e)
    assert not store.exists()


def test_an_id_fork_is_refused(store, monkeypatch):
    _pin(monkeypatch, EVE_2026)
    _seed(store, ED_2025)
    before = store.read_bytes()
    forked = _xlsx(ED_2026, header=("code", "country", "year", "KOFGI_new", "KOFEcGI_new"))
    _serve(monkeypatch, Site(y2026=_ok(forked)))
    with pytest.raises(DefinitiveError, match=r"0\.0% of the 12 stored keys current in 2023") as e:
        K.update(None, "2023-12-31")
    assert _opening(e).startswith("kof_globalization: 2026 edition is an id fork"), _opening(e)
    assert "parsed 0 rows" not in str(e.value)
    assert _stuck_alarm_counts_it_failed(e)
    assert store.read_bytes() == before


# The key floor's boundary and its denominator (review F4, F6): 25 countries x 2 columns = 50 current keys.
WIDE = [(f"C{i:02d}", f"Country {i:02d}") for i in range(1, 26)]


def _wide_rows(last_year, drop_second_col_for=0):
    """The 2025-style edition over WIDE; the KOFEcGI column is blank for the first `drop_second_col_for`
    countries, so those keys are absent from the parsed edition."""
    rows = []
    for i, (iso, name) in enumerate(WIDE):
        for yr in range(1970, last_year + 1):
            b = None if i < drop_second_col_for else _v(i, yr, 1, 0.0)
            rows.append((iso, name, yr, _v(i, yr, 0, 0.0), b))
    return rows


def _seed_wide(path, extra_orphans=0):
    _seed(path, _wide_rows(2023))
    if extra_orphans:                                  # keys an older edition carried, last observed in 2010
        t = pq.read_table(str(path))
        keys = [f"KOF:OLDIDX:C{i:02d}" for i in range(1, extra_orphans + 1) for _ in range(1970, 2011)]
        dates = [dt.date(y, 12, 31) for _ in range(extra_orphans) for y in range(1970, 2011)]
        old = pa.table({"series_key": pa.array(keys, pa.string()), "obs_date": pa.array(dates, pa.date32()),
                        "value": pa.array([1.0] * len(keys), pa.float64())})
        pq.write_table(pa.concat_tables([t, old]), str(path))


def test_a_94_percent_key_share_is_refused(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed_wide(store)
    before = store.read_bytes()
    _serve(monkeypatch, Site(y2025=_ok(_xlsx(_wide_rows(2023, drop_second_col_for=3)))))   # 47 of 50
    with pytest.raises(DefinitiveError, match=r"94\.0% of the 50 stored keys current in 2023") as e:
        K.update(None, "2023-12-31")
    assert _opening(e).startswith("kof_globalization: 2025 edition is an id fork"), _opening(e)
    assert store.read_bytes() == before


def test_a_96_percent_key_share_is_merged(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed_wide(store)
    _serve(monkeypatch, Site(y2025=_ok(_xlsx(_wide_rows(2023, drop_second_col_for=2)))))   # 48 of 50
    res = K.update(None, "2023-12-31")
    assert res.status == "no_change", (res.status, res.error)


def test_keys_an_older_edition_dropped_do_not_lower_the_share(store, monkeypatch):
    """30 orphan keys (last obs 2010) beside 50 current ones: over every stored key the share would be
    50/80 = 62.5% and the edition refused; over the current keys it is 100%."""
    _pin(monkeypatch, NOV_2026)
    _seed_wide(store, extra_orphans=30)
    _serve(monkeypatch, Site(y2025=_ok(_xlsx(_wide_rows(2023)))))
    res = K.update(None, "2023-12-31")
    assert res.status == "no_change", (res.status, res.error)
    assert sum(1 for k, _d in _cells(store) if k.startswith("KOF:OLDIDX:")) == 30 * 41   # orphans are kept


# ---------------------------------------------------------------- revisions and the edition gap

def test_a_revision_that_adds_no_row_reads_ok_not_no_change(store, monkeypatch):
    _pin(monkeypatch, NOV_2026)
    _seed(store, ED_2025)
    revised = _edition_rows(2023, bump=0.5)
    _serve(monkeypatch, Site(y2025=_ok(_xlsx(revised))))
    res = K.update(None, "2023-12-31")
    assert res.status == "ok", (res.status, res.error)
    assert "revised stored values without adding rows" in res.error
    assert _cells(store)[("KOF:KOFGI:CHE", dt.date(2000, 12, 31))] == _v(0, 2000, 0, 0.5)
    assert sorted(res.changed_keys) == sorted({f"KOF:{c}:{iso}" for c in ("KOFGI", "KOFECGI") for iso, _n in COUNTRIES})


def test_a_missing_newer_edition_in_the_next_year_is_partial(store, monkeypatch):
    _pin(monkeypatch, NEW_2027)
    _seed(store, ED_2025)
    _serve(monkeypatch, Site(y2025=_ok(_xlsx(ED_2025))))
    res = K.update(None, "2023-12-31")
    assert res.status == "partial", (res.status, res.error)
    assert res.error.startswith("newest edition gap:")
    assert "2026 edition is missing" in res.error


def test_a_gap_partial_with_no_change_derives_no_csv(store, monkeypatch):
    """Review F1: a gap partial is due on every pass. With no changed value it must hand the CSV phase an EMPTY
    merge-measured set, or the phase takes every cursor key and re-derives every kof series each pass."""
    from updater import derive, orchestrate
    _pin(monkeypatch, NEW_2027)
    _seed(store, ED_2025)
    _serve(monkeypatch, Site(y2025=_ok(_xlsx(ED_2025))))
    res = K.update(None, "2023-12-31")
    assert res.status == "partial" and res.changed_keys == {}, (res.status, res.changed_keys)
    assert len(res.series_cursors) == 12                  # the cursors are still all there ...
    assert orchestrate._should_derive_csvs(res.status, res.changed_keys) is True   # ... and a partial derives

    def _no_derive(*a, **k):
        raise AssertionError("the CSV phase derived a kof series on a pass that changed nothing")
    monkeypatch.setattr(derive, "derive_and_put", _no_derive)

    class _Unit:
        source_id, unit_id, key = "kof_globalization", "_all", "kof_globalization/_all"

    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(_Unit(), res, blob=None)
    assert (failed, note, deferred, reasons) == ([], None, [], {})


def test_the_same_edition_late_in_its_own_next_year_is_not_a_gap(store, monkeypatch):
    _pin(monkeypatch, EVE_2026)
    _seed(store, ED_2025)
    _serve(monkeypatch, Site(y2025=_ok(_xlsx(ED_2025))))
    res = K.update(None, "2023-12-31")
    assert res.status == "no_change", (res.status, res.error)


# ---------------------------------------------------------------- probe and fetch agree

@pytest.mark.parametrize("routes, year", [
    ({"y2026": "ED_2026", "y2025": "ED_2025"}, 2026),
    ({"y2025": "ED_2025"}, 2025),
    ({"y2026": "SOFT", "y2025": "ED_2025"}, 2025),
], ids=["2026-published", "only-2025", "2026-soft-404"])
def test_probe_and_fetch_pick_the_same_year(store, monkeypatch, routes, year):
    _pin(monkeypatch, EVE_2026)
    built = {"ED_2026": _ok(_xlsx(ED_2026)), "ED_2025": _ok(_xlsx(ED_2025)), "SOFT": SOFT_404}
    _serve(monkeypatch, Site(**{k: built[v] for k, v in routes.items()}))
    token = K.current_vintage(None)
    res = K.update(None, None)
    assert token.split(":", 1)[0] == str(year)
    assert f"edition {year} (KOFGI_{year}_public.xlsx)" in res.error


def test_no_stamp_probe_and_no_fetcher_token():
    """The probe token gates nothing until an owner-approved admission (R1154): no STAMP_PROBE here."""
    assert getattr(K, "STAMP_PROBE", False) is False


# ---------------------------------------------------------------- the cold-start ingester walks the same years

@pytest.fixture
def J(tmp_path, monkeypatch):
    from jobs import ingest_kof_globalization as mod
    monkeypatch.setattr(mod, "OUT", str(tmp_path / "jobs_out"))
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    return mod


def test_the_ingester_walks_the_same_literal_years(J, monkeypatch):
    _pin(monkeypatch, EVE_2026)
    assert J.kof_urls() == [U[2027], U[2026], U[2025], U[2024]]


def test_the_ingester_stops_on_an_edition_it_cannot_judge(J, monkeypatch):
    _pin(monkeypatch, EVE_2026)
    site = _serve(monkeypatch, Site(y2026=_Resp(503), y2025=_ok(_xlsx(ED_2025))))
    J.main()
    assert U[2025] not in site.gets
    assert not os.path.exists(os.path.join(J.OUT, "kof_globalization.parquet"))


def test_the_ingester_takes_2026_when_it_is_published(J, monkeypatch):
    _pin(monkeypatch, EVE_2026)
    _serve(monkeypatch, Site(y2026=_ok(_xlsx(ED_2026)), y2025=_ok(_xlsx(ED_2025))))
    J.main()
    t = pq.read_table(os.path.join(J.OUT, "kof_globalization.parquet"))
    assert dt.date(2024, 12, 31) in set(t.column("obs_date").to_pylist())
