"""The Retry-After class sweep (follow-up to AR-471/AR-473, ledger R1870-R1872).

#256 gave the fetchers one strict parser, `_common.parse_retry_after_s`, which never raises. Five
other call sites still parsed the header by hand. Each one is tested HERE through its real call
site, with the sleep recorded instead of slept:

  insee_melodi._request   `int(ra) if ra.isdigit()` raised on "²" outside the retry loop's except
                          (the R1870 crash, in a live fetcher). Cap 120 s, default 60 s, unchanged.
  ingest_oecd             `float(ra)` with no cap on the non-429 retry: "inf" raised OverflowError
                          from time.sleep, "1000000" slept 11.6 days. Cap now 300 s.
  ingest_defillama        `float(ra) if ra.isdigit()` with no cap. Cap now 300 s.
  ingest_gus_dbw          its own int()/date parser; the call site's 30 s floor and 900 s cap stay.
  ingest_wikidata         `int(ra) if ra.isdigit()`; the 180 s cap stays.

For every site the rule is the same: an ASCII delta-seconds header keeps the wait it had before
(the base-vs-head pins), a long or absurd one is capped, and a garbage one falls back to the
site's own backoff. Nothing in the header can raise.
"""
from __future__ import annotations

import datetime as dt
import email.utils
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import insee_melodi as IM  # noqa: E402
from jobs import ingest_defillama as DL  # noqa: E402
from jobs import ingest_gus_dbw as G  # noqa: E402
from jobs import ingest_oecd as OE  # noqa: E402
from jobs import ingest_wikidata as W  # noqa: E402

# ASCII delta-seconds headers and the wait each site gave BEFORE this sweep (read from the base
# code, c5840d0e8, and re-checked by running the base module against the same cases).
GARBAGE = ["²", "³", "١٢", "-5", "+5", "12.5", "1e3", "inf", "nan", "abc", "Mon, 99 Foo 2026",
           "0x10", "1_000", "", "   "]
LONG = ["1000000", "9" * 30, "1" * 5000]


class _Resp:
    def __init__(self, status, headers=None, payload=None):
        self.status_code = status
        self.headers = headers or {}
        self._payload = payload

    def json(self):
        return self._payload


def _http_date(seconds_from_now):
    when = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=seconds_from_now)
    return email.utils.format_datetime(when, usegmt=True)


@pytest.fixture
def slept(monkeypatch):
    waits = []
    monkeypatch.setattr("time.sleep", lambda s: waits.append(s))
    return waits


# --------------------------------------------------------------------------------- insee_melodi
class _Clock:
    def fits(self, _s):
        return True


class _Sess:
    def __init__(self, responses):
        self._r = list(responses)

    def get(self, *_a, **_k):
        return self._r.pop(0)


def _insee_wait(header, slept):
    headers = {} if header is None else {"Retry-After": header}
    sess = _Sess([_Resp(429, headers), _Resp(200, payload={"ok": 1})])
    assert IM._request(sess, "https://example.invalid/x", _Clock()) == ("ok", {"ok": 1})
    assert len(slept) == 1, slept
    return slept[0]


@pytest.mark.parametrize("header,want", [
    ("0", 0), ("1", 1), ("7", 7), ("30", 30), ("60", 60), ("119", 119), ("120", 120),
    ("121", 120), ("3600", 120), ("000060", 60), ("0000061", 61), ("00000000", 0),
    ("123456", 120), (None, 60),
])
def test_insee_valid_headers_keep_the_old_wait(header, want, slept):
    assert _insee_wait(header, slept) == want


@pytest.mark.parametrize("header", LONG)
def test_insee_long_delays_are_capped_and_never_raise(header, slept):
    assert _insee_wait(header, slept) == IM.RETRY_AFTER_CAP_S == 120


@pytest.mark.parametrize("header", GARBAGE)
def test_insee_garbage_falls_back_to_the_default_and_never_raises(header, slept):
    assert _insee_wait(header, slept) == IM.RETRY_AFTER_DEFAULT_S == 60


def test_insee_superscript_two_no_longer_ends_the_pass(slept):
    # the R1870 crash itself: base raised ValueError out of _request on this header
    assert _insee_wait("²", slept) == 60


def test_insee_http_date_is_honoured_within_the_cap(slept):
    assert 40 <= _insee_wait(_http_date(45), slept) <= 45
    slept.clear()
    assert _insee_wait(_http_date(3600), slept) == 120
    slept.clear()
    assert _insee_wait(_http_date(-3600), slept) == 0       # a past date: retry now


# --------------------------------------------------------------------------------- ingest_oecd
class _Stream(_Resp):
    content = b"err"

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def iter_content(self, _n):
        yield b"DATAFLOW,X,TIME_PERIOD,OBS_VALUE"


def _oecd_wait(header, slept, monkeypatch, tmp_path):
    monkeypatch.setattr(OE, "throttle", lambda: None)
    headers = {} if header is None else {"Retry-After": header}
    sess = _Sess([_Stream(503, headers), _Stream(200)])
    st, _nb, ok, *_ = OE.stream_full_retrying(sess, "https://example.invalid/x",
                                              str(tmp_path / "f.csv"), 5, retries=3)
    assert (st, ok) == (200, True)
    assert len(slept) == 1, slept
    return slept[0]


# attempt 0 of the ladder waits min(60, 4) + 0 = 4 s; a header can only lengthen that
@pytest.mark.parametrize("header,want", [
    (None, 4), ("0", 4), ("3", 4), ("4", 4), ("30", 30), ("000060", 60), ("299", 299),
    ("300", 300), ("301", 300), ("3600", 300),
])
def test_oecd_valid_headers_keep_the_old_wait_up_to_the_cap(header, want, slept, monkeypatch,
                                                            tmp_path):
    assert _oecd_wait(header, slept, monkeypatch, tmp_path) == want


@pytest.mark.parametrize("header", LONG)
def test_oecd_long_delays_are_capped(header, slept, monkeypatch, tmp_path):
    assert _oecd_wait(header, slept, monkeypatch, tmp_path) == OE.RETRY_AFTER_CAP_S == 300


@pytest.mark.parametrize("header", GARBAGE)
def test_oecd_garbage_keeps_the_ladder_wait_and_never_raises(header, slept, monkeypatch,
                                                             tmp_path):
    # "inf" was OverflowError from time.sleep; "1e9" was 31 years
    assert _oecd_wait(header, slept, monkeypatch, tmp_path) == 4


def test_oecd_inf_and_1e9_no_longer_reach_sleep(slept, monkeypatch, tmp_path):
    assert _oecd_wait("inf", slept, monkeypatch, tmp_path) == 4
    slept.clear()
    assert _oecd_wait("1e9", slept, monkeypatch, tmp_path) == 4


def test_oecd_http_date_is_honoured_within_the_cap(slept, monkeypatch, tmp_path):
    assert 40 <= _oecd_wait(_http_date(45), slept, monkeypatch, tmp_path) <= 45
    slept.clear()
    assert _oecd_wait(_http_date(-3600), slept, monkeypatch, tmp_path) == 4


def test_oecd_429_still_waits_its_fixed_cooldown(slept, monkeypatch, tmp_path):
    # the 429 branch never read the header (OECD sends Retry-After: 0); it must not start to
    monkeypatch.setattr(OE, "throttle", lambda: None)
    sess = _Sess([_Stream(429, {"Retry-After": "1000000"}), _Stream(200)])
    OE.stream_full_retrying(sess, "https://example.invalid/x", str(tmp_path / "f.csv"), 5, retries=3)
    assert slept == [35.0]


# --------------------------------------------------------------------------------- ingest_defillama
def _defillama_wait(header, slept, monkeypatch):
    headers = {} if header is None else {"Retry-After": header}
    sess = _Sess([_Resp(429, headers), _Resp(200, payload={"ok": 1})])
    monkeypatch.setattr(DL, "sess", lambda: sess)
    assert DL.get("https://example.invalid/x") == {"ok": 1}
    assert len(slept) == 1, slept
    return slept[0]


@pytest.mark.parametrize("header,want", [
    (None, 2.0), ("0", 0), ("7", 7), ("000060", 60), ("300", 300), ("301", 300), ("3600", 300),
])
def test_defillama_valid_headers_keep_the_old_wait_up_to_the_cap(header, want, slept, monkeypatch):
    assert _defillama_wait(header, slept, monkeypatch) == want


@pytest.mark.parametrize("header", LONG)
def test_defillama_long_delays_are_capped(header, slept, monkeypatch):
    assert _defillama_wait(header, slept, monkeypatch) == DL.RETRY_AFTER_CAP_S == 300


@pytest.mark.parametrize("header", GARBAGE)
def test_defillama_garbage_backs_off_and_never_raises(header, slept, monkeypatch):
    assert _defillama_wait(header, slept, monkeypatch) == 2.0


def test_defillama_http_date_is_honoured_within_the_cap(slept, monkeypatch):
    assert 40 <= _defillama_wait(_http_date(45), slept, monkeypatch) <= 45
    slept.clear()
    assert _defillama_wait(_http_date(-3600), slept, monkeypatch) == 0


# --------------------------------------------------------------------------------- ingest_gus_dbw
def _gus_wait(header, slept, monkeypatch):
    headers = {} if header is None else {"Retry-After": header}
    monkeypatch.setattr(G, "throttle", lambda: None)
    monkeypatch.setattr(G, "SESSION", _Sess([_Resp(429, headers), _Resp(200, payload={"ok": 1})]))
    assert G.api_get("/x") == {"ok": 1}
    assert len(slept) == 1, slept
    return slept[0]


@pytest.mark.parametrize("header,want", [
    (None, 30), ("0", 30), ("7", 30), ("30", 30), ("100", 100), ("000100", 100), ("900", 900),
    ("901", 900), ("86400", 900),
])
def test_gus_valid_headers_keep_the_old_wait(header, want, slept, monkeypatch):
    assert _gus_wait(header, slept, monkeypatch) == want


@pytest.mark.parametrize("header", LONG)
def test_gus_long_delays_take_the_cap_not_the_floor(header, slept, monkeypatch):
    # base read 5,000 digits as 0 (int() raised, the date parse failed) and waited the 30 s floor
    assert _gus_wait(header, slept, monkeypatch) == 900


@pytest.mark.parametrize("header", GARBAGE)
def test_gus_garbage_waits_the_floor_and_never_raises(header, slept, monkeypatch):
    assert _gus_wait(header, slept, monkeypatch) == 30


def test_gus_http_date_is_honoured(slept, monkeypatch):
    assert 595 <= _gus_wait(_http_date(600), slept, monkeypatch) <= 600
    slept.clear()
    assert _gus_wait(_http_date(-3600), slept, monkeypatch) == 30


@pytest.mark.parametrize("val,want", [(None, 0), ("", 0), ("45", 45), ("²", 0), ("-5", 0),
                                      ("1" * 5000, 999_999)])
def test_gus_parse_retry_after_keeps_its_contract(val, want):
    assert G._parse_retry_after(val) == want


# --------------------------------------------------------------------------------- ingest_wikidata
def _wikidata_wait(header, slept, monkeypatch):
    headers = {} if header is None else {"Retry-After": header}
    ok = _Resp(200, payload={"results": {"bindings": [{"a": 1}]}})
    ok.raise_for_status = lambda: None
    sess = _Sess([_Resp(429, headers), ok])
    monkeypatch.setattr(W.requests, "get", sess.get)
    assert W.run_sparql("SELECT 1", "t") == [{"a": 1}]
    assert len(slept) == 1, slept
    return slept[0]


@pytest.mark.parametrize("header,want", [
    (None, 10), ("0", 0), ("7", 7), ("000060", 60), ("180", 180), ("181", 180), ("3600", 180),
])
def test_wikidata_valid_headers_keep_the_old_wait(header, want, slept, monkeypatch):
    assert _wikidata_wait(header, slept, monkeypatch) == want


@pytest.mark.parametrize("header", LONG)
def test_wikidata_long_delays_are_capped(header, slept, monkeypatch):
    assert _wikidata_wait(header, slept, monkeypatch) == W.RETRY_AFTER_CAP_S == 180


@pytest.mark.parametrize("header", GARBAGE)
def test_wikidata_garbage_keeps_the_backoff_and_never_raises(header, slept, monkeypatch):
    assert _wikidata_wait(header, slept, monkeypatch) == 10


def test_wikidata_http_date_is_honoured_within_the_cap(slept, monkeypatch):
    assert 40 <= _wikidata_wait(_http_date(45), slept, monkeypatch) <= 45


def test_wikidata_past_http_date_waits_zero_not_negative(slept, monkeypatch):
    assert _wikidata_wait(_http_date(-3600), slept, monkeypatch) == 0


def test_gus_parse_retry_after_floors_a_past_date():
    assert G._parse_retry_after(_http_date(-3600)) == 0


# --------------------------------------------------------- the urllib3 adapters (review defect 1)
# defillama's sess() and oecd's make_session() mount a urllib3 Retry that handles 429/503 BEFORE
# the code above sees a response. Uncapped it slept up to 21,600 s per retry. These go through the
# REAL adapter config, not a fake session.
import http.server  # noqa: E402
import threading  # noqa: E402

import requests  # noqa: E402

from updater.strategies.fetchers import _common as C  # noqa: E402


class _UrllibResp:
    def __init__(self, header):
        self.status = 429
        self.headers = {"Retry-After": header}

    def getheader(self, k, default=None):
        return self.headers.get(k, default)


def _adapters():
    DL._local.__dict__.pop("s", None)
    return {"defillama": DL.sess().get_adapter("https://x").max_retries,
            "oecd": OE.make_session().get_adapter("https://x").max_retries}


@pytest.mark.parametrize("name", ["defillama", "oecd"])
@pytest.mark.parametrize("header,want", [("7", 7), ("300", 300), ("301", 300), ("1000000", 300)])
def test_the_session_adapter_caps_retry_after(name, header, want):
    retry = _adapters()[name]
    assert retry.respect_retry_after_header is True
    assert retry.get_retry_after(_UrllibResp(header)) == want


@pytest.mark.parametrize("name", ["defillama", "oecd"])
def test_the_session_adapter_keeps_its_retry_policy(name):
    retry = _adapters()[name]
    want = {"defillama": (5, {429, 500, 502, 503, 504}), "oecd": (4, {429, 502, 503, 504})}[name]
    assert (retry.total, set(retry.status_forcelist)) == want
    assert retry.backoff_factor == 1.5


def test_an_old_urllib3_without_retry_after_max_does_not_honour_the_header(monkeypatch):
    import urllib3.util.retry as UR

    class _Old(UR.Retry):
        def __init__(self, *a, retry_after_max=None, **kw):
            if retry_after_max is not None:
                raise TypeError("unexpected keyword argument 'retry_after_max'")
            super().__init__(*a, **kw)

    monkeypatch.setattr(UR, "Retry", _Old)
    r = C.capped_urllib3_retry(300, total=2, status_forcelist=(429,))
    assert r.respect_retry_after_header is False


class _H(http.server.BaseHTTPRequestHandler):
    hits = []

    def do_GET(self):  # noqa: N802
        _H.hits.append(self.path)
        if len(_H.hits) == 1:
            self.send_response(429)
            self.send_header("Retry-After", "1000000")
            self.send_header("Content-Length", "0")
            self.end_headers()
        else:
            body = b'{"ok": 1}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, *_a):
        pass


@pytest.fixture
def server():
    _H.hits = []
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _H)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


@pytest.mark.parametrize("name", ["defillama", "oecd"])
def test_end_to_end_a_huge_retry_after_through_the_real_adapter_waits_the_cap(name, server, slept):
    # the job's own adapter, mounted for http so a localhost server can answer
    retry = _adapters()[name]
    s = requests.Session()
    s.mount("http://", requests.adapters.HTTPAdapter(max_retries=retry))
    r = s.get(server + "/x", timeout=10)
    assert r.status_code == 200 and len(_H.hits) == 2
    assert slept == [300], slept
