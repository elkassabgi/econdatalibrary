"""tools/census_r1760_fetch.py: fetch only, the key never written, the request names each row's identity."""
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import census_r1760_fetch as F  # noqa: E402

KEY = "k3y-SECRET-123"


class Resp:
    def __init__(self, body, status=200, ctype="application/json"):
        self.content, self.status_code, self.headers = body, status, {"Content-Type": ctype}


class Sess:
    def __init__(self, resp):
        self.resp, self.calls = resp, []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, dict(params)))
        return self.resp


def test_the_request_carries_identity_and_only_the_bad_columns():
    url, p = F.request("enduse", "2015-06")
    assert url.endswith("/imports/enduse") and p["CTY_CODE"] == "-" and p["time"] == "2015-06"
    assert p["get"].split(",") == ["I_ENDUSE", "SUMMARY_LVL", "COMM_LVL", "SUMMARY_LVL2", "DISTRICT",
                                   "CON_CHA_YR", "DUT_VAL_YR", "DUT_VAL_MO", "AIR_VAL_YR"]
    assert "CON_CHA_YR" not in F.request("hitech", "2015-06")[1]["get"], "hitech's CON_CHA_YR was clean"


def test_json_is_recorded_and_the_key_is_never_written(tmp_path):
    body = json.dumps([["I_ENDUSE", "COMM_LVL"], ["0", "1"], ["1", "1"]]).encode()
    s = Sess(Resp(body))
    rec = F.fetch("enduse", "2015-06", str(tmp_path), KEY, s)
    assert s.calls[0][1]["key"] == KEY, "the key goes to the request"
    assert rec["kind"] == "json" and rec["rows"] == 2 and "key" not in rec["params"]
    for p in tmp_path.iterdir():
        assert KEY.encode() not in p.read_bytes(), f"{p.name} holds the key"


def test_a_missing_key_page_is_html_not_data(tmp_path):
    rec = F.fetch("enduse", "2015-06", str(tmp_path), KEY,
                  Sess(Resp(b"<html><title>Missing Key</title></html>", ctype="text/html")))
    assert rec["kind"] == "html" and "rows" not in rec


def test_a_body_echoing_the_key_stops_before_anything_is_written(tmp_path):
    out = tmp_path / "out"
    with pytest.raises(SystemExit, match="echoes the key; nothing written"):
        F.fetch("enduse", "2015-06", str(out), KEY, Sess(Resp(b'[["x"],["' + KEY.encode() + b'"]]')))
    assert not out.exists()


def test_the_plan_stays_inside_each_affected_range():
    assert F.plan("enduse", 2026) == ["2026-01", "2026-02"]
    assert len(F.plan("enduse", 2015)) == 12
    assert F.plan("naics", 2018) == [] and F.plan("sitc", 2023) == []


@pytest.fixture
def offline(monkeypatch):
    """No test may reach the real key or the network (review AR-440 r2): a guard that failed to fire used to
    fall through to _key() and requests."""
    def no_key():
        raise AssertionError("the real key loader was reached")

    class NoNet:
        def get(self, *a, **k):
            raise AssertionError("a request was about to be sent")
    monkeypatch.setattr(F, "_key", no_key)
    monkeypatch.setattr(F.requests, "Session", NoNet)


def test_out_must_be_outside_the_checkout(offline):
    with pytest.raises(SystemExit, match="outside the checkout"):
        F.main(["--calibrate", "--out", os.path.join(ROOT, "data", "x")])


@pytest.mark.skipif(os.path.normcase("A") != "a", reason="paths are case-sensitive here: an upper-cased path "
                                                         "is a different folder")
def test_out_must_be_outside_the_checkout_whatever_the_case(offline):
    with pytest.raises(SystemExit, match="outside the checkout"):
        F.main(["--calibrate", "--out", os.path.join(ROOT, "data", "x").upper()])


class Raising:
    def get(self, url, params=None, headers=None, timeout=None):
        raise F.requests.ConnectionError(f"Max retries exceeded with url: /x?get=a&time=2015-06&key={params['key']}")


def test_an_encoded_key_is_scrubbed_too(tmp_path):
    k = "a+b/c=d"

    class R:
        def get(self, url, params=None, headers=None, timeout=None):
            raise F.requests.ConnectionError("url: /x?key=" + F.quote(k, safe="") + " raw " + k)
    with pytest.raises(SystemExit) as e:
        F.fetch("enduse", "2015-06", str(tmp_path), k, R())
    assert k not in str(e.value) and F.quote(k, safe="") not in str(e.value)


def test_a_connection_error_never_shows_the_key(tmp_path, capsys):
    """AR-440: requests' ConnectionError carries the full query string, key included."""
    with pytest.raises(SystemExit) as e:
        F.fetch("enduse", "2015-06", str(tmp_path), KEY, Raising())
    assert KEY not in str(e.value) and "<key>" in str(e.value)
    assert e.value.__cause__ is None and e.value.__suppress_context__, "no chained traceback carries it"
    out = capsys.readouterr()
    assert KEY not in out.out + out.err


def test_main_stops_at_the_first_html_answer(tmp_path, monkeypatch):
    monkeypatch.setattr(F, "_key", lambda: KEY)
    monkeypatch.setattr(F.requests, "Session", lambda: Sess(Resp(b"<html>Missing Key</html>", ctype="text/html")))
    monkeypatch.setattr(F.time, "sleep", lambda s: None)
    assert F.main(["--calibrate", "--out", str(tmp_path / "o")]) == 2
    lines = (tmp_path / "o" / "manifest.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1, "one html answer, then nothing more is asked"
