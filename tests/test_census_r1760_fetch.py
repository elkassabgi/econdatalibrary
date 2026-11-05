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
    assert p["get"].split(",") == ["I_ENDUSE", "COMM_LVL", "SUMMARY_LVL2", "DISTRICT",
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


def test_the_plan_never_reaches_clean_months():
    assert F.plan("enduse", 2026) == ["2026-01", "2026-02"]
    assert len(F.plan("enduse", 2015)) == 12


def test_out_must_be_outside_the_checkout():
    with pytest.raises(SystemExit, match="outside the checkout"):
        F.main(["--calibrate", "--out", os.path.join(ROOT, "data", "x")])
