"""The dev shim applies the Worker's redistribution gate (2026-09-17).

api/devserver.py promises to answer what the Cloudflare Worker answers (its docstring, CONTRACT.md), but it had no
gate: a gated series was served locally while production answered 451. The shim now reads the same denylist.ts
(the set via core/gen_denylist.committed_gate, the series carve-outs from SERIES_CARVEOUTS) and answers 451 first on
both series routes. No gated id is printed by these tests; fixtures are built from the parsed data in memory.
"""
from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
import threading
import urllib.error
import urllib.request

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DENYLIST = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")
DEVSERVER = os.path.join(ROOT, "api", "devserver.py")

if not os.path.exists(DENYLIST):
    pytest.skip("no worker checkout", allow_module_level=True)


@pytest.fixture(scope="module")
def shim():
    spec = importlib.util.spec_from_file_location("devserver_under_test", DEVSERVER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_carveouts_parse_matches_an_independent_count(shim):
    src = open(DENYLIST, encoding="utf-8").read()
    body = re.search(r"SERIES_CARVEOUTS[^=]*=\s*\{(.*?)\};", src, re.S).group(1)
    body = re.sub(r"//[^\n]*", "", body)
    keys = [ln for ln in body.splitlines() if re.match(r"""\s*[\w"']+\s*:\s*\[""", ln)]
    assert len(shim._CARVEOUTS) == len(keys) > 0
    assert sum(len(v) for v in shim._CARVEOUTS.values()) == len(re.findall(r"""["'][^"']+["']\s*[,\]]""", body)) - \
        sum(1 for k in keys if re.match(r"""\s*["']""", k))
    assert len(shim._GATE) > 0


def test_gate_positive_and_negative_cases(shim):
    g = sorted(shim._GATE)[0]
    assert shim._is_gated(f"{g}:ANY:KEY")
    assert shim._is_gated(g)                                    # no ':' -> the whole id is the source
    assert not shim._is_gated("zz_hosted_fixture:ANY")
    k, inds = next((k, v) for k, v in sorted(shim._CARVEOUTS.items()) if k not in shim._GATE and v)
    assert shim._is_gated(f"{k}:{inds[0]}:AGO")
    assert not shim._is_gated(f"{k}:zz_not_a_carveout:AGO")      # the carve-out source itself stays served
    assert not shim._is_gated(f"{k.upper()}:{inds[0]}:AGO") or k.upper() == k   # case-sensitive, like the Worker


def _handler_src(name):
    src = open(DEVSERVER, encoding="utf-8").read()
    i = src.index(f"def {name}(")
    j = src.find("\n    def ", i + 1)
    return src[i:j]


def test_gate_runs_first_in_both_handlers():
    csv = _handler_src("h_csv")
    assert csv.index("_is_gated(") < csv.index("_catalog.get_series(")
    meta = _handler_src("h_metadata")
    assert meta.index("_is_gated(") < meta.index("_req_lang(")


def test_served_451_bodies_through_a_real_request(shim, tmp_path):
    cfg = shim._Cfg(str(tmp_path), str(tmp_path / "no_catalog.db"), str(tmp_path / "no_state.db"))
    srv = shim.make_server("127.0.0.1", 0, cfg)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        base = f"http://127.0.0.1:{srv.server_address[1]}"
        g = sorted(shim._GATE)[0]
        sid = f"{g}:X"
        for suffix in (".csv", ".metadata.json?lang=xx"):
            try:
                urllib.request.urlopen(f"{base}/v1/series/{urllib.request.quote(sid, safe='')}{suffix}", timeout=30)
                raise AssertionError("gated id was not refused")
            except urllib.error.HTTPError as e:
                assert e.code == 451, (suffix, e.code)
                body = json.loads(e.read())
                assert body["error"] == "not_redistributable" and body["series_id"] == sid
        # control: a non-gated id is not answered 451 (it may 404 or 500 against the empty fixture catalogue)
        try:
            urllib.request.urlopen(f"{base}/v1/series/zz_hosted_fixture%3AX.csv", timeout=30)
        except urllib.error.HTTPError as e:
            assert e.code != 451
    finally:
        srv.shutdown()
