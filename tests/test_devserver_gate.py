"""The dev shim applies the Worker's redistribution gate on EVERY route that names a source or a series (2026-09-17).

api/devserver.py promises to answer what the Cloudflare Worker answers, but it had no gate: gated series and sources
were served locally while production refused them. The first version of this change gated only the two series routes;
review of #39 found /v1/bundle, /v1/catalog, /v1/sources and /v1/last-updates still serving them. The shim now reads
denylist.ts through core/gen_denylist (the repo's one reader) and mirrors each Worker route's rule.

A real shim is booted against fixture catalogue + state databases holding a gated source, a carve-out, the carve-out
source's other series and a hosted control. No assertion expression contains a gated id, so a failure cannot print one.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DENYLIST = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")
DEVSERVER = os.path.join(ROOT, "api", "devserver.py")
if not os.path.exists(DENYLIST):
    pytest.skip("no worker checkout", allow_module_level=True)
HOSTED = "zz_hosted_fixture"


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    spec = importlib.util.spec_from_file_location("devserver_under_test", DEVSERVER)
    shim = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(shim)
    gated = sorted(shim._GATE)[0]
    carve_src, inds = next((k, v) for k, v in sorted(shim._CARVEOUTS.items()) if k not in shim._GATE and v)
    ids = {"gated": f"{gated}:A", "carved": f"{carve_src}:{inds[0]}:AGO", "carve_other": f"{carve_src}:zz_not_carved:AGO",
           "hosted": f"{HOSTED}:A"}
    d = tmp_path_factory.mktemp("shim")
    cat, st = str(d / "catalog.db"), str(d / "state.db")
    c = sqlite3.connect(cat)
    c.executescript("CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, url TEXT, reservable INTEGER, "
                    "commercial_ok INTEGER, attribution_required INTEGER, no_modify INTEGER);"
                    "CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT, "
                    "attribution TEXT, terms_url TEXT);"
                    "CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT, "
                    "geography TEXT, license_id TEXT, start_date TEXT, end_date TEXT, last_updated TEXT, category TEXT, "
                    "metadata TEXT);")
    c.execute("INSERT INTO license VALUES ('lic','lic','',1,1,0,0)")
    for s in (gated, carve_src, HOSTED):
        c.execute("INSERT INTO source VALUES (?,?,?,?,?,?)", (s, "n", "", "lic", "", ""))
    for sid in ids.values():
        c.execute("INSERT INTO series (series_id, source_id, title, license_id) VALUES (?,?,?,?)",
                  (sid, sid.split(":")[0], "fixture title", "lic"))
    c.commit()
    c.close()
    s = sqlite3.connect(st)
    s.executescript("CREATE TABLE source_state (source_id TEXT PRIMARY KEY, cadence TEXT, status TEXT, last_success_utc TEXT);"
                    "CREATE TABLE unit_state (source_id TEXT, unit_id TEXT, status TEXT, last_success_utc TEXT, "
                    "upstream_vintage TEXT, last_obs_date TEXT, obs_count INTEGER, PRIMARY KEY (source_id, unit_id));")
    for src in (gated, HOSTED):
        s.execute("INSERT INTO source_state VALUES (?,?,?,?)", (src, "daily", "ok", "2026-09-01T00:00:00+00:00"))
        s.execute("INSERT INTO unit_state VALUES (?,?,?,?,?,?,?)", (src, "_all", "ok", "2026-09-01T00:00:00+00:00", None, None, 1))
    s.commit()
    s.close()
    srv = shim.make_server("127.0.0.1", 0, shim._Cfg(str(d), cat, st))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield shim, ids, gated, carve_src, f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def _get(base, path):
    try:
        with urllib.request.urlopen(base + path, timeout=30) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, None


def q(s):
    return urllib.parse.quote(s, safe="")


def test_the_gate_is_read_through_the_repo_reader(env):
    shim = env[0]
    from core import gen_denylist
    same = shim._GATE == frozenset(gen_denylist.committed_gate()) and \
        shim._CARVEOUTS == {k: tuple(v) for k, v in gen_denylist.committed_carveouts().items()}
    assert same and len(shim._GATE) > 0 and len(shim._CARVEOUTS) > 0, "shim gate differs from core/gen_denylist"


def test_series_routes_answer_451_with_series_id(env):
    shim, ids, gated, carve_src, base = env
    ok = True
    for key in ("gated", "carved"):
        for suffix in (".csv", ".metadata.json?lang=xx"):
            code, body = _get(base, f"/v1/series/{q(ids[key])}{suffix}")
            ok &= code == 451 and body and body.get("error") == "not_redistributable" and body.get("series_id") == ids[key]
    code, _ = _get(base, f"/v1/series/{q(ids['hosted'])}.metadata.json")
    ok &= code != 451
    assert ok, "a series route did not refuse a gated id with the Worker's 451 body, or refused the hosted control"


def test_sources_and_last_updates_unlist_a_gated_source(env):
    shim, ids, gated, carve_src, base = env
    _, srcs = _get(base, "/v1/sources")
    listed = {x["source"] for x in srcs["sources"]}
    _, lu = _get(base, "/v1/last-updates")
    lu_src = {x["source"] for x in lu["datasets"]}
    ok = gated not in listed and HOSTED in listed and carve_src in listed and srcs["total"] == len(srcs["sources"]) \
        and gated not in lu_src and HOSTED in lu_src
    assert ok, "sources/last-updates still list a gated source, or dropped a control"


def test_catalog_refuses_a_gated_source_and_hides_carveouts(env):
    shim, ids, gated, carve_src, base = env
    code, _ = _get(base, f"/v1/catalog?source={q(gated)}")
    _, page = _get(base, f"/v1/catalog?source={q(carve_src)}")
    got = {r["series_id"] for r in page["results"]}
    _, allp = _get(base, "/v1/catalog?limit=500")
    all_ids = {r["series_id"] for r in allp["results"]}
    ok = code == 451 and ids["carved"] not in got and ids["carve_other"] in got \
        and ids["gated"] not in all_ids and ids["carved"] not in all_ids and ids["hosted"] in all_ids
    assert ok, "catalog served a gated source or a carve-out, or hid a control"


def test_bundle_refuses_gated_ids_and_sources(env):
    shim, ids, gated, carve_src, base = env
    code, _ = _get(base, f"/v1/bundle?source={q(gated)}")
    _, m = _get(base, "/v1/bundle?ids=" + ",".join(q(ids[k]) for k in ("gated", "carved", "hosted")))
    unresolved = {u["id"]: u["reason"] for u in m["econdl:unresolved"]}
    advertised = {s for r in m["resources"] for s in r["econdl:series_ids"]}
    _, ms = _get(base, f"/v1/bundle?source={q(carve_src)}")
    enumerated = set(ms.get("econdl:series_requested") or []) if isinstance(ms, dict) else set()
    ok = code == 400 \
        and unresolved.get(ids["gated"], "").startswith("not_redistributable") \
        and unresolved.get(ids["carved"], "").startswith("not_redistributable") \
        and ids["gated"] not in advertised and ids["carved"] not in advertised \
        and ids["carved"] not in enumerated and ids["carve_other"] in enumerated
    assert ok, "bundle advertised or enumerated a gated series, or accepted a gated source"
