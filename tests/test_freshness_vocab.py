"""The public freshness vocabulary - "late", "paused", "retired" (owner decisions, Ahmed, 2026-10-20).

The rule is core/freshness_vocab.py (dev shim) and api/worker/src/freshness.ts (production), with the classes
the worker needs generated into api/worker/src/freshnessClasses.ts by tools/gen_freshness_classes.py. Pinned:

  1. the generated file is what the code gives (and the check can fail);
  2. no generated class names a gated source (booleans only - the set is never printed);
  3. the retired RULE selects exactly the pinned ids, so a change in a run mechanism shows up here (R262),
     and nothing it selects is in the worker's SUPPORTED_SOURCES;
  4. the declarations: `rotating: true` only on a fetcher that rotates; the validator refuses bad values;
  5. statcan's LANE_CURRENT_PATTERN against the REAL verdict() output of every branch - the committed
     fixture api/worker/test/statcan_verdicts.json is regenerated from verdict() and must not drift;
  6. the shared cases (api/worker/test/freshness_cases.json) in Python AND in TypeScript (through node);
  7. the dev shim's two handlers print the labels and never print a note.
"""
from __future__ import annotations

import copy
import datetime as dt
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import freshness_vocab as fv  # noqa: E402

WORKER = os.path.join(ROOT, "api", "worker")
CASES = os.path.join(WORKER, "test", "freshness_cases.json")
VERDICTS = os.path.join(WORKER, "test", "statcan_verdicts.json")
RUNNER = os.path.join(WORKER, "test", "_freshness_runner.ts")

# THE RETIRED SET, PINNED (R262: a run-mechanism definition decays; a change must show up as a diff here).
# Registry entries the rule selects: no live:true, no matrix, no sec-edgar-daily, no run_location: local, no
# lane, not paused, not discontinued. Only ksh has state rows / appears on the API (measured 2026-10-20).
RETIRED_REGISTERED = {"cftc", "edgar_13f", "gii", "insee_sirene", "ksh", "pxweb", "worldbank_extra"}
# Ids the API carries (D1 state rows) that have no registry entry and no runner (measured 2026-10-20).
RETIRED_UNREGISTERED_ON_API = {"imf_hpdd", "imf_fiscaldecentralization"}


@pytest.fixture(scope="module")
def cls():
    return fv.classes()


# ---- 1. the generated file ------------------------------------------------------------------------------
def test_the_generated_file_is_current():
    r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "gen_freshness_classes.py"), "--check"],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr + r.stdout


def test_the_check_can_fail(cls):
    """NEGATIVE CONTROL: a class change must change the rendered file (else --check could never fail)."""
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import gen_freshness_classes as g
    other = copy.deepcopy(cls)
    other["paused"]["zz_planted"] = "planted"
    assert g.render(other) != g.render(cls)
    other = copy.deepcopy(cls)
    other["run_by"] = [s for s in other["run_by"] if s != "istat"]
    assert g.render(other) != g.render(cls)


# ---- 2. the gate --------------------------------------------------------------------------------------------
def test_no_generated_class_names_a_gated_source(cls):
    from core import sync_state_d1
    gated = sync_state_d1._gated_ids()
    assert gated, "the gate must be readable for this check to mean anything"
    for k in ("run_by", "discontinued", "lane", "rotating", "paused"):
        assert not any(str(s).lower() in gated for s in cls[k]), f"class {k} holds a gated id (not named here)"
    text = open(os.path.join(WORKER, "src", "freshnessClasses.ts"), encoding="utf-8").read()
    named = set(re.findall(r'"([a-z0-9_]+)"', text))
    assert not (named & gated), "freshnessClasses.ts quotes a gated id (not named here)"


# ---- 3. the retired rule ----------------------------------------------------------------------------------
def _retired(sid, cls):
    return sid not in set(cls["discontinued"]) and sid not in cls["paused"] and sid not in set(cls["run_by"])


def test_the_retired_rule_selects_exactly_the_pinned_registry_entries(cls):
    import yaml
    reg = yaml.safe_load(open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8"))["sources"]
    got = {e["source_id"] for e in reg if _retired(e["source_id"], cls)}
    assert got == RETIRED_REGISTERED, (sorted(got - RETIRED_REGISTERED), sorted(RETIRED_REGISTERED - got))


def test_the_retired_rule_on_the_api_ids_and_its_controls(cls):
    for sid in RETIRED_UNREGISTERED_ON_API | {"ksh"}:
        assert _retired(sid, cls), sid
    # one id per run mechanism, a first-pass crawler (stranded), the lane, paused, discontinued
    for sid in ("ecb", "bundesbank", "sec_edgar", "istat", "statcan", "cbs_nl", "gus_dbw", "insee_melodi",
                "unctad_bopcaba"):
        assert not _retired(sid, cls), sid
    assert _retired("zz_not_a_source", cls), "negative control: an unknown id is retired by the rule"


def test_nothing_retired_is_in_the_workers_supported_sources(cls):
    """A source the API calls retired while the worker lists it as served would contradict itself."""
    from tools import audit_schedule_coverage as cov
    supported = cov.supported_sources()
    assert "ecb" in supported, "control: the parser reads util.ts"
    bad = sorted(s for s in RETIRED_REGISTERED | RETIRED_UNREGISTERED_ON_API if s in supported)
    assert not bad, bad


# ---- 4. the declarations ----------------------------------------------------------------------------------
def test_today_s_declarations(cls):
    assert cls["rotating"] == ["ecb"]
    assert cls["lane"] == ["statcan"]
    assert set(cls["paused"]) == {"insee_melodi"}
    assert "cso" not in cls["rotating"], "cso is late by the owner's decision; it must not be declared"


def test_a_rotating_source_has_a_rotating_fetcher(cls):
    for sid in cls["rotating"]:
        src = open(os.path.join(ROOT, "updater", "strategies", "fetchers", f"{sid}.py"), encoding="utf-8").read()
        assert "rotate_after(" in src and "load_rotation(" in src and "deferred_unit(" in src, sid


def test_the_registry_refuses_a_bad_rotating_value():
    from updater import registry
    base = {"source_id": "zz", "strategy": "extend_by_date", "cadence": "daily", "live": True}
    ok = registry.validate({"sources": [dict(base, rotating=True)]})
    assert not [p for p in ok if "rotating" in p], ok
    for bad in ("yes", 1, False, None):
        probs = registry.validate({"sources": [dict(base, rotating=bad)]})
        assert any("rotating must be true" in p for p in probs), (bad, probs)
    probs = registry.validate({"sources": [dict(base, live=False, rotating=True, paused="on hold")]})
    assert any("rotating and paused" in p for p in probs), probs


# ---- 5. statcan's lane grammar against the REAL verdicts ---------------------------------------------------
NOW = dt.datetime(2026, 10, 20, 12, 0, tzinfo=dt.timezone.utc)


def _z(hours_ago):
    return (NOW - dt.timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _verdict_cases():
    """(branch, current?, state, progress) for every outcome of statcan.verdict()."""
    st = {"cubes": {"1": {"max_obs": "2026-09-01"}}}
    tail = {"merge": 0, "serve": 0, "new_cubes": 2}
    fresh = {"beat_utc": _z(0.5), "state": "idle", "owed": dict(tail)}
    return [
        ("ok", True, st, fresh),
        ("amber quarantined", True, st, dict(fresh, owed=dict(tail, quarantined=["10100145", "18100212"]))),
        ("amber serve refused", True, st, dict(fresh, owed=dict(tail, serve_refused=["2410: split map"]))),
        ("amber new-cube debt", True, st, dict(fresh, owed=dict(tail, oldest_new_cube="2026-08-14T12:30"))),
        ("amber quarantined + refused", True, st,
         dict(fresh, owed=dict(tail, quarantined=["1"], serve_refused=["2: x"]))),
        ("amber with an owed release inside 14 d", True, st,
         dict(fresh, owed=dict(tail, merge=3, oldest_owed_release="2026-10-15", quarantined=["1"]))),
        ("draining (behind, progressing)", False, st,
         dict(fresh, last_progress_utc=_z(1), owed=dict(tail, merge=17, oldest_owed_release="2026-09-08"))),
        ("behind, not progressing", False, st,
         dict(fresh, last_progress_utc=_z(30), owed=dict(tail, merge=23, oldest_owed_release="2026-09-08"))),
        ("cubes failing", False, st, dict(fresh, owed=dict(tail, failing=["1: store fault"]))),
        ("dead (old beat)", False, st, dict(fresh, beat_utc=_z(10))),
        ("store unreachable", False, st, dict(fresh, state="store_unreachable")),
        ("no progress published", False, None, None),
    ]


def _verdict_notes():
    from updater.errors import DefinitiveError
    from updater.strategies.fetchers import statcan
    out = []
    for branch, current, state, progress in _verdict_cases():
        try:
            status, msg, _n = statcan.verdict(state, progress, NOW)
            kind = status
        except DefinitiveError as e:
            msg, kind = str(e), "DefinitiveError"
        # one message names the store path of THIS machine: not part of the grammar, not for a public file
        msg = msg.replace(statcan.LANE_PROGRESS, "<LANE_PROGRESS>")
        out.append({"branch": branch, "outcome": kind, "current": current, "note": msg})
    return out


def test_the_lane_grammar_matches_exactly_the_current_verdicts():
    from updater.strategies.fetchers import statcan
    rx = re.compile(statcan.LANE_CURRENT_PATTERN)
    notes = _verdict_notes()
    for v in notes:
        assert bool(rx.match(v["note"])) is v["current"], v
        # the verdict's own outcome agrees: current = ok or the amber partial, never a raise
        assert (v["outcome"] in ("ok", "partial") and "DRAINING" not in v["note"]) is v["current"], v
    # the shared tail alone never matches, with or without the "statcan: " opening
    for t in ("0 cube(s) owe a merge, 0 owe serving, 2 released cube(s) not held (catalogue debt)",
              "statcan: 2 released cube(s) not held (catalogue debt)", "statcan: ", ""):
        assert not rx.match(t), t
    # today's stored note (state.db copy, statcan/_all, 2026-10-20T12:27:54Z) is current
    assert rx.match("statcan: 4 cube(s) QUARANTINED (each failed 3x on one release): 10100145, 18100212, "
                    "18100275, 25100045; 0 cube(s) owe a merge, 0 owe serving, 2 released cube(s) not held "
                    "(catalogue debt)")


def test_the_committed_verdict_fixture_is_the_real_output():
    """node:test (test/freshness.test.ts) checks the TS regex against this file; it must be verdict()'s
    output, not hand-typed text (regenerate: python tests/test_freshness_vocab.py --write-fixture)."""
    want = {"_doc": "GENERATED by tests/test_freshness_vocab.py from statcan.verdict() - do not edit",
            "verdicts": _verdict_notes()}
    have = json.load(open(VERDICTS, encoding="utf-8"))
    assert have == want, "api/worker/test/statcan_verdicts.json is stale: run python tests/test_freshness_vocab.py --write-fixture"


def _write_verdict_fixture():                       # pragma: no cover - a maintenance helper
    want = {"_doc": "GENERATED by tests/test_freshness_vocab.py from statcan.verdict() - do not edit",
            "verdicts": _verdict_notes()}
    with open(VERDICTS, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(want, fh, indent=1, ensure_ascii=False)
        fh.write("\n")


# ---- 6. the shared cases, Python and TypeScript -------------------------------------------------------------
def _planted(cls, p):
    c = dict(cls)
    c.update(paused=p["paused"], run_by=p["run_by"], discontinued=p["discontinued"], rotating=p["rotating"],
             lane=p["lane"])
    return c


def _python_answers(cls):
    doc = json.load(open(CASES, encoding="utf-8"))
    c = _planted(cls, doc["planted"])
    now = dt.datetime.fromisoformat(doc["now"].replace("Z", "+00:00"))
    out = []
    for k in doc["cases"]:
        lab = fv.source_label(k["sid"], k["src"], k["units"], now, c)
        out.append({"source": lab, "units": [fv.unit_status(u["status"], lab) for u in k["units"]],
                    "next": fv.promises_next_update(lab)})
    return doc, out


def test_the_shared_cases_in_python(cls):
    doc, got = _python_answers(cls)
    assert len(doc["cases"]) >= 30
    for k, g in zip(doc["cases"], got):
        assert (g["source"], g["units"], g["next"]) == (k["expect_source"], k["expect_units"], k["expect_next"]), \
            k["name"]


def _node():
    node = shutil.which("node")
    if not node:
        return None
    v = subprocess.run([node, "--version"], capture_output=True, text=True).stdout.strip().lstrip("v")
    major, minor = (int(x) for x in v.split(".")[:2])
    return node if (major, minor) >= (22, 6) else None


@pytest.mark.skipif(_node() is None, reason="node >= 22.6 absent (CI installs node 22; the node:test step "
                                              "test/freshness.test.ts runs the same cases there)")
def test_typescript_gives_the_same_answers(cls):
    node = _node()
    r = subprocess.run([node, "--experimental-strip-types", "--no-warnings", RUNNER, CASES, VERDICTS],
                       capture_output=True, text=True, cwd=WORKER)
    assert r.returncode == 0, r.stderr
    ts = json.loads(r.stdout)
    _doc, py = _python_answers(cls)
    assert ts["cases"] == py
    assert ts["lane"] == [v["current"] for v in json.load(open(VERDICTS, encoding="utf-8"))["verdicts"]]


# ---- 7. the dev shim -----------------------------------------------------------------------------------------
def _shim_state(path, now):
    def ago(d):
        return (now - dt.timedelta(days=d)).isoformat()
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE source_state (source_id TEXT PRIMARY KEY, strategy TEXT, cadence TEXT, status TEXT, "
                "last_success_utc TEXT, last_attempt_utc TEXT, owner TEXT, enabled INTEGER DEFAULT 1, note TEXT)")
    con.execute("CREATE TABLE unit_state (source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT, "
                "last_success_utc TEXT, last_attempt_utc TEXT, status TEXT, last_obs_date TEXT, "
                "obs_count INTEGER DEFAULT 0, attempt_count INTEGER DEFAULT 0, last_error TEXT, "
                "PRIMARY KEY(source_id, unit_id))")
    src = [("istat", "monthly", "ok", ago(98)), ("ecb", "daily", "ok", ago(95)), ("bcrp", "daily", "ok", ago(0.5)),
           ("ksh", "annual", "ok", ago(118))]
    con.executemany("INSERT INTO source_state (source_id, cadence, status, last_success_utc) VALUES (?,?,?,?)", src)
    note = "164 sub-unit(s) attempted, none failed; 376 deferred by budget, not reached in this pass [A.parquet]"
    units = [("istat", "partial", ago(98), ago(0.1), "istat: 1/12 sub-unit(s) returned 200 but parsed 0 rows"),
             ("ecb", "partial", ago(95), ago(0.2), note), ("bcrp", "ok", ago(0.5), ago(0.5), None),
             ("ksh", "ok", ago(118), ago(118), None), ("insee_melodi", "transient_fail", None, ago(0.1), "x")]
    con.executemany("INSERT INTO unit_state (source_id, unit_id, status, last_success_utc, last_attempt_utc, "
                    "last_error) VALUES (?, '_all', ?, ?, ?, ?)", units)
    con.commit()
    con.close()


def _shim_catalog(path):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT)")
    con.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, url TEXT, reservable INTEGER, "
                "commercial_ok INTEGER, attribution_required INTEGER, no_modify INTEGER)")
    con.executemany("INSERT INTO source VALUES (?, ?, NULL, NULL)",
                    [(s, s) for s in ("istat", "ecb", "bcrp", "insee_melodi")])
    con.commit()
    con.close()


@pytest.fixture
def shim(tmp_path):
    sys.path.insert(0, os.path.join(ROOT, "api"))
    import devserver
    now = dt.datetime.now(dt.timezone.utc)
    st, cat = str(tmp_path / "state.db"), str(tmp_path / "catalog.db")
    _shim_state(st, now)
    _shim_catalog(cat)
    h = devserver.Handler.__new__(devserver.Handler)
    h.server = SimpleNamespace(cfg=SimpleNamespace(state=st, catalog=cat, data_root=str(tmp_path)))
    got = []
    h._json = lambda code, obj: got.append((code, obj))
    return h, got


def test_the_dev_shim_last_updates(shim):
    h, got = shim
    h.h_last_updates()
    code, obj = got[0]
    by = {d["source"]: d for d in obj["datasets"]}
    assert by["istat"]["status"] == "late" and by["istat"]["next_update_expected"] is not None
    assert by["ecb"]["status"] == "partial", "a declared rotator's clean deferral is not late"
    assert by["bcrp"]["status"] == "ok", "control"
    assert by["ksh"]["status"] == "retired" and by["ksh"]["next_update_expected"] is None
    assert by["insee_melodi"]["status"] == "paused" and by["insee_melodi"]["next_update_expected"] is None
    body = json.dumps(obj)
    assert "sub-unit(s)" not in body and "last_error" not in body and "last_attempt" not in body, \
        "a note or an attempt stamp reached the response"


def test_the_dev_shim_sources(shim):
    h, got = shim
    h.h_sources()
    code, obj = got[0]
    by = {s["source"]: s for s in obj["sources"]}
    assert by["istat"]["freshness"]["status"] == "late"
    assert by["ecb"]["freshness"]["status"] == "ok"
    assert by["bcrp"]["freshness"]["status"] == "ok"
    assert by["insee_melodi"]["freshness"]["status"] == "paused", by["insee_melodi"]
    assert "sub-unit(s)" not in json.dumps(obj)


if __name__ == "__main__":                          # python tests/test_freshness_vocab.py --write-fixture
    if sys.argv[1:] == ["--write-fixture"]:
        _write_verdict_fixture()
        print(f"wrote {VERDICTS}")
    else:
        raise SystemExit("usage: python tests/test_freshness_vocab.py --write-fixture")
