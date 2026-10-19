"""dst tables WITHHELD from users (owner decision, Ahmed, 2026-10-19): the 35 catalogued dst tables whose one stored
series is all zeros because the fetcher's select-all query was over MAX_CELLS and cut every variable to one code - a
cell Statistics Denmark holds no row for. They stay hidden until a re-grain makes them correct; an OPERATOR releases
them (tools/withheld_list.py release), never a fetcher.

What this pins:
  - a dst catalogue id has a SECOND colon (dst:DST:DNINTVAL); the worker's id rule, the Python validator and the
    site generator all admit it, with ONE pattern, and still refuse the shapes that could break the inlined SQL;
  - the committed list names dst, so the floor closes dst whole while the list is unreadable (src/withheld.ts);
  - every dst entry is `until: "operator"`, and withheld_list.py refuses `--until complete` for a source no fetcher
    un-hides;
  - the site page subtracts the withheld dst tables."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import gen_withheld_floor as G  # noqa: E402
import withheld_list as T  # noqa: E402

DST = "dst:DST:DNINTVAL"
COMMITTED = json.load(open(G.JSON_PATH, encoding="utf-8"))


def _ts_rule() -> str:
    src = open(os.path.join(ROOT, "api", "worker", "src", "withheld.ts"), encoding="utf-8").read()
    m = re.search(r"export const WITHHELD_ID_RE = /(.+)/;", src)
    assert m, "WITHHELD_ID_RE not found in withheld.ts"
    return m.group(1)


def _site_rule() -> str:
    src = open(os.path.join(ROOT, "catalog", "gen_site.py"), encoding="utf-8").read()
    body = src[src.index("def load_withheld():"):src.index("WITHHELD = load_withheld()")]
    m = re.search(r're\.fullmatch\(r"(.+?)", sid\)', body)
    assert m, "the id rule of load_withheld not found"
    return m.group(1)


def test_one_id_rule_in_the_worker_the_validator_and_the_site():
    ts = _ts_rule()
    assert G.ID_RE.pattern == ts, f"gen_withheld_floor {G.ID_RE.pattern!r} != withheld.ts {ts!r}"
    assert "^" + _site_rule() + "$" == ts, f"gen_site {_site_rule()!r} != withheld.ts {ts!r}"


@pytest.mark.parametrize("good", [DST, "dst:DST:VAN66P", "insee_melodi:DS_TOUR_CAP", "ecb:EXR.D.AUD-EUR"])
def test_the_rule_admits_dst_ids(good):
    assert G.ID_RE.fullmatch(good)
    assert G.validate({"version": 1, "ids": {good: {"until": "operator"}}}) == [good]


@pytest.mark.parametrize("bad", ["dst:DST:x'y", "dst:DST:a b", "dst:", "dst:DST:%", "DST:DST:X", "dst",
                                 "dst:DST:X;DROP", 'dst:DST:"x"', "dst:DST:x\n"])
def test_the_rule_still_refuses_what_could_break_the_literal(bad):
    assert not G.ID_RE.fullmatch(bad)
    with pytest.raises(SystemExit):
        G.validate({"version": 1, "ids": {bad: {"until": "operator"}}})


def test_the_committed_list_names_the_35_dst_tables_all_until_operator():
    dst = {k: v for k, v in COMMITTED["ids"].items() if k.startswith("dst:")}
    assert len(dst) == 35 and DST in dst
    assert all(k.startswith("dst:DST:") and v["until"] == "operator" for k, v in dst.items()), \
        "a dst id is released by an operator only: no fetcher un-hides dst"
    assert G.validate(COMMITTED)


def test_the_bundle_gives_a_listed_id_its_sources_own_reason():
    """The wiring test (workerd, CI) reads the reason end to end; this pins the call site where it can run anywhere:
    a listed id's reason comes from withheldReason(id), not from one text for every source."""
    src = open(os.path.join(ROOT, "api", "worker", "src", "bundle.ts"), encoding="utf-8").read()
    branch = src[src.index("if (isWithheld(withheld, id)) {"):src.index("Canonical order (CONTRACT.md v1.1")]
    assert "withheld.ids.has(id)\n        ? withheldReason(id)" in branch.replace("\r\n", "\n")
    assert '"withheld: our copy is incomplete' not in branch, "a fixed text for every source is back in bundle.ts"


def test_the_floor_names_dst():
    ts = open(G.TS_PATH, encoding="utf-8").read()
    assert f'"{DST}"' in ts and ts.count('"dst:DST:') == 35


class Store:
    def __init__(self):
        self.objs, self.puts = {}, 0

    def get(self, key):
        return self.objs.get(key)

    def etag(self, key):
        return hashlib.md5(self.objs[key]).hexdigest() if key in self.objs else None

    def put_if(self, key, data, *, if_match=None, if_none_match=False):
        if if_none_match and key in self.objs:
            return None
        if if_match is not None and self.etag(key) != if_match:
            return None
        self.objs[key] = data
        self.puts += 1
        return hashlib.md5(data).hexdigest()


@pytest.fixture
def st(monkeypatch):
    s = Store()
    # the live list BEFORE the operator's step: the insee_melodi ids only (what the store held on 2026-10-19)
    s.objs[T.KEY] = T.encode({"version": 1, "ids": {k: v for k, v in COMMITTED["ids"].items()
                                                    if k.startswith("insee_melodi:")}})
    monkeypatch.setattr(T, "_store", lambda: s)
    monkeypatch.setattr(T, "catalogued", lambda ids: set(ids))
    return s


def _ids(s):
    return json.loads(s.objs[T.KEY])["ids"]


def test_the_operator_step_withholds_the_35_until_operator(st):
    dst = sorted(k for k in COMMITTED["ids"] if k.startswith("dst:"))
    n = len(_ids(st))
    assert T.main(["withhold", *dst, "--until", "operator", "--why", "one all-zero series"]) == 0 and st.puts == 0
    assert T.main(["withhold", *dst, "--until", "operator", "--why", "one all-zero series", "--apply"]) == 0
    got = _ids(st)
    assert len(got) == n + 35 and all(got[i]["until"] == "operator" for i in dst)
    assert sorted(got) == sorted(COMMITTED["ids"]), "after the step the store list == the committed copy"
    assert T.main(["release", DST, "--apply"]) == 0 and DST not in _ids(st), "an operator releases one id"


def test_until_complete_is_refused_for_a_source_no_fetcher_un_hides(st, capsys):
    before = st.puts
    assert T.main(["withhold", DST, "--why", "x", "--apply"]) == 2, "the default --until is complete"
    assert T.main(["withhold", DST, "--until", "complete", "--why", "x", "--apply"]) == 2
    assert st.puts == before and DST not in _ids(st)
    assert "no fetcher un-hides their source" in capsys.readouterr().out
    assert T.main(["withhold", "insee_melodi:DS_NEW", "--why", "x", "--apply"]) == 0, "insee_melodi: allowed"


def test_a_source_the_floor_does_not_name_is_still_refused_first(st):
    before = st.puts
    assert T.main(["withhold", "ecb:EXR.D.NEW", "--until", "operator", "--why", "x", "--apply"]) == 2
    assert st.puts == before


def test_the_site_subtracts_the_withheld_dst_tables(capsys):
    import importlib.util
    spec = importlib.util.spec_from_file_location("gen_site_dst_under_test", os.path.join(ROOT, "catalog", "gen_site.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    dst = sorted(k for k in COMMITTED["ids"] if k.startswith("dst:"))
    assert mod.WITHHELD.get("dst") == dst, "the generator dropped the dst ids (the id rule)"
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, start_date TEXT, end_date TEXT, "
                "geography TEXT, last_updated TEXT)")
    con.executemany("INSERT INTO series VALUES (?, 'dst', ?, ?, 'DK', '2026-10-01')",
                    [(DST, "1999-12-01", "2026-08-01"), ("dst:DST:FOLK1A", "2008-01-01", "2026-07-01")])
    roll = {"dst": {"n_series": 2}}
    mod.subtract_withheld(con, roll, {"dst": [DST, "dst:DST:NOT_CATALOGUED"]})
    assert roll["dst"]["n_series"] == 1 and roll["dst"]["min_start"] == "2008-01-01"
    assert "withheld: dst n_series -1" in capsys.readouterr().out
