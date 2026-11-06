"""stat_estonia: the boundary period is re-requested (D1), past periods can be re-pulled (D0, OFF by
default), an over-cap table is pulled in time chunks, and every merge is counted from a whole-row
compare (measured 2026-11-06: F:/work/fwd_estonia/REPORT.md, review AR-452).

Every test drives the REAL update(): the real ingester parser (jobs/ingest_stat_estonia.py), the real
time-axis resolver, the real merge on a local store. Only PxWeb is faked, and the fake answers what the
POSTed query asks - so a test fails on main by an assertion on the store or the note, not by an
ImportError.
"""
from __future__ import annotations

import datetime as dt
import itertools
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import stat_estonia as S  # noqa: E402

# Literal names, so a run against main fails by ASSERTION (main lacks the constants); pinned to
# the module in test_the_switches_default_boundary_on_and_revision_off.
BOUNDARY_ENV, REVISION_ENV, REVISION_FILE = ("STAT_ESTONIA_BOUNDARY_REFETCH",
                                             "STAT_ESTONIA_REVISION_FROM", "_revision.json")
Y = dt.date.today().year            # the fetcher's cold rule reads today's date: keep tables hot
END = lambda y: dt.date(y, 12, 31)  # noqa: E731


def _month(back):
    """(code, date) of the month `back` months before the current one."""
    t = dt.date.today().replace(day=1)
    m = t.month - back
    y = t.year
    while m <= 0:
        m += 12
        y -= 1
    return f"{y}M{m:02d}", dt.date(y, m, 1)


class Pub:
    """A fake andmed.stat.ee: tables {path: (variables, value_fn)}; value_fn(dict code->value) -> float|None.
    Records every POST; `fail` = {(path, n-th POST of that path)} answers with a TransientError."""

    def __init__(self, tables, fail=()):
        self.tables, self.fail, self.posts = tables, set(fail), []

    def meta(self, sess, url):
        path = url.split("https://x/", 1)[1]
        return {"variables": [dict(v) for v in self.tables[path][0]]} if path in self.tables else None

    def post(self, sess, url, body):
        path = url.split("https://x/", 1)[1]
        n = sum(1 for p, _ in self.posts if p == path)
        self.posts.append((path, body))
        if (path, n) in self.fail:
            raise S.TransientError("fake 503")
        variables, fn = self.tables[path]
        q = body["query"]
        ids = [x["code"] for x in q]
        sels = [list(x["selection"]["values"]) for x in q]
        vals = [fn(dict(zip(ids, combo))) for combo in itertools.product(*sels)]
        tcode = next((v["code"] for v in variables if v.get("time")), None)
        return {"id": ids, "size": [len(s) for s in sels], "value": vals,
                "role": {"time": [tcode]} if tcode else {},
                "dimension": {i: {"category": {"index": {c: k for k, c in enumerate(s)}}}
                              for i, s in zip(ids, sels)}}

    def cells(self, path):
        return [len(list(itertools.product(*[x["selection"]["values"] for x in b["query"]])))
                for p, b in self.posts if p == path]

    def asked_times(self, path, tcode):
        out = []
        for p, b in self.posts:
            if p == path:
                out += next(x["selection"]["values"] for x in b["query"] if x["code"] == tcode)
        return out


def _write_store(tmp_path, subj, rows):
    pq.write_table(pa.table({"series_key": pa.array([r[0] for r in rows], pa.string()),
                             "obs_date": pa.array([r[1] for r in rows], pa.date32()),
                             "value": pa.array([r[2] for r in rows], pa.float64())}),
                   str(tmp_path / f"{subj}.parquet"))


def _store(tmp_path, subj):
    t = pq.read_table(str(tmp_path / f"{subj}.parquet"))
    return {(k, d): v for k, d, v in zip(t.column("series_key").to_pylist(),
                                         t.column("obs_date").to_pylist(), t.column("value").to_pylist())}


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Wire update() to a local store in tmp_path, the real ingester (MAX_CELLS settable) and a Pub."""
    for k in ("AQUEDUCT_BACKEND", BOUNDARY_ENV, REVISION_ENV, "STAT_ESTONIA_REVISION_YEARS",
              "STAT_ESTONIA_REVISION_BUDGET_MIN", "STAT_ESTONIA_REVISION_EVERY_DAYS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(S, "OPERATOR_ACTIVATED_ON", None, raising=False)
    real = S._ingester()
    real.BASE = "https://x"
    monkeypatch.setattr(S, "_ingester", lambda: real)
    monkeypatch.setattr(S.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(S.time, "sleep", lambda s: None)
    box = types.SimpleNamespace(ing=real, tmp=tmp_path, mp=monkeypatch, pub=None)

    def run(pub, paths):
        box.pub = pub
        (tmp_path / "_catalog.json").write_text(json.dumps([{"path": p} for p in paths]), encoding="utf-8")
        monkeypatch.setattr(S, "_get_meta", pub.meta)
        monkeypatch.setattr(S, "_post_data", pub.post)
        return S.update(types.SimpleNamespace(config={}, key="stat_estonia/_all"), None)
    box.run = run
    return box


# ---- D1: a year axis with the quarter in the key ------------------------------------------------
RAA = "majandus/rahvamajandus/RAA0012.PX"
RAA_VARS = [{"code": "Aasta", "time": True, "values": [str(y) for y in range(Y - 2, Y + 1)]},
            {"code": "Kvartal", "values": ["1", "2", "3", "4"]}]


def _raa_store(tmp_path, q_this_year=("1",), v=10.0):
    pre = S._table_prefix(RAA)
    rows = [(f"{pre}:Kvartal={q}", END(y), v) for y in (Y - 2, Y - 1) for q in "1234"]
    rows += [(f"{pre}:Kvartal={q}", END(Y), v) for q in q_this_year]
    _write_store(tmp_path, "majandus", rows)
    return pre


def test_d1_a_later_quarter_of_the_stored_year_is_fetched(env):
    """RAA0012/RAA0042/IA002 shape: '2026' stored with Kvartal=I only; the publisher now has II. On
    main the request asks only years > 2026, so II is never asked (measured: 11 + 173 + 70 rows)."""
    pre = _raa_store(env.tmp)
    pub = Pub({RAA: (RAA_VARS, lambda c: 10.0 if c["Kvartal"] in ("1", "2") or c["Aasta"] != str(Y) else None)})
    res = env.run(pub, [RAA])
    st = _store(env.tmp, "majandus")
    assert st.get((f"{pre}:Kvartal=2", END(Y))) == 10.0, "the second quarter of the stored year merged"
    assert pub.asked_times(RAA, "Aasta") == [str(Y)], "the boundary year is asked, nothing older"
    assert res.changed_keys == {pre: END(Y).isoformat()}, res.changed_keys
    assert "merge note: 1 table(s) compared - changed 0 added 1 held 0" in res.error, res.error


def test_an_identical_boundary_refetch_writes_nothing_and_books_no_change(env):
    """The boundary is re-asked on every visit; an identical answer must not rewrite the subject parquet
    (CHALLENGE M2), must report no changed table, and must not count as new rows."""
    _raa_store(env.tmp, q_this_year=("1", "2"))
    calls = []
    real = S.merge.merge_and_write
    env.mp.setattr(S.merge, "merge_and_write", lambda *a, **k: calls.append(a) or real(*a, **k))
    pub = Pub({RAA: (RAA_VARS, lambda c: 10.0 if c["Kvartal"] in ("1", "2") or c["Aasta"] != str(Y) else None)})
    res = env.run(pub, [RAA])
    assert calls == [], "no write when nothing changed"
    assert res.status == "no_change" and res.changed_keys == {}, (res.status, res.changed_keys)


def test_a_revised_boundary_value_is_merged_and_booked_revised(env):
    """A changed value on a stored (key, date) overwrites in place and books revised_unit, so the
    pass is ok and the CSV phase runs (R1125). On main the boundary is never asked."""
    pre = _raa_store(env.tmp)
    pub = Pub({RAA: (RAA_VARS, lambda c: (11.5 if c["Aasta"] == str(Y) else 10.0) if c["Kvartal"] == "1" else None)})
    res = env.run(pub, [RAA])
    assert _store(env.tmp, "majandus")[(f"{pre}:Kvartal=1", END(Y))] == 11.5
    assert res.status == "ok" and "revised stored values" in res.error, (res.status, res.error)
    assert "changed 1 added 0" in res.error, res.error


def test_a_stored_row_the_publisher_no_longer_returns_is_held_never_deleted(env):
    pre = _raa_store(env.tmp, q_this_year=("1", "2"))
    pub = Pub({RAA: (RAA_VARS, lambda c: 10.0 if c["Kvartal"] == "1" else None)})   # II withdrawn
    res = env.run(pub, [RAA])
    assert _store(env.tmp, "majandus")[(f"{pre}:Kvartal=2", END(Y))] == 10.0, "kept"
    assert "held 1" in res.error, res.error


# ---- switches --------------------------------------------------------------------------------------
def test_the_switches_default_boundary_on_and_revision_off():
    assert (S.BOUNDARY_ENV, S.REVISION_ENV, S.REVISION_FILE) == (BOUNDARY_ENV, REVISION_ENV, REVISION_FILE)
    assert S.OPERATOR_ACTIVATED_ON is None, "merging this code must not arm the revision re-pull"
    assert S.revision_activated_on({}) is None
    assert S.revision_activated_on({S.REVISION_ENV: "2026-11-06"}) == dt.date(2026, 11, 6)
    assert S.revision_activated_on({S.REVISION_ENV: "soon"}) is None, "unparseable = off"
    assert S.boundary_refetch_on({}) is True
    assert S.boundary_refetch_on({S.BOUNDARY_ENV: "0"}) is False


def test_revision_off_by_default_asks_nothing_old_and_touches_no_sidecar(env):
    """Off means off: no period before the boundary is asked and _revision.json is neither read nor
    written. The SAME test with the switch armed (the mutation) must see both - so this is not a test
    that passes because the feature is absent."""
    _raa_store(env.tmp)
    seen = []
    rb, wb = S.blob.read_bytes, S.blob.write_bytes_atomic
    env.mp.setattr(S.blob, "read_bytes", lambda p: seen.append(("r", p)) or rb(p))
    env.mp.setattr(S.blob, "write_bytes_atomic", lambda p, d: seen.append(("w", p)) or wb(p, d))
    pub = Pub({RAA: (RAA_VARS, lambda c: 10.0)})
    env.run(pub, [RAA])
    assert pub.asked_times(RAA, "Aasta") == [str(Y)]
    assert not [s for s in seen if s[1].endswith(REVISION_FILE)], seen
    # mutation: arm it
    env.mp.setattr(S, "OPERATOR_ACTIVATED_ON", dt.date.today(), raising=False)
    seen.clear()
    pub2 = Pub({RAA: (RAA_VARS, lambda c: 10.0)})
    env.run(pub2, [RAA])
    assert str(Y - 2) in pub2.asked_times(RAA, "Aasta"), "armed: the window is asked"
    assert ("w", os.path.join(str(env.tmp), REVISION_FILE)) in seen


def test_the_boundary_valve_restores_the_strictly_newer_request(env):
    _raa_store(env.tmp)
    env.mp.setenv(BOUNDARY_ENV, "0")
    pub = Pub({RAA: (RAA_VARS, lambda c: 10.0)})
    env.run(pub, [RAA])
    assert pub.posts == [], "valve off: nothing newer than the stored year, so no POST (main's behaviour)"


# ---- D0: the revision window -------------------------------------------------------------------
def test_d0_armed_window_merges_revised_history_and_stamps_the_table(env):
    """RAA0063/RAA0042 shape: a past year revised. Armed by its env switch, the last N years are asked
    and the revision merges; a year outside the window is not asked; the table is stamped, so a second
    pass the same day does not re-pull it (slow cadence). On main the env switch does not exist."""
    pre = _raa_store(env.tmp)
    env.mp.setenv(REVISION_ENV, dt.date.today().isoformat())
    env.mp.setenv("STAT_ESTONIA_REVISION_YEARS", "2")            # window = Y-1 .. Y
    pub = Pub({RAA: (RAA_VARS, lambda c: (12.0 if c["Aasta"] != str(Y) else 10.0) if c["Kvartal"] == "1" or c["Aasta"] != str(Y) else None)})
    res = env.run(pub, [RAA])
    st = _store(env.tmp, "majandus")
    assert st[(f"{pre}:Kvartal=3", END(Y - 1))] == 12.0, "revised inside the window"
    assert st[(f"{pre}:Kvartal=3", END(Y - 2))] == 10.0, "outside the window: not asked, unchanged"
    assert str(Y - 2) not in pub.asked_times(RAA, "Aasta")
    side = json.loads((env.tmp / REVISION_FILE).read_text())
    assert side[RAA]["outcome"] == "merged", side
    assert "revision window 1 table(s) re-pulled, 0 due" in res.error, res.error
    pub2 = Pub(pub.tables)
    env.run(pub2, [RAA])
    assert pub2.asked_times(RAA, "Aasta") == [str(Y)], "stamped today: not due, boundary only"


def test_revision_rows_that_would_add_many_rows_are_withheld_but_changed_rows_merge(env):
    """TO0053 shape (CHALLENGE M3): the window returns 378 revised values AND detail history the store
    never held. The revisions merge; the additions under known series are withheld and named."""
    path = "majandus/toostus/TO0053.PX"
    codes = [_month(b) for b in range(14, 0, -1)]                  # 14 months, oldest first
    tvars = [{"code": "Kuu", "time": True, "values": [c for c, _ in codes]},
             {"code": "EMTAK", "values": [f"C{i:02d}" for i in range(60)]}]
    pre = S._table_prefix(path)
    rows = [(f"{pre}:EMTAK=C00", d, 1.0) for _, d in codes[:-1]]              # aggregate history
    rows += [(f"{pre}:EMTAK=C{i:02d}", codes[-2][1], 1.0) for i in range(1, 60)]  # detail at the max
    _write_store(env.tmp, "majandus", rows)
    env.mp.setenv(REVISION_ENV, dt.date.today().isoformat())
    pub = Pub({path: (tvars, lambda c: 2.0 if c["EMTAK"] == "C00" else 1.0)})
    res = env.run(pub, [path])
    st = _store(env.tmp, "majandus")
    assert st[(f"{pre}:EMTAK=C00", codes[0][1])] == 2.0, "the aggregate key's revision merged"
    assert (f"{pre}:EMTAK=C05", codes[0][1]) not in st, "detail history withheld (store repair is separate)"
    assert "withheld" in res.error and "TO0053.PX" in res.error, res.error
    assert json.loads((env.tmp / REVISION_FILE).read_text())[path]["outcome"] == "withheld"
    pub2 = Pub(pub.tables)
    env.run(pub2, [path])
    assert pub2.asked_times(path, "Kuu") == [codes[-1][0]], \
        "stamped withheld: waits its cadence, only the boundary (the month merged above) is asked"


# ---- B1: the tail must not mint a mixed-grain table ---------------------------------------------------
def test_a_new_period_in_full_detail_does_not_add_series_an_aggregate_table_never_held(env):
    """The bulk ingest stored this table AGGREGATED (whole history over MAX_CELLS) while one month in
    full detail fits. Main asks the new month in full detail and adds every detail series - a new
    D2 table. Now those rows are withheld and the aggregate key's new month still merges."""
    path = "majandus/ehitus/EH045.PX"
    codes = [_month(b) for b in (3, 2, 1)]
    tvars = [{"code": "Kuu", "time": True, "values": [c for c, _ in codes]},
             {"code": "Liik", "values": [f"L{i:03d}" for i in range(120)]}]
    pre = S._table_prefix(path)
    _write_store(env.tmp, "majandus", [(f"{pre}:Liik=L000", d, 5.0) for _, d in codes[:2]])
    pub = Pub({path: (tvars, lambda c: 5.0)})
    res = env.run(pub, [path])
    st = _store(env.tmp, "majandus")
    assert (f"{pre}:Liik=L000", codes[2][1]) in st, "the stored grain's new month merged"
    assert not [k for k, _ in st if not k.endswith("L000")], "no detail series added"
    # 119 detail series at the boundary month AND at the new month: the boundary path is guarded too
    assert "withheld 238 new-series" in res.error, res.error


# ---- chunked pull ---------------------------------------------------------------------------------
TO = "majandus/toostus/TO0083.PX"
MONTHS = [_month(b) for b in range(5, 0, -1)]          # 5 recent months, oldest first


def _to_vars(desc=True):
    """The publisher lists the month codes NEWEST FIRST, so only a sort makes the chunks ascending."""
    codes = [c for c, _ in MONTHS]
    return [{"code": "Kuu", "time": True, "values": codes[::-1] if desc else codes},
            {"code": "Tegevusala", "values": ["A", "B", "C"]}]


def _to_store(tmp_path, extra=()):
    pre = S._table_prefix(TO)
    _write_store(tmp_path, "majandus", [(f"{pre}:Tegevusala={x}", MONTHS[0][1], 1.0) for x in "ABC"]
                 + list(extra))
    return pre


def test_an_over_cap_table_is_pulled_in_time_chunks_at_full_detail(env):
    """Boundary + 4 new months x 3 codes = 15 cells over a 6-cell cap. Main measures the cap against the
    restricted list and collapses Tegevusala to one code (the D2 mechanism); now each POST stays under
    the cap, the codes go OLDEST FIRST although the publisher lists them newest first, and every
    detail series lands."""
    env.ing.MAX_CELLS = 6
    pre = _to_store(env.tmp)
    pub = Pub({TO: (_to_vars(), lambda c: 1.0)})
    env.run(pub, [TO])
    st = _store(env.tmp, "majandus")
    assert len(st) == 15 and {k for k, _ in st} == {f"{pre}:Tegevusala={x}" for x in "ABC"}, sorted(st)
    assert pub.cells(TO) == [6, 6, 3]
    asked = pub.asked_times(TO, "Kuu")
    assert asked == [c for c, _ in MONTHS], "ascending, so a failed later chunk can never land before an older one"


def test_a_table_with_no_stored_rows_keeps_the_ingesters_single_request(env):
    """VERIFY V-M1: ~1,500 visited tables hold no rows. Their full pull stays main's ONE request at the
    ingester's whole-history grain; the chunked full-detail pull is the store repair's path only."""
    env.ing.MAX_CELLS = 6
    pub = Pub({TO: (_to_vars(), lambda c: 1.0)})
    env.run(pub, [TO])
    assert pub.cells(TO) == [5], "one aggregated POST, as the ingester sends"
    plan = S._plan_table(env.ing, _to_vars(), None, full_pull_grain="table")
    assert [len(q[0]["selection"]["values"]) for q in plan["queries"]] == [2, 2, 1]
    assert plan["grain"] == "full" and [q[0]["selection"]["values"] for q in plan["queries"]][0] == \
        [c for c, _ in MONTHS][::-1][:2], "repair path: publisher order kept for a full pull (positional codes)"


def test_a_failed_chunk_merges_nothing_of_the_table(env):
    env.ing.MAX_CELLS = 6
    other = "majandus/toostus/TO0001.PX"
    po = S._table_prefix(other)
    _to_store(env.tmp, extra=[(f"{po}:Tegevusala={x}", MONTHS[0][1], 1.0) for x in "ABC"])
    pub = Pub({TO: (_to_vars(), lambda c: 2.0), other: (_to_vars(), lambda c: 2.0)}, fail={(TO, 1)})
    res = env.run(pub, [TO, other])
    st = _store(env.tmp, "majandus")
    assert sorted(v for (k, _), v in st.items() if "TO0083" in k) == [1.0] * 3, "all or nothing"
    assert len([k for k, _ in st if "TO0001" in k]) == 15, "the subject's other table still merged"
    assert res.status == "partial" and "TO0083.PX" in res.error, res.error


def test_the_deadline_between_chunks_abandons_the_table_and_resumes_at_it(env):
    env.ing.MAX_CELLS = 6
    calls = {"n": 0}

    class _DL:
        def __init__(self, minutes):
            pass

        def spent(self):               # 1: subject check, 2: table check, then the chunk check
            calls["n"] += 1
            return calls["n"] >= 3

        def elapsed_min(self):
            return 0.0
    env.mp.setattr(S, "Deadline", _DL)
    _to_store(env.tmp)
    pub = Pub({TO: (_to_vars(), lambda c: 2.0)})
    res = env.run(pub, [TO])
    assert len(_store(env.tmp, "majandus")) == 3, "nothing of the table merged"
    assert "stopped between its 3 request chunks" in res.error, res.error
    bm = json.loads((env.tmp / "_rotation_table.json").read_text())["after"]
    assert bm == "majandus|", "the table bookmark points BEFORE the abandoned table"


def test_grain_parity_with_the_ingester(env, monkeypatch):
    """Two parsers (R333): when the whole history fits, the fetcher's full pull asks exactly what the
    ingester asks; when one period is over the cap, both choose the same aggregate selection."""
    ing = env.ing
    for cap, tvars in ((100, _to_vars()),
                       (2, [{"code": "Aasta", "time": True, "values": ["2024", "2025"]},
                            {"code": "Tegevusala", "values": ["X", "TOTAL", "Y"]},
                            {"code": "Kuu", "values": ["1", "2"]}])):
        ing.MAX_CELLS = cap
        got = {}
        monkeypatch.setattr(ing, "get_json", lambda url, retries=3: {"variables": tvars})
        monkeypatch.setattr(ing, "post_json", lambda url, body, retries=3: got.setdefault("b", body) and None)
        ing.query_table({"path": "x/T.PX"})
        plan = S._plan_table(ing, tvars, None)
        sel = lambda q: {x["code"]: x["selection"]["values"] for x in q if x["code"] != "Aasta"}  # noqa: E731
        assert all(sel(q) == sel(got["b"]["query"]) for q in plan["queries"]), (cap, plan, got)


# ---- refusals, the never-delete guard, the note ---------------------------------------------------
def test_a_repeated_key_in_the_fetch_refuses_that_table_only(env):
    """Two time codes that parse to one date ('2025' and '2025-12-31') make (series_key, obs_date) no
    key; main's `seen` set dropped the repeat silently and merged. Now the table is refused, named and
    transient, and the subject's other table still merges."""
    bad, good = "majandus/a/BAD1.PX", "majandus/a/GOOD1.PX"
    pb, pg = S._table_prefix(bad), S._table_prefix(good)
    _write_store(env.tmp, "majandus", [(f"{pb}:X=1", END(Y - 1), 1.0), (f"{pg}:X=1", END(Y - 1), 1.0)])
    bvars = [{"code": "Aasta", "time": True, "values": [str(Y - 1), str(Y), f"{Y}-12-31"]},
             {"code": "X", "values": ["1"]}]
    gvars = [{"code": "Aasta", "time": True, "values": [str(Y - 1), str(Y)]}, {"code": "X", "values": ["1"]}]
    pub = Pub({bad: (bvars, lambda c: 2.0), good: (gvars, lambda c: 2.0)})
    res = env.run(pub, [bad, good])
    st = _store(env.tmp, "majandus")
    assert (pb + ":X=1", END(Y)) not in st and st[(pb + ":X=1", END(Y - 1))] == 1.0, "refused: untouched"
    assert st[(pg + ":X=1", END(Y))] == 2.0, "the other table merged"
    assert res.status == "partial" and "BAD1.PX: merge refused" in res.error, res.error


def test_a_duplicate_elsewhere_in_the_stored_file_is_a_subject_transient_not_a_lost_row(env):
    """min_ratio=1.0: the merge would collapse a stored duplicate (a delete). It is refused for that
    subject and booked transient; main's 0.97 ratio collapsed it silently."""
    pre = _raa_store(env.tmp)
    t = pq.read_table(str(env.tmp / "majandus.parquet"))
    extra = pa.table({"series_key": ["EE:majandus:zz:OTHER.PX:A=1"] * 2,
                      "obs_date": pa.array([END(Y - 1)] * 2, pa.date32()), "value": [1.0, 2.0]})
    pq.write_table(pa.concat_tables([t, extra]), str(env.tmp / "majandus.parquet"))
    n_before = pq.read_metadata(str(env.tmp / "majandus.parquet")).num_rows
    pub = Pub({RAA: (RAA_VARS, lambda c: 10.0)})
    res = env.run(pub, [RAA])
    assert pq.read_metadata(str(env.tmp / "majandus.parquet")).num_rows == n_before, "nothing lost"
    assert res.status == "partial" and "merge refused" in res.error, res.error
    assert (f"{pre}:Kvartal=2", END(Y)) not in _store(env.tmp, "majandus")


def test_the_merge_note_keeps_a_deferral_pass_rotating_and_a_failure_attention(env):
    """The note is a non-failure tail (base.MERGE_NOTE): a pure deferral pass stays ROTATING; the same
    pass with one transient reads ATTENTION. No '; ' inside the note, even from a table name."""
    from updater.health import _deferral_only
    weird = "majandus/a/W; 1.PX"          # "; " in the LEAF, the part the note prints
    pw = S._table_prefix(weird)
    pre = _raa_store(env.tmp)
    t = pq.read_table(str(env.tmp / "majandus.parquet"))
    _write_store(env.tmp, "majandus", list(zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                                              t.column("value").to_pylist())) + [(f"{pw}:X=1", END(Y), 1.0)])
    wvars = [{"code": "Aasta", "time": True, "values": [str(Y)]}, {"code": "X", "values": ["1"]}]
    # an archive subject with a cold, not-due table makes the pass a deferral pass
    arch = "Lepetatud_tabelid/A/OLD1.PX"
    _write_store(env.tmp, "Lepetatud_tabelid", [(S._table_prefix(arch) + ":X=1", dt.date(2001, 12, 31), 1.0)])
    env.mp.setenv("STAT_ESTONIA_COLD_SLICE", "0")
    pub = Pub({RAA: (RAA_VARS, lambda c: 10.0 if c["Kvartal"] in ("1", "2") or c["Aasta"] != str(Y) else None),
               weird: (wvars, lambda c: 3.0)})
    res = env.run(pub, [RAA, weird, arch])
    note = res.error.split("; merge note:", 1)[1]
    assert "; " not in note and "W, 1.PX" in note, res.error
    assert _deferral_only([{"status": res.status, "last_error": res.error}]), res.error
    assert not _deferral_only([{"status": "partial",
                                "last_error": "1/3 sub-unit(s) transient-failed; will retry [x]; merge" + note}])
    assert res.changed_keys == {pre: END(Y).isoformat(), pw: END(Y).isoformat()}, res.changed_keys


def test_a_cold_table_sends_no_boundary_request(env):
    arch = "Lepetatud_tabelid/A/OLD1.PX"
    avars = [{"code": "Aasta", "time": True, "values": ["2000", "2001"]}, {"code": "X", "values": ["1"]}]
    _write_store(env.tmp, "Lepetatud_tabelid", [(S._table_prefix(arch) + ":X=1", dt.date(2001, 12, 31), 1.0)])
    pub = Pub({arch: (avars, lambda c: 9.0)})
    env.run(pub, [arch])
    assert pub.posts == [], "cold (archive): strictly newer only, and nothing is newer"


def test_a_merge_report_that_disagrees_with_the_compare_is_a_named_defect(env):
    """VERIFY minor 4: the merge's changed-key report must equal the compare's changed + added keys."""
    _raa_store(env.tmp)
    real = S.merge.merge_and_write

    def _lying(*a, **k):
        n, md, ch = real(*a, **k)
        return n, md, {}
    env.mp.setattr(S.merge, "merge_and_write", _lying)
    pub = Pub({RAA: (RAA_VARS, lambda c: 10.0 if c["Kvartal"] in ("1", "2") or c["Aasta"] != str(Y) else None)})
    res = env.run(pub, [RAA])
    assert res.status == "partial" and "merge DEFECT" in res.error and "changed-key report" in res.error, res.error
