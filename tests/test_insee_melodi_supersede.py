"""F1b - the operator-authorised SUPERSEDE re-pull - and AR-334 R3-1 (a finalize no pass can fit).

Design AR-335 (conditions D1-D12, gaps G1-G4). Harness: tests/test_insee_melodi_repull.py (FakeMelodi, env,
obs(); PAGE_SIZE 10, CHUNK_PAGES 3). The DD_CNA shape: INSEE added ONE constant dimension (GEO=2025-FRANCE-F)
to every key of a flow whose stored keys never carried it.
"""
from __future__ import annotations

import datetime as dt
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S
from test_insee_melodi_repull import (P, FakeMelodi, env, obs, series, _control_flows,  # noqa: F401
                                      _seed_store, run_until_quiet)

F = "DD_CNA_FIXT"          # the listed flow
G = "DD_CNA_OTHER"         # the same respelling, NOT listed
NEW_MOD = "2026-10-15T00:00:00"
D = dt.date


def cna(n, years, geo=False, aa=False, bump=0.0):
    """n series x years; geo=True adds the constant GEO=2025-FRANCE-F (at the START of the sorted key, or in
    the MIDDLE when aa=True adds a dimension sorting before it)."""
    out = []
    for y in years:
        for i in range(n):
            d = {"NA": f"N{i:03d}", "SEX": "_T"}
            if aa:
                d["AA"] = "1"
            if geo:
                d["GEO"] = "2025-FRANCE-F"
            out.append(obs(d, y, float(i) + y / 10000 + bump))
    return out


RESIDUE = [obs({"NA": "GONE", "SEX": "_T"}, 2023, 7.0), obs({"NA": "GONE", "SEX": "_T"}, 2024, 8.0)]
EMPTY_KEY = [obs({}, 2025, 9.0)]                     # G1: AGREGATS / PATRIMOINE each store one '' key row
OLD = cna(25, [2023, 2024]) + RESIDUE + EMPTY_KEY    # 53 stored rows
NEW = cna(25, [2023, 2024, 2025], geo=True)          # 75 rows INSEE now serves


def _force(env, body):
    (env.store / S.FORCE_FILE).write_text(json.dumps(body), encoding="utf-8")


def _edit_state(env, fn):
    p = env.store / S.STATE_FILE
    data = json.loads(p.read_text(encoding="utf-8"))
    fn(data)
    p.write_text(json.dumps(data, indent=1, sort_keys=True), encoding="utf-8")


def _settled(env, extra=None):
    """F (and G) stored in the OLD spelling and classified; then INSEE switches to the NEW spelling."""
    flows = _control_flows()
    flows[F] = list(OLD)
    if extra:
        flows.update(extra)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put(F, OLD)
    for c in (extra or {}):
        env.put(c, flows[c])
    run_until_quiet(env, fake)
    assert env.state()["flows"][F]["status"] in (S.LEGACY, S.COMPLETE)
    fake.flows[F] = list(NEW)
    fake.modified[F] = NEW_MOD
    return fake


def _keys(env, code):
    t = pq.read_table(str(env.store / f"{code}.parquet"))
    return sorted(zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                      t.column("value").to_pylist()))


def _errs(results):
    return " | ".join(r.error or "" for r in results)


# ------------------------------------------------------------------------------------------ the 6-tail shape
def test_supersede_replaces_the_stored_spelling_keeps_residue_and_the_empty_key(env):
    fake = _settled(env)
    _force(env, {"supersede": [F], "only": "supersede"})
    res = run_until_quiet(env, fake)
    fe = env.state()["flows"][F]
    assert fe["status"] == S.COMPLETE and not fe.get("hold") and "supersede" not in fe, fe
    assert fe["supersede_key"] == "listed"
    rows = _keys(env, F)
    # every NEW row, plus the stored rows with no counterpart: the GONE series and the dimensionless key (G1)
    assert len(rows) == 75 + 3
    assert ("NA=GONE:SEX=_T", D(2023, 1, 1), 7.0) in rows or any(k == "NA=GONE:SEX=_T" for k, _, _ in rows)
    assert any(k == "" for k, _, _ in rows), "the dimensionless stored key must survive as residue"
    assert not any(k.startswith("NA=N") for k, _, _ in rows), "an old-spelling row with a counterpart survived"
    sup = fe["superseded"]
    assert sup["dimension"] == "GEO=2025-FRANCE-F"
    assert (sup["stored"], sup["superseded"], sup["kept_old_only"]) == (53, 50, 3)
    assert (sup["staged"], sup["new_with_counterpart"], sup["new_only"], sup["revised"]) == (75, 50, 25, 0)
    assert sup["published"] == 78 and fe["residue"]["rows"] == 3
    assert any(F in (r.changed_keys or {}) for r in res), "the CSV is owed in the same run"
    assert env.chunks() == []


def test_the_supersede_strips_a_component_in_the_MIDDLE_of_the_key(env):
    old = cna(12, [2023], aa=True)
    flows = _control_flows()
    flows[F] = old
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put(F, old)
    run_until_quiet(env, fake)
    fake.flows[F] = cna(12, [2023, 2024], aa=True, geo=True)
    fake.modified[F] = NEW_MOD
    _force(env, {"supersede": [F]})
    run_until_quiet(env, fake)
    fe = env.state()["flows"][F]
    assert fe["superseded"]["superseded"] == 12 and env.rows(F) == 24
    assert all(":GEO=2025-FRANCE-F:" in k for k, _, _ in _keys(env, F))


def test_a_NON_listed_flow_with_the_same_respelling_is_still_HELD(env):
    fake = _settled(env, extra={G: cna(25, [2023, 2024])})
    fake.flows[G] = cna(25, [2023, 2024, 2025], geo=True)
    fake.modified[G] = NEW_MOD
    _force(env, {"supersede": [F], "flows": [G]})
    for _ in range(6):
        env.run(fake)
    st = env.state()["flows"]
    assert st[F]["superseded"]["published"] == 78
    assert st[G].get("hold") == "spelling" and env.rows(G) == 50, st[G]


# ------------------------------------------------------------------------------------------ refusals (unit)
def _tbl(rows):
    return pa.table({"flow": ["F"] * len(rows), "series_key": [r[0] for r in rows],
                     "obs_date": pa.array([r[1] for r in rows], pa.date32()),
                     "value": pa.array([r[2] for r in rows], pa.float64())})


def _plan(tmp_path, staged, stored):
    sp, op = tmp_path / "staged.parquet", tmp_path / "stored.parquet"
    pq.write_table(_tbl(staged), str(sp))
    pq.write_table(_tbl(stored), str(op))
    con = duckdb.connect()
    try:
        sup = M._plan_supersede(con, M._sq(sp), M._sq(op))
        pub = sorted(con.execute("SELECT series_key, obs_date, value FROM _sup_pub").fetchall()) \
            if sup["refused"] is None else None
    finally:
        con.close()
    return sup, pub


GF = "GEO=2025-FRANCE-F"
OLD_U = [("NA=1:SEX=T", D(2023, 12, 31), 1.0), ("NA=2:SEX=T", D(2023, 12, 31), 2.0),
         ("NA=3:SEX=T", D(2023, 12, 31), 3.0), ("NA=4:SEX=T", D(2023, 12, 31), 4.0),
         ("NA=5:SEX=T", D(2023, 12, 31), 5.0)]
NEW_U = [(f"{GF}:{k}", d, v) for k, d, v in OLD_U]


def test_unit_the_plain_supersede_is_accepted(tmp_path):
    sup, pub = _plan(tmp_path, NEW_U, OLD_U)
    assert sup["refused"] is None and sup["superseded"] == 5 and sup["published"] == 5 and len(pub) == 5


@pytest.mark.parametrize("staged,stored,why", [
    # a GEO-VINTAGE respelling: the same dimension names - not this kind of respelling, and the old vintage
    # (not re-crawlable) is never dropped
    ([("GEO=2026-COM-1:SEX=T", D(2023, 12, 31), 1.0)], [("GEO=2025-COM-1:SEX=T", D(2023, 12, 31), 1.0)],
     "nothing to supersede"),
    # two added dimensions
    ([(f"{GF}:NA=1:REF=X:SEX=T", D(2023, 12, 31), 1.0)], [("NA=1:SEX=T", D(2023, 12, 31), 1.0)],
     "nothing to supersede"),
    # a dimension the staged side LOST (several old series would collapse onto one key)
    ([(f"{GF}:NA=1", D(2023, 12, 31), 1.0)], [("NA=1:SEX=T", D(2023, 12, 31), 1.0)], "nothing to supersede"),
    # a multi-valued added dimension
    ([(f"{GF}:NA=1:SEX=T", D(2023, 12, 31), 1.0), ("GEO=2025-FRANCE-M:NA=2:SEX=T", D(2023, 12, 31), 2.0)],
     OLD_U[:2], "several values"),
    # an added key WITHOUT the dimension (b)
    (NEW_U + [("NA=9:SEX=T", D(2023, 12, 31), 9.0)], OLD_U, "several values"),
    # many-to-one per row: two staged rows on one (stripped key, date)
    (NEW_U + [NEW_U[0]], OLD_U, "not one-to-one"),
    # G3: a stored (key, date) twice - counted, never assumed
    (NEW_U, OLD_U + [OLD_U[0]], "not one-to-one"),
    # two staged KEYS on one stripped key (the tool's per-key check)
    (NEW_U + [("NA=1:GEO=2025-FRANCE-F:SEX=T", D(2022, 12, 31), 0.5)], OLD_U, "not one-to-one"),
])
def test_unit_refusals(tmp_path, staged, stored, why):
    sup, _ = _plan(tmp_path, staged, stored)
    assert sup["refused"] and why in sup["refused"], sup


def test_unit_revised_over_20_percent_refuses_and_20_percent_does_not(tmp_path):
    one = [(k, d, v + (1.0 if i == 0 else 0.0)) for i, (k, d, v) in enumerate(NEW_U)]           # 1 of 5
    three = [(k, d, v + (1.0 if i < 3 else 0.0)) for i, (k, d, v) in enumerate(NEW_U)]         # 3 of 5
    s1, p1 = _plan(tmp_path, one, OLD_U)
    assert s1["refused"] is None and s1["revised"] == 1
    assert (f"{GF}:NA=1:SEX=T", D(2023, 12, 31), 2.0) in p1, "the new value wins (D2)"
    s3, _ = _plan(tmp_path, three, OLD_U)
    assert s3["refused"] and "different value" in s3["refused"] and s3["revised"] == 3


def test_unit_the_sanity_bound_refuses(tmp_path, monkeypatch):
    monkeypatch.setattr(M, "SUPERSEDE_MAX_ROWS", 4)
    sup, _ = _plan(tmp_path, NEW_U, OLD_U)
    assert sup["refused"] and "SUPERSEDE_MAX_ROWS" in sup["refused"]


@pytest.mark.parametrize("over,why", [
    ({"published": 9, "stored": 10}, "smaller than the stored one"),
    ({"superseded": 6, "staged": 5}, "superseded by only"),
    ({"duplicates": 1}, "duplicate"),
])
def test_unit_the_shrink_and_backstop_refusals(over, why):
    base = {"duplicates": 0, "published": 10, "stored": 10, "superseded": 5, "staged": 5,
            "new_with_counterpart": 5, "revised": 0}
    r = M._supersede_refusal({**base, **over})
    assert r and why in r


def test_unit_a_dimensionless_key_on_either_side(tmp_path):
    """G1: '' adds no dimension name to either union and pairs only with an identical key."""
    sup, pub = _plan(tmp_path, NEW_U + [("", D(2025, 12, 31), 9.5)], OLD_U + [("", D(2025, 12, 31), 9.0)])
    assert sup["refused"] is None and sup["superseded"] == 6 and sup["kept_old_only"] == 0
    assert ("", D(2025, 12, 31), 9.5) in pub
    sup, pub = _plan(tmp_path, NEW_U, OLD_U + [("", D(2025, 12, 31), 9.0)])
    assert sup["refused"] is None and sup["kept_old_only"] == 1 and ("", D(2025, 12, 31), 9.0) in pub


# ------------------------------------------------------------------------------------------ the repair tool's rule
# tools/repair_insee_spelling.py (PR #163) is NOT imported (AR-335 D1: the rule lives in the fetcher). Its
# fixture and expected publish are copied here verbatim, so a drift between the two rules fails this test.
TOOL_OLD = [("A=1:B=x", D(2024, 12, 31), 1.0), ("A=1:B=x", D(2025, 12, 31), 2.0),
            ("A=2:B=y", D(2025, 12, 31), 3.0), ("A=3:B=z", D(2020, 12, 31), 4.0)]
TOOL_NEW = [("A=1:B=x:GEO=2025-FRANCE-F", D(2024, 12, 31), 1.0),
            ("A=1:B=x:GEO=2025-FRANCE-F", D(2025, 12, 31), 2.5),
            ("A=3:B=z:GEO=2025-FRANCE-F", D(2021, 12, 31), 5.0)]
TOOL_PUB = sorted([("A=1:B=x:GEO=2025-FRANCE-F", D(2024, 12, 31), 1.0),
                   ("A=1:B=x:GEO=2025-FRANCE-F", D(2025, 12, 31), 2.5),
                   ("A=3:B=z:GEO=2025-FRANCE-F", D(2021, 12, 31), 5.0),
                   ("A=2:B=y", D(2025, 12, 31), 3.0), ("A=3:B=z", D(2020, 12, 31), 4.0)])


def test_the_fetcher_and_the_repair_tool_agree_on_the_tools_fixture(tmp_path, monkeypatch):
    # the tool's fixture revises 1 of its 2 counterparts (50%): D2's cap refuses that by design, so the cap is
    # lifted here to compare the PAIRING itself
    s0, _ = _plan(tmp_path, TOOL_NEW, TOOL_OLD)
    assert s0["refused"] and "different value" in s0["refused"]
    monkeypatch.setattr(M, "SUPERSEDE_MAX_REVISED", 1.0)
    sup, pub = _plan(tmp_path, TOOL_NEW, TOOL_OLD)
    assert sup["refused"] is None and pub == TOOL_PUB
    # the tool's report: dropped_old_rows 2, kept_old_rows_residue 2, revised 1, residue rows 2
    assert (sup["superseded"], sup["kept_old_only"], sup["revised"], sup["residue"]["rows"]) == (2, 2, 1, 2)


@pytest.mark.parametrize("extra_old,extra_new", [
    ([("A=9:GEO=FR", D(2020, 12, 31), 9.0)], []),                      # an old key with a GEO
    ([], [("A=7:B=q", D(2025, 12, 31), 7.0)]),                         # an added key without the GEO
    ([], [("A=1:GEO=2025-FRANCE-F:B=x", D(2023, 12, 31), 0.5)]),       # two new keys on one stripped key
])
def test_the_tools_refusals_are_refused_by_the_fetcher_too(tmp_path, monkeypatch, extra_old, extra_new):
    monkeypatch.setattr(M, "SUPERSEDE_MAX_REVISED", 1.0)
    sup, _ = _plan(tmp_path, TOOL_NEW + extra_new, TOOL_OLD + extra_old)
    assert sup["refused"], sup


# ------------------------------------------------------------------------------------------ refusal end to end, D3
def _refused_supersede(env):
    fake = _settled(env)
    fake.flows[F] = cna(25, [2023, 2024, 2025], geo=True, bump=0.25)      # every counterpart revised
    _force(env, {"supersede": [F]})
    res = run_until_quiet(env, fake, passes=3)
    fe = env.state()["flows"][F]
    assert fe["status"] == S.MERGE_PENDING and fe.get("supersede") and fe.get("merge_refusals"), fe
    assert env.rows(F) == 53 and env.chunks(), "a refused supersede publishes nothing and keeps every chunk"
    assert "supersede refused" in _errs(res) and "different value" in _errs(res)
    return fake


def test_a_refused_supersede_is_loud_keeps_merge_pending_and_the_chunks(env):
    _refused_supersede(env)


def test_D3_unlisting_after_a_refusal_cancels_and_the_next_pass_HOLDS(env):
    fake = _refused_supersede(env)
    _force(env, {"supersede": []})
    for _ in range(3):
        env.run(fake)
    fe = env.state()["flows"][F]
    assert "supersede" not in fe and "supersede_key" not in fe
    assert fe.get("hold") == "spelling" and env.rows(F) == 53, fe


# ------------------------------------------------------------------------------------------ D4 / G4
def test_D4_a_kill_after_the_publish_keeps_the_first_plans_counts_and_owes_the_csv(env, monkeypatch):
    fake = _settled(env)
    _force(env, {"supersede": [F], "only": "supersede"})
    real = M.blob.commit_local_file
    seen = {}

    def publish_then_die(tmp, path):
        if path.endswith(f"{F}.parquet"):
            # G4: pre_merge_identity and the plan are on the store BEFORE the commit
            at = json.loads((env.store / S.STATE_FILE).read_text(encoding="utf-8"))["flows"][F]["attempt"]
            seen["pre"], seen["plan"] = at.get("pre_merge_identity"), at.get("supersede_plan")
            real(tmp, path)
            raise KeyboardInterrupt("killed after the publish")
        return real(tmp, path)
    monkeypatch.setattr(M.blob, "commit_local_file", publish_then_die)
    with pytest.raises(KeyboardInterrupt):
        run_until_quiet(env, fake)
    assert seen["pre"] and seen["plan"]["published"] == 78
    monkeypatch.setattr(M.blob, "commit_local_file", real)
    r = env.run(fake)
    fe = env.state()["flows"][F]
    assert F in (r.changed_keys or {}), "the CSV of the landed publish is owed"
    sup = fe["superseded"]
    assert (sup["superseded"], sup["kept_old_only"], sup["published"]) == (50, 3, 78), sup
    assert env.rows(F) == 78 and fe["status"] == S.COMPLETE


@pytest.mark.parametrize("stale", [5, 10_000])
def test_G4_the_shrink_guard_reads_the_stored_object_never_a_stale_served_rows(env, stale):
    fake = _settled(env)

    def lie(d):
        d["flows"][F]["served"]["rows"] = stale
    _edit_state(env, lie)
    _force(env, {"supersede": [F], "only": "supersede"})
    run_until_quiet(env, fake)
    sup = env.state()["flows"][F]["superseded"]
    assert sup["stored"] == 53 and sup["published"] == 78 and env.rows(F) == 78


# ------------------------------------------------------------------------------------------ D5 "only"
def test_D5_only_writes_nothing_for_an_unlisted_flow_and_an_unlisted_in_progress_flow_does_not_block(env):
    fake = _settled(env)

    def park(d):
        d["flows"]["DS_CTRL"].update(status=S.IN_PROGRESS, attempt={
            "id": "a20261017000000-abcdef", "mode": "origin", "next_page": 2, "chunks": 0, "rows_staged": 0,
            "pages": 1, "started_utc": "2026-10-17T00:00:00+00:00"})
    _edit_state(env, park)
    before = {c: fe for c, fe in env.state()["flows"].items() if c != F}
    _force(env, {"supersede": [F], "only": "supersede"})
    n0 = len(fake.calls)
    r = env.run(fake)
    after = {c: fe for c, fe in env.state()["flows"].items() if c != F}
    assert after == before, "an 'only' pass wrote to an unlisted flow"
    other = [u for u in fake.calls[n0:] if "/dataflow/all" not in u and f"/{F}" not in u]
    assert other == [], other[:5]
    assert env.state()["flows"][F]["superseded"]["published"] == 78
    assert "SKIPPED" in (r.error or "") and r.status not in ("ok", "no_change")


def test_D5_a_stale_only_is_ignored_and_the_pass_runs_in_full(env):
    fake = _settled(env)
    _force(env, {"supersede": [F], "only": "supersede"})
    run_until_quiet(env, fake)
    assert env.state()["flows"][F]["superseded"]
    r = env.run(fake)                                                   # the file is left as it was
    assert 'ignored - no listed flow has a supersede pending' in (r.error or "")
    assert "SKIPPED" not in (r.error or "")


def test_D5_only_with_an_empty_list_is_ignored(env):
    fake = _settled(env)
    _force(env, {"supersede": [], "only": "supersede"})
    r = env.run(fake)
    assert 'ignored - no listed flow has a supersede pending' in (r.error or "")


# ------------------------------------------------------------------------------------------ D6 / G2
def _held_origin(env):
    """F's origin pull of the NEW spelling, held by the F1 rule (not listed) - every page staged."""
    fake = _settled(env)
    _force(env, {"flows": [F]})
    for _ in range(4):
        env.run(fake)
        if env.state()["flows"][F].get("hold"):
            break
    fe = env.state()["flows"][F]
    assert fe.get("hold") == "spelling" and fe["attempt"]["mode"] == "origin" and env.chunks()
    return fake


def test_D6_a_moved_modified_drops_the_held_pull_and_supersedes_with_a_fresh_one(env):
    fake = _held_origin(env)
    fake.flows[F] = cna(25, [2023, 2024, 2025, 2026], geo=True)        # INSEE published again
    fake.modified[F] = "2026-11-01T00:00:00"
    _force(env, {"flows": [F], "supersede": [F], "only": "supersede"})
    res = run_until_quiet(env, fake)
    assert "is not the pull's" in _errs(res)
    fe = env.state()["flows"][F]
    assert fe["superseded"]["staged"] == 100 and env.rows(F) == 103, fe.get("superseded")


def test_G2_an_unreadable_modified_DEFERS_and_keeps_the_pull(env):
    fake = _held_origin(env)
    keep = fake.modified[F]
    fake.modified[F] = None                                            # the catalogue answers 500
    _force(env, {"flows": [F], "supersede": [F], "only": "supersede"})
    r = env.run(fake)
    fe = env.state()["flows"][F]
    assert "supersede deferred" in (r.error or "")
    assert fe["status"] == S.MERGE_PENDING and fe["attempt"]["mode"] == "origin" and env.chunks()
    assert env.rows(F) == 53
    fake.modified[F] = keep
    run_until_quiet(env, fake)
    assert env.state()["flows"][F]["superseded"]["published"] == 78


# ------------------------------------------------------------------------------------------ D10
def test_D10_flows_and_supersede_supersede_wins(env):
    fake = _settled(env)
    _force(env, {"flows": [F], "supersede": [F]})
    res = run_until_quiet(env, fake)
    assert "supersede wins; the force_origin entry is not used" in _errs(res)
    fe = env.state()["flows"][F]
    assert fe["superseded"]["published"] == 78 and not fe.get("forced_key")


def test_D10_supersede_and_release_holds_supersede_wins_and_the_release_is_logged_unused(env):
    fake = _held_origin(env)
    _force(env, {"supersede": [F], "release_holds": [F]})
    res = run_until_quiet(env, fake)
    assert "the release is not used" in _errs(res)
    fe = env.state()["flows"][F]
    assert fe["superseded"]["published"] == 78 and env.rows(F) == 78 and not fe.get("hold_release_key")


def test_D10_a_held_walk_TAIL_is_dropped_and_an_origin_pull_supersedes(env):
    """The reverted tails' state: legacy, modified None - the walk tails them, and the tail is HELD."""
    fake = _settled(env)

    def revert(d):
        d["flows"][F]["modified"] = None
    _edit_state(env, revert)
    for _ in range(3):
        env.run(fake)
        if env.state()["flows"][F].get("hold"):
            break
    fe = env.state()["flows"][F]
    assert fe.get("hold") and fe["attempt"]["mode"] == "tail", (fe.get("status"), fe.get("attempt"))
    _force(env, {"supersede": [F]})
    res = run_until_quiet(env, fake)
    assert "operator supersede (was merge_pending) - an origin pull is queued" in _errs(res)
    fe = env.state()["flows"][F]
    assert fe["superseded"]["published"] == 78 and fe["basis"] == "origin"


def test_D10_listing_before_the_walk_wins_the_tail_never_runs(env):
    """apply_force runs before the walk: a listed reverted tail is queued for origin and never tailed."""
    fake = _settled(env)

    def revert(d):
        d["flows"][F]["modified"] = None
    _edit_state(env, revert)
    _force(env, {"supersede": [F]})
    n0 = len(fake.calls)
    for _ in range(6):
        r = env.run(fake)
        fe = env.state()["flows"][F]
        assert not fe.get("hold") and (fe.get("attempt") or {}).get("mode") in (None, "origin"), fe
        if r.status != "transient_fail":
            break
    assert fe["superseded"]["published"] == 78
    assert not [u for u in fake.calls[n0:] if f"/{F}?startPeriod" in u], "the walk tailed a listed flow"


def test_the_supersede_list_must_be_a_list_or_an_object():
    assert "not a list" in S.force_problem({"supersede": "DD_CNA_REBS"})
    assert S.force_problem({"supersede": ["DD_CNA_REBS"], "only": "supersede"}) is None
    assert S.supersede_entries({"supersede": {"X": "2026-10-17"}}) == {"X": "utc:2026-10-17"}


# ------------------------------------------------------------------------------------------ AR-334 R3-1 (D12)
def test_R3_1_a_finalize_that_can_never_fit_does_not_wedge_the_queue_silently(env, monkeypatch):
    """AR-334 round 3 ADV3 (cfedf82c9), ported unchanged but for the harness import. The margin grew ~4.5x
    (35 us per staged+stored row). A merge_pending flow whose margin exceeds what the repair slot's head can
    offer is deferred EVERY pass, returns "progress", and repair_slot returns on "progress" - so no other flow
    is ever worked. Here: margin forced above the whole window."""
    flows = _control_flows()
    flows["DS_ZNEXT"] = series(25)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_ZNEXT", flows["DS_ZNEXT"][:P])
    monkeypatch.setattr(S, "SPLIT_S_PER_ROW", 1000.0)                 # every finalize needs > 45 min
    errs = []
    for _ in range(4):
        r = env.run(fake)
        errs.append(r.error or "")
    st = env.state()["flows"]
    worked = (st["DS_ZNEXT"].get("attempt") or {}).get("next_page", 1) > 1 or st["DS_ZNEXT"]["status"] != S.NEEDS_ORIGIN
    loud = any("NEEDS THE OWNER" in e or "never fit" in e or "cannot fit" in e for e in errs)
    assert worked or loud, (st["DS_ZNEXT"]["status"], [s["status"] for s in st.values()], errs[-1][:300])


def _blocked(env, monkeypatch):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    real = S.merge_margin_s
    monkeypatch.setattr(S, "merge_margin_s",
                        lambda rows, stored: 10 ** 6 if rows == 25 else real(rows, stored))
    res = [env.run(fake) for _ in range(4)]
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe["status"] == S.MERGE_PENDING and fe.get("finalize_blocked"), fe
    return fake, res


def test_R3_1_a_blocked_finalize_is_loud_and_does_not_keep_the_source_due(env, monkeypatch):
    fake, res = _blocked(env, monkeypatch)
    last = res[-1]
    assert "more than this pass can give" in (last.error or "") and "BLOCKED" in (last.error or "")
    assert last.status != "transient_fail", "a blocked finalize kept the source due like an open repair"
    assert env.chunks(), "the staged pages are kept"


def test_R3_1_a_blocked_finalize_counts_as_held_not_open_and_does_not_cut_the_walk(env, monkeypatch):
    fake, _ = _blocked(env, monkeypatch)
    pa_ = M._Pass(fake, S.Clock(now=env.clock, window_s=45 * 60, source="test"), S.FlowState.load(), set())
    note = pa_.note()
    assert "0 open, 1 held" in note and "BLOCKED" in note, note
    ends = []
    real_work = M._Pass.work

    def spy(self, code, slot_end, *, in_walk, modified):
        if in_walk:
            ends.append((slot_end, self.clock.soft_end))
        return real_work(self, code, slot_end, in_walk=in_walk, modified=modified)
    monkeypatch.setattr(M._Pass, "work", spy)
    fake.modified["DS_CTRL"] = NEW_MOD                                  # a flow the walk must visit
    env.run(fake)
    assert ends and all(e == s for e, s in ends), "a blocked finalize cut the walk at minute 15"


def test_R3_1_a_block_set_by_a_narrow_window_is_lifted_by_a_wide_one(env, monkeypatch):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    real = S.merge_margin_s
    # needs 20 min: more than a 25-min window's slot (25 - margins - 15), less than a 45-min window's
    monkeypatch.setattr(S, "merge_margin_s", lambda rows, stored: 1200.0 if rows == 25 else real(rows, stored))
    for _ in range(4):
        env.run(fake, window_s=25 * 60)
    assert env.state()["flows"]["DS_RP_TD_SEED"].get("finalize_blocked")
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe["status"] == S.COMPLETE and "finalize_blocked" not in fe and env.rows("DS_RP_TD_SEED") == 25
