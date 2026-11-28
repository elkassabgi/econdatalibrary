"""B24 regression tests for the code review of PR #344 (F:/work/econ_finish/b24_reconcile/review_code/VERDICT.md):
one test per path the reviewer's mutants showed no test could fail (M5: RM1-RM34), and one per new behaviour
(H1, H2, M1-M4, L1, L3-L11). Each test plants its own data; a refusal is checked by its MESSAGE, so a later guard
refusing for another reason cannot hide the one under test. Planted data only; no real catalogue, no D1, no .env.
"""
import ast
import json
import os
import shutil
import sqlite3
import time
import types

import pytest

from test_reconcile_catalogue import (  # noqa: F401 - the builder's fixtures, reused unchanged
    GATE, SECRET, FakeD1, _c1, _dry_summary, _grep_outputs, _plan_pipeline, _t0_build, agg, allow_for, cells, corecat,
    cp, env, lic, make_local, q, rc, rows, run_classify, set_t0, snapshot, src, srow)


def _say(capsys):
    return capsys.readouterr().out


# ---- RM1: the lost-page check ----------------------------------------------------------------------------------

def test_rm1_a_row_lost_between_pages_is_a_failure(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i}") for i in range(9)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:0")])
    snap = tmp / "snap" / f"d1_{rc.PRIMARY}.sqlite"
    calls = {"n": 0}

    def lose(sql, rows_):
        if "FROM series WHERE" in sql and "LIMIT" in sql:
            calls["n"] += 1
            if calls["n"] == 2:                                # between page 1 and page 2: a stored row vanishes
                c = sqlite3.connect(str(snap))
                c.execute("DELETE FROM series WHERE series_id = 'a:0'")
                c.commit()
                c.close()
        return rows_
    fake.inject = lose
    with pytest.raises(rc.Failed, match="lost page"):
        snapshot(fake, local, tmp / "snap", page_rows=3)


# ---- RM2: the catalogue changed while it was classified --------------------------------------------------------

def test_rm2_a_catalogue_changed_while_classified_fails(env, monkeypatch, capsys):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:1")])
    snapshot(fake, local, tmp / "snap")
    _c1(tmp, [])
    real = rc.classify

    def classify_then_write(**kw):
        res = real(**kw)
        c = sqlite3.connect(local)
        c.execute("UPDATE series SET title='changed under the read' WHERE series_id='a:1'")
        c.commit()
        c.close()
        return res
    monkeypatch.setattr(rc, "classify", classify_then_write)
    _say(capsys)
    assert rc.main(["classify", "--out", str(tmp / "cls"), "--catalogue", local, "--snapshot-dir", str(tmp / "snap"),
                    "--c1", str(tmp / "c1.tsv")]) == 1
    assert "changed while it was classified" in _say(capsys)


# ---- RM3-RM7: apply's own refusals, each by its message -----------------------------------------------------------

def _apply_argv(tmp, out, reh, sha):
    return ["apply", "--out", str(tmp / "ap"), "--plan", str(out / "plan.json"), "--plan-sha", sha,
            "--fts-measure", str(reh), "--sample-every", "0.01"]


def test_rm3_to_rm7_apply_refuses_each_changed_input_by_its_own_check(env, monkeypatch, capsys):
    tmp, fake = env
    extra, reh = _t0_build(tmp, fake, [srow("c1src:1", unit="old"), srow("a:1")],
                           [srow("c1src:1", unit="new"), srow("a:1")])
    _c1(tmp, [("c1src", "unit")])
    out = tmp / "cls"
    assert rc.main(["classify", "--out", str(out), "--snapshot-dir", str(tmp / "snap"), "--c1", str(tmp / "c1.tsv"),
                    *extra]) == 0
    sha = open(str(out / "plan.json.sha256")).read().strip()
    argv = _apply_argv(tmp, out, reh, sha)
    _say(capsys)

    def refused(why, a=None):
        with cp.writer_lock():
            code = rc.main(a or argv)
        said = _say(capsys)
        assert code == 2 and why in said, (code, said)

    # RM7: a plan classified before the flag (re-pinned so only this check can refuse it)
    plan = json.load(open(out / "plan.json"))
    pre = tmp / "pre"
    pre.mkdir()
    rc.write_json(str(pre / "plan.json"), {**plan, "cut_over_at_classify": False})
    refused("classified before the flag", _apply_argv(tmp, pre, reh, rc.file_sha256(str(pre / "plan.json"))))
    # RM3: a snapshot changed since the plan
    snap = tmp / "snap" / f"d1_{rc.PRIMARY}.sqlite"
    keep = tmp / "snap_keep.sqlite"
    shutil.copyfile(snap, keep)
    c = sqlite3.connect(str(snap))
    c.execute("INSERT OR REPLACE INTO _state VALUES ('tampered', 'true')")
    c.commit()
    c.close()
    refused("D1 snapshot differs from the plan's")
    shutil.copyfile(keep, snap)
    # RM4: the queue copy changed since the plan
    qf = tmp / "queue.txt"
    qf.write_text("a:1\nb:2\n")
    refused("queue copy differs")
    qf.write_text("a:1\n")
    # RM5: the gate changed since the plan
    monkeypatch.setattr(rc, "gate_sha", lambda: "gate-sha-CHANGED")
    refused("gate changed")
    monkeypatch.setattr(rc, "gate_sha", lambda: "gate-sha-1")
    # RM6: too little free space for the rehearsal's journal peak + growth
    big = tmp / "reh_big.json"
    big.write_text(json.dumps({"fts_A": {"journal_peak_bytes": 10 ** 6, "growth_bytes": 0}}))
    real_du = shutil.disk_usage
    monkeypatch.setattr(rc.shutil, "disk_usage", lambda p: types.SimpleNamespace(free=10, total=10, used=0))
    refused("free on the build's drive", _apply_argv(tmp, out, big, sha))
    monkeypatch.setattr(rc.shutil, "disk_usage", real_du)
    # positive control: with every input as planned, apply goes through
    with cp.writer_lock():
        assert rc.main(argv) == 0, _say(capsys)


# ---- RM8 / RM29 / RM30: the allow-list digest checks -------------------------------------------------------------

def _c3_file(tmp, extra_sources=()):
    p = tmp / "c3.tsv"
    p.write_text("source\twriter\tevidence\nsec_edgar\tx\tt\n" + "".join(f"{s}\tx\tt\n" for s in extra_sources))
    return str(p)


def test_rm8_rehearse_refuses_an_allow_list_that_differs_from_the_plan(env, capsys):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1")])
    fake.finish()
    local = make_local(tmp / "copy.db", [srow("a:1")])
    snapshot(fake, local, tmp / "snap")
    _c1(tmp, [])
    out, sha = _plan_pipeline(tmp, local)
    _say(capsys)
    assert rc.main(["rehearse", "--out", str(tmp / "reh"), "--catalogue", local, "--plan", str(out / "plan.json"),
                    "--plan-sha", sha, "--c3", _c3_file(tmp, ["newwriter"])]) == 2
    assert "allow-list differs from the plan's" in _say(capsys)


def test_rm29_classify_refuses_a_snapshot_made_with_another_allow_list(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap", c3n=("sec_edgar", "newwriter"))
    with pytest.raises(rc.Refused, match="different allow-list"):
        run_classify(local, snaps, tmp / "out")


def test_rm30_read_d1_refuses_to_resume_a_snapshot_of_another_allow_list(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:1")])
    snapshot(fake, local, tmp / "snap")
    with pytest.raises(rc.Refused, match="different allow-list"):
        snapshot(fake, local, tmp / "snap", c3n=("sec_edgar", "newwriter"))


# ---- RM9 / RM11 and L8: the search ---------------------------------------------------------------------------

def _search_setup(tmp, fake, d1_rows, local_rows):
    fake.add(rc.PRIMARY, "series", d1_rows)
    fake.finish()
    local = make_local(tmp / "l.db", local_rows)
    snaps, _ = snapshot(fake, local, tmp / "snap")
    res = run_classify(local, snaps, tmp / "out")
    (tmp / "q.txt").write_text('"wages"\n')
    return local, snaps, res


def test_rm9_a_query_over_the_local_ceiling_is_refused(env, monkeypatch, capsys):
    tmp, fake = env
    local, _s, _r = _search_setup(tmp, fake, [srow("a:1", title="wages"), srow("a:2", title="wages")],
                                  [srow("a:1", title="wages"), srow("a:2", title="wages")])
    monkeypatch.setattr(rc, "SEARCH_LOCAL_MAX", 1)
    _say(capsys)
    assert rc.main(["search", "--out", str(tmp / "s"), "--catalogue", local, "--budget-rows", "100000", "--queries",
                    str(tmp / "q.txt"), "--classify-db", str(tmp / "out" / "classify.sqlite")]) == 2
    assert "matches 2 local rows" in _say(capsys)


def test_rm11_a_gated_d1_row_that_matches_is_not_in_the_search_output(env):
    tmp, fake = env
    local, _s, _r = _search_setup(tmp, fake, [srow("a:1", title="wages"), srow("a:2", title="wages d1 only"),
                                              srow("gatedsrc:1", title=f"wages {SECRET}")],
                                  [srow("a:1", title="wages")])
    assert rc.main(["search", "--out", str(tmp / "s"), "--catalogue", local, "--budget-rows", "100000", "--queries",
                    str(tmp / "q.txt"), "--classify-db", str(tmp / "out" / "classify.sqlite")]) == 0
    blob = open(tmp / "s" / "search_compare.json", "rb").read()
    assert b"a:2" in blob                                  # positive control: an allowed D1-only id is listed
    assert b"gatedsrc" not in blob and SECRET.encode() not in blob


def test_l8_a_c3_taken_row_is_an_expected_search_difference_before_apply(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "source", [src("sec_edgar"), src("a")])
    local, snaps, _r = _search_setup(tmp, fake, [srow("a:1", title="wages"), srow("sec_edgar:N", title="wages filer")],
                                     [srow("a:1", title="wages"), srow("sec_edgar:A")])
    assert ("series", "sec_edgar:N", "C3-TAKEN") in rows(tmp / "out")
    con = cp.connect_path(local, write=False)
    try:
        res = rc.search_compare(con, ['"wages"'], str(tmp / "out" / "classify.sqlite"), rc.Budget(10 ** 5),
                                allow_for(local, snaps), snaps)
    finally:
        con.close()
    why = {d["id"]: (d["expected"], d["why"]) for d in res["queries"][0]["diffs"]}
    assert why["sec_edgar:N"][0] and why["sec_edgar:N"][1].startswith("C3-TAKEN") and res["pass"] is True


# ---- RM14: the ROUNDTRIP class -----------------------------------------------------------------------------------

def test_rm14_a_number_the_d1_answer_cannot_carry_is_roundtrip(env, tmp_path, monkeypatch):
    ddl = corecat.SCHEMA.replace("CREATE TABLE IF NOT EXISTS series (", "CREATE TABLE series (").replace(
        "frequency TEXT, unit TEXT,", "frequency TEXT, unit INTEGER,")
    assert "unit INTEGER" in ddl
    fake = FakeD1(tmp_path / "f2", series_ddl=ddl)
    monkeypatch.setattr(rc, "D1_QUERY", fake)
    big = 2 ** 60 + 1
    fake.add(rc.PRIMARY, "series", [srow("s:1", unit=big)])
    fake.finish()
    local = make_local(tmp_path / "l.db", [srow("s:1", unit=str(big))])
    snaps, _ = snapshot(fake, local, tmp_path / "snap")
    res = run_classify(local, snaps, tmp_path / "out")
    assert cells(tmp_path / "out")[("series", "s:1", "unit")][:2] == ("ROUNDTRIP", "local")
    assert res["stats"]["roundtrip"] == 1


# ---- M3 / RM16-RM20: one transaction, the write-time checks, verify V1 and V3 -------------------------------------

def _plan_with(tmp, fake, local_rows, d1_rows, c1_rows=(), deletes=()):
    fake.add(rc.PRIMARY, "series", d1_rows)
    fake.finish()
    local = make_local(tmp / "copy.db", local_rows)
    snapshot(fake, local, tmp / "snap")
    _c1(tmp, list(c1_rows))
    extra = ()
    if deletes:
        (tmp / "del.txt").write_text("".join(f"{d}\n" for d in deletes))
        extra = ("--approved-deletes", str(tmp / "del.txt"))
    out, sha = _plan_pipeline(tmp, local, extra)
    return local, out, sha, json.load(open(out / "plan.json"))


class _FailOn:
    """A connection whose execute raises on the n-th statement starting with `prefix` (isolation_level None)."""

    def __init__(self, con, prefix, nth=1):
        self._con, self._prefix, self._nth, self._seen = con, prefix, nth, 0
        self.isolation_level = None

    def __getattr__(self, n):
        return getattr(self._con, n)

    def execute(self, sql, *a):
        if sql.startswith(self._prefix):
            self._seen += 1
            if self._seen == self._nth:
                raise sqlite3.OperationalError("planted failure")
        return self._con.execute(sql, *a)


@pytest.mark.parametrize("prefix,nth", [("UPDATE", 2), ("DELETE FROM \"series\"", 1),
                                        (rc.FTS_INSERT[:20], 1)])
def test_m3_rm16_a_failure_anywhere_leaves_the_catalogue_unchanged_and_a_rerun_succeeds(env, prefix, nth):
    tmp, fake = env
    local, out, sha, plan = _plan_with(tmp, fake, [srow("c1src:1", unit="o1"), srow("c1src:2", unit="o2"),
                                                   srow("a:del")],
                                       [srow("c1src:1", unit="n1"), srow("c1src:2", unit="n2")],
                                       c1_rows=[("c1src", "unit")], deletes=["a:del"])
    assert len(plan["writes"]["cells"]) == 2 and len(plan["writes"]["deletes"]) == 1
    before = rc.file_sha256(local)
    con = cp.connect_path(local, write=True, isolation_level=None)
    allow = rc._allow_for_plan(con, plan, set(GATE), {"sec_edgar"})
    rec = {}
    with pytest.raises(sqlite3.OperationalError, match="planted"):
        rc.run_plan(_FailOn(con, prefix, nth), local, plan, str(tmp), "t", "rebuild_fts", 0.01, allow, rec)
    con.close()
    assert rec["rolled_back"] is True and rc.file_sha256(local) == before          # byte for byte unchanged
    assert rc.main(["rehearse", "--out", str(tmp / "reh"), "--catalogue", local, "--plan", str(out / "plan.json"),
                    "--plan-sha", sha, "--sample-every", "0.01"]) == 0             # the same pinned plan applies
    c = sqlite3.connect(local)
    assert c.execute("SELECT unit FROM series WHERE series_id='c1src:2'").fetchone()[0] == "n2"
    assert not c.execute("SELECT 1 FROM series WHERE series_id='a:del'").fetchone()
    c.close()


def test_m3_a_failed_verify_rolls_back_inside_the_transaction(env, monkeypatch):
    tmp, fake = env
    local, out, sha, plan = _plan_with(tmp, fake, [srow("c1src:1", unit="o1")], [srow("c1src:1", unit="n1")],
                                       c1_rows=[("c1src", "unit")])
    before = rc.file_sha256(local)
    monkeypatch.setattr(rc, "verify", lambda con, plan, allow: {"ok": False, "failed": {"V4": 1}})
    assert rc.main(["rehearse", "--out", str(tmp / "reh"), "--catalogue", local, "--plan", str(out / "plan.json"),
                    "--plan-sha", sha, "--sample-every", "0.01"]) == 1
    assert rc.file_sha256(local) == before
    assert json.load(open(tmp / "reh" / "rehearse.json"))["fts_A"]["rolled_back"] is True


def test_rm17_an_insert_that_appeared_locally_since_the_plan_is_refused(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "source", [src("sec_edgar"), src("a")])
    local, out, sha, plan = _plan_with(tmp, fake, [srow("sec_edgar:A"), srow("a:1")],
                                       [srow("sec_edgar:A"), srow("sec_edgar:N"), srow("a:1")])
    assert [x[1] for x in plan["writes"]["inserts"]] == ["sec_edgar:N"]
    c = sqlite3.connect(local)
    c.execute("INSERT INTO series (series_id, source_id, license_id) VALUES ('sec_edgar:N', 'sec_edgar', 'lic-open')")
    c.commit()
    c.close()
    con = cp.connect_path(local, write=True, isolation_level=None)
    with pytest.raises(rc.Refused, match="appeared locally"):
        rc.apply_writes(con, plan, str(tmp), "t")
    con.close()


def test_rm18_a_delete_whose_row_changed_since_the_plan_is_refused(env):
    tmp, fake = env
    local, out, sha, plan = _plan_with(tmp, fake, [srow("a:1"), srow("a:del")], [srow("a:1")], deletes=["a:del"])
    c = sqlite3.connect(local)
    c.execute("UPDATE series SET title='edited' WHERE series_id='a:del'")
    c.commit()
    c.close()
    con = cp.connect_path(local, write=True, isolation_level=None)
    with pytest.raises(rc.Refused, match=r"changed since the plan was made \(delete\)"):
        rc.apply_writes(con, plan, str(tmp), "t")
    con.close()
    c = sqlite3.connect(local)
    assert c.execute("SELECT 1 FROM series WHERE series_id='a:del'").fetchone()                 # still there
    c.close()


def test_rm19_rm20_verify_v1_and_v3_catch_writes_that_did_not_happen(env):
    tmp, fake = env
    local, out, sha, plan = _plan_with(tmp, fake, [srow("c1src:1", unit="o1"), srow("a:del")],
                                       [srow("c1src:1", unit="n1")], c1_rows=[("c1src", "unit")], deletes=["a:del"])
    con = cp.connect_path(local, write=True, isolation_level=None)
    allow = rc._allow_for_plan(con, plan, set(GATE), {"sec_edgar"})
    v = rc.verify(con, plan, allow)                        # nothing was applied
    con.close()
    assert v["ok"] is False and v["failed"].get("V1") == 1 and v["failed"].get("V3") == 1


# ---- RM21 / RM23 and H2: what read-d1 stores --------------------------------------------------------------------

def test_rm21_a_d1_source_row_naming_the_gate_is_stored_redacted(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:1")])
    fake.add(rc.PRIMARY, "source", [src("alpha", name=f"about gatedsrc {SECRET}", homepage="h-alpha")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    hits, _ = _grep_outputs([tmp / "snap"], ["gatedsrc", SECRET])
    assert hits == []
    c = sqlite3.connect(snaps[rc.PRIMARY])
    assert c.execute("SELECT homepage FROM source WHERE source_id='alpha'").fetchone()[0] == "h-alpha"   # stored
    c.close()


def test_rm23_a_series_row_whose_source_id_is_gated_is_not_admitted_by_its_prefix(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:1"), srow("alpha:9", source_id="gatedsrc")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    c = sqlite3.connect(snaps[rc.PRIMARY])
    assert [r[0] for r in c.execute("SELECT series_id FROM series ORDER BY 1")] == ["alpha:1"]
    c.close()


def test_h2_a_hosted_title_holding_a_gated_token_is_equal_not_a_v7_difference(env):
    tmp, fake = env
    rowsx = [srow("sec_edgar:X", title="Filer GATEDSRC Holdings"), srow("a:1")]
    fake.add(rc.PRIMARY, "series", rowsx)
    fake.finish()
    local = make_local(tmp / "l.db", rowsx)
    snaps, _ = snapshot(fake, local, tmp / "snap")
    hits, _ = _grep_outputs([tmp / "snap"], ["Filer GATEDSRC"])
    assert hits == []                                       # the text is not stored ...
    res = run_classify(local, snaps, tmp / "out")
    assert res["stats"]["c6_cells"] == 0 and res["stats"]["equal_withheld"] >= 1   # ... and still compares equal


def test_h2_v7_fails_when_read_d1_dropped_a_sec_edgar_row(env):
    tmp, fake = env
    extra, _reh = _t0_build(tmp, fake, [srow("a:1")], [srow("a:1"), srow("sec_edgar:Z", source_id="gatedsrc")])
    _c1(tmp, [])
    st = rc.snapshot_state(str(tmp / "snap" / f"d1_{rc.PRIMARY}.sqlite"))
    assert st.get("c6_dropped_on_receipt") is True
    assert rc.main(["classify", "--out", str(tmp / "cls"), "--snapshot-dir", str(tmp / "snap"), "--c1",
                    str(tmp / "c1.tsv"), *extra]) == 1


# ---- RM25: rehearse after T0 ---------------------------------------------------------------------------------------

def test_rm25_rehearse_is_refused_after_t0(env, capsys):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1")])
    fake.finish()
    local = make_local(tmp / "copy.db", [srow("a:1")])
    snapshot(fake, local, tmp / "snap")
    _c1(tmp, [])
    out, sha = _plan_pipeline(tmp, local)
    before = rc.file_sha256(local)
    set_t0()
    _say(capsys)
    assert rc.main(["rehearse", "--out", str(tmp / "reh"), "--catalogue", local, "--plan", str(out / "plan.json"),
                    "--plan-sha", sha]) == 2
    assert "before-T0 step" in _say(capsys) and rc.file_sha256(local) == before


# ---- RM28: C5 needs D1's own column statistics -----------------------------------------------------------------

def test_rm28_a_cell_without_column_statistics_is_not_c5(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("s:1", unit="d1 unit")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("s:1", unit="local unit")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    c = sqlite3.connect(snaps[rc.PRIMARY])
    c.execute("DELETE FROM _colstats")
    c.commit()
    c.close()
    run_classify(local, snaps, tmp / "out")
    assert cells(tmp / "out")[("series", "s:1", "unit")][:2] == ("DEFAULT", "local")


# ---- RM31: copy's post-copy checks -------------------------------------------------------------------------------

def test_rm31_copy_fails_when_the_copy_does_not_hash_equal(env, monkeypatch, capsys):
    tmp, fake = env
    src_db = make_local(tmp / "src.db", [srow("a:1")])
    real = rc.file_sha256
    dst = str(tmp / "dst.db")
    monkeypatch.setattr(rc, "file_sha256", lambda p, *a: "0" * 64 if os.path.abspath(p) == os.path.abspath(dst)
                        else real(p, *a))
    _say(capsys)
    assert rc.main(["copy", "--out", str(tmp / "c"), "--from", src_db, "--to", dst]) == 1
    assert "copy FAILED" in _say(capsys)


# ---- RM32: classify's exit code on item-17 ids on both sides -------------------------------------------------------

def test_rm32_classify_exits_1_when_an_item17_id_is_on_both_sides(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1"), srow("a:del")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:1"), srow("a:del")])
    snapshot(fake, local, tmp / "snap")
    _c1(tmp, [])
    (tmp / "del.txt").write_text("a:del\n")
    assert rc.main(["classify", "--out", str(tmp / "cls"), "--catalogue", local, "--snapshot-dir", str(tmp / "snap"),
                    "--c1", str(tmp / "c1.tsv"), "--approved-deletes", str(tmp / "del.txt")]) == 1


def test_m2_an_approved_id_misplaced_in_the_other_shard_is_on_both_sides(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("noaa:del")])      # noaa's home is the climate shard
    fake.finish()
    local = make_local(tmp / "l.db", [srow("noaa:del"), srow("a:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    res = run_classify(local, snaps, tmp / "out", deletes={"noaa:del"})
    assert res["stats"]["del_both"] == 1 and not res["plan"]["deletes"]


# ---- RM33 / RM34: a plain open of the catalogue under the tool's real name `path` -----------------------------

def _catalogue_names(tree):
    """Per function: the names passed as the first argument to _catalogue_ro / _catalogue_rw."""
    out = {}
    for f in ast.walk(tree):
        if isinstance(f, ast.FunctionDef):
            names = {n.args[0].id for n in ast.walk(f) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                     and n.func.id in ("_catalogue_ro", "_catalogue_rw") and n.args
                     and isinstance(n.args[0], ast.Name)}
            if names:
                out[f.name] = (f, names)
    return out


def _plain_opens_of_catalogue_names(tree):
    bad = []
    for fname, (f, names) in _catalogue_names(tree).items():
        for n in ast.walk(f):
            if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "connect"
                    and ast.unparse(n.func.value) == "sqlite3" and n.args
                    and {x.id for x in ast.walk(n.args[0]) if isinstance(x, ast.Name)} & names):
                bad.append((fname, n.lineno))
    return bad


def test_rm33_rm34_no_plain_open_of_a_name_the_function_opens_as_the_catalogue():
    with open(rc.__file__, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    found = _catalogue_names(tree)
    assert {"cmd_rehearse", "cmd_apply", "classify"} <= set(found)                 # the rule sees `path` too
    assert _plain_opens_of_catalogue_names(tree) == []
    nl = chr(10)
    planted = ast.parse("import sqlite3" + nl + "def cmd_apply(a):" + nl + "    path = a.x" + nl
                        + "    con = _catalogue_rw(path)" + nl + "    c = sqlite3.connect(path)" + nl)
    assert _plain_opens_of_catalogue_names(planted) == [("cmd_apply", 5)]        # positive control


# ---- H1: the snapshot's read times and the CUTOVER side ---------------------------------------------------------

def test_h1_a_snapshot_records_when_and_on_which_side_of_the_flag_it_was_read(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    st = rc.snapshot_state(snaps[rc.PRIMARY])
    assert st["read_complete"] is True and st["cut_over_at_start"] is False and st["cut_over_at_end"] is False
    assert st["read_started_utc"] <= st["read_ended_utc"]
    allow = rc._allow_from_catalogue(local, set(GATE), {"sec_edgar"})
    set_t0()
    with pytest.raises(rc.Refused, match="other side of the CUTOVER flag"):
        rc.read_db_main(rc.Budget(10 ** 5), rc.PRIMARY, snaps[rc.PRIMARY], 3, allow, None)
    with pytest.raises(rc.Refused, match="wholly AFTER the CUTOVER flag"):
        rc.check_snapshots_for_use(snaps)


def test_h1_an_incomplete_snapshot_is_refused_by_classify(env, capsys):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i}") for i in range(9)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:0")])
    snaps, _ = snapshot(fake, local, tmp / "snap", page_rows=3)
    c = sqlite3.connect(snaps[rc.CLIMATE])                 # as if read-d1 had stopped before the end
    c.execute("DELETE FROM _state WHERE k = 'read_complete'")
    c.commit()
    c.close()
    _c1(tmp, [])
    _say(capsys)
    assert rc.main(["classify", "--out", str(tmp / "cls"), "--catalogue", local, "--snapshot-dir", str(tmp / "snap"),
                    "--c1", str(tmp / "c1.tsv")]) == 2
    assert "not complete" in _say(capsys)


# ---- M4 / L10: page 1 is always measured -------------------------------------------------------------------------

def test_m4_without_a_probe_page_one_is_measured_and_the_total_reprojected(env, capsys):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i:03d}") for i in range(30)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:000")])
    fake.e = 400                                           # D1 reads far more per page than the e = 1 guess
    _say(capsys)
    code = rc.main(["read-d1", "--out", str(tmp / "r"), "--catalogue", local, "--budget-rows", "9000",
                    "--page-rows", "3"])
    said = _say(capsys)
    assert code == 3 and "measured e makes the projection" in said, said
    assert len([c for c in fake.calls if "FROM series WHERE" in c[1]]) == 1     # stopped before page 2


def test_l10_r2_l2_a_resume_after_a_drift_stop_rechecks_before_reading(env):
    # round 2 L2 supersedes round 1 L10: the resume re-checks the drift BEFORE any page (no page is read twice and
    # none is read with a probe that is known to be wrong); a fresh probe with the measured e reads on
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i}") for i in range(9)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:0")])
    fake.e = 5
    with pytest.raises(rc.Stop, match="differs from the probe"):
        snapshot(fake, local, tmp / "s", page_rows=3, probe_e=1)
    fake.calls.clear()
    with pytest.raises(rc.Stop, match="still differs from the measured e"):
        snapshot(fake, local, tmp / "s", page_rows=3, probe_e=1)   # the same wrong probe: refused before a page
    assert not [c for c in fake.calls if "FROM series WHERE" in c[1]]
    snapshot(fake, local, tmp / "s", page_rows=3, probe_e=5)       # re-probed: reads on
    st = rc.snapshot_state(str(tmp / "s" / f"d1_{rc.PRIMARY}.sqlite"))
    assert st["read_complete"] is True and st["e_measured"] == 5


def test_r2_l2_a_resume_reprojects_the_whole_read_and_refuses_above_the_budget(env, capsys):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i:03d}") for i in range(30)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:000")])
    fake.e = 400
    argv = ["read-d1", "--out", str(tmp / "r"), "--catalogue", local, "--budget-rows", "9000", "--page-rows", "3"]
    assert rc.main(argv) == 3                              # page 1 measured e 400: STOP (M4)
    fake.calls.clear()
    _say(capsys)
    assert rc.main(argv) == 2                              # the resume re-projects with the MEASURED e: refused
    assert "re-projects at" in _say(capsys)
    assert not [c for c in fake.calls if "FROM series WHERE" in c[1]]


# ---- L1: errors on a page ------------------------------------------------------------------------------------------

def test_l1_an_unknown_d1_error_stops_at_once_and_is_charged(env, monkeypatch):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i}") for i in range(9)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:0")])
    real = fake.__call__
    asked = []

    def broken(db, sql, params):
        if "FROM series WHERE" in sql:
            asked.append(params[1])
            raise RuntimeError("D1 econ-catalog: no such column: x")
        return real(db, sql, params)
    monkeypatch.setattr(rc, "D1_QUERY", broken)
    out = tmp / "s"
    with pytest.raises(RuntimeError):
        snapshot(fake, local, out, page_rows=200)
    assert asked == [200]                                  # not halved: an unknown error is not an answer size
    led = sqlite3.connect(str(out / "budget.sqlite"))
    assert led.execute("SELECT COUNT(*) FROM spent WHERE what LIKE '%(failed;%'").fetchone()[0] == 1   # L11
    led.close()


# ---- L3 / L5 / L7 / L9 / L4 ------------------------------------------------------------------------------------------

def test_l3_classify_after_t0_needs_the_dry_runs_summary(env, capsys):
    tmp, fake = env
    extra, _reh = _t0_build(tmp, fake, [srow("a:1")], [srow("a:1")])
    _c1(tmp, [])
    i = extra.index("--compare-to")
    _say(capsys)
    assert rc.main(["classify", "--out", str(tmp / "cls"), "--snapshot-dir", str(tmp / "snap"), "--c1",
                    str(tmp / "c1.tsv"), *(extra[:i] + extra[i + 2:])]) == 2
    assert "--compare-to" in _say(capsys)


def test_l5_the_compare_to_summarys_classify_db_is_guarded(env, capsys):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:1")])
    snapshot(fake, local, tmp / "snap")
    _c1(tmp, [])
    dry = tmp / "dry.json"
    dry.write_text(json.dumps({"per_source": {}, "classify_db": os.path.join(cp.LIVE_STORE_ROOT, "x.sqlite")}))
    _say(capsys)
    assert rc.main(["classify", "--out", str(tmp / "cls"), "--catalogue", local, "--snapshot-dir", str(tmp / "snap"),
                    "--c1", str(tmp / "c1.tsv"), "--compare-to", str(dry)]) == 2
    assert "E: repository" in _say(capsys)


def test_l7_source_attribution_and_terms_url_are_held_licence_cells(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:1")])
    fake.add(rc.PRIMARY, "source", [src("alpha", attribution="D1 attribution", terms_url="https://d1/terms")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    res = run_classify(local, snaps, tmp / "out")
    c = cells(tmp / "out")
    assert c[("source", "alpha", "attribution")][:2] == ("LIC-HELD", "held")
    assert c[("source", "alpha", "terms_url")][:2] == ("LIC-HELD", "held")
    assert not res["plan"]["cells"]
    con = cp.connect_path(local, write=True, isolation_level=None)
    try:
        plan = {"counts": {}, "writes": {"cells": [["source", "alpha", "attribution", "text", "a", "b", "C1"]],
                                         "inserts": [], "deletes": []}}
        with pytest.raises(rc.Refused, match="licence cell"):
            rc.preflight_writes(con, plan, apply=False)
    finally:
        con.close()


def test_l9_c7_sizes_count_allowed_ids_only(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    res = run_classify(local, snaps, tmp / "out")
    c7 = rc.C7()
    c7.add_ids(["alpha:1", "gatedsrc:1", "gatedsrc:2"], "queue")
    s = rc.summarise(str(tmp / "out"), res, c7, {"gate_sha256": "g"}, None, allow_for(local, snaps), {})
    assert s["c7_sizes"]["ids"] == 1


def test_l4_rehearse_records_the_s11_time_pieces(env, monkeypatch):
    tmp, fake = env
    local, out, sha, _plan = _plan_with(tmp, fake, [srow("c1src:1", unit="o1")], [srow("c1src:1", unit="n1")],
                                        c1_rows=[("c1src", "unit")])
    real = rc.file_sha256

    def slow_for_snapshots(p, *a):
        if os.path.basename(str(p)).startswith("d1_"):
            time.sleep(0.1)                                # a measurable snapshot hash (the real ones are GBs)
        return real(p, *a)
    monkeypatch.setattr(rc, "file_sha256", slow_for_snapshots)
    assert rc.main(["rehearse", "--out", str(tmp / "reh"), "--catalogue", local, "--plan", str(out / "plan.json"),
                    "--plan-sha", sha, "--sample-every", "0.01"]) == 0
    rec = json.load(open(tmp / "reh" / "rehearse.json"))
    assert set(rec["s11_pieces_s"]) >= {"snapshot_hash", "hash", "writes", "fts", "verify", "v4", "v5_content",
                                        "commit"}                  # round 2 L7: the snapshot hashes are in it
    assert rec["snapshot_hash_s"] >= 0.2
    assert abs(rec["s11_lock_held_estimate_s"]
               - (rec["snapshot_hash_s"] + rec["hash_s"] + rec["fts_A"]["elapsed_s"])) < 0.01
    assert rec["fts_A"]["one_transaction"] is True


# ---- round 2 C1: withheld cells decided AT RECEIPT; no salt, no digest; the snapshot bound to its catalogue -------

def _marker_hits(dirs):
    """(raw marker hits, hex ($blob) marker hits) in every file under dirs."""
    raw, hexed = [], []
    for d in dirs:
        for base, _ds, fs in os.walk(d):
            for name in fs:
                b = open(os.path.join(base, name), "rb").read()
                if rc.WITHHELD in b:
                    raw.append(name)
                if rc.WITHHELD.hex().encode() in b or rc.WITHHELD.hex().upper().encode() in b:
                    hexed.append(name)
    return raw, hexed


def test_c1_withheld_cells_are_decided_at_receipt_and_store_no_digest(env):
    tmp, fake = env
    d1_rows = [srow("sec_edgar:X", title="Filer GATEDSRC Holdings"),             # equal on both sides
               srow("sec_edgar:Y", title="GATEDSRC new name"),                    # differs
               srow("alpha:1", metadata='{"note":"gatedsrc x","k":1}')]           # NORM-equal (key order)
    fake.add(rc.PRIMARY, "series", d1_rows)
    fake.finish()
    local = make_local(tmp / "l.db", [srow("sec_edgar:X", title="Filer GATEDSRC Holdings"),
                                      srow("sec_edgar:Y", title="GATEDSRC old name"),
                                      srow("alpha:1", metadata='{"k":1,"note":"gatedsrc x"}')])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    c = sqlite3.connect(snaps[rc.PRIMARY])
    got = {(k, col): v for k in ("sec_edgar:X", "sec_edgar:Y", "alpha:1") for col, v in
           zip(("title", "metadata"), c.execute("SELECT title, metadata FROM series WHERE series_id=?", (k,)).fetchone())}
    keys = {r[0] for r in c.execute("SELECT k FROM _state")}
    c.close()
    assert got[("sec_edgar:X", "title")] == rc.WITHHELD_EQUAL
    assert got[("sec_edgar:Y", "title")] == rc.WITHHELD_DIFFERS
    assert got[("alpha:1", "metadata")] == rc.WITHHELD_EQUAL                     # normalisation kept
    assert all(len(v) == len(rc.WITHHELD) + 1 for v in got.values() if isinstance(v, bytes))   # no digest
    assert "withheld_salt" not in keys and rc.snapshot_state(snaps[rc.PRIMARY])["catalogue_sha256"] == \
        rc.file_sha256(local)
    hits, _ = _grep_outputs([tmp / "snap"], ["Filer GATEDSRC", "GATEDSRC new", "gatedsrc x"])
    assert hits == []
    res = run_classify(local, snaps, tmp / "out")
    assert res["stats"]["equal_withheld"] == 2 and res["stats"]["c6_cells"] == 1
    rc.summarise(str(tmp / "out"), res, rc.C7(), {"gate_sha256": "g"}, None, allow_for(local, snaps),
                 rc.dropped_flags(snaps), rc.redacted_flags(snaps))
    raw_out, hex_out = _marker_hits([tmp / "out"])
    assert raw_out == [] and hex_out == []                 # no output carries a marker, raw or as {"$blob": hex}
    raw_snap, _ = _marker_hits([tmp / "snap"])
    assert raw_snap                                        # positive control: the snapshot does hold the markers


def test_c1_binding_classify_refuses_a_snapshot_read_against_another_catalogue(env, capsys):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:1")])
    snapshot(fake, local, tmp / "snap")
    _c1(tmp, [])
    argv = ["classify", "--out", str(tmp / "cls"), "--catalogue", local, "--snapshot-dir", str(tmp / "snap"),
            "--c1", str(tmp / "c1.tsv")]
    assert rc.main(argv) == 0                              # positive control: the same catalogue
    c = sqlite3.connect(local)
    c.execute("UPDATE series SET title='changed after the read' WHERE series_id='a:1'")
    c.commit()
    c.close()
    _say(capsys)
    assert rc.main(argv) == 2 and "read against another catalogue" in _say(capsys)


def test_c1_a_resume_refuses_a_changed_catalogue(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("a:1")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    allow = rc._allow_from_catalogue(local, set(GATE), {"sec_edgar"})
    with pytest.raises(rc.Refused, match="local catalogue changed since this snapshot began"):
        rc.read_db_main(rc.Budget(10 ** 5), rc.PRIMARY, snaps[rc.PRIMARY], 3, allow, None,
                        catalogue_sha="0" * 64)


# ---- round 2 L1, L3, L4, L5, L6 --------------------------------------------------------------------------------------

def test_r2_l1_the_http_status_decides_before_the_text():
    k = rc._d1_error_kind
    assert k(RuntimeError("D1 econ-catalog: HTTP 429 b'rate limit exceeded'")) == "transient"
    assert k(RuntimeError("D1 econ-catalog: HTTP 503 b'payload service'")) == "transient"
    assert k(RuntimeError("D1 econ-catalog: HTTP 413 b'payload too large'")) == "size"
    assert k(RuntimeError("D1 econ-catalog: HTTP 400 b'too large'")) == "other"
    assert k(RuntimeError('D1 econ-catalog: [{"code": 7500, "message": "response too large"}]')) == "size"
    assert k(RuntimeError("D1 econ-catalog: no such column: x")) == "other"
    assert k(OSError("timed out")) == "transient"


def test_r2_l1_a_rate_limit_exceeded_is_retried_not_halved(env, monkeypatch):
    tmp, fake = env
    monkeypatch.setattr(rc, "RETRY_SLEEPS", (0.0, 0.0, 0.0))
    fake.add(rc.PRIMARY, "series", [srow(f"a:{i:03d}") for i in range(300)])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("a:000")])
    real = fake.__call__
    state = {"n": 0}

    def limited(db, sql, params):
        if "FROM series WHERE" in sql:
            state["n"] += 1
            if state["n"] == 1:
                raise RuntimeError("D1 econ-catalog: HTTP 429 b'rate limit exceeded'")
        return real(db, sql, params)
    monkeypatch.setattr(rc, "D1_QUERY", limited)
    snaps, _ = snapshot(fake, local, tmp / "snap", page_rows=200)
    c = sqlite3.connect(snaps[rc.PRIMARY])
    asked = [r[0] for r in c.execute("SELECT page_rows_asked FROM _pages WHERE tbl='series'")]
    c.close()
    assert asked and max(asked) == 200


def test_r2_l3_the_count_check_sums_only_sources_whose_home_is_that_database(env, monkeypatch):
    tmp, fake = env
    monkeypatch.setattr(rc, "COUNT_TOL_ROWS", 0)
    monkeypatch.setattr(rc, "COUNT_TOL_FRAC", 0.0)
    fake.add(rc.PRIMARY, "series", [srow("a:1")])
    fake.add(rc.CLIMATE, "series", [srow("noaa:1")])
    fake.finish()
    fake.raw(rc.PRIMARY, "INSERT INTO source_counts VALUES ('noaa', 5)")    # a shard source's row in the primary
    local = make_local(tmp / "l.db", [srow("a:1"), srow("noaa:1")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    chk = rc.snapshot_count_check(snaps)
    assert chk[rc.PRIMARY]["ok"] is True and chk[rc.PRIMARY]["source_counts"] == 1
    assert chk[rc.CLIMATE]["ok"] is True and chk[rc.CLIMATE]["source_counts"] == 1


def test_r2_l4_dropped_rows_and_redacted_hosted_text_are_two_lines(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:1", title="about gatedsrc")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:1", title="about gatedsrc")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    res = run_classify(local, snaps, tmp / "out")
    s = rc.summarise(str(tmp / "out"), res, rc.C7(), {"gate_sha256": "g"}, None, allow_for(local, snaps),
                     rc.dropped_flags(snaps), rc.redacted_flags(snaps))
    assert s["outside_the_allow_list_dropped"][rc.PRIMARY] == "NO"
    assert s["hosted_text_redacted"][rc.PRIMARY] == "YES"
    md = open(tmp / "out" / "summary.md", encoding="utf-8").read()
    assert "HOSTED rows whose non-key text named a gated id" in md and "D1 held ROWS outside" in md


def test_r2_l5_a_d1_only_licence_row_with_withheld_text_is_still_held_by_its_key(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:3", license_id="lic-z")])
    fake.add(rc.PRIMARY, "license", [lic("lic-open"), lic("lic-z", name="terms of the gatedsrc programme")])
    fake.finish()
    local = make_local(tmp / "l.db", [srow("alpha:3", license_id="lic-z")], [src("alpha")], [lic("lic-open")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    out = tmp / "out"
    run_classify(local, snaps, out)
    held = q(out, "SELECT flags, row_json FROM rows WHERE tbl='license' AND key='lic-z' AND cls='LIC-HELD-ROW'")
    assert len(held) == 1
    flags, row_json = held[0]
    assert json.loads(flags)["text_withheld"] is True and row_json is None
    raw_out, hex_out = _marker_hits([out])
    assert raw_out == [] and hex_out == []


def test_r2_l6_guard_out_and_an_own_list_test_the_e_string_before_any_resolve(env, monkeypatch):
    seen = []
    for mod, name in ((os, "stat"), (os.path, "exists"), (os.path, "realpath"), (os.path, "isfile"),
                      (os.path, "samefile")):
        real = getattr(mod, name)

        def spy(p, *a, _real=real, **k):
            seen.append(str(p))
            return _real(p, *a, **k)
        monkeypatch.setattr(mod, name, spy)
    with pytest.raises(rc.Refused):
        rc.guard_out(os.path.join(cp.LIVE_STORE_ROOT, "out"))
    with pytest.raises(rc.Refused):
        rc.guard_input(os.path.join(cp.LIVE_STORE_ROOT, "tools", "selfhost", "reconcile", "c1_columns.tsv"), "--c1",
                       own_list=True)
    assert not [p for p in seen if p.lower().startswith("e:")], seen
