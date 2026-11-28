"""The code reviewer's tests rv1-rv8 for B24 (review of PR #344 at c1d1687df, F:/work/econ_finish/b24_reconcile/
review_code/VERDICT.md), copied into the branch as REGRESSION tests (coordinator 2026-11-28). Each failed at that
head. Where a coordinator ruling or another finding changes what the right behaviour is, the test is adapted and the
change is said in its comment:
- rv1, rv7: after T0 classify also needs --compare-to (finding L3), so the T0 inputs carry one; rv1 also checks
  that the refusal is the H1 one, and has a post-flag positive control.
- rv3: the fix READS ON after a short page (a confirming PK read finds the cut rows), so the regression is "nothing
  is lost and the short page is recorded"; a truncation the end check cannot see is the count check's job (rv3b).
- rv5: the coordinator RULED "a hosted key is not a mention" (H2), so a hosted source whose id carries a gated
  token is reconciled and may be named; the mirror asserts that, and that no GATED row or text is printed.
Planted data only; no real catalogue, no D1, no .env.
"""
import json
import os
import sqlite3

from test_reconcile_catalogue import (  # noqa: F401 - the builder's fixtures, reused unchanged
    GATE, SECRET, FakeD1, _dry_summary, allow_for, cells, cp, env, lic, make_local, rc, rows, run_classify, set_t0,
    snapshot, src, srow, _c1, _grep_outputs)


def _t0_inputs(tmp):
    qf = tmp / "queue.txt"
    qf.write_text("a:1\n")
    s4c = tmp / "s4c.json"
    s4c.write_text(json.dumps({"sha256": rc.file_sha256(str(qf)), "size": qf.stat().st_size, "lines": 1}))
    runs = tmp / "runs.tsv"
    runs.write_text("run_key\twriter\tutc\tsources\treceipt\tgrain\tbooking\n")
    reh = tmp / "reh.json"
    reh.write_text(json.dumps({"fts_A": {"journal_peak_bytes": 0, "growth_bytes": 0}}))
    return ["--c7-queue", str(qf), "--s4c-receipt", str(s4c), "--c7-runs", str(runs),
            "--compare-to", _dry_summary(tmp)], reh


# RV1 (S11 "a fresh D1 read"; DESIGN 4.2): a snapshot read BEFORE the flag must not feed an S11 plan.
def test_rv1_a_snapshot_read_before_the_flag_is_refused_at_s11(env, capsys):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("c1src:1", unit="new"), srow("a:1")])
    fake.finish()
    make_local(cp.BUILD_PATH, [srow("c1src:1", unit="old"), srow("a:1")])
    snapshot(fake, cp.BUILD_PATH, tmp / "snap")          # read-d1 BEFORE T0 (the dry run's snapshot)
    set_t0()                                               # ... then the flag
    extra, reh = _t0_inputs(tmp)
    _c1(tmp, [("c1src", "unit")])
    out = tmp / "cls"
    capsys.readouterr()
    code = rc.main(["classify", "--out", str(out), "--snapshot-dir", str(tmp / "snap"), "--c1", str(tmp / "c1.tsv"),
                    *extra])
    if code == 0:                                          # the defect: show apply also takes it
        sha = open(str(out / "plan.json.sha256")).read().strip()
        with cp.writer_lock():
            ap = rc.main(["apply", "--out", str(tmp / "ap"), "--plan", str(out / "plan.json"), "--plan-sha", sha,
                          "--fts-measure", str(reh), "--sample-every", "0.01"])
        assert ap != 0, "apply wrote the build from a D1 snapshot read BEFORE the flag"
    assert code == 2, "classify after T0 accepted a D1 snapshot read before the flag"
    assert "wholly AFTER the CUTOVER flag" in capsys.readouterr().out
    # positive control: the same inputs with a snapshot read AFTER the flag are accepted
    snapshot(fake, cp.BUILD_PATH, tmp / "snap2")
    assert rc.main(["classify", "--out", str(tmp / "cls2"), "--snapshot-dir", str(tmp / "snap2"), "--c1",
                    str(tmp / "c1.tsv"), *extra]) == 0


# RV2 (R492 / DESIGN 5): a probe run in the same --out must not switch read-d1's projection refusal off.
def test_rv2_probe_in_the_same_out_does_not_skip_the_projection(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow(f"alpha:{i:03d}") for i in range(60)])
    fake.finish()
    local = make_local(tmp / "local.db", [srow(f"alpha:{i:03d}") for i in range(60)])
    out = tmp / "run"
    assert rc.main(["probe", "--out", str(out), "--catalogue", local, "--budget-rows", "100000",
                    "--page-rows", "5"]) == 0
    spent = rc.Budget(100000, str(out / "budget.sqlite")).spent
    code = rc.main(["read-d1", "--out", str(out), "--catalogue", local, "--budget-rows", str(spent + 12),
                    "--page-rows", "5", "--probe", str(out / "probe.json")])
    assert code == 2, f"read-d1 was not refused by its projection (rc {code}); the probe's statements made it 'resume'"


# RV3, adapted (see the module docstring): a SHORT page in the middle of a database is not its end - the read goes
# on, nothing is lost, and the snapshot records that a short page was seen.
def test_rv3_a_short_page_mid_database_is_not_a_silent_end(env):
    tmp, fake = env
    d = [srow(f"alpha:{i:03d}") for i in range(12)]
    fake.add(rc.PRIMARY, "series", d)
    fake.finish()
    local = make_local(tmp / "local.db", d)
    state = {"n": 0}

    def cap(sql, rows_):
        if "FROM series WHERE" in sql and "MATCH" not in sql and "LIMIT" in sql:
            state["n"] += 1
            if state["n"] == 2:
                return rows_[:-1]                          # page 2 answered one row short (a capped answer)
        return rows_
    fake.inject = cap
    code = rc.main(["read-d1", "--out", str(tmp / "r"), "--catalogue", local, "--budget-rows", "100000",
                    "--page-rows", "3"])
    con = sqlite3.connect(str(tmp / "r" / f"d1_{rc.PRIMARY}.sqlite"))
    stored = sorted(r[0] for r in con.execute("SELECT series_id FROM series"))
    short = con.execute("SELECT v FROM _state WHERE k='short_page_seen'").fetchone()
    con.close()
    assert code == 0 and stored == sorted(r["series_id"] for r in d), (code, len(stored))
    assert short and json.loads(short[0]) is True


# RV3b: a truncation the end check cannot see (D1 itself answers short for every read after a key) is caught by
# the stored-vs-source_counts check, which FAILS classify (the tolerance is set to 0 for 12 planted rows).
def test_rv3b_a_truncated_snapshot_fails_the_count_check(env, monkeypatch, capsys):
    tmp, fake = env
    d = [srow(f"alpha:{i:03d}") for i in range(12)]
    fake.add(rc.PRIMARY, "series", d)
    fake.finish()
    local = make_local(tmp / "local.db", d)
    monkeypatch.setattr(rc, "COUNT_TOL_ROWS", 0)
    monkeypatch.setattr(rc, "COUNT_TOL_FRAC", 0.0)
    assert rc.main(["read-d1", "--out", str(tmp / "ok"), "--catalogue", local, "--budget-rows", "100000",
                    "--page-rows", "3"]) == 0
    _c1(tmp, [])
    assert rc.main(["classify", "--out", str(tmp / "cls_ok"), "--catalogue", local, "--snapshot-dir",
                    str(tmp / "ok"), "--c1", str(tmp / "c1.tsv")]) == 0          # positive control
    fake.inject = lambda sql, rows_: [r for r in rows_ if not ("FROM series WHERE" in sql and r.get(
        "series_id", "") > "alpha:004")]                  # D1 "loses" every row after alpha:004
    assert rc.main(["read-d1", "--out", str(tmp / "cut"), "--catalogue", local, "--budget-rows", "100000",
                    "--page-rows", "3"]) == 0
    capsys.readouterr()
    assert rc.main(["classify", "--out", str(tmp / "cls_cut"), "--catalogue", local, "--snapshot-dir",
                    str(tmp / "cut"), "--c1", str(tmp / "c1.tsv")]) == 1
    assert "fall short of D1's source_counts" in capsys.readouterr().out


# RV4 (ruling l, review M9): an approved item-17 id still present locally is a FAILED check even when its source is
# outside the allow-list.
def test_rv4_an_approved_delete_outside_the_allow_list_is_not_silently_skipped(env):
    tmp, fake = env
    L = [srow("alpha:1"), srow("gatedsrc:1")]
    fake.add(rc.PRIMARY, "series", [srow("alpha:1"), srow("gatedsrc:1")])
    fake.finish()
    local = make_local(tmp / "local.db", L)
    snaps, _ = snapshot(fake, local, tmp / "snap")
    res = run_classify(local, snaps, tmp / "cls", deletes={"gatedsrc:1"})
    st = res["stats"]
    assert st["del_both"] or st["del_withheld"], "an approved delete id still on both sides was skipped silently"
    assert st["del_outside_allow"] is True
    hits, _ = _grep_outputs([tmp / "cls"], ["gatedsrc:1"])
    assert hits == []                                      # a YES/NO only: the id is not printed


# RV5, MIRRORED (coordinator ruling on H2: a hosted key is not a mention). A HOSTED source whose id carries a gated
# token is RECONCILED like any other (its differing title is a cell), and nothing of the GATED source is printed.
def test_rv5_a_hosted_source_whose_id_carries_a_gated_token_is_reconciled(env):
    tmp, fake = env
    L = [srow("gatedsrc_v2:1", title="local title"), srow("alpha:1"), srow("gatedsrc:9", title=SECRET)]
    fake.add(rc.PRIMARY, "series", [srow("gatedsrc_v2:1", title="x"), srow("alpha:1"),
                                    srow("gatedsrc:9", title=SECRET + " d1")])
    fake.add(rc.PRIMARY, "source", [src("gatedsrc_v2", name="GATEDSRC_V2", homepage="h2"), src("alpha"),
                                    src("gatedsrc", name=f"Gated {SECRET}")])
    fake.finish()
    local = make_local(tmp / "local.db", L)
    snaps, _ = snapshot(fake, local, tmp / "snap")
    out = tmp / "cls"
    res = run_classify(local, snaps, out)
    rc.summarise(str(out), res, rc.C7(), {"gate_sha256": "g"}, None, allow_for(local, snaps), {})
    assert cells(out)[("series", "gatedsrc_v2:1", "title")][0] == "DEFAULT"      # hosted: reconciled, not skipped
    assert ("source", "gatedsrc_v2", "homepage") in cells(out)
    blob = b""
    for f in os.listdir(str(out)):
        with open(os.path.join(str(out), f), "rb") as fh:
            blob += fh.read()
    assert b"alpha" in blob                                # positive control
    assert SECRET.encode() not in blob and b"gatedsrc:9" not in blob and b'"gatedsrc"' not in blob


# RV6 (row classes): a licence row present on BOTH sides, whose D1 copy names a gated id, is not "only local".
def test_rv6_a_licence_row_on_both_sides_is_not_reported_local_only(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("alpha:1", license_id="lic-q")])
    fake.add(rc.PRIMARY, "source", [src("alpha", license_id="lic-q")])
    fake.add(rc.PRIMARY, "license", [lic("lic-q", name="see the gatedsrc terms")])
    fake.finish()
    local = make_local(tmp / "local.db", [srow("alpha:1", license_id="lic-q")], [src("alpha", license_id="lic-q")],
                       [lic("lic-q", name="plain terms")])
    snaps, _ = snapshot(fake, local, tmp / "snap")
    out = tmp / "cls"
    run_classify(local, snaps, out)
    assert ("license", "lic-q", "C4") not in rows(out), "a licence row in D1 too is listed as 'only local'"
    hits, _ = _grep_outputs([out, tmp / "snap"], ["gatedsrc"])
    assert hits == []


# RV7 (V7 / S5 Road B proof): a sec_edgar row whose id carries a gated token is still COUNTED by V7.
def test_rv7_v7_counts_a_sec_edgar_row_whose_id_carries_a_gated_token(env):
    tmp, fake = env
    fake.add(rc.PRIMARY, "series", [srow("sec_edgar:GATEDSRC", start_date="2002-01-01"), srow("a:1")])
    fake.finish()
    make_local(cp.BUILD_PATH, [srow("sec_edgar:GATEDSRC", start_date="2001-01-01"), srow("a:1")])
    set_t0()
    snapshot(fake, cp.BUILD_PATH, tmp / "snap")
    extra, _reh = _t0_inputs(tmp)
    _c1(tmp, [])
    code = rc.main(["classify", "--out", str(tmp / "cls"), "--snapshot-dir", str(tmp / "snap"), "--c1",
                    str(tmp / "c1.tsv"), *extra])
    assert code == 1, "V7 passed while a sec_edgar row differs (its id carries a gated token)"


# RV8 (R.6/L5 halving is for an answer too large): one transient error must not shrink every later page.
def test_rv8_one_transient_error_does_not_halve_every_later_page(env, monkeypatch):
    tmp, fake = env
    monkeypatch.setattr(rc, "RETRY_SLEEPS", (0.0, 0.0, 0.0))
    d = [srow(f"alpha:{i:04d}") for i in range(900)]
    fake.add(rc.PRIMARY, "series", d)
    fake.finish()
    local = make_local(tmp / "local.db", d)
    state = {"n": 0}
    real = fake.__call__

    def flaky(db, sql, params):
        if "FROM series WHERE" in sql and "MATCH" not in sql:
            state["n"] += 1
            if state["n"] == 1:
                raise RuntimeError("D1 econ-catalog: HTTP 503 b'temporarily unavailable'")
        return real(db, sql, params)
    rc.D1_QUERY = flaky
    try:
        assert rc.main(["read-d1", "--out", str(tmp / "r"), "--catalogue", local, "--budget-rows", "100000",
                        "--page-rows", "200"]) == 0
    finally:
        rc.D1_QUERY = fake
    con = sqlite3.connect(str(tmp / "r" / f"d1_{rc.PRIMARY}.sqlite"))
    asked = [r[0] for r in con.execute("SELECT page_rows_asked FROM _pages WHERE tbl='series'")]
    con.close()
    assert asked and max(asked) == 200, f"pages asked after one 503: {asked} (a transient error halved P for good)"
