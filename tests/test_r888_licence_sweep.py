"""tools/selfhost/r888_licence_sweep.py (B32): the allow-list licence delete, the VACUUM receipt, the receipt sweep
and the raw residue scan - on small synthetic catalogues with FAKE gate members injected by monkeypatching
core.gen_denylist.committed_gate(). No real gated id appears here; every assertion is on a bool computed in a helper
(pytest's assertion rewriting would print compared values). The real E:/ProgramData paths are never touched:
PROOF_ROOTS, BUILD_PATH, LIVE_STORE_ROOT, LOCK_PATH and FLAG_PATH are monkeypatched to temporary paths.

The git-reading helpers (tree_clean, code_named, history_members) are monkeypatched in the `env` fixture - this
file is itself tracked, so the real code_named would find its FAKE licence ids "named by a tracked file" (code
review H2) - and each is tested on its own against a temporary git repository the test builds."""
import contextlib
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
import r888_licence_sweep as T  # noqa: E402

from core import catalog_path as cp  # noqa: E402
from core import cutover  # noqa: E402

FAKE = {"fakegate_alpha", "zzq_beta", "qqx"}        # FAKE members: none is a real source id
FORMER = {"oldgone_delta"}                           # a FAKE member of an older version of the gate only
SCHEMA = """
CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, reservable INTEGER, commercial_ok INTEGER,
  attribution_required INTEGER, no_modify INTEGER DEFAULT 0, url TEXT);
CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT, attribution TEXT,
  terms_url TEXT);
CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
  geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, last_updated TEXT, metadata TEXT);
CREATE INDEX ix_series_source_id ON series(source_id);
CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
CREATE TABLE blobonly (id INTEGER PRIMARY KEY, b BLOB);
"""
LICENCES = [
    ("lic-hosted", "Hosted Open Licence", 1, 1, 1, 0, "https://example.org/l1"),
    ("cc-by-4.0-zzq-beta", "terms of a removed source", 1, 0, 1, 0, None),       # '-' spelling, UNREFERENCED
    ("terms-fakegate_alpha", "Fakegate Alpha terms", 1, 0, 1, 0, None),          # SHARED: a source uses it
    ("lic-series-only", "Licence of Fakegate-Alpha data", 1, 0, 1, 0, None),     # only a SERIES row uses it
    ("clean-orphan", "Some Unused Licence", 0, 0, 1, 0, None),                   # unreferenced, carries nothing
    ("lic-oldgone-delta", "former terms", 1, 0, 1, 0, None),                     # unreferenced, a FORMER member
]
NAMED = [("named-clean", "A Licence Code Names", 1, 0, 1, 0, None),             # unreferenced, code names it
         ("named-qqx-terms", "terms for qqx", 1, 0, 1, 0, None)]                  # unreferenced + member + named
SOURCES = [("hostsrc", "Host Source", "https://example.org", "lic-hosted", "Host", None),
           ("sharesrc", "Share Source", "https://example.net", "terms-fakegate_alpha", "Share", None)]
KEPT = {"lic-hosted", "terms-fakegate_alpha", "lic-series-only"}


def _series():
    out = [("hostsrc:A", "hostsrc", "a title naming zzq_beta data", "A", None, "WLD", None, "lic-hosted", None, None,
            None, "{}"),
           ("hostsrc:B", "hostsrc", "plain", "A", None, None, None, "lic-series-only", None, None, None, "{}")]
    out += [(f"sharesrc:S{i}", "sharesrc", f"series number {i}", "M", None, None, None, "terms-fakegate_alpha",
             None, None, None, "{}") for i in range(40)]
    return out


def make_db(path, series=None, licences=None, sources=None):
    c = sqlite3.connect(path)
    c.executescript(SCHEMA)
    c.executemany("INSERT INTO license VALUES (?,?,?,?,?,?,?)", licences or LICENCES)
    c.executemany("INSERT INTO source VALUES (?,?,?,?,?,?)", sources or SOURCES)
    rows = series if series is not None else _series()
    c.executemany("INSERT INTO series VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    c.executemany("INSERT INTO series_fts (series_id, title, geography) VALUES (?,?,?)",
                  [(r[0], r[2], r[5]) for r in rows])
    c.execute("INSERT INTO blobonly (b) VALUES (?)", (b"\x00\x01",))
    c.commit()
    c.close()


@pytest.fixture
def env(tmp_path, monkeypatch):
    proof = tmp_path / "proof"
    live = tmp_path / "live"
    (live / "data").mkdir(parents=True)
    (live / "logs").mkdir()
    proof.mkdir()
    monkeypatch.setattr(T, "PROOF_ROOTS", (str(proof),))
    monkeypatch.setattr(T, "LIVE_AREAS", (str(tmp_path / "econ_live"),))
    monkeypatch.setattr(cp, "LIVE_STORE_ROOT", str(live))
    monkeypatch.setattr(cp, "BUILD_PATH", str(live / "data" / "build.db"))
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(live / "data" / "build.db"))
    monkeypatch.setattr(cp, "LOCK_PATH", str(tmp_path / "econ_live" / "state" / "writer.lock"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(T.gen_denylist, "committed_gate", lambda src=None: set(FAKE))
    monkeypatch.setattr(T.gen_denylist, "committed_carveouts", lambda src=None: {})
    monkeypatch.setattr(T, "tree_clean", lambda paths=T.HEAD_INPUTS: True)
    monkeypatch.setattr(T, "head_sha", lambda: "0" * 40)
    monkeypatch.setattr(T, "code_named", lambda ids: (set(), "0" * 40))
    monkeypatch.setattr(T, "history_members", lambda: set(FAKE) | set(FORMER))
    db = proof / "proof.db"
    make_db(db)
    n = {"i": 0}

    def fresh(kind):
        n["i"] += 1
        return str(proof / f"{kind}{n['i']}.json")
    return {"tmp": tmp_path, "proof": proof, "live": live, "db": str(db), "fresh": fresh,
            "older": str(tmp_path / "older.db")}


def run(argv):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = T.main(argv)
    return rc, buf.getvalue()


def no_fake_member(text: str) -> bool:
    """True when no FAKE member (current or former), in any spelling the tool's matcher knows, is in `text`."""
    return not any(T.Gate(FAKE | FORMER).cell(line) for line in text.splitlines())


def plan(e, db=None, live=False):
    st = e["fresh"]("state")
    rc, out = run(["--plan", "--db", db or e["db"], "--state", st] + (["--live"] if live else []))
    tok = next((x.split()[1] for x in out.splitlines() if x.startswith("--confirm")), "")
    return rc, out, st, tok


def tmpd(e, st):
    return os.path.join(str(e["proof"]), "vtmp", os.path.basename(st))


def apply(e, st, tok, db=None, live=False, extra=()):
    return run(["--apply", "--db", db or e["db"], "--state", st, "--confirm", tok, "--tmpdir", tmpd(e, st),
                "--backup", e["fresh"]("backup"), "--older-copy", e["older"]] + (["--live"] if live else [])
               + list(extra))


def licence_ids(db):
    c = sqlite3.connect(db)
    try:
        return {r[0] for r in c.execute("SELECT license_id FROM license")}
    finally:
        c.close()


def freelist(db):
    c = sqlite3.connect(db)
    try:
        return c.execute("PRAGMA freelist_count").fetchone()[0]
    finally:
        c.close()


def add_free_pages(db):
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE filler (v TEXT)")
    c.executemany("INSERT INTO filler VALUES (?)", [("x" * 3000,) for _ in range(40)])
    c.commit()
    c.execute("DROP TABLE filler")
    c.commit()
    c.close()


# ---- matching (L1) ---------------------------------------------------------------------------------------------
def test_matcher_senses_and_normalisation():
    g = T.Gate(FAKE)
    checks = [
        "SEPN" in g.cell("cc-by-4.0-zzq-beta"),                  # '-' for '_'
        "SEPN" in g.cell("ZZQ.BETA terms"),                      # '.' and case
        "SEPN" in g.cell("zzq\u2013beta"),                       # en dash
        "SEPN" in g.cell("zzq\u00a0beta"),                       # no-break space
        bool(g.cell("x %7A%7A%71%5F%62%65%74%61 y")),            # fully URL-escaped
        "CONCAT" in g.cell("the zzqbeta file"),
        "TOKEN" in g.cell("about qqx data"),
        "LOOSE" in g.cell("ingest_qqx.py") and "TOKEN" not in g.cell("ingest_qqx.py"),
        "KEY" in g.cell("fakegate_alpha:CODE1"),
        "EXACT" in g.cell("FAKEGATE_ALPHA"),
        g.cell("aqqxb mixed in a word") == {"SUBALL"},             # a short member inside a word: SUBALL only
        not g.cell("~~~~ #### ~~~~"),
        bool(T.Gate({"_lead"}).cell("a _lead b")),                 # a leading separator still has a prefilter (L6)
    ]
    assert all(checks)


def test_base_and_norm_are_defined_as_documented():
    checks = [T.base("AbC") == "abc", T.norm(T.base("a-b.c d_e")) == "a_b_c_d_e",
              T.base("a\u200bb") == "ab", T.base("x\u2212y") == "x-y", T.base("\uff21") == "a"]
    assert all(checks)


def test_names_token_quoted_unquoted_and_not_a_longer_id():
    k = b"cc-by-4.0"
    checks = [T.names_token(b"licenses:\n  cc-by-4.0:\n    name: x", k),       # unquoted YAML key
              T.names_token(b'LIC = "cc-by-4.0"', k), T.names_token(b"under cc-by-4.0. Then", k),
              not T.names_token(b'"cc-by-4.0-imf"', k), not T.names_token(b"xcc-by-4.0", k)]
    assert all(checks)


def test_record_image_matches_sqlite(tmp_path):
    db = tmp_path / "r.db"
    make_db(db)
    c = sqlite3.connect(db)
    sigs = T.row_signatures(c, str(db), ["clean-orphan", "lic-hosted"])
    c.close()
    data = db.read_bytes()
    assert len(sigs) == 4 and all(s in data for s in sigs)


# ---- keep set (R908 rule 2, review rev 6.6 L2) -------------------------------------------------------------------
def test_keep_set_from_the_records(monkeypatch):
    mods = {"audit_license_coverage": types.SimpleNamespace(assessed=lambda: {"zzq_beta": "CLEARED (open)",
                                                                              "other": "RESTRICTED"}),
            "audit_schedule_coverage": types.SimpleNamespace(supported_sources=lambda: {"qqx", "x"})}
    monkeypatch.setattr(T, "_load_tool", lambda name: mods[name])
    monkeypatch.setattr(T.gen_denylist, "committed_carveouts", lambda src=None: {})
    sel, why, keep = T.selected_members(T.Gate(FAKE), ["hostsrc"])
    ok = [keep == {"zzq_beta", "qqx"}, set(sel.ms) == {"fakegate_alpha"}, why["CLEARED"], why["served"],
          why[T.UNVERDICTED]]
    mods["audit_schedule_coverage"] = types.SimpleNamespace(supported_sources=lambda: (_ for _ in ()).throw(
        SystemExit("x")))
    with pytest.raises(T.Refused):
        T.selected_members(T.Gate(FAKE), [])
    assert all(ok)


# ---- git helpers, on a temporary repository ----------------------------------------------------------------------
def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.invalid"] + list(args),
                   check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path, monkeypatch):
    if shutil.which("git") is None:
        pytest.skip("git is not installed")
    r = tmp_path / "repo"
    (r / "api" / "worker" / "src").mkdir(parents=True)
    _git(r, "init", "-q")
    gate_file = r / "api" / "worker" / "src" / "denylist.ts"
    gate_file.write_text('export const NON_REDISTRIBUTABLE = new Set(["oldgone_delta", "qqx"]);\n')
    (r / "configs").mkdir()
    (r / "configs" / "sources.yaml").write_text("licenses:\n  named-clean:\n    name: x\n")
    _git(r, "add", "-A")
    _git(r, "commit", "-q", "-m", "v1")
    gate_file.write_text('export const NON_REDISTRIBUTABLE = new Set(["qqx", "zzq_beta"]);\n')
    _git(r, "commit", "-q", "-am", "v2")
    monkeypatch.setattr(T, "ROOT", str(r))
    return r


def test_history_members_reads_every_version(repo):
    h = T.history_members()
    assert {"oldgone_delta", "qqx", "zzq_beta"} <= h


def test_code_named_reads_tracked_files_at_head(repo):
    named, head = T.code_named({"named-clean", "clean-orphan", "named"})
    assert named == {"named-clean"} and len(head) == 40


def test_tree_clean_sees_an_edit_and_an_untracked_input(repo):
    paths = ("api/worker/src/denylist.ts", "configs/sources.yaml")
    a = T.tree_clean(paths)
    (repo / "configs" / "sources.yaml").write_text("changed\n")
    b = T.tree_clean(paths)
    _git(repo, "checkout", "--", "configs/sources.yaml")
    c = T.tree_clean(paths + ("not/tracked.py",))
    assert a and not b and not c


# ---- plan / apply ----------------------------------------------------------------------------------------------
def test_plan_prints_no_count_and_no_member(env):
    rc, out, st, tok = plan(env)
    digit_free = all(not any(ch.isdigit() for ch in ln.replace("B34", "")) for ln in out.splitlines()
                     if ln.startswith(("licence rows", "   ...", "unreferenced rows")))
    with open(st, encoding="utf-8") as f:
        body = json.load(f)["body"]
    ok = [rc == 0, "PLAN PASS" in out, no_fake_member(out), digit_free, tok.startswith("B32-"),
          body["sel_hash"] not in out, body["keep_hash"] not in out, body["held_hash"] not in out]
    assert all(ok)


def test_apply_deletes_uncoded_unreferenced_rows_and_keeps_the_rest(env):
    _rc, _out, st, tok = plan(env)
    rc, out = apply(env, st, tok)
    side = any(os.path.exists(env["db"] + s) for s in ("-journal", "-wal", "-shm"))
    backups = [p for p in os.listdir(env["proof"]) if p.startswith("backup")]
    with open(os.path.join(env["proof"], backups[0]), encoding="utf-8") as f:
        btxt = f.read()
    ok = [rc == 0, "APPLY PASS" in out, licence_ids(env["db"]) == KEPT, freelist(env["db"]) == 0, not side,
          no_fake_member(out), len(backups) == 1, "clean-orphan" in btxt, no_fake_member(btxt),
          "lic-oldgone-delta" not in btxt,                       # a FORMER member's row is never written (H1)
          "the deleted rows' record images are found NOWHERE in the file after VACUUM" in out]
    assert all(ok)


def test_no_vacuum_then_vacuum(env):
    add_free_pages(env["db"])
    _rc, _out, st, tok = plan(env)
    rc1, out1 = apply(env, st, tok, extra=["--no-vacuum"])
    fl1 = freelist(env["db"])
    rc2, out2 = run(["--vacuum", "--db", env["db"], "--tmpdir", tmpd(env, st + "v")])
    ok = [rc1 == 0, "APPLY PASS" in out1, "VACUUM: done" not in out1, fl1 > 0,
          "the deleted rows' record images are in NO live cell after the delete" in out1,
          licence_ids(env["db"]) == KEPT, rc2 == 0, "VACUUM PASS" in out2, freelist(env["db"]) == 0,
          no_fake_member(out1 + out2)]
    assert all(ok)


def test_vacuum_refuses_a_busy_file_and_too_little_space(env, monkeypatch):
    add_free_pages(env["db"])
    if os.name == "nt":
        other = sqlite3.connect(env["db"])
        try:
            other.execute("SELECT 1").fetchone()
            seen = T.other_handles(env["db"])
            rc_busy, out_busy = run(["--vacuum", "--db", env["db"], "--tmpdir", tmpd(env, "b1")])
        finally:
            other.close()
        busy_ok = seen is True and rc_busy == 2 and freelist(env["db"]) > 0 and T.other_handles(env["db"]) is False
    else:
        busy_ok = T.other_handles(env["db"]) is None
    real = shutil.disk_usage
    monkeypatch.setattr(shutil, "disk_usage", lambda p: type(real(p))(10, 9, 1))
    rc_space, out_space = run(["--vacuum", "--db", env["db"], "--tmpdir", tmpd(env, "b2")])
    assert busy_ok and rc_space == 2 and "2x the file size" in out_space and freelist(env["db"]) > 0


def test_an_unknown_handle_answer_refuses_on_windows(env, monkeypatch):
    monkeypatch.setattr(T, "other_handles", lambda p: None)
    monkeypatch.setattr(T.os, "name", "nt")
    with pytest.raises(T.Refused):
        T.vacuum_preconditions(env["db"])


def test_code_named_rows_are_kept(env, monkeypatch):
    db2 = os.path.join(env["proof"], "named.db")
    make_db(db2, licences=LICENCES + NAMED)
    monkeypatch.setattr(T, "code_named", lambda ids: ({i for i in ids if i.startswith("named-")}, "0" * 40))
    _rc, out0, st, tok = plan(env, db=db2)
    rc, out = apply(env, st, tok, db=db2)
    rc2, out2 = run(["--sweep", "--db", db2, "--workers", "1"])
    ids = licence_ids(db2)
    ok = [rc == 0, {"named-clean", "named-qqx-terms"} <= ids, "clean-orphan" not in ids,
          "unreferenced rows a tracked file names, carrying a member (KEPT; B34's code PR first): YES" in out0,
          rc2 == 0, "K1N HELD        an unreferenced row a tracked file names that carries a member: YES" in out2]
    assert all(ok)


def test_sweep_fails_before_and_passes_after_apply(env):
    rc0, out0 = run(["--sweep", "--db", env["db"], "--workers", "1"])
    _rc, _o, st, tok = plan(env)
    apply(env, st, tok)
    rc1, out1 = run(["--sweep", "--db", env["db"], "--workers", "1", "--compare-state", st])
    rc2, out2 = run(["--sweep", "--db", env["db"], "--licence-only"])
    ok = [rc0 == 1, "K1A FAIL-class  an unreferenced licence row that no tracked file names: YES" in out0,
          rc1 == 0, "SWEEP PASS" in out1,
          "K3  HELD (v6)   referenced by a source row and carrying a member:  YES" in out1,
          "K3  HELD (v6)   referenced by series rows only, carrying a member: YES" in out1,
          "v6 keys equal the reference state's (REPORTED, not a FAIL - owner item 40): YES" in out1,
          "blobonly: UNMEASURED" in out1,
          "planted control (in-memory licence rows through the same classifier): PASS" in out1,
          "planted control (in-memory copy of a real row): PASS" in out1,
          "FAIL" not in out1.replace("FAIL-class", "").replace("not a FAIL", ""),
          rc2 == 0, "-- series:" not in out2, no_fake_member(out0 + out1 + out2)]
    assert all(ok)


def test_a_v6_change_is_reported_not_failed(env):
    _rc, _o, st, tok = plan(env)
    apply(env, st, tok)
    c = sqlite3.connect(env["db"])
    c.execute("INSERT INTO license VALUES ('lic-new-qqx', 'qqx terms', 1, 0, 1, 0, NULL)")
    c.execute("UPDATE source SET license_id='lic-new-qqx' WHERE source_id='hostsrc'")
    c.commit()
    c.execute("VACUUM")
    c.close()
    rc, out = run(["--sweep", "--db", env["db"], "--workers", "1", "--compare-state", st])
    assert rc == 0 and "v6 keys equal the reference state's (REPORTED, not a FAIL - owner item 40): NO" in out


def test_sweep_free_pages_fail_unless_allowed(env):
    _rc, _o, st, tok = plan(env)
    apply(env, st, tok)
    add_free_pages(env["db"])
    rc1, _o1 = run(["--sweep", "--db", env["db"], "--licence-only"])
    rc2, out2 = run(["--sweep", "--db", env["db"], "--licence-only", "--allow-free-pages"])
    assert rc1 == 1 and rc2 == 0 and "SWEEP PASS" in out2


def test_sweep_shards_over_workers(env, monkeypatch):
    monkeypatch.setattr(T, "SHARD_ROWS", 5)
    rc, out = run(["--sweep", "--db", env["db"], "--workers", "1"])
    assert "series: 42 rows scanned, complete: YES" in out and rc == 1


def test_sweep_k2_rows_of_a_gated_source_also_respelled(env):
    db2 = os.path.join(env["proof"], "k2.db")
    rows = _series() + [("hostsrc:Z1", "zzq-beta", "x", "A", None, None, None, "lic-hosted", None, None, None, "{}")]
    make_db(db2, series=rows)
    rc, out = run(["--sweep", "--db", db2, "--workers", "1"])
    assert rc == 1 and "K2  FAIL-class  rows of a deny-listed source:            YES" in out


def test_wrong_or_missing_state_token_backup_deletes_nothing(env, tmp_path):
    _rc, _o, st, tok = plan(env)
    before = licence_ids(env["db"])
    rc1, _ = apply(env, st, "B32-0000000000000000")
    rc2, _ = run(["--apply", "--db", env["db"], "--confirm", tok])
    with open(st, encoding="utf-8") as f:
        d = json.load(f)
    d["body"]["sel_hash"] = "0" * 64
    tampered = env["fresh"]("state")
    with open(tampered, "w", encoding="utf-8") as f:
        json.dump(d, f)
    rc3, out3 = apply(env, tampered, tok)
    outside = tmp_path / "outside_state.json"
    rc4, _ = run(["--plan", "--db", env["db"], "--state", str(outside)])
    ok = [rc1 == 2, rc2 == 2, rc3 == 2, "seal" in out3, rc4 == 2, not outside.exists(),
          licence_ids(env["db"]) == before]
    assert all(ok)


def test_a_change_after_plan_voids_the_token(env):
    _rc, _o, st, tok = plan(env)
    c = sqlite3.connect(env["db"])
    c.execute("UPDATE source SET name='Host Source 2' WHERE source_id='hostsrc'")
    c.commit()
    c.close()
    before = licence_ids(env["db"])
    rc, out = apply(env, st, tok)
    assert rc == 2 and "REFUSED" in out and licence_ids(env["db"]) == before


def test_an_unclean_checkout_refuses(env, monkeypatch):
    monkeypatch.setattr(T, "tree_clean", lambda paths=T.HEAD_INPUTS: False)
    a = _refused(["--plan", "--db", env["db"], "--state", env["fresh"]("s")])
    b = _refused(["--sweep", "--db", env["db"]])
    assert a and b


def test_another_open_handle_blocks_the_apply(env):
    if os.name != "nt":
        pytest.skip("the share-mode handle check is Windows-only; elsewhere BEGIN EXCLUSIVE is the check")
    _rc, _o, st, tok = plan(env)
    before = licence_ids(env["db"])
    other = sqlite3.connect(env["db"])
    try:
        other.execute("SELECT 1").fetchone()
        rc, out = apply(env, st, tok)
    finally:
        other.close()
    assert rc == 2 and licence_ids(env["db"]) == before


def test_too_little_free_space_blocks_the_apply(env, monkeypatch):
    _rc, _o, st, tok = plan(env)
    real = shutil.disk_usage
    monkeypatch.setattr(shutil, "disk_usage", lambda p: type(real(p))(10, 9, 1))
    before = licence_ids(env["db"])
    rc, out = apply(env, st, tok)
    assert rc == 2 and "2x the file size" in out and licence_ids(env["db"]) == before


def test_backup_refuses_a_member_row(env):
    p = env["fresh"]("backup")
    with pytest.raises(T.Refused):
        T.write_backup(p, T.Gate(FAKE), ["license_id", "name"], [("x", "the zzq_beta terms")])
    assert not os.path.exists(p)


def test_older_copy_line(env):
    _rc, _o, st, tok = plan(env)
    make_db(env["older"], licences=[r for r in LICENCES if not r[0].startswith("cc-by")])
    rc, out = apply(env, st, tok)
    ok = [rc == 0, "a deleted member-carrying row is absent from the older F: copy (no F: copy holds it now): YES"
          in out]
    db2 = os.path.join(env["proof"], "p2.db")
    make_db(db2)
    _rc2, _o2, st2, tok2 = plan(env, db=db2)
    os.remove(env["older"])
    make_db(env["older"])
    rc2, out2 = apply(env, st2, tok2, db=db2)
    ok += [rc2 == 0, "absent from the older F: copy (no F: copy holds it now): NO" in out2]
    assert all(ok)


# ---- refusals ---------------------------------------------------------------------------------------------------
def _refused(argv) -> bool:
    rc, out = run(argv)
    return rc == 2 and "REFUSED" in out


def test_refusal_paths(env, monkeypatch, tmp_path):
    db = env["db"]
    outside = tmp_path / "elsewhere.db"
    make_db(outside)
    linked = env["proof"] / "linked.db"
    make_db(linked)
    os.link(linked, env["proof"] / "linked2.db")
    side = env["proof"] / "side.db"
    make_db(side)
    open(str(side) + "-journal", "wb").close()
    wal = env["proof"] / "wal.db"
    make_db(wal)
    c = sqlite3.connect(wal)
    c.execute("PRAGMA journal_mode=WAL")
    c.close()
    build = env["live"] / "data" / "build.db"
    make_db(build)
    _rc, _o, st, tok = plan(env)
    checks = {
        "outside PROOF_ROOTS": _refused(["--plan", "--db", str(outside), "--state", env["fresh"]("s")]),
        "hard link": _refused(["--plan", "--db", str(linked), "--state", env["fresh"]("s")]),
        "sidecar journal": _refused(["--sweep", "--db", str(side)]),
        "WAL header": _refused(["--plan", "--db", str(wal), "--state", env["fresh"]("s")]),
        "the build without --live": _refused(["--sweep", "--db", str(build)]),
        "long-path spelling": _refused(["--sweep", "--db", "\\\\?\\" + db]),
        "--live on a copy": _refused(["--sweep", "--live", "--db", db]),
        "tmpdir outside the work folder": _refused(["--apply", "--db", db, "--state", st, "--confirm", tok,
                                                    "--backup", env["fresh"]("b"), "--tmpdir",
                                                    str(tmp_path / "elsewhere_tmp")]),
        "state already exists": _refused(["--plan", "--db", db, "--state", st]),
    }
    monkeypatch.setattr(T.gen_denylist, "committed_gate", lambda src=None: set())
    checks["empty gate"] = _refused(["--sweep", "--db", db])

    def boom(src=None):
        raise T.gen_denylist.GateParseError("x")
    monkeypatch.setattr(T.gen_denylist, "committed_gate", boom)
    checks["unreadable gate"] = _refused(["--residue", "--db", db])
    (env["tmp"] / "CUTOVER").write_text("")
    checks["after T0 without --live"] = _refused(["--sweep", "--db", db])
    assert all(checks.values()), [k for k, v in checks.items() if not v]
    assert not (tmp_path / "elsewhere_tmp").exists() and licence_ids(db) == {r[0] for r in LICENCES}


def test_argparse_needs_db():
    with pytest.raises(SystemExit):
        T.main(["--plan"])


# ---- the --live path (S4d), simulated -----------------------------------------------------------------------------
@pytest.fixture
def live(env, monkeypatch):
    build = env["live"] / "data" / "build.db"
    make_db(build)
    mods = {n: T._load_tool(n) for n in ("audit_license_coverage", "audit_schedule_coverage")}   # the REAL records
    monkeypatch.setattr(T, "_load_tool", lambda n: mods[n])
    monkeypatch.setattr(T, "ROOT", str(env["live"]))
    for n in ("STOP_DISPATCH", "local_heavy.DONE"):
        (env["live"] / "logs" / n).write_text("")
    return str(build)


def test_live_needs_the_writers_held(env, live):
    os.remove(env["live"] / "logs" / "STOP_DISPATCH")
    a = _refused(["--sweep", "--live", "--db", live])
    (env["live"] / "logs" / "STOP_DISPATCH").write_text("")
    (env["live"] / "logs" / "local_heavy.lock").write_text("")
    b = _refused(["--sweep", "--live", "--db", live])
    os.remove(env["live"] / "logs" / "local_heavy.lock")
    rc, _o, _st, _tok = plan(env, db=live, live=True)
    assert a and b and rc == 0


def test_live_apply_and_vacuum_take_the_writer_lock(env, live, monkeypatch):
    seen = []
    real = cp.writer_lock

    @contextlib.contextmanager
    def spy():
        with real():
            seen.append(cp._held is not None)
            yield
    monkeypatch.setattr(cp, "writer_lock", spy)
    _rc, _o, st, tok = plan(env, db=live, live=True)
    rc, out = apply(env, st, tok, db=live, live=True, extra=["--no-vacuum"])
    rc2, out2 = run(["--vacuum", "--live", "--db", live, "--tmpdir", tmpd(env, "lv")])
    assert rc == 0 and rc2 == 0 and seen == [True, True] and "clean-orphan" not in licence_ids(live)


def test_live_apply_refused_when_the_lock_is_held_elsewhere(env, live):
    _rc, _o, st, tok = plan(env, db=live, live=True)
    before = licence_ids(live)
    os.makedirs(os.path.dirname(cp.LOCK_PATH), exist_ok=True)
    fh = open(cp.LOCK_PATH, "a+b")
    try:
        cp._lock(fh)
        rc, out = apply(env, st, tok, db=live, live=True)
        rc2, out2 = run(["--vacuum", "--live", "--db", live, "--tmpdir", tmpd(env, "lh")])
    finally:
        cp._unlock(fh)
        fh.close()
    assert rc == 2 and rc2 == 2 and licence_ids(live) == before


def test_live_after_t0_works_only_with_the_lock(env, live):
    (env["tmp"] / "CUTOVER").write_text("")
    _rc, _o, st, tok = plan(env, db=live, live=True)
    rc, out = apply(env, st, tok, db=live, live=True)
    assert rc == 0 and "APPLY PASS" in out


# ---- residue -----------------------------------------------------------------------------------------------------
def test_residue_sees_a_deleted_member_in_free_pages_and_vacuum_clears_it(env):
    db = os.path.join(env["proof"], "res.db")
    c = sqlite3.connect(db)
    c.execute("PRAGMA secure_delete=OFF")
    c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    c.executemany("INSERT INTO t (v) VALUES (?)", [("payload zzq_beta " + "x" * 3000,) for _ in range(50)])
    c.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT)")
    c.commit()
    c.execute("DELETE FROM t")
    c.commit()
    c.close()
    rc0, out0 = run(["--residue", "--db", db])
    c = sqlite3.connect(db)
    c.execute("VACUUM")
    c.close()
    rc1, out1 = run(["--residue", "--db", db])
    ok = [rc0 == 0, "deny-list member in FREE pages (freelist trunk + leaf): YES" in out0,
          rc1 == 0, "deny-list member in FREE pages (freelist trunk + leaf): NO" in out1,
          "raw-scan control: planted member fires: PASS" in out0]
    assert all(ok)


def test_signatures_are_seen_before_and_gone_after(env):
    c = sqlite3.connect(env["db"])
    sigs = T.row_signatures(c, env["db"], ["clean-orphan", "cc-by-4.0-zzq-beta"])
    c.close()
    before = T.residue_scan(env["db"], T.Gate(FAKE), sigs=sigs)
    _rc, _o, st, tok = plan(env)
    apply(env, st, tok)
    after = T.residue_scan(env["db"], T.Gate(FAKE), sigs=sigs)
    ok = [before["SIG_SEEN"], before["SIG_LIVE"], not after["SIG_LIVE"], not after["SIG_DEAD"], len(sigs) == 4]
    assert all(ok)


def test_vacuum_keeps_fts_rowids(env):
    c = sqlite3.connect(env["db"])
    a = c.execute("SELECT rowid, series_id FROM series_fts ORDER BY rowid").fetchall()
    c.close()
    _rc, _o, st, tok = plan(env)
    apply(env, st, tok)
    c = sqlite3.connect(env["db"])
    b = c.execute("SELECT rowid, series_id FROM series_fts ORDER BY rowid").fetchall()
    c.close()
    assert a == b


# ---- the R888 scan of this build item's own files, against the REAL gate ----------------------------------------
def test_no_real_member_in_the_b32_files():
    from core import gen_denylist                                     # the real module, not the monkeypatched one
    real = T.Gate(gen_denylist.committed_gate())
    files = [os.path.join(ROOT, "tools", "selfhost", "r888_licence_sweep.py"), os.path.abspath(__file__)]
    carries = False
    for f in files:
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                h = real.cell(line)
                carries |= bool(h - {"SUBALL"})          # SUBALL alone fires on short members inside words
    assert not carries
