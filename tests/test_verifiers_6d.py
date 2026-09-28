"""EVERY FILE THAT READS THE CLOUD COPY AND DOES NOTHING AT T0, CLASSIFIED (plan section 5, step 6d: "the full list
is enumerated by grep in step 1").

After T0 core.r2_util.client() refuses EVERY call, reads included (so does updater.blob's R2 backend, which uses
it); only core.r2_util.cloud_client() - the named final-sync readers - still reads the frozen R2 copy, and
core.d1_remote still answers a plain D1 read with the read token. So a file on those roads either stops loudly or
keeps running against the FROZEN copy. What that means depends on what the file does with the answer - each class
below says. (R1243: this said "the R2 guard stops WRITES only", which was false.) The inventory is COMPLETE by construction: every .py under tools/
(recursive), core/, jobs/, updater/ and pipeline/ whose CODE (comments and docstrings removed) reaches the cloud and
has no cutover handling must be in exactly one class, and an entry that no longer qualifies must leave (R1241: the
first version scanned verifier-named files at the top of tools/ and missed tools/store_inventory.py, among others).

A file that READ the cloud and WROTE the live local data is not a class here: it must refuse after T0 (eight did
not, 2026-09-24 - mirror_sync, resync_and_repair, sync_source_rows_d1_to_local, clear_csv_desktop_owed,
repair_stale_csvs, rekey_ons_uk, catalog_statcan_tables, catalog_worldbank_esg_gaps - and now do).
Outside this repository: hfdatalibrary's .claude/skills/adversarial-review/tools/ledger_check.py (D1, head_object)."""
import ast
import io
import os
import re
import sys
import tokenize

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Matched against the code's TOKENS joined by single spaces (see _code_only). `r2_util . ROOT` is a path constant,
# not a cloud read (the idb cost tools use only that). AQUEDUCT_BACKEND=r2 / BACKEND = "r2" is the road that reads
# R2 through updater.blob without naming a client - the first scan could not see it (R1243).
CLOUD = re.compile(r"r2_util \. (?!ROOT\b)\w|core \. r2_util import (?!ROOT\b)|d1_remote|head_object|list_objects|"
                   r"get_object|_d1_json|R2Blob|workers\.dev|boto3|wrangler[^\n]*\bd1\b|/d1/database/|"
                   r"""['"]AQUEDUCT_BACKEND['"] (?:\] =|,) ['"]r2['"]|\bBACKEND = ['"]r2['"]|"""
                   r"""['"]--backend['"] , default = ['"]r2['"]""")
HANDLES_T0 = re.compile(r"is_cut_over|refuse_if_cut_over|refuse_unless_live_checkout|CutoverRefused")

# Judges "served" / store health FROM the cloud copy: after T0 every answer is about the frozen copy. Step 6d
# re-points each to the edge or the local store, or makes it refuse.
VERIFIERS_6D = set()   # all re-pointed or refusing (2026-09-24); a new one must be ported, not listed here
# Reads what USERS get, before and after T0 alike: the public site, the edge API (econdl-api.workers.dev, which
# forwards to the origin after T0) and hf's own login database (stays in D1). Nothing to re-point.
USER_FACING = {"tools/audit_site.py"}
# Measures the cloud copy ITSELF (its size, cost, reads, public answers): still true after T0, until the copy is
# decommissioned - that is exactly what these are for (plan: R2/D1 analytics until step 7).
CLOUD_COST = {
    "tools/billing_guard.py", "jobs/r2_bucket_sizes.py", "tools/selfhost/watch_edge.py", "tools/cost/smoke_public.py",
}
# Measure the cloud copy through core.r2_util.client(), which REFUSES after T0: they stop loudly (never a
# frozen-copy answer). If one is still wanted after T0 it moves to cloud_client(), a named-reader decision (R1243).
COST_REFUSED_AFTER_T0 = {
    "tools/cost/bucket_prefixes.py", "tools/cost/burst_measure.py", "tools/cost/gzip_fraction.py",
    "tools/cost/idb_baseline.py", "tools/cost/size_series_prefix.py", "tools/cost/storage_saving.py",
}
# Reads only to WRITE the cloud copy, and that write is refused after T0 (core.d1_remote refuses every D1 write;
# the R2 guard every R2 write) - they fail loudly, never quietly. Their self-hosted ports are owed.
CLOUD_WRITE_REFUSED = {
    "core/sync_state_d1.py", "tools/rebuild_series_fts.py", "tools/refresh_flowgrain_dates.py",
    "tools/stamp_source_data_through.py", "tools/derive_statcan_tables.py",
}
# No cloud call at run time: builds a D1 SQL FILE (loading it is a separate, refused wrangler step).
OFFLINE = {"core/export_d1.py"}
# Completed one-shots, defused (tests/test_defused_one_shots.py).
DEFUSED = {"tools/_delete_statcan_r2.py", "tools/trim_bfs_corrupt_tail.py", "tools/purge_unpermitted_r2.py"}
# Runs only on GitHub, in updater-daily.yml, which T0 disables (t0_ready's ci-writers check).
CI_ONLY = {"updater/send_digest.py"}
# Names the cloud roads in order to REFUSE them (plan change 5).
BY_DESIGN = {"tools/selfhost/cutover_hook.py"}
# (R1243 round 3 had an ENV_ROUTED class here: nine tools whose r2 is only a default of AQUEDUCT_BACKEND. R1249
# showed it false for four catalogue writers that read the checkout's parquet with duckdb whatever the backend,
# and a worktree run after T0 replaced live catalogue rows. Every one of the nine now handles T0 itself - a
# catalogue writer runs only from the live checkout; an audit refuses r2 and a non-live checkout - so the scan no
# longer lists them and the class is gone. test_after_t0_the_backend_routed_tools_refuse_outside_the_live_checkout.)

CLASSES = {"VERIFIERS_6D": VERIFIERS_6D, "USER_FACING": USER_FACING, "CLOUD_COST": CLOUD_COST,
           "COST_REFUSED_AFTER_T0": COST_REFUSED_AFTER_T0, "CLOUD_WRITE_REFUSED": CLOUD_WRITE_REFUSED,
           "OFFLINE": OFFLINE, "DEFUSED": DEFUSED, "CI_ONLY": CI_ONLY, "BY_DESIGN": BY_DESIGN}


def _code_only(src):
    """Source without comments and docstrings (a mention is not a call)."""
    tree = ast.parse(src)
    doc_lines = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and body \
                and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                and isinstance(body[0].value.value, str):
            doc_lines.update(range(body[0].lineno, body[0].end_lineno + 1))
    toks = tokenize.generate_tokens(io.StringIO(src).readline)
    return " ".join(t.string for t in toks if t.type != tokenize.COMMENT and t.start[0] not in doc_lines)


def _scan(root=ROOT):
    found = set()
    for top in ("tools", "core", "jobs", "updater", "pipeline"):
        for dirpath, dirs, files in os.walk(os.path.join(root, top)):
            dirs[:] = [d for d in dirs if d not in ("__pycache__", "node_modules")]
            for f in files:
                if f.endswith(".py"):
                    p = os.path.join(dirpath, f)
                    code = _code_only(open(p, encoding="utf-8-sig", errors="replace").read())
                    if CLOUD.search(code) and not HANDLES_T0.search(code):
                        found.add(os.path.relpath(p, root).replace(os.sep, "/"))
    return found


def test_every_cloud_reader_without_t0_handling_is_classified_exactly_once():
    listed = [f for cls in CLASSES.values() for f in cls]
    assert len(listed) == len(set(listed)), "a file in two classes"
    found = _scan()
    unclassified = sorted(found - set(listed))
    stale = sorted(set(listed) - found)
    assert not unclassified, (f"these read the cloud copy and do nothing at T0 - classify them here, or make them "
                              f"refuse after T0 if they write local data: {unclassified}")
    assert not stale, f"no longer a cloud reader without T0 handling - remove from its class: {stale}"


def test_the_scan_can_fail(tmp_path):
    """Planted files in places and shapes the first version missed (R1241): a subfolder, core/, a wrangler D1
    read, a direct boto3 client, and a file whose only cutover mention is a comment. A file that handles T0 is not
    reported, and a mention in a docstring is not a call."""
    plants = {
        "tools/cost/new_probe.py": "import boto3\nc = boto3.client('s3')\n",
        "core/new_reader.py": "from core import d1_remote\nd1_remote.rows('econ-catalog', 'SELECT 1')\n",
        "tools/named_otherwise.py": "import subprocess\nsubprocess.run(['npx', 'wrangler', 'd1', 'execute'])\n",
        "tools/comment_only.py": "# is_cut_over\nfrom core import r2_util\nr2_util.client()\n",
        "tools/handles.py": "from core import r2_util, cutover\ncutover.refuse_if_cut_over('x')\nr2_util.client()\n",
        "tools/doc_only.py": '"""uses r2_util in prose only"""\nx = 1\n',
        # R1243: the backend road, in its three spellings, and r2_util used only for its ROOT path
        "tools/env_setdefault.py": 'import os\nos.environ.setdefault("AQUEDUCT_BACKEND", "r2")\n',
        "tools/env_assign.py": "import os\nos.environ['AQUEDUCT_BACKEND'] = 'r2'\n",
        "tools/config_assign.py": 'from updater import config\nconfig.BACKEND = "r2"\n',
        "tools/root_only.py": "import os\nfrom core import r2_util\nSTORE = os.path.join(r2_util.ROOT, 'data')\n",
        "tools/env_local.py": 'import os\nos.environ.setdefault("AQUEDUCT_BACKEND", "local")\n',
        "tools/backend_flag.py": 'import argparse, os\nap = argparse.ArgumentParser()\n'
                                 'ap.add_argument("--backend", default="r2")\n'
                                 'os.environ["AQUEDUCT_BACKEND"] = ap.parse_args().backend\n',
    }
    for rel, src in plants.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(src, encoding="utf-8")
    assert _scan(str(tmp_path)) == {"tools/cost/new_probe.py", "core/new_reader.py", "tools/named_otherwise.py",
                                    "tools/comment_only.py", "tools/env_setdefault.py", "tools/env_assign.py",
                                    "tools/config_assign.py", "tools/backend_flag.py"}


PULLERS = [("mirror_sync", ["--from-json", "x.json", "--apply"]), ("resync_and_repair", ["--source", "zz"]),
           ("sync_source_rows_d1_to_local", ["--source", "zz", "--apply"]),
           ("clear_csv_desktop_owed", ["--source", "zz", "--apply"]), ("repair_stale_csvs", ["--source", "zz"]),
           ("rekey_ons_uk", []), ("catalog_statcan_tables", ["--apply"]), ("catalog_worldbank_esg_gaps", ["--apply"]),
           # R1243: it FORCES config.BACKEND = "r2" (whatever the caller set) to rewrite the live state.db
           ("purge_state_cursors_bundle", [])]


@pytest.mark.parametrize("name,argv", PULLERS, ids=[p[0] for p in PULLERS])
def test_after_t0_a_tool_that_pulls_the_cloud_into_local_data_refuses_first(tmp_path, monkeypatch, name, argv):
    import argparse
    import importlib
    from core import cutover, r2_util, d1_remote
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    before = {k: os.environ.get(k) for k in ("AQUEDUCT_BACKEND", "AQUEDUCT_DERIVE_WORKERS")}
    cwd = os.getcwd()
    mod = importlib.import_module(name)
    # importing a tool must not change this process (R1239: an import-time backend setting moved later tests
    # onto R2; rekey_ons_uk did it again with a setdefault)
    assert {k: os.environ.get(k) for k in before} == before and os.getcwd() == cwd, f"importing {name} changed the process"
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 reached"))
    monkeypatch.setattr(d1_remote, "rows", lambda *a, **k: pytest.fail("D1 reached"))
    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", lambda *a, **k: pytest.fail("arguments parsed first"))
    monkeypatch.setattr(sys, "argv", [name, *argv])
    with pytest.raises(cutover.CutoverRefused, match="self-hosted since T0"):
        mod.main()


# Step 6d: verifiers whose question has no meaning after T0 (one store; D1 and R2 frozen copies) refuse then.
REFUSED_6D_MAIN = ["footer_diff", "audit_d1_vs_catalog", "audit_d1_source_counts",
                   # R1243: it FORCES config.BACKEND = "r2" to emulate the cloud mapper - no meaning after T0
                   "audit_cursor_grain"]
REFUSED_6D_MODULE = ["verify_statcan_store_bytes", "audit_serving_coherence"]


@pytest.mark.parametrize("name", REFUSED_6D_MAIN)
def test_after_t0_a_meaningless_verifier_refuses_first(tmp_path, monkeypatch, name):
    import argparse
    import importlib
    from core import cutover, r2_util, d1_remote
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    mod = importlib.import_module(name)
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 reached"))
    monkeypatch.setattr(d1_remote, "rows", lambda *a, **k: pytest.fail("D1 reached"))
    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", lambda *a, **k: pytest.fail("arguments parsed first"))
    monkeypatch.setattr(sys, "argv", [name])
    with pytest.raises(cutover.CutoverRefused, match="self-hosted since T0"):
        mod.main()


def _t0_elsewhere(tmp_path, monkeypatch):
    """T0 on, and this process is NOT the live checkout."""
    from core import cutover, catalog_path
    from updater import blob
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(catalog_path, "connect", lambda *a, **k: pytest.fail("the catalogue was opened first"))
    monkeypatch.setattr(catalog_path, "write_session", lambda *a, **k: pytest.fail("the writer lock was taken first"))
    return cutover


# (tool, argv, how main is called). R1249: a catalogue writer run from a worktree after T0 replaced live
# catalogue rows from scratch data; four verifiers read the live build before they refused.
BACKEND_ROUTED = [
    ("catalog_complete", [], "sources"), ("catalog_cepii_baci", [], None), ("catalog_dip_tables", [], None),
    ("catalog_imts_tables", [], None), ("catalog_mfs_tables", [], None), ("catalog_pip_tables", [], None),
    ("audit_tree_frontier", ["--source", "zz"], None), ("audit_current_vintage", [], None),
    ("audit_store_present", ["--backend", "local"], None),
    ("audit_r2_vs_catalog", ["zz"], None), ("audit_csv_staleness", ["--source", "zz"], None),
    ("sample_source_coverage", ["--source", "zz"], None), ("verify_derive_parity", ["--source", "zz"], None),
]


@pytest.mark.parametrize("name,argv,call", BACKEND_ROUTED, ids=[b[0] for b in BACKEND_ROUTED])
def test_after_t0_the_backend_routed_tools_refuse_outside_the_live_checkout(tmp_path, monkeypatch, name, argv, call):
    import importlib
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    mod = importlib.import_module(name)
    cutover = _t0_elsewhere(tmp_path, monkeypatch)
    monkeypatch.setenv("AQUEDUCT_BACKEND", "selfhost")
    monkeypatch.setattr(sys, "argv", [name, *argv])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        mod.main(["zz"]) if call == "sources" else mod.main()


def _no_network(mod, monkeypatch):
    """If a refusal ever stops firing, the tool must fail FAST here - not run for real. audit_current_vintage probes
    every fetcher's publisher over the network (a mutant that bypassed its refusal took 3.5 min of live probes)."""
    if hasattr(mod, "all_fetchers"):
        monkeypatch.setattr(mod, "all_fetchers", lambda: [])


@pytest.mark.parametrize("name,argv", [("audit_tree_frontier", ["--source", "zz"]), ("audit_current_vintage", []),
                                       ("audit_store_present", ["--backend", "r2"])])
def test_after_t0_an_audit_on_r2_refuses_even_in_the_live_checkout(tmp_path, monkeypatch, name, argv):
    """r2 after T0 is a frozen copy: the refusal names it, and comes before any read."""
    import importlib
    from updater import blob
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    mod = importlib.import_module(name)
    _no_network(mod, monkeypatch)
    cutover = _t0_elsewhere(tmp_path, monkeypatch)
    monkeypatch.setattr(blob, "refuse_unless_live_checkout", lambda *a, **k: None)   # as if live
    monkeypatch.setenv("AQUEDUCT_BACKEND", "r2")
    monkeypatch.setattr(sys, "argv", [name, *argv])
    with pytest.raises(cutover.CutoverRefused, match="frozen copy after T0"):
        mod.main()


def test_importing_audit_current_vintage_leaves_the_backend_alone(monkeypatch):
    """R1249 A11: the env line runs only as a script; an import must not move this process onto R2 (R1239)."""
    import importlib
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    sys.modules.pop("audit_current_vintage", None)
    importlib.import_module("audit_current_vintage")
    assert os.environ.get("AQUEDUCT_BACKEND") is None


def test_after_t0_audit_impossible_dates_refuses_r2_and_scratch(tmp_path, monkeypatch):
    """R1243: after T0 `--r2` reads a frozen copy and `--local` outside the live checkout reads scratch."""
    import importlib
    from core import cutover
    from updater import blob
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    mod = importlib.import_module("audit_impossible_dates")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    for argv, match in ((["--r2"], "self-hosted since T0"), (["--local"], "R1203")):
        monkeypatch.setattr(sys, "argv", ["audit_impossible_dates.py", *argv, "--source", "zz"])
        with pytest.raises(cutover.CutoverRefused, match=match):
            mod.main()


def test_the_cost_tools_classed_refused_really_use_the_refusing_client():
    """COST_REFUSED_AFTER_T0 is a claim about the road: r2_util.client(), never cloud_client()."""
    for rel in sorted(COST_REFUSED_AFTER_T0):
        code = _code_only(open(os.path.join(ROOT, rel), encoding="utf-8-sig").read())
        assert re.search(r"r2_util \. client \(", code) and "cloud_client" not in code, rel


@pytest.mark.parametrize("name", REFUSED_6D_MODULE)
def test_after_t0_a_meaningless_verifier_script_refuses_at_import(tmp_path, monkeypatch, name):
    """No main(): the refusal is the first thing the script does after its imports."""
    import importlib.util
    from core import cutover, r2_util
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 reached"))
    monkeypatch.setattr(sys, "argv", [name])
    spec = importlib.util.spec_from_file_location(f"_t0_{name}", os.path.join(ROOT, "tools", f"{name}.py"))
    with pytest.raises(cutover.CutoverRefused, match="self-hosted since T0"):
        spec.loader.exec_module(importlib.util.module_from_spec(spec))

@pytest.mark.parametrize("name,argv,env", [("audit_store_present", ["--backend", " R2"], "local"),
                                           ("audit_current_vintage", [], " R2 "),
                                           ("audit_tree_frontier", ["--source", "zz"], "R2")])
def test_after_t0_the_r2_refusal_ignores_case_and_spaces(tmp_path, monkeypatch, name, argv, env):
    """R1253: ` R2` / `R2 ` are the r2 backend to updater.blob (it strips and lowercases) - the refusal must agree."""
    import importlib
    from updater import blob
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    mod = importlib.import_module(name)
    _no_network(mod, monkeypatch)
    cutover = _t0_elsewhere(tmp_path, monkeypatch)
    monkeypatch.setattr(blob, "refuse_unless_live_checkout", lambda *a, **k: None)   # as if live
    monkeypatch.setenv("AQUEDUCT_BACKEND", env)
    monkeypatch.setattr(sys, "argv", [name, *argv])
    with pytest.raises(cutover.CutoverRefused, match="frozen copy after T0"):
        mod.main()


def test_the_repull_tools_print_an_audit_command_that_runs_after_t0():
    """R1249/R1253: after T0 they printed `audit_impossible_dates.py --source X`, which argparse rejects (--local
    or --r2 is required, and --r2 refuses after T0)."""
    cso = open(os.path.join(ROOT, "tools", "cso_repull_subject.py"), encoding="utf-8").read()
    rep = open(os.path.join(ROOT, "tools", "repull_file.py"), encoding="utf-8").read()
    assert "{'--local' if cutover.is_cut_over() else '--r2'} --source cso" in cso
    assert "audit_impossible_dates.py --local --source {a.source} from the live checkout" in rep


_ASP_DRIVER = (
    "import os, runpy, sys\n"
    "root, flag, argv = sys.argv[1], sys.argv[2], sys.argv[3:]\n"
    "sys.path.insert(0, root)\n"
    "from core import catalog_path, cutover\n"
    "assert 'updater.config' not in sys.modules, 'the driver froze config itself'\n"
    "cutover.FLAG_PATH = flag\n"
    "catalog_path.LIVE_STORE_ROOT = root\n"
    "catalog_path.BUILD_PATH = os.path.join(root, 'data', 'catalog.db')\n"
    "sys.argv = ['audit_store_present.py', *argv]\n"
    "runpy.run_path(os.path.join(root, 'tools', 'audit_store_present.py'), run_name='__main__')\n")


@pytest.mark.parametrize("t0,backend", [(False, "local"), (True, "selfhost")])
def test_audit_store_present_resolves_the_backend_it_was_given(tmp_path, t0, backend):
    """R1253 #3 / review round 5 N7: --backend must reach updater.config, which freezes BACKEND from the environment
    at its FIRST import - so the tool sets it before anything (the post-T0 guard included) imports updater. Only a
    fresh process is an honest test: in this one updater.config was imported long ago."""
    import subprocess
    flag = tmp_path / "CUTOVER"
    if t0:
        flag.write_text("")
    env = {k: v for k, v in os.environ.items()
           if k not in ("ECONDL_ROOT", "ECONDL_DATA", "ECONDL_CATALOG", "AQUEDUCT_DATA_ROOT")}
    env.update(AQUEDUCT_BACKEND="r2", AQUEDUCT_STATE_DIR=str(tmp_path / "state"), PYTHONDONTWRITEBYTECODE="1")
    r = subprocess.run([sys.executable, "-B", "-c", _ASP_DRIVER, ROOT, str(flag), "--backend", backend,
                        "--source", "zz_no_such_source"], capture_output=True, text=True, env=env, timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
    assert f"backend resolved: {backend}" in r.stdout, r.stdout[-2000:]
