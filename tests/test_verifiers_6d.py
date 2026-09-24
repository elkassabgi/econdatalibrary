"""EVERY FILE THAT READS THE CLOUD COPY AND DOES NOTHING AT T0, CLASSIFIED (plan section 5, step 6d: "the full list
is enumerated by grep in step 1").

After T0 the R2 guard stops WRITES only, and core.d1_remote still answers a plain read with the read token. So a
file that reads R2 or D1 keeps running after T0 against the FROZEN copy. What that means depends on what the file
does with the answer - each class below says. The inventory is COMPLETE by construction: every .py under tools/
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
CLOUD = re.compile(r"r2_util|d1_remote|head_object|list_objects|get_object|_d1_json|R2Blob|workers\.dev|"
                   r"boto3|wrangler[^\n]*\bd1\b|/d1/database/")
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
    "tools/billing_guard.py", "jobs/r2_bucket_sizes.py", "tools/selfhost/watch_edge.py",
    "tools/cost/bucket_prefixes.py", "tools/cost/burst_measure.py", "tools/cost/gzip_fraction.py",
    "tools/cost/idb_affected_series.py", "tools/cost/idb_baseline.py", "tools/cost/idb_by_dataset.py",
    "tools/cost/idb_starvation.py", "tools/cost/idb_underkeyed_licences.py", "tools/cost/idb_underkeyed_packages.py",
    "tools/cost/size_series_prefix.py", "tools/cost/smoke_public.py", "tools/cost/storage_saving.py",
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

CLASSES = {"VERIFIERS_6D": VERIFIERS_6D, "USER_FACING": USER_FACING, "CLOUD_COST": CLOUD_COST, "CLOUD_WRITE_REFUSED": CLOUD_WRITE_REFUSED,
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
    }
    for rel, src in plants.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(src, encoding="utf-8")
    assert _scan(str(tmp_path)) == {"tools/cost/new_probe.py", "core/new_reader.py", "tools/named_otherwise.py",
                                    "tools/comment_only.py"}


PULLERS = [("mirror_sync", ["--from-json", "x.json", "--apply"]), ("resync_and_repair", ["--source", "zz"]),
           ("sync_source_rows_d1_to_local", ["--source", "zz", "--apply"]),
           ("clear_csv_desktop_owed", ["--source", "zz", "--apply"]), ("repair_stale_csvs", ["--source", "zz"]),
           ("rekey_ons_uk", []), ("catalog_statcan_tables", ["--apply"]), ("catalog_worldbank_esg_gaps", ["--apply"])]


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
REFUSED_6D_MAIN = ["footer_diff", "audit_d1_vs_catalog", "audit_d1_source_counts"]
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
