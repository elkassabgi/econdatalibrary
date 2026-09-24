"""Is econ ready for T0? Run this on the workstation, from the production checkout, IMMEDIATELY before the
machine-wide CUTOVER flag is created (docs/ECON_SELF_HOSTING_PLAN.md step 6a). The flag is created only when
this prints READY (review AR-153: nothing checked that the legacy roads were gone before the switch that
assumes they are).

    python tools/selfhost/t0_ready.py            -> one line per check, then READY (exit 0) or NOT READY (1)

Each check is mechanical and names what fails:
  legacy-catalogue   tests/catalog_db_legacy.txt is empty: every catalogue open goes through
                     core.catalog_path (after T0 a legacy opener writes the build without the writer lock)
  legacy-remote-d1   LEGACY_REMOTE_D1 in tests/test_d1_remote.py is empty: no script reaches remote D1 except
                     through core.d1_remote (after T0 the rest would write the retired copy, or fail)
  ratchets           the four repo ratchets pass (no new catalogue opener, remote-D1 caller, raw boto3 client,
                     or cloud_client caller)
  launcher           tools/run_local_heavy.ps1 runs the updater self-hosted: AQUEDUCT_BACKEND selfhost, no
                     --pull-state / --push-state (the preflight would refuse every run otherwise)
  preflight          updater/run.py's own post-T0 preflight passes from THIS checkout, run as if cut over
  ci-writers         updater-daily, updater-heavy and sec-edgar-daily are disabled on GitHub (gh workflow list)
  edge-state         the deployed edge reports edge_state "users" (plan step 5 is done)
  state-db           the live state.db has unit_state and source_state rows: the origin copies build the
                     freshness projection (/v1/last-updates) from it - the production catalog.db has none of
                     those tables (R1186)
  d1-only-sources    every source whose freshness only D1 holds today (sync_state_d1.DATA_THROUGH_FROM_D1:
                     sec_edgar) has a local writer (LOCAL_FRESHNESS_WRITERS). D1 is frozen after T0, and the
                     catalogue copy is not their truth (R737), so without one their data_through reads null
                     and their state stops moving (R1191)
  flag              the flag does not exist yet (information: a READY with the flag present is a late check)
Nothing here writes anything.
"""
from __future__ import annotations

import ast
import contextlib
import io
import json
import os
import re
import subprocess
import sys
import types

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
CI_WRITERS = ("updater-daily", "updater-heavy", "sec-edgar-daily")
LAUNCHER = os.path.join(ROOT, "tools", "run_local_heavy.ps1")


def legacy_catalogue(root: str = ROOT) -> tuple[bool, str]:
    with open(os.path.join(root, "tests", "catalog_db_legacy.txt"), encoding="utf-8") as fh:
        n = [l for l in fh if l.strip() and not l.startswith("#")]
    return not n, f"{len(n)} file(s) still open the catalogue outside core.catalog_path"


NAME = "LEGACY_REMOTE_D1"


def legacy_remote_d1(root: str = ROOT) -> tuple[bool, str]:
    """Read the list only when it is bound EXACTLY ONCE, as a plain set literal or a bare set(), and never
    changed anywhere in the file (no |=, no .add/.update/..., no second assignment). Anything else is
    "cannot tell", never "empty" (R1183: any call read as empty; R1185: a later |= or .add was not seen)."""
    tree = ast.parse(open(os.path.join(root, "tests", "test_d1_remote.py"), encoding="utf-8").read())
    binds, changes = [], []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == NAME for t in node.targets):
            binds.append(node)
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)) and getattr(node.target, "id", None) == NAME:
            changes.append(ast.unparse(node)[:60])
        elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
              and getattr(node.func.value, "id", None) == NAME):
            changes.append(ast.unparse(node)[:60])
        elif isinstance(node, (ast.Delete, ast.Global)) and NAME in ast.unparse(node):
            changes.append(ast.unparse(node)[:60])
    if not binds:
        return False, f"{NAME} not found in tests/test_d1_remote.py - cannot tell"
    if len(binds) > 1 or changes:
        return False, (f"{NAME} is bound {len(binds)} times and changed by {changes[:2]} - cannot tell; "
                       "write it once as a set literal")
    v = binds[0].value
    try:
        if isinstance(v, ast.Set):
            value = ast.literal_eval(v)
        elif isinstance(v, ast.Call) and getattr(v.func, "id", None) == "set" and not v.args and not v.keywords:
            value = set()
        else:
            raise ValueError
    except ValueError:                                  # {*X}, frozenset({...}), set([...]), a name ...
        return False, f"{NAME} is written as {ast.unparse(v)[:60]!r} - cannot tell; use a set literal"
    return not value, f"{len(value)} file(s) still call D1 remotely outside core.d1_remote"


def ratchets(root: str = ROOT) -> tuple[bool, str]:
    tests = ["tests/test_repo_walk.py", "tests/test_catalog_path.py", "tests/test_d1_remote.py",
             "tests/test_r2_cutover_guard.py"]
    r = subprocess.run([sys.executable, "-B", "-m", "pytest", "-q", "-p", "no:cacheprovider", *tests], cwd=root,
                       capture_output=True, text=True, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
    last = (r.stdout.strip().splitlines() or ["no output"])[-1]
    return r.returncode == 0, last


def launcher(path: str = LAUNCHER) -> tuple[bool, str]:
    src = open(path, encoding="utf-8").read()
    code = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
    problems = []
    backends = re.findall(r"\$env:AQUEDUCT_BACKEND\s*=\s*['\"]([^'\"]*)['\"]", code)
    if not backends or any(b.lower() != "selfhost" for b in backends):
        problems.append(f"AQUEDUCT_BACKEND set to {backends or 'nothing'}")
    for flag in ("--pull-state", "--push-state"):
        if flag in code:
            problems.append(f"runs {flag}")
    return not problems, "; ".join(problems) or "self-hosted"


def preflight() -> tuple[bool, str]:
    """updater/run.py's _selfhost_preflight, run as if the flag existed, in this process's environment plus
    AQUEDUCT_BACKEND=selfhost (the launcher's own setting is the 'launcher' check)."""
    from core import cutover
    from updater import run
    args = types.SimpleNamespace(pull_state=False, push_state=False)
    err = io.StringIO()
    saved, backend = cutover.is_cut_over, os.environ.get("AQUEDUCT_BACKEND")
    cutover.is_cut_over = lambda *a, **k: True
    os.environ["AQUEDUCT_BACKEND"] = "selfhost"
    try:
        with contextlib.redirect_stderr(err):
            run._selfhost_preflight(args)
        return True, f"passes from {ROOT}"
    except SystemExit:
        return False, err.getvalue().strip().splitlines()[-1] if err.getvalue().strip() else "refused"
    finally:
        cutover.is_cut_over = saved
        if backend is None:
            os.environ.pop("AQUEDUCT_BACKEND", None)
        else:
            os.environ["AQUEDUCT_BACKEND"] = backend


def ci_writers(run=subprocess.run) -> tuple[bool, str]:
    r = run(["gh", "workflow", "list", "--all", "--json", "name,path,state"], cwd=ROOT, capture_output=True,
            text=True)
    if r.returncode != 0:
        return False, f"gh workflow list failed: {r.stderr.strip()[:200]} - cannot tell"
    by_file = {os.path.splitext(os.path.basename(w["path"]))[0]: w["state"] for w in json.loads(r.stdout)}
    bad = [f"{n}={by_file.get(n, 'MISSING')}" for n in CI_WRITERS if by_file.get(n) != "disabled_manually"]
    return not bad, ("still enabled: " + ", ".join(bad)) if bad else "all disabled"


def edge_state() -> tuple[bool, str]:
    sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
    import watch_edge
    status, body = watch_edge.get_json(watch_edge.EDGE + "/v1/edge-status")
    if status != 200 or not isinstance(body, dict):
        return False, f"/v1/edge-status answered {status} - the step-5 edge is not deployed"
    return body.get("edge_state") == "users", f"edge_state={body.get('edge_state')!r} forward={body.get('forward')!r}"


def state_db(path: str | None = None) -> tuple[bool, str]:
    import sqlite3
    from core import catalog_path
    p = path or os.path.join(catalog_path.LIVE_STATE_DIR, "state.db")
    if not os.path.isfile(p):
        return False, f"no state.db at {p}"
    con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        n = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("unit_state", "source_state")}
    except sqlite3.Error as e:
        return False, f"{p}: {e}"
    finally:
        con.close()
    return all(n.values()), f"{p}: {n}"


def d1_only_sources() -> tuple[bool, str]:
    from core import sync_state_d1
    missing = sorted(set(sync_state_d1.DATA_THROUGH_FROM_D1) - set(sync_state_d1.LOCAL_FRESHNESS_WRITERS))
    return not missing, (("no local freshness writer yet for: " + ", ".join(missing)) if missing
                         else "every D1-stamped source has a local writer")


def flag() -> tuple[bool, str]:
    from core import cutover
    return (not cutover.is_cut_over()), ("not set yet" if not cutover.is_cut_over() else "ALREADY SET")


CHECKS = [("legacy-catalogue", legacy_catalogue), ("legacy-remote-d1", legacy_remote_d1), ("ratchets", ratchets),
          ("launcher", launcher), ("preflight", preflight), ("ci-writers", ci_writers),
          ("edge-state", edge_state), ("state-db", state_db), ("d1-only-sources", d1_only_sources),
          ("flag", flag)]


def main() -> int:
    ok_all = True
    for name, fn in CHECKS:
        try:
            ok, detail = fn()
        except Exception as e:  # noqa: BLE001 - a check that cannot run is a failure, never a pass
            ok, detail = False, f"could not check: {type(e).__name__}: {e}"
        ok_all &= ok
        print(f"{'PASS' if ok else 'FAIL'}  {name:17s} {detail}")
    print("READY" if ok_all else "NOT READY - do not create the CUTOVER flag")
    return 0 if ok_all else 1


if __name__ == "__main__":
    raise SystemExit(main())
