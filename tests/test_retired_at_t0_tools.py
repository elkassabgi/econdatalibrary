"""Two tools whose work has no meaning after T0 refuse to run then (plan: RETIRE AT T0), before touching anything:
rebuild_cso_retired_from_csv (a completed one-shot; its output is in the local store) and sec_edgar_union_repair
(it reunites the local mirror with R2; after T0 there is one store). Before T0 they run as before."""
import os
import sys

import pytest

from core import cutover, r2_util

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import rebuild_cso_retired_from_csv  # noqa: E402
import sec_edgar_union_repair  # noqa: E402


@pytest.mark.parametrize("mod,argv", [(rebuild_cso_retired_from_csv, ["--upload"]),
                                      (sec_edgar_union_repair, ["--ticker", "XOM", "--apply"])])
def test_after_t0_they_refuse_before_anything(tmp_path, monkeypatch, mod, argv):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 reached"))
    monkeypatch.setattr(sys, "argv", [mod.__name__, *argv])
    with pytest.raises(cutover.CutoverRefused, match="after T0"):
        mod.main()


import subprocess  # noqa: E402

import _upload_biotrademerch_store  # noqa: E402
import _upload_clean_full_parquet  # noqa: E402
import delist_timeless_tables  # noqa: E402
import refresh_r2_catalog  # noqa: E402
import upload_statcan_store  # noqa: E402
from core import upload_r2  # noqa: E402

# The R2-copy tools (RETIRE AT T0 in tests/test_object_writers_ratchet.py): they only copy to or delete from R2
# (and D1), which are frozen after T0. Each refuses as its FIRST statement - before its arguments are parsed,
# before credentials, before any read. argv here is one each tool would otherwise act on or reject.
COPY_TOOLS = [(upload_r2, ["--bucket", "econ-data"]), (_upload_biotrademerch_store, []),
              (_upload_clean_full_parquet, []), (upload_statcan_store, []),
              (refresh_r2_catalog, ["--dry-run"]), (delist_timeless_tables, ["--apply"])]


@pytest.mark.parametrize("mod,argv", COPY_TOOLS, ids=lambda x: getattr(x, "__name__", ""))
def test_after_t0_the_r2_copy_tools_refuse_first(tmp_path, monkeypatch, mod, argv):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 reached"))
    from core import d1_remote
    monkeypatch.setattr(d1_remote, "run_json", lambda *a, **k: pytest.fail("D1 reached"))
    import argparse
    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", lambda *a, **k: pytest.fail("arguments parsed first"))
    monkeypatch.setattr(sys, "argv", [mod.__name__, *argv])
    with pytest.raises(cutover.CutoverRefused, match="self-hosted since T0"):
        mod.main()


@pytest.mark.parametrize("cmd", [["-m", "core.upload_r2", "--help"], ["tools/upload_statcan_store.py", "--help"],
                                 ["tools/refresh_r2_catalog.py", "--help"],
                                 ["tools/delist_timeless_tables.py", "--help"],
                                 ["tools/_upload_clean_full_parquet.py"]])
def test_as_a_script_the_refusal_imports_resolve(cmd):
    """Run the way people run them: the refusal's import of core must resolve from a script's own path. Only
    --help or a usage error (nothing is touched); on a machine past T0 the refusal itself is the answer."""
    r = subprocess.run([sys.executable, "-B", *cmd], cwd=ROOT, capture_output=True, text=True, timeout=120)
    out = r.stdout + r.stderr
    assert "ModuleNotFoundError" not in out and "ImportError" not in out, out[-800:]
    assert ("usage" in out.lower() and r.returncode in (0, 2)) or "self-hosted since T0" in out, out[-800:]


@pytest.mark.parametrize("mod,argv", [(rebuild_cso_retired_from_csv, []),
                                      (sec_edgar_union_repair, ["--ticker", "XOM"])])
def test_before_t0_they_run_as_before(tmp_path, monkeypatch, mod, argv):
    """Before T0 the refusal is not reached: they go on to R2 (stubbed to stop them there)."""
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag"))

    class Reached(Exception):
        pass
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: (_ for _ in ()).throw(Reached()))
    monkeypatch.setattr(sys, "argv", [mod.__name__, *argv])
    with pytest.raises(Reached):
        mod.main()
