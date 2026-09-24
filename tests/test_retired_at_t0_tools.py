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
