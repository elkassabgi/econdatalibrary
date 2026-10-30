"""AR-410 probe, copied in at AR-410's request (PR #208 fix): is the W6 stop sentence true when the margin is set by the merge
reserve (newest slice) rather than by 2 x the slice's bound? Reuses the head suite's fixture."""
import re

from tests.test_imts_window_cap import F, SRC, env, _slices  # noqa: F401  (env is the fixture)


def test_probe_newest_cut_then_window_between_2lb_and_need(env, monkeypatch, capsys):
    env.setup("base")
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")           # 60 s floor
    monkeypatch.setenv("AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "1")    # 60 s reserve
    env.imf.per_request, env.imf.chunks = 0.0, 1
    env.ceiling(100.0)                     # closed slices need 60 s: all 8 taken; newest needs 120 s
    r1 = F.update(None, None)
    assert r1.status == "partial", r1.error
    assert len(_slices()) == 8, _slices()
    env.imf.per_request, env.imf.chunks = 400.0, 8
    env.ceiling(130.0)                     # newest needs 120 s: starts, cut at the window end
    r2 = F.update(None, None)
    assert r2.status == "partial", r2.error
    cut = F._load_marker(F._slice_dir(SRC)).get("cut", {})
    lb = cut["2025-2027"]["elapsed_s"]
    capsys.readouterr()
    w = 2 * lb + 30.0                      # 2 x lb <= window < 2 x lb + 60 s reserve = the margin
    env.ceiling(w)
    r3 = F.update(None, None)
    out = capsys.readouterr().out
    line = [ln for ln in out.splitlines() if "STOP before the window end" in ln]
    print("PROBE lb", lb, "window", w, "LINE", line)
    m = re.search(r"under the ([0-9.]+)-min margin", out)
    need = float(m.group(1)) * 60
    claims_under_2lb = "under 2 x the slice's measured lower bound" in out
    print("PROBE need", need, "claims window < 2 x lb:", claims_under_2lb, "| true:", w < 2 * lb)
    assert not (claims_under_2lb and not w < 2 * lb), "FALSE SENTENCE: window is not under 2 x the bound"
