"""tools/check_istat_rate.py: the receipt must be able to FAIL (DESIGN 12 TF, M26; verdict V2).

Planted logs only (no lock, no request). A window is CLOSED: [t, t + 60] holds both ends. Exit 0 only with
tokens > 0 and max <= 4; exit 1 over the budget or a start outside [slot, slot + SLIP_MAX_S]; exit 2 when it
cannot measure (no log, an unparseable line, 0 tokens in the range)."""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
_spec = importlib.util.spec_from_file_location("check_istat_rate", os.path.join(ROOT, "tools", "check_istat_rate.py"))
chk = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(chk)

T0 = 1_800_000_000.0
RANGE = ["--from", str(T0 - 10), "--to", str(T0 + 3600)]


def R(t, tok, part="updater", mode="shared", flow="F1", kind="http", cls="data", host="esploradati.istat.it"):
    return (f"R {chk.iso(t)} {t:.3f} pid=11 part={part} host={host} kind={kind} class={cls} flow={flow} "
            f"mode={mode} waited=0.000 tok={tok}")


def O(tok, start, slot, outcome="200", el=0.5):
    return f"O {chk.iso(start)} tok={tok} outcome={outcome} el={el:.3f} start={start:.3f} slip={start - slot:.3f}"


def _write(path, lines):
    path.write_text("".join(x + "\n" for x in lines), encoding="ascii")
    return str(path)


def run(tmp_path, lines, *extra, process=None):
    log = _write(tmp_path / "requests_test.log", lines)
    argv = RANGE + ["--log", log] + list(extra)
    if process is not None:
        argv += ["--process-log", _write(tmp_path / "guard.log", process)]
    return chk.main(argv)


def test_tf_exactly_four_starts_in_60_s_exit_0(tmp_path, capsys):
    lines = [R(T0 + 16 * i, f"11-{i}") for i in range(4)] + [R(T0 + 64 + 16 * i, f"11-{4 + i}") for i in range(4)]
    assert run(tmp_path, lines) == 0
    out = capsys.readouterr().out
    assert "overall: 4" in out and "OK:" in out


def test_tf_five_starts_in_60_s_exit_1(tmp_path, capsys):
    lines = [R(T0 + 15 * i, f"11-{i}") for i in range(5)]           # 0, 15, 30, 45, 60
    assert run(tmp_path, lines) == 1
    out = capsys.readouterr().out
    assert "overall: 5" in out and "OVER BUDGET" in out and "FAIL" in out


def test_tf_the_window_is_closed_at_both_ends(tmp_path):
    """0, 20, 40 and two at exactly 60: a half-open window [t, t+60) would count 4."""
    lines = [R(T0, "a"), R(T0 + 20, "b"), R(T0 + 40, "c"), R(T0 + 60, "d"), R(T0 + 60.0, "e")]
    assert run(tmp_path, lines) == 1
    assert chk.max_in_window([0, 20, 40, 60, 60], 60.0)[0] == 5
    assert chk.max_in_window([0, 20, 40, 60.001], 60.0)[0] == 3


def test_tf_an_empty_log_cannot_measure_exit_2(tmp_path, capsys):
    """M26: an empty log is not a pass."""
    assert run(tmp_path, []) == 2
    assert "CANNOT MEASURE: 0 tokens" in capsys.readouterr().out


def test_tf_no_log_at_all_exit_2(tmp_path):
    assert chk.main(RANGE + ["--log", str(tmp_path / "absent.log")]) == 2


def test_tf_an_unparseable_line_exit_2(tmp_path, capsys):
    lines = [R(T0, "a"), "this is not a pacer line", R(T0 + 16, "b")]
    assert run(tmp_path, lines) == 2
    assert "unparseable" in capsys.readouterr().out


def test_tf_a_line_missing_a_field_is_unparseable(tmp_path):
    assert run(tmp_path, [R(T0, "a").replace(" mode=shared", "")]) == 2


def test_tf_zero_tokens_in_the_range_exit_2(tmp_path):
    log = _write(tmp_path / "requests_x.log", [R(T0 - 9000, "a")])
    assert chk.main(RANGE + ["--log", log]) == 2


def test_tf_a_failsafe_process_log_line_is_counted(tmp_path, capsys):
    """4 shared starts in the request log plus one fail-safe token that only the guard log carries = 5."""
    lines = [R(T0 + 16 * i, f"11-{i}") for i in range(4)]
    guard = ["[20:14:50] some crawler line",
             "[istat-pacer] FAILSAFE-TOKEN " + R(T0 + 50, "12-f1", part="crawler", mode="failsafe"),
             "[20:14:51] another line"]
    assert run(tmp_path, lines, process=guard) == 1
    out = capsys.readouterr().out
    assert "fail-safe tokens: 1" in out and "overall: 5" in out and "1 pacer line(s)" in out


def test_tf_an_unlogged_line_from_a_process_log_is_counted(tmp_path, capsys):
    lines = [R(T0 + 16 * i, f"11-{i}") for i in range(3)]
    guard = ["x [istat-pacer] UNLOGGED " + R(T0 + 48, "11-3"), "[istat-pacer] UNLOGGED " + R(T0 + 60, "11-4")]
    assert run(tmp_path, lines, process=guard) == 1
    assert "overall: 5" in capsys.readouterr().out


def test_tf_the_same_line_in_both_logs_is_counted_once(tmp_path, capsys):
    lines = [R(T0 + 16 * i, f"11-{i}") for i in range(4)]
    guard = ["[istat-pacer] UNLOGGED " + lines[3]]
    assert run(tmp_path, lines, process=guard) == 0
    assert "overall: 4" in capsys.readouterr().out


def test_v2_actual_starts_not_slots_decide_the_window(tmp_path, capsys):
    """Slots 16 s apart, but one send slipped 10 s: 5 ACTUAL starts in 60 s even though the SLOTS allow 4.
    The slip is outside SLIP_MAX_S as well, so both reasons fail it."""
    slots = [T0 + 16 * i for i in range(5)]                         # 0 16 32 48 64
    starts = list(slots)
    starts[0] = slots[0] + 10.0                                      # 10 ... 64: five in [10, 70]
    lines = [R(s, f"11-{i}") for i, s in enumerate(slots)] + [O(f"11-{i}", st, sl)
                                                              for i, (st, sl) in enumerate(zip(starts, slots))]
    assert run(tmp_path, lines) == 1
    out = capsys.readouterr().out
    assert "overall: 5" in out and "LATE START tok=11-0 slip 10.000" in out and "max slip" in out


def test_v2_a_slip_within_the_limit_passes_and_is_printed(tmp_path, capsys):
    slots = [T0 + 16 * i for i in range(4)]
    lines = [R(s, f"11-{i}") for i, s in enumerate(slots)] + [O(f"11-{i}", s + 1.5, s) for i, s in enumerate(slots)]
    assert run(tmp_path, lines) == 0
    assert "max slip (actual start - slot): 1.500 s" in capsys.readouterr().out


def test_v2_a_late_start_alone_fails_even_under_the_budget(tmp_path):
    lines = [R(T0, "a"), O("a", T0 + 3.0, T0)]
    assert run(tmp_path, lines) == 1


def test_v2_an_early_start_fails(tmp_path):
    lines = [R(T0, "a"), O("a", T0 - 0.5, T0)]
    assert run(tmp_path, lines) == 1


def test_v2_an_abandoned_token_is_not_a_start(tmp_path, capsys):
    """V2(b): a token whose process woke too late is abandoned (X line) and never sent."""
    lines = [R(T0 + 16 * i, f"11-{i}") for i in range(4)] + [R(T0 + 64, "11-x"),
                                                             f"X {chk.iso(T0 + 70)} tok=11-x reason=late slip=6.000"]
    lines.insert(2, R(T0 + 20, "11-y"))
    lines.append(f"X {chk.iso(T0 + 25)} tok=11-y reason=late slip=5.000")
    assert run(tmp_path, lines) == 0
    assert "abandoned, never sent: 2" in capsys.readouterr().out


def test_a_named_log_that_is_missing_cannot_measure(tmp_path, capsys):
    """Review 2(a): another log (or a process log) existing must not hide a named log that is not there."""
    ok = _write(tmp_path / "ok.log", [R(T0 + 16 * i, f"11-{i}") for i in range(4)])
    assert chk.main(RANGE + ["--log", ok, "--log", str(tmp_path / "gone.log")]) == 2
    guard = _write(tmp_path / "guard.log", ["nothing"])
    assert chk.main(RANGE + ["--log", str(tmp_path / "gone.log"), "--process-log", guard]) == 2
    assert chk.main(RANGE + ["--log", ok, "--process-log", str(tmp_path / "gone_guard.log")]) == 2
    assert "named log missing" in capsys.readouterr().out


def test_an_outcome_without_its_reservation_is_counted_and_cannot_measure(tmp_path, capsys):
    """Review 2(b): the R line sits in the month the slot was RESERVED in. An O line whose R line is in no log
    read is still a send: it is counted, and the run cannot measure (exit 2) - or fails (exit 1) if it breaks
    the budget even so."""
    lines = [R(T0 + 16 * i, f"11-{i}") for i in range(3)] + [O("99-1", T0 + 50, T0 + 50)]
    assert run(tmp_path, lines) == 2
    out = capsys.readouterr().out
    assert "ORPHAN OUTCOMES: 1" in out and "overall: 4" in out
    lines = [R(T0 + 16 * i, f"11-{i}") for i in range(4)] + [O("99-1", T0 + 55, T0 + 55)]
    assert run(tmp_path, lines) == 1


def test_windows_that_straddle_the_range_start_are_counted(tmp_path, capsys):
    """Review 2(c): 5 starts in [from - 30, from + 30] break the budget although only 3 are in the range."""
    t_from = T0 + 1000.0
    lines = [R(t_from - 30 + 15 * i, f"11-{i}") for i in range(5)]
    log = _write(tmp_path / "requests_e.log", lines)
    assert chk.main(["--from", str(t_from), "--to", str(t_from + 3600), "--log", log]) == 1
    assert "overall: 5" in capsys.readouterr().out
    t_to = T0 + 1000.0                                  # and at the range end
    log = _write(tmp_path / "requests_f.log", [R(t_to - 30 + 15 * i, f"12-{i}") for i in range(5)])
    assert chk.main(["--from", str(t_to - 3600), "--to", str(t_to), "--log", log]) == 1


def test_gets_per_flow_penalties_and_outcomes_are_reported(tmp_path, capsys):
    lines = [R(T0, "a", flow="22_289"), R(T0 + 16, "b", flow="22_289"), R(T0 + 32, "c", flow="151_874"),
             R(T0 + 48, "d", part="crawler", flow="-"), O("a", T0, T0, "200"), O("b", T0 + 16, T0 + 16, "404"),
             f"P {chk.iso(T0 + 40)} {T0 + 40:.3f} pid=11 part=updater seconds=60.000 until={T0 + 100:.3f}",
             f"T {chk.iso(T0)} {T0:.3f} pid=11 turn={T0 + 3600:.3f} what=begin"]
    assert run(tmp_path, lines) == 0
    out = capsys.readouterr().out
    assert "updater GETs per flow: 2 flow(s), min 1, median 1.5, max 2, mean 1.50" in out
    assert "penalties (429) in range: 1; longest 60 s" in out
    assert "200 1" in out and "404 1" in out and "(no outcome line) 2" in out
    assert "updater       max 3" in out and "crawler       max 1" in out


def test_the_cli_exit_codes_are_the_contract(tmp_path):
    """The same through the command line, as the receipt runs it."""
    ok = _write(tmp_path / "ok.log", [R(T0 + 16 * i, f"11-{i}") for i in range(4)])
    bad = _write(tmp_path / "bad.log", [R(T0 + 15 * i, f"11-{i}") for i in range(5)])
    empty = _write(tmp_path / "empty.log", [])
    tool = os.path.join(ROOT, "tools", "check_istat_rate.py")
    codes = [subprocess.run([sys.executable, "-B", tool] + RANGE + ["--log", p], capture_output=True,
                            text=True, timeout=120).returncode for p in (ok, bad, empty)]
    assert codes == [0, 1, 2]


def test_the_scaled_window_option(tmp_path):
    lines = [R(T0 + 0.26 * i, f"11-{i}") for i in range(8)]
    assert run(tmp_path, lines, "--window", "1.0") == 0
    lines = [R(T0 + 0.24 * i, f"11-{i}") for i in range(8)]
    assert run(tmp_path, lines, "--window", "1.0") == 1


def test_default_logs_name_every_month_of_the_range(monkeypatch, tmp_path):
    monkeypatch.setattr(chk.pacer, "STATE_DIR", str(tmp_path))
    import datetime as dt
    a = dt.datetime(2026, 11, 30, 23, 0, tzinfo=dt.timezone.utc).timestamp()
    b = dt.datetime(2027, 1, 1, 1, 0, tzinfo=dt.timezone.utc).timestamp()
    names = [os.path.basename(p) for p in chk.default_logs(a, b)]
    assert names == ["requests_202611.log", "requests_202612.log", "requests_202701.log"]


@pytest.mark.parametrize("s,expect", [("2026-11-24T13:30:00Z", 1795527000.0), ("1795527000", 1795527000.0),
                                      ("2026-11-24T13:30:00", 1795527000.0),
                                      ("2026-11-24T14:30:00+01:00", 1795527000.0)])
def test_time_parsing_is_utc(s, expect):
    assert chk.parse_time(s) == expect
