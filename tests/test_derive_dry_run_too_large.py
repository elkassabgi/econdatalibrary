"""A dry run must not file an OVER-CAP series under "unresolvable".

"Unresolvable" means the STORE HAS NO DATA for this id. A table of 136,120,337 rows has plenty;
it is simply too big for the in-memory path. Filing one as the other put every over-cap table
under a cause nobody had checked (R219) — and it hid something worse: `--allow-stream` sat inert
in dry runs, because all of its handling lives in `_derive_and_put`, which the dry-run branch
never reaches. So the one mode you would use to PLAN a streaming campaign could neither exercise
the streaming path nor describe it honestly.

Both directions, because a label that is merely different is not a fix:
  * a TooLarge id is reported as too-large and counted apart from `unresolvable`;
  * an ordinary failure is STILL reported as unresolvable — the control that stops this test
    passing on a build that simply renamed every skip.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.derive_csv as cdc  # noqa: E402


def _run_dry(monkeypatch, capsys, raiser, extra_argv=()):
    monkeypatch.setattr(cdc, "_catalog_ids", lambda limit, source: [("zz:one", "zz")])
    monkeypatch.setattr(cdc, "_series_csv_bytes", raiser)
    monkeypatch.setattr(sys, "argv",
                        ["derive_csv.py", "--dry-run", "--source", "zz", *extra_argv])
    cdc.main()
    return capsys.readouterr().out


def test_an_over_cap_series_is_not_called_unresolvable(monkeypatch, capsys):
    def _too_big(_sid):
        raise cdc.TooLarge("136,120,337 rows exceeds the in-memory cap")

    out = _run_dry(monkeypatch, capsys, _too_big)
    assert "TOO LARGE for the in-memory path" in out, out
    assert "SKIP(unresolvable)" not in out, (
        "an over-cap table was filed as unresolvable — that word means the store holds NO data "
        "for the id, which is the opposite of the truth here (R219)")
    assert "too-large-for-memory 1" in out, (
        "the summary must count over-cap ids in their own bucket; folded into `unresolvable` "
        "the campaign you would plan from this output is the wrong campaign")
    assert "--allow-stream" in out, "the output must name the flag that would derive it"


def test_an_ordinary_failure_is_still_unresolvable(monkeypatch, capsys):
    """The control. Without it, a build that renamed EVERY skip would pass the test above."""
    def _broken(_sid):
        raise RuntimeError("no parquet for this id")

    out = _run_dry(monkeypatch, capsys, _broken)
    assert "SKIP(unresolvable)" in out, out
    assert "TOO LARGE for the in-memory path" not in out, out
    assert "too-large-for-memory" not in out, (
        "the over-cap counter appeared for an ordinary failure, so the two causes are still "
        "being conflated — in the other direction")


def test_allow_stream_reaches_the_streaming_path_in_a_dry_run(monkeypatch, capsys):
    """--allow-stream used to be inert here: its handling lives in `_derive_and_put`, which the
    dry-run branch never reaches. A dry run that cannot exercise the flag cannot plan with it."""
    def _too_big(_sid):
        raise cdc.TooLarge("too big for memory")

    calls = []

    def _fake_stream(sid, path):
        calls.append(sid)
        with open(path, "wb") as fh:
            fh.write(b"\x1f\x8b" + b"0" * 64)      # a plausible non-empty artefact

    monkeypatch.setattr(cdc, "_series_csv_to_file_sorted", _fake_stream)
    out = _run_dry(monkeypatch, capsys, _too_big, extra_argv=("--allow-stream",))
    assert calls == ["zz:one"], (
        f"the streaming derive was not reached with --allow-stream; called {calls!r}")
    assert "STREAMED zz:one" in out, out
    assert "deleted, nothing uploaded" in out, (
        "a dry run that leaves the artefact behind, or does not say it removed it, is not dry")
