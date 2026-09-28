"""tools/prune_series_cursors.py after T0: its write to the updater's state.db (a DELETE and a VACUUM) holds
core.catalog_path's single-writer lock - the catalogue guard refuses VACUUM without it after T0 (review R1219) -
and is refused at once, with nothing deleted, while another process holds the lock. Before T0 nothing changes."""
import os
import subprocess
import sys

import pytest

from core import catalog_path, cutover
from updater.state import StateStore

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import prune_series_cursors as psc  # noqa: E402

LONG = "x" * 200                                  # a legacy key (> MIN_LEGACY_KEY)
SHORT = "ds1"                                      # a current key


@pytest.fixture
def world(tmp_path, monkeypatch):
    state = tmp_path / "state.db"
    st = StateStore(path=str(state))
    st.put_series_cursors("ons_uk", {SHORT: "2026-01-01", LONG: "2020-01-01"})
    st.close()
    monkeypatch.setattr(psc, "StateStore", lambda: StateStore(path=str(state)))
    monkeypatch.setattr(psc, "keep_set", lambda source: {SHORT})
    monkeypatch.setattr(psc, "runs_in_flight", lambda: [])
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "no_build" / "catalog.db"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(sys, "argv", ["prune_series_cursors.py", "ons_uk", "--apply"])
    return tmp_path, state


def _cursors(state):
    st = StateStore(path=str(state))
    try:
        return set(st.series_cursors("ons_uk"))
    finally:
        st.close()


def test_before_t0_the_prune_runs_as_before(world):
    tmp_path, state = world
    assert psc.main() == 0
    assert _cursors(state) == {SHORT}


def test_after_t0_the_prune_holds_the_lock(world, monkeypatch, capsys):
    tmp_path, state = world
    (tmp_path / "CUTOVER").write_text("")
    held = []
    real = catalog_path.write_session

    def spy():
        cm = real()

        class Wrap:
            def __enter__(self):
                r = cm.__enter__()
                held.append(catalog_path._held is not None)
                return r

            def __exit__(self, *e):
                return cm.__exit__(*e)
        return Wrap()
    monkeypatch.setattr(catalog_path, "write_session", spy)
    assert psc.main() == 0
    assert held == [True], "the delete and the VACUUM ran under the writer lock"
    assert _cursors(state) == {SHORT}
    assert "nothing to push" in capsys.readouterr().out


_HOLDER = """
import sys
sys.path.insert(0, sys.argv[1])
from core import catalog_path
fh = open(sys.argv[2], "a+b")
catalog_path._lock(fh)
print("held", flush=True)
sys.stdin.readline()
"""


def test_after_t0_the_prune_is_refused_while_another_process_holds_the_lock(world):
    tmp_path, state = world
    (tmp_path / "CUTOVER").write_text("")
    os.makedirs(os.path.dirname(catalog_path.LOCK_PATH), exist_ok=True)
    p = subprocess.Popen([sys.executable, "-c", _HOLDER, ROOT, catalog_path.LOCK_PATH],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "held"
        with pytest.raises(cutover.CutoverRefused, match="another process holds"):
            psc.main()
        assert _cursors(state) == {SHORT, LONG}, "nothing deleted"
    finally:
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=30)
