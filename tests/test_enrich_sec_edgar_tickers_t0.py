"""tools/enrich_sec_edgar_tickers.py on the chokepoints (plan step 1): the catalogue through core.catalog_path (the
build after T0, written from the live checkout under the lock), D1 through core.d1_remote.execute_file, and
--d1 refused after T0 before anything is fetched."""
import os
import sqlite3
import sys

import pytest

from core import catalog_path, cutover, d1_remote
from updater import blob

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import enrich_sec_edgar_tickers as E  # noqa: E402

TICKERS = ({1652044: {"GOOGL", "GOOG"}}, {"GOOGL": 1652044, "GOOG": 1652044})


def _catalogue(path):
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT)")
        c.execute("INSERT INTO series VALUES ('sec_edgar:GOOGL', 'sec_edgar', 'Alphabet Inc.', 'US')")
    c.close()


def _title(path):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return c.execute("SELECT title FROM series WHERE series_id='sec_edgar:GOOGL'").fetchone()[0]
    finally:
        c.close()


@pytest.fixture
def t0(tmp_path, monkeypatch):
    live = tmp_path / "live"
    (live / "data").mkdir(parents=True)
    build = live / "data" / "catalog.db"
    _catalogue(build)
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(live))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(blob, "_code_root", lambda: str(live))
    from updater import config as updater_config
    monkeypatch.setattr(updater_config, "ROOT", str(live))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(live / "data" / "clean_full"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(E, "sec_ticker_map", lambda: TICKERS)
    monkeypatch.setattr(d1_remote, "execute_file", lambda *a, **k: pytest.fail("D1 reached after T0"))
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path, build


def test_after_t0_d1_is_refused_before_anything(t0, monkeypatch):
    monkeypatch.setattr(E, "sec_ticker_map", lambda: pytest.fail("fetched before refusing"))
    monkeypatch.setattr(sys, "argv", ["enrich_sec_edgar_tickers.py", "--d1"])
    with pytest.raises(cutover.CutoverRefused, match="self-hosted since T0"):
        E.main()


def test_after_t0_titles_go_to_the_build_under_the_lock(t0, monkeypatch, capsys):
    tmp, build = t0
    seen = []
    real = catalog_path.connect

    def spy(*a, **k):
        if k.get("write"):
            seen.append(catalog_path._held is not None)
        return real(*a, **k)
    monkeypatch.setattr(catalog_path, "connect", spy)
    monkeypatch.setattr(sys, "argv", ["enrich_sec_edgar_tickers.py"])
    assert E.main() == 0
    assert _title(build) == "Alphabet Inc. (GOOGL, GOOG)"
    assert seen == [True] and catalog_path._held is None
    assert "blue/green swap" in capsys.readouterr().out


def test_after_t0_another_checkout_is_refused(t0, monkeypatch, tmp_path):
    tmp, build = t0
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["enrich_sec_edgar_tickers.py"])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        E.main()
    assert _title(build) == "Alphabet Inc."


def test_before_t0_d1_goes_through_the_chokepoint(tmp_path, monkeypatch):
    """Before T0 the local checkout's catalogue and D1 through d1_remote.execute_file: the series UPDATEs, then
    - only when all applied - the two-statement series_fts rebuild; a failed chunk stops before the rebuild."""
    checkout = tmp_path / "checkout" / "catalog.db"
    checkout.parent.mkdir()
    _catalogue(checkout)
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag"))
    monkeypatch.setattr(catalog_path, "CHECKOUT_PATH", str(checkout))
    monkeypatch.setattr(E, "ROOT", str(tmp_path))
    (tmp_path / "data").mkdir()
    monkeypatch.setattr(E, "sec_ticker_map", lambda: TICKERS)
    calls = []

    def execute_file(db, path, **k):
        calls.append((db, open(path, encoding="utf-8").read()))
        return ""
    monkeypatch.setattr(d1_remote, "execute_file", execute_file)
    monkeypatch.setattr(sys, "argv", ["enrich_sec_edgar_tickers.py", "--d1"])
    assert E.main() == 0
    assert _title(checkout) == "Alphabet Inc. (GOOGL, GOOG)"
    assert [c[0] for c in calls] == ["econ-catalog", "econ-catalog"]
    assert "UPDATE series SET title='Alphabet Inc. (GOOGL, GOOG)'" in calls[0][1]
    assert "DELETE FROM series_fts" in calls[1][1]

    calls.clear()

    def fails(db, path, **k):
        calls.append(db)
        raise RuntimeError("D1 econ-catalog: exit 1: boom")
    monkeypatch.setattr(d1_remote, "execute_file", fails)
    assert E.main() == 0
    assert calls == ["econ-catalog"], "the rebuild is skipped when the series UPDATEs did not all apply"
