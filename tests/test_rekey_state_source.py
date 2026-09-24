"""tools/rekey_state_source.py - the state half of the 13F entry's rename (sec_edgar -> sec_edgar_13f)."""
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from tools import rekey_state_source as R
from updater.state import StateStore


def _db(tmp_path):
    p = str(tmp_path / "state.db")
    StateStore(p)                                           # the real schema
    c = sqlite3.connect(p)
    c.execute("INSERT INTO source_state(source_id, strategy, cadence, status) "
              "VALUES ('sec_edgar','giant_changed_units','quarterly','ok')")
    c.execute("INSERT INTO unit_state(source_id, unit_id, status) VALUES ('sec_edgar','_all','no_change')")
    for i in range(3):
        c.execute("INSERT INTO runs(ts_utc, source_id, unit_id) VALUES ('2026-09-2%dT00:00:00+00:00','sec_edgar','_all')" % i)
    c.execute("INSERT INTO full_rederive_owed(source_id, note) VALUES ('sec_edgar','csv coherence unmet')")
    c.execute("INSERT INTO source_state(source_id, status) VALUES ('ecb','ok')")          # untouched
    c.commit()
    c.close()
    return p


ARGS = ["--from", "sec_edgar", "--to", "sec_edgar_13f", "--expect", "source_state=1,unit_state=1,runs=3",
        "--drop", "full_rederive_owed"]


def _rows(p, sid):
    c = sqlite3.connect(p)
    try:
        return R.counts(c, sid)
    finally:
        c.close()


def test_a_dry_run_writes_nothing(tmp_path, capsys):
    p = _db(tmp_path)
    assert R.main(ARGS + ["--db", p]) == 0
    assert "dry run" in capsys.readouterr().out
    assert _rows(p, "sec_edgar")["runs"] == 3 and sum(_rows(p, "sec_edgar_13f").values()) == 0


def test_apply_moves_every_row_and_drops_the_named_table(tmp_path):
    p = _db(tmp_path)
    assert R.main(ARGS + ["--db", p, "--apply"]) == 0
    assert sum(_rows(p, "sec_edgar").values()) == 0, "no old-id row left for the D1 sync to push"
    new = _rows(p, "sec_edgar_13f")
    assert (new["source_state"], new["unit_state"], new["runs"], new["full_rederive_owed"]) == (1, 1, 3, 0)
    c = sqlite3.connect(p)
    assert c.execute("SELECT strategy FROM source_state WHERE source_id='sec_edgar_13f'").fetchone() == \
        ("giant_changed_units",)
    assert c.execute("SELECT COUNT(*) FROM source_state WHERE source_id='ecb'").fetchone() == (1,)
    c.close()


def test_a_count_that_moved_is_refused(tmp_path):
    p = _db(tmp_path)
    with pytest.raises(SystemExit, match="not what was planned"):
        R.main(["--from", "sec_edgar", "--to", "sec_edgar_13f", "--expect", "source_state=1,unit_state=1,runs=2",
                "--drop", "full_rederive_owed", "--db", p, "--apply"])
    assert _rows(p, "sec_edgar")["runs"] == 3, "nothing written"


def test_an_unplanned_table_is_refused(tmp_path):
    p = _db(tmp_path)
    with pytest.raises(SystemExit, match="not planned"):
        R.main(["--from", "sec_edgar", "--to", "sec_edgar_13f", "--expect", "source_state=1,unit_state=1,runs=3",
                "--db", p, "--apply"])                        # the owed row is neither expected nor dropped


def test_an_occupied_target_is_refused(tmp_path):
    p = _db(tmp_path)
    c = sqlite3.connect(p)
    c.execute("INSERT INTO unit_state(source_id, unit_id) VALUES ('sec_edgar_13f','_all')")
    c.commit()
    c.close()
    with pytest.raises(SystemExit, match="already has rows"):
        R.main(ARGS + ["--db", p, "--apply"])


def test_a_held_lease_is_refused_and_an_expired_one_is_not(tmp_path):
    p = _db(tmp_path)
    c = sqlite3.connect(p)
    later = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(timespec="seconds")
    c.execute("INSERT INTO leases VALUES ('sec_edgar/_all','ci', ?)", (later,))
    c.commit()
    with pytest.raises(SystemExit, match="a run holds"):
        R.main(ARGS + ["--db", p, "--apply"])
    earlier = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(timespec="seconds")
    c.execute("UPDATE leases SET expires_utc=?", (earlier,))
    c.commit()
    c.close()
    assert R.main(ARGS + ["--db", p, "--apply"]) == 0


def test_a_failure_mid_move_rolls_everything_back(tmp_path, monkeypatch):
    p = _db(tmp_path)
    real = R.counts
    calls = {"n": 0}

    def counts(con, sid):
        calls["n"] += 1
        got = real(con, sid)
        if sid == "sec_edgar" and calls["n"] > 2:            # the post-move assertion sees a row left
            got["runs"] = 1
        return got
    monkeypatch.setattr(R, "counts", counts)
    with pytest.raises(RuntimeError, match="still present"):
        R.main(ARGS + ["--db", p, "--apply"])
    monkeypatch.setattr(R, "counts", real)
    assert _rows(p, "sec_edgar")["runs"] == 3 and sum(_rows(p, "sec_edgar_13f").values()) == 0, "rolled back"
