"""tools/fix_statcan_frequency.py: dry writes nothing; --apply rewrites exactly the planned rows, by primary key,
guarded on the old value, with the rollback file first; every refusal stops BEFORE any write (R1608)."""
import hashlib
import importlib.util
import json
import os
import sqlite3
import sys

import psutil
import pytest

from core import catalog_path as cp
from core import cutover
from updater import writer_lock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SCHEMA = """
CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
  geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, last_updated TEXT, metadata TEXT);
CREATE INDEX ix_series_source_id ON series(source_id);
CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
"""
# (series_id, frequency now) and the cube's StatCan frequencyCode
ROWS = [("statcan:10", "A"), ("statcan:10#a", "A"), ("statcan:10#b", "A"),    # 9 Quarterly -> Q
        ("statcan:20", "M"),                                                    # 18 Occasional -> irregular
        ("statcan:30", "A"),                                                    # 12 Annual: unchanged
        ("statcan:40", None),                                                   # no code: unchanged
        ("statcan:50", "D")]                                                    # 16 every 5 years -> NULL
CODES = {10: 9, 20: 18, 30: 12, 40: None, 50: 16}
WANT = {"statcan:10": "Q", "statcan:10#a": "Q", "statcan:10#b": "Q", "statcan:20": "irregular",
        "statcan:30": "A", "statcan:40": None, "statcan:50": None}
N_CHANGE = 5


def _load():
    spec = importlib.util.spec_from_file_location("_fix_statcan_frequency_test",
                                                  os.path.join(ROOT, "tools", "fix_statcan_frequency.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def world(tmp_path, monkeypatch):
    live = tmp_path / "live"
    store = live / "data" / "clean_full" / "statcan"
    store.mkdir(parents=True)
    db = cp.under(str(live))
    con = sqlite3.connect(db)
    con.executescript(SCHEMA)
    for sid, f in ROWS:
        con.execute("INSERT INTO series (series_id, source_id, title, frequency) VALUES (?,?,?,?)",
                    (sid, "statcan", f"t {sid}", f))
        con.execute("INSERT INTO series_fts VALUES (?,?,?)", (sid, f"t {sid}", "Canada"))
    con.execute("INSERT INTO series (series_id, source_id, title, frequency) VALUES ('statcan9:x','statcan9','x','A')")
    con.execute("INSERT INTO series (series_id, source_id, title, frequency) VALUES ('abs:X','abs','x','A')")
    con.commit()
    con.close()
    for pid, code in CODES.items():
        (store / f"{pid}.done").write_text(json.dumps({"productId": pid, "frequencyCode": code}), encoding="utf-8")
    m = _load()
    monkeypatch.setattr(m, "LIVE_ROOT", str(live))
    monkeypatch.setattr(m, "CATALOGUE", db)
    monkeypatch.setattr(m, "STORE", str(store))
    monkeypatch.setattr(m, "RECEIPTS", str(tmp_path / "receipts"))
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(live / "logs"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_CUTOVER"))
    monkeypatch.setattr(m._cat(), "db_holders", lambda path, pids=None: [])
    return {"m": m, "db": db, "store": str(store), "live": live, "receipts": tmp_path / "receipts", "tmp": tmp_path}


def _freqs(db):
    con = sqlite3.connect(db)
    try:
        return dict(con.execute("SELECT series_id, frequency FROM series"))
    finally:
        con.close()


def _md5(p):
    return hashlib.md5(open(p, "rb").read()).hexdigest()


def _apply(w, *extra):
    return w["m"].main(["--db", w["db"], "--store", w["store"], "--apply", *extra])


def test_dry_run_writes_nothing(world, capsys):
    before = _md5(world["db"])
    assert world["m"].main(["--db", world["db"], "--store", world["store"]]) == 0
    assert _md5(world["db"]) == before and not world["receipts"].exists()
    out = capsys.readouterr().out
    assert f"rows to change: {N_CHANGE}" in out and "18          M -> irregular" in out


def test_apply_changes_exactly_the_planned_rows(world):
    fts_before = sqlite3.connect(world["db"]).execute("SELECT rowid, * FROM series_fts ORDER BY rowid").fetchall()
    assert _apply(world, "--expect-rows", str(N_CHANGE), "--batch", "2") == 0
    got = _freqs(world["db"])
    assert {k: v for k, v in got.items() if k.startswith("statcan:")} == WANT
    assert got["abs:X"] == "A" and got["statcan9:x"] == "A"          # outside the range: untouched
    assert sqlite3.connect(world["db"]).execute("SELECT rowid, * FROM series_fts ORDER BY rowid").fetchall() == fts_before
    (run,) = list(world["receipts"].iterdir())
    rb = [json.loads(ln) for ln in open(run / "rollback.jsonl", encoding="utf-8")]
    assert len(rb) == N_CHANGE and {r["series_id"] for r in rb} == {s for s, f in ROWS if WANT[s] != f}
    rec = json.load(open(run / "receipt.json", encoding="utf-8"))
    assert rec["rows_written"] == N_CHANGE and rec["rows_not_as_planned"] == 0
    assert open(run / "changed_ids.txt", encoding="utf-8").read().split() == sorted(r["series_id"] for r in rb)
    assert writer_lock.owner("statcan_writer") is None                # released
    # a re-run finds nothing left to do
    assert world["m"].main(["--db", world["db"], "--store", world["store"]]) == 0
    assert _apply(world, "--expect-rows", "0") == 0


def test_rollback_restores_the_old_values(world):
    original = _freqs(world["db"])
    assert _apply(world, "--expect-rows", str(N_CHANGE)) == 0
    (run,) = list(world["receipts"].iterdir())
    assert _apply(world, "--rollback", str(run / "rollback.jsonl"), "--expect-rows", str(N_CHANGE)) == 0
    assert _freqs(world["db"]) == original


@pytest.mark.parametrize("case", ["no_expect", "wrong_expect", "t0", "holder", "lock_held", "not_live",
                                  "missing_sidecar", "fts_frequency"])
def test_every_refusal_writes_nothing(world, monkeypatch, case):
    m, args = world["m"], ["--expect-rows", str(N_CHANGE)]
    if case == "no_expect":
        args = []
    elif case == "wrong_expect":
        args = ["--expect-rows", str(N_CHANGE + 1)]
    elif case == "t0":
        flag = world["tmp"] / "CUTOVER"
        flag.write_text("")
        monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    elif case == "holder":
        monkeypatch.setattr(m._cat(), "db_holders", lambda path, pids=None: [(4242, "python.exe")])
    elif case == "lock_held":
        other = psutil.Process(os.getppid())                         # a live process that is not this one
        os.makedirs(writer_lock.LOCK_DIR, exist_ok=True)
        with open(writer_lock.lock_path("statcan_writer"), "w", encoding="utf-8") as fh:
            json.dump({"pid": other.pid, "started": other.create_time(), "what": "the lane"}, fh)
    elif case == "not_live":
        monkeypatch.setattr(m, "LIVE_ROOT", str(world["tmp"] / "elsewhere"))
    elif case == "missing_sidecar":
        os.remove(os.path.join(world["store"], "50.done"))
    elif case == "fts_frequency":
        con = sqlite3.connect(world["db"])
        con.executescript("DROP TABLE series_fts; CREATE VIRTUAL TABLE series_fts USING fts5(series_id, frequency);")
        con.close()
    before = _md5(world["db"])
    with pytest.raises((SystemExit, cutover.CutoverRefused)) as e:
        _apply(world, *args)
    assert e.value.args and str(e.value.args[0]).startswith(("REFUSING", "refused")), e.value.args
    assert _md5(world["db"]) == before and not world["receipts"].exists()


def test_a_row_changed_since_the_plan_stops_its_batch(world):
    m = world["m"]
    con = cp.connect_path(world["db"], write=True)
    changes = [("statcan:10", "A", "Q"), ("statcan:20", "Q", "irregular")]   # statcan:20 holds M, not Q
    with pytest.raises(RuntimeError, match="changed since the plan"):
        m.apply_changes(con, changes, batch=10)
    con.close()
    got = _freqs(world["db"])
    assert got["statcan:10"] == "A" and got["statcan:20"] == "M"             # the whole batch rolled back


def test_the_rollback_file_is_written_before_any_update(world, monkeypatch):
    def boom(con, changes, batch):
        raise RuntimeError("killed")
    monkeypatch.setattr(world["m"], "apply_changes", boom)
    with pytest.raises(RuntimeError, match="killed"):
        _apply(world, "--expect-rows", str(N_CHANGE))
    (run,) = list(world["receipts"].iterdir())
    assert sum(1 for _ in open(run / "rollback.jsonl", encoding="utf-8")) == N_CHANGE
    assert not (run / "receipt.json").exists()
    assert writer_lock.owner("statcan_writer") is None                       # released on the way out


def test_the_tool_uses_the_cataloguers_map():
    m = _load()
    import importlib
    cat = m._cat()
    assert m.make_plan({"statcan:1": "A"}, {"1": 9})[0] == [("statcan:1", "A", cat.FREQ[9])]
    assert sys.modules["_catalog_statcan_tables_for_freq_fix"] is cat and importlib
