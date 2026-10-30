"""wid is resumable and stops before the pass's window end (2026-10-30, PLAN R10.3 step 4, W3b).

WHAT WAS WRONG (measured on main f5fb6609d by review AR-406, F:/work/ar406/pk_main_kill.out):
  * The marker was saved inside the loop only when `len(done) % 25 == 0`. On a re-issue every country
    already has an entry, so the count never moves (424, and 424 % 25 = 24), and a pass killed
    mid-loop (run_local_heavy.ps1's taskkill) lost every stamp it had written.
  * A pure value revision (WID's usual re-issue, R764) added no row, `tally.added_unit(n - before)`
    scored it EMPTY, and with more than 10 countries finalize() raised a false "structural break" -
    AFTER the marker had stamped every country. The orchestrator books that as partial with no
    Result, so nothing derived and the following pass skipped every country.
  * merge_cursor_map's "cap hit" was thrown away, and the 50,000 cursors (about six countries) were
    the derive set.
  * Nothing in wid knew the pass's run ceiling.

WHAT THESE TESTS HOLD.
  (1) A REAL process death (os._exit in a subprocess) mid re-issue keeps the merged countries stamped
      OWED; the following pass, through the REAL orchestrate.run_once, does not download them again
      AND derives their served ids, then makes the marks final.
  (2) A revision-only re-issue does not raise; it reports the merge-measured changed keys.
  (3) A raise after merges (a structural country) leaves the merged countries owed, and a later pass
      hands their keys over without downloading them.
  (4) The window cap: max(floor, 2 x longest), the floor alone, no ceiling = no-op, a capped stop is
      partial with no last_success (real run_once), a cap that fires on stamped countries stays ok.
  (5) Past CURSOR_CAP changed keys the set is reported unknown and full_rederive_owed is booked.
  (6) Timing lines on every outcome; the real skip gate for header-only entities.
The fixture is a copy of AR-406's planted-kill harness (F:/work/ar406/planted_kill.py): a fake WID
listing, 3 variables x 3 years per country, every country republished. Nothing goes to the network.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
import types

import pyarrow as pa
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import blob, config, derive, orchestrate, registry  # noqa: E402
from updater.errors import DefinitiveError  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.fetchers import wid  # noqa: E402

OLD, NEW, SIZE = "2026-07-25 18:58", "2026-09-01 23:55", "17M"
N = 30                                              # > finalize's empty-window floor of 10
COUNTRIES = sorted(f"{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(N))
VARS = ("ahwealj999", "sptincj992", "anninci999")


def _csv(country, bump):
    lines = ["country;variable;percentile;year;value;age;pop"]
    for v in VARS:
        for y in (2022, 2023, 2024):
            lines.append(f"{country};{v};p0p100;{y};{100.0 + y + bump};999;j")
    return "\n".join(lines) + "\n"


def _keys(country):
    return [f"WID:{v}:p0p100:999:j:{country}" for v in VARS]


class _Resp:
    def __init__(self, text, status=200):
        self.status_code = status
        self.text = text
        self.content = text.encode()


def _listing(countries, mod=NEW, size=SIZE):
    rows = "".join(f'<tr><td><a href="WID_data_{c}.csv">WID_data_{c}.csv</a></td>'
                   f'<td align="right">{mod}  </td><td align="right">{size}</td></tr>\n'
                   for c in countries)
    return f"<html><table>{rows}</table></html>"


def _setup(out, countries=COUNTRIES):
    """Every country stored at the OLD values with a FINAL stamp at the old listing time."""
    os.makedirs(out, exist_ok=True)
    done = {}
    for c in countries:
        keys, dates, vals, _ = wid._parse(_csv(c, bump=0.0), c)
        blob.write_table_atomic(os.path.join(out, f"{c}.parquet"),
                                pa.table({"series_key": pa.array(keys, pa.string()),
                                          "obs_date": pa.array(dates, pa.date32()),
                                          "value": pa.array(vals, pa.float64())}))
        done[c] = f"{OLD}|{SIZE}"
    blob.write_bytes_atomic(os.path.join(out, wid.DONE_FILE), json.dumps(done).encode())


def _marker(out):
    raw = blob.read_bytes(os.path.join(out, wid.DONE_FILE))
    return json.loads(raw) if raw else {}


class _Clock:
    """wid's clock. Starts at the real time of its first read (so it lines up with a real run ceiling)
    and moves only when a fake download advances it."""

    def __init__(self):
        self.t = None

    def time(self):
        import time as _t
        if self.t is None:
            self.t = _t.time()
        return self.t


@pytest.fixture
def env(tmp_path, monkeypatch):
    """wid on a local store under tmp_path, a fake WID, a fake clock and no run ceiling."""
    for k in ("AQUEDUCT_BACKEND", "AQUEDUCT_WID_MARGIN_MIN"):
        monkeypatch.delenv(k, raising=False)
    data = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    clock = _Clock()
    monkeypatch.setattr(wid, "time", clock)
    e = types.SimpleNamespace(out=str(data / "clean_full" / "wid"), clock=clock, fetched=[],
                              countries=list(COUNTRIES), bump=1.0, dur=1.0, bodies={})

    def get(url, headers=None, timeout=None):
        if url == wid.INDEX:
            return _Resp(_listing(e.countries))
        c = url.rsplit("WID_data_", 1)[1][:-4]
        e.fetched.append(c)
        clock.time()
        clock.t += e.dur
        if c in e.bodies:
            return e.bodies[c]
        return _Resp(_csv(c, bump=e.bump))

    monkeypatch.setattr(wid.requests, "get", get)
    return e


# ---- (1) a REAL process death, then the REAL orchestrator --------------------------------------

_CHILD = textwrap.dedent('''
    import json, os, sys
    ROOT, DATA, KILL_AFTER, COUNTRIES = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4].split(",")
    KILL_ON_FETCH = sys.argv[5]
    os.environ["AQUEDUCT_DATA_ROOT"] = DATA
    os.environ.pop("AQUEDUCT_BACKEND", None)
    sys.path.insert(0, ROOT)
    from updater import merge
    from updater.strategies.fetchers import wid
    def body(c):
        lines = ["country;variable;percentile;year;value;age;pop"]
        for v in ("ahwealj999", "sptincj992", "anninci999"):
            for y in (2022, 2023, 2024):
                lines.append(f"{c};{v};p0p100;{y};{100.0 + y + 1.0};999;j")
        return "\\n".join(lines) + "\\n"
    class R:
        def __init__(self, t):
            self.status_code, self.text, self.content = 200, t, t.encode()
    def get(url, headers=None, timeout=None):
        if url == wid.INDEX:
            return R("".join('<tr><td><a href="WID_data_%%s.csv">x</a></td><td align="right">%(new)s  </td>'
                             '<td align="right">%(size)s</td></tr>' %% c for c in COUNTRIES))
        c = url.rsplit("WID_data_", 1)[1][:-4]
        print("FETCH", c, flush=True)
        if c == KILL_ON_FETCH:
            print("PLANTED KILL in the download of", c, flush=True)
            os._exit(137)
        return R(body(c))
    wid.requests.get = get
    real, n = merge.merge_and_write, [0]
    def counting(*a, **k):
        r = real(*a, **k)
        n[0] += 1
        if KILL_AFTER and n[0] >= KILL_AFTER:
            print("PLANTED KILL after merge", n[0], flush=True)
            os._exit(137)                       # a hard kill: no except, no finally, no save
        return r
    wid.merge.merge_and_write = counting
    print("CHILD_STATUS", wid.update(None, None).status, flush=True)
''') % {"new": NEW, "size": SIZE}


def _child(tmp_path, data_root, kill_after=0, kill_on_fetch="-"):
    script = tmp_path / "child.py"
    script.write_text(_CHILD, encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k != "AQUEDUCT_BACKEND"}
    p = subprocess.run([sys.executable, "-B", str(script), ROOT, data_root, str(kill_after),
                        ",".join(COUNTRIES), kill_on_fetch], capture_output=True, text=True, cwd=str(tmp_path),
                       env=env, timeout=300)
    fetched = [ln.split()[1] for ln in p.stdout.splitlines() if ln.startswith("FETCH ")]
    return p.returncode, p.stdout + p.stderr, fetched


def _catalog(path, ids):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, metadata TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?,?)", [(i, "wid", "{}") for i in ids])
    con.commit()
    con.close()


ALL_IDS = sorted(f"wid:{k}" for c in COUNTRIES for k in _keys(c))


@pytest.fixture
def rig(tmp_path, monkeypatch, env):
    """The REAL orchestrate.run_once for the registry entry `wid`, whose strategy calls the REAL
    wid.update; the REAL mapping, derive_and_put and resolver; the CSV PUT goes to a list."""
    for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_BUDGET_MIN",
              "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_DERIVE_BUDGET_MIN", "AQUEDUCT_FLOW_DERIVE_MAX_ROWS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    data = tmp_path / "data"
    os.makedirs(data / "_aqueduct", exist_ok=True)
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    monkeypatch.setenv("ECONDL_DATA", str(data / "clean_full"))
    _catalog(tmp_path / "catalog.db", ALL_IDS)
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "catalog.db"))
    orchestrate._REG_ENTRIES = None

    class _Blob:
        def __init__(self):
            self.keys = []

        def put_atomic(self, key, data):
            self.keys.append(key)

    r = types.SimpleNamespace(data=str(data / "clean_full"), blob=_Blob(),
                              state=StateStore(str(data / "_aqueduct" / "state.db")), env=env)
    unit = next(u for u in registry.all_units() if u.source_id == "wid")
    real = orchestrate.get_strategy(unit.strategy)

    class _Strategy:                                     # is_due / detect_change forced; run is REAL
        def is_due(self, unit, us, **k):
            return True

        def detect_change(self, unit, us):
            return "wid-index:test"

        def run(self, unit, since=None):
            return real.run(unit, since=since)

    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: _Strategy())

    def run():
        r.blob.keys.clear()
        return orchestrate.run_once(sources=["wid"], store=r.state, blob=r.blob)

    def put_ids():
        want = {derive.r2_key(i): i for i in ALL_IDS}
        return sorted(want[k] for k in r.blob.keys if k in want)

    r.run, r.put_ids = run, put_ids
    yield r
    r.state.close()


def test_a_hard_kill_mid_reissue_keeps_owed_stamps_and_the_next_pass_derives_them(rig, tmp_path):
    out = rig.env.out
    _setup(out)
    rc, log, fetched = _child(tmp_path, rig.data, kill_after=8)
    assert rc == 137, log
    assert fetched == COUNTRIES[:8], fetched
    done = _marker(out)                                   # the ARTEFACT, read through blob (R273)
    owed = sorted(c for c, s in done.items() if s == f"{NEW}|{SIZE}|owed")
    # merges 1..7 landed their stamp; merge 8 landed its data and died before its stamp
    assert owed == COUNTRIES[:7], ("the stamps written before the kill must survive it "
                                   f"(main: the % 25 save never ran on a re-issue): {done}")
    # AH's merge PUBLISHED and the process died before its owed stamp: the "merging|" form says so
    assert done["AH"] == f"merging|{OLD}|{SIZE}", done["AH"]
    assert "[wid] country 8/30 start AH" in log and "country 8/30 done" not in log, log

    results = rig.run()                                   # the following pass, REAL orchestrator
    assert results == [("wid/_all", "ok")], results
    assert rig.env.fetched == COUNTRIES[7:], ("(a) a killed-and-stamped country is not downloaded "
                                              f"again: {rig.env.fetched}")
    # (b) and its keys are handed over: every served id of every country is PUT, the 7 carried ones
    # included (a carried country reports all of its keys; the 23 merged here changed every value)
    assert rig.put_ids() == ALL_IDS, sorted(set(ALL_IDS) - set(rig.put_ids()))[:5]
    final = _marker(out)
    assert all(s == f"{NEW}|{SIZE}" for s in final.values()), final
    row = rig.state.get_unit("wid", "_all")
    assert row["status"] == "ok" and row["last_success_utc"], row


def test_a_hard_kill_inside_a_download_keeps_the_last_merged_country_stamped(rig, tmp_path):
    """The kill lands in country 8's DOWNLOAD (no merge open). Country 7's owed stamp must already be on
    disk: the save after each merge, not only the one before the next merge, is what puts it there."""
    out = rig.env.out
    _setup(out)
    rc, log, fetched = _child(tmp_path, rig.data, kill_on_fetch="AH")
    assert rc == 137 and fetched == COUNTRIES[:8], log
    done = _marker(out)
    assert sorted(c for c, s in done.items() if s == f"{NEW}|{SIZE}|owed") == COUNTRIES[:7], done
    assert done["AH"] == f"{OLD}|{SIZE}", "a country killed before its merge keeps its old stamp"
    results = rig.run()
    assert results == [("wid/_all", "ok")], results
    assert rig.env.fetched == COUNTRIES[7:], rig.env.fetched
    assert rig.put_ids() == ALL_IDS


# ---- (2)-(3) the change signal and the owed marks without a kill ---------------------------------

def test_a_revision_only_reissue_does_not_raise_and_reports_the_changed_keys(env):
    _setup(env.out)
    res = wid.update(None, None)                          # main: DefinitiveError "structural break"
    assert res.status == "ok", res
    assert "30 sub-unit(s) revised" in res.error, res.error
    assert sorted(res.changed_keys) == sorted(k for c in COUNTRIES for k in _keys(c))
    assert not res.cursor_cap_hit
    assert all(s == f"{NEW}|{SIZE}" for s in _marker(env.out).values())


def test_an_identical_reissue_is_no_change_and_does_not_raise(env):
    _setup(env.out)
    env.bump = 0.0                                        # listing moved, values did not
    res = wid.update(None, None)
    assert res.status == "no_change" and res.changed_keys == {}, res
    assert all(s == f"{NEW}|{SIZE}" for s in _marker(env.out).values()), "nothing owed: final"


def test_a_raise_after_merges_keeps_the_owed_marks_and_a_later_pass_hands_them_over(env):
    _setup(env.out)
    env.bodies["AE"] = _Resp("<html>maintenance</html>")  # not the WID CSV -> structural -> raise
    with pytest.raises(DefinitiveError):
        wid.update(None, None)
    done = _marker(env.out)
    merged = [c for c in COUNTRIES if c != "AE"]
    assert all(done[c] == f"{NEW}|{SIZE}|owed" for c in merged), done
    assert done["AE"] == f"{OLD}|{SIZE}", "the refused country keeps its old stamp"
    env.bodies.clear()
    env.fetched.clear()
    res = wid.update(None, None)
    assert env.fetched == ["AE"], env.fetched
    assert res.status == "ok", res
    assert set(res.changed_keys) == {k for c in COUNTRIES for k in _keys(c)}
    assert all(s == f"{NEW}|{SIZE}" for s in _marker(env.out).values())


def test_a_carry_only_pass_runs_the_csv_step(env):
    """Nothing to download, owed marks to hand over: never no_change (the CSV step needs ok/partial)."""
    _setup(env.out)
    done = {c: f"{NEW}|{SIZE}|owed" for c in COUNTRIES[:2]}
    done.update({c: f"{NEW}|{SIZE}" for c in COUNTRIES[2:]})
    wid._save_done(env.out, done)
    res = wid.update(None, None)
    assert env.fetched == []
    assert res.status == "ok" and "carried" in res.error, res
    assert set(res.changed_keys) == set(_keys("AA") + _keys("AB"))


def test_an_owed_mark_past_the_ceiling_stays_owed_and_the_pass_is_partial(env, monkeypatch):
    """Reading a stored file for a carry takes time too: past the ceiling it is not done, the mark
    stays owed, and the pass is partial (never ok, never a cleared mark without its keys)."""
    _setup(env.out)
    done = {c: f"{NEW}|{SIZE}" for c in COUNTRIES}
    done["AA"] = f"{NEW}|{SIZE}|owed"
    wid._save_done(env.out, done)
    monkeypatch.setenv("AQUEDUCT_WID_MARGIN_MIN", "0.1")
    _ceiling(env, monkeypatch, 5)                          # 5 s left, floor 6 s
    res = wid.update(None, None)
    assert res.status == "partial" and res.changed_keys == {}, res
    assert _marker(env.out)["AA"] == f"{NEW}|{SIZE}|owed"


def test_negative_control_a_quiet_pass_without_marks_is_no_change(env):
    _setup(env.out)
    wid._save_done(env.out, {c: f"{NEW}|{SIZE}" for c in COUNTRIES})
    res = wid.update(None, None)
    assert env.fetched == [] and res.status == "no_change" and res.changed_keys == {}, res


def test_an_unreadable_owed_country_keeps_its_mark(env, monkeypatch):
    _setup(env.out)
    done = {c: f"{NEW}|{SIZE}" for c in COUNTRIES}
    done["AA"] = f"{NEW}|{SIZE}|owed"
    wid._save_done(env.out, done)

    def boom(path):
        raise OSError("read failed")

    monkeypatch.setattr(wid, "_carry", boom)
    res = wid.update(None, None)
    assert _marker(env.out)["AA"] == f"{NEW}|{SIZE}|owed", "never cleared without its keys"
    assert res.changed_keys == {}


# ---- (4) the window cap ---------------------------------------------------------------------------

def _ceiling(env, monkeypatch, seconds):
    env.clock.time()
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", env.clock.t + seconds)


def test_the_margin_is_twice_the_longest_country(env, monkeypatch):
    """10 s per country, floor 6 s, ceiling at +60 s. Starts at 0,10,20,30,40 (left 20 >= 2 x 10);
    at 50 only 10 s are left. Kills `1 x longest` (6 fetched) and `<=` (4 fetched)."""
    _setup(env.out)
    env.dur = 10.0
    monkeypatch.setenv("AQUEDUCT_WID_MARGIN_MIN", "0.1")
    _ceiling(env, monkeypatch, 60)
    res = wid.update(None, None)
    assert env.fetched == COUNTRIES[:5], env.fetched
    assert res.status == "partial" and "25 countries deferred" in res.error, res
    done = _marker(env.out)
    assert all(done[c] == f"{OLD}|{SIZE}" for c in COUNTRIES[5:]), "deferred = untouched"
    assert all(done[c] == f"{NEW}|{SIZE}" for c in COUNTRIES[:5])


def test_the_floor_alone_stops_the_first_country(env, monkeypatch):
    _setup(env.out)
    monkeypatch.setenv("AQUEDUCT_WID_MARGIN_MIN", "0.1")             # 6 s
    _ceiling(env, monkeypatch, 5)
    res = wid.update(None, None)
    assert env.fetched == [] and res.status == "partial" and "30 countries deferred" in res.error, res


def test_no_ceiling_is_a_strict_no_op(env, monkeypatch):
    _setup(env.out)
    env.dur = 300.0                                       # 30 x 5 min = 150 min < BUDGET_S (180)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    res = wid.update(None, None)
    assert env.fetched == COUNTRIES and res.status == "ok", res


@pytest.mark.parametrize("bad", [float("nan"), True, "soon", float("inf")])
def test_an_unusable_ceiling_is_no_ceiling(env, monkeypatch, bad):
    _setup(env.out)
    env.dur = 300.0
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", bad)
    res = wid.update(None, None)
    assert env.fetched == COUNTRIES and res.status == "ok", res


def test_the_cap_on_already_stamped_countries_stays_ok(env, monkeypatch):
    """AR-406 O6(i): only AA is republished; the cap would fire after it, but every later country is
    already stamped, so nothing is deferred and the pass is ok."""
    _setup(env.out)
    wid._save_done(env.out, {c: (f"{OLD}|{SIZE}" if c == "AA" else f"{NEW}|{SIZE}") for c in COUNTRIES})
    env.dur = 10.0
    monkeypatch.setenv("AQUEDUCT_WID_MARGIN_MIN", "0.1")
    _ceiling(env, monkeypatch, 15)
    res = wid.update(None, None)
    assert env.fetched == ["AA"] and res.status == "ok", res


@pytest.mark.parametrize("raw,want", [("nan", 300.0), ("-1", 300.0), ("x", 300.0), ("inf", 300.0),
                                      ("", 300.0), ("0", 0.0), ("2", 120.0)])
def test_the_margin_floor_setting(monkeypatch, raw, want):
    monkeypatch.setenv("AQUEDUCT_WID_MARGIN_MIN", raw)
    assert wid._margin_floor_s() == want


def test_a_capped_stop_through_the_real_run_once_is_partial_and_sets_no_success(rig, monkeypatch):
    """The ceiling comes from run_once itself (AQUEDUCT_RUN_BUDGET_MIN = 0.5 -> 30 s). 8 s per country,
    floor 6 s: starts at +0 and +8 (22 s left >= 16), stops at +16 (14 s left)."""
    _setup(rig.env.out)
    rig.env.dur = 8.0
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "0.5")
    monkeypatch.setenv("AQUEDUCT_WID_MARGIN_MIN", "0.1")
    results = rig.run()
    assert rig.env.fetched == COUNTRIES[:2], rig.env.fetched
    assert results == [("wid/_all", "partial")], results
    row = rig.state.get_unit("wid", "_all")
    assert row["status"] == "partial" and row["last_success_utc"] is None, row
    assert "28 countries deferred" in (row["last_error"] or ""), row
    assert rig.put_ids() == sorted(f"wid:{k}" for c in COUNTRIES[:2] for k in _keys(c))
    assert all(s == f"{NEW}|{SIZE}" for c, s in _marker(rig.env.out).items() if c in COUNTRIES[:2])


# ---- (5) past the cap --------------------------------------------------------------------------

def test_past_the_cap_the_changed_set_is_unknown_and_a_full_rederive_is_booked(rig, monkeypatch):
    _setup(rig.env.out)
    monkeypatch.setattr(wid, "CURSOR_CAP", 20)            # 30 countries x 3 keys = 90 > 20
    results = rig.run()
    assert results[0][1] in ("ok", "partial"), results
    assert rig.state.db.execute(
        "SELECT 1 FROM full_rederive_owed WHERE source_id='wid'").fetchone(), "the debt is booked"
    # the marks are final: the Result carried cursor_cap_hit, which the orchestrator booked above
    assert all(s == f"{NEW}|{SIZE}" for s in _marker(rig.env.out).values())


def test_under_the_cap_no_debt_is_booked(rig):
    _setup(rig.env.out)
    results = rig.run()
    assert results == [("wid/_all", "ok")], results
    assert not rig.state.db.execute("SELECT 1 FROM full_rederive_owed WHERE source_id='wid'").fetchone()
    assert rig.put_ids() == ALL_IDS


# ---- (6) timing lines and the real skip gate -----------------------------------------------------

def test_every_attempted_country_prints_a_start_and_a_done_line(env, capsys, monkeypatch):
    _setup(env.out)
    env.countries = COUNTRIES[:5] + ["ON"]
    env.bodies["AB"] = _Resp("", status=503)                           # transient
    env.bodies["ON"] = _Resp("country;variable;percentile;year;value;age;pop\n")   # empty
    wid._save_done(env.out, {"AC": f"{NEW}|{SIZE}"})                    # skipped: no lines
    real = wid.merge.merge_and_write

    def merge_fail_on_ad(path, *a, **k):
        if path.endswith("AD.parquet"):
            raise RuntimeError("disk")
        return real(path, *a, **k)

    monkeypatch.setattr(wid.merge, "merge_and_write", merge_fail_on_ad)
    with pytest.raises(RuntimeError):
        wid.update(None, None)
    out = capsys.readouterr().out
    starts = [ln for ln in out.splitlines() if " start " in ln and ln.startswith("[wid] country")]
    dones = [ln for ln in out.splitlines() if " done " in ln and ln.startswith("[wid] country")]
    assert [ln.split()[4] for ln in starts] == ["AA", "AB", "AD"], out
    assert [ln.split()[4].rstrip(":") for ln in dones] == ["AA", "AB", "AD"], out
    assert "download" in dones[0] and "parse" in dones[0] and "merge" in dones[0], dones[0]
    assert "transient (HTTP 503)" in dones[1]
    assert "AD: error" in dones[2], dones[2]
    assert "[wid] no run ceiling set" in out


def test_the_ceiling_is_printed_in_utc(env, capsys, monkeypatch):
    _setup(env.out, COUNTRIES[:1])
    env.countries = COUNTRIES[:1]
    _ceiling(env, monkeypatch, 3600)
    wid.update(None, None)
    assert "[wid] run ceiling " in capsys.readouterr().out


def test_the_real_gate_skips_a_stamped_empty_entity_without_a_parquet(env):
    """tests/test_wid_empty_entities_are_stamped.py re-implements the gate; this runs the real one."""
    os.makedirs(env.out, exist_ok=True)
    env.countries = ["ON", "FR"]
    wid._save_done(env.out, {"ON": f"{NEW}|{SIZE}|empty", "FR": f"{NEW}|{SIZE}"})
    wid.update(None, None)
    assert env.fetched == ["FR"], "ON skipped with no parquet; FR has a final stamp and no parquet"
