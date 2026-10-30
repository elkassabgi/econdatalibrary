"""imf_imts_direct pulls in period slices, stops before the pass's window end, and keeps finished
slices until the merge succeeds (2026-10-30, PLAN R10.3 step 4).

WHAT WAS WRONG ON MAIN (0ba3b4a20; measured with the real fetcher and a planted-kill harness,
F:/work/econ_imts_cap_tmp/pkc_main.out, and by review AR-410):
  * imf_imts_direct went through _imf_direct.run: ONE whole-flow GET, sliced only after a size error
    that never happens on the desktop. Nothing read orchestrate._RUN_DEADLINE_TS, each request was
    bounded only per socket wait (300 s / 180 s, 4 retries), and a kill kept nothing. On 2026-10-10 the
    GET ran 6,575 s and reached 5,036,310,528 bytes before the ps1 hard stop.
  * The ingester's sliced fallback started at 1950 (the store starts 1948-12-31), keyed kept slices by
    LIST INDEX and by an IMF version that never moves (R1101), and built key dims from each slice's
    own attribute union.

WHAT THESE TESTS HOLD.
  (1) _plan: contiguous, disjoint, 1900..Y+1, bounds-named, and a pinned-clock year rollover.
  (2) Sliced == whole-flow: the same store rows, sidecar and SERVED CSV bytes, on a fake with A/Q/M/S
      and day periods, data from 1948 and Y+1 forecasts; distinct (key, date) == rows.
  (3) The window stop: margin, newest + reserve, merge reserve; a stop is partial with deferred units
      (health._DEFERRAL_BASE -> ROTATING once a slice was pulled), merged_rows=0, and the next pass
      re-pulls no finished closed slice.
  (4) Every request: timeout clamped, a trickling body stopped between chunks, retry sleeps counted,
      Retry-After honoured, 429/503 never read as empty.
  (5) Pinned dims fail closed (named TransientError), a slice with a different attribute set is
      refused by name and mints no key; a window's 400 is named as that window; a store older than
      1900 is refused.
  (6) A REAL process death (os._exit) after k slices resumes from the kept bounds, across a year
      rollover; the open window's age bound cannot livelock a pass-per-week cadence.
  (7) PR B ((b)/(c)/(j), tests/test_imts_owed_marks.py holds the rest): a revision-only pull reads ok and
      hands over the revised keys, where main reads no_change; a re-merge after a killed merge hands over
      every key, where main reads no_change; a structural pass with nothing owed raises as on main.
  (8) Through the REAL orchestrate.run_once: a stop is partial with no success and still reaches the
      csv retry-queue drain; a merging pass derives the tables it changed (main: every table).
  (9) The opt-in is pinned: only imf_imts_direct calls run_sliced, every other _imf_direct importer still
      calls _base.run, and the shared modules do not read the ceiling.
"""
from __future__ import annotations

import ast
import datetime as dt
import glob
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
import types
import urllib.request

import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
sys.path.insert(0, os.path.join(ROOT, "tests"))

import _imts_fake as fake  # noqa: E402
from updater import blob, config, derive, health, orchestrate, registry  # noqa: E402
from updater.errors import DefinitiveError, TransientError  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.fetchers import _imf_direct as base  # noqa: E402
from updater.strategies.fetchers import imf_imts_direct as F  # noqa: E402
from jobs import ingest_imf_direct as ing  # noqa: E402

T0 = dt.datetime(2026, 10, 30, 12, 0, tzinfo=dt.timezone.utc).timestamp()
SRC = "imf_imts_direct"
PLAN_2026 = ["1900-1959", "1960-1969", "1970-1979", "1980-1989", "1990-1999", "2000-2009",
             "2010-2019", "2020-2024", "2025-2027"]
TABLE_IDS = sorted(f"{SRC}:IMTS:{cp}.{f}.MG_CIF_USD" for cp, _c, f in fake.SERIES)


@pytest.fixture
def env(tmp_path, monkeypatch):
    for k in ("AQUEDUCT_BACKEND", "AQUEDUCT_IMTS_MARGIN_MIN", "AQUEDUCT_IMTS_MERGE_RESERVE_MIN",
              "AQUEDUCT_IMTS_CLOSED_MAX_AGE_DAYS", "AQUEDUCT_IMTS_OPEN_MAX_AGE_DAYS"):
        monkeypatch.delenv(k, raising=False)
    clock = fake.Clock(T0)
    imf = fake.Imf(clock)
    monkeypatch.setattr(F, "time", clock, raising=False)       # raising=False: the same file runs on main
    monkeypatch.setattr(urllib.request, "urlopen", imf.urlopen)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    # PR B reads state.db (read-only) to settle an owed mark: never the worktree's real one here
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "_aqueduct_absent" / "state.db"))
    monkeypatch.setattr(ing.time, "sleep", lambda s: None)        # main's retry sleeps (reference only)
    e = types.SimpleNamespace(tmp=tmp_path, clock=clock, imf=imf, mp=monkeypatch)

    def root(name):
        data = tmp_path / name / "data"
        monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
        return str(data / "clean_full")

    def store():
        return os.path.join(config.source_dir(SRC), f"{SRC}.parquet")

    def setup(up="base", settled=True):
        """The store as main's whole-flow ingester writes it (rows AND the real 5-dim sidecar).
        settled=True also writes the explicit "nothing owed" mark ({}) that a settled PR B pass leaves;
        settled=False is the store exactly as main leaves it (no mark: owed once, R1766)."""
        imf.up = up
        os.makedirs(config.source_dir(SRC), exist_ok=True)
        assert ing.pull("IMTS", "IMF.STA", SRC, out_path=store(), min_obs=0) > 0
        if settled:
            blob.write_bytes_atomic(os.path.join(config.source_dir(SRC), "_imts_owed.json"), b"{}")
        imf.requests.clear()

    def ceiling(seconds_left):
        monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", clock.t + seconds_left)

    e.root, e.store, e.setup, e.ceiling = root, store, setup, ceiling
    e.root("a")
    return e


def _rows(path):
    t = pq.read_table(path)
    return sorted(zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                      t.column("value").to_pylist()))


def _slices():
    d = F._slice_dir(SRC)
    return sorted(f for f in os.listdir(d) if f.startswith("slice_")) if os.path.isdir(d) else []


def _csv_bytes(data_root, ids):
    from core import derive_csv
    old = os.environ.get("ECONDL_DATA")
    os.environ["ECONDL_DATA"] = data_root
    try:
        return {i: derive_csv._series_csv_bytes(i) for i in ids}
    finally:
        if old is None:
            os.environ.pop("ECONDL_DATA", None)
        else:
            os.environ["ECONDL_DATA"] = old


# ---- (1) the plan --------------------------------------------------------------------------------

def test_plan_is_contiguous_disjoint_bounds_named_and_runs_to_next_year():
    for today in (dt.date(2026, 10, 30), dt.date(2026, 12, 31), dt.date(2027, 1, 1), dt.date(2030, 6, 1),
                  dt.date(2031, 6, 1)):
        p = F._plan(today)
        assert p[0][1] == "1900" and p[0][2] == "1959"
        assert p[-1][1:] == (str(today.year - 1), str(today.year + 1), "newest")
        for (la, a, b, _k), (_lb, c, _d, _k2) in zip(p, p[1:]):
            assert int(c) == int(b) + 1, (today, la)
        assert all(lab == f"{a}-{b}" for lab, a, b, _k in p)
        assert [k for _l, _a, b, k in p[:-1]] == ["open" if int(b) >= today.year - 2 else "closed"
                                                  for _l, _a, b, _k in p[:-1]]
        assert sum(1 for x in p if x[3] == "open") == 1
    assert [x[0] for x in F._plan(dt.date(2026, 10, 30))] == PLAN_2026
    assert [x[0] for x in F._plan(dt.date(2027, 1, 1))][-2:] == ["2020-2025", "2026-2028"]


def test_today_comes_from_the_module_clock(env):
    assert F._today() == dt.date(2026, 10, 30)
    env.clock.t = dt.datetime(2027, 1, 1, 0, 0, 1, tzinfo=dt.timezone.utc).timestamp()
    assert F._today() == dt.date(2027, 1, 1)


# ---- (2) sliced == whole-flow ---------------------------------------------------------------------

@pytest.mark.parametrize("first_ingest", [True, False])
def test_sliced_output_equals_the_whole_flow_output(env, first_ingest):
    roots = {}
    for name, how in (("whole", "whole"), ("sliced", "sliced")):
        roots[name] = env.root(name)
        if not first_ingest:
            env.setup("base")
        env.imf.up = "add"
        res = (base.run("IMTS", "IMF.STA", SRC) if how == "whole" else F.update(None, None))
        assert res.status == "ok", (how, res.status, res.error)
    a = os.path.join(roots["whole"], SRC, f"{SRC}.parquet")
    b = os.path.join(roots["sliced"], SRC, f"{SRC}.parquet")
    ra, rb = _rows(a), _rows(b)
    assert ra == rb
    assert len({(k, d) for k, d, _v in rb}) == len(rb)
    years = {d.year for _k, d, _v in rb}
    assert min(years) == 1948 and 2027 in years                    # pre-1950 data and Y+1 forecasts
    assert {k.split(".")[2] for k, _d, _v in rb} == {"A", "Q", "M", "S", "D"}
    assert json.loads(open(a + ".dims.json").read())["key_dims"] == fake.DIMS
    assert json.loads(open(b + ".dims.json").read())["key_dims"] == fake.DIMS
    assert _csv_bytes(roots["whole"], TABLE_IDS) == _csv_bytes(roots["sliced"], TABLE_IDS)
    assert _slices() == [], "slices are removed once the merge succeeded"


def test_a_sweep_makes_one_request_per_window(env):
    env.setup("base")
    env.imf.up = "add"
    F.update(None, None)
    asked = [u.split("?")[1] for u in env.imf.data_requests()]
    assert asked == [f"startPeriod={x.split('-')[0]}&endPeriod={x.split('-')[1]}" for x in PLAN_2026]
    assert env.imf.requests[0][0].endswith("/dataflow")
    gaps = [s for s in env.clock.slept if s > 0]
    assert len(gaps) == len(PLAN_2026) - 1 and all(abs(s - F.PACE_S) < 1e-6 for s in gaps), gaps


def test_a_publisher_answering_outside_the_window_cannot_put_a_row_in_two_slices(env, tmp_path):
    env.imf.leak = True
    p = tmp_path / "leaky.xml"
    p.write_bytes(fake.body("base", "1960", "1969", leak=True))
    tbl, dims, st = F._parse_slice(str(p), "IMTS", "1960", "1969", fake.DIMS)
    assert st["outside"] > 0
    assert {d.year for d in tbl.column("obs_date").to_pylist()} <= set(range(1960, 1970))
    seen, n = set(), 0
    for lab, a, b, _k in F._plan(dt.date(2026, 10, 30)):
        q = tmp_path / f"{lab}.xml"
        q.write_bytes(fake.body("base", a, b, leak=True))
        t, _d, _s = F._parse_slice(str(q), "IMTS", a, b, fake.DIMS)
        if t is not None:
            pairs = set(zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist()))
            assert not (pairs & seen), lab
            seen |= pairs
            n += t.num_rows
    assert n == len(seen)


def test_a_kill_inside_the_merge_resumes_with_only_the_newest_window(env, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    real = F.merge.merge_and_write

    def die(*a, **k):
        raise KeyboardInterrupt("planted kill inside the merge, before it writes")
    monkeypatch.setattr(F.merge, "merge_and_write", die)
    with pytest.raises(KeyboardInterrupt):
        F.update(None, None)
    monkeypatch.setattr(F.merge, "merge_and_write", real)
    env.imf.requests.clear()
    assert F.update(None, None).status == "ok"
    assert [u.split("?")[1] for u in env.imf.data_requests()] == ["startPeriod=2025&endPeriod=2027"]


def test_kept_slices_are_discarded_when_the_store_changed_since(env, monkeypatch):
    env.setup("base")
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    env.imf.per_request = 100.0
    env.ceiling(250.0)
    assert F.update(None, None).status == "partial"
    assert _slices() == ["slice_1900-1959.parquet"]
    import pyarrow as pa
    t = pq.read_table(env.store())
    pq.write_table(pa.concat_tables([t, t.slice(0, 1).set_column(
        1, "obs_date", pa.array([dt.date(2026, 4, 30)], pa.date32())).set_column(
        0, "series_key", pa.array(["IMTS:ABW.USA.A.MG_CIF_USD.IMTS"], t.schema.field(0).type))]),
        env.store())
    env.imf.requests.clear()
    env.imf.per_request = 0.0
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    assert F.update(None, None).status in ("ok", "no_change")
    assert len(env.imf.data_requests()) == len(PLAN_2026), "nothing kept against a changed store"


# ---- (3) the window stop -------------------------------------------------------------------------

def test_no_ceiling_means_no_stop(env):
    env.setup("base")
    env.imf.up, env.imf.per_request = "add", 10_000.0
    assert F.update(None, None).status == "ok"
    assert len(env.imf.data_requests()) == len(PLAN_2026)


def test_a_stop_is_partial_deferred_rotating_and_keeps_its_slices(env, monkeypatch):
    env.setup("base")
    before = _rows(env.store())
    env.imf.up, env.imf.per_request = "add", 100.0
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    env.ceiling(350.0)          # 1st: 350 >= 60; 2nd: ~249 >= 200; 3rd: ~148 < 200 -> stop
    res = F.update(None, None)
    assert res.status == "partial" and res.merged_rows == 0 and not res.series_cursors
    assert len(env.imf.data_requests()) == 2
    assert health._DEFERRAL_BASE.match(res.error), res.error         # ROTATING, not ATTENTION
    assert res.error.startswith("2 sub-unit(s) attempted, none failed; 8 deferred")
    assert "1980-1989" in res.error and "merge" in res.error
    assert orchestrate._should_derive_csvs(res.status)
    assert _rows(env.store()) == before, "a stop merges nothing"
    assert _slices() == ["slice_1900-1959.parquet", "slice_1960-1969.parquet"]
    # the next pass, no ceiling: the two kept slices are NOT asked again
    env.imf.requests.clear()
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    res2 = F.update(None, None)
    assert res2.status == "ok", res2.error
    asked = [u.split("?")[1] for u in env.imf.data_requests()]
    assert not any("startPeriod=1900&" in q or "startPeriod=1960&" in q for q in asked), asked
    assert len(asked) == len(PLAN_2026) - 2


def test_a_stop_before_any_slice_reads_attention_not_rotating(env):
    env.setup("base")
    env.ceiling(30.0)                     # under the 5-min CHOSEN floor at once
    res = F.update(None, None)
    assert res.status == "partial" and res.merged_rows == 0
    assert res.error.startswith("0 sub-unit(s) attempted")
    assert not health._DEFERRAL_BASE.match(res.error), "no progress must not read as rotating"
    assert env.imf.data_requests() == []


def test_the_newest_slice_needs_the_merge_reserve_too(env, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    monkeypatch.setenv("AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "10")
    env.ceiling(500.0)                    # every closed slice is instant; 500 s < 60 + 600 s
    res = F.update(None, None)
    assert res.status == "partial", res.error
    assert "[2025-2027, merge]" in res.error
    assert not any("startPeriod=2025" in u for u in env.imf.data_requests())
    assert len(_slices()) == len(PLAN_2026) - 1


def test_the_merge_reserve_stops_before_the_merge_and_nothing_closed_is_pulled_again(env, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    env.imf.per_request = 0.0
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    monkeypatch.setenv("AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "10")
    env.ceiling(700.0)
    calls = []
    real = F._get_to_file

    def slow_newest(url, dest, dl):
        n = real(url, dest, dl)
        if "startPeriod=2025" in url:
            env.clock.t += 200.0                      # 700 - 200 = 500 s < the 600-s reserve
        calls.append(url)
        return n
    monkeypatch.setattr(F, "_get_to_file", slow_newest)
    res = F.update(None, None)
    assert res.status == "partial" and res.error.endswith("deferred by budget, not reached in this "
                                                          "pass [merge]"), res.error
    env.imf.requests.clear()
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    assert F.update(None, None).status == "ok"
    assert [u.split("?")[1] for u in env.imf.data_requests()] == ["startPeriod=2025&endPeriod=2027"]


# ---- (4) every request is bounded --------------------------------------------------------------

def test_the_timeout_is_clamped_to_the_time_left(env, monkeypatch):
    env.setup("base")
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    env.ceiling(200.0)
    F.update(None, None)
    data = [t for u, t in env.imf.requests if u.startswith(fake.DATA)]
    assert data and all(t <= 200.0 for t in data), data
    flow = [t for u, t in env.imf.requests if u.endswith("/dataflow")]
    assert flow == [180]                      # 200 s left: the 180-s cap holds


def test_a_trickling_body_is_stopped_between_chunks(env, monkeypatch):
    env.setup("base")
    env.imf.per_request, env.imf.chunks = 400.0, 8     # 50 s a chunk
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    env.ceiling(120.0)
    res = F.update(None, None)
    assert res.status == "partial" and res.merged_rows == 0
    assert res.error.startswith("0 sub-unit(s) attempted"), res.error
    assert "1900-1959" in res.error
    assert env.clock.t - T0 <= 120.0 + 50.0 + 1e-6, "at most one chunk past the window end"
    assert _slices() == [], "nothing from the interrupted request is kept"
    assert not [f for f in os.listdir(F._slice_dir(SRC)) if f.endswith(".sdmx.tmp")]


def test_the_socket_timeout_is_re_clamped_before_every_chunk(env, monkeypatch):
    env.setup("base")
    env.imf.per_request, env.imf.chunks = 40.0, 4                 # 10 s a chunk
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    env.ceiling(100.0)
    seen = []

    class _Sock:
        def settimeout(self, t):
            seen.append((round(env.clock.t - T0, 1), t))
    real = env.imf.urlopen

    def urlopen(req, timeout=None, *a, **k):
        r = real(req, timeout, *a, **k)
        r.fp = types.SimpleNamespace(raw=types.SimpleNamespace(_sock=_Sock()))
        return r
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    F.update(None, None)
    first = [t for at, t in seen if at <= 40.0]
    assert len(first) >= 4 and first == sorted(first, reverse=True), seen
    assert all(abs((T0 + at + t) - orchestrate._RUN_DEADLINE_TS) < 1e-6 for at, t in seen), seen


def test_a_resumed_pass_sizes_its_margin_from_the_kept_slices(env, monkeypatch):
    env.setup("base")
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    env.imf.per_request = 100.0
    env.ceiling(250.0)
    assert F.update(None, None).status == "partial"               # 1900-1959 kept, 100 s recorded
    env.imf.requests.clear()
    env.ceiling(150.0)                    # 150 >= the 60-s floor, but < 2 x the kept 100 s
    res = F.update(None, None)
    assert res.status == "partial" and env.imf.data_requests() == [], res.error


def _cut_first_slice(env, monkeypatch):
    """A pass whose window end cuts 1900-1959 mid-body: 400 s a request in 8 chunks of 50 s, 120 s
    window, so the cut comes after 150 s. Returns the lower bound the marker recorded."""
    env.setup("base")
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    env.imf.per_request, env.imf.chunks = 400.0, 8
    env.ceiling(120.0)
    res = F.update(None, None)
    assert res.status == "partial" and res.merged_rows == 0 and _slices() == [], res.error
    cut = F._load_marker(F._slice_dir(SRC)).get("cut", {})
    assert set(cut) == {"1900-1959"}, cut
    assert cut["1900-1959"]["start"] == "1900" and cut["1900-1959"]["end"] == "1959"
    lb = cut["1900-1959"]["elapsed_s"]
    assert lb >= 120.0, lb
    env.imf.requests.clear()
    return lb


def _margin_s(out, lab="1900-1959"):
    import re
    m = re.search(rf"under the ([0-9.]+)-min margin before slice {lab}", out)
    assert m, out
    return float(m.group(1)) * 60


def test_w6_a_cut_slice_records_a_lower_bound_and_the_next_margin_is_at_least_twice_it(
        env, monkeypatch, capsys):
    lb = _cut_first_slice(env, monkeypatch)
    capsys.readouterr()
    env.ceiling(250.0)                 # >= the 60-s floor (main's WIP started here), < 2 x the bound
    res = F.update(None, None)
    out = capsys.readouterr().out
    assert res.status == "partial" and env.imf.data_requests() == [], res.error
    assert _margin_s(out) >= 2 * lb - 6.0, (out, lb)       # the sentence prints the margin to 0.1 min
    assert "under 2 x the slice's measured lower bound" in out, out
    assert "window shorter than the slice's measured lower bound" not in out, out


def test_w6_a_window_shorter_than_the_bound_is_named_in_the_stop_sentence(env, monkeypatch, capsys):
    lb = _cut_first_slice(env, monkeypatch)
    capsys.readouterr()
    env.ceiling(100.0)                 # >= the floor, < the bound itself
    assert 100.0 < lb
    res = F.update(None, None)
    out = capsys.readouterr().out
    assert res.status == "partial" and env.imf.data_requests() == [], res.error
    assert "window shorter than the slice's measured lower bound" in out, out
    assert "release: a longer window (operator)" in out, out


def test_w6_the_bound_goes_once_the_slice_finishes_and_ages_out_after_the_open_bound(env, monkeypatch):
    lb = _cut_first_slice(env, monkeypatch)
    env.ceiling(2 * lb + 400.0)        # starts 1900-1959 (400 s), then stops before 1960-1969
    res = F.update(None, None)
    assert res.status == "partial", res.error
    mk = F._load_marker(F._slice_dir(SRC))
    assert "1900-1959" in mk["slices"] and mk.get("cut") == {}, mk
    # a fresh cut, then 32 days on: the 31-d open bound has passed and the floor alone applies again
    env.root("b")
    lb = _cut_first_slice(env, monkeypatch)
    env.clock.t += 32 * 86400
    env.ceiling(250.0)
    F.update(None, None)
    assert any("startPeriod=1900&" in u for u in env.imf.data_requests()), "an aged bound still applied"


def _cut_newest_slice(env, monkeypatch):
    """AR-410's scenario: 60-s floor, 60-s merge reserve. Pass 1 keeps the 8 closed and open slices and
    stops before the newest (it needs 120 s); pass 2 starts the newest and the window end cuts it.
    Returns the newest slice's lower bound."""
    env.setup("base")
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    monkeypatch.setenv("AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "1")
    env.imf.per_request, env.imf.chunks = 0.0, 1
    env.ceiling(100.0)
    assert F.update(None, None).status == "partial"
    assert len(_slices()) == 8, _slices()
    env.imf.per_request, env.imf.chunks = 400.0, 8
    env.ceiling(130.0)
    assert F.update(None, None).status == "partial"
    cut = F._load_marker(F._slice_dir(SRC)).get("cut", {})
    assert set(cut) == {"2025-2027"}, cut
    env.imf.requests.clear()
    return cut["2025-2027"]["elapsed_s"]


def test_w6_a_window_over_2x_the_bound_but_under_the_margin_names_the_term_that_set_it(
        env, monkeypatch, capsys):
    lb = _cut_newest_slice(env, monkeypatch)
    capsys.readouterr()
    w = 2 * lb + 30.0                  # 2 x lb <= window < 2 x lb + the 60-s reserve = the margin
    env.ceiling(w)
    res = F.update(None, None)
    out = capsys.readouterr().out
    assert res.status == "partial" and env.imf.data_requests() == [], res.error
    assert 2 * lb <= w < _margin_s(out, "2025-2027"), (w, lb, out)
    assert "under 2 x the slice's measured lower bound" not in out, out
    assert "window shorter than the slice's measured lower bound" not in out, out
    assert "clears 2 x the slice's measured lower bound" in out, out
    assert ("set by 2 x the slice's measured lower bound" in out
            and "plus the 1.0-min merge reserve the newest slice needs" in out), out
    assert "release: a longer window (operator)" in out, out


def test_w6_a_window_over_2x_the_bound_but_under_the_floor_names_the_floor(env, monkeypatch, capsys):
    lb = _cut_first_slice(env, monkeypatch)
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "10")          # 600 s floor > 2 x lb
    assert 2 * lb < 2 * lb + 50.0 < 600.0
    capsys.readouterr()
    env.ceiling(2 * lb + 50.0)
    res = F.update(None, None)
    out = capsys.readouterr().out
    assert res.status == "partial" and env.imf.data_requests() == [], res.error
    stop = [ln for ln in out.splitlines() if "STOP before the window end" in ln]
    assert len(stop) == 1, out
    assert "under 2 x the slice's measured lower bound" not in stop[0], stop
    assert "set by the 10.0-min floor" in stop[0] and "merge reserve" not in stop[0], stop


def test_margin_terms_names_the_largest_term_and_the_reserve():
    assert F._margin_terms(60.0, 10.0, 100.0, 0.0).startswith("2 x the slice's measured lower bound (3.3")
    assert F._margin_terms(60.0, 200.0, 100.0, 0.0).startswith("2 x the slowest slice this pass (6.7")
    assert F._margin_terms(900.0, 200.0, 100.0, 0.0) == "the 15.0-min floor"
    assert F._margin_terms(60.0, 0.0, 100.0, 60.0).endswith("plus the 1.0-min merge reserve the newest "
                                                             "slice needs")
    assert F._margin_terms(60.0, 100.0, 100.0, 0.0).startswith("2 x the slice's measured lower bound")


def test_w6_a_later_shorter_cut_keeps_the_larger_earlier_bound(env, monkeypatch):
    env.setup("base")
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "0.5")         # 30-s floor
    env.imf.per_request, env.imf.chunks = 400.0, 8                # 50-s chunks
    env.ceiling(40.0)
    assert F.update(None, None).status == "partial"
    lb = F._load_marker(F._slice_dir(SRC))["cut"]["1900-1959"]["elapsed_s"]
    assert 40.0 <= lb < 60.0, lb                  # so 2 x lb + 10 s stays under the 121-s retry wait
    env.imf.requests.clear()
    env.imf.status["1900"] = [(503, 120)]         # the slice starts, then a 121-s wait it cannot take
    env.ceiling(2 * lb + 10.0)
    res = F.update(None, None)
    assert res.status == "partial" and len(env.imf.data_requests()) == 1, res.error
    cut = F._load_marker(F._slice_dir(SRC)).get("cut", {})
    assert cut["1900-1959"]["elapsed_s"] == lb, (cut, lb)       # max over the cuts, not the last one


# ---- kept slices: the age bounds and the checks on reuse (AR-410) --------------------------------

def _keep_eight(env, monkeypatch):
    env.setup("base")
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    monkeypatch.setenv("AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "1")
    env.imf.per_request, env.imf.chunks = 0.0, 1
    env.ceiling(100.0)                 # closed + open slices need 60 s, the newest 120 s: it stops
    assert F.update(None, None).status == "partial"
    assert len(_slices()) == 8, _slices()
    env.imf.requests.clear()
    env.mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)


def _starts(env):
    return sorted(u.split("startPeriod=")[1].split("&")[0] for u in env.imf.data_requests())


@pytest.mark.parametrize("days,pulled", [
    (5, ["2025"]),                                         # inside both bounds: only the newest
    (30, ["2025"]),                                        # still inside the 31-d open bound
    (32, ["2020", "2025"]),                                # the open slice aged out, no closed one
])
def test_kept_slices_are_re_pulled_only_past_their_age_bound(env, monkeypatch, days, pulled):
    assert (F.CLOSED_MAX_AGE_DAYS_DEFAULT, F.OPEN_MAX_AGE_DAYS_DEFAULT) == (365.0, 31.0)
    _keep_eight(env, monkeypatch)
    env.clock.t += days * 86400
    assert F.update(None, None).status in ("ok", "no_change")
    assert _starts(env) == pulled, (days, _starts(env))


@pytest.mark.parametrize("days,pulled", [
    (9, ["2025"]),
    (11, ["2020", "2025"]),                                # past the 10-d open bound only
    (19, ["2020", "2025"]),
    (21, sorted(p.split("-")[0] for p in PLAN_2026)),      # past the 20-d closed bound too
])
def test_the_closed_age_bound_re_pulls_the_closed_slices(env, monkeypatch, days, pulled):
    """365 d cannot pass on the test clock without a new year changing the plan's bounds (which
    re-pulls by bounds, not by age), so the closed bound is tested through its knob."""
    monkeypatch.setenv("AQUEDUCT_IMTS_CLOSED_MAX_AGE_DAYS", "20")
    monkeypatch.setenv("AQUEDUCT_IMTS_OPEN_MAX_AGE_DAYS", "10")
    _keep_eight(env, monkeypatch)
    env.clock.t += days * 86400
    assert F.update(None, None).status in ("ok", "no_change")
    assert _starts(env) == pulled, (days, _starts(env))


@pytest.mark.parametrize("field", ["dims", "rows"])
def test_a_kept_slice_whose_record_disagrees_is_re_pulled(env, monkeypatch, field):
    _keep_eight(env, monkeypatch)
    d = F._slice_dir(SRC)
    mk = F._load_marker(d)
    rec = mk["slices"]["1970-1979"]
    if field == "dims":
        rec["dims"] = list(rec["dims"])[:-1]               # recorded under other dims than the store's
    else:
        rec["rows"] = rec["rows"] + 1                      # the file on disk no longer has those rows
    F._save_marker(d, mk)
    assert F.update(None, None).status in ("ok", "no_change")
    assert _starts(env) == ["1970", "2025"], (field, _starts(env))


def test_a_retry_wait_that_would_pass_the_window_end_stops_the_pass(env, monkeypatch):
    env.setup("base")
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    env.imf.status["1900"] = [(503, 100)]
    env.ceiling(90.0)
    res = F.update(None, None)
    assert res.status == "partial" and "deferred" in res.error, res.error
    assert len(env.imf.data_requests()) == 1, "no retry starts past the window end"
    assert env.clock.t <= orchestrate._RUN_DEADLINE_TS, "the wait itself never passes the window end"


def test_429_is_transient_honours_retry_after_and_is_never_read_as_empty(env):
    env.setup("base")
    env.imf.status["1900"] = [(429, 7)] * ing.RETRIES
    res = F.update(None, None)
    assert res.status == "partial" and "transient-failed" in res.error, res.error
    assert "1900-1959" in res.error
    assert len(env.imf.data_requests()) == ing.RETRIES
    assert env.clock.slept.count(8) == ing.RETRIES - 1               # Retry-After 7 -> 8 s (+1)


def test_a_503_then_success_merges_normally(env):
    env.setup("base")
    env.imf.up = "add"
    env.imf.status["1970"] = [(503, 2)]
    assert F.update(None, None).status == "ok"


# ---- (5) pinned dims, refusals ------------------------------------------------------------------

def test_a_missing_or_corrupt_sidecar_is_a_named_transient_error(env):
    env.setup("base")
    os.remove(env.store() + ".dims.json")
    with pytest.raises(TransientError, match="MISSING"):
        F.update(None, None)
    with open(env.store() + ".dims.json", "w") as fh:
        fh.write("{nope")
    with pytest.raises(TransientError, match="CORRUPT"):
        F.update(None, None)
    assert env.imf.data_requests() == [], "nothing is pulled without the pinned dims"


@pytest.mark.parametrize("drop", [(1960, 1969, "METHODOLOGY"), (1980, 1989, "SCALE", "ISSUER")])
def test_a_slice_with_different_identity_attributes_is_refused_and_mints_no_key(env, drop):
    env.setup("base")
    before = _rows(env.store())
    env.imf.up, env.imf.drop_attr = "add", drop
    with pytest.raises(DefinitiveError, match=f"{drop[0]}-{drop[1]}: identity attributes differ"):
        F.update(None, None)
    after = _rows(env.store())
    assert after == before
    assert {len(k.split(".")) for k, _d, _v in after} == {5}


def test_a_window_400_is_named_as_that_window_not_a_flow_move(env):
    env.setup("base")
    env.imf.status["1990"] = [(400, None)]
    with pytest.raises(DefinitiveError, match="window 1990-1999: HTTP 400") as ei:
        F.update(None, None)
    assert "not a flow move" in str(ei.value)


def test_a_store_older_than_the_first_window_is_refused(env):
    env.setup("base")
    t = pq.read_table(env.store())
    import pyarrow as pa
    extra = pa.table({"series_key": ["IMTS:ABW.USA.A.MG_CIF_USD.IMTS"],
                      "obs_date": pa.array([dt.date(1899, 12, 31)], pa.date32()), "value": [1.0]})
    pq.write_table(pa.concat_tables([t, extra.cast(t.schema)]), env.store())
    with pytest.raises(DefinitiveError, match="before the first window's start 1900"):
        F.update(None, None)
    assert env.imf.data_requests() == []


def test_the_marker_write_fails_closed(env, monkeypatch):
    env.setup("base")
    n = [0]

    class _Json:                                     # the REAL _save_marker; its json.dump fails
        loads, dumps, load = json.loads, json.dumps, json.load

        @staticmethod
        def dump(obj, fh, **k):
            n[0] += 1
            if n[0] == 2:
                raise OSError("planted: disk full")
            return json.dump(obj, fh, **k)
    monkeypatch.setattr(F, "json", _Json)
    with pytest.raises(TransientError, match="marker not writable"):
        F.update(None, None)
    assert len(env.imf.data_requests()) == 1, "the pass stops at the failed save"


# ---- (6) a REAL process death, resume, rollover, convergence --------------------------------------

_CHILD = textwrap.dedent('''
    import os, sys, json
    ROOT, DATA, TS, KILL_AFTER, UP = sys.argv[1], sys.argv[2], float(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
    os.environ["AQUEDUCT_DATA_ROOT"] = DATA
    os.environ.pop("AQUEDUCT_BACKEND", None)
    sys.path[:0] = [ROOT, os.path.join(ROOT, "tests")]
    import urllib.request
    import _imts_fake as fake
    clock = fake.Clock(TS)
    imf = fake.Imf(clock)
    imf.up = UP
    urllib.request.urlopen = imf.urlopen
    from updater.strategies.fetchers import imf_imts_direct as F
    F.time = clock
    real = F._save_marker
    n = [0]
    def save(d, obj):
        real(d, obj)
        n[0] += 1
        if KILL_AFTER and n[0] > KILL_AFTER:       # the first save is the pass-start one
            print("PLANTED KILL after", len(obj["slices"]), "kept slices", flush=True)
            os._exit(137)
    F._save_marker = save
    res = F.update(None, None)
    print("CHILD", json.dumps({"status": res.status, "asked": [u.split("?")[1] for u in imf.data_requests()]}))
''')


def _child(tmp_path, data_root, ts, kill_after, up):
    script = tmp_path / "child.py"
    script.write_text(_CHILD, encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k != "AQUEDUCT_BACKEND"}
    p = subprocess.run([sys.executable, "-B", str(script), ROOT, data_root, str(ts), str(kill_after), up],
                       capture_output=True, text=True, cwd=str(tmp_path), env=env, timeout=300)
    out = [json.loads(ln[6:]) for ln in p.stdout.splitlines() if ln.startswith("CHILD ")]
    return p.returncode, p.stdout + p.stderr, (out[0] if out else None)


def test_a_real_kill_mid_sweep_resumes_from_the_kept_slices(env):
    env.setup("base")
    data_root = config.DATA_ROOT
    rc, log, _ = _child(env.tmp, data_root, T0, 3, "add")
    assert rc == 137 and "PLANTED KILL after 3 kept slices" in log, log
    assert _slices() == ["slice_1900-1959.parquet", "slice_1960-1969.parquet", "slice_1970-1979.parquet"]
    rc, log, out = _child(env.tmp, data_root, T0 + 3600, 0, "add")
    assert rc == 0 and out["status"] == "ok", log
    assert out["asked"][0] == "startPeriod=1980&endPeriod=1989", out
    assert len(out["asked"]) == len(PLAN_2026) - 3
    # the result equals a whole-flow pull of the same upstream
    whole = env.root("ref")
    env.setup("base")
    env.imf.up = "add"
    base.run("IMTS", "IMF.STA", SRC)
    assert _rows(os.path.join(data_root, SRC, f"{SRC}.parquet")) == _rows(
        os.path.join(whole, SRC, f"{SRC}.parquet"))


def test_a_kill_before_new_year_resumes_after_it_by_bounds(env):
    env.setup("base")
    data_root = config.DATA_ROOT
    dec31 = dt.datetime(2026, 12, 31, 23, 0, tzinfo=dt.timezone.utc).timestamp()
    rc, log, _ = _child(env.tmp, data_root, dec31, 8, "add")        # every window but the newest
    assert rc == 137, log
    assert "slice_2020-2024.parquet" in _slices()
    jan1 = dt.datetime(2027, 1, 1, 1, 0, tzinfo=dt.timezone.utc).timestamp()
    rc, log, out = _child(env.tmp, data_root, jan1, 0, "add")
    assert rc == 0 and out["status"] == "ok", log
    # 2027's plan: the closed decades are reused; 2020-2025 and 2026-2028 are new bounds
    assert out["asked"] == ["startPeriod=2020&endPeriod=2025", "startPeriod=2026&endPeriod=2028"], out
    t = pq.read_table(os.path.join(data_root, SRC, f"{SRC}.parquet"))
    assert len({(k, d) for k, d in zip(t.column("series_key").to_pylist(),
                                       t.column("obs_date").to_pylist())}) == t.num_rows


def test_one_window_a_week_converges_without_a_re_expiry_loop(env, monkeypatch):
    env.setup("base")
    env.imf.up, env.imf.per_request = "add", 100.0
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    monkeypatch.setenv("AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "0.5")
    statuses = []
    for k in range(1, 15):
        env.imf.requests.clear()
        env.ceiling(250.0)                    # room for exactly one slice (and the reserve)
        res = F.update(None, None)
        statuses.append(res.status)
        assert len(env.imf.data_requests()) == 1, (k, env.imf.data_requests())
        if res.status == "ok":
            break
        env.clock.t += 7 * 86400
    assert statuses[-1] == "ok" and len(statuses) == len(PLAN_2026), statuses


# ---- (7) PR B: (c) and (b) now differ from main; (j) with nothing owed does not -------------------

def _both(env, scenario):
    out = {}
    for how in ("main", "head"):
        env.root(how)
        out[how] = scenario(how)
    return out


def _run(how):
    try:
        r = base.run("IMTS", "IMF.STA", SRC) if how == "main" else F.update(None, None)
        ck = getattr(r, "changed_keys", None)
        return (r.status, orchestrate._should_derive_csvs(r.status), None if ck is None else len(ck))
    except DefinitiveError as e:
        return ("DefinitiveError", type(e).__name__, None)


def test_c_revision_only_reads_ok_and_hands_over_the_revised_keys(env):
    def sc(how):
        env.setup("base")
        env.imf.up = "rev"
        return _run(how)
    out = _both(env, sc)
    assert out["main"] == ("no_change", False, None), out
    # rev moves 1949 (A), 2005-Q2 (Q) and 2026-M04 (M): three keys, merge-measured
    assert out["head"] == ("ok", True, 3), out


def test_b_and_s1n_a_re_merge_after_a_killed_merge_hands_over_every_key(env, monkeypatch):
    def sc(how):
        env.setup("base")
        env.imf.up = "add"
        real = base.merge.merge_and_write

        def killed(*a, **k):
            real(*a, **k)
            raise KeyboardInterrupt("planted kill after the merge")
        monkeypatch.setattr(base.merge, "merge_and_write", killed)
        with pytest.raises(KeyboardInterrupt):
            (base.run("IMTS", "IMF.STA", SRC) if how == "main" else F.update(None, None))
        monkeypatch.setattr(base.merge, "merge_and_write", real)
        return _run(how)
    out = _both(env, sc)
    assert out["main"] == ("no_change", False, None), out
    assert out["head"] == ("ok", True, len(fake.SERIES)), out      # every stored key, not the re-merge's 0


def test_j_a_structural_pass_with_nothing_owed_reads_the_same_as_main(env):
    def sc(how):
        env.setup("base")
        env.imf.up = "empty"
        return _run(how)
    out = _both(env, sc)
    assert out["main"] == out["head"] == ("DefinitiveError", "DefinitiveError", None), out


# ---- (8) through the REAL run_once -------------------------------------------------------------

def _catalog(path, ids):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, metadata TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?,?)", [(i, SRC, "{}") for i in ids])
    con.commit()
    con.close()


@pytest.fixture
def rig(env, tmp_path, monkeypatch):
    for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_BUDGET_MIN",
              "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_DERIVE_BUDGET_MIN", "AQUEDUCT_FLOW_DERIVE_MAX_ROWS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    data = tmp_path / "a" / "data"
    os.makedirs(data / "_aqueduct", exist_ok=True)
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    monkeypatch.setenv("ECONDL_DATA", str(data / "clean_full"))
    _catalog(tmp_path / "catalog.db", TABLE_IDS)
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "catalog.db"))
    orchestrate._REG_ENTRIES = None

    class _Blob:
        def __init__(self):
            self.keys = []

        def put_atomic(self, key, data):
            self.keys.append(key)

    r = types.SimpleNamespace(blob=_Blob(), state=StateStore(str(data / "_aqueduct" / "state.db")),
                              env=env, drained=[])
    unit = next(u for u in registry.all_units() if u.source_id == SRC)
    real = orchestrate.get_strategy(unit.strategy)

    class _Strategy:
        def is_due(self, unit, us, **k):
            return True

        def detect_change(self, unit, us):
            return "imts:test"

        def run(self, unit, since=None):
            return real.run(unit, since=since)

    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: _Strategy())
    real_drain = orchestrate._drain_csv_retry_queue

    def drain(unit, blob_, store):
        r.drained.append(unit.source_id)
        return real_drain(unit, blob_, store)
    monkeypatch.setattr(orchestrate, "_drain_csv_retry_queue", drain)

    def run():
        r.blob.keys.clear()
        import time as _t
        env.clock.t = _t.time()          # run_once sets its ceiling from the real clock
        return orchestrate.run_once(sources=[SRC], store=r.state, blob=r.blob)

    def put_ids():
        want = {derive.r2_key(i): i for i in TABLE_IDS}
        return sorted(want[k] for k in r.blob.keys if k in want)
    r.run, r.put_ids = run, put_ids
    yield r
    r.state.close()


def test_a_stop_through_the_real_run_once_is_partial_no_success_and_reaches_the_drain(rig, monkeypatch):
    rig.env.setup("base")
    rig.env.imf.up = "add"
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "0.5")     # 30 s: under the 5-min floor
    results = rig.run()
    assert results == [(f"{SRC}/_all", "partial")], results
    row = rig.state.get_unit(SRC, "_all")
    assert row["status"] == "partial" and row["last_success_utc"] is None, row
    assert "deferred by budget" in (row["last_error"] or ""), row
    assert rig.drained == [SRC], "a merged_rows=0 partial still reaches the retry-queue drain"
    assert not rig.state.db.execute(
        "SELECT 1 FROM full_rederive_owed WHERE source_id=?", (SRC,)).fetchone()
    assert rig.put_ids() == []


def test_a_merging_pass_through_the_real_run_once_derives_the_tables_it_changed(rig):
    rig.env.setup("base")
    rig.env.imf.up = "add"
    results = rig.run()
    assert results == [(f"{SRC}/_all", "ok")], results
    # add appends 2026-M05 to the one monthly series: one changed table (main derived all five)
    assert rig.put_ids() == [i for i in TABLE_IDS if i.endswith(".M.MG_CIF_USD")]


# ---- (9) the opt-in is pinned ----------------------------------------------------------------------

FETCHERS = os.path.join(ROOT, "updater", "strategies", "fetchers")


def test_only_imf_imts_direct_runs_sliced_and_every_other_importer_still_calls_base_run():
    sliced, base_run = [], []
    for p in sorted(glob.glob(os.path.join(FETCHERS, "*.py"))):
        name = os.path.basename(p)[:-3]
        src = open(p, encoding="utf-8").read()
        if "run_sliced" in src and name != SRC:
            sliced.append(name)
        if "from . import _imf_direct as _base" in src and name not in (SRC,):
            tree = ast.parse(src)
            calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                     and isinstance(n.func, ast.Attribute) and n.func.attr == "run"
                     and isinstance(n.func.value, ast.Name) and n.func.value.id == "_base"]
            if calls:
                base_run.append(name)
    assert sliced == []
    # Measured on main 0ba3b4a20: 48 fetchers call _base.run, imf_imts_direct among them. Every one of
    # the other 47 still does.
    assert len(base_run) == 47, base_run
    imts = ast.parse(open(os.path.join(FETCHERS, f"{SRC}.py"), encoding="utf-8").read())
    upd = next(n for n in imts.body if isinstance(n, ast.FunctionDef) and n.name == "update")
    assert "run_sliced" in ast.unparse(upd) and "_base.run" not in ast.unparse(upd)


def test_the_shared_default_path_does_not_read_the_ceiling():
    for rel in ("updater/strategies/fetchers/_imf_direct.py", "jobs/ingest_imf_direct.py"):
        assert "_RUN_DEADLINE_TS" not in open(os.path.join(ROOT, rel), encoding="utf-8").read(), rel
