"""insee_melodi's resumable re-pull (design AR-330; conditions C1-C15 and R2-1..R2-8).

Melodi is faked (no network): a paged fixture with injectable faults and a fake clock that every request
advances. PAGE_SIZE is 10 and CHUNK_PAGES 3 so a fixture of a few dozen rows exercises chunks, resume and
the exact-multiple rules. The store is a temporary local store at <tmp>/data/clean_full/insee_melodi, so the
blob key rules (and the strict chunk-delete regex) apply as on R2.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
from urllib.parse import urlparse, parse_qs

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from updater import config
from updater.errors import DefinitiveError
from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S

P = 10            # PAGE_SIZE in these tests


# --------------------------------------------------------------------------------------------- harness
class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class Resp:
    def __init__(self, code, body=None):
        self.status_code, self._body, self.headers = code, body, {}

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


def obs(key_dims: dict, year: int, value: float):
    d = dict(key_dims)
    d["TIME_PERIOD"] = str(year)
    return {"dimensions": d, "measures": {"OBS_VALUE_NIVEAU": {"value": value}}}


def series(n, geo="2026", year=2023, prefix="X", extra=None):
    out = []
    for i in range(n):
        dims = {"GEO": f"{geo}-COM-{i:05d}", "SEX": "_T"}
        if extra:
            dims.update(extra)
        out.append(obs(dims, year, float(i) + 0.5))
    return out


class FakeMelodi:
    """/dataflow/all, /catalog/{FLOW} and /data/{FLOW}?[startPeriod=Y&]page=N."""

    def __init__(self, clock, flows: dict, step=3.0):
        self.clock, self.flows, self.step = clock, flows, step
        self.modified = {c: "2026-06-25T00:00:00" for c in flows}
        self.faults = {}               # (flow, page) -> int status | "empty" | "short_nolast" | "nolast"
        self.ignore_start = set()
        self.calls = []
        self.flip_page1 = set()        # flows whose page 1 changes order between calls (non-determinism)
        self._flip = 0

    def get(self, url, headers=None, timeout=None):
        self.clock.t += self.step
        self.calls.append(url)
        u = urlparse(url)
        path = u.path.replace("/melodi", "")
        if path == "/dataflow/all":
            return Resp(200, [{"code": c} for c in sorted(self.flows)])
        if path.startswith("/catalog/"):
            c = path.split("/")[-1]
            m = self.modified.get(c)
            return Resp(200, {"identifier": c, "modified": m}) if m else Resp(500)
        flow = path.split("/")[-1]
        q = parse_qs(u.query)
        page = int(q.get("page", ["1"])[0])
        if flow not in self.flows:
            return Resp(400, {"error": "n'existe pas"})
        rows = self.flows[flow]
        if "startPeriod" in q and flow not in self.ignore_start:
            y = int(q["startPeriod"][0])
            rows = [r for r in rows if int(r["dimensions"]["TIME_PERIOD"][:4]) >= y]
        f = self.faults.get((flow, page))
        if isinstance(f, int):
            return Resp(f, {})
        chunk = rows[(page - 1) * P: page * P]
        if flow in self.flip_page1 and page == 1:
            self._flip += 1
            if self._flip % 2 == 0:
                chunk = list(reversed(chunk))[:-1] + [obs({"GEO": "9999-COM-X", "SEX": "_T"}, 2023, 7.0)]
        last = page * P >= len(rows)
        if f == "empty":
            chunk, last = [], False
        elif f == "short_nolast":
            chunk, last = chunk[:3], False
        elif f == "nolast":
            last = False
        return Resp(200, {"observations": chunk, "paging": {"first": "x", "isLast": last}})


@pytest.fixture
def env(tmp_path, monkeypatch):
    store = tmp_path / "data" / "clean_full" / "insee_melodi"
    store.mkdir(parents=True)
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(config, "source_dir", lambda s: str(tmp_path / "data" / "clean_full" / s))
    monkeypatch.setattr(S, "_ALLOW_LOCAL_BACKEND", True)
    monkeypatch.setattr(S, "_cut_over", lambda: False)
    monkeypatch.setenv("ECONDL_DUCKDB_TMP", str(tmp_path / "spill"))
    monkeypatch.setattr(M.time, "sleep", lambda s: None)
    monkeypatch.setattr(M, "PAGE_SIZE", P)
    monkeypatch.setattr(M, "CHUNK_PAGES", 3)
    clock = FakeClock()

    class E:
        pass
    e = E()
    e.store, e.clock, e.tmp = store, clock, tmp_path

    def put(code, observations):
        tbl, _, _, _ = M._parse_page(code, observations)
        pq.write_table(tbl, str(store / f"{code}.parquet"))
    e.put = put

    def rows(code):
        p = store / f"{code}.parquet"
        return pq.read_table(str(p)).num_rows if p.exists() else 0
    e.rows = rows

    def run(fake, window_s=45 * 60):
        c = S.Clock(now=clock, window_s=window_s, source="test")
        return M.update(None, None, _session=fake, _clock=c)
    e.run = run

    def state():
        return json.loads((store / S.STATE_FILE).read_text(encoding="utf-8"))
    e.state = state

    def chunks():
        return sorted(str(p.relative_to(store)).replace("\\", "/") for p in store.rglob("*.chunk"))
    e.chunks = chunks
    return e


def _control_flows():
    """Every fixture carries the two probe controls: a one-page TD seed and the empty control."""
    return {"DS_RP_TD_SEED": series(25), M.PROBE_EMPTY_CONTROL: series(3)}


def _seed_store(env, fake):
    env.put("DS_RP_TD_SEED", fake.flows["DS_RP_TD_SEED"][:P])          # truncated at one page
    env.put(M.PROBE_EMPTY_CONTROL, fake.flows[M.PROBE_EMPTY_CONTROL])


def run_until_quiet(env, fake, passes=12):
    out = []
    for _ in range(passes):
        r = env.run(fake)
        out.append(r)
        if r.status != "transient_fail":
            break
    return out


# --------------------------------------------------------------------------------------------- C1 / repair
def test_a_one_page_seed_is_re_pulled_in_full_and_reported_changed(env):
    fake = FakeMelodi(env.clock, _control_flows())
    _seed_store(env, fake)
    res = run_until_quiet(env, fake)
    assert env.rows("DS_RP_TD_SEED") == 25
    assert res[-1].status in ("ok", "no_change")                        # nothing left to repair
    assert any("DS_RP_TD_SEED" in (r.changed_keys or {}) for r in res)
    st = env.state()["flows"]["DS_RP_TD_SEED"]
    assert st["status"] == S.COMPLETE and st["why"].startswith("complete per INSEE paging")
    assert env.chunks() == []                                           # staging deleted after the merge


def test_a_page_multiple_PARTITION_sends_a_non_multiple_total_to_origin(env):
    """AR-330 C1: 10,000 + 14,296 (here 10 + 14) - the total is no multiple, one spelling partition is."""
    # INSEE now serves ONLY the 2026 vintage (19 rows): the page-beyond probe (page 24//10+2 = 4) comes
    # back empty, so nothing but the PARTITION rule can see that the 2026 spelling holds exactly one page.
    flows = _control_flows()
    flows["DS_RP_LOGEMENT_DOM_PRINC"] = series(19, geo="2026")
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_RP_LOGEMENT_DOM_PRINC", series(10, geo="2026") + series(14, geo="2025", year=2022))
    env.run(fake)
    st = env.state()["flows"]["DS_RP_LOGEMENT_DOM_PRINC"]
    assert st.get("repair") is True, st                                 # classified needs_origin, by partition
    assert st.get("basis") == "origin" or st["status"] in (S.NEEDS_ORIGIN, S.IN_PROGRESS)


def test_negative_control_one_partition_of_eleven_stays_legacy_before_its_probe(env):
    flows = _control_flows()
    flows["DS_OK"] = series(11)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_OK", flows["DS_OK"])
    env.run(fake)
    st = env.state()["flows"]["DS_OK"]
    assert st["status"] in (S.LEGACY, S.IN_PROGRESS, S.COMPLETE)
    assert st.get("probe", {}).get("raw") == 0                          # its probe page came back empty


def test_probe_page_uses_the_distinct_normalised_count_not_the_inflated_total(env):
    """R2-1: a DS_FLORES_A5-shaped store (every observation held twice, under 2025 and 2026 vintages, one
    with FREQ) must probe the page past the DISTINCT normalised count, not past the doubled total."""
    flows = _control_flows()
    flows["DS_FLORES_A5"] = series(26)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_FLORES_A5", series(13, geo="2025") + series(13, geo="2026", extra={"FREQ": "A"}))
    env.run(fake)
    st = env.state()["flows"]["DS_FLORES_A5"]
    assert st["s_norm"] == 13
    assert st["probe"]["page"] == 13 // P + 2 == 3                      # the doubled total (26) would ask page 4
    assert st["status"] != S.LEGACY                                     # INSEE serves 26 > 13: truncation found


def test_the_walk_never_writes_an_unclassified_flow(env):
    """R2-2: with the classification slot already spent, an unclassified one-page flow is not tailed."""
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    c = S.Clock(now=env.clock, window_s=45 * 60, source="test")
    env.clock.t += M.CLASSIFY_UNTIL_MIN * 60 + 1                        # classification slot over before it starts
    M.update(None, None, _session=fake, _clock=c)
    assert not [u for u in fake.calls if "/data/DS_RP_TD_SEED" in u]
    assert env.rows("DS_RP_TD_SEED") == P


def test_a_flow_in_the_ci_delta_list_goes_to_origin(env):
    flows = _control_flows()
    flows["DS_TOUR_CAP"] = series(23)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_TOUR_CAP", series(17))                                   # no exact multiple anywhere
    env.run(fake)
    assert env.state()["flows"]["DS_TOUR_CAP"]["why"].startswith(("a CI-logged delta", "complete"))


def test_a_failed_probe_control_voids_every_probe_of_the_pass(env):
    flows = _control_flows()
    flows["DS_OK"] = series(11)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_OK", flows["DS_OK"])
    fake.faults[(M.PROBE_EMPTY_CONTROL, 2)] = "nolast"                  # the empty control answers non-empty
    fake.flows[M.PROBE_EMPTY_CONTROL] = series(25)
    r = env.run(fake)
    assert "PROBE VOID" in r.error
    assert env.state()["flows"]["DS_OK"]["probe"].get("void")


# --------------------------------------------------------------------------------------------- D / C2
@pytest.mark.parametrize("fault,label", [(400, "HTTP 400"), ("empty", "empty without isLast"),
                                         ("short_nolast", "without isLast")])
def test_an_early_end_is_never_read_as_done(env, fault, label):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    fake.faults[("DS_RP_TD_SEED", 2)] = fault
    before = (env.store / "DS_RP_TD_SEED.parquet").read_bytes()
    r = env.run(fake)
    assert r.status == "transient_fail" and label in r.error
    assert (env.store / "DS_RP_TD_SEED.parquet").read_bytes() == before        # served file untouched
    assert "DS_RP_TD_SEED" not in (r.changed_keys or {})


def test_isLast_on_a_full_page_needs_an_empty_page_after_it(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(20)                                  # exactly two full pages
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    assert any("page=3" in u and "DS_RP_TD_SEED" in u for u in fake.calls)    # the page+1 confirmation
    assert env.rows("DS_RP_TD_SEED") == 20


def test_isLast_contradicted_by_a_non_empty_next_page_is_not_complete(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(25)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    orig = fake.get

    def lying(url, headers=None, timeout=None):
        r = orig(url, headers, timeout)
        if "/data/DS_RP_TD_SEED" in url and "page=2" in url and "startPeriod" not in url:
            r._body["paging"]["isLast"] = True
        return r
    fake.get = lying
    r = env.run(fake)
    assert "contradicted" in r.error
    assert env.state()["flows"]["DS_RP_TD_SEED"]["status"] != S.COMPLETE


def test_cross_page_duplicates_restart_within_page_duplicates_are_labelled(env):
    flows = _control_flows()
    dup = series(12)
    dup[11] = dup[2]                                                     # page 2 repeats a page-1 observation
    flows["DS_RP_TD_SEED"] = dup
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    r = env.run(fake)
    assert "ACROSS pages" in r.error
    flows2 = _control_flows()
    within = series(25)
    within[1] = within[0]                                                # inside page 1
    fake2 = FakeMelodi(env.clock, flows2)
    fake2.flows["DS_RP_TD_SEED"] = within
    (env.store / S.STATE_FILE).unlink()
    for p in env.store.rglob("*.chunk"):
        p.unlink()
    rs = run_until_quiet(env, fake2)
    assert any("PARSE COLLISION" in (x.error or "") for x in rs)
    assert env.state()["flows"]["DS_RP_TD_SEED"]["status"] == S.COMPLETE


# --------------------------------------------------------------------------------------------- B / C4 / C6
def test_a_stopped_pull_resumes_at_its_cursor_and_completes_without_duplicates(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)                                  # 10 pages, 4 chunks
    fake = FakeMelodi(env.clock, flows, step=200.0)                       # slow server: the pass stops mid-pull
    _seed_store(env, fake)
    res = run_until_quiet(env, fake, passes=20)
    assert env.rows("DS_RP_TD_SEED") == 95
    seen = pq.read_table(str(env.store / "DS_RP_TD_SEED.parquet")).to_pylist()
    assert len({(r["series_key"], r["obs_date"]) for r in seen}) == 95
    assert len(res) > 1                                                  # it really took more than one pass


def test_a_changed_page_on_resume_restarts_the_attempt_and_deletes_its_chunks(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    env.run(fake)
    at = env.state()["flows"]["DS_RP_TD_SEED"]["attempt"]
    assert at.get("chunks", 0) >= 1
    fake.flows["DS_RP_TD_SEED"] = list(reversed(series(95)))             # paging order changed upstream
    r = env.run(fake)
    assert "restarted" in r.error and "changed since it was staged" in r.error   # the fingerprint, not a later check
    st = env.state()["flows"]["DS_RP_TD_SEED"]
    assert (st.get("attempt") or {}).get("id") != at["id"]
    assert not [c for c in env.chunks() if at["id"] in c]


def test_a_missing_chunk_means_restart_never_a_merge(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    env.run(fake)
    first = sorted(env.store.rglob("00000.chunk"))[0]
    first.unlink()
    st = env.state()
    st["flows"]["DS_RP_TD_SEED"]["status"] = S.MERGE_PENDING
    raw = json.dumps(st, indent=1, sort_keys=True).encode()
    (env.store / S.STATE_FILE).write_bytes(raw)
    r = env.run(fake)
    assert "chunk 0 missing" in r.error
    assert env.rows("DS_RP_TD_SEED") == P


def test_k3_a_kill_after_the_publish_still_derives_the_csv_next_pass(env, monkeypatch):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    real = M.merge.merge_and_write_bounded

    def publish_then_die(*a, **k):
        real(*a, **k)
        raise KeyboardInterrupt("killed after the publish")
    monkeypatch.setattr(M.merge, "merge_and_write_bounded", publish_then_die)
    with pytest.raises(KeyboardInterrupt):
        run_until_quiet(env, fake)
    monkeypatch.setattr(M.merge, "merge_and_write_bounded", real)
    r = env.run(fake)
    assert env.rows("DS_RP_TD_SEED") == 25
    assert "DS_RP_TD_SEED" in (r.changed_keys or {})


def test_k4_chunks_left_by_a_kill_after_the_complete_state_are_swept(env, monkeypatch):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    real = M._Pass._sweep
    calls = {"n": 0}

    def die_on_first(self, code, keep):
        if code == "DS_RP_TD_SEED":
            calls["n"] += 1
            if calls["n"] == 1:
                raise KeyboardInterrupt("killed before the deletes")
        return real(self, code, keep)
    monkeypatch.setattr(M._Pass, "_sweep", die_on_first)
    with pytest.raises(KeyboardInterrupt):
        run_until_quiet(env, fake)
    assert env.chunks()
    monkeypatch.setattr(M._Pass, "_sweep", real)
    env.run(fake)
    assert not [c for c in env.chunks() if "DS_RP_TD_SEED" in c]


# --------------------------------------------------------------------------------------------- C3 / C12
def test_origin_residue_is_measured_and_kept(env):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    old = fake.flows["DS_RP_TD_SEED"][:P - 2] + series(2, geo="2025", year=2021)   # two rows INSEE no longer has
    env.put("DS_RP_TD_SEED", old)
    run_until_quiet(env, fake)
    st = env.state()["flows"]["DS_RP_TD_SEED"]
    assert st["residue"]["rows"] == 2
    assert env.rows("DS_RP_TD_SEED") == 25 + 2                           # MERGE: kept, never deleted (Q1)


def test_a_no_op_tail_reports_no_change_and_writes_nothing(env):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    before = (env.store / "DS_RP_TD_SEED.parquet").read_bytes()
    fake.modified["DS_RP_TD_SEED"] = "2026-10-01T00:00:00"              # catalog says changed; data did not
    r = env.run(fake)
    assert "DS_RP_TD_SEED" not in (r.changed_keys or {})
    assert (env.store / "DS_RP_TD_SEED.parquet").read_bytes() == before


# --------------------------------------------------------------------------------------------- C9 / C10
def test_a_concurrent_state_write_is_refused_not_overwritten(env, monkeypatch):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.run(fake)
    real_save = S.FlowState.save
    state_path = env.store / S.STATE_FILE

    def other_writer_first(self):
        d = json.loads(state_path.read_text())
        d["writer"] = "someone else"
        state_path.write_text(json.dumps(d))
        monkeypatch.setattr(S.FlowState, "save", real_save)
        return real_save(self)
    monkeypatch.setattr(S.FlowState, "save", other_writer_first)
    r = env.run(fake)
    assert "STATE CONFLICT" in r.error and r.status == "transient_fail"
    assert json.loads(state_path.read_text())["writer"] == "someone else"


def test_a_corrupt_state_file_is_copied_and_rebootstrapped(env):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    (env.store / S.STATE_FILE).write_bytes(b"{not json")
    r = env.run(fake)
    assert "STATE CORRUPT" in r.error
    assert list(env.store.glob(S.STATE_FILE + ".corrupt-*"))


def test_a_fresh_foreign_lease_is_left_alone(env):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(95)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    env.run(fake)
    st = env.state()
    st["flows"]["DS_RP_TD_SEED"]["attempt"]["owner"] = "gh:other-run"
    st["flows"]["DS_RP_TD_SEED"]["attempt"]["heartbeat_utc"] = S.utcnow()
    (env.store / S.STATE_FILE).write_text(json.dumps(st, indent=1, sort_keys=True))
    n0 = len([u for u in fake.calls if "/data/DS_RP_TD_SEED" in u])
    r = env.run(fake)
    assert "left alone" in r.error
    assert len([u for u in fake.calls if "/data/DS_RP_TD_SEED" in u]) == n0


def test_delete_only_touches_strict_chunk_keys(env):
    d = env.store
    good = d / S.REPULL_DIR / "DS_X" / "a20261016120000-abcdef" / "00000.chunk"
    good.parent.mkdir(parents=True)
    good.write_bytes(b"x")
    assert not S.delete_chunk(str(d / "DS_X.parquet"), flow="DS_X", current_attempt=None)
    assert not S.delete_chunk(str(good), flow="DS_Y", current_attempt=None)
    assert not S.delete_chunk(str(good), flow="DS_X", current_attempt="a20261016120000-abcdef")
    bad = d / S.REPULL_DIR / "DS_X" / "a20261016120000-abcdef" / "00000.parquet"
    bad.write_bytes(b"x")
    assert not S.delete_chunk(str(bad), flow="DS_X", current_attempt=None)
    assert S.delete_chunk(str(good), flow="DS_X", current_attempt=None) and not good.exists()
    assert bad.exists()


# --------------------------------------------------------------------------------------------- R2-6 / C13
def test_a_non_deterministic_page_1_blocks_the_repair_source_wide(env):
    flows = _control_flows()
    flows["DS_WALK"] = series(5)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_WALK", flows["DS_WALK"][:4])
    fake.flip_page1.add("DS_RP_TD_SEED")
    r = env.run(fake)
    assert "REPAIR BLOCKED: Melodi paging not deterministic" in r.error
    assert env.rows("DS_RP_TD_SEED") == P                                # no repair pull
    assert r.status == "transient_fail"


def test_progress_notes_differ_and_the_stall_label_fires_at_four_not_three(env, monkeypatch):
    flows = _control_flows()
    flows["DS_RP_TD_SEED"] = series(400)
    fake = FakeMelodi(env.clock, flows, step=200.0)
    _seed_store(env, fake)
    a, b = env.run(fake), env.run(fake)
    assert a.status == b.status == "transient_fail"
    assert a.error != b.error and "rows staged" in b.error
    fake.faults.update({("DS_RP_TD_SEED", p): 503 for p in range(1, 60)})
    notes = [env.run(fake).error for _ in range(4)]                     # passes without progress: 1, 2, 3, 4
    assert not any("REPAIR STALLED" in n for n in notes[:3]), notes      # not at 3
    assert "REPAIR STALLED" in notes[3] and "in 4 passes" in notes[3]    # at 4


# --------------------------------------------------------------------------------------------- R2-3 / R2-7 / R2-8 / C9
def test_the_clock_reads_the_armed_alarm_first(monkeypatch):
    import signal
    from updater import orchestrate
    if not hasattr(signal, "setitimer"):
        pytest.skip("no setitimer on this platform: the armed-alarm path is POSIX-only")

    class Armed:
        armed = True
    monkeypatch.setattr(orchestrate, "_ARMED_DEADLINE", Armed())
    monkeypatch.setattr(signal, "getitimer", lambda which: (600.0, 0.0))
    w, src = S.unit_window_s()
    assert (w, src) == (600.0, "armed alarm")


def test_without_an_alarm_the_clamped_window_bounds_the_pass(monkeypatch):
    from updater import orchestrate
    monkeypatch.setattr(orchestrate, "_ARMED_DEADLINE", None)
    monkeypatch.setattr(orchestrate, "_unit_window_min", lambda: 10.0)
    w, src = S.unit_window_s()
    assert (w, src) == (600.0, "clamped window")
    c = S.Clock(now=lambda: 0.0, window_s=w, source=src)
    assert c.hard_end == 600.0 - S.HARD_MARGIN_S and c.soft_end == c.hard_end


def test_the_merge_waits_when_the_margin_does_not_fit(env):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    r = env.run(fake, window_s=S.HARD_MARGIN_S + 125)                   # room to page, not to merge
    assert env.rows("DS_RP_TD_SEED") == P
    st = env.state()["flows"]["DS_RP_TD_SEED"]
    assert st["status"] in (S.MERGE_PENDING, S.IN_PROGRESS, S.NEEDS_ORIGIN)


def test_a_pass_ends_by_the_soft_stop_with_all_three_slots_busy(env):
    flows = _control_flows()
    for i in range(30):
        flows[f"DS_W{i:02d}"] = series(5)
    flows["DS_RP_TD_SEED"] = series(400)
    fake = FakeMelodi(env.clock, flows, step=20.0)
    _seed_store(env, fake)
    for i in range(30):
        env.put(f"DS_W{i:02d}", flows[f"DS_W{i:02d}"][:4])
    c = S.Clock(now=env.clock, window_s=45 * 60, source="test")
    M.update(None, None, _session=fake, _clock=c)
    assert env.clock.t <= c.soft_end + M.TIMEOUT + 2 * fake.step


def test_staging_on_D_is_refused(monkeypatch):
    monkeypatch.setenv("ECONDL_DUCKDB_TMP", "D:\\temp\\spill")
    with pytest.raises(DefinitiveError, match="D:"):
        S.staging_dir()


def test_before_T0_a_local_backend_run_is_refused(monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(S, "_cut_over", lambda: False)
    monkeypatch.setattr(S, "_ALLOW_LOCAL_BACKEND", False)
    with pytest.raises(DefinitiveError, match="before T0"):
        S.refuse_wrong_backend()


# --------------------------------------------------------------------------------------------- R2-4 on R2
class FakeR2:
    """put_if / get / head_meta of R2Blob, in memory. ETag = md5 of the bytes (a single-part PUT)."""

    def __init__(self):
        self.objs, self.puts = {}, []

    def get(self, key):
        return self.objs.get(key)

    def put_if(self, key, data, *, if_match=None, if_none_match=False):
        import hashlib
        cur = self.objs.get(key)
        self.puts.append((key, if_match, if_none_match))
        if if_none_match and cur is not None:
            return None
        if if_match is not None and (cur is None or hashlib.md5(cur).hexdigest() != if_match):
            return None
        self.objs[key] = data
        return hashlib.md5(data).hexdigest()

    def head_meta(self, key):
        return None


def test_on_r2_the_if_match_token_is_md5_of_the_bytes_read_and_a_race_is_refused(tmp_path, monkeypatch):
    import hashlib
    from updater import blob
    fake = FakeR2()
    monkeypatch.setattr(config, "source_dir", lambda s: str(tmp_path / "data" / "clean_full" / s))
    monkeypatch.setattr(blob, "_r2_routed", lambda: fake)
    key = "clean_full/insee_melodi/_flows.json"
    first = json.dumps({"schema": S.SCHEMA, "flows": {}}).encode()
    fake.objs[key] = first
    st = S.FlowState.load()
    assert st.token == hashlib.md5(first).hexdigest()                  # the token describes the bytes read
    st.save()
    assert fake.puts[-1][1] == hashlib.md5(first).hexdigest()
    fake.objs[key] = b'{"schema": 1, "flows": {}, "writer": "other"}'   # another writer lands in between
    with pytest.raises(S.StateConflict):
        st.save()
    assert b"other" in fake.objs[key]


# --------------------------------------------------------------------------------------------- C11 / C5 / C7
def test_a_refused_merge_keeps_the_flow_merge_pending_not_a_source_veto(env, monkeypatch):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)

    def refuse(*a, **k):
        raise DefinitiveError("would shrink (test)")
    monkeypatch.setattr(M.merge, "merge_and_write_bounded", refuse)
    r = env.run(fake)
    assert r.status == "transient_fail" and "MERGE REFUSED" in r.error
    assert env.state()["flows"]["DS_RP_TD_SEED"]["status"] == S.MERGE_PENDING


def test_a_tail_the_server_answers_whole_is_detected_and_measured_as_a_full_pull(env):
    flows = _control_flows()
    # 10 rows at INSEE: the page-beyond probe (page 2) is empty, so the flow stays legacy and is TAILED
    flows["DS_RP_ACT"] = series(4, year=2012) + series(3, year=2017) + series(3, year=2023)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_RP_ACT", series(3, year=2023) + series(1, geo="2025", year=2023))
    fake.ignore_start.add("DS_RP_ACT")                                   # Melodi ignores startPeriod for RP flows
    run_until_quiet(env, fake)
    st = env.state()["flows"]["DS_RP_ACT"]
    assert st["start_ignored"] is True and st["basis"] == "origin"
    assert st["residue"]["rows"] == 1                                    # the old-vintage row, kept


def test_an_unreadable_catalog_counts_as_changed_never_as_a_skip(env):
    flows = _control_flows()
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    n0 = len([u for u in fake.calls if f"/data/{M.PROBE_EMPTY_CONTROL}" in u])
    fake.modified[M.PROBE_EMPTY_CONTROL] = None                          # catalog answers 500
    env.run(fake)
    assert len([u for u in fake.calls if f"/data/{M.PROBE_EMPTY_CONTROL}" in u]) > n0
