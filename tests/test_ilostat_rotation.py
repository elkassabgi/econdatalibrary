"""ilostat's budgeted walk must resume where the last pass stopped (AR-328; R190, R313, R377).

WHAT WAS WRONG. update() walked sorted(TOC, key=id) from the alphabetical top on every pass. The
sidecar (_toc_vintages.json) skips an indicator only while ILO's last.update stamp is unchanged, and
ILO re-stamps most of the list between weekly passes, so every 25-minute pass re-fetched the same head.
Measured on state.db and the CI logs for all 11 passes from 2026-07-30 to 2026-10-14: the first
deferred id sat at positions 675-1298 of ~1,960, attempted + unchanged + deferred matched the TOC
exactly, and 658 indicators after the furthest cut were never fetched by any pass. The sidecar was
persisting - the head simply never stopped changing (R313: a sidecar drains a fixed order only while
the head stops moving).

THE FIX. Resume just after the last STARTED indicator (load_rotation / rotate_after, with a
bisect_right resume when the bookmarked id has left the TOC), and save the bookmark once per
indicator, after the deferral branch and BEFORE the work, so an indicator that never completes cannot
stall the walk (review AR-329, the ecb / insee_melodi / abs placement, R273).

Hermetic: the ILO downloader is faked and the store is a tmp folder under the LOCAL backend. T1 is the
negative control: on the code before this fix every pass works the same three ids and it fails.
"""
from __future__ import annotations

import inspect
import json
import os
import re
import sqlite3
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import config, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.fetchers import ilostat as I  # noqa: E402
from updater.strategies.fetchers._common import ROTATION_FILE  # noqa: E402

IDS = [f"IND{i:02d}_A" for i in range(1, 11)]          # 10 indicators, already in id order


class _Budget:
    """Stands in for Deadline: `per_pass` indicators may START, then the budget is spent."""

    def __init__(self, per_pass):
        self.per_pass = per_pass

    def __call__(self, minutes):
        left = [self.per_pass]

        class _D:
            def spent(self_inner):
                if left[0] <= 0:
                    return True
                left[0] -= 1
                return False
        return _D()


class _Rig:
    """A fake ILO whose every indicator is re-stamped before each pass (what ILO does weekly)."""

    def __init__(self, tmp_path, monkeypatch, ids=IDS, per_pass=3, kill_on=None):
        self.tmp, self.mp, self.ids = tmp_path, monkeypatch, list(ids)
        self.passes = 0
        self.worked: list[list[str]] = []
        self.kill_on = kill_on
        monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
        monkeypatch.delenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", raising=False)
        monkeypatch.setattr(I.config, "source_dir", lambda s: str(tmp_path))
        monkeypatch.setattr(I.ig, "download_toc", lambda: None)
        monkeypatch.setattr(I.ig, "read_toc", self._toc)
        monkeypatch.setattr(I.ig, "process_one", self._process)
        monkeypatch.setattr(I, "Deadline", _Budget(per_pass))

    def _toc(self):
        stamp = f"0{self.passes}/10/2026 10:00:00"           # every id re-stamped each pass
        return [{"id": i, "last.update": stamp, "n.records": 5} for i in self.ids]

    def _process(self, ds):
        iid = ds["id"]
        if self.kill_on and iid == self.kill_on:
            raise _Kill(iid)
        self.worked[-1].append(iid)
        pq.write_table(pa.table({"series_key": [f"{iid}|x"],
                                 "obs_date": pa.array(["2025-12-31"]).cast(pa.date32()),
                                 "value": [1.0]}), str(self.tmp / f"{iid}.parquet"))
        return iid, 1, 1, 1, "ok", "2025-12-31", "2025-12-31", 10, 0.1

    def run_pass(self):
        self.passes += 1
        self.worked.append([])
        return I.update(None, None)

    def bookmark(self):
        p = self.tmp / ROTATION_FILE
        return json.loads(p.read_text(encoding="utf-8"))["after"] if p.exists() else None


class _Kill(BaseException):
    """A kill mid-indicator (BaseException: the fetcher's `except Exception` must not swallow it)."""


# ---- T1: the R313 property, through update() ---------------------------------------------------
def test_t1_every_pass_resumes_where_the_last_stopped_while_the_head_keeps_changing(tmp_path, monkeypatch):
    rig = _Rig(tmp_path, monkeypatch, per_pass=3)
    for _ in range(4):
        res = rig.run_pass()
        assert res.status == "partial", res.status
    assert rig.worked == [IDS[0:3], IDS[3:6], IDS[6:9], [IDS[9], IDS[0], IDS[1]]], rig.worked
    covered = set().union(*rig.worked)
    assert covered == set(IDS), f"never fetched: {sorted(set(IDS) - covered)}"


def test_t1_the_deferral_note_names_the_tail_after_the_cut(tmp_path, monkeypatch):
    rig = _Rig(tmp_path, monkeypatch, per_pass=3)
    rig.run_pass()
    res = rig.run_pass()
    assert "deferred by budget" in res.error
    first = res.error[res.error.index("[") + 1:].split(",")[0].strip()
    assert first == IDS[6], (first, res.error)        # pass 2 worked IND04-06, so IND07 is first deferred


# ---- T2: a bookmark that left the TOC ----------------------------------------------------------
def test_t2_a_bookmark_no_longer_in_the_toc_resumes_at_the_next_id_not_the_top(tmp_path, monkeypatch):
    rig = _Rig(tmp_path, monkeypatch, per_pass=3)
    rig.run_pass()                                      # works IND01-03, bookmark IND03
    assert rig.bookmark() == "IND03_A"
    rig.ids.remove("IND03_A")                           # ILO renamed / withdrew it
    rig.run_pass()
    assert rig.worked[-1] == ["IND04_A", "IND05_A", "IND06_A"], rig.worked[-1]


def test_t2_resume_order_wraps_past_the_last_id():
    rows = [{"id": i} for i in ("C", "A", "B")]
    assert [r["id"] for r in I._resume_order(rows, "B")] == ["C", "A", "B"]
    assert [r["id"] for r in I._resume_order(rows, "BB")] == ["C", "A", "B"]     # vanished, between
    assert [r["id"] for r in I._resume_order(rows, "Z")] == ["A", "B", "C"]      # vanished, after the last


# ---- T3: no bookmark, empty, corrupt ------------------------------------------------------------
@pytest.mark.parametrize("content", [None, b"", b"{not json", b'{"after": ""}', b'{"other": 1}'])
def test_t3_a_missing_empty_or_corrupt_bookmark_starts_at_the_top_and_skips_nothing(tmp_path, monkeypatch,
                                                                                    content):
    if content is not None:
        (tmp_path / ROTATION_FILE).write_bytes(content)
    rig = _Rig(tmp_path, monkeypatch, per_pass=20)
    rig.run_pass()
    assert rig.worked[-1] == IDS, rig.worked[-1]


# ---- T4: one save per indicator, after the deferral branch and BEFORE the work ------------------
def test_t4_a_kill_mid_indicator_bookmarks_it_so_the_next_pass_starts_after_it(tmp_path, monkeypatch):
    """Saved before the work (R273, review AR-329): a kill inside X leaves the bookmark AT X, so the
    next pass starts after X instead of starting on X and dying there again."""
    rig = _Rig(tmp_path, monkeypatch, per_pass=10, kill_on="IND03_A")
    with pytest.raises(_Kill):
        rig.run_pass()
    assert rig.bookmark() == "IND03_A", rig.bookmark()
    rig.kill_on = None
    rig.run_pass()
    assert rig.worked[-1][0] == "IND04_A", "the pass after a kill starts AFTER the killed indicator"
    assert "IND03_A" in rig.worked[-1], "the killed indicator is worked again when the rotation wraps"


def _poison_passes(rig, exc, passes=6):
    covered = set(rig.worked[-1])
    for _ in range(passes):
        try:
            rig.run_pass()
        except exc:
            pass
        covered |= set(rig.worked[-1])
    return covered


def test_p1_an_indicator_that_never_completes_does_not_stall_the_walk(tmp_path, monkeypatch):
    """R313's real test: the window must move when the head NEVER completes. IND04 dies every pass."""
    rig = _Rig(tmp_path, monkeypatch, per_pass=3)
    rig.run_pass()                                      # IND01-03, bookmark IND03
    rig.kill_on = "IND04_A"
    covered = _poison_passes(rig, _Kill)
    missing = sorted(set(IDS) - covered - {"IND04_A"})
    assert not missing, f"stalled at the poison id; never fetched in 6 passes: {missing}"


def test_p1b_a_publish_failure_escaping_update_does_not_stall_the_walk(tmp_path, monkeypatch):
    """Same, with an ordinary exception raised OUTSIDE the per-indicator try (publish_file)."""
    rig = _Rig(tmp_path, monkeypatch, per_pass=3)
    rig.run_pass()
    real = I.blob.publish_file

    def pub(path):
        if path.endswith("IND04_A.parquet"):
            raise RuntimeError("store PUT refused")
        return real(path)
    monkeypatch.setattr(I.blob, "publish_file", pub)
    covered = _poison_passes(rig, RuntimeError)
    missing = sorted(set(IDS) - covered - {"IND04_A"})
    assert not missing, f"stalled at the poison id; never fetched in 6 passes: {missing}"


def test_t4_a_deferred_or_unchanged_id_is_never_bookmarked(tmp_path, monkeypatch):
    rig = _Rig(tmp_path, monkeypatch, per_pass=3)
    rig.run_pass()
    assert rig.bookmark() == "IND03_A", "the last STARTED id, not the first deferred one (IND04)"
    frozen = rig._toc()                                 # from now on ILO re-stamps nothing
    monkeypatch.setattr(I.ig, "read_toc", lambda: frozen)
    expected = [["IND04_A", "IND05_A", "IND06_A"], ["IND07_A", "IND08_A", "IND09_A"], ["IND10_A"], []]
    for want in expected:                               # pass 4: IND01-03 read unchanged and are skipped
        rig.run_pass()
        assert rig.worked[-1] == want, rig.worked
    assert rig.bookmark() == "IND10_A", "unchanged skips (pass 4's IND01-03, pass 5's all) never move it"


def test_t4_one_save_after_the_deferral_branch_and_before_the_work():
    src = inspect.getsource(I.update)
    saves = [m.start() for m in re.finditer(r"save_rotation\(", src)]
    assert len(saves) == 1, f"exactly one bookmark save per indicator: {saves}"
    s = saves[0]
    assert src[s:].startswith("save_rotation(out_dir, iid)"), src[s:s + 40]
    assert s > src.index("tally.deferred_unit(iid)"), "a save before the deferral branch bookmarks a deferred id"
    assert s < src.index("ig.process_one(ds)"), "saved after the work, a never-completing id stalls the walk"
    assert s < src.index("if unchanged:"), "the bookmark must be saved per indicator, not after the loop"
    assert "_resume_order(rows, load_rotation(out_dir))" in src, "the walk must start at the bookmark"


# ---- T5: through the real run path -------------------------------------------------------------
@pytest.fixture
def orch(tmp_path, monkeypatch):
    for k in ("AQUEDUCT_BACKEND", "AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_BUDGET_MIN",
              "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_DERIVE_BUDGET_MIN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    data = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    store_dir = data / "clean_full" / "ilostat"
    os.makedirs(store_dir)
    os.makedirs(data / "_aqueduct")
    cat = tmp_path / "catalog.db"                       # no ilostat rows: the csv phase maps nothing
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    orchestrate._REG_ENTRIES = None
    rig = _Rig(store_dir, monkeypatch, per_pass=3)
    st = StateStore(str(data / "_aqueduct" / "state.db"))
    yield rig, st
    st.close()


def _first_deferred_per_run(st):
    con = sqlite3.connect(config.STATE_DB)
    notes = [r[0] for r in con.execute(
        "SELECT note FROM runs WHERE source_id='ilostat' ORDER BY id")]
    con.close()
    out = []
    for n in notes:
        m = re.search(r"(\d+) sub-unit\(s\) attempted, none failed; (\d+) deferred by budget", n or "")
        assert m, n
        out.append((int(m.group(1)), n[n.index("[", m.end()) + 1:].split(",")[0].strip()))
    return out


def test_t5_run_once_passes_advance_the_first_deferred_position(orch):
    rig, st = orch
    for _ in range(3):
        rig.passes += 1
        rig.worked.append([])
        orchestrate.run_once(sources=["ilostat"], force=True, store=st, blob=object())
    got = _first_deferred_per_run(st)
    assert len(got) == 3, got
    pos = [IDS.index(f) for _a, f in got]
    for (att, _f), p0, p1 in zip(got[1:], pos, pos[1:]):
        assert (p1 - p0) % len(IDS) >= att, (pos, got)      # the AR-328 measurement, on our own runs
    assert set().union(*rig.worked) == set(IDS[:9]), rig.worked
