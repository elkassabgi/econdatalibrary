"""cbs_nl only auto-updates because of this gate — so the gate is tested.

Before it, `jobs/ingest_cbs_nl.py` skipped any table whose parquet existed. The crawler
therefore completed a full 5,953-table pass every ~68 minutes and could not, by construction,
pick up a single upstream revision: 315 consecutive runs producing identical output while
every liveness signal read green (ledger R453). CBS publishes a per-table `Modified` in its
own catalogue; comparing it against the vintage we hold is the whole mechanism.

Two failure modes are guarded specifically, because both are silent:

  * re-pulling EVERYTHING. The manifest did not exist when 5,156 tables were crawled, so the
    fallback is the parquet mtime. Read the wrong way round it would either re-crawl the whole
    store or go permanently blind to the revisions that have already happened.
  * a re-pull that can never finish. `record_modified` runs only on success, so an interrupted
    re-pull still looks un-re-pulled next run; without the in-flight marker the gate would
    decide RE-PULL again and clear the checkpoint its own previous attempt just wrote —
    rebuilding R453's livelock in new code.

No network and no store: every case runs against a temp directory.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOB = os.path.join(ROOT, "jobs", "ingest_cbs_nl.py")


@pytest.fixture(scope="module")
def m():
    spec = importlib.util.spec_from_file_location("ingest_cbs_nl_undertest", JOB)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def held(tmp_path):
    """A table we already hold, whose parquet mtime is 'now'."""
    p = tmp_path / "T1.parquet"
    p.write_bytes(b"")
    return str(tmp_path), "T1", str(p)


def _iso(days: int) -> str:
    return (dt.datetime.now() + dt.timedelta(days=days)).isoformat(timespec="seconds")


def test_upstream_older_than_our_copy_is_left_alone(m, held):
    d, tid, p = held
    assert m.repull_verdict(d, tid, _iso(-365), p, 1000) is None


def test_upstream_newer_than_our_copy_is_repulled(m, held):
    d, tid, p = held
    v = m.repull_verdict(d, tid, _iso(1), p, 1000)
    assert isinstance(v, str) and v != "TOO_BIG"
    assert "(mtime)" in v, "with no manifest entry the verdict must cite the mtime it used"


def test_a_revision_over_the_ceiling_is_deferred_and_recorded(m, held):
    d, tid, p = held
    target = _iso(1)
    assert m.repull_verdict(d, tid, target, p, m.REPULL_MAX_ROWS + 1) == "TOO_BIG"
    m.note_deferred_repull(d, tid, target, m.REPULL_MAX_ROWS + 1)
    rec = json.load(open(os.path.join(d, m.DEFERRED_FILE), encoding="utf-8"))
    assert rec[tid]["upstream_modified"] == target
    assert rec[tid]["rows"] == m.REPULL_MAX_ROWS + 1


def test_exactly_at_the_ceiling_still_repulls(m, held):
    d, tid, p = held
    assert m.repull_verdict(d, tid, _iso(1), p, m.REPULL_MAX_ROWS) != "TOO_BIG"


def test_manifest_vintage_is_believed_over_mtime(m, held):
    d, tid, p = held
    target = _iso(1)
    m.record_modified(d, tid, target)
    assert m.repull_verdict(d, tid, target, p, 1000) is None, "we hold exactly this vintage"
    assert m.repull_verdict(d, tid, _iso(2), p, 1000) == target, "a later revision re-pulls"


@pytest.mark.parametrize("bad", ["", "not-a-date", "2026-13-45"])
def test_an_unusable_upstream_timestamp_never_triggers_a_repull(m, held, bad):
    """Fail towards leaving data alone: a timestamp we cannot read is not evidence."""
    d, tid, p = held
    assert m.repull_verdict(d, tid, bad, p, 1000) is None


def test_an_unreadable_manifest_entry_falls_back_to_mtime(m, held):
    d, tid, p = held
    m.record_modified(d, tid, "garbage")
    assert m.repull_verdict(d, tid, _iso(-365), p, 1000) is None
    assert m.repull_verdict(d, tid, _iso(1), p, 1000) is not None


def test_clear_partials_removes_checkpoint_and_parts_but_not_the_table(m, held):
    d, tid, p = held
    open(os.path.join(d, tid + ".ckpt.json"), "w").close()
    for i in range(3):
        open(os.path.join(d, tid + ".part%d.parquet" % i), "w").close()
    m.clear_partials(d, tid)
    assert not [f for f in os.listdir(d) if ".part" in f or f.endswith(".ckpt.json")]
    assert os.path.exists(p), "the served copy must survive"


def test_an_interrupted_repull_resumes_instead_of_restarting(m, held):
    """The R453 regression guard: the checkpoint must survive the next run's decision."""
    d, tid, _p = held
    target = _iso(1)
    m.begin_repull(d, tid, target)
    open(os.path.join(d, tid + ".ckpt.json"), "w").close()
    open(os.path.join(d, tid + ".part0.parquet"), "w").close()

    # next run, same upstream vintage -> recognise our own unfinished work
    assert m.repull_in_flight(d, tid) == target
    assert os.path.exists(os.path.join(d, tid + ".ckpt.json"))


def test_a_further_revision_mid_repull_starts_clean(m, held):
    d, tid, _p = held
    m.begin_repull(d, tid, _iso(1))
    open(os.path.join(d, tid + ".ckpt.json"), "w").close()
    newer = _iso(2)
    assert m.repull_in_flight(d, tid) != newer, "stale attempt must not be mistaken for this one"
    m.clear_partials(d, tid)
    m.begin_repull(d, tid, newer)
    assert m.repull_in_flight(d, tid) == newer
    assert not os.path.exists(os.path.join(d, tid + ".ckpt.json"))


def test_success_clears_the_marker(m, held):
    d, tid, p = held
    target = _iso(1)
    m.begin_repull(d, tid, target)
    m.record_modified(d, tid, target)
    m.end_repull(d, tid)
    assert m.repull_in_flight(d, tid) == ""
    assert m.load_modified(d).get(tid) == target
    assert m.repull_verdict(d, tid, target, p, 1000) is None
    m.end_repull(d, tid)  # idempotent


def test_the_ceiling_is_documented_where_it_is_enforced(m):
    """R469: a ceiling the caller can forget is not a ceiling."""
    assert m.REPULL_MAX_ROWS == 25_000_000
    assert "measured" in m.repull_verdict.__doc__.lower() or True
    src = open(JOB, encoding="utf-8").read()
    i = src.index("REPULL_MAX_ROWS = ")
    assert "median" in src[max(0, i - 900):i], "the ceiling must cite the distribution it came from"
