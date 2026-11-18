"""The gus_dbw post-rebuild row-loss guard and the owed-publish record (ledger R1936).

After a COMPLETE refresh sweep, `finish_area` rebuilds a CANDIDATE beside the area's final, compares
it with the final, and replaces the final only when nothing below the refresh window changed and the
tail lost no more than a small floor. A passed rebuild is recorded as owed for publish in the
crawler's own append-only `_publish_owed.jsonl`; a separate publisher (not in the crawler) pays it and
writes `_published.jsonl`. Hermetic: local files only, no network, no store client.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from jobs import gus_dbw_refresh as R          # noqa: E402
from jobs import ingest_gus_dbw as G           # noqa: E402

TAIL = 2025


def _write(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.table({
        "series_key": pa.array([r[0] for r in rows], pa.string()),
        "obs_date": pa.array([r[1] for r in rows], pa.date32()),
        "value": pa.array([r[2] for r in rows], pa.float64()),
    }), path, compression="zstd")
    return path


def _md5(p):
    return hashlib.md5(open(p, "rb").read()).hexdigest()


HISTORY = [(f"v1:s1:k{i}", dt.date(2010 + i % 10, 12, 31), float(i)) for i in range(50)]
OLD_TAIL = [(f"v1:s1:t{i}", dt.date(2025, 12, 31), float(i)) for i in range(300)]


@pytest.fixture
def env(tmp_path, monkeypatch):
    out = tmp_path / "gus_dbw"
    parts = out / "parts"
    monkeypatch.setattr(G, "OUT", str(out))
    monkeypatch.setattr(G, "REFRESH_PARTS", str(parts / "_refresh"))
    monkeypatch.setattr(G, "REFUSED_PARTS", str(parts / "_refresh_refused"))
    monkeypatch.setattr(G, "GUARD_SPILL", str(out / "_tmp" / "guard_spill"))
    final = _write(str(out / "area_1.parquet"), HISTORY + OLD_TAIL)
    return {"out": str(out), "final": final, "tail_dir": str(parts / "_refresh" / "area_1"),
            "refused": str(parts / "_refresh_refused" / "area_1")}


def _tail_part(env, rows, name="v1_s1_y2025.parquet"):
    return _write(os.path.join(env["tail_dir"], name), rows)


def _owed_lines(env):
    p = os.path.join(env["out"], R.OWED_FILE)
    return [json.loads(x) for x in open(p, encoding="utf-8")] if os.path.exists(p) else []


def _assert_refused(env, before_md5, state):
    assert _md5(env["final"]) == before_md5, "a refused rebuild must leave the final byte-identical"
    assert not os.path.exists(env["final"] + G.CANDIDATE_SUFFIX), "the candidate must be removed"
    assert _owed_lines(env) == [], "a refused rebuild must owe nothing for publish"
    assert "1" not in state, "a refused area must stay due"
    assert R.tail_parts_for_area(1, G.REFRESH_PARTS) == [], "refused tail parts must leave the rebuild's reach"
    stamps = os.listdir(env["refused"])
    assert len(stamps) == 1 and os.path.exists(os.path.join(env["refused"], stamps[0], "guard.json"))


# ------------------------------------------------------------------ pass
def test_a_normal_rebuild_passes_replaces_the_final_and_is_owed(env):
    # the publisher revised one value, withdrew 3 pairs (the area_43 shape) and added a new year
    new_tail = [(k, d, (v + 0.5 if k == "v1:s1:t0" else v)) for k, d, v in OLD_TAIL[3:]]
    new_tail += [("v1:s1:t0", dt.date(2026, 12, 31), 7.0)]
    _tail_part(env, new_tail)
    state = {}
    assert G.finish_area(1, env["final"], state, TAIL) is True
    t = pq.read_table(env["final"])
    assert t.num_rows == len(HISTORY) + len(new_tail)
    assert "1" in state
    owed = _owed_lines(env)
    assert len(owed) == 1
    rec = owed[0]
    assert rec["area"] == "1" and rec["md5"] == _md5(env["final"]) and rec["rows"] == t.num_rows
    assert rec["size"] == os.path.getsize(env["final"])
    assert rec["guard"]["tail_lost"] == 3 and rec["guard"]["pre_lost"] == 0
    assert not os.path.exists(env["final"] + G.CANDIDATE_SUFFIX)
    assert R.tail_parts_for_area(1, G.REFRESH_PARTS) == []


# ------------------------------------------------------------------ refusals
def test_a_lost_row_below_the_window_is_refused(env, monkeypatch):
    _tail_part(env, OLD_TAIL)
    real = R.rebuild_with_tail

    def buggy(final_path, tail_parts, tail_start, out_path=None):
        res = real(final_path, tail_parts, tail_start, out_path=out_path)
        t = pq.read_table(out_path)
        pq.write_table(t.slice(1), out_path)            # drops one pre-window row, silently
        return res

    monkeypatch.setattr(R, "rebuild_with_tail", buggy)
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)
    g = json.load(open(os.path.join(env["refused"], os.listdir(env["refused"])[0], "guard.json")))
    assert g["pre_lost"] == 1 and g["ok"] is False


def test_a_changed_value_below_the_window_is_refused(env, monkeypatch):
    _tail_part(env, OLD_TAIL)
    real = R.rebuild_with_tail

    def buggy(final_path, tail_parts, tail_start, out_path=None):
        res = real(final_path, tail_parts, tail_start, out_path=out_path)
        t = pq.read_table(out_path).to_pydict()
        i = next(i for i, d in enumerate(t["obs_date"]) if d < dt.date(TAIL, 1, 1))
        t["value"][i] += 1.0                              # same row count, one history value moved
        pq.write_table(pa.table(t, schema=pq.read_schema(out_path)), out_path)
        return res

    monkeypatch.setattr(R, "rebuild_with_tail", buggy)
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)


def test_a_silent_empty_tail_sweep_is_refused(env):
    # the sweep "completed" (no exception) but brought back nothing: 300 tail pairs would vanish
    os.makedirs(env["tail_dir"], exist_ok=True)
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)


def test_tail_loss_just_above_the_floor_is_refused_and_at_the_floor_passes(env, monkeypatch):
    monkeypatch.setattr(R, "GUARD_TAIL_LOSS_ABS", 10)
    monkeypatch.setattr(R, "GUARD_TAIL_LOSS_FRAC", 0.0)
    _tail_part(env, OLD_TAIL[11:])                        # 11 lost > floor 10
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)
    _tail_part(env, OLD_TAIL[10:])                        # 10 lost == floor 10
    assert G.finish_area(1, env["final"], state, TAIL) is True
    assert len(_owed_lines(env)) == 1


def test_a_guard_that_cannot_run_refuses(env, monkeypatch):
    _tail_part(env, OLD_TAIL)

    def broken(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(R, "row_loss_guard", broken)
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)


def test_the_guard_counts_duplicate_pairs_as_a_multiset(tmp_path):
    k, d = "v1:s1:x", dt.date(2025, 6, 1)
    old = _write(str(tmp_path / "old.parquet"), [(k, d, 1.0), (k, d, 1.0), (k, d, 1.0)])
    new = _write(str(tmp_path / "new.parquet"), [(k, d, 1.0)])
    g = R.row_loss_guard(old, new, TAIL)
    assert g["tail_lost"] == 2 and g["tail_rows_old"] == 3 and g["tail_rows_new"] == 1


# ------------------------------------------------------------------ the owed record
def test_a_failed_owed_record_leaves_the_area_due_and_a_retry_records_it(env, monkeypatch):
    _tail_part(env, OLD_TAIL)
    real = R.note_publish_owed

    def fails(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(R, "note_publish_owed", fails)
    state = {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    assert "1" not in state, "an area whose debt was not recorded must stay due"
    assert _owed_lines(env) == []
    monkeypatch.setattr(R, "note_publish_owed", real)
    _tail_part(env, OLD_TAIL)                             # the next run's sweep
    assert G.finish_area(1, env["final"], state, TAIL) is True
    assert "1" in state and len(_owed_lines(env)) == 1
    assert _owed_lines(env)[0]["md5"] == _md5(env["final"])


def test_owed_until_published_at_that_md5_and_owed_again_after_a_newer_rebuild(tmp_path):
    out = str(tmp_path)
    f = _write(str(tmp_path / "area_7.parquet"), HISTORY)
    g = {"ok": True, "tail_lost": 0}
    r1 = R.note_publish_owed(out, 7, f, g, {"rows_before": 50, "rows_after": 50})
    owed, bad = R.owed_areas(out)
    assert set(owed) == {"7"} and bad == 0
    with open(os.path.join(out, R.PUBLISHED_FILE), "a", encoding="utf-8") as fh:   # the publisher's file
        fh.write(json.dumps({"area": "7", "md5": r1["md5"]}) + "\n")
    assert R.owed_areas(out)[0] == {}
    _write(f, HISTORY + OLD_TAIL)                                                  # a later rebuild
    r2 = R.note_publish_owed(out, 7, f, g, {"rows_before": 50, "rows_after": 350})
    owed, _ = R.owed_areas(out)
    assert set(owed) == {"7"} and owed["7"]["md5"] == r2["md5"] != r1["md5"]


def test_a_torn_line_is_counted_not_hidden(tmp_path):
    out = str(tmp_path)
    f = _write(str(tmp_path / "area_7.parquet"), HISTORY)
    R.note_publish_owed(out, 7, f, {"ok": True}, {})
    with open(os.path.join(out, R.OWED_FILE), "a", encoding="utf-8") as fh:
        fh.write('{"area": "8", "md5": "ab')                                       # crash mid-append
    owed, bad = R.owed_areas(out)
    assert set(owed) == {"7"} and bad == 1


# ------------------------------------------------------------------ what the crawler must not do
def test_the_crawler_holds_no_store_client_and_publishes_nothing():
    """R1936: no upload, no derive, no R2 or store client in the crawler. The publisher is separate."""
    banned = ("r2_util", "boto3", "botocore", "csv_store", "upload_r2", "derive_csv", "updater.blob",
              "from updater import blob", "put_object", "upload_file", "state.db", "StateStore")
    for p in (os.path.join(ROOT, "jobs", "ingest_gus_dbw.py"), os.path.join(ROOT, "jobs", "gus_dbw_refresh.py")):
        code = "\n".join(ln.split("#", 1)[0] for ln in open(p, encoding="utf-8"))   # comments may name them
        hits = [b for b in banned if b in code]
        assert not hits, f"{os.path.basename(p)} reaches a store or publisher: {hits}"


def test_the_candidate_is_never_a_parquet_name_and_startup_sweeps_it():
    assert not G.CANDIDATE_SUFFIX.endswith(".parquet")
    import inspect
    assert "CANDIDATE_SUFFIX" in inspect.getsource(G.main)
