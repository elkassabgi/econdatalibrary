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
# 100 series x 3 tail dates, DATE-MAJOR: OLD_TAIL[:k] for k <= 100 withdraws k pairs from k different
# series without emptying any series, so the per-series rule stays out of the floor tests' way.
TAIL_DATES = (dt.date(2025, 3, 31), dt.date(2025, 6, 30), dt.date(2025, 12, 31))
OLD_TAIL = [(f"v1:s1:t{i}", d, float(i)) for d in TAIL_DATES for i in range(100)]


def _small_env(env, n_series, dates=TAIL_DATES[:2]):
    """Rewrite the fixture's final with a SMALL tail (n_series x len(dates) pairs) - the R1937 shape."""
    tail = [(f"v1:s1:u{i}", d, float(i)) for d in dates for i in range(n_series)]
    _write(env["final"], HISTORY + tail)
    return tail


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
    monkeypatch.setattr(R, "GUARD_TAIL_LOSS_BP", 400)    # 4% of 300 pairs = floor 12
    _tail_part(env, OLD_TAIL[13:])                        # 13 lost > floor 12
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)
    _tail_part(env, OLD_TAIL[12:])                        # 12 lost == floor 12
    assert G.finish_area(1, env["final"], state, TAIL) is True
    assert len(_owed_lines(env)) == 1


def test_the_floor_is_a_fraction_with_no_absolute_term(tmp_path):
    """R1937: the old max(100, 1%) floor was 100 on 165 of 194 areas. 1% of 300 is 3; of 40 it is 0."""
    a = _write(str(tmp_path / "a.parquet"), HISTORY + OLD_TAIL)
    assert R.row_loss_guard(a, a, TAIL)["tail_loss_floor"] == 3
    b = _write(str(tmp_path / "b.parquet"), HISTORY + OLD_TAIL[:40])
    assert R.row_loss_guard(b, b, TAIL)["tail_loss_floor"] == 0


# ------------------------------------------------------------------ R1937: small tails and series
def test_a_small_tail_that_goes_empty_is_refused(env):
    """A tail under 100 pairs that comes back EMPTY. The old floor of 100 passed this."""
    _small_env(env, 20)                                   # 40 tail pairs
    os.makedirs(env["tail_dir"], exist_ok=True)           # the sweep "completed" with nothing
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)
    g = json.load(open(os.path.join(env["refused"], os.listdir(env["refused"])[0], "guard.json")))
    assert any(r.startswith("EMPTY sweep") for r in g["reasons"])


def test_the_empty_sweep_rule_refuses_on_its_own(env, monkeypatch):
    """With the floor at 100% and every series allow-listed, ONLY the empty-sweep rule can refuse."""
    tail = _small_env(env, 20)
    monkeypatch.setattr(R, "GUARD_TAIL_LOSS_BP", 10_000)
    monkeypatch.setattr(R, "GUARD_SERIES_DROP_ALLOW", frozenset(k for k, _d, _v in tail))
    os.makedirs(env["tail_dir"], exist_ok=True)
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)


def test_one_lost_pair_in_a_small_tail_is_refused(env):
    """40 tail pairs, one withdrawn, every series still present: floor 0, so refused. Old floor: 100."""
    tail = _small_env(env, 20)
    _tail_part(env, tail[1:])
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)


def test_a_small_area_whole_series_drop_is_refused(env):
    """One whole series (3 pairs) vanishes from a 300-pair tail: 3 lost == floor 3, so ONLY the
    per-series rule refuses it (R1936)."""
    _tail_part(env, [r for r in OLD_TAIL if r[0] != "v1:s1:t5"])
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)
    g = json.load(open(os.path.join(env["refused"], os.listdir(env["refused"])[0], "guard.json")))
    assert g["series_dropped"] == 1 and g["series_dropped_eg"] == ["v1:s1:t5"] and g["tail_lost"] == 3


def test_an_allow_listed_series_drop_passes(env, monkeypatch):
    monkeypatch.setattr(R, "GUARD_SERIES_DROP_ALLOW", frozenset({"v1:s1:t5"}))
    _tail_part(env, [r for r in OLD_TAIL if r[0] != "v1:s1:t5"])
    state = {}
    assert G.finish_area(1, env["final"], state, TAIL) is True
    assert _owed_lines(env)[0]["guard"]["series_dropped"] == 0


def test_the_allow_list_is_empty_by_default():
    assert R.GUARD_SERIES_DROP_ALLOW == frozenset()


# ------------------------------------------------------------------ R1937: the refused folder is capped
def test_refused_parts_are_capped_to_the_newest_per_area(tmp_path):
    parts, refused = str(tmp_path / "_refresh"), str(tmp_path / "_refresh_refused")
    t0 = dt.datetime(2026, 11, 18, 0, 0, 0, tzinfo=dt.timezone.utc)
    for i in range(R.REFUSED_KEEP_PER_AREA + 2):
        _write(os.path.join(parts, "area_1", "v1_s1_y2025.parquet"), OLD_TAIL)
        R.quarantine_tail_parts(1, parts, refused, {"ok": False}, when=t0 + dt.timedelta(hours=i))
    kept = sorted(os.listdir(os.path.join(refused, "area_1")))
    want = [(t0 + dt.timedelta(hours=i)).strftime("%Y%m%dT%H%M%SZ")
            for i in range(2, R.REFUSED_KEEP_PER_AREA + 2)]
    assert kept == want, "the newest refusals must be kept and only those"


def test_refused_still_due_lists_only_refusals_after_the_last_refresh(tmp_path):
    out = str(tmp_path)
    root = os.path.join(out, R.REFUSED_SUBDIR)
    for a, stamp in (("1", "20261118T100000Z"), ("2", "20261110T100000Z"), ("3", "20261118T100000Z")):
        os.makedirs(os.path.join(root, "area_" + a, stamp))
    state = {}
    R.mark_refreshed("2", state, when=dt.datetime(2026, 11, 15, 0, 0, 0, tzinfo=dt.timezone.utc))
    R.mark_refreshed("3", state, when=dt.datetime(2026, 11, 1, 0, 0, 0, tzinfo=dt.timezone.utc))
    R.save_state(state, out)
    got = R.refused_still_due(out)
    assert set(got) == {"1", "3"}, "area 2 refreshed after its refusal; 1 never refreshed; 3 refused later"


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


# ------------------------------------------------------------------ R1938: NULL series_key
def test_a_vanished_null_key_series_is_refused(tmp_path):
    """`NOT list_contains(list, NULL)` is NULL and an ANTI JOIN USING never matches NULL, so a series
    whose key is NULL used to vanish from the tail uncounted (review R1938)."""
    nulls = [(None, d, 1.0) for d in TAIL_DATES]
    old = _write(str(tmp_path / "old.parquet"), HISTORY + OLD_TAIL + nulls)
    new = _write(str(tmp_path / "new.parquet"), HISTORY + OLD_TAIL)
    g = R.row_loss_guard(old, new, TAIL)
    assert not g["ok"] and g["series_dropped"] == 1 and g["series_dropped_eg"] == ["<NULL series_key>"]
    assert g["tail_lost"] == 3


def test_a_null_key_series_present_on_both_sides_is_neither_dropped_nor_lost(tmp_path):
    """Negative control: NULL keys match as keys, so identical NULL-key rows are no loss."""
    nulls = [(None, d, 1.0) for d in TAIL_DATES]
    old = _write(str(tmp_path / "old.parquet"), HISTORY + [(None, dt.date(2011, 1, 1), 2.0)] + OLD_TAIL + nulls)
    g = R.row_loss_guard(old, old, TAIL)
    assert g["ok"] and g["series_dropped"] == 0 and g["tail_lost"] == 0 and g["pre_lost"] == 0


def test_a_lost_null_date_row_is_refused(tmp_path):
    """A row with a NULL obs_date is in neither `< cut` nor `>= cut`; the rebuild drops it (pc.less is
    NULL there), so it is counted with the rows below the window."""
    old = _write(str(tmp_path / "old.parquet"), HISTORY + [("v1:s1:k1", None, 9.0)] + OLD_TAIL)
    new = _write(str(tmp_path / "new.parquet"), HISTORY + OLD_TAIL)
    g = R.row_loss_guard(old, new, TAIL)
    assert not g["ok"] and g["pre_lost"] == 1


# ------------------------------------------------------------------ R1938: clearing a refusal by count
def _clear(env, now=None, **over):
    rec = {"area": "1", "max_series_dropped": 1, "max_tail_lost": 3,
           "reviewed_refusal": "20261118T110000Z", "by": "reviewer",
           "utc": (now or dt.datetime.now(dt.timezone.utc)).isoformat(timespec="seconds")}
    rec.update(over)
    os.makedirs(env["out"], exist_ok=True)
    with open(os.path.join(env["out"], R.CLEARANCE_FILE), "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
    return rec


def _drop_t5(env):
    _tail_part(env, [r for r in OLD_TAIL if r[0] != "v1:s1:t5"])


def test_a_reviewed_count_clearance_passes_the_next_rebuild_once(env):
    _drop_t5(env)
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False          # refused without it
    _assert_refused(env, before, state)
    _clear(env)
    _drop_t5(env)
    assert G.finish_area(1, env["final"], state, TAIL) is True
    g = _owed_lines(env)[0]["guard"]
    assert g["series_dropped"] == 1 and g["clearance"]["by"] == "reviewer"
    assert R.guard_clearance(env["out"], 1, state=state) is None, "a used clearance must be spent"


def test_a_clearance_below_the_measured_drop_still_refuses(env):
    _clear(env, max_series_dropped=0)
    _drop_t5(env)
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)


def test_a_clearance_never_clears_an_empty_sweep(env):
    _clear(env, max_series_dropped=10 ** 9, max_tail_lost=10 ** 9)
    os.makedirs(env["tail_dir"], exist_ok=True)
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)


def test_expired_other_area_and_malformed_clearances_are_ignored(env):
    now = dt.datetime.now(dt.timezone.utc)
    _clear(env, now=now - dt.timedelta(days=R.GUARD_CLEARANCE_DAYS + 1))   # expired
    _clear(env, area="2")                                                  # another area
    _clear(env, by="")                                                     # nobody reviewed it
    _clear(env, max_series_dropped="many")                                 # not a count
    with open(os.path.join(env["out"], R.CLEARANCE_FILE), "a", encoding="utf-8") as f:
        f.write('{"area": "1", "max_ser')                                  # torn
    assert R.guard_clearance(env["out"], 1) is None
    _drop_t5(env)
    before, state = _md5(env["final"]), {}
    assert G.finish_area(1, env["final"], state, TAIL) is False
    _assert_refused(env, before, state)


def test_a_clearance_written_before_the_last_refresh_is_spent(tmp_path):
    with_state = {"1": {"last_refresh": "2026-11-18T12:00:00+00:00"}}
    d = str(tmp_path)
    with open(os.path.join(d, R.CLEARANCE_FILE), "w", encoding="utf-8") as f:
        f.write(json.dumps({"area": "1", "max_series_dropped": 5, "max_tail_lost": 5, "by": "r",
                            "reviewed_refusal": "x", "utc": "2026-11-18T11:00:00+00:00"}) + "\n")
    now = dt.datetime(2026, 11, 18, 13, tzinfo=dt.timezone.utc)
    assert R.guard_clearance(d, 1, now=now, state=with_state) is None
    assert R.guard_clearance(d, 1, now=now, state={})["max_series_dropped"] == 5
