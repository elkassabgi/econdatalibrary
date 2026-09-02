"""What a cbs_nl crawl COMMITS when something goes wrong mid-table.

Three defects, all of them cases where the crawl looked healthy and the data did not survive:

  R621  a failed period-title fetch wrote a short table and a no-retry ZERO registry entry;
  R623  the failure then checkpointed PAST the page it had half-parsed, so the next pass
        resumed after it and completed the table with the aggregate missing;
  R626  the partition boundary checkpointed rows that were still in memory, so anything that
        dropped the buffer - the rewind, or a guard kill - lost whole partitions the
        checkpoint had already declared done.

Each test runs the real ingest_table with only the HTTP boundary replaced, then asserts on what
is on disk afterwards, because every one of these defects was invisible from the outside.
"""
import json
import os
import urllib.parse

import pyarrow.parquet as pq
import pytest

from jobs import ingest_cbs_nl as mod

AGG_CODE = "1989G300"
AGG_TITLE = "1989/1991"


def _titles_for(rows):
    return {"value": [{"Key": r["Perioden"],
                       "Title": AGG_TITLE if r["Perioden"] == AGG_CODE else r["Perioden"]}
                      for r in rows]}


def _schema():
    return {"value": [{"Key": "Perioden", "Type": "TimeDimension"},
                      {"Key": "Region", "Type": "Dimension"},
                      {"Key": "Waarde_1", "Type": "Topic", "Datatype": "Double"}]}


def _install(monkeypatch, rows_by_part, titles_ok, page=50, parts=None):
    calls = []

    def get_json(url):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        if "/DataProperties" in url:
            return _schema()
        if "/Perioden" in url and "TypedDataSet" not in url:
            calls.append("titles")
            if not titles_ok:
                return None
            return _titles_for([r for rs in rows_by_part.values() for r in rs])
        flt = q.get("$filter", [""])[0]
        part = next((p for p in rows_by_part if p is not None and f"'{p}'" in flt), None)
        if part is None:
            part = next(iter(rows_by_part))
        skip = int(q.get("$skip", ["0"])[0])
        top = int(q.get("$top", [str(page)])[0])
        calls.append(f"{part}:{skip}")
        return {"value": rows_by_part[part][skip:skip + top]}

    monkeypatch.setattr(mod, "PAGE", page, raising=False)
    monkeypatch.setattr(mod, "get_json", get_json)
    monkeypatch.setattr(mod, "get_table_columns", lambda t: ["ID", "Perioden", "Region", "Waarde_1"])
    monkeypatch.setattr(mod, "table_row_count", lambda t: sum(len(v) for v in rows_by_part.values()))
    if parts is not None:
        monkeypatch.setattr(mod, "PARTITION_MIN_ROWS", 1, raising=False)
        monkeypatch.setattr(mod, "period_keys", lambda t, c: list(parts))
    return calls


def _rows(part, n, with_agg=False):
    out = []
    for i in range(n):
        code = AGG_CODE if (with_agg and i == n - 1) else f"{1990 + i % 20}MM{1 + i % 12:02d}"
        out.append({"ID": i, "Perioden": code, "Region": part or "R", "Waarde_1": float(i)})
    return out


def _state(d, tid="T"):
    out = os.path.join(d, f"{tid}.parquet")
    obs, keys = 0, []
    if os.path.exists(out):
        t = pq.read_table(out)
        obs, keys = t.num_rows, t.column("series_key").to_pylist()
    ck = os.path.join(d, f"{tid}.ckpt.json")
    zero = os.path.join(d, "_repull_zero.json")
    return {
        "obs": obs,
        "spans": sum(1 for k in keys if "period_span" in k),
        "ckpt": json.load(open(ck)) if os.path.exists(ck) else None,
        "in_zero": tid in json.load(open(zero, encoding="utf-8")) if os.path.exists(zero) else False,
    }


def test_a_failed_title_fetch_commits_nothing_and_is_not_recorded_as_empty(tmp_path, monkeypatch):
    d = str(tmp_path)
    rows = {None: _rows(None, 103, with_agg=True)}
    _install(monkeypatch, rows, titles_ok=False)
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    s = _state(d)
    assert s["obs"] == 0 and not s["in_zero"], s


def test_the_next_pass_then_completes_the_table_WITH_the_aggregate(tmp_path, monkeypatch):
    d = str(tmp_path)
    rows = {None: _rows(None, 103, with_agg=True)}
    _install(monkeypatch, rows, titles_ok=False)
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    _install(monkeypatch, rows, titles_ok=True)
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    s = _state(d)
    assert s["obs"] == 103 and s["spans"] == 1, s


def test_a_partition_boundary_does_not_checkpoint_rows_that_are_still_in_memory(tmp_path, monkeypatch):
    """R626: parts/written describe DISK. Declaring a partition done while its rows are
    buffered loses them to anything that drops the buffer - the rewind, or a guard kill."""
    d = str(tmp_path)
    rows = {"P1": _rows("P1", 30), "P2": _rows("P2", 30), "P3": _rows("P3", 30, with_agg=True)}
    _install(monkeypatch, rows, titles_ok=False, parts=["P1", "P2", "P3"])
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    s = _state(d)
    assert s["ckpt"]["written"] == 60 and s["ckpt"]["parts"] == 2, s["ckpt"]
    assert s["ckpt"]["part_val"] == "P3", s["ckpt"]
    _install(monkeypatch, rows, titles_ok=True, parts=["P1", "P2", "P3"])
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    s = _state(d)
    assert s["obs"] == 90 and s["spans"] == 1, s


def test_the_checkpointed_partition_VALUE_beats_a_moved_index(tmp_path, monkeypatch):
    """R626: pidx indexes a list re-fetched every pass; one insertion shifts every later
    index, skipping a partition or re-reading it."""
    d = str(tmp_path)
    rows = {"P1": _rows("P1", 30), "P2": _rows("P2", 30), "P3": _rows("P3", 30, with_agg=True)}
    _install(monkeypatch, rows, titles_ok=False, parts=["P1", "P2", "P3"])
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    rows_moved = dict(rows)
    rows_moved["P0"] = []
    _install(monkeypatch, rows_moved, titles_ok=True, parts=["P0", "P1", "P2", "P3"])
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    s = _state(d)
    assert s["obs"] == 90 and s["spans"] == 1, s


def test_a_partition_whose_length_is_an_exact_multiple_of_PAGE_also_flushes(tmp_path, monkeypatch):
    """R627: a partition ends on an EMPTY page whenever its row count divides by PAGE, and that
    branch had no flush and wrote no part_val. Rows per period is the product of the dimension
    cardinalities, so a table with 500 regions x 20 categories hits this in EVERY partition."""
    d = str(tmp_path)
    rows = {"P1": _rows("P1", 10), "P2": _rows("P2", 5, with_agg=True)}
    _install(monkeypatch, rows, titles_ok=False, page=10, parts=["P1", "P2"])
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    s = _state(d)
    assert s["ckpt"]["written"] == 10 and s["ckpt"]["parts"] == 1, s["ckpt"]
    assert s["ckpt"]["part_val"] == "P2", s["ckpt"]
    _install(monkeypatch, rows, titles_ok=True, page=10, parts=["P1", "P2"])
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    s = _state(d)
    assert s["obs"] == 15 and s["spans"] == 1, s


def _five_hundred_after(monkeypatch, pages_ok):
    """A get_json wrapper that serves `pages_ok` data pages and then answers 500."""
    real = mod.get_json
    state = {"n": 0}

    def failing(url):
        if "TypedDataSet" in url and "$skip" in url:
            state["n"] += 1
            if state["n"] > pages_ok:
                mod.LAST_ERROR.clear()
                mod.LAST_ERROR.update(status=500, body="Fout bij het lezen", url=url)
                return None
        return real(url)

    monkeypatch.setattr(mod, "get_json", failing)


def test_orphan_parts_with_no_checkpoint_are_cleared_by_the_500_path(tmp_path, monkeypatch):
    """R640, and narrower than it first looked. Every part this crawl writes comes with a
    checkpoint - the in-loop flush and finish_partition both write one - so the only parts this
    branch can meet WITHOUT a checkpoint are orphans left by an earlier run. That is the
    285-across-120-tables population, and they are what pollutes the `<table>.part*.parquet`
    glob in state_recently_touched."""
    import pyarrow as pa
    d = str(tmp_path)
    for i in range(2):
        pq.write_table(pa.table({"series_key": ["k"], "obs_date": pa.array(
            [__import__("datetime").date(2020, 1, 1)], pa.date32()), "value": [1.0]}),
            os.path.join(d, f"T.part{i}.parquet"))
    assert not os.path.exists(os.path.join(d, "T.ckpt.json"))
    rows = {None: _rows(None, 120)}
    _install(monkeypatch, rows, titles_ok=True, page=20)
    _five_hundred_after(monkeypatch, pages_ok=0)
    mod.ingest_table("T", "t", d, "2027-01-01T00:00:00")
    left = sorted(f for f in os.listdir(d) if f.startswith("T.part"))
    assert "T" in mod.load_broken(d), "the 500 was not recorded"
    assert not left, f"orphan parts were left behind: {left}"


def test_a_RESUMED_crawls_parts_survive_a_500(tmp_path, monkeypatch):
    """R640: an existing checkpoint ADDRESSES those parts, and deleting them destroys
    accumulated work - the live store's one real checkpoint is 85477NED with parts=70 and
    35,518,400 observations, and the largest table is the one CBS is most likely to 500 on.

    NO served copy here, which is both the realistic shape and the necessary one: a table WITH
    a served copy takes the re-pull path, and a re-pull deliberately clears partials before it
    starts, so the parts would be gone before the 500 ever arrived. 85477NED is exactly this
    shape - a long first crawl, interrupted, resuming."""
    import pyarrow as pa
    d = str(tmp_path)
    for i in range(2):
        pq.write_table(pa.table({"series_key": ["k"] * 30,
                                 "obs_date": pa.array([__import__("datetime").date(2020, 1, 1)] * 30,
                                                      pa.date32()),
                                 "value": [1.0] * 30}), os.path.join(d, f"T.part{i}.parquet"))
    with open(os.path.join(d, "T.ckpt.json"), "w") as fh:
        json.dump({"skip": 50, "parts": 2, "written": 60, "pidx": 0, "part_val": None}, fh)
    rows = {None: _rows(None, 120)}
    _install(monkeypatch, rows, titles_ok=True, page=20)
    _five_hundred_after(monkeypatch, pages_ok=0)          # 500 on the very first page
    mod.ingest_table("T", "t", d, "2027-01-01T00:00:00")
    left = sorted(f for f in os.listdir(d) if f.startswith("T.part"))
    assert left == ["T.part0.parquet", "T.part1.parquet"], f"the resumed parts were destroyed: {left}"
    assert os.path.exists(os.path.join(d, "T.ckpt.json")), "the checkpoint that addresses them was deleted"


def test_the_boundary_checkpoint_carries_the_partition_value_with_no_failure_at_all(tmp_path, monkeypatch):
    """R635: once the fetch_error path also wrote part_val, reverting it from
    finish_partition's checkpoint was caught by NOTHING - the title-failure exit supplied the
    value the other assertions read. But finish_partition's is the checkpoint that survives a
    GUARD KILL, where no fetch_error path runs at all, which is the R626 scenario. So this
    reads the checkpoint mid-crawl, with every fetch succeeding."""
    d = str(tmp_path)
    rows = {"P1": _rows("P1", 30), "P2": _rows("P2", 30), "P3": _rows("P3", 30)}
    _install(monkeypatch, rows, titles_ok=True, parts=["P1", "P2", "P3"])
    seen = {}
    real_write = mod.pq.write_table

    def watch(url):
        # after the second partition's boundary, capture whatever the checkpoint says
        ck = os.path.join(d, "T.ckpt.json")
        if os.path.exists(ck) and "second" not in seen:
            with open(ck, encoding="utf-8") as fh:
                c = json.load(fh)
            if c.get("pidx") == 1:
                seen["second"] = c
        return None

    orig = mod.get_json

    def get_json(url):
        watch(url)
        return orig(url)

    monkeypatch.setattr(mod, "get_json", get_json)
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    assert "second" in seen, "never observed a mid-crawl boundary checkpoint"
    assert seen["second"].get("part_val") == "P2", seen["second"]
    assert seen["second"].get("parts") == 1 and seen["second"].get("written") == 30, seen["second"]


def test_a_500_after_a_flush_does_not_truncate_the_served_table(tmp_path, monkeypatch):
    """R635, and it was LIVE. The interrupted-crawl guard sat inside `if parts == 0:`, so it
    protected only a crawl that had written nothing. A CBS 500 arriving after one flush fell
    through to the assembly, replaced the served table with the fraction crawled before the
    fault, advanced the stamp, and - because a successful crawl clears the broken registry -
    erased the record that anything had gone wrong."""
    d = str(tmp_path)
    served = os.path.join(d, "T.parquet")
    import pyarrow as pa
    pq.write_table(pa.table({"series_key": ["k"] * 60,
                             "obs_date": pa.array([__import__("datetime").date(2020, 1, 1)] * 60,
                                                  pa.date32()),
                             "value": [1.0] * 60}), served)
    rows = {None: _rows(None, 60)}
    calls = _install(monkeypatch, rows, titles_ok=True, page=25)
    real_get = mod.get_json
    state = {"pages": 0}

    def failing(url):
        if "TypedDataSet" in url and "$skip" in url:
            state["pages"] += 1
            if state["pages"] > 2:                      # two pages land, then CBS breaks
                mod.LAST_ERROR.clear()
                mod.LAST_ERROR.update(status=500, body="Fout bij het lezen", url=url)
                return None
        return real_get(url)

    monkeypatch.setattr(mod, "get_json", failing)
    monkeypatch.setattr(mod, "record_modified", lambda *a, **k: None)
    # A stamp LATER than the served file's mtime, or the vintage gate skips the table and the
    # crawl never runs - which would make this test pass by doing nothing.
    mod.ingest_table("T", "t", d, "2027-01-01T00:00:00")
    kept = pq.read_table(served).num_rows
    assert kept == 60, f"the served table was truncated to {kept} rows"
    assert "T" in mod.load_broken(d), "the 500 was not recorded as upstream-broken"
    # R636: the broken path writes no checkpoint, so anything it flushed is addressed by
    # nothing - and a leftover part feeds the glob in state_recently_touched, making the table
    # look in-flight for STALE_STATE_HOURS.
    orphans = [f for f in os.listdir(d) if f.startswith("T.part") and f.endswith(".parquet")]
    assert not orphans, f"the broken path left {orphans} behind"


def test_the_fifth_consecutive_title_failure_registers_the_table_as_broken(tmp_path, monkeypatch):
    """FIVE, as a literal. Taking the bound from TITLE_FAIL_LIMIT made this assert only that
    escalation happens EVENTUALLY: raising the constant to 999,999 does not fail the test, it
    hangs it (R633). The threshold is part of the behaviour, so the test states it."""
    assert mod.TITLE_FAIL_LIMIT == 5, "the threshold changed; decide it deliberately"
    d = str(tmp_path)
    rows = {None: _rows(None, 40, with_agg=True)}
    for i in range(4):
        _install(monkeypatch, rows, titles_ok=False)
        mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
        assert "T" not in mod.load_broken(d), f"escalated after only {i + 1} failure(s)"
    _install(monkeypatch, rows, titles_ok=False)
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    assert "T" in mod.load_broken(d), mod.load_broken(d)


def test_a_fetch_error_checkpoint_carries_the_partition_value(tmp_path, monkeypatch):
    """R633: the fetch_error path wrote its checkpoint only when the buffer was non-empty, and
    without part_val - so the resume-by-value protection was absent on the path that produces
    most checkpoints, and the rewound offset was never persisted at all.

    The failure is in the FIRST partition on purpose: with the aggregate in a later one, the
    boundary checkpoint would already carry part_val and this would pass on the old code too.
    My first version of this test did exactly that and I caught it with a revert check."""
    d = str(tmp_path)
    rows = {"P1": _rows("P1", 30, with_agg=True), "P2": _rows("P2", 30)}
    _install(monkeypatch, rows, titles_ok=False, parts=["P1", "P2"])
    mod.ingest_table("T", "t", d, "2026-09-02T00:00:00")
    s = _state(d)
    assert s["ckpt"] is not None, "the fetch_error path wrote no checkpoint at all"
    assert s["ckpt"].get("part_val") == "P1", s["ckpt"]
    assert s["ckpt"].get("skip") == 0, s["ckpt"]        # rewound to the last committed offset


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
