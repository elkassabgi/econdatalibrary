"""tools/repair_insee_spelling.py (AR-333): the plan on synthetic tables, every refusal, the read-only client,
and an apply killed between each step and then resumed, then rolled back - all against a fake store."""
from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import io
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))
import repair_insee_spelling as R  # noqa: E402

D = dt.date
OLD = [("F", "A=1:B=x", D(2024, 12, 31), 1.0), ("F", "A=1:B=x", D(2025, 12, 31), 2.0),
       ("F", "A=2:B=y", D(2025, 12, 31), 3.0),                         # old-only series: INSEE dropped it
       ("F", "A=3:B=z", D(2020, 12, 31), 4.0)]                         # a date the new pull lacks
NEW = [("F", "A=1:B=x:GEO=2025-FRANCE-F", D(2024, 12, 31), 1.0),
       ("F", "A=1:B=x:GEO=2025-FRANCE-F", D(2025, 12, 31), 2.5),       # a revised value
       ("F", "A=3:B=z:GEO=2025-FRANCE-F", D(2021, 12, 31), 5.0)]


def tbl(rows, flow=None):
    return pa.table({"flow": [flow or r[0] for r in rows], "series_key": [r[1] for r in rows],
                     "obs_date": pa.array([r[2] for r in rows], pa.date32()), "value": [r[3] for r in rows]})


def pbytes(t):
    b = io.BytesIO()
    pq.write_table(t, b)
    return b.getvalue()


# ------------------------------------------------------------------ the plan
def test_supersede_drops_only_exact_counterparts_and_keeps_the_rest():
    pub, rep = R.plan_supersede(tbl(OLD + NEW), tbl(OLD))
    got = sorted((r["series_key"], r["obs_date"], r["value"]) for r in pub.to_pylist())
    assert got == sorted([("A=1:B=x:GEO=2025-FRANCE-F", D(2024, 12, 31), 1.0),
                          ("A=1:B=x:GEO=2025-FRANCE-F", D(2025, 12, 31), 2.5),
                          ("A=3:B=z:GEO=2025-FRANCE-F", D(2021, 12, 31), 5.0),
                          ("A=2:B=y", D(2025, 12, 31), 3.0), ("A=3:B=z", D(2020, 12, 31), 4.0)])
    assert rep["dropped_old_rows"] == 2 and rep["kept_old_rows_residue"] == 2 and rep["kept_old_series"] == 2
    assert rep["dropped_rows_whose_value_the_new_pull_revised"] == 1 and rep["residue"]["rows"] == 2


def test_revert_publishes_the_mirror():
    pub, rep = R.plan_revert(tbl(OLD + NEW), tbl(OLD))
    assert pub.num_rows == len(OLD) and rep["added_rows_removed"] == len(NEW)


@pytest.mark.parametrize("planner", [R.plan_supersede, R.plan_revert])
def test_a_served_value_that_differs_from_the_mirror_refuses(planner):
    served = [OLD[0], ("F", "A=1:B=x", D(2025, 12, 31), 2.000001)] + OLD[2:] + NEW
    with pytest.raises(R.Refused, match="differ from the mirror by value"):
        planner(tbl(served), tbl(OLD))


def test_a_mirror_row_missing_on_r2_refuses():
    with pytest.raises(R.Refused, match="not on R2"):
        R.plan_revert(tbl(OLD[1:] + NEW), tbl(OLD))


def test_served_not_mirror_plus_added_refuses():
    with pytest.raises(R.Refused, match="duplicate|mirror"):
        R.plan_revert(tbl(OLD + NEW + [NEW[0]]), tbl(OLD))


@pytest.mark.parametrize("extra_old,extra_new", [
    ([("F", "A=9:GEO=FR", D(2020, 12, 31), 9.0)], []),                 # an old key with a GEO
    ([], [("F", "A=7:B=q", D(2025, 12, 31), 7.0)]),                    # an added key without the GEO
    ([], [("F", "A=1:GEO=2025-FRANCE-F:B=x", D(2023, 12, 31), 0.5)]),  # two new keys on one stripped key
])
def test_an_inexact_spelling_rule_refuses(extra_old, extra_new):
    old, new = OLD + extra_old, NEW + extra_new
    with pytest.raises(R.Refused, match="not exact"):
        R.plan_supersede(tbl(old + new), tbl(old))


def test_the_planned_entry_never_touches_the_fetchers_repair_flag():
    fe = {"status": "legacy", "served": {"identity": {"etag": "x"}}, "modified": "m"}
    _, rep = R.plan_revert(tbl(OLD + NEW), tbl(OLD))
    e = R.planned_entry(fe, "F", rep, {"partitions": {"p": 4}, "s_norm": 4}, "md5", 9, "2025-12-31", 3, "2026-10-17")
    assert "repair" not in e and e["spelling_repair"]["planned_md5"] == "md5" and e["modified"] is None
    assert e["csv_owed"] == "2025-12-31" and e["s_norm"] == 4 and "reverted" in e["why"]


# ------------------------------------------------------------------ the read-only client
def test_the_plan_client_can_only_get_and_head():
    calls = []

    class C:
        def __getattr__(self, name):
            calls.append(name)
            raise AssertionError(name)
    s = R.ReadOnlyStore(C())
    assert not hasattr(s, "put_if") and not hasattr(s, "put_object")
    with pytest.raises(AssertionError):
        s.get("k")
    assert calls == ["get_object"]


# ------------------------------------------------------------------ plan + apply + rollback on a fake store
class FakeStore(R.WriteStore):
    def __init__(self):
        self.objs, self.puts = {}, []

    def set(self, key, data, meta=None):
        self.objs[key] = (data, meta or {})

    def get(self, key):
        if key not in self.objs:
            raise KeyError(key)
        d, m = self.objs[key]
        return d, R.md5(d), {"ContentType": m.get("ContentType"), "ContentEncoding": m.get("ContentEncoding"),
                             "Metadata": m.get("Metadata") or {}}

    def etag(self, key):
        return R.md5(self.objs[key][0]) if key in self.objs else None

    def put_if(self, key, data, if_match, meta=None):
        if self.etag(key) != if_match.strip('"'):
            return None
        self.objs[key] = (data, meta or {})
        self.puts.append(key)
        return R.md5(data)


def csv_body(n):
    return gzip.compress(("h\n" + "x\n" * n).encode(), mtime=0)


@pytest.fixture
def world(tmp_path, monkeypatch):
    flow = "DD_CNA_APU"
    st = FakeStore()
    served = pbytes(tbl(OLD + NEW, flow))
    st.set(R.pkey(flow), served)
    st.set(R.csv_key(flow), csv_body(len(OLD + NEW)), {"ContentEncoding": "gzip", "Metadata": {"csvmd5": "a"}})
    state = {"writer": R.RUN_WRITER, "written_utc": "2026-10-17T11:46:01+00:00", "flows": {
        flow: {"status": "complete_per_paging", "repair": True, "served": {"identity": {"etag": R.md5(served)},
                                                                            "rows": 7}}}}
    st.set(R.STATE_KEY, json.dumps(state).encode())
    mirror = tmp_path / "mirror"
    mirror.mkdir()
    pq.write_table(tbl(OLD, flow), str(mirror / f"{flow}.parquet"))
    monkeypatch.setattr(R, "_registry_live", lambda repo: False)
    monkeypatch.setattr(R, "planned_csv", lambda repo, root, f: {
        "data_lines": pq.read_metadata(os.path.join(root, "insee_melodi", f"{f}.parquet")).num_rows, "md5": "m"})

    def derive(repo, root, ids, *, dry):
        if dry:
            return "--only: selected 1 of 1\nDRY RUN: derived 1, unresolvable 0 (no R2 contact)"
        f = open(ids).read().strip().split(":", 1)[1]
        n = pq.read_metadata(os.path.join(root, "insee_melodi", f"{f}.parquet")).num_rows
        st.objs[R.csv_key(f)] = (csv_body(n), {"ContentEncoding": "gzip"})
        return "ok"
    work = tmp_path / "work"
    return {"flow": flow, "st": st, "mirror": str(mirror), "work": str(work), "derive": derive, "served": served}


def _plan(w):
    return R.plan(w["st"], w["work"], w["mirror"], "repo", [w["flow"]], today="2026-10-17", derive=w["derive"])


def test_plan_refuses_a_state_another_writer_wrote(world):
    raw = json.loads(world["st"].objs[R.STATE_KEY][0])
    raw["writer"] = "gh:999"
    world["st"].set(R.STATE_KEY, json.dumps(raw).encode())
    with pytest.raises(R.Refused, match="last written by 'gh:999'"):
        _plan(world)


def test_plan_refuses_an_object_the_state_does_not_name(world):
    world["st"].set(R.pkey(world["flow"]), world["served"] + b"")
    raw = json.loads(world["st"].objs[R.STATE_KEY][0])
    raw["flows"][world["flow"]]["served"]["identity"]["etag"] = "0" * 32
    world["st"].set(R.STATE_KEY, json.dumps(raw).encode())
    with pytest.raises(R.Refused, match="is not the one _flows.json records"):
        _plan(world)


@pytest.mark.parametrize("kill_at", ["parquet", "state", "csv"])
def test_an_apply_killed_after_any_step_resumes_and_ends_consistent(world, kill_at):
    rep = _plan(world)
    f = world["flow"]
    assert world["st"].puts == []                                       # the plan wrote nothing

    def hook(step, flow):
        if step == kill_at:
            raise KeyboardInterrupt(f"killed after {step}")
    with pytest.raises(KeyboardInterrupt):
        R.apply(world["st"], world["work"], "repo", [f], derive=world["derive"], hook=hook)
    out = R.apply(world["st"], world["work"], "repo", [f], derive=world["derive"])
    assert world["st"].puts.count(R.pkey(f)) == 1                       # the parquet was written ONCE
    state = json.loads(world["st"].objs[R.STATE_KEY][0])
    fe = state["flows"][f]
    assert state["writer"] == R.TOOL_WRITER and fe["repair"] is True    # the fetcher's flag untouched
    assert fe["spelling_repair"]["planned_md5"] == rep["flows"][f]["planned_md5"] and "csv_owed" not in fe
    assert fe["served"]["identity"]["etag"] == world["st"].etag(R.pkey(f)) and fe["served"]["rows"] == 5
    r = out["receipts"][f]
    assert r["parquet_rows"] == 5 and r["csv_data_lines"] == 5


def test_rollback_restores_parquet_state_entry_and_csv(world):
    _plan(world)
    f = world["flow"]
    csv0 = world["st"].objs[R.csv_key(f)]
    R.apply(world["st"], world["work"], "repo", [f], derive=world["derive"])
    R.rollback(world["st"], world["work"], [f])
    assert world["st"].objs[R.pkey(f)][0] == world["served"]
    assert world["st"].objs[R.csv_key(f)][0] == csv0[0]
    assert world["st"].objs[R.csv_key(f)][1].get("Metadata") == {"csvmd5": "a"}
    fe = json.loads(world["st"].objs[R.STATE_KEY][0])["flows"][f]
    assert fe["served"]["identity"]["etag"] == R.md5(world["served"]) and "spelling_repair" not in fe


def test_apply_refuses_an_object_that_is_neither_before_nor_after(world):
    _plan(world)
    f = world["flow"]
    world["st"].set(R.pkey(f), b"someone else's bytes")
    with pytest.raises(R.Refused, match="neither the plan's before nor its after"):
        R.apply(world["st"], world["work"], "repo", [f], derive=world["derive"])


def test_apply_refuses_unless_the_source_is_paused(tmp_path, monkeypatch):
    monkeypatch.setattr(R, "_registry_live", lambda repo: True)
    monkeypatch.setattr(R, "_r2", lambda write: pytest.fail("no client may be built"))
    import core.cutover as cut
    monkeypatch.setattr(cut, "is_cut_over", lambda: False)
    with pytest.raises(R.Refused, match="live in the registry"):
        R.main(["apply", "--work", str(tmp_path / "w"), "--catalog-repo", str(tmp_path)])


def test_the_tool_refuses_flows_outside_the_eight(tmp_path):
    with pytest.raises(R.Refused, match="DS_ANTIPOL is untouched"):
        R.main(["plan", "--work", str(tmp_path), "--mirror", str(tmp_path), "--catalog-repo", str(tmp_path),
                "--flows", "DS_ANTIPOL"])
