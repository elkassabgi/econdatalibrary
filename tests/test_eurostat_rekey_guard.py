"""The eurostat re-key guard must not be disarmable by a PARTIAL migration.

THE HOLE IT HAD: `_require_rekeyed` spot-checked `blob.list_parquets(out_dir)[:5]` — the first
five of a SORTED list (blob returns sorted in both the R2 and local branches), and
tools/rekey_eurostat.py walks that identical sorted list. So an --apply that died partway had
already converted exactly those five, and the guard released at 0.06% of 7,754 files. The next
daily tick would then merge stable-key fetches into ~3,300 still-unstable files under two key
schemes — the duplication never-shrink cannot catch, which is the whole reason the guard exists.

That interrupt is OBSERVED, not hypothetical: rekey_eurostat.py's own comment records a pass
dying at file 4,403 of 7,754 after ~4 hours on a transient R2 read.

Now two independent checks: a completion marker the migration writes only after a full clean
pass, and a content sample taken at EVENLY SPACED indices so it is uncorrelated with the
migration's walk order.
"""
import json
import os

import pyarrow as pa
import pytest

from updater.errors import DefinitiveError
from updater.strategies.fetchers import eurostat as E


UNSTABLE = "LAST UPDATE=13/05/26 11:00:00:freq=A:unit=THS:geo=AT"
STABLE = "freq=A:unit=THS:geo=AT"


class FakeBlob:
    """Minimal stand-in: N sorted parquet names, a per-file key, and a marker blob."""

    def __init__(self, n, unstable_idx, marker=None):
        self.names = [f"F{i:05d}.parquet" for i in range(n)]
        self.unstable = set(unstable_idx)
        self.marker = marker
        self.reads = []

    def list_parquets(self, _d):
        return list(self.names)                     # already sorted, like the real one

    def read_bytes(self, path):
        return (json.dumps(self.marker).encode() if self.marker is not None
                and path.endswith(E.REKEY_MARKER) else None)

    def read_table(self, path, columns=None):
        name = os.path.basename(path)
        i = self.names.index(name)
        self.reads.append(i)
        key = UNSTABLE if i in self.unstable else STABLE
        return pa.table({"series_key": pa.array([key], pa.string())})


@pytest.fixture
def patched(monkeypatch):
    def _apply(fb):
        monkeypatch.setattr(E.blob, "list_parquets", fb.list_parquets)
        monkeypatch.setattr(E.blob, "read_bytes", fb.read_bytes)
        monkeypatch.setattr(E.blob, "read_table", fb.read_table)
        return fb
    return _apply


def test_no_marker_means_the_guard_fires_even_if_every_file_is_clean():
    """The marker is the primary check: 'looks converted' is not 'was converted'."""
    fb = FakeBlob(7754, unstable_idx=[])
    with pytest.raises(DefinitiveError, match="has not completed"):
        _guard(fb)


def test_THE_REGRESSION_a_partial_migration_cannot_disarm_it(patched):
    """Exactly the old hole: the first five converted, the other 7,749 not, no marker."""
    fb = patched(FakeBlob(7754, unstable_idx=range(5, 7754)))
    with pytest.raises(DefinitiveError):
        E._require_rekeyed()


def test_a_marker_whose_count_is_stale_does_not_vouch_for_a_grown_store(patched):
    """A marker from an older, smaller store must not cover files added since."""
    fb = patched(FakeBlob(7754, unstable_idx=[], marker={"files_seen": 7000}))
    with pytest.raises(DefinitiveError, match="7754"):
        E._require_rekeyed()


def test_marker_plus_clean_data_passes(patched):
    fb = patched(FakeBlob(7754, unstable_idx=[], marker={"files_seen": 7754}))
    E._require_rekeyed()                              # must not raise


def test_a_LYING_marker_is_caught_by_the_content_sample(patched):
    """Belt and braces: the marker says done, the data says otherwise."""
    fb = patched(FakeBlob(7754, unstable_idx=[7753], marker={"files_seen": 7754}))
    with pytest.raises(DefinitiveError, match="marker does not match"):
        E._require_rekeyed()


def test_the_sample_is_NOT_a_head_prefix(patched):
    """The ordering fix itself: a run that stopped ~57% through is caught by a later sample,
    which a [:5] head slice never would be.

    Note the guard stops at the FIRST offender, so it does not read every sampled index — the
    property under test is that it looks well past the head, not that it reads all five.
    """
    n = 7754
    stopped_at = int(n * 0.57)
    fb = patched(FakeBlob(n, unstable_idx=range(stopped_at, n), marker={"files_seen": n}))
    with pytest.raises(DefinitiveError):
        E._require_rekeyed()
    assert 0 in fb.reads, "still checks the head"
    assert max(fb.reads) > 5, "and reaches far past it — a [:5] slice would have missed this"
    assert max(fb.reads) >= stopped_at, "it found an index inside the unconverted tail"


def test_a_fully_clean_store_samples_the_LAST_file_too(patched):
    """When nothing raises, every sampled index is visited — including n-1, so a migration
    that stopped one file short is still caught."""
    n = 7754
    fb = patched(FakeBlob(n, unstable_idx=[], marker={"files_seen": n}))
    E._require_rekeyed()
    assert 0 in fb.reads and (n - 1) in fb.reads
    assert len(fb.reads) == 5, "head, quarter, half, three-quarter, last"


def test_an_empty_store_is_not_gated(patched):
    """Nothing stored yet means nothing to protect; the guard must not block a first run."""
    patched(FakeBlob(0, unstable_idx=[]))
    E._require_rekeyed()


def _guard(fb):
    import unittest.mock as mock
    with mock.patch.object(E.blob, "list_parquets", fb.list_parquets), \
         mock.patch.object(E.blob, "read_bytes", fb.read_bytes), \
         mock.patch.object(E.blob, "read_table", fb.read_table):
        E._require_rekeyed()


# ---- the guard must not lock itself when the fetcher lands a NEW flow (2026-09-23, R1141) --------
class GrowBlob(FakeBlob):
    """FakeBlob that can grow: new files appear during 'the run', and the marker can be written."""

    def __init__(self, n, marker, new_files=(), unstable_new=()):
        super().__init__(n, unstable_idx=[], marker=marker)
        self.pending = list(new_files)
        self.unstable_new = set(unstable_new)
        self.writes = []

    def land(self):
        self.names = sorted(self.names + self.pending)

    def read_table(self, path, columns=None):
        name = os.path.basename(path)
        if name in self.pending:
            key = UNSTABLE if name in self.unstable_new else STABLE
            return pa.table({"series_key": pa.array([STABLE, key], pa.string())})
        return super().read_table(path, columns)

    def write_bytes_atomic(self, path, data):
        self.writes.append((path, data))
        self.marker = json.loads(data.decode())


def _update(monkeypatch, gb, land=True):
    monkeypatch.setattr(E.blob, "list_parquets", gb.list_parquets)
    monkeypatch.setattr(E.blob, "read_bytes", gb.read_bytes)
    monkeypatch.setattr(E.blob, "read_table", gb.read_table)
    monkeypatch.setattr(E.blob, "write_bytes_atomic", gb.write_bytes_atomic)
    monkeypatch.setattr(E, "_run", lambda unit: (gb.land() if land else None) or "ran")
    return E.update(None, None)


def test_a_run_that_lands_a_new_flow_grows_the_marker_so_the_next_run_is_admitted(monkeypatch):
    gb = GrowBlob(8, {"files_seen": 8}, new_files=["NEW_A.parquet", "NEW_B.parquet"])
    assert _update(monkeypatch, gb) == "ran"
    assert gb.marker["files_seen"] == 10 and gb.marker["grown"][0]["files"] == ["NEW_A.parquet", "NEW_B.parquet"]
    gb.pending = []
    assert _update(monkeypatch, gb, land=False) == "ran", "the next run passes the guard"


def test_negative_control_without_the_growth_the_next_run_is_refused(monkeypatch):
    gb = GrowBlob(8, {"files_seen": 8}, new_files=["NEW_A.parquet"])
    monkeypatch.setattr(E, "_grow_marker", lambda before: None)
    _update(monkeypatch, gb)
    gb.pending = []
    with pytest.raises(DefinitiveError, match="has not completed"):
        _update(monkeypatch, gb, land=False)


def test_a_new_file_with_unstable_keys_does_not_grow_the_marker(monkeypatch):
    gb = GrowBlob(8, {"files_seen": 8}, new_files=["NEW_A.parquet"], unstable_new=["NEW_A.parquet"])
    _update(monkeypatch, gb)
    assert gb.writes == [] and gb.marker["files_seen"] == 8


def test_a_marker_changed_during_the_run_is_left_alone(monkeypatch):
    gb = GrowBlob(8, {"files_seen": 8}, new_files=["NEW_A.parquet"])

    def _run(unit):
        gb.land()
        gb.marker = {"files_seen": 9, "by": "rekey tool"}
        return "ran"
    monkeypatch.setattr(E.blob, "list_parquets", gb.list_parquets)
    monkeypatch.setattr(E.blob, "read_bytes", gb.read_bytes)
    monkeypatch.setattr(E.blob, "read_table", gb.read_table)
    monkeypatch.setattr(E.blob, "write_bytes_atomic", gb.write_bytes_atomic)
    monkeypatch.setattr(E, "_run", _run)
    E.update(None, None)
    assert gb.writes == [] and gb.marker == {"files_seen": 9, "by": "rekey tool"}


def test_the_marker_grows_even_when_the_run_raises(monkeypatch):
    gb = GrowBlob(8, {"files_seen": 8}, new_files=["NEW_A.parquet"])

    def _run(unit):
        gb.land()
        raise RuntimeError("killed mid-sweep")
    monkeypatch.setattr(E.blob, "list_parquets", gb.list_parquets)
    monkeypatch.setattr(E.blob, "read_bytes", gb.read_bytes)
    monkeypatch.setattr(E.blob, "read_table", gb.read_table)
    monkeypatch.setattr(E.blob, "write_bytes_atomic", gb.write_bytes_atomic)
    monkeypatch.setattr(E, "_run", _run)
    with pytest.raises(RuntimeError):
        E.update(None, None)
    assert gb.marker["files_seen"] == 9


def _wire(monkeypatch, gb, run):
    for name in ("list_parquets", "read_bytes", "read_table", "write_bytes_atomic"):
        monkeypatch.setattr(E.blob, name, getattr(gb, name))
    monkeypatch.setattr(E, "_run", run)


def test_a_failure_inside_the_growth_never_replaces_the_runs_result(monkeypatch, capsys):
    """R1145: `except Exception` -> `except ValueError` survived the first tests."""
    gb = GrowBlob(8, {"files_seen": 8}, new_files=["NEW_A.parquet"])
    monkeypatch.setattr(gb, "read_table", lambda path, columns=None: (_ for _ in ()).throw(OSError("R2 read")))
    _wire(monkeypatch, gb, lambda unit: gb.land() or "ran")
    assert E.update(None, None) == "ran" and gb.writes == []
    assert "re-key marker NOT grown (OSError" in capsys.readouterr().out


def test_a_failure_inside_the_growth_never_replaces_the_runs_own_exception(monkeypatch):
    gb = GrowBlob(8, {"files_seen": 8}, new_files=["NEW_A.parquet"])

    def _run(unit):
        gb.land()
        raise KeyError("the run's own failure")
    monkeypatch.setattr(gb, "read_table", lambda path, columns=None: (_ for _ in ()).throw(OSError("R2 read")))
    _wire(monkeypatch, gb, _run)
    with pytest.raises(KeyError, match="the run's own failure"):
        E.update(None, None)


def test_the_unit_alarm_inside_the_growth_is_named_and_the_result_kept(monkeypatch, capsys):
    class UnitTimeout(Exception):
        pass
    gb = GrowBlob(8, {"files_seen": 8}, new_files=["NEW_A.parquet"])
    monkeypatch.setattr(gb, "read_table", lambda path, columns=None: (_ for _ in ()).throw(UnitTimeout("alarm")))
    _wire(monkeypatch, gb, lambda unit: gb.land() or "ran")
    assert E.update(None, None) == "ran"
    assert "INTERRUPTED by the unit alarm" in capsys.readouterr().out


def test_a_vanished_file_blocks_the_growth(monkeypatch):
    gb = GrowBlob(8, {"files_seen": 8}, new_files=["NEW_A.parquet"])

    def _run(unit):
        gb.land()
        gb.names.remove("F00003.parquet")
        return "ran"
    _wire(monkeypatch, gb, _run)
    E.update(None, None)
    assert gb.writes == []


def test_the_stable_check_is_exact_over_every_distinct_key(monkeypatch):
    rows = ["freq=A:geo=AT"] * 100_000 + ["LAST UPDATE=1:freq=A:geo=AT"]
    monkeypatch.setattr(E.blob, "read_table", lambda p, columns=None: pa.table({"series_key": pa.array(rows)}))
    assert E._stable_file("d", "x.parquet") is False
    monkeypatch.setattr(E.blob, "read_table", lambda p, columns=None: pa.table({"series_key": pa.array(rows[:-1])}))
    assert E._stable_file("d", "x.parquet") is True


def test_the_one_time_tool_checks_the_count_and_every_key(monkeypatch, tmp_path):
    import importlib
    sys_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools")
    import sys
    sys.path.insert(0, sys_path)
    tool = importlib.import_module("grow_eurostat_rekey_marker")
    gb = GrowBlob(8, {"files_seen": 8}, new_files=["NEW_A.parquet", "NEW_B.parquet"])
    gb.land()
    for name in ("list_parquets", "read_bytes", "read_table", "write_bytes_atomic"):
        monkeypatch.setattr(E.blob, name, getattr(gb, name))
        monkeypatch.setattr(tool.blob, name, getattr(gb, name))
    assert tool.main(["--files", "NEW_A.parquet"]) == 2, "count does not close: refused"
    assert tool.main(["--files", "NEW_A.parquet,NEW_B.parquet"]) == 0 and gb.writes == [], "dry run"
    assert tool.main(["--files", "NEW_A.parquet,NEW_B.parquet", "--apply"]) == 0
    assert gb.marker["files_seen"] == 10 and gb.marker["grown"][-1]["by"].startswith("tools/")
    # "F00000a" sorts to index 1, which the guard's evenly spaced sample (0, 2, 4, 6, 8) never reads
    gb2 = GrowBlob(8, {"files_seen": 8}, new_files=["F00000a.parquet"], unstable_new=["F00000a.parquet"])
    gb2.land()
    for name in ("list_parquets", "read_bytes", "read_table", "write_bytes_atomic"):
        monkeypatch.setattr(E.blob, name, getattr(gb2, name))
        monkeypatch.setattr(tool.blob, name, getattr(gb2, name))
    assert tool.main(["--files", "F00000a.parquet", "--apply"]) == 2 and gb2.writes == []
