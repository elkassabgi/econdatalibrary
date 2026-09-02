"""A source with a live half reads as both halves; one without is untouched.

freeze-and-forward: the frozen half is the store exactly as it stood at the cut and is never
rewritten again, and the live half accumulates in front of it at `<src>/_live/<same name>`.

The rule lives in `resolve()` and nowhere else. There are more than sixty `_resolve_*`
functions, each building its own path, and a rule that lives in sixty places has already begun
to drift by the time anyone checks (R469) - which is why `resolve()`'s own comment says it
applies cross-cutting policy centrally.

The property that matters most here is the NEGATIVE one: a source with no `_live` directory
must keep the exact `str` it has today. Every consumer downstream has only ever been handed a
string, and a partition that silently changed that shape for all 322 sources would be a much
larger change than the one being made.
"""
from __future__ import annotations

import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "clients", "python"))

from econdl._resolve import LIVE_DIR, live_sibling, with_live_half  # noqa: E402


def _write(p, rows=1):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    pq.write_table(pa.table({"n": list(range(rows))}), p)
    return p


def test_no_live_directory_leaves_the_path_a_STRING(tmp_path):
    """The negative case, and the important one: 322 sources have no partition and every
    consumer downstream has only ever been handed a str."""
    p = _write(str(tmp_path / "src" / "table.parquet"))
    assert live_sibling(p) is None
    got = with_live_half(p)
    assert got == p and isinstance(got, str), got


def test_an_existing_live_half_is_appended(tmp_path):
    p = _write(str(tmp_path / "src" / "table.parquet"))
    live = _write(str(tmp_path / "src" / LIVE_DIR / "table.parquet"))
    assert live_sibling(p) == live
    assert with_live_half(p) == [p, live]


def test_a_live_half_for_a_DIFFERENT_table_is_not_picked_up(tmp_path):
    """The sibling is matched by filename, so a live half of another table must not attach."""
    p = _write(str(tmp_path / "src" / "table.parquet"))
    _write(str(tmp_path / "src" / LIVE_DIR / "other.parquet"))
    assert live_sibling(p) is None
    assert with_live_half(p) == p


def test_the_live_half_does_not_recurse(tmp_path):
    """A path already inside `_live` has no live half of its own, or resolve() would append
    `_live/_live/...` and the union would silently double-count on the second pass.

    THE NESTED FILE IS WRITTEN ON PURPOSE. Without it `os.path.exists` answers False anyway
    and the guard is never the reason this passes - which is how the first version of this
    test survived deleting the guard entirely, caught by my own mutation sweep (R632)."""
    live = _write(str(tmp_path / "src" / LIVE_DIR / "table.parquet"))
    nested = _write(str(tmp_path / "src" / LIVE_DIR / LIVE_DIR / "table.parquet"))
    assert os.path.exists(nested), "the trap this test needs was not created"
    assert live_sibling(live) is None, "the live half attached a live half of its own"
    assert with_live_half(live) == live


def test_a_list_gets_each_of_its_own_live_halves(tmp_path):
    a = _write(str(tmp_path / "src" / "a.parquet"))
    b = _write(str(tmp_path / "src" / "b.parquet"))
    a_live = _write(str(tmp_path / "src" / LIVE_DIR / "a.parquet"))
    got = with_live_half([a, b])
    assert got == [a, a_live, b], got


def test_a_directory_path_is_left_alone(tmp_path):
    """Some resolvers return a directory, which pyarrow reads as a dataset. A directory has no
    single sibling file, and inventing one would point at nothing."""
    d = str(tmp_path / "src")
    os.makedirs(d, exist_ok=True)
    assert live_sibling(d) is None
    assert with_live_half(d) == d


def test_a_non_parquet_path_is_left_alone():
    assert live_sibling("/x/y.csv") is None
    assert live_sibling(None) is None
    assert with_live_half(None) is None


def test_resolve_applies_it_centrally(tmp_path, monkeypatch):
    """A hook the entry point does not call is not a hook. Sixty resolvers each build their own
    path; exactly one place may add the live half.

    THIS CALLS resolve(), it does not read its source. The first version asserted that
    `inspect.getsource` contained the call, so commenting the line out left the string in the
    source and the mutation survived - the exact weakness the sibling suite records in its own
    header, "passes even if the print it is looking for is unreachable"."""
    from econdl import _catalog
    from econdl import _resolve as m

    p = _write(str(tmp_path / "probe" / "t.parquet"))
    live = _write(str(tmp_path / "probe" / LIVE_DIR / "t.parquet"))

    def fake(series_id, root):
        return m.Resolution(series_id=series_id, source="probe", parquet_path=p,
                            key_col="n", predicate=None)

    monkeypatch.setattr(_catalog, "source_of", lambda _sid: "probe")
    monkeypatch.setitem(m._RESOLVERS, "probe", fake)
    got = m.resolve("probe:t", root=str(tmp_path))
    assert got.parquet_path == [p, live], got.parquet_path


def test_the_two_halves_read_as_one_series(tmp_path):
    """The assumption the layout rests on, end to end through pyarrow's dataset reader."""
    import pyarrow.compute as pc
    import pyarrow.dataset as pads

    frozen = pa.table({"series_key": ["k"] * 3, "obs_date": ["2024-01-01", "2024-02-01",
                                                             "2024-03-01"],
                       "value": [1.0, 2.0, 3.0]})
    live = pa.table({"series_key": ["k"] * 2, "obs_date": ["2026-09-01", "2026-10-01"],
                     "value": [4.0, 5.0]})
    p = str(tmp_path / "src" / "t.parquet")
    lp = str(tmp_path / "src" / LIVE_DIR / "t.parquet")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    os.makedirs(os.path.dirname(lp), exist_ok=True)
    pq.write_table(frozen, p)
    pq.write_table(live, lp)
    paths = with_live_half(p)
    assert isinstance(paths, list) and len(paths) == 2
    t = pads.dataset(paths).to_table(filter=pc.field("series_key") == "k").sort_by(
        [("obs_date", "ascending")])
    assert t.num_rows == 5, t.num_rows
    assert t.column("obs_date").to_pylist()[-1] == "2026-10-01"
    assert t.column("value").to_pylist() == [1.0, 2.0, 3.0, 4.0, 5.0]


def test_the_cataloguer_does_not_see_the_live_half_as_a_new_table(tmp_path, monkeypatch):
    """The live half carries the SAME native id as its frozen sibling, so walking into it
    catalogues every partitioned table twice. `series` is INSERT OR IGNORE and `series_fts`
    has no unique constraint, which is the exact mismatch that put 8.00 copies of every boc
    series in the live index."""
    import tools.catalog_table_grain as ctg
    src = tmp_path / "clean_full" / "probe"
    (src / LIVE_DIR).mkdir(parents=True)
    _write(str(src / "t1.parquet"))
    _write(str(src / "t2.parquet"))
    _write(str(src / LIVE_DIR / "t1.parquet"))          # same id, live half
    monkeypatch.setattr(ctg, "STORE", str(tmp_path / "clean_full"))
    got = [t for t, _p in ctg._tables("probe")]
    assert got == ["t1", "t2"], got
    assert not any("_live" in p.replace(os.sep, "/") for _t, p in ctg._tables("probe")),         "a live-half file reached the cataloguer's table list"


def test_the_uploader_DOES_see_the_live_half():
    """The mirror must carry both halves or the served copy loses everything after the cut.
    The opposite of the rule above, and a single set shared between the two tools would have
    made one of them wrong."""
    import core.upload_r2 as up
    assert not up._is_intermediate("src/_live/t1.parquet"), \
        "the live half is excluded from the upload; the mirror would lose it"
    assert up._is_intermediate("src/parts/t1.parquet")
    assert up._is_intermediate("src/_cache/t1.parquet")
