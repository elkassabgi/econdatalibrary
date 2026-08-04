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
