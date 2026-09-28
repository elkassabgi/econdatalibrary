"""tests/_bounded_reads.py is the one judge of "a bounded read of the live build" for four trace tests - so it is
tested itself, with planted positives in every shape (receipts must be able to fail) and the real shapes the tools
send as negatives."""
import pytest

import _bounded_reads as B


@pytest.mark.parametrize("q", [
    "SELECT source_id, count(*) FROM series GROUP BY 1",
    "SELECT COUNT(*) FROM series WHERE source_id='zz'",
    "SELECT series_id FROM series WHERE source_id = 'zz' ORDER BY RANDOM() LIMIT 4",
    "SELECT source_id FROM series",
    "SELECT source_id FROM series WHERE series_id >= '' OR series_id IS NULL",          # round 5 N6
    "SELECT series_id, source_id FROM series WHERE series_id > '' ORDER BY series_id LIMIT 1000000000000",  # N4
    "SELECT series_id, source_id FROM series WHERE series_id > '' ORDER BY series_id LIMIT 0",
    "SELECT series_id FROM series WHERE series_id >= '' AND series_id < '~'",           # an empty lower bound
    "SELECT series_id FROM series WHERE series_id IS NULL",
    "SELECT series_id FROM series WHERE series_id IS NULL OR series_id = '' OR 1=1",
    "SELECT series_id FROM series ORDER BY RANDOM() LIMIT 4",
])
def test_an_unbounded_read_is_caught(q):
    assert B.unbounded(q), q


@pytest.mark.parametrize("q", [
    "SELECT series_id, source_id FROM series WHERE series_id IS NULL OR series_id = ''",
    "SELECT series_id, source_id FROM series WHERE series_id > '' ORDER BY series_id LIMIT 200000",
    "SELECT source_id FROM series WHERE series_id > 'zz:b' ORDER BY series_id LIMIT 1000000",
    "SELECT COUNT(*) FROM series WHERE series_id >= 'zz:' AND series_id < 'zz;'",
    "SELECT series_id FROM series WHERE series_id >= 'zz:' AND series_id < 'zz;' ORDER BY RANDOM() LIMIT 4",
    "SELECT series_id FROM series WHERE series_id > 'o''neil' ORDER BY series_id LIMIT 5",
])
def test_the_bounded_shapes_the_tools_send_pass(q):
    assert not B.unbounded(q), q
