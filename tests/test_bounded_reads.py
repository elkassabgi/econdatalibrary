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
    # review round 6 (R1267) survivors - each passed the suffix check
    "SELECT source_id FROM series WHERE series_id >= ' ' AND series_id < '\U0010ffff'",                  # M4/M6
    "SELECT source_id FROM series WHERE series_id > 'a' UNION ALL SELECT source_id FROM series "
    "WHERE series_id IS NULL OR series_id = ''",                                                           # M5
    "SELECT series_id FROM series WHERE series_id >= '!' AND series_id < 'zz;'",                          # M7/M8
    "SELECT series_id FROM series WHERE series_id >= '' AND series_id LIKE 'zz:%'",                       # M10
    "WITH x AS (SELECT 1) SELECT series_id FROM series, x WHERE series_id IS NULL OR series_id = ''",
    "SELECT s.series_id FROM series s JOIN series t ON 1=1 WHERE s.series_id IS NULL OR s.series_id = ''",
    "SELECT series_id FROM series WHERE series_id >= 'zz:' AND series_id < 'zzz;'",                      # two sources
    "SELECT series_id FROM series WHERE series_id >= 'a:b:' AND series_id < 'a:b;'",
    "SELECT series_id FROM (SELECT * FROM series) WHERE series_id IS NULL OR series_id = ''",
])
def test_an_unbounded_read_is_caught(q):
    assert B.unbounded(q), q


def test_a_range_for_another_source_is_caught_when_the_source_is_known():
    q = "SELECT series_id FROM series WHERE series_id >= 'zy:' AND series_id < 'zy;'"
    assert not B.unbounded(q) and B.unbounded(q, source="zz") and not B.unbounded(q, source="zy")


@pytest.mark.parametrize("q,reads", [
    ("SELECT 1 FROM series", True), ("select 1 from main.series", True), ('SELECT 1 FROM "series"', True),
    ("SELECT 1 FROM [series]", True), ("SELECT 1 FROM `series`", True), ("SELECT 1 FROM x JOIN series ON 1", True),
    ("SELECT 1 FROM series_fts", False), ("SELECT 1 FROM license", False), ("SELECT 1 FROM series2", False),
])
def test_every_form_of_a_series_read_is_seen(q, reads):
    """R1267: the filter saw only `FROM series`, so `main.series`, quoted names and JOINs were never judged."""
    assert B.reads_series(q) is reads, q


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
