"""ONE predicate for "this read of the live build is bounded" (R1253; reviews round 5 N4/N6 and round 6 R1267), shared
by every test that traces a tool's SQL.

Round 6 showed a suffix check is not a check: anything that merely ENDED in an accepted shape passed - a UNION ALL
of a whole-table read, a range from ' ' to U+10FFFF, a lower bound of '!' - and `FROM main.series` / `JOIN series`
were never judged at all. Now the WHOLE statement must be exactly one of three shapes, with the trace's bound values
(the trace callback sees them filled in):
  - the NULL/'' read by the primary key:   SELECT cols FROM series WHERE series_id IS NULL OR series_id = ''
  - ONE source's primary-key range:         SELECT cols FROM series WHERE series_id >= 'X:' AND series_id < 'X;'
                                            [ORDER BY RANDOM() LIMIT n]  - the SAME non-empty X on both sides
  - one iter_series chunk:                  SELECT cols FROM series WHERE series_id > '...' ORDER BY series_id
                                            LIMIT n  (0 < n <= MAX_CHUNK)
Anything else that reads `series` - UNION, WITH, JOIN, a subquery, GROUP BY, source_id, another range - is unbounded.
"""
from __future__ import annotations

import re

MAX_CHUNK = 1_000_000
_COLS = r"(?:COUNT\(\*\)|[A-Z_]+(?:, ?[A-Z_]+)*)"
_STR = r"'((?:[^']|'')*)'"
_NULL_READ = re.compile(rf"SELECT {_COLS} FROM SERIES WHERE SERIES_ID IS NULL OR SERIES_ID = ''")
_PK_RANGE = re.compile(rf"SELECT {_COLS} FROM SERIES WHERE SERIES_ID >= {_STR} AND SERIES_ID < {_STR}"
                       r"(?: ORDER BY RANDOM\(\) LIMIT \d+)?")
_CHUNK = re.compile(rf"SELECT {_COLS} FROM SERIES WHERE SERIES_ID > {_STR} ORDER BY SERIES_ID LIMIT (\d+)")
# a read of `series` in ANY form: FROM/JOIN, schema prefix, "series" / [series] / `series`
_READS_SERIES = re.compile(r"\b(?:FROM|JOIN)\s+(?:[\w\"\[\]`]+\.)?[\"\[`]?series[\"\]`]?(?![\w])", re.I)


def _norm(q: str) -> str:
    return " ".join(q.upper().split()).rstrip(";").rstrip()


def unbounded(q: str, source: str | None = None) -> bool:
    """True unless `q` is exactly one of the three bounded shapes. `source`, when given, must be the range's X."""
    u = _norm(q)
    if _NULL_READ.fullmatch(u):
        return False
    m = _PK_RANGE.fullmatch(u)
    if m:
        lo, hi = m.group(1), m.group(2)
        if not (lo.endswith(":") and hi.endswith(";") and lo[:-1] == hi[:-1] and lo[:-1]):
            return True                                  # not ONE source's range (R1267: ' '..U+10FFFF, '!'..X;)
        if ":" in lo[:-1] or ";" in lo[:-1]:
            return True
        return source is not None and lo[:-1] != source.upper()
    m = _CHUNK.fullmatch(u)
    if m:
        return not (0 < int(m.group(2)) <= MAX_CHUNK)
    return True


def reads_series(q: str) -> bool:
    return bool(_READS_SERIES.search(q))


def trace_series_reads(monkeypatch, source: str | None = None):
    """Trace every SQL statement a tool sends through the catalogue resolver (both roads). Returns a function giving
    (reads of `series`, the unbounded ones)."""
    from core import catalog_path
    sql = []
    for name in ("connect", "connect_path"):
        real = getattr(catalog_path, name)

        def traced(*a, _real=real, **k):
            con = _real(*a, **k)
            con.set_trace_callback(sql.append)
            return con
        monkeypatch.setattr(catalog_path, name, traced)

    def whole_table_reads():
        reads = [q for q in sql if reads_series(q)]
        return reads, [q for q in reads if unbounded(q, source)]
    return whole_table_reads
