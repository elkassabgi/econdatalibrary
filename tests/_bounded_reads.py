"""ONE predicate for "this read of the live build is bounded" (R1253; review round 5 N4/N6), shared by every test
that traces a tool's SQL. Four looser copies accepted any statement that merely CONTAINED "LIMIT", "SERIES_ID >="
or "IS NULL" - so a 10**12 chunk and `WHERE series_id >= '' OR series_id IS NULL` (the whole table) both passed.

The trace callback sees the statement with its bound values filled in, so the LIMIT and the key bounds are the
real ones. A read is bounded when it is one of exactly three shapes:
  - the NULL/'' read by the primary key:        WHERE series_id IS NULL OR series_id = ''
  - one source's primary-key range:             WHERE series_id >= 'x…' AND series_id < 'y…'   (x not empty)
  - one iter_series chunk:                       WHERE series_id > '…' ORDER BY series_id LIMIT n (n <= MAX_CHUNK)
and it never groups or filters by source_id (no index: a full scan)."""
from __future__ import annotations

import re

MAX_CHUNK = 1_000_000
_NULL_READ = re.compile(r"\bWHERE SERIES_ID IS NULL OR SERIES_ID = ''$")
_PK_RANGE = re.compile(r"\bWHERE SERIES_ID >= '((?:[^']|'')+)' AND SERIES_ID < '((?:[^']|'')+)'(?: ORDER BY RANDOM\(\) LIMIT \d+)?$")
_CHUNK = re.compile(r"\bWHERE SERIES_ID > '(?:[^']|'')*' ORDER BY SERIES_ID LIMIT (\d+)$")


def _norm(q: str) -> str:
    return " ".join(q.upper().split()).rstrip(";").rstrip()


def unbounded(q: str) -> bool:
    u = _norm(q)
    if "GROUP BY" in u or re.search(r"SOURCE_ID\s*=", u):
        return True
    if _NULL_READ.search(u) or _PK_RANGE.search(u):
        return False
    m = _CHUNK.search(u)
    return not (m and 0 < int(m.group(1)) <= MAX_CHUNK)


def trace_series_reads(monkeypatch):
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
        reads = [q for q in sql if re.search(r"\bFROM\s+SERIES\b", q, re.I)]
        return reads, [q for q in reads if unbounded(q)]
    return whole_table_reads
