"""sec_edgar's data_through for the self-hosted origin copy (plan: "sec_edgar IS A T0 PREREQUISITE").

WHY THIS SOURCE HAS ITS OWN RULE. Every other source's data_through is a statistic the sync computes over the
catalogue. sec_edgar's catalogue rows are written by its own refresher, and until T0 only on D1 (R726), where
tools/stamp_source_data_through.py stamps MAX(end_date <= today). After T0 D1 is frozen, and the origin copy
needs the value from the local rows. core.sync_state_d1.LOCAL_FRESHNESS_WRITERS names this module;
core.sync_state_d1.local_writer_rows calls data_through() on the copy being built.

THE RULE: MAX(end_date) over the source's rows, read by primary-key range. NO `<= today` clamp. The D1 stamp's
clamp hides a forward row and then creeps with the calendar as that date comes round (R737). The refresher's
span rule (tools/refresh_sec_edgar.coverage_span) keeps an end date at or before its own filing, with two
recorded exceptions - its fallback when no fact has ended, and a filing made after 17:30 ET, which EDGAR dates
on the next business day (R1193). A row that ends after today is therefore a defect or a transient, and either
way the copy must not be published with it: this function RAISES, and the build that called it fails with the
running generation left serving.

WHAT THIS IS NOT. It is not a proof that the local rows are the refresher's truth. Before T0 they are not (the
CI refresher writes D1). That proof is a gate - tools/selfhost/t0_ready.py, check `sec-edgar-local`, which
reads the receipt of tools/selfhost/sec_edgar_local_check.py - never a marker inside this function (review
AR-194: a side-file hash goes stale at the first daily run, a last_updated marker is false in both directions).

Imports nothing heavy on purpose: it is imported inside every origin-copy build.
"""
from __future__ import annotations

import datetime as dt

SOURCE = "sec_edgar"
# The source's rows by PRIMARY KEY RANGE - an index range, never `WHERE source_id=` (R721/R723). ';' is the
# character after ':', so the range is exactly the ids that start with "sec_edgar:". The 13F product's own
# key, "sec_edgar_13f:", sorts after "sec_edgar;" and is outside it.
_RANGE = "series_id >= 'sec_edgar:' AND series_id < 'sec_edgar;'"
_NAMED = 10          # how many offending ids an error names


class NotPublishable(RuntimeError):
    """The copy's sec_edgar rows must not be served as they are."""


def store_name(ident: str) -> str:
    """The store file's base name for a company ident (the part of the series id after "sec_edgar:"). ONE
    definition: the refresher writes `<store_name>.parquet`, and the local check maps catalogue ids to files
    with the same function - always id -> file, never a file name back to an id (the mapping loses "/" and ":")."""
    return ident.replace("/", "_").replace(":", "_")


def _today_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def data_through(conn, today: str | None = None) -> str | None:
    """MAX(end_date) of the source's rows in `conn` (the copy being built), or None when it has no row.

    Raises NotPublishable when a row ends after `today` (UTC; injected by tests) or has no end date. The
    message names the rows and the repair, because the reader is whoever finds a failed swap."""
    today = today or _today_utc()
    n, mx, nulls = conn.execute(
        f"SELECT COUNT(*), MAX(end_date), SUM(end_date IS NULL) FROM series WHERE {_RANGE}").fetchone()
    if not n:
        return None
    if nulls:
        ids = [r[0] for r in conn.execute(
            f"SELECT series_id FROM series WHERE {_RANGE} AND end_date IS NULL ORDER BY series_id LIMIT {_NAMED}")]
        raise NotPublishable(
            f"{SOURCE}: {nulls:,} row(s) have no end_date (e.g. {ids}) - the refresher writes a span for every "
            f"company; re-run it for these: python tools/refresh_sec_edgar.py --ciks <cik> --apply")
    # ISO dates compare as text; a value that is not an ISO date would sort unpredictably, so it is caught too
    forward = conn.execute(
        f"SELECT series_id, end_date FROM series WHERE {_RANGE} AND (end_date > ? OR length(end_date) != 10) "
        f"ORDER BY end_date DESC, series_id LIMIT {_NAMED}", (today,)).fetchall()
    if forward:
        count = conn.execute(
            f"SELECT COUNT(*) FROM series WHERE {_RANGE} AND (end_date > ? OR length(end_date) != 10)",
            (today,)).fetchone()[0]
        raise NotPublishable(
            f"{SOURCE}: {count:,} row(s) end after today ({today} UTC) or carry a malformed date: "
            f"{[tuple(r) for r in forward]}. A reported period cannot end after it was filed, so this is a "
            f"filer typo taken by the span rule's fallback, or a filing dated the next business day. The copy is "
            f"NOT published (the running generation keeps serving). A next-day date heals by itself tomorrow; "
            f"otherwise re-run the refresher for the company: python tools/refresh_sec_edgar.py --ciks <cik> --apply")
    return mx
