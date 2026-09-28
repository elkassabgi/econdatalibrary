"""Stamp one source's /v1/sources data_through from D1 itself: MAX(end_date) over periods that have
already ended, read by primary-key range, upserted into source_data_through. Read back afterwards.

WHY (R730, R737, 2026-09-05). core/sync_state_d1.py computes every source's data_through from the
R2 coherence copy of the local catalogue at each updater run. For sec_edgar that copy is not the
truth - the CI refresher writes D1 only (R726) - and today the copy still carried filer-typo rows
(2215-09-30) and old-rule forward rows, so the sync overwrote a correct hand stamp twice (13:1xZ
after the 12:1xZ respan, and 13:14Z after the 13:04Z re-stamp) and the "observed-only cap" written
against it would have stamped 2026-09-01, a forward row, creeping with the calendar. The only store
that holds this source's truth is D1's own `series` rows: this tool reads them and stamps from
them, and the sync skips the source (DATA_THROUGH_FROM_D1). Called by tools/refresh_sec_edgar.py
after every run; safe to run by hand.

Cost: one index-range read (rows_read ~= the source's row count, 17,468 for sec_edgar), one
single-row upsert, one PK read back. Never a scan, never series_fts.

    python tools/stamp_source_data_through.py --source sec_edgar [--dry-run]
"""
from __future__ import annotations
import argparse
import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def d1_json(sql: str, timeout: int = 600):
    """One statement on econ-catalog, wrangler's parsed --json list. The road, the auth-transient retries
    (R733) and the post-T0 refusal of writes are core.d1_remote's (plan step 1)."""
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from core import d1_remote                                           # noqa: PLC0415 - plan step 1
    try:
        return d1_remote.run_json("econ-catalog", sql, timeout=timeout)
    except RuntimeError as e:
        raise RuntimeError(f"D1 statement failed: {e}") from None


def _refuse_if_gated(source: str) -> None:
    """The second publisher of source_data_through must honour the gate as core/sync_state_d1.py does.
    An absent or unreadable gate refuses too: a publisher cannot tell 'not gated' from 'gate unread'."""
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from core import gen_denylist
    if not os.path.exists(gen_denylist.OUT):
        raise SystemExit(f"FATAL: the worker gate {gen_denylist.OUT} is absent - refusing to stamp")
    if source.lower() in {s.lower() for s in gen_denylist.committed_gate()}:
        raise SystemExit("FATAL: this source is gated - refusing to publish its data_through row")


def stamp(source: str, apply: bool = True) -> tuple[str | None, str | None, int]:
    """Returns (value_stamped, value_read_back, rows_read). value None means the source has no ended row."""
    if apply:
        _refuse_if_gated(source)
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    res = d1_json(f"SELECT MAX(end_date) AS mx FROM series WHERE series_id >= '{source}:' AND series_id < '{source};' "
                  f"AND end_date IS NOT NULL AND end_date <= '{today}'")
    mx = next((row.get("mx") for e in res for row in (e.get("results") or []) if "mx" in row), None)
    rows_read = sum(int((e.get("meta") or {}).get("rows_read") or 0) for e in res)
    if not apply:
        return mx, None, rows_read
    if mx is None:
        # no ended row: a NULL stamp is the honest value, never a future date
        d1_json(f"INSERT INTO source_data_through (source_id, data_through) VALUES ('{source}', NULL) "
                f"ON CONFLICT(source_id) DO UPDATE SET data_through=NULL")
    else:
        d1_json(f"INSERT INTO source_data_through (source_id, data_through) VALUES ('{source}', '{mx}') "
                f"ON CONFLICT(source_id) DO UPDATE SET data_through=excluded.data_through")
    back = d1_json(f"SELECT data_through FROM source_data_through WHERE source_id='{source}'")
    got = next((row.get("data_through") for e in back for row in (e.get("results") or []) if "data_through" in row), None)
    return mx, got, rows_read


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", required=True)
    ap.add_argument("--dry-run", action="store_true", help="read the value, write nothing (named in ARGV for the cost guard, R323)")
    a = ap.parse_args()
    t0 = dt.datetime.now(dt.timezone.utc)
    mx, got, rows_read = stamp(a.source, apply=not a.dry_run)
    if a.dry_run:
        print(f"{t0:%H:%M:%SZ} {a.source}: D1 max ended end_date = {mx} (rows_read {rows_read:,}); dry run, nothing stamped")
        return 0
    ok = got == mx
    print(f"{t0:%H:%M:%SZ} {a.source}: stamped data_through={mx} from D1 (rows_read {rows_read:,}); read back {got} -> "
          f"{'OK' if ok else 'MISMATCH'}. /v1/sources shows it within max-age=300. The updater sync leaves this source alone "
          f"(DATA_THROUGH_FROM_D1) from the commit that ships with this tool; until that commit is on main, the next sync "
          f"overwrites it and this tool must run again after it.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
