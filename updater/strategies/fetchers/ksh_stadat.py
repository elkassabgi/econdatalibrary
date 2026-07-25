"""S5 bulk fetcher — KSH (Hungarian Central Statistical Office) STADAT tables. No key.

~1,642 STADAT tables fan IN to one parquet per THEME (theme = table_id[:3]); 26 files on R2,
schema (series_key, obs_date, value). series_key = "KSH:{table_id}:{row_label}:{col_label}",
built by jobs.ingest_ksh_stadat.make_key via parse_table — imported here so keys match disk
byte-for-byte (duplication invariant).

Manifest = https://www.ksh.hu/stadat_files/toc.json: every table carries `updatedAt` and
`correctedAt` (a real per-table vintage; verified 1,642/1,642 populated, max updatedAt = today).
The CSVs expose NO Last-Modified/ETag, so an rba-style conditional GET is impossible — the
toc.json vintage IS the gate. Token = "{updatedAt}|{correctedAt}"; correctedAt catches silent
revisions that updatedAt misses (same rationale as faostat's DateUpdate|rows|size triple).

Because many tables share one theme parquet, changed tables are grouped BY THEME and each theme
is merged once (dedup + never-shrink), and a theme's tables only have their vintages advanced
after that theme's merge succeeds — so a failed merge can never strand a table as "done".

Store I/O via blob (R36); sidecar on the store. Downloads run across a small pool (R40).

HONEST-STATUS: toc.json failure -> TransientError. Per-table fetch failure / WAF page -> transient_unit.
A table that parses to zero rows -> empty_unit (KSH has genuinely empty tables; its vintage still
advances so we don't refetch it every tick — a content change moves the token and we re-examine).
Cursors emitted for merged series (R41).
"""
from __future__ import annotations
import hashlib
import json
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize
from jobs import ingest_ksh_stadat as ig   # reuse catalog + THE table parser / key builder

SOURCE = "ksh_stadat"
DEDUP = ("series_key", "obs_date")
SIDECAR = "_bulk_vintages.json"       # {table_id: "updatedAt|correctedAt"}
# www.ksh.hu is SLOW and refuses load: run 30136135069 spent 32 minutes on connect-timeouts
# (60s each) against /stadat_files/*/en/*.csv at 5 workers x 400 tables and never finished.
# Keep concurrency low and the per-run batch small — the 1,632-table backlog drains over many
# ticks, which is fine for a source whose tables update on a monthly-ish cadence. (R40b)
MAX_WORKERS = 2
MAX_PER_RUN = 60


def _vintage(entry) -> str:
    return f"{entry.get('updatedAt', '')}|{entry.get('correctedAt', '')}"


def _table_id(entry):
    for k in ("id", "tableId", "table_id", "code"):
        v = entry.get(k)
        if v:
            return str(v)
    return None


def _catalog(raise_transient: bool):
    try:
        cat = ig.load_catalog()
    except Exception as e:
        if raise_transient:
            raise TransientError(f"ksh_stadat: toc.json fetch failed: {e}")
        return None
    if not cat:
        if raise_transient:
            raise TransientError("ksh_stadat: toc.json returned no tables")
        return None
    return cat


def current_vintage(unit) -> str | None:
    cat = _catalog(raise_transient=False)
    if not cat:
        return None
    h = hashlib.sha256()
    for e in sorted(cat, key=lambda x: str(_table_id(x) or "")):
        tid = _table_id(e)
        if tid:
            h.update(f"{tid}={_vintage(e)};".encode())
    return f"ksh:{h.hexdigest()[:16]}"


def _load_sidecar(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save_sidecar(out_dir, data) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, sort_keys=True).encode("utf-8"))


def _fetch_table(tid):
    """Thread task -> (tid, rows|None). None marks a transport/WAF failure (transient)."""
    theme = tid[:3].lower()
    url = f"{ig.BASE}/{theme}/en/{tid}.csv"
    try:
        raw = ig.get_bytes(url)          # returns None on WAF page / failure
    except Exception:
        return tid, None
    if not raw:
        return tid, None
    try:
        rows, _err = ig.parse_table(tid, raw.decode("utf-8", errors="replace"))
    except Exception:
        return tid, None
    return tid, rows or []


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    cat = _catalog(raise_transient=True)
    sidecar = _load_sidecar(out_dir)

    todo = []
    for e in cat:
        tid = _table_id(e)
        if not tid:
            continue
        cur_v = _vintage(e)
        theme_path = os.path.join(out_dir, f"{tid[:3].lower()}.parquet")
        if sidecar.get(tid) == cur_v and blob.exists(theme_path):
            continue
        todo.append((tid, cur_v))
    todo.sort()

    tally = Tally()
    capped = len(todo) > MAX_PER_RUN
    batch = todo[:MAX_PER_RUN]

    # fetch+parse concurrently, accumulating rows per THEME (many tables -> one parquet)
    by_theme = defaultdict(list)           # theme -> [(key, date, val), ...]
    theme_tables = defaultdict(list)       # theme -> [(tid, vintage), ...] pending vintage bump
    if batch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(_fetch_table, tid): (tid, v) for tid, v in batch}
            for fut in as_completed(futs):
                tid, cur_v = futs[fut]
                _t, rows = fut.result()
                if rows is None:
                    tally.transient_unit()
                    continue
                theme = tid[:3].lower()
                if not rows:
                    # genuinely empty table: advance its vintage so we don't refetch every tick
                    tally.empty_unit()
                    sidecar[tid] = cur_v
                    continue
                by_theme[theme].extend(rows)
                theme_tables[theme].append((tid, cur_v))
                tally.added_unit(len(rows))

    cursors: dict[str, str] = {}
    maxd = None
    published = 0
    for theme, rows in by_theme.items():
        keys = [r[0] for r in rows]
        dates = [r[1] for r in rows]
        vals = [r[2] for r in rows]
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        path = os.path.join(out_dir, f"{theme}.parquet")
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError:
            # isolate to this theme; its tables keep their OLD vintages so they retry next tick
            tally.transient_unit()
            continue
        published += n
        for k, d in zip(keys, dates):
            iso = d.isoformat()
            if k not in cursors or iso > cursors[k]:
                cursors[k] = iso
        if md and (maxd is None or str(md) > str(maxd)):
            maxd = md
        for tid, cur_v in theme_tables[theme]:
            sidecar[tid] = cur_v            # advance ONLY after the theme merged cleanly

    _save_sidecar(out_dir, sidecar)

    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))

    res = finalize(tally, published, maxd or (since or None), source=SOURCE,
                   series_cursors=cursors)
    if capped:
        res.new_vintage = None
    return res
