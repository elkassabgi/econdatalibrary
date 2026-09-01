"""S4 fetcher — Eurostat (one of the four named giants, ~7,600 datasets, ~6.1B obs).

Change-feed: the dissemination catalogue TOC
  https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt
is a tab-separated, quoted table whose columns are
  title | code | type | last update of data | last table structure change | data start | data end | values
Rows of type `dataset`/`table` are real datasets; `folder` rows are not. The
"last update of data" cell (DD.MM.YYYY) is the per-dataset vintage: re-fetch only
codes whose lastUpdate moved (plus new codes, plus any flow whose last run was
partial/failed/empty/absent).

Per changed flow: incremental SDMX-CSV pull
  .../sdmx/2.1/data/<code>/?format=SDMX-CSV&startPeriod=<last_obs+tail>
merged into clean_full/eurostat/<CODE>.parquet (series_key, obs_date, value).

STABLE series_key — the load-bearing fix:
  The SDMX-CSV columns are DATAFLOW, `LAST UPDATE`, <dims...>, TIME_PERIOD,
  OBS_VALUE, OBS_FLAG, CONF_STATUS. The legacy ingest joined ALL non-time/value
  columns as `k=v`, so `LAST UPDATE` (a SPACE, which slipped past the LAST_UPDATE
  underscore skip-set) AND OBS_FLAG/CONF_STATUS leaked into the key:
      LAST UPDATE=13/05/26 11:00:00:freq=A:...:OBS_FLAG=e
  Because `LAST UPDATE` changes EVERY release, every series got a brand-new key on
  every publish -> the whole file duplicated. Here the key is built from the
  DIMENSION columns only (everything strictly between `LAST UPDATE` and
  TIME_PERIOD), so it is STABLE across releases. lastUpdate is carried in the
  change-feed sidecar, not the key. See the RE-KEY data-op note in
  giant_changed_units.py for the existing-data migration (designed, NOT executed).
"""
from __future__ import annotations

import csv
import datetime as _dt
import gzip
import io
import json
import os

import pyarrow as pa

from ..base import Result
from ... import blob, config
from ...errors import DefinitiveError
from . import _giant
from ._giant import http_get

TOC_URL = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt"
DATA_URL = ("https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
            "{code}/?format=SDMX-CSV")
GZIP_MAGIC = bytes([0x1F, 0x8B])   # a .csv.gz body served with a CSV Content-Type

# A FLOW THIS BIG CANNOT BE PARSED ON A 16 GB RUNNER, AND FINDING OUT BY TRYING KILLS THE RUN.
# Un-breaking the gzip bodies (below) made the giants parse for the first time, and the very
# first forced run OOM-died on one: [mem] avail fell 1221MB -> 26MB and GitHub cancelled the
# job at flow ~125 of 400, discarding 12.3M rows of real progress because an OOM kill skips
# save_state. migr_asyrescra alone is 213,650,346 observations.
#
# tools/audit_cloud_capacity.py calibrates this from OBSERVED CI deaths, not theory: bis
# destroyed a runner at a 36,379,671-row file and bls at 66,161,839, while abs and adb
# survived at ~3M. The ceiling here sits well under the smallest observed kill and above
# ef_oga_main (9,009,130 observations), which parses fine.
#
# The count is taken by STREAMING the decompressed bytes and counting newlines - bounded
# memory, no arrays - so the decision costs time on a giant flow and nothing on a normal one.
# It has to PREVENT the parse: _giant's per-flow handler catches exceptions, and a process
# that is killed raises nothing. Ledger R473.
NEWLINE_BYTE = bytes([0x0A])
MAX_FLOW_ROWS = 20_000_000
CSV_ACCEPT = "text/html,*/*"   # Eurostat ignores Accept; format is in the query
RATE = 1.0
TIMEOUT = 600

# Columns that are NEVER part of the (stable) series_key. `LAST UPDATE` (space) is
# the critical one — it changes every release. OBS_FLAG/CONF_STATUS/OBS_STATUS are
# per-observation attributes, not series identity. DATAFLOW/TIME_PERIOD/OBS_VALUE
# are structural. Matched case-insensitively and also with '_'<->' ' normalized.
# The STRUCTURAL PREFIX of an SDMX-CSV row — the columns that precede the dimensions. Only
# these may be removed positionally; everything else before TIME_PERIOD is a dimension, even
# when its name collides with an observation/attribute name.
_STRUCTURAL = {"DATAFLOW", "STRUCTURE", "STRUCTURE_ID", "STRUCTURE_NAME", "ACTION",
               "LAST UPDATE", "LAST_UPDATE"}

_NON_KEY = {
    "DATAFLOW", "STRUCTURE", "STRUCTURE_ID", "STRUCTURE_NAME", "ACTION",
    "LAST UPDATE", "LAST_UPDATE", "TIME_PERIOD", "TIME", "PERIOD", "DATE",
    "OBS_VALUE", "VALUE", "OBS_FLAG", "OBS_STATUS", "CONF_STATUS", "FLAG",
}


def _norm(col: str) -> str:
    return col.strip().upper().replace(" ", "_")


def _parse_toc(text: str) -> dict:
    """Return {code: {"vintage": <DD.MM.YYYY str>, "filename": <CODE.parquet>}}."""
    out = {}
    reader = csv.reader(io.StringIO(text), delimiter="\t", quotechar='"')
    header = next(reader, None)
    if not header:
        raise DefinitiveError("eurostat TOC: empty header")
    cols = [c.strip().strip('"').lower() for c in header]
    try:
        i_code = cols.index("code")
        i_type = cols.index("type")
        i_upd = cols.index("last update of data")
    except ValueError as e:
        raise DefinitiveError(f"eurostat TOC: missing expected column ({e}); columns={cols}")
    for row in reader:
        if len(row) <= max(i_code, i_type, i_upd):
            continue
        code = row[i_code].strip().strip('"')
        typ = row[i_type].strip().strip('"').lower()
        upd = row[i_upd].strip().strip('"')
        if typ not in ("dataset", "table") or not code:
            continue
        out[code] = {"vintage": upd or "", "filename": code.upper() + ".parquet"}
    return out


def fetch_catalog() -> dict:
    raw = http_get(TOC_URL, CSV_ACCEPT, 120, rate=RATE)
    if raw is None:
        # 4xx on the TOC is unusual; treat the catalogue as undeterminable -> empty
        # so run_giant raises a transient (never a false 'no flows changed').
        return {}
    return _parse_toc(raw.decode("utf-8", errors="replace"))


def _build_key(row: dict, dim_cols: list[str]) -> str:
    return ":".join(f"{c}={row[c]}" for c in dim_cols if row.get(c))


class TooBigForRunner(Exception):
    """This flow's decompressed body is past what a 16 GB runner can hold (R473)."""


def _gz_line_count(content: bytes) -> int:
    """Rows in a gzip body, counted by STREAMING - constant memory, no arrays built.

    Counting costs a second decompression on flows we go on to parse, which is cheap
    relative to parsing them, and nothing at all on the giants we defer.
    """
    n = 0
    with gzip.GzipFile(fileobj=io.BytesIO(content)) as fh:
        while True:
            chunk = fh.read(8 << 20)
            if not chunk:
                break
            n += chunk.count(NEWLINE_BYTE)
    return max(0, n - 1)          # minus the header line


def _parse_csv(content: bytes):
    """Parse Eurostat SDMX-CSV -> (keys, dates, values) with a STABLE series_key.
    Returns (None, "structural") if a non-trivial body parsed 0 usable rows."""
    # EUROSTAT SOMETIMES SERVES A .csv.gz AND DOES NOT SAY SO. For large extractions the
    # API hands back the pre-generated bulk file: HTTP 200, Content-Type
    # application/vnd.sdmx.data+csv, Content-Disposition naming estat_<code>_en.csv.gz,
    # and NO Content-Encoding header - so requests does not decompress and the body
    # arrives as raw DEFLATE. Decoding that as utf-8 yields binary, and a lone CR inside
    # it is exactly the "new-line character seen in unquoted field" that killed the
    # 2026-08-24 run at flow ~51 of 400, abandoning the other ~350.
    #
    # The quieter half of the same bug matters more: whether it RAISES depends on whether
    # the first LF or the first lone CR comes first in the compressed stream, because
    # DictReader.fieldnames reads only to the first LF. migr_asyrescra (CR at 46, LF at
    # 366) raises; ef_oga_main (LF at 334, CR at 559) does not - it yields junk
    # fieldnames, finds no TIME_PERIOD column, returns (None, None, None) and the caller
    # marks the flow `structural` PERMANENTLY, with no error line anywhere. Measured:
    # ef_oga_main is 67,622,364 gzip bytes -> 1,324,771,242 bytes of clean CSV ->
    # 9,009,130 observations, the exact count Eurostat's own TOC advertises.
    #
    # Confirmed end-to-end against the live publisher 2026-08-24: migr_asyrescra returns
    # 661,045,036 bytes, Content-Encoding None, body gzip, filename estat_migr_asyrescra_en
    # .csv.gz. The pre-fix path raises on it; this path parses 213,650,346 observations.
    # That is one flow - the one that aborted a 400-flow sweep after 50.
    #
    # Decompress on the MAGIC BYTES, and leave csv's newline handling alone. Normalising
    # or stripping CRs, or pre-splitting the lines, was measured against the real failing
    # bytes: each recovers 0 of those 9,009,130 rows and converts the loud crash into the
    # silent `structural` mislabel above. Stream it rather than gzip.decompress() - these
    # bodies expand ~20x and materialising both a 1.3 GB bytes and a 1.3 GB str is
    # gratuitous. newline="" is correctness, not decoration: TextIOWrapper's default would
    # translate CR to LF inside quoted fields and alter values.
    if content[:2] == GZIP_MAGIC:
        rows = _gz_line_count(content)
        if rows > MAX_FLOW_ROWS:
            raise TooBigForRunner(
                f"gzip body holds ~{rows:,} rows, over the {MAX_FLOW_ROWS:,} ceiling for a "
                f"16 GB runner; deferred rather than parsed (R473)")
        reader = csv.DictReader(io.TextIOWrapper(
            gzip.GzipFile(fileobj=io.BytesIO(content)),
            encoding="utf-8-sig", errors="replace", newline=""))
    else:
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig", errors="replace")))
    if not reader.fieldnames:
        return None, None, None
    fields = reader.fieldnames
    time_col = next((c for c in fields if _norm(c) in ("TIME_PERIOD", "TIME", "PERIOD", "DATE")), None)
    # EXACT OBS_VALUE FIRST, then the bare VALUE alias. A flow can carry a real DIMENSION named
    # `value`, and in DSD order it sits BEFORE OBS_VALUE — measured 2026-09-01 on sbs_pen_7b1,
    # whose SDMX-CSV header is
    #   DATAFLOW, LAST UPDATE, freq, value, nace_r1, geo, TIME_PERIOD, OBS_VALUE, OBS_FLAG, ...
    # A single next() over both names picked `value`, so every row's "observation" was a
    # dimension CODE, float() rejected it, and the flow parsed to ZERO rows in silence —
    # measured by running the shipped parser over sbs_pen_7b1's real bytes: 0 rows before,
    # 1,363 after.
    #
    # THIS BUG HAD NEVER FIRED, and I first wrote here that it was "the likely reason all
    # seven sit in the never-ingested set". That was wrong and the review refuted it. Checked
    # against the state store: eurostat has ONE unit_state row (_all, partial, last_success
    # NULL) and 8 runs ever, every one stopped by _require_rekeyed with "the one-time re-key
    # migration has not completed over this store" — the fetcher has never parsed a single
    # eurostat flow in production. The store was built by the BULK TSV ingester, which has no
    # OBS_VALUE column and so cannot have this defect. And 433 of the 440 missing flows carry
    # no `value` dimension at all, yet were equally absent, so the gap has a different,
    # shared cause. The fix is still right; the story I attached to it was not.
    obs_col = (next((c for c in fields if _norm(c) == "OBS_VALUE"), None)
               or next((c for c in fields if _norm(c) == "VALUE"), None))
    if not time_col or not obs_col:
        return None, None, None
    # DIMENSIONS ARE POSITIONAL in SDMX-CSV: DATAFLOW, [LAST UPDATE], <dimensions>, TIME_PERIOD,
    # OBS_VALUE, <attributes>. Cutting at time_col and removing only the structural PREFIX keeps
    # a dimension whose name collides with _NON_KEY, which a name blacklist cannot do. Proven a
    # no-op elsewhere: over the real dimension lists of all 7,638 flows the two rules agree on
    # every flow except the seven that carry a `value` dimension (tests/test_eurostat_value_dim).
    _i_time = fields.index(time_col)
    dim_cols = [c for c in fields[:_i_time] if _norm(c) not in _STRUCTURAL]
    keys, dates, vals = [], [], []
    for row in reader:
        raw_v = (row.get(obs_col) or "").strip()
        if not raw_v or raw_v in ("NaN", "nan", "NA", "N/A", ".", "...", ":"):
            continue
        try:
            v = float(raw_v.split()[0])
        except (ValueError, IndexError):
            continue
        d = _parse_period(row.get(time_col, ""))
        if d is None:
            continue
        keys.append(_build_key(row, dim_cols))
        dates.append(d)
        vals.append(v)
    return keys, dates, vals


def _parse_period(s: str):
    """SDMX TIME_PERIOD -> date. Annual YYYY -> Dec 31 (matches existing eurostat
    parquet), monthly -> 1st, quarterly/semester -> first month, daily ISO."""
    s = (s or "").strip()
    try:
        if len(s) == 4 and s.isdigit():
            return _dt.date(int(s), 12, 31)
        if len(s) == 7 and s[4] == "-":
            if s[5] == "Q":
                return _dt.date(int(s[:4]), (int(s[6]) - 1) * 3 + 1, 1)
            if s[5] == "S":
                return _dt.date(int(s[:4]), 1 if s[6] == "1" else 7, 1)
            if s[5:].isdigit():
                return _dt.date(int(s[:4]), int(s[5:]), 1)
        # ISO week "2024-W05": W sits at index 5. The original test read s[6] == "W" —
        # an off-by-one that made this branch DEAD (s[6] is the first week digit), so
        # every weekly period parsed to None while the ingester parsed it fine. Found by
        # the fetcher-vs-ingester parity battery, same class as the insee_bdm S/B drift.
        if len(s) == 8 and s[4] == "-" and s[5] == "W":
            return _dt.date.fromisocalendar(int(s[:4]), int(s[6:]), 1)
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return _dt.date.fromisoformat(s)
    except Exception:
        return None
    return None


def _since_param(since: str | None) -> str:
    """Translate a stored max obs_date into a Eurostat &startPeriod=. We re-request
    FROM the stored max (not max+1) so the latest period's revisions are captured;
    dedup-on-(series_key,obs_date) makes the overlap a no-op."""
    if not since:
        return ""
    try:
        d = since if isinstance(since, _dt.date) else _dt.date.fromisoformat(str(since)[:10])
    except Exception:
        return ""
    return f"&startPeriod={d.year:04d}"  # year granularity is safe across all freqs


def fetch_flow(flow_id, meta, since, session):
    """Fetch one changed Eurostat dataset incrementally. Returns (table, status)."""
    url = DATA_URL.format(code=flow_id) + _since_param(since)
    content = http_get(url, CSV_ACCEPT, TIMEOUT, rate=RATE, session=session)
    if content is None:
        # hard 4xx/404 on the data endpoint for a code the TOC lists = structural
        return None, "structural"
    head = content[:64].lstrip()
    if head.startswith(b"<"):
        # Eurostat returned an XML error doc instead of CSV -> structural for this flow.
        return None, "structural"
    keys, dates, vals = _parse_csv(content)
    if keys is None:
        # CSV header missing required columns from a non-trivial body
        return (None, "structural") if len(content) > 200 else (None, "empty")
    if not keys:
        # 200 with a body but no usable rows in the requested tail = quiet flow.
        return None, "empty"
    table = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    return table, "ok"


REKEY_MARKER = "_rekeyed.json"          # written by tools/rekey_eurostat.py --apply


def _require_rekeyed() -> None:
    """Refuse incremental eurostat until the one-time re-key migration has stripped
    the unstable 'LAST UPDATE=' prefix from existing series_key. Running before that
    splits keys (stable new tail vs unstable history) and GROWS the file with the
    same (series,obs_date) under two key schemes — which never-shrink cannot catch.

    TWO CHECKS, because the content check alone had a hole big enough to drive the
    migration through.

    THE HOLE: this used to be `blob.list_parquets(out_dir)[:5]` — the first five of a
    SORTED list (blob.py returns sorted in both the R2 and local branches), and
    tools/rekey_eurostat.py walks that identical sorted list. So a partial --apply
    converts exactly those five FIRST and the guard releases at 0.06% of 7,754 files.
    Not hypothetical: that tool's own comment records a pass dying at file 4,403 of
    7,754 after ~4 hours on a transient R2 read. A daily tick resuming after such a
    death would merge stable-key fetches into ~3,300 still-unstable files — precisely
    the mixed-scheme duplication this guard exists to prevent.

      1. COMPLETION MARKER. The migration writes _rekeyed.json only after a FULL pass
         with zero unreadable files, recording how many it saw. A run that dies leaves
         no marker, so the guard still bites. The count must still match the store, so
         a marker from an older, smaller store does not vouch for files added since.

      2. CONTENT SPOT-CHECK, sampled at EVENLY SPACED indices rather than the head.
         Deterministic (a fetcher should not flap run to run) but uncorrelated with the
         migration's walk order, so a pass that stopped at 57% is caught by the 3/4 and
         last samples. Belt and braces: the marker proves the migration ran to the end,
         this proves the data actually changed.

    Enumerates and reads through blob so the guard still bites under AQUEDUCT_BACKEND=r2 —
    a raw local glob returns [] on a CI runner and would silently DISABLE the guard exactly
    where un-re-keyed data would do the most damage."""
    out_dir = config.source_dir("eurostat")
    files = blob.list_parquets(out_dir)
    if not files:
        return                                   # nothing stored yet; nothing to protect

    marker = None
    try:
        raw = blob.read_bytes(os.path.join(out_dir, REKEY_MARKER))
        marker = json.loads(raw.decode("utf-8")) if raw else None
    except Exception:                            # noqa: BLE001
        marker = None
    seen = marker.get("files_seen") if isinstance(marker, dict) else None
    if seen != len(files):
        raise DefinitiveError(
            f"eurostat: the one-time re-key migration has not completed over this store "
            f"({REKEY_MARKER} says {seen!r}, store holds {len(files)} parquet file(s)). "
            f"Run tools/rekey_eurostat.py --apply to completion before incremental updates; "
            f"a PARTIAL re-key is the dangerous case, because new stable-key fetches would "
            f"merge into still-unstable files under two key schemes. Existing data untouched.")

    n = len(files)
    idx = sorted({0, n // 4, n // 2, (3 * n) // 4, n - 1})
    for i in idx:
        try:
            t = blob.read_table(os.path.join(out_dir, files[i]), columns=["series_key"])
        except Exception:                        # noqa: BLE001
            continue
        if t.num_rows and any("LAST UPDATE" in (k or "")
                              for k in t.column("series_key")[:50].to_pylist()):
            raise DefinitiveError(
                f"eurostat: {files[i]} still uses the UNSTABLE 'LAST UPDATE=' series_key even "
                f"though {REKEY_MARKER} claims a completed re-key — the marker does not match "
                f"the data. Re-run tools/rekey_eurostat.py --apply. Existing data untouched.")


def update(unit, since) -> Result:
    """Entry point used by the giant_changed_units strategy / the fetcher contract."""
    _require_rekeyed()   # gate: never run incrementally over un-re-keyed (unstable-key) data
    return _giant.run_giant(
        unit, source="eurostat",
        fetch_catalog=fetch_catalog, fetch_flow=fetch_flow,
        csv_accept=CSV_ACCEPT, rate=RATE, timeout=TIMEOUT)


# S4 strategy also calls current_vintage() (cheap catalogue probe) for detect_change.
def current_vintage(unit) -> str | None:
    cat = fetch_catalog()
    return _giant._catalog_token(cat) if cat else None
