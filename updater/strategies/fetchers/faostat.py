"""S5 bulk fetcher — FAOSTAT (FAO, CC-BY-4.0), the canonical bulk-dump source.

FAOSTAT ships ~68 DOMAINS, each a whole-domain "(Normalized).zip" long-format
dump with NO server-side date filter. A row-level delta is impossible; the only
honest refresh is "re-download the whole domain and merge". What makes that cheap
is FAO's machine MANIFEST:

    https://bulks-faostat.fao.org/production/datasets_E.json

which carries, per domain, a DateUpdate + FileRows + FileSize. Those three form a
per-domain VINTAGE: a domain whose (DateUpdate, FileRows, FileSize) is unchanged
since we last published it is skipped entirely — no download, no parse. Only
domains whose manifest entry moved are re-downloaded and merged. (DateUpdate alone
can miss a same-day silent republication, and FileRows/FileSize alone can miss a
revision that keeps the row count; the triple is far more sensitive than any one.)

Contract exposed to the S5 strategy (== overwrite_if_changed's fetcher contract):

    current_vintage(unit) -> str | None   # cheap manifest hash (None if probe fails)
    update(unit, since)    -> Result       # per-domain gate -> re-download -> merge

Honest status (Tally/finalize): each DOMAIN is one sub-unit.
  * manifest unreachable in update()                  -> TransientError  (retry)
  * a domain download times out / 5xx / net drop      -> tally.transient (-> partial)
  * a domain ZIP is a 200 with a real body that parses -> tally.added/empty
  * a domain ZIP is corrupt / 200-but-0-rows-from-body -> tally.structural (-> DefinitiveError)
  * an UNCHANGED domain (vintage match)               -> NOT counted (it is up to date)

DUPLICATION INVARIANT: the series_key is built by the PRODUCTION parser
(jobs.ingest_faostat) as `CODE|dim=val|...` with NO vintage/date/size token in it,
so re-merging the same domain dedups to 0 new rows and a revised domain UPDATES
the affected (series_key, obs_date) rows — it can never duplicate or shrink. The
per-domain manifest vintage lives ONLY in the `_bulk_vintages.json` sidecar and in
unit_state.upstream_vintage, never in the data.

NON-DESTRUCTIVE BY DESIGN: every publish goes through merge.merge_and_write (dedup
+ never-shrink + atomic). The raw zip is streamed to a TEMP file and removed after
parse (we do NOT reuse a cached raw zip — that was the documented stale-data trap:
a cached zip would silently feed the OLD bytes after a vintage bump).
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import zipfile

import pyarrow as pa
import requests

from ... import config, merge, blob
from ...errors import DefinitiveError
from ..base import Result
from ._common import CURSOR_CAP, Tally, cursors_from_table, finalize, merge_cursor_map

# Reuse the production parser (header-driven role mapping + stable key builder).
# Importing the module also pins the exact key construction used in clean_full,
# so the updater and the first-pass ingest agree byte-for-byte on series_key.
from jobs import ingest_faostat as ig

SOURCE = "faostat"
DEDUP = ("series_key", "obs_date")
MANIFEST = ig.MANIFEST
UA = ig.UA
_TRANSIENT_HTTP = (429, 500, 502, 503, 504)

# Per-domain vintage sidecar (lives beside the data, NOT in it). Maps
# DatasetCode -> "DateUpdate|FileRows|FileSize" so an unchanged domain is skipped.
VINTAGE_SIDECAR = "_bulk_vintages.json"

_SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
    ("flag", pa.string()),
])


# --------------------------------------------------------------------------- #
# manifest + vintage helpers
# --------------------------------------------------------------------------- #
def _domain_vintage(d: dict) -> str:
    """Per-domain vintage token from the manifest entry (no bytes downloaded)."""
    return f"{d.get('DateUpdate', '')}|{d.get('FileRows', '')}|{d.get('FileSize', '')}"


def _fetch_manifest(raise_transient: bool):
    """Return the list of domain dicts. In detect_change we swallow probe failures
    (return None -> strategy fetches anyway / cadence-gated); in update we RAISE
    TransientError so a manifest outage is never laundered into no_change."""
    try:
        r = requests.get(MANIFEST, headers=UA, timeout=120)
    except (requests.Timeout, requests.ConnectionError) as e:
        if raise_transient:
            from ...errors import TransientError
            raise TransientError(f"{SOURCE}: manifest fetch failed: {e}")
        return None
    if r.status_code in _TRANSIENT_HTTP:
        if raise_transient:
            from ...errors import TransientError
            raise TransientError(f"{SOURCE}: manifest HTTP {r.status_code}")
        return None
    if r.status_code != 200:
        return None
    try:
        return r.json()["Datasets"]["Dataset"]
    except (ValueError, KeyError, TypeError):
        return None


def _manifest_hash(ds: list) -> str:
    """Unit-level vintage: a hash over EVERY domain's (DateUpdate|FileRows|FileSize).
    Changes iff ANY domain's manifest entry moved. Computed off an already-fetched
    manifest so detect_change() and update() agree without a second HTTP call."""
    h = hashlib.sha256()
    for d in sorted(ds, key=lambda x: x.get("DatasetCode", "")):
        h.update(f"{d.get('DatasetCode', '')}={_domain_vintage(d)};".encode())
    return f"manifest:{h.hexdigest()[:16]}"


def current_vintage(unit) -> str | None:
    """Cheap probe (one manifest GET, no bulk bytes): the manifest hash.

    Changes iff any domain's manifest entry moved, so the strategy skips the whole
    68-domain re-pull when nothing upstream changed. Returns None if the manifest
    can't be reached (strategy then fetches anyway, which is safe: update() gates
    each domain on its own stored vintage, so an undeterminable probe re-pulls
    nothing that is actually unchanged)."""
    ds = _fetch_manifest(raise_transient=False)
    if not ds:
        return None
    return _manifest_hash(ds)


def _load_sidecar(out_dir: str) -> dict:
    # blob-routed (R36): the vintage sidecar lives beside the data in the STORE (R2 in CI),
    # not on the ephemeral runner disk — otherwise every CI run starts with an empty sidecar
    # and re-downloads all 68 domains.
    raw = blob.read_bytes(os.path.join(out_dir, VINTAGE_SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save_sidecar_atomic(out_dir: str, data: dict) -> None:
    payload = json.dumps(data, indent=2, sort_keys=True).encode("utf-8")
    blob.write_bytes_atomic(os.path.join(out_dir, VINTAGE_SIDECAR), payload)


# --------------------------------------------------------------------------- #
# per-domain download + parse-to-long (reusing the production parser)
# --------------------------------------------------------------------------- #
def _download_zip(url: str, dest: str) -> str:
    """Stream a bulk zip to `dest` (a temp path). Returns 'ok' | 'transient' |
    'structural'. Never reuses a cached raw zip (stale-data trap)."""
    try:
        r = requests.get(url, headers=UA, stream=True, timeout=600)
    except (requests.Timeout, requests.ConnectionError):
        return "transient"
    if r.status_code in _TRANSIENT_HTTP:
        return "transient"
    if r.status_code != 200:
        return "structural"
    try:
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
    except (requests.Timeout, requests.ConnectionError, requests.exceptions.ChunkedEncodingError):
        return "transient"
    return "ok"


def _parse_zip_to_table(zip_path: str):
    """Parse one domain zip to a long pyarrow.Table using the PRODUCTION parser
    (same role mapping + same vintage-free series_key as clean_full). Returns the
    Table (possibly 0 rows). Raises zipfile.BadZipFile on a corrupt archive so the
    caller can mark it structural."""
    zf, member = ig.open_zip_member(zip_path)
    try:
        rows_iter = ig.stream_csv_rows(zf, member)
        header = next(rows_iter)
        role, dim_idx, _dim_names = ig.plan_columns(header)
        vi = role.get("value")
        yi = role.get("year")
        si = role.get("survey")
        mi = role.get("months")
        fi = role.get("flag")
        ncol = len(header)
        # DatasetCode == the parquet stem; the production key is CODE|... so we
        # need the code prefix. Derive it from the member/zip name fallback to the
        # caller-provided code (set on the table builder below).
        keys, dates, vals, flags = [], [], [], []
        code = _CODE_FOR_PARSE[0]
        for row in rows_iter:
            if len(row) < ncol:
                row = row + [""] * (ncol - len(row))
            val = ig.parse_value(row[vi]) if vi is not None else None
            if val is None:
                continue
            month_lbl = row[mi] if mi is not None else ""
            if yi is not None:
                year_src = row[yi]
            elif si is not None:
                year_src = row[si]
            else:
                year_src = ""
            od = ig.build_date(year_src, month_lbl)
            if od is None:
                continue
            parts = []
            for j in dim_idx:
                cell = row[j].strip().lstrip("'")
                if cell:
                    parts.append(cell)
            if mi is not None and month_lbl and month_lbl.strip().lower() not in ig.MONTHS:
                parts.append(month_lbl.strip())
            sk = code + "|" + "|".join(parts)
            flag = row[fi].strip() if (fi is not None and fi < len(row)) else ""
            keys.append(sk); dates.append(od); vals.append(val); flags.append(flag)
        return pa.table({
            "series_key": pa.array(keys, type=pa.string()),
            "obs_date": pa.array(dates, type=pa.date32()),
            "value": pa.array(vals, type=pa.float64()),
            "flag": pa.array(flags, type=pa.string()),
        }, schema=_SCHEMA)
    finally:
        zf.close()


# Tiny module-global so _parse_zip_to_table can prefix the series_key with the
# current domain code without changing the production parser's signature. Set
# under the per-domain loop (single-threaded per unit; the orchestrator leases the
# unit so no two runners parse the same source concurrently).
_CODE_FOR_PARSE = [""]


def _process_domain(d: dict, out_dir: str, tally: Tally, sidecar: dict,
                    only: set | None, cursors=None) -> None:
    """Gate one domain on its manifest vintage; if changed, download + parse +
    merge. Updates `tally` (one sub-unit per CHANGED domain) and `sidecar`."""
    code = d.get("DatasetCode")
    url = d.get("FileLocation")
    if not code or not url:
        return
    if only is not None and code not in only:
        return

    out_path = os.path.join(out_dir, code + ".parquet")
    cur_v = _domain_vintage(d)
    stored_v = sidecar.get(code)
    # Skip an unchanged domain ENTIRELY (and only if we actually already hold it).
    if stored_v == cur_v and blob.exists(out_path):
        return

    # --- download to a temp file (no cached-zip reuse) ---
    fd, tmp_zip = tempfile.mkstemp(prefix=f"faostat_{code}_", suffix=".zip")
    os.close(fd)
    try:
        st = _download_zip(url, tmp_zip)
        if st == "transient":
            tally.transient_unit(f"{code}: bulk zip download failed (transient)")
            return
        if st == "structural":
            tally.structural_unit(f"{code}: bulk zip download failed (structural)")
            return
        # --- parse to long (production parser) ---
        _CODE_FOR_PARSE[0] = code
        try:
            tbl = _parse_zip_to_table(tmp_zip)
        except zipfile.BadZipFile:
            tally.structural_unit(f"{code}: bulk archive is not a readable zip")
            return
        if tbl.num_rows == 0:
            # 200 with a real archive but parsed 0 rows -> structural break.
            tally.structural_unit(f"{code}: real archive parsed to 0 rows")
            return
        # --- merge (dedup + never-shrink + atomic) ---
        before = blob.row_count(out_path)
        try:
            n, _ = merge.merge_and_write(out_path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError:
            # A legit >3% domain revision trips never-shrink. ISOLATE it to THIS
            # domain — existing data kept, vintage NOT advanced, surfaced as partial
            # for review — instead of aborting the whole-source run (which would also
            # lose vintages saved for domains merged earlier this tick). NAMED, because a
            # refusal with numbers reads as a network failure when it carries only a count.
            tally.transient_unit(
                f"{code}: never-shrink refused the merge over {before:,} stored rows")
            return
        tally.added_unit(max(0, n - before))
        # Report WHICH series moved. Without this the orchestrator cannot re-derive their
        # CSVs (§5.7) and logs 'merged N obs but reported no series_cursors' - which is
        # exactly what happened on 2026-08-01: 170,645,319 observations merged into the
        # parquets while every served CSV stayed at its old values. The parquet is right and
        # the download is stale, and nothing fails, which is what makes it easy to miss.
        # Cursors come from the NEW table, not the merged file: reporting every series in the
        # file would re-derive millions of unchanged CSVs on a source this size.
        if cursors is not None:
            merge_cursor_map(cursors, cursors_from_table(tbl, cap=CURSOR_CAP),
                             cap=CURSOR_CAP)
        # advance the per-domain vintage ONLY after a clean publish.
        sidecar[code] = cur_v
    finally:
        if os.path.exists(tmp_zip):
            try:
                os.remove(tmp_zip)
            except OSError:
                pass


def update(unit, since) -> Result:
    """Re-snapshot only the domains whose manifest vintage moved; merge each.

    `unit.config` may carry {'only': [codes]} to restrict to specific domains
    (used by the non-destructive test harness against a temp DATA_ROOT)."""
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    ds = _fetch_manifest(raise_transient=True)  # manifest outage -> TransientError
    if not ds:
        # 200 but unparseable manifest body -> structural; keep existing data.
        from ...errors import DefinitiveError
        raise DefinitiveError(f"{SOURCE}: manifest returned no parseable domain list")

    only_cfg = (unit.config or {}).get("only")
    only = set(only_cfg) if only_cfg else None

    sidecar = _load_sidecar(out_dir)
    tally = Tally()
    cursors: dict[str, str] = {}
    for d in ds:
        _process_domain(d, out_dir, tally, sidecar, only, cursors=cursors)

    # Persist the per-domain vintages we successfully published (atomic sidecar).
    _save_sidecar_atomic(out_dir, sidecar)

    # Total published rows across all domains we own (honest obs count). blob-routed
    # listing (R36): in CI the parquets live on R2, not the runner disk.
    total_rows = 0
    for fn in blob.list_parquets(out_dir):
        total_rows += blob.row_count(os.path.join(out_dir, fn))

    last_obs = _max_obs_date_over_dir(out_dir)
    # empty_window_floor=10: if a LARGE number of attempted domains all came back
    # empty/404, that's a structural break — but here `attempted` counts only
    # CHANGED domains, so a quiet month (0 attempted) cleanly yields no_change.
    res = finalize(tally, total_rows, last_obs, source=SOURCE,
                   series_cursors=cursors or None)
    # finalize() stamps new_vintage="date-tail" (it is shared with the S2 date-tail
    # fetchers); for a bulk source the unit-level gate token is the MANIFEST HASH.
    # Overwrite it so the orchestrator persists the right upstream_vintage and can
    # skip the whole re-pull next tick. Three guards keep the gate honest:
    #   * only stamp on a clean ok/no_change (a 'partial' still owes a domain, so the
    #     gate must NOT advance — set None and the orchestrator keeps the old vintage);
    #   * only stamp the FULL-manifest hash when we evaluated the FULL manifest
    #     (`only` is None). A subset run (test harness) must not claim all domains are
    #     current, so leave new_vintage=None there;
    #   * the per-domain truth always lives in the sidecar regardless.
    if res.status in ("ok", "no_change") and only is None:
        res.new_vintage = _manifest_hash(ds)
    else:
        res.new_vintage = None
    return res


def _max_obs_date_over_dir(out_dir: str) -> str | None:
    import pyarrow.compute as pc
    mx = None
    for fn in blob.list_parquets(out_dir):   # blob-routed listing (R36)
        p = os.path.join(out_dir, fn)
        try:
            t = blob.read_table(p, columns=["obs_date"])   # project: don't load whole domains
        except Exception:
            continue
        if t.num_rows == 0:
            continue
        m = pc.max(t.column("obs_date")).as_py()
        if m is not None:
            s = str(m)
            if mx is None or s > mx:
                mx = s
    return mx
