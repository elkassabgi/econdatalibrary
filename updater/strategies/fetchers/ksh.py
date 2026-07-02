"""S1 fetcher — KSH Hungary (Központi Statisztikai Hivatal) STADAT tables.

Open data (attribution to KSH, www.ksh.hu). Multi-file source: one parquet per
theme under clean_full/ksh/{theme}.parquet, schema (series_key, obs_date, value);
series_key = ``KSH:{theme}{NNNN}:...`` and obs are annual (Dec-31). This MERGES
revisions into the existing theme files produced by jobs/ingest_ksh_hungary.py —
it reuses that ingester's URL channel + parse_ksh_csv() so keys line up exactly.

Vintage signal (registry hint): the catalog
``https://www.ksh.hu/stadat_files/toc.json`` lists every table with an
``updatedAt`` / ``correctedAt`` timestamp. current_vintage() hashes that catalog
(changes iff ANY table's metadata moved). update() re-fetches ONLY the themes
whose catalog max(updatedAt) advanced past what we last merged (a per-theme
vintage sidecar ``_toc_vintage.json``; on first run it falls back to the parquet
file mtime, so we re-pull exactly the themes genuinely revised since the build).

KSH runs an F5 WAF, but toc.json + the English CSVs answer the library User-Agent
with HTTP 200 (verified live); we keep the gentle RATE the ingester uses. A theme
re-fetch that loses CSVs to the WAF / timeouts is tallied transient (status
'partial', re-runs next tick) — never laundered into ok. To keep any one tick
tractable (and WAF-friendly) we re-fetch at most ``max_themes_per_run`` stale
themes per call, oldest-first; the rest land on later ticks.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import re
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import content_hash, UA

# Reuse the existing ingester's parse logic (same keys as the published files).
# jobs/ is a top-level namespace package on sys.path (the orchestrator and the
# live-test both put the repo root there); fall back to loading it by file path.
try:
    from jobs.ingest_ksh_hungary import parse_ksh_csv  # type: ignore
except ImportError:  # pragma: no cover - path fallback
    import importlib.util as _ilu

    _src = os.path.join(config.ROOT, "jobs", "ingest_ksh_hungary.py")
    _spec = _ilu.spec_from_file_location("ingest_ksh_hungary", _src)
    _mod = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)  # type: ignore
    parse_ksh_csv = _mod.parse_ksh_csv

SOURCE = "ksh"
BASE = "https://www.ksh.hu/stadat_files"
TOC_URL = f"{BASE}/toc.json"
DEDUP = ("series_key", "obs_date")
RATE = 0.3
SIDECAR = "_toc_vintage.json"
TRANSIENT_HTTP = (429, 500, 502, 503, 504)
# Default per-tick theme budget (overridable via unit.config or env for tests).
MAX_THEMES_PER_RUN = 6


# ----------------------------------------------------------------- toc helpers
def _fetch_toc():
    """Return (raw_bytes, tables_list) or (None, None) on transient. tables_list
    is None and raw is not None only if the body is a 200 that parsed no tables."""
    try:
        r = requests.get(TOC_URL, headers=UA, timeout=60)
    except (requests.Timeout, requests.ConnectionError):
        return None, None
    if r.status_code in TRANSIENT_HTTP:
        return None, None
    if r.status_code != 200 or not r.content:
        return None, None
    try:
        j = json.loads(r.content.decode("utf-8-sig"))
    except (ValueError, UnicodeDecodeError):
        return r.content, []  # 200 but unparseable catalog -> structural signal
    tables = [t for t in j.get("tables", []) if isinstance(t, dict) and t.get("id")]
    return r.content, tables


def _by_theme(tables):
    out: dict[str, list] = {}
    for t in tables:
        tid = str(t["id"])
        th = tid[:3].lower()
        if re.fullmatch(r"[a-z]{3}", th):
            out.setdefault(th, []).append(t)
    return out


def _theme_token(theme_tables) -> str:
    """Max (updatedAt|correctedAt) across a theme — its catalog vintage."""
    return max((f"{t.get('updatedAt') or ''}|{t.get('correctedAt') or ''}"
                for t in theme_tables), default="")


def _max_updated(theme_tables) -> str:
    return max((t.get("updatedAt") or "" for t in theme_tables), default="")


# ----------------------------------------------------------------- sidecar I/O
def _sidecar_path():
    return os.path.join(config.source_dir(SOURCE), SIDECAR)


def _load_sidecar() -> dict:
    p = _sidecar_path()
    if os.path.exists(p):
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except (ValueError, OSError):
            return {}
    return {}


def _save_sidecar(d: dict) -> None:
    p = _sidecar_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = f"{p}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
        os.replace(tmp, p)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _file_mtime_iso(path) -> str:
    try:
        return dt.datetime.fromtimestamp(
            os.path.getmtime(path), dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except OSError:
        return ""


def _is_stale(theme, theme_tables, sidecar) -> bool:
    """A theme needs a re-fetch iff its catalog vintage advanced since we last
    merged it. With a sidecar entry: token differs. Without one (first run on a
    pre-existing parquet): the catalog's max updatedAt is newer than the parquet
    file's mtime. No parquet yet -> stale (build it)."""
    tok = _theme_token(theme_tables)
    if theme in sidecar:
        return sidecar[theme] != tok
    path = os.path.join(config.source_dir(SOURCE), f"{theme}.parquet")
    if not os.path.exists(path):
        return True
    return _max_updated(theme_tables) > _file_mtime_iso(path)


# ----------------------------------------------------------------- fetch CSV
def _get_csv(url, retries=3):
    """Returns (text, outcome): outcome in {'ok','missing','transient'}."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=30)
        except (requests.Timeout, requests.ConnectionError):
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 200:
            head = r.content[:600].lstrip().lower()
            if b"request rejected" in head:  # F5 WAF served as 200
                time.sleep(5 * (attempt + 1))
                continue
            return r.text, "ok"
        if r.status_code == 404:
            return None, "missing"
        if r.status_code in TRANSIENT_HTTP:
            time.sleep(5 * (attempt + 1))
            continue
        return None, "missing"  # other hard 4xx for one table — treat as absent
    return None, "transient"


# ----------------------------------------------------------------- public API
def current_vintage(unit):
    """Cheap probe: hash of the catalog. Changes iff any table's metadata moved.
    None on transient (strategy then fetches anyway, which is safe)."""
    raw, tables = _fetch_toc()
    if raw is None:
        return None
    return content_hash(raw)


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    cfg = unit.config or {}
    budget = int(cfg.get("max_themes_per_run")
                 or os.environ.get("KSH_MAX_THEMES", MAX_THEMES_PER_RUN))

    raw, tables = _fetch_toc()
    tally = Tally()
    if raw is None:
        tally.transient_unit()
        return finalize(tally, _total_rows(out_dir), None, source=SOURCE)
    if not tables:
        tally.structural_unit()  # 200 catalog that parsed no tables -> real break
        return finalize(tally, _total_rows(out_dir), None, source=SOURCE)

    by_theme = _by_theme(tables)
    sidecar = _load_sidecar()

    stale = [(th, tabs) for th, tabs in by_theme.items() if _is_stale(th, tabs, sidecar)]
    # Oldest catalog-vintage first so the most overdue themes refresh soonest.
    stale.sort(key=lambda kv: _max_updated(kv[1]))
    to_run = stale[:budget] if budget > 0 else stale

    cursors: dict[str, str] = {}
    last_obs = None
    for theme, theme_tables in to_run:
        keys, dates, vals = [], [], []
        seen: set[tuple] = set()
        theme_transient = False
        for t in theme_tables:
            tid = str(t["id"])
            try:
                num = int(tid[3:])
            except ValueError:
                continue
            text, outcome = _get_csv(f"{BASE}/{theme}/en/{tid}.csv")
            time.sleep(RATE)
            if outcome == "transient":
                theme_transient = True
                continue
            if outcome == "missing" or text is None:
                continue
            for key, d, v in parse_ksh_csv(text, theme, num):
                tok = (key, d)
                if tok in seen:
                    continue
                seen.add(tok)
                keys.append(key); dates.append(d); vals.append(v)

        if theme_transient and not keys:
            tally.transient_unit()  # lost the theme to WAF/timeout — retry next tick
            continue

        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        path = os.path.join(out_dir, f"{theme}.parquet")
        before = blob.row_count(path)

        if tbl.num_rows == 0:
            # Theme fetched fine but produced no rows. Known-empty themes (ele/ido)
            # legitimately yield nothing; a revision merge with 0 new rows is also
            # fine. Either way: not a structural break, and nothing to publish.
            if not os.path.exists(path):
                tally.empty_unit()
                # Record vintage so we don't re-crawl an empty theme every tick.
                if not theme_transient:
                    sidecar[theme] = _theme_token(theme_tables)
                continue
            tally.empty_unit()
            if not theme_transient:
                sidecar[theme] = _theme_token(theme_tables)
            continue

        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except Exception:
            # merge guard tripped (e.g. partial theme would shrink): keep old data,
            # surface as transient so the theme re-runs with a full pull next tick.
            tally.transient_unit()
            continue

        tally.added_unit(max(0, n - before))
        if md and (last_obs is None or md > last_obs):
            last_obs = md
        # Per-table cursors so a frozen table can't hide behind the theme max.
        for k, d in _series_maxes(tbl).items():
            if k not in cursors or d > cursors[k]:
                cursors[k] = d
        # Only stamp the catalog vintage when the theme merged cleanly with no
        # transient losses; otherwise leave it stale to retry.
        if not theme_transient:
            sidecar[theme] = _theme_token(theme_tables)

    _save_sidecar(sidecar)
    total = _total_rows(out_dir)
    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors)


# ----------------------------------------------------------------- small utils
def _series_maxes(tbl) -> dict:
    out: dict[str, dt.date] = {}
    for k, d in zip(tbl.column("series_key").to_pylist(),
                    tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def _total_rows(out_dir) -> int:
    if not os.path.isdir(out_dir):
        return 0
    total = 0
    for fn in os.listdir(out_dir):
        if fn.endswith(".parquet"):
            total += blob.row_count(os.path.join(out_dir, fn))
    return total
