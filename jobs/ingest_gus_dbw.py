#!/usr/bin/env python3
"""GUS Poland — DBW (Dziedzinowe Bazy Wiedzy) API, full crawl.

License: Open data (GUS public data policy, CC BY 4.0)
Source: https://api-dbw.stat.gov.pl/ (replaces the dead sdmx.stat.gov.pl)
No API key required; optional registered key via env GUS_DBW_CLIENT_ID
(sent as X-ClientId header).

Endpoints (api/1.2.0):
  * /area/area-area?lang=en                       → flat tree of subject areas
  * /area/area-variable?id-obszaru={a}&lang=en    → variables of an area
  * /variable/variable-meta?id-zmiennej={v}&lang=en → sections (przekroje) with
        frequency (id-czestotliwosc) and year range (szereg-czasowy)
  * /dictionaries/periods-dictionary?lang=en      → id-okres → symbol (M01..M12,
        K1..K4, P1/P2, R...; id-okres=282 = annual)
  * /variable/variable-data-section?id-zmienna=&id-przekroj=&id-rok=&id-okres=
        &ile-na-stronie=5000&numer-strony=&lang=en → observations ("wartosc")

RATE LIMITS (anonymous): 5 req/s, 100 req/15min, 1000 req/12h, 10000 req/7days.
  → 45 s minimum between requests (anonymous), 9 s with X-ClientId
    (registered tier: 10/s, 500/15min, 5000/12h, 50000/7d).
  This source takes WEEKS at anonymous rates — it is a long-haul crawler built
  around aggressive checkpointing: a JSON checkpoint of completed
  variable×section pairs, metadata cached on disk so restarts spend no budget,
  and one parquet part file per completed variable×section. When every
  variable of an area is done, parts merge into area_{id}.parquet.

series_key: GUSDBW:{var_id}:{section_id}:{id-pozycja-* values}[:m{measure}]
obs_date:   annual → Dec 31; quarter → quarter start; month/half/week → start.

Run: python jobs/ingest_gus_dbw.py
"""
from __future__ import annotations
import datetime as dt, glob, json, os, re, time
from email.utils import parsedate_to_datetime
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT   = os.path.join(ROOT, "data", "clean_full", "gus_dbw")
CACHE = os.path.join(OUT, "_cache")
PARTS = os.path.join(OUT, "parts")
CKPT  = os.path.join(OUT, "_checkpoint.json")
BASE  = "https://api-dbw.stat.gov.pl/api/1.2.0"

CLIENT_ID = os.environ.get("GUS_DBW_CLIENT_ID", "").strip()
# Pacing is bound by the MOST restrictive published window, which is the 7-day cap:
#   registered 50000/7d -> 604800/50000 = 12.10 s/request (NOT the old 9 s, which
#   exceeded the weekly cap on a sustained crawl and would eventually trip 429s);
#   anonymous  10000/7d -> 604800/10000 = 60.48 s/request.
# (Verified 2026-06-24 against the live OpenAPI spec api-dbw.stat.gov.pl/apidocs.)
# 2026-07-13: GUS (api-dbw@stat.gov.pl) DISABLED the request limits on this
# registered ClientId (email confirmation on file). The old 12.1s pacing existed
# only to stay under the 50000/7d cap, which no longer applies. Ease to a still-
# respectful ~1 req/s (well under the 10 req/s technical tier, honoring the
# "capped, low-impact" commitment made to GUS): ~12x faster, ~13wk backfill -> ~1wk.
# api_get() still honors Retry-After and backs off on any 429/503, so this
# self-throttles if the gateway ever pushes back.
SLEEP = 1.0 if CLIENT_ID else 60.5
HEADERS = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
if CLIENT_ID:
    HEADERS["X-ClientId"] = CLIENT_ID

PAGE_SIZE = 5000
THIS_YEAR = dt.date.today().year
# Split connect vs read timeout. The api-dbw gateway intermittently load-sheds new
# TCP connections under concurrent load (confirmed by live probe: ~50% of fresh
# connects hang); a single 300 s value made every dropped SYN cost 5 min × 6 retries
# (~40 min/page). A 15 s CONNECT ceiling fails a dead connect fast, while the 300 s
# READ ceiling still covers deep-pagination pages on very large variables
# (e.g. var 581 ≈ 343k obs/year). Rate-limit overage is HTTP 429, never a timeout.
CONNECT_TIMEOUT = 15
READ_TIMEOUT = 300
PAGE_CAP = 2000            # safety stop per (year, period); was 300 — too low
                           # for the largest variables.

# Reuse one keep-alive connection across requests. Establishing fewer NEW TCP/TLS
# connections is the main lever against the gateway's connection load-shedding
# (a kept-alive socket idle for ~12 s stays well under the IIS 120 s keep-alive
# timeout). urllib3 auto-retry is left OFF here — api_get() owns retry/backoff so
# 429 (honor Retry-After) and definitive 4xx are handled distinctly.
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
_adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


_last_req = [0.0]


def throttle():
    wait = SLEEP - (time.time() - _last_req[0])
    if wait > 0:
        time.sleep(wait)


class TransientError(Exception):
    """A request kept failing transiently (timeout / 5xx / network / persistent
    429) after every retry. Callers MUST NOT treat this as 'no data' or 'done' —
    the query has to be retried on a later run so coverage stays complete. This
    is the guard against silently dropping pages of a large variable."""


def _parse_retry_after(val) -> int:
    """Retry-After is delta-seconds OR an HTTP-date (RFC 7231). Return seconds >= 0.

    The api-dbw gateway is Microsoft-IIS/ARR, which commonly emits the HTTP-date
    form; parsing it as int() (the old behavior) raised and silently degraded to a
    0 → 30 s floor, defeating a multi-hour backoff. This honors both forms."""
    if not val:
        return 0
    s = str(val).strip()
    try:
        return max(int(s), 0)
    except (TypeError, ValueError):
        pass
    try:
        when = parsedate_to_datetime(s)
        if when is None:
            return 0
        now = dt.datetime.now(when.tzinfo) if when.tzinfo else dt.datetime.now()
        return max(int((when - now).total_seconds()), 0)
    except Exception:
        return 0


def api_get(path: str, retries: int = 6):
    """GET BASE+path with the mandatory inter-request gap.

    Returns parsed JSON on HTTP 200, or None for a DEFINITIVE empty response
    (HTTP 400/404/422 — the query legitimately has no data). Raises
    TransientError if the request keeps failing transiently after `retries`
    attempts, so a timeout is never mistaken for 'no more pages'.
    """
    attempt = 0
    rl_waits = 0
    while attempt < retries:
        throttle()
        try:
            r = SESSION.get(BASE + path, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            _last_req[0] = time.time()
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404, 422):
                log(f"  HTTP {r.status_code}: {path[:120]}")
                return None
            if r.status_code == 429:
                # Rate-limit overage (the ONLY way the cap manifests — never a
                # timeout). Honor Retry-After, floor to a sane minimum, cap the
                # wait, and bound CONSECUTIVE waits so a 7-day-cap exhaustion
                # eventually defers to the next run instead of spinning. A 429
                # is not a failure, so it does NOT consume a retry attempt.
                ra = _parse_retry_after(r.headers.get("Retry-After"))
                wait = min(max(ra, 30), 900)
                rl_waits += 1
                if rl_waits > 30:
                    raise TransientError(
                        f"persistent 429 rate cap after {rl_waits} waits: {path[:120]}")
                log(f"  429 rate-limited (Retry-After={ra or 'n/a'}) — sleeping {wait}s [{rl_waits}]")
                time.sleep(wait)
                continue
            log(f"  HTTP {r.status_code} (try {attempt + 1}/{retries}): {path[:120]}")
        except Exception as e:
            _last_req[0] = time.time()
            log(f"  ERR {e} (try {attempt + 1}/{retries}) on {path[:90]}")
        attempt += 1
        if attempt < retries:
            time.sleep(min(30 * attempt, 120))   # capped exponential-ish backoff
    raise TransientError(f"{retries} failed attempts: {path[:140]}")


def cached_get(name: str, path: str):
    """Disk-cached API call — restarts must not re-spend the request budget on metadata."""
    f = os.path.join(CACHE, f"{name}.json")
    if os.path.exists(f):
        try:
            with open(f, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            pass
    j = api_get(path)
    if j is not None:
        tmp = f + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(j, fh, ensure_ascii=False)
        os.replace(tmp, f)
    return j


# ---------------------------------------------------------------- checkpoint
def load_ckpt() -> dict:
    if os.path.exists(CKPT):
        try:
            with open(CKPT, encoding="utf-8") as f:
                ck = json.load(f)
            ck.setdefault("done_sections", [])
            ck.setdefault("done_areas", [])
            ck.setdefault("done_year_sections", [])   # year-granular resume within a section
            return ck
        except Exception as e:
            log(f"checkpoint unreadable ({e}); starting fresh")
    return {"done_sections": [], "done_areas": [], "done_year_sections": []}


def save_ckpt(ck: dict):
    tmp = CKPT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(ck, f)
    os.replace(tmp, CKPT)


# ---------------------------------------------------------------- periods
def fetch_periods() -> list[dict]:
    f = os.path.join(CACHE, "periods.json")
    if os.path.exists(f):
        with open(f, encoding="utf-8") as fh:
            return json.load(fh)
    entries, page = [], 0
    while True:
        j = api_get(f"/dictionaries/periods-dictionary?lang=en&ile-na-stronie=100&numer-strony={page}")
        if not isinstance(j, dict):
            break
        data = j.get("data") or []
        entries.extend(data)
        pc = j.get("page-count") or 1
        page += 1
        if page >= pc or not data:
            break
    if entries:
        tmp = f + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(entries, fh, ensure_ascii=False)
        os.replace(tmp, f)
    return entries


def build_period_map(entries: list[dict]) -> dict[int, tuple[str, int]]:
    """id-okres → (kind, n) for plain calendar periods only (M/K/P/R/W symbols).
    Exotic period types (cumulative ranges etc.) are excluded so one series_key
    never mixes two period concepts at the same date."""
    pmap: dict[int, tuple[str, int]] = {}
    for e in entries:
        try:
            pid = int(e.get("id-okres"))
        except (TypeError, ValueError):
            continue
        sym = str(e.get("symbol") or "").strip().upper()
        opis = str(e.get("opis") or "").lower()
        freqname = str(e.get("nazwa-czestotliwosc") or "").lower()
        m = re.fullmatch(r"M(\d{1,2})", sym)
        if m and 1 <= int(m.group(1)) <= 12:
            pmap[pid] = ("M", int(m.group(1)))
            continue
        m = re.fullmatch(r"[QK]([1-4])", sym)   # DBW uses Q1..Q4 (id 270-273)
        if m:
            pmap[pid] = ("Q", int(m.group(1)))
            continue
        m = re.fullmatch(r"P([12])", sym)
        if m:
            pmap[pid] = ("H", int(m.group(1)))
            continue
        m = re.fullmatch(r"W(\d{1,2})", sym)
        if m and 1 <= int(m.group(1)) <= 53:
            pmap[pid] = ("W", int(m.group(1)))
            continue
        if sym == "R" or "year" in freqname or "rok" in opis or "annual" in opis or "year" in opis:
            pmap[pid] = ("A", 0)
    pmap.setdefault(282, ("A", 0))   # documented: id-okres=282 = annual
    return pmap


def period_date(year: int, pid: int, pmap: dict) -> dt.date | None:
    k = pmap.get(pid)
    if not k or not (1900 <= year <= 2100):
        return None
    kind, n = k
    try:
        if kind == "A":
            return dt.date(year, 12, 31)
        if kind == "M":
            return dt.date(year, n, 1)
        if kind == "Q":
            return dt.date(year, (n - 1) * 3 + 1, 1)
        if kind == "H":
            return dt.date(year, 1 if n == 1 else 7, 1)
        if kind == "W":
            return dt.date.fromisocalendar(year, n, 1)
    except ValueError:
        return None
    return None


def candidate_periods(freq_id, entries, pmap) -> list[int]:
    """All plain-calendar periods of the section's frequency. A frequency can
    publish under several period concepts (e.g. annual freq → id 282 'year' OR
    id 328/329 March/October snapshots; quarterly freq → Q1..Q4 OR end-of-quarter
    months) — fetch_section() learns which ones actually carry data."""
    cands = []
    for e in entries:
        try:
            pid = int(e.get("id-okres"))
        except (TypeError, ValueError):
            continue
        if e.get("id-czestotliwosc") == freq_id and pid in pmap and pid not in cands:
            cands.append(pid)
    cands.sort(key=lambda p: (pmap[p][0], pmap[p][1]))
    return cands


def year_range(pz: dict) -> tuple[int, int] | None:
    s = str(pz.get("szereg-czasowy") or "")
    m = re.search(r"(\d{4})\s*[-–]\s*(\d{4})", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
    else:
        m = re.search(r"(\d{4})", s)
        if not m:
            return None
        a = b = int(m.group(1))
    a, b = max(1900, a), min(max(a, b), THIS_YEAR + 1)
    return (a, b) if a <= b else None


# ---------------------------------------------------------------- data fetch
def fetch_year(var: int, sec: int, year: int, cands: list[int], pmap: dict,
               out_rows: list, seen: set) -> dict[int, int]:
    per_period: dict[int, int] = {}
    n_new = 0
    for pid in cands:
        period_start = n_new
        page, last_sig = 0, None
        while True:
            j = api_get(f"/variable/variable-data-section?id-zmienna={var}&id-przekroj={sec}"
                        f"&id-rok={year}&id-okres={pid}&ile-na-stronie={PAGE_SIZE}"
                        f"&numer-strony={page}&lang=en")
            if not isinstance(j, dict):
                break
            data = j.get("data") or []
            if not data:
                break
            sig = (data[0].get("rownumber"), len(data))
            if sig == last_sig:        # guard against 0/1-based page echo
                break
            last_sig = sig
            for rec in data:
                if not isinstance(rec, dict):
                    continue
                w = rec.get("wartosc")
                if not isinstance(w, (int, float)) or w != w:
                    continue
                try:
                    yr = int(rec.get("id-daty", year))
                except (TypeError, ValueError):
                    yr = year
                try:
                    okr = int(rec.get("id-okres", pid))
                except (TypeError, ValueError):
                    okr = pid
                d = period_date(yr, okr, pmap)
                if d is None:
                    continue
                poz = []
                for kk, vv in rec.items():
                    m = re.fullmatch(r"id-pozycja-(\d+)", kk)
                    if m and vv is not None:
                        poz.append((int(m.group(1)), str(vv)))
                poz.sort()
                parts = ["GUSDBW", str(var), str(sec)] + [p[1] for p in poz]
                mi = rec.get("id-sposob-prezentacji-miara")
                if mi is not None:
                    parts.append(f"m{mi}")
                key = ":".join(parts)
                tok = (key, d)
                if tok in seen:
                    continue
                seen.add(tok)
                out_rows.append((key, d, float(w)))
                n_new += 1
            if len(data) < PAGE_SIZE:
                break
            page += 1
            if page > PAGE_CAP:
                log(f"    !! page cap {PAGE_CAP} hit var={var} sec={sec} "
                    f"y={year} okres={pid} — possible truncation, raise PAGE_CAP")
                break
        per_period[pid] = n_new - period_start
        log(f"    y={year} okres={pid}: {n_new - period_start} obs")
    return per_period


def fetch_section(aid: int, var: int, sec: int, yr: tuple[int, int] | None,
                  cands: list[int], pmap: dict, ck: dict,
                  done_year_sections: set) -> int:
    """Sweep years × candidate periods and WRITE the section's data, returning the
    total rows written this call.

    For a bounded year range we checkpoint at YEAR granularity: each year is
    written to its own part (v{var}_s{sec}_y{year}.parquet) and recorded in
    done_year_sections the moment it is durably stored. So a TransientError on a
    late year of a giant section (e.g. v571 'Live births' ≈ 4.4M rows / ~880
    pages) NEVER discards the years already fetched, and the next run resumes at
    the failing year instead of re-spending the 7-day request cap on megabytes it
    already has. A frequency can map to several period concepts; once any year
    produced data we restrict later years to the periods that have ever carried
    data (ever_hit, persisted in the checkpoint) — saving the request budget.

    The unknown-year-range case keeps the original adaptive descent + single
    whole-section part: its 'stop after 4 empty years' heuristic relies on a live
    empty-streak that can't be reconstructed from a year checkpoint, and these
    sections are small/rare, so per-year checkpointing buys little there."""
    section_dir = os.path.join(PARTS, f"area_{aid}")
    skey = f"v{var}:s{sec}"
    # ever_hit is WITHIN-RUN only (not persisted): a resumed run re-probes the full
    # candidate set on its first fresh year and can re-widen coverage, instead of
    # inheriting and freezing a prior partial run's possibly-too-narrow hit set.
    ever_hit: set[int] = set()

    def plist():
        return sorted(ever_hit, key=lambda p: (pmap[p][0], pmap[p][1])) if ever_hit else cands

    if yr is not None:
        total = 0
        for y in range(yr[0], yr[1] + 1):
            ykey = f"{skey}:y{y}"
            if ykey in done_year_sections:
                continue                       # attempted on a prior run — skip
            rows: list[tuple] = []
            seen: set = set()                  # per-year: (key,date) carry the year, so this is complete
            per = fetch_year(var, sec, y, plist(), pmap, rows, seen)
            for pid, n in per.items():
                if n > 0:
                    ever_hit.add(pid)
            if rows:                           # write the part BEFORE checkpointing the year
                write_part(os.path.join(section_dir, f"v{var}_s{sec}_y{y}.parquet"), rows)
                total += len(rows)
            done_year_sections.add(ykey)
            ck["done_year_sections"] = sorted(done_year_sections)
            save_ckpt(ck)
        return total

    # unknown year range — adaptive descent, single whole-section part
    rows = []
    seen = set()
    streak = 0
    for y in range(THIS_YEAR, 1989, -1):
        before = len(rows)
        per = fetch_year(var, sec, y, plist(), pmap, rows, seen)
        for pid, n in per.items():
            if n > 0:
                ever_hit.add(pid)
        streak = 0 if (len(rows) - before) > 0 else streak + 1
        if streak >= 4:
            break
    if rows:
        write_part(os.path.join(section_dir, f"v{var}_s{sec}.parquet"), rows)
    return len(rows)


def write_part(path: str, rows: list[tuple]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tbl = pa.table({
        "series_key": pa.array([r[0] for r in rows], pa.string()),
        "obs_date":   pa.array([r[1] for r in rows], pa.date32()),
        "value":      pa.array([r[2] for r in rows], pa.float64()),
    })
    # Atomic publish: a crash mid-write must never leave a corrupt part that a
    # saved year-checkpoint would later treat as complete and merge.
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        pq.write_table(tbl, tmp, compression="zstd")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


# ---------------------------------------------------------------- main
def main():
    for d in (OUT, CACHE, PARTS):
        os.makedirs(d, exist_ok=True)
    # Reclaim any *.tmp orphaned by a hard-killed prior run (write_part, the area
    # merge, save_ckpt and cached_get all write tmp + os.replace; a SIGKILL/OOM
    # between the two can leave a stray tmp). They are never merged (the glob is
    # *.parquet), just disk litter — sweep them so a multi-week crawl stays clean.
    swept = 0
    for pat in (os.path.join(PARTS, "area_*", "*.tmp"),
                os.path.join(OUT, "*.tmp"),
                os.path.join(CACHE, "*.tmp")):
        for stale in glob.glob(pat):
            try:
                os.remove(stale)
                swept += 1
            except OSError:
                pass
    if swept:
        log(f"Swept {swept} stale .tmp file(s) from a prior run")
    tier = "registered (X-ClientId)" if CLIENT_ID else "anonymous"
    log(f"GUS DBW crawl — {tier} tier, {SLEEP:.0f}s between requests")

    ck = load_ckpt()
    done_sections = set(ck["done_sections"])
    done_areas = set(ck["done_areas"])
    done_year_sections = set(ck.get("done_year_sections", []))

    areas = cached_get("areas", "/area/area-area?lang=en")
    if not isinstance(areas, list) or not areas:
        log("FATAL: cannot fetch area tree")
        return
    log(f"Areas: {len(areas)} nodes")

    entries = fetch_periods()
    pmap = build_period_map(entries)
    kinds = {}
    for k, _ in pmap.values():
        kinds[k] = kinds.get(k, 0) + 1
    log(f"Periods dictionary: {len(entries)} entries, usable calendar periods: {kinds}")

    var_areas = sorted([a for a in areas if isinstance(a, dict) and a.get("czy-zmienne")],
                       key=lambda a: a.get("id", 0))
    log(f"Areas with variables: {len(var_areas)}")

    total_obs = 0
    all_complete = True
    for a in var_areas:
        aid = a.get("id")
        final = os.path.join(OUT, f"area_{aid}.parquet")
        if os.path.exists(final):
            try:
                n = pq.read_metadata(final).num_rows
            except Exception as e:
                # Corrupt final (e.g. disk fault). Do NOT crash the crawler on
                # every watchdog relaunch — skip this area this run and surface it
                # for manual repair; the other areas still progress.
                log(f"Area {aid}: final parquet unreadable ({e}) — skipping this run, needs manual repair")
                all_complete = False
                continue
            total_obs += n
            if aid not in done_areas:
                done_areas.add(aid)
                ck["done_areas"] = sorted(done_areas)
                save_ckpt(ck)
            log(f"Skip area {aid}: {n:,} rows")
            continue
        if aid in done_areas:
            # Completed on a prior run but yielded 0 rows (no final written). Skip
            # it — never re-crawl a known-empty area and re-burn the scarce 7-day cap.
            log(f"Skip area {aid}: previously completed, 0 rows")
            continue

        try:
            vlist = cached_get(f"vars_{aid}", f"/area/area-variable?id-obszaru={aid}&lang=en") or []
        except TransientError as e:
            log(f"Area {aid}: variable list fetch failed transiently ({e}); "
                f"not finalizing — retry next run")
            all_complete = False
            continue
        var_ids = sorted({int(e["id-zmienna"]) for e in vlist
                          if isinstance(e, dict) and e.get("id-zmienna") is not None})
        log(f"Area {aid} '{a.get('nazwa', '')}': {len(var_ids)} variables")

        # An area is finalized (merged + marked done) ONLY if every one of its
        # sections succeeds. Any transient failure flips this flag and the area
        # is left for the next run, so coverage is never silently partial.
        area_complete = True
        for var in var_ids:
            try:
                meta = cached_get(f"meta_{var}", f"/variable/variable-meta?id-zmiennej={var}&lang=en")
            except TransientError as e:
                log(f"  var {var}: metadata fetch failed transiently ({e}); retry next run")
                area_complete = False
                continue
            if not isinstance(meta, dict):
                log(f"  var {var}: no metadata, skip")
                continue
            przekroje = meta.get("przekroje") or []
            if not przekroje:
                log(f"  var {var}: no sections, skip")
                continue
            for pz in przekroje:
                sec = pz.get("id-przekroj")
                if sec is None:
                    continue
                skey = f"v{var}:s{sec}"
                if skey in done_sections:
                    continue
                part = os.path.join(PARTS, f"area_{aid}", f"v{var}_s{sec}.parquet")
                if os.path.exists(part):
                    done_sections.add(skey)
                    ck["done_sections"] = sorted(done_sections)
                    save_ckpt(ck)
                    continue
                freq = pz.get("id-czestotliwosc")
                cands = candidate_periods(freq, entries, pmap)
                if not cands:
                    log(f"  var {var} sec {sec}: no calendar periods for freq "
                        f"{freq} ({pz.get('nazwa-czestotliwosc')}), skip")
                    done_sections.add(skey)
                    ck["done_sections"] = sorted(done_sections)
                    save_ckpt(ck)
                    continue
                yr = year_range(pz)
                log(f"  var {var} sec {sec} [{pz.get('nazwa-czestotliwosc')}, "
                    f"{pz.get('szereg-czasowy') or 'years unknown'}; "
                    f"{len(cands)} period(s)/yr] '{str(meta.get('nazwa',''))[:60]}'")
                try:
                    n_obs = fetch_section(aid, var, sec, yr, cands, pmap, ck, done_year_sections)
                except Exception as e:
                    log(f"  var {var} sec {sec} ERR: {e} — section retried next run")
                    area_complete = False
                    continue
                # Section fully fetched. Mark it done and prune its year-level
                # checkpoint entries (the year parts persist on disk until the
                # area merges). done_year_sections therefore holds only the year
                # keys of sections ATTEMPTED-but-incomplete in the current pass —
                # a failed section keeps its keys for resume and is pruned once it
                # later completes; bounded, and self-heals as sections finish.
                done_sections.add(skey)
                done_year_sections -= {k for k in done_year_sections
                                       if k.startswith(skey + ":y")}
                ck["done_sections"] = sorted(done_sections)
                ck["done_year_sections"] = sorted(done_year_sections)
                save_ckpt(ck)
                log(f"  var {var} sec {sec}: {n_obs:,} obs")

        # Finalize the area ONLY if every section completed. Otherwise leave the
        # section parts in place and do NOT mark the area done, so the next run
        # retries the failed sections (completed parts are skipped on restart).
        if not area_complete:
            all_complete = False
            kept = len(glob.glob(os.path.join(PARTS, f"area_{aid}", "*.parquet")))
            log(f"Area {aid}: INCOMPLETE — {kept} section part(s) kept, not merged; "
                f"failed sections will be retried next run.")
            continue

        # merge area parts → final parquet (atomic: tmp + os.replace, so a crash
        # mid-write can never leave a corrupt final that wedges the next run).
        part_files = sorted(glob.glob(os.path.join(PARTS, f"area_{aid}", "*.parquet")))
        if part_files:
            tbl = pa.concat_tables([pq.read_table(p) for p in part_files])
            tmp = final + ".tmp"
            try:
                pq.write_table(tbl, tmp, compression="zstd")
                os.replace(tmp, final)
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
            n = pq.read_metadata(final).num_rows
            total_obs += n
            log(f"Area {aid} merged: {n:,} obs from {len(part_files)} parts")
            for p in part_files:
                try:
                    os.remove(p)
                except OSError:
                    pass
        else:
            log(f"Area {aid}: 0 obs")
        done_areas.add(aid)
        ck["done_areas"] = sorted(done_areas)
        save_ckpt(ck)

    if all_complete:
        log(f"DONE: {total_obs:,} total GUS DBW observations — all areas complete")
        # Signal the watchdog to stop relaunching this job.
        try:
            done_path = os.path.join(ROOT, "logs", "gus_dbw.DONE")
            os.makedirs(os.path.dirname(done_path), exist_ok=True)
            with open(done_path, "w", encoding="utf-8") as fh:
                fh.write(f"{total_obs} obs at {dt.datetime.now().isoformat()}\n")
        except OSError:
            pass
    else:
        log(f"PASS COMPLETE WITH GAPS: {total_obs:,} obs so far; some areas had "
            f"transient failures and were left for the next run (no .DONE written).")


if __name__ == "__main__":
    main()
