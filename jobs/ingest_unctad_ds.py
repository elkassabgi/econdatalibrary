#!/usr/bin/env python3
"""Generic UNCTADstat dataset ingest — the publisher's own documented data API.

One source per CURRENT dataset (successor family to the 38 retired DBnomics-era
unctad_* slugs; upstream re-coded all dataset ids, 0 of 38 match — see #70). The full
contract was read from the app's own generated code sample and PROVEN live 2026-08-07
(scratchpad unctad_auth_findings.md):

  catalogue+schema (keyless):
    GET https://unctadstat-api.unctad.org/api/datacenter/en            (99 datasets)
    GET https://unctadstat-api.unctad.org/api/reportMetadata/{DS}/en   (dims+measures+version)
  observations (keyed — UNCTAD_CLIENT_ID / UNCTAD_API_KEY from .env, NEVER in code):
    POST https://unctadstat-user-api.unctad.org/{DS}/cur/Facts?culture=en
    headers ClientId / ClientSecret; multipart form $select/$filter/$format=csv

Series identity: non-time dimension codes in (rowAxe, pageAxe) order + the measure code,
joined with '.', e.g. '0000.02.M0100' = World / exports / US$-current. obs_date from the
isTime dimension (Year -> Dec-31, matching the family's annual convention elsewhere).
One Facts POST per measure group, base (magnitude=1) variant only — the magnitude
variants are the same number scaled, not new data.

Licence: CC BY 3.0 IGO — verbatim audit §UNCTAD (DATABASE_LICENSES_VERBATIM.md:2501-2507),
CLEARED, re-host OK with attribution.

Run: python jobs/ingest_unctad_ds.py US.TradeMerchTotal [--dry]
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import re
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

META = "https://unctadstat-api.unctad.org/api"
FACTS = "https://unctadstat-user-api.unctad.org"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

SCHEMA = pa.schema([
    ("series_key", pa.string()), ("obs_date", pa.date32()), ("value", pa.float64()),
])


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def creds():
    # Environment first (CI secrets), .env fallback (workstation). CI has no .env file,
    # so an env-less lookup there must fail LOUDLY, not FileNotFoundError obscurely.
    cid = os.environ.get("UNCTAD_CLIENT_ID")
    key = os.environ.get("UNCTAD_API_KEY")
    if cid and key:
        return cid, key
    env_path = os.path.join(ROOT, ".env")
    if os.path.exists(env_path):
        out = {}
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("UNCTAD_") and "=" in line:
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip().strip('"').strip("'")
        cid = cid or out.get("UNCTAD_CLIENT_ID")
        key = key or out.get("UNCTAD_API_KEY")
    if not (cid and key):
        raise SystemExit("UNCTAD_CLIENT_ID / UNCTAD_API_KEY missing "
                         "(environment and .env both empty)")
    return cid, key


def report_metadata(ds_name: str) -> dict:
    r = requests.get(f"{META}/reportMetadata/{ds_name}/en", headers=UA, timeout=120)
    r.raise_for_status()
    return r.json()


# Mechanical slugs that would COLLIDE with a legacy DBnomics-era source id get an
# explicit override. R399: source_id_for("US.Cpi_A") produced "unctad_cpia" — the exact
# id of a legacy source with 637 live series — and the ingest silently overwrote its
# store (recovered exactly from the served CSVs). The guard in ingest() now refuses any
# id that already exists in the catalog's source table unless its homepage records THIS
# dataset, so a future collision fails loudly instead of clobbering.
SOURCE_ID_OVERRIDES = {
    "US.Cpi_A": "unctad_cpi_annual",
}


def source_id_for(ds_name: str) -> str:
    # US.TradeMerchTotal -> unctad_trademerchtotal (successor naming; legacy slugs retired)
    if ds_name in SOURCE_ID_OVERRIDES:
        return SOURCE_ID_OVERRIDES[ds_name]
    return "unctad_" + ds_name.split(".", 1)[1].replace("_", "").lower()


def assert_no_source_collision(src: str, ds_name: str) -> None:
    """Refuse to write under a source id that belongs to something else (R399)."""
    import sqlite3
    cat = os.path.join(ROOT, "data", "catalog.db")
    if not os.path.exists(cat):
        return
    con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True, timeout=60)
    row = con.execute("SELECT homepage FROM source WHERE source_id=?", (src,)).fetchone()
    con.close()
    if row and (not row[0] or f"/dataviewer/{ds_name}" not in row[0]):
        raise SystemExit(
            f"COLLISION: source id {src!r} already exists in the catalog and its homepage "
            f"({row[0]!r}) does not record dataset {ds_name!r}. Add an entry to "
            f"SOURCE_ID_OVERRIDES instead of overwriting a legacy source (R399).")


def parse_time(v: str, is_year: bool) -> dt.date | None:
    s = str(v).strip()
    try:
        if is_year or (len(s) == 4 and s.isdigit()):
            return dt.date(int(s), 12, 31)
        if len(s) == 10:
            return dt.date.fromisoformat(s)
        if len(s) == 7 and s[4] == "-":
            return dt.date(int(s[:4]), int(s[5:7]), 1)
        # Quarterly '2005Q01' (US.MerchVolumeQuarterly's Quarter axis, isTime=true).
        # Period-start like Q conventions elsewhere in the repo: Q1->Jan .. Q4->Oct.
        if len(s) == 7 and s[4] in "Qq" and s[:4].isdigit() and s[5:].isdigit():
            q = int(s[5:])
            if 1 <= q <= 4:
                return dt.date(int(s[:4]), (q - 1) * 3 + 1, 1)
        # Monthly '1995M01' (US.UCPI_M / CommodityPrice_M family's Period axis).
        if len(s) == 7 and s[4] in "Mm" and s[:4].isdigit() and s[5:].isdigit():
            m = int(s[5:])
            if 1 <= m <= 12:
                return dt.date(int(s[:4]), m, 1)
        # Semiannual '2018S01' (US.PortCallsArrivals_S). S1->Jan, S2->Jul (period-start).
        if len(s) == 7 and s[4] in "Ss" and s[:4].isdigit() and s[5:].isdigit():
            h = int(s[5:])
            if 1 <= h <= 2:
                return dt.date(int(s[:4]), (h - 1) * 6 + 1, 1)
    except ValueError:
        pass
    return None


def parse_period_code(v: str) -> tuple[dt.date, str | None] | None:
    """Period-coded time axes (isTime FALSE, field 'Year', string codetype).

    MEASURED on US.TradeMerchGR (52 codes): TWO families share the axis —
      annual YoY   '19801981' (consecutive years, label '1981')  -> obs at end-year
      multi-year   '19921995', '19952000' (span averages)        -> obs at end-year
    and their END-YEARS COLLIDE (7 duplicates: an annual '19941995' AND the span
    '19921995' both end 1995). So the span family carries a series-key suffix by span
    LENGTH — '|SPAN=5Y' — which makes the six 5-year averages ONE proper time series
    (obs at each span's end-year) instead of a flood of single-observation series,
    while the usda-style suffix still separates a period-average from the annual
    figure under one identity. Within a span length the end-year is unique by
    construction (same length + same end = same code), so no collisions.
    Returns (obs_date, suffix_or_None); None for an unparseable code.
    """
    s = str(v).strip()
    if len(s) == 8 and s.isdigit():
        y0, y1 = int(s[:4]), int(s[4:])
        if 1500 < y0 <= y1 < 2200:
            if y1 - y0 == 1:
                return dt.date(y1, 12, 31), None
            return dt.date(y1, 12, 31), f"|SPAN={y1 - y0}Y"
    if len(s) == 4 and s.isdigit():
        return dt.date(int(s), 12, 31), None
    return None


class FactsUnreachable(RuntimeError):
    """A Facts POST kept dying transiently (stream truncation / 5xx) after all
    retries. The chunked puller SPLITS the offending work item on this — a smaller
    response is likelier to complete before whatever kills the stream — and only a
    single-cell-set item re-raises."""


class FactsSizeCap(RuntimeError):
    """The API refuses requests estimated over ~62,500 cells with HTTP 400:
    'The request exceed the maximal size. Maximal size : 62500. Estimated size : N.'
    (measured on US.IntraTrade, estimate 6,988,144). Carries both numbers so the
    caller can compute how many time-chunks are needed."""

    def __init__(self, cap: int, estimated: int):
        super().__init__(f"Facts size cap {cap} < estimated {estimated}")
        self.cap, self.estimated = cap, estimated


def _post_with_deadline(url, headers, files, connect_timeout=20,
                        chunk_timeout=120, total_deadline=900):
    """POST with a hard TOTAL wall-clock deadline.

    requests' `timeout` bounds connect + BETWEEN-BYTES gaps only; a server (or
    broken NAT path) that drips a byte every few seconds evades it forever —
    measured 2026-08-12: one request at the M4023->M0100 measure boundary hung
    2h40m on a dead socket while an independent probe answered the identical
    leaf query in 0.3s. Streaming with a per-chunk timeout plus an explicit
    total deadline caps the worst case at one 15-minute attempt, which the
    caller's retry ladder re-issues on a fresh connection. Returns an object
    with .status_code and .text, like the plain call.
    """
    t0 = time.monotonic()
    with requests.post(url, headers=headers, files=files, stream=True,
                       timeout=(connect_timeout, chunk_timeout)) as r:
        chunks = []
        for chunk in r.iter_content(chunk_size=1 << 16):
            if chunk:
                chunks.append(chunk)
            if time.monotonic() - t0 > total_deadline:
                raise requests.RequestException(
                    f"total deadline {total_deadline}s exceeded "
                    f"({sum(len(c) for c in chunks)} bytes in)")
        body = b"".join(chunks)
        status, enc = r.status_code, (r.encoding or "utf-8")

    class _Resp:
        pass
    resp = _Resp()
    resp.status_code = status
    resp.text = body.decode(enc, "replace")
    return resp


def facts_csv(ds_name: str, select: str, cid: str, key: str, flt: str | None = None) -> str:
    form = {"$select": select, "$format": "csv"}
    if flt:
        form["$filter"] = flt
    files = {k: (None, v) for k, v in form.items()}
    for attempt in range(6):
        try:
            r = _post_with_deadline(f"{FACTS}/{ds_name}/cur/Facts?culture=en",
                                    headers={"ClientId": cid, "ClientSecret": key, **UA},
                                    files=files)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 502, 503, 504):
                time.sleep(20 * (attempt + 1)); continue
            if r.status_code == 401:
                # MEASURED 2026-08-10: a 401 killed the US.BiotradeMerch pull at hour
                # ~19 — and the SAME credentials returned 200 through this exact code
                # path minutes later. UNCTAD's gateway emits transient 401s; treating
                # them as instantly fatal converts a blip into a total-loss crash.
                # Retry with long backoff; only a PERSISTENT 401 (all attempts) is a
                # real revocation and falls through to FactsUnreachable.
                log(f"  Facts HTTP 401 (transient gateway auth blip); "
                    f"retry {attempt + 1} in {60 * (attempt + 1)}s")
                time.sleep(60 * (attempt + 1)); continue
            if r.status_code == 400 and "maximal size" in r.text.lower():
                m = re.search(r"Maximal size\s*:\s*(\d+).*?Estimated size\s*:\s*(\d+)",
                              r.text, re.S)
                if m:
                    raise FactsSizeCap(int(m.group(1)), int(m.group(2)))
            raise RuntimeError(f"Facts HTTP {r.status_code}: {r.text[:300]}")
        except requests.RequestException as e:
            log(f"  transient {type(e).__name__}; retry {attempt + 1}")
            time.sleep(30 * (attempt + 1))
    raise FactsUnreachable(f"Facts unreachable for {ds_name} after retries")


def dim_codes(ds_name: str, version, dim_table: str) -> list[str]:
    """Keyless dimension table -> ordered code list (for chunked Facts pulls)."""
    r = requests.get(f"{META.replace('/api', '')}/datamart-api/{ds_name}/{version}/"
                     f"{dim_table}?$orderby=Order&culture=en", headers=UA, timeout=120)
    r.raise_for_status()
    body = r.json()
    vals = body.get("value", body) if isinstance(body, dict) else body
    return [x["Code"] for x in vals if x.get("Code") is not None]


def facts_csv_chunked(ds_name: str, select: str, cid: str, key: str, meta: dict,
                      tdim: dict, progress=None) -> list[str]:
    """Full-dataset pull that respects the size cap: try one POST; on FactsSizeCap,
    partition the TIME dimension's codes into groups sized from the error's own
    numbers (cap/estimated, 15% headroom) and pull per group; a group that still
    caps is split in half recursively (down to single codes)."""
    try:
        return [facts_csv(ds_name, select, cid, key)]
    except FactsSizeCap as e:
        codes = dim_codes(ds_name, meta.get("version"), tdim["name"])
        if not codes:
            raise
        frac = max(1e-6, e.cap / e.estimated * 0.85)
        per = max(1, int(len(codes) * frac))
        if progress:
            progress(f"  size cap {e.cap:,} < est {e.estimated:,}: chunking "
                     f"{len(codes)} {tdim['name']} codes, ~{per}/chunk")
        numeric = (tdim.get("codetype") == "number")
        tfield = tdim["field"]

        def flt_for(group):
            if numeric:
                return f"{tfield} in ({','.join(str(c) for c in group)})"
            return f"{tfield}/Code in ({','.join(repr(str(c)) for c in group)})"

        # Multi-level split: on datasets with partner/product dimensions ONE time code
        # alone can exceed the cap (measured: US.IntraTrade, 225,424 cells in a single
        # year vs the 62,500 cap) — and on the densest (US.NonPlasticSubstsTradeByPartner,
        # est 167,478 for one Economy x one Year) even one time code x one first-dim code
        # still caps, so the split recurses across EVERY key dim in axis order before
        # giving up. Work items are (time_group, [dim_codes_group per engaged kdim]).
        kdims = [d for axe in ("rowAxe", "colAxe", "pageAxe")
                 for d in (meta["defaults"].get(axe) or [])
                 if not (bool(d.get("isTime"))
                         or d.get("field", "").lower() in ("year", "period"))]
        kdim_codes: dict[int, list] = {}   # kdim index -> full code list (lazy)

        def flt_for_item(tgroup, restr):
            f = flt_for(tgroup)
            for di, dgroup in enumerate(restr):
                f += (f" and {kdims[di]['field']}/Code in "
                      f"({','.join(repr(str(c)) for c in dgroup)})")
            return f

        # CHUNK SPILL + RESUME (added 2026-08-10). The US.BiotradeMerch pull died on
        # a transient 401 at hour ~19 with EVERYTHING in memory — total loss, because
        # this loop held every chunk's CSV in `out` until the final write. Each
        # completed chunk now spills to disk keyed by a hash of its (tgroup, restr)
        # work item; a restart under the SAME dataset version replays the identical
        # deterministic split walk and reloads finished chunks instead of refetching.
        # Version-gated exactly like the IMF sliced resume (tests/test_imf_sliced_resume):
        # a new UNCTAD release wipes the spills — two vintages must never be assembled.
        import hashlib
        import shutil as _sh
        spill_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                                 "data", "_unctad_spill", source_id_for(ds_name))
        spill_dir = os.path.abspath(spill_dir)
        token = f"{meta.get('version')}"
        token_path = os.path.join(spill_dir, "_token.txt")
        if os.path.isdir(spill_dir):
            old = (open(token_path, encoding="utf-8").read().strip()
                   if os.path.exists(token_path) else None)
            if old != token:
                _sh.rmtree(spill_dir, ignore_errors=True)
        os.makedirs(spill_dir, exist_ok=True)
        with open(token_path, "w", encoding="utf-8") as fh:
            fh.write(token)

        reused = 0
        out: list[str] = []
        stack = [(codes[i:i + per], []) for i in range(0, len(codes), per)]
        while stack:
            tgroup, restr = stack.pop(0)
            # The MEASURE must be part of the cache key. The select string embeds
            # M<code>; without it, measure #2's walk cache-hits measure #1's leaf
            # files and the row parser (which reads M<code>_Value) silently skips
            # every row in them — a measure that quietly assembles as EMPTY.
            # Caught 2026-08-12 on US.BiotradeMerch before any store was written.
            # Cost of the change: spills written before it (M4023's 199,981) no
            # longer cache-hit a resume — acceptable; that measure's store is
            # already assembled by tools/_merge_unctad_spills.py, which keys rows
            # by each file's own header and is unaffected by file names.
            ck = hashlib.sha1(repr((select, tgroup, restr)).encode()).hexdigest()[:16]
            sp = os.path.join(spill_dir, ck + ".csv")
            if os.path.exists(sp):
                try:
                    out.append(open(sp, encoding="utf-8").read())
                    reused += 1
                    continue
                except OSError:
                    pass  # unreadable spill -> refetch it
            try:
                text = facts_csv(ds_name, select, cid, key,
                                 flt=flt_for_item(tgroup, restr))
                with open(sp + ".tmp", "w", encoding="utf-8") as fh:
                    fh.write(text)
                os.replace(sp + ".tmp", sp)
                out.append(text)
            except (FactsSizeCap, FactsUnreachable) as split_err:
                atomic = (len(tgroup) == 1
                          and all(len(g) == 1 for g in restr)
                          and len(restr) >= len(kdims))
                if isinstance(split_err, FactsUnreachable) and atomic:
                    raise    # smallest possible request still dies — real outage
                # fall through to the split ladder below (identical for both causes)
                if len(tgroup) > 1:
                    mid = len(tgroup) // 2
                    stack[:0] = [(tgroup[:mid], restr), (tgroup[mid:], restr)]
                    continue
                # halve the deepest engaged dim group that can still split
                for di in range(len(restr) - 1, -1, -1):
                    if len(restr[di]) > 1:
                        mid = len(restr[di]) // 2
                        a = restr[:di] + [restr[di][:mid]] + restr[di + 1:]
                        b = restr[:di] + [restr[di][mid:]] + restr[di + 1:]
                        stack[:0] = [(tgroup, a), (tgroup, b)]
                        break
                else:
                    # every engaged dim is a single code — engage the NEXT key dim
                    nxt = len(restr)
                    if nxt >= len(kdims):
                        raise   # atomic cell set still too big — new layout class
                    if nxt not in kdim_codes:
                        kdim_codes[nxt] = dim_codes(ds_name, meta.get("version"),
                                                    kdims[nxt]["name"])
                        if progress:
                            progress(f"  still caps at single codes: also splitting "
                                     f"by {kdims[nxt]['name']} "
                                     f"({len(kdim_codes[nxt])} codes)")
                    full = kdim_codes[nxt]
                    mid = max(1, len(full) // 2)
                    stack[:0] = [(tgroup, restr + [full[:mid]]),
                                 (tgroup, restr + [full[mid:]])]
        return out


class UnsupportedLayout(RuntimeError):
    """A dataset shape this generic machinery has not been taught. Callers decide
    whether that is fatal (ingest) or a structural unit-failure (fetcher)."""


def dataset_layout(meta: dict):
    """(kfields, tfield, is_year, period_axis, measures) from reportMetadata.

    Non-time dims in (rowAxe, colAxe, pageAxe) order form the series key; the single
    time axis is the observation axis. isTime is authoritative when SET — but
    PERIOD-CODED axes (growth-rate datasets) report isTime FALSE while still being
    the time axis (measured: US.TradeMerchGR's colAxe is name=Periods, field=Year,
    isTime=false, codes like '19921995'); the field name 'Year' is the API's own
    time marker in that layout. Measures: the magnitude-1 base per observation group
    (the variants are the same number scaled).
    """
    defaults = meta["defaults"]
    dims = [d for axe in ("rowAxe", "colAxe", "pageAxe") for d in defaults.get(axe) or []]

    def _is_time(d):
        # UNCTAD marks most time axes isTime; some layouts instead carry a bare
        # 'Year' field, and the growth-rate datasets (CreativeGoodsGR) a 'Period'
        # field whose codes are the 8-digit YYYYYYYY spans parse_period_code
        # already handles. A non-time dim named Period would fail loudly at parse.
        return (bool(d.get("isTime"))
                or d.get("field", "").lower() in ("year", "period"))

    time_dims = [d for d in dims if _is_time(d)]
    key_dims = [d for d in dims if not _is_time(d)]
    if len(time_dims) != 1:
        raise UnsupportedLayout(f"{meta.get('name')}: {len(time_dims)} time dims")
    tdim = time_dims[0]
    measures = []
    for grp in defaults.get("observations") or []:
        base = next((m for m in grp.get("measures", []) if m.get("magnitude") == 1), None)
        if base:
            measures.append(base["code"])
    if not measures:
        raise UnsupportedLayout(f"{meta.get('name')}: no magnitude-1 measures")
    return ([d["field"] for d in key_dims], tdim["field"],
            bool(tdim.get("isTime")) and tdim["field"].lower() == "year",
            not tdim.get("isTime"), measures)


def dataset_time_dim(meta: dict) -> dict:
    """The raw time-axis dict (name/field/codetype) — needed for chunked pulls.
    Same predicate as dataset_layout's _is_time (keep the two in lockstep —
    the CreativeGoodsGR retry failed here after only _is_time learned 'period')."""
    defaults = meta["defaults"]
    dims = [d for axe in ("rowAxe", "colAxe", "pageAxe") for d in defaults.get(axe) or []]
    for d in dims:
        if bool(d.get("isTime")) or d.get("field", "").lower() in ("year", "period"):
            return d
    raise UnsupportedLayout(f"{meta.get('name')}: no time dim")


def pull_rows(ds_name: str, cid: str, key: str, meta: dict, progress=None,
              measures_filter: list[str] | None = None):
    """Fetch + parse ALL observations for one dataset. THE single row-building path —
    the ingest below and every fetcher call this, so the parse rules cannot drift
    between the two (the insee_bdm/eurostat parity lesson, R-ledger 2026-08-08).
    Returns (rows_k, rows_d, rows_v).

    measures_filter: restrict the pull to these measure codes (e.g. ["M0100"]).
    Used to resume ONE measure's campaign when the others are already complete
    in the spill cache (US.BiotradeMerch 2026-08-12: M4023 done, M0100 unpulled)
    — the spills persist either way and tools/_merge_unctad_spills.py assembles
    the full store from all measures once each campaign has run."""
    kfields, tfield, is_year, period_axis, measures = dataset_layout(meta)
    if measures_filter:
        keep = {m[1:] if m.upper().startswith("M") else m for m in measures_filter}
        measures = [m for m in measures if m in keep]
        if not measures:
            raise SystemExit(f"--measures matched nothing; dataset has {keep} vs "
                             f"{dataset_layout(meta)[4]}")
    tdim = dataset_time_dim(meta)
    rows_k, rows_d, rows_v = [], [], []
    for mcode in measures:
        select = ", ".join(f"{f}/Code" for f in kfields) + f", {tfield}, M{mcode}/Value"
        texts = facts_csv_chunked(ds_name, select, cid, key, meta, tdim, progress=progress)
        n0 = len(rows_k)
        for rec in (rec for text in texts for rec in csv.DictReader(io.StringIO(text))):
            vals = [rec.get(f"{f}_Code", "") for f in kfields]
            tv = rec.get(tfield) or rec.get(f"{tfield}_Code", "")
            vv = rec.get(f"M{mcode}_Value", "")
            suffix = None
            if period_axis:
                parsed = parse_period_code(tv)
                if parsed is None:
                    continue
                d, suffix = parsed
            else:
                d = parse_time(tv, is_year)
            if d is None or vv in ("", None):
                continue
            try:
                v = float(vv)
            except ValueError:
                continue
            rows_k.append(".".join(vals + [f"M{mcode}"]) + (suffix or ""))
            rows_d.append(d)
            rows_v.append(v)
        if progress:
            progress(f"  M{mcode}: {len(rows_k) - n0:,} obs")
    return rows_k, rows_d, rows_v


def ingest(ds_name: str, dry: bool, measures_only: list[str] | None = None) -> int:
    if measures_only and not dry:
        # A one-measure pull is a CACHE-FILLING campaign, never a store write: the
        # ingest's own write would replace the canonical parquet with a partial-
        # measure store. Assemble via tools/_merge_unctad_spills.py instead.
        raise SystemExit("--measures pulls a subset; it must run with --dry "
                         "(spills persist; merge with tools/_merge_unctad_spills.py)")
    cid, key = creds()
    meta = report_metadata(ds_name)
    src = source_id_for(ds_name)
    assert_no_source_collision(src, ds_name)
    out_dir = os.path.join(ROOT, "data", "clean_full", src)

    try:
        kfields, tfield, _, _, measures = dataset_layout(meta)
    except UnsupportedLayout as e:
        raise SystemExit(f"{e} — teach the job this layout")
    log(f"{ds_name} -> {src}: key dims {kfields}, time {tfield}, "
        f"measures {['M' + c for c in measures]}, version {meta.get('version')}")

    rows_k, rows_d, rows_v = pull_rows(ds_name, cid, key, meta, progress=log,
                                        measures_filter=measures_only)

    if not rows_k:
        log(f"  {ds_name}: 0 obs — refusing to write an empty store")
        return 0
    n_series = len(set(rows_k))
    if dry:
        log(f"  DRY: {len(rows_k):,} obs / {n_series:,} series")
        return len(rows_k)
    os.makedirs(out_dir, exist_ok=True)
    tbl = pa.table({"series_key": pa.array(rows_k, pa.string()),
                    "obs_date": pa.array(rows_d, pa.date32()),
                    "value": pa.array(rows_v, pa.float64())}, schema=SCHEMA)
    path = os.path.join(out_dir, f"{src}.parquet")
    pq.write_table(tbl, path, compression="zstd")
    n = pq.read_metadata(path).num_rows
    log(f"  WROTE {path}: {n:,} obs / {n_series:,} series")
    return n


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        raise SystemExit("usage: ingest_unctad_ds.py <US.DatasetName> [--dry]")
    measures_only = None
    for a in sys.argv[1:]:
        if a.startswith("--measures="):
            measures_only = a.split("=", 1)[1].split(",")
    ingest(args[0], "--dry" in sys.argv, measures_only)
