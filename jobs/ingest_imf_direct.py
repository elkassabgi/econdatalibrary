#!/usr/bin/env python3
"""IMF — DIRECT from api.imf.org (SDMX 2.1), replacing the DBnomics relay.

WHY DIRECT: 37.4% of this library (635,750 series) arrives via DBnomics, an
aggregator. That makes our freshness a function of THEIR refresh cadence and puts a
third party between us and the source. Every IMF dataset we relay is published by
IMF itself at api.imf.org with no key required.

It is also, measurably, MORE data. Full-history comparison, 2026-07-28:

    flow      direct     ours   note
    FAS       27,603   13,960   direct has ~2x our series
    WORLD      3,245    2,268   +43%
    FDI        1,728    1,728   exact match, 192 countries both
    AFRREO     1,652    1,654   ~100%
    APDREO       250      265   94%
    COFER        140      154   91%
    WHDREO       287      322   89% of series but MORE countries (48 vs 37)
    MCDREO       623    1,095   57%  <- direct is SMALLER; do not switch blind
    FM           128    1,356   9%   <- direct is much smaller; investigate first

THE ENDPOINT THAT MATTERS: `api.imf.org/external/sdmx/2.1`. Two wrong turns cost
real time and are recorded so nobody repeats them:
  * `sdmxcentral.imf.org` is IMF's DATA-COLLECTION portal. It answers 200 and lists
    101 dataflows with internal ids (01R, BCG, BOP6) — none of the public datasets.
    It looks like success and returns the wrong catalogue.
  * `dataservices.imf.org` (the old SDMX_JSON host) does not connect at all.

AGENCY IDS ARE NOT UNIFORM — read them from the dataflow catalogue, never assume
IMF.STA. Guessing produced four spurious 404s in a first pass: FDI is IMF.MCM,
AFRREO IMF.AFR, MCDREO IMF.MCD, APDREO IMF.APD, FM/WORLD IMF.FAD.

KEY IDENTITY — the open question this script deliberately does NOT decide. Our
stored keys are legacy dotted IFS-style codes from DBnomics (`IMF_CPI:A.AE.PCPI_IX`)
while IMF's modern API uses named dimensions (COUNTRY, INDEX_TYPE, COICOP_1999,
TYPE_OF_TRANSFORMATION, FREQUENCY). IMF RETIRED IFS — there are zero IFS dataflows
in the public catalogue — so for restructured datasets there is no crosswalk to
recover the old identities, and a naive switch would both re-key every series and,
for CPI specifically, drop 41 countries and every annual frequency. This script
writes under its own source ids so the decision to retire the DBnomics-era series
stays a deliberate one.

Usage:
    python jobs/ingest_imf_direct.py --flow FDI --agency IMF.MCM
    python jobs/ingest_imf_direct.py --list
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "data", "clean_full")
BASE = "https://api.imf.org/external/sdmx/2.1"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
TIMEOUT = 300
RETRIES = 4

# Dimensions that describe PUBLICATION rather than identity. Including them in the
# series key would make the key churn whenever IMF re-tags a series, so they are
# dropped — but they are dropped by NAME, explicitly, never by position.
NON_IDENTITY = {
    "ACCESS_SHARING_LEVEL", "SECURITY_CLASSIFICATION", "OVERLAP", "SCALE",
    "DECIMALS_DISPLAYED", "COMMON_REFERENCE_PERIOD", "DERIVATION_TYPE",
}


def http_get(url: str) -> bytes:
    last = None
    for attempt in range(RETRIES):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                raise                                        # definitive: no such flow
            last = e
        except Exception as e:                               # noqa: BLE001
            # THE ORCHESTRATOR'S KILL MUST KILL. UnitTimeout is deliberately an Exception so
            # the UNIT handler demotes one source — but this broad retry caught it, slept,
            # and RETRIED: the SIGALRM's one shot consumed, the unit unbounded (PIP ran 80+
            # minutes past a 45-minute deadline before this was found). Name-based to avoid
            # an import cycle (orchestrate -> strategies -> _imf_direct -> this module).
            if type(e).__name__ == "UnitTimeout":
                raise
            last = e
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET failed after {RETRIES} tries: {url} ({last!r})")


def http_get_to_file(url: str, dest: str) -> int:
    """Stream a response to DEST in chunks. Returns bytes written.

    WHY NOT http_get(). It does `return r.read()`, materialising the whole body as one bytes
    object, and ET.fromstring then builds the whole DOM on top of that. Both break on the big
    flows: measured 2026-08-01, four of six GFS dataflows failed outright -

        GFS_BS, GFS_COFOG, GFS_SOO   OverflowError('size does not fit in an int')
        GFS_SFCP                     ParseError('out of memory: line 1, column 0')

    The OverflowError is the same int32 ceiling that keeps biting: a single Python bytes
    object cannot carry a >2 GiB read from this API, and expat runs out of memory building a
    DOM for a document that size. The two GFS flows that DID succeed were simply the small
    ones (GFS_SOEF 15,600 series; GFS_SSUC 102,961), which is what made the failure look
    source-specific rather than size-specific.

    Streaming to disk removes both ceilings: nothing larger than a chunk is ever in memory,
    and iterparse then walks the file without a DOM.
    """
    last = None
    for attempt in range(RETRIES):
        try:
            req = urllib.request.Request(url, headers=UA)
            n = 0
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r, open(dest, "wb") as fh:
                while True:
                    chunk = r.read(1 << 20)
                    if not chunk:
                        break
                    fh.write(chunk)
                    n += len(chunk)
            return n
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                raise                                        # definitive: no such flow
            last = e
        except Exception as e:                               # noqa: BLE001
            # THE ORCHESTRATOR'S KILL MUST KILL. UnitTimeout is deliberately an Exception so
            # the UNIT handler demotes one source — but this broad retry caught it, slept,
            # and RETRIED: the SIGALRM's one shot consumed, the unit unbounded (PIP ran 80+
            # minutes past a 45-minute deadline before this was found). Name-based to avoid
            # an import cycle (orchestrate -> strategies -> _imf_direct -> this module).
            if type(e).__name__ == "UnitTimeout":
                raise
            last = e
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET failed after {RETRIES} tries: {url} ({last!r})")


def iter_series(path: str):
    """Yield each <Series> element from an SDMX file, DETACHING it as we go.

    Two passes are cheap on a local file and the alternative is unsafe: identity dims are the
    UNION of attribute keys across every series, so deciding them from the first few would
    silently change the key shape whenever IMF reorders or adds an attribute.

    MEMORY DOES NOT STAY FLAT WITH `el.clear()` ALONE, which is what this used to do and what
    its docstring used to claim. clear() empties an element's own children, but the PARENT
    still holds a reference to the (now empty) element, so the tree grows by one node per
    series — 297,673 of them for GFS_BS — and nothing is ever freed. That is invisible on a
    workstation with 383 GB and fatal on a 16 GB CI runner, which is exactly the split
    observed: GFS_BS parses here at 2,293,565,648 bytes and produces 954,482 observations,
    while CI reported OverflowError('size does not fit in an int') for it and
    ParseError('out of memory') for GFS_SFCP. So the element is now REMOVED from its parent
    too, which is what actually bounds the memory.

    ElementTree has no getparent(), so the parent is tracked with an explicit stack over
    start/end events rather than by clearing the root — clearing the root would detach the
    in-progress <DataSet> and let its children accumulate on a node nothing ever clears again.
    """
    stack = []
    for ev, el in ET.iterparse(path, events=("start", "end")):
        if ev == "start":
            stack.append(el)
            continue
        stack.pop()
        if el.tag.split("}")[-1] != "Series":
            continue
        yield el
        el.clear()
        if stack:
            # Drop it from the parent as well; clear() alone leaves the husk attached.
            stack[-1].remove(el)


def list_flows() -> list[tuple[str, str, str]]:
    root = ET.fromstring(http_get(f"{BASE}/dataflow"))
    out = []
    for e in root.iter():
        if e.tag.split("}")[-1] == "Dataflow" and e.get("id"):
            name = ""
            for c in e:
                if c.tag.split("}")[-1] == "Name":
                    name = (c.text or "").strip()
                    break
            out.append((e.get("id"), e.get("agencyID") or "", name))
    return sorted(out)


def parse_period(p: str):
    """SDMX TIME_PERIOD -> date. Handles 2026, 2026-M01, 2026-Q1, 2026-01, 2026-01-31.

    Period-END convention, matching the rest of the store: an annual observation is
    stamped 12-31 so it sorts after that year's monthly points rather than before.
    """
    if not p:
        return None
    p = p.strip()
    try:
        if len(p) == 4:                                      # 2026
            return dt.date(int(p), 12, 31)
        if "-M" in p:                                        # 2026-M01
            y, m = p.split("-M")
            return _month_end(int(y), int(m))
        if "-Q" in p:                                        # 2026-Q1
            y, q = p.split("-Q")
            return _month_end(int(y), int(q) * 3)
        if "-S" in p:                                        # semester
            y, s = p.split("-S")
            return _month_end(int(y), int(s) * 6)
        if len(p) == 7 and "-" in p:                         # 2026-01
            y, m = p.split("-")
            return _month_end(int(y), int(m))
        if len(p) == 10:                                     # 2026-01-31
            return dt.date(int(p[:4]), int(p[5:7]), int(p[8:10]))
    except (ValueError, TypeError):
        return None
    return None


def _month_end(y: int, m: int) -> dt.date:
    if m >= 12:
        return dt.date(y, 12, 31)
    return dt.date(y, m + 1, 1) - dt.timedelta(days=1)


def pull(flow: str, agency: str, source_id: str, out_path: str | None = None,
         min_obs: int = 0, resume_token: str | None = None) -> int:
    """Fetch one dataflow and write it as parquet. Returns rows written, 0 on refusal.

    out_path lets the updater stage the pull somewhere distinct from the published
    file. Without it the fetcher would read and merge the SAME path, which happens
    to work only because the local filesystem and the R2 blob store share path
    strings — an accident, not a design, and one that breaks the moment either side
    changes its layout.

    min_obs is the floor below which a parsed response is treated as PARTIAL and
    refused (see the completeness gate below). Pass 0 on a first ingest, when there
    is nothing published to compare against.
    """
    url = f"{BASE}/data/{agency},{flow}/all"
    print(f"[imf_direct] GET {url}", flush=True)
    try:
        try:
            return _pull_streamed(url, flow, agency, source_id, out_path, min_obs)
        except Exception as e:                               # noqa: BLE001
            # A TIMEOUT IS NOT A SIZE ERROR. Falling through here on UnitTimeout turned the
            # orchestrator's 45-minute kill into the STARTING GUN for a full sliced re-pull —
            # the unit that was being stopped began its work again from zero.
            if type(e).__name__ == "UnitTimeout":
                raise
            # A BELT-AND-BRACES FALLBACK for the size errors CI reports —
            # OverflowError('size does not fit in an int') on GFS_BS, GFS_COFOG and GFS_SOO,
            # ParseError('out of memory') on GFS_SFCP.
            #
            # I FIRST READ THOSE AS A HARD 2 GiB PYEXPAT CEILING and wrote this as the primary
            # fix. Testing it disproved that: GFS_BS parses HERE at 2,293,565,648 bytes and
            # yields 954,482 observations, so there is no absolute document-size wall. The real
            # cause was the retained-node leak in iter_series (see its docstring) — invisible on
            # a 383 GB workstation, fatal on a 16 GB runner. That is now fixed, and the same
            # pull peaks at 137 MB.
            #
            # This path is kept anyway because it is free when it does not fire and cheap
            # insurance if a flow ever genuinely outgrows the parser. The whole-flow pull is
            # tried first and is untouched, so the flows that work today take exactly the path
            # they always did; only one that has already failed on size reaches the sliced one.
            if not _is_size_ceiling(e):
                raise
            print(f"[imf_direct] {flow}: {type(e).__name__}({str(e)[:60]}) — document exceeds "
                  f"pyexpat's 2 GiB ceiling; retrying as period slices", flush=True)
            return _pull_sliced(flow, agency, source_id, out_path, min_obs, resume_token)
    finally:
        # The staged SDMX document is multi-GB for the big flows; leaving one behind per
        # flow would fill the disk in a few runs. Removed on success, failure and exception
        # alike - the early-return paths inside cannot be relied on to cover all three.
        _tmp = (out_path or os.path.join(OUT, f"{source_id}.parquet")) + ".sdmx.tmp"
        try:
            if os.path.exists(_tmp):
                os.remove(_tmp)
        except OSError:
            pass


def _is_size_ceiling(e: BaseException) -> bool:
    """Is this pyexpat refusing an oversized document, rather than a real parse error?

    Matched on TYPE AND MESSAGE, not type alone: a genuine malformed-XML ParseError must keep
    propagating, because retrying it as slices would turn 'IMF served us broken XML' into a
    slow, silent, partial success. Only these two signatures mean 'too big'.
    """
    if isinstance(e, OverflowError):
        return "does not fit in an int" in str(e)
    if isinstance(e, ET.ParseError):
        return "out of memory" in str(e).lower()
    return False


def _slice_windows(first: int = 1950) -> list:
    """[(startPeriod, endPeriod), ...] decade windows from `first` to next year.

    Decades rather than years: GFS_BS is ~2 GiB whole, so a decade is comfortably inside the
    ceiling while keeping the request count (and therefore the rate-limit exposure) small. The
    window runs one year PAST today so a flow that has already published next year's forecast
    is not silently truncated.
    """
    end = dt.date.today().year + 1
    return [(str(y), str(min(y + 9, end))) for y in range(first, end + 1, 10)]


def _pull_sliced(flow: str, agency: str, source_id: str, out_path, min_obs: int,
                 resume_token: str | None = None) -> int:
    """Pull one flow as period slices and merge them, for documents past the 2 GiB ceiling.

    THE COMPLETENESS GATE MOVES TO THE TOTAL. _pull_streamed refuses any response carrying
    fewer than min_obs rows, which exists to stop IMF's occasional ~5%-of-the-data responses
    being recorded as a successful no-op. Applied per slice that gate would refuse every
    legitimate slice, so each slice is pulled with min_obs=0 and the floor is checked ONCE
    against the assembled total. A slice that is genuinely empty is normal — IMF publishes
    nothing for many flows before ~1990 — so an empty slice is counted and skipped, never
    treated as failure. A slice that RAISES still fails the run: a partial pull silently
    reported as complete is the exact lie this gate exists to prevent.
    """
    import pyarrow.parquet as _pq

    windows = _slice_windows()
    parts, empty, total = [], 0, 0
    base = out_path or os.path.join(OUT, f"{source_id}.parquet")

    # RESUME ACROSS THE ORCHESTRATOR'S KILL. The unit deadline is a SIGALRM
    # (orchestrate.py:158) that interrupts mid-pull on CI, and this function has no
    # try/finally, so a kill leaves the finished .sliceNN.parquet files on disk — and
    # every later run then re-pulled them from slice 0. A flow needing longer than the
    # 45-minute limit could therefore never converge: killed, restarted, killed again.
    # Reusing what is already on disk turns that into forward progress.
    #
    # GUARDED ON THE VINTAGE, because mixing slices from two IMF releases would
    # assemble a dataset that never existed. The sidecar records the token this slice
    # set was pulled under; a mismatch (or no token) wipes the set and starts clean.
    marker = f"{base}.slices.json"
    prior = None
    try:
        if os.path.exists(marker):
            prior = json.load(open(marker, encoding="utf-8"))
    except Exception:                                        # noqa: BLE001
        prior = None
    reusable = bool(prior) and prior.get("token") == resume_token and resume_token is not None
    if not reusable:
        stale = 0
        for i in range(len(windows) + 64):                   # +64: window count can shrink
            p = f"{base}.slice{i:02d}.parquet"
            if os.path.exists(p):
                try:
                    os.remove(p)
                    stale += 1
                except OSError:
                    pass
        if stale:
            print(f"[imf_direct] {flow}: discarded {stale} slice(s) from a different vintage "
                  f"({(prior or {}).get('token')!r} != {resume_token!r})", flush=True)
    try:
        json.dump({"token": resume_token, "windows": len(windows)},
                  open(marker, "w", encoding="utf-8"))
    except OSError:
        pass

    for i, (a, b) in enumerate(windows):
        url = f"{BASE}/data/{agency},{flow}/all?startPeriod={a}&endPeriod={b}"
        part = f"{base}.slice{i:02d}.parquet"
        if reusable and os.path.exists(part):
            try:
                have = _pq.read_metadata(part).num_rows
            except Exception:                                # noqa: BLE001
                have = 0                                     # truncated by the kill — re-pull
            if have:
                print(f"[imf_direct] slice {i + 1}/{len(windows)} {a}-{b} RESUMED "
                      f"({have:,} rows already on disk)", flush=True)
                parts.append(part)
                total += have
                continue
        print(f"[imf_direct] slice {i + 1}/{len(windows)} {a}-{b}", flush=True)
        n = _pull_streamed(url, flow, agency, source_id, part, 0)
        if not n:
            empty += 1
            continue
        parts.append(part)
        total += n
    if not parts:
        print(f"[imf_direct] FAIL {flow}: every one of {len(windows)} slices was empty",
              flush=True)
        return 0
    if total < min_obs:
        # The floor, checked once on the assembled total — see the docstring.
        print(f"[imf_direct] FAIL {flow}: sliced pull assembled {total:,} rows, under the "
              f"{min_obs:,} floor; refusing to publish a partial as complete", flush=True)
        for p in parts:
            try:
                os.remove(p)
            except OSError:
                pass
        return 0

    tbl = pa.concat_tables([_pq.read_table(p) for p in parts], promote_options="default")
    tbl = tbl.combine_chunks()
    _pq.write_table(tbl, base, compression="zstd")
    for p in parts:
        try:
            os.remove(p)
        except OSError:
            pass
    print(f"[imf_direct] {flow}: {len(parts)} slice(s) merged, {empty} empty, "
          f"{tbl.num_rows:,} rows -> {base}", flush=True)
    return tbl.num_rows


def _pull_streamed(url: str, flow: str, agency: str, source_id: str,
                   out_path, min_obs: int) -> int:
    xml_tmp = (out_path or os.path.join(OUT, f"{source_id}.parquet")) + ".sdmx.tmp"
    os.makedirs(os.path.dirname(xml_tmp), exist_ok=True)
    n_bytes = http_get_to_file(url, xml_tmp)
    print(f"[imf_direct] {n_bytes:,} bytes streamed to disk", flush=True)

    # PASS 1 - identity dims only. Attributes are read and the element dropped, so a
    # multi-GB document costs a few MB of memory here.
    n_series = 0
    dimset: set = set()
    for el in iter_series(xml_tmp):
        dimset |= set(el.attrib)
        n_series += 1
    if not n_series:
        # A 200 that parsed no series is a STRUCTURAL signal, not an empty dataset —
        # report it rather than writing an empty file over good data.
        print(f"[imf_direct] FAIL {flow}: 200 but ZERO series parsed "
              f"({n_bytes:,} bytes) — schema change or wrong flow id", flush=True)
        try:
            os.remove(xml_tmp)
        except OSError:
            pass
        return 0

    # Identity dimensions = whatever this flow actually declares, minus the
    # publication-metadata ones. Order is sorted for stability: IMF may reorder
    # attributes between releases and the key must not move when they do.
    dims = sorted(dimset - NON_IDENTITY)
    print(f"[imf_direct] {n_series:,} series; identity dims: {', '.join(dims)}",
          flush=True)

    # RECORD THE KEY ORDER. The key is positional and this order is the ONLY thing that says
    # which part means what. It cannot be recovered from the DSD afterwards: the order here is
    # alphabetical over the attributes the DATA actually carries, and the DSD both declares a
    # different order and omits attributes that appear in the data (METHODOLOGY is absent from
    # the GFS datastructure yet present in every GFS key). Reconstructing it from the DSD
    # shifts every part after the missing one, so `S1311B` - a SECTOR code - gets read as a
    # TYPE_OF_TRANSFORMATION and the resulting title is confidently wrong.
    #
    # Written next to the parquet so a catalogue/title builder can decode keys without
    # re-deriving a guess. Best-effort: a sidecar problem must never sink a good publish.
    try:
        # THE SAME PATH THE PARQUET ACTUALLY GOES TO. The published file is written to
        # OUT/<source_id>/<source_id>.parquet (see the write at the end of this function), but
        # this fallback said OUT/<source_id>.parquet - one directory too shallow. So the sidecar
        # landed where tools/imf_direct_titles.load_dims never looks, load_dims returned None,
        # and EVERY _direct source silently fell back to INFERRING the key order this file had
        # just recorded exactly. The comment above already claimed "written next to the
        # parquet"; it simply was not.
        #
        # It cost imf_bop_direct its titles. Inference demands that every key part resolve, and
        # BOP has 7 key parts against 5 codelisted dims (IFS_FLAG and METHODOLOGY are not DSD
        # Dimensions, so nothing can resolve BPM6), so no ordering fits and all 260,931 series
        # fell back to their raw key - while this sidecar held the right answer all along.
        dims_path = (out_path or os.path.join(OUT, source_id,
                                              f"{source_id}.parquet")) + ".dims.json"
        # THROUGH blob, NOT open(). The parquet is published via blob and lands in R2 under
        # AQUEDUCT_BACKEND=r2; a plain open() puts the sidecar on the runner's local disk, where
        # it dies with the container. blob.read_bytes' own docstring warns about precisely this
        # ("a plain open(path) sees nothing on a CI runner"). write_bytes_atomic mirrors local
        # AND R2, and makedirs the new subdirectory, which the bare open() would have needed
        # since the sidecar is written before the parquet creates that directory.
        from updater import blob as _blob
        _blob.write_bytes_atomic(dims_path, json.dumps(
            {"flow": flow, "agency": agency, "key_dims": dims}, indent=1).encode("utf-8"))
    except Exception as e:                                   # noqa: BLE001
        print(f"[imf_direct] WARNING: could not record key dims ({e!r}); a title "
              f"builder will have to re-derive them", flush=True)

    keys, dates, vals = [], [], []
    # THREE counters, not one. SDMX routinely declares period slots with no value,
    # so "empty" is normal and "unparseable" is a DEFECT — lumping them together
    # sends the next reader chasing a non-issue, or worse, teaches them to ignore a
    # real one. FAS reports 164,719 empty of 362,005 observations: all genuinely
    # blank in IMF's feed, zero non-numeric. That number is alarming until it is
    # broken down, and harmless once it is.
    n_empty = n_badval = n_baddate = 0
    for s in iter_series(xml_tmp):        # PASS 2 - observations, same streaming guarantee
        key = f"{flow}:" + ".".join((s.attrib.get(d) or "").replace(".", "_")
                                    for d in dims)
        for o in s:
            if o.tag.split("}")[-1] != "Obs":
                continue
            raw_p = o.attrib.get("TIME_PERIOD", "")
            v = o.attrib.get("OBS_VALUE")
            if v in (None, "", "NaN"):
                n_empty += 1                                 # normal: no value published
                continue
            d = parse_period(raw_p)
            if d is None:
                n_baddate += 1                               # DEFECT: unhandled period format
                continue
            try:
                fv = float(v)
            except ValueError:
                n_badval += 1                                # DEFECT: value we cannot read
                continue
            keys.append(key)
            dates.append(d)
            vals.append(fv)

    if not keys:
        print(f"[imf_direct] FAIL {flow}: series present but NO usable observations "
              f"(empty={n_empty:,} bad_date={n_baddate:,} bad_value={n_badval:,}) "
              f"— this is a DEFECT, not an empty dataset", flush=True)
        return 0
    if n_baddate or n_badval:
        # Loud and separate: an unhandled period format or unreadable value is a
        # parser bug that silently drops real data, and it must never hide inside
        # a benign "skipped" total.
        print(f"[imf_direct] WARNING {flow}: {n_baddate:,} obs with an unparseable "
              f"TIME_PERIOD and {n_badval:,} with an unreadable OBS_VALUE — these "
              f"are DROPPED REAL DATA, investigate the parser", flush=True)

    # COMPLETENESS GATE. Observed 2026-07-28 on IMF.RES,PCPS: a 200 returning a
    # complete, properly-closed document that declares every series and carries
    # almost none of the data — 1,264 series / 10,100 usable obs against the same
    # URL's full 1,270 / 221,749, i.e. 4.6%. Its trailing elements were bare
    # `<Series .../>` with no Obs children. It parses without error, so neither guard
    # above fires: series ARE present and the observations that exist ARE usable.
    # Without this check the pull writes 5% of the dataset and reports success.
    #
    # min_obs comes from the caller because only the caller knows what is PUBLISHED
    # (under the r2 backend that count lives in the blob store, not on this disk).
    # The threshold is deliberately loose: this catches the collapse class, not
    # ordinary revisions, and must never wedge the daily run when IMF legitimately
    # withdraws history. Returning 0 routes to the caller's existing structural path.
    if min_obs and len(keys) < min_obs:
        print(f"[imf_direct] FAIL {flow}: PARTIAL RESPONSE — {len(keys):,} usable obs "
              f"across {len(set(keys)):,} series, below the floor of {min_obs:,} "
              f"({n_bytes:,} bytes). The document is well-formed; the DATA is "
              f"missing. Refusing to write — existing rows are kept.", flush=True)
        return 0
    print(f"[imf_direct] {len(keys):,} usable obs / {len(set(keys)):,} series = "
          f"{len(keys) / max(len(set(keys)), 1):.1f} obs/series"
          + (f" (floor {min_obs:,})" if min_obs else " (no floor: nothing published "
             "yet to compare against)"), flush=True)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    d = os.path.join(OUT, source_id)
    os.makedirs(d, exist_ok=True)
    path = out_path or os.path.join(d, f"{source_id}.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, path)
    print(f"[imf_direct] wrote {tbl.num_rows:,} obs / "
          f"{len(set(keys)):,} series -> {path}"
          + (f"  (empty={n_empty:,})" if n_empty else ""),
          flush=True)
    return tbl.num_rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--list", action="store_true", help="list public dataflows and exit")
    ap.add_argument("--flow", help="dataflow id, e.g. FDI")
    ap.add_argument("--agency", help="agency id; looked up from the catalogue if omitted")
    ap.add_argument("--source-id", help="output source id (default imf_<flow>_direct)")
    a = ap.parse_args()

    if a.list:
        for fid, ag, nm in list_flows():
            print(f"  {fid:<32} {ag:<10} {nm[:56]}")
        return 0
    if not a.flow:
        ap.error("--flow is required (or --list)")

    agency = a.agency
    if not agency:
        match = [f for f in list_flows() if f[0].upper() == a.flow.upper()]
        if not match:
            print(f"no such dataflow: {a.flow}", file=sys.stderr)
            return 1
        agency = match[0][1]
        print(f"[imf_direct] agency resolved from catalogue: {agency}", flush=True)

    sid = a.source_id or f"imf_{a.flow.lower()}_direct"
    return 0 if pull(a.flow, agency, sid) else 1


if __name__ == "__main__":
    sys.exit(main())
