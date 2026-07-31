"""S3 fetcher — INSEE BDM (Banque de Donnees Macroeconomiques). SDMX 2.1, no key.

Layout (set by jobs/ingest_insee_bdm.py): ONE parquet per DATAFLOW under
clean_full/insee_bdm/<FLOW_ID>.parquet. Each file holds that flow's full long table:
  idbank   : the BDM series id (a flow contains many idbanks)
  obs_date : date32, parsed from SDMX TIME_PERIOD (M=YYYY-MM->1st, T=YYYY-Qn->1st of
             quarter, A=YYYY->Dec-31, daily=YYYY-MM-DD)
  value    : float64 (OBS_VALUE)
  dataflow : the flow id (constant within a file)
The natural identity within a flow is (idbank, obs_date) — that is the dedup key.

Incremental (sdmx_delta / date-tail). Each on-disk dataflow file is a SUB-UNIT:
  - read the file's max(obs_date) and request ONLY the year-tail from upstream via the
    SDMX 2.1 query  GET /data/<FLOW_ID>?startPeriod=<year-of-max>
    startPeriod with a bare year matches January of that year onward and works
    UNIFORMLY for monthly/quarterly/annual series (verified live), so we re-fetch at
    most ~1 year of overlap per flow — cheap — and, because it RE-fetches the boundary
    period, an in-place revision of the latest value is also captured (merge dedups the
    overlap, new row wins). No per-series FREQ detection is needed.
  - parse with the SAME logic as jobs/ingest_insee_bdm.py (Series IDBANK attr, Obs
    TIME_PERIOD / OBS_VALUE, parse_period) — enumeration + endpoints reused verbatim.
  - merge ONLY via merge.merge_and_write(path, tbl, mode="merge",
    dedup_keys=("idbank","obs_date")); never write parquet here.

INSEE BDM does NOT reliably honor ?updatedAfter (it returns the full series), so we do
not depend on it; the year-tail startPeriod is the cheap delta primitive.

HONEST-STATUS CONTRACT (Tally + finalize):
  Per dataflow sub-unit we record:
    added_unit(n)    rows merged for the flow (n>0 new, n==0 empty)
    empty_unit()     200 with a valid SDMX envelope but no new Obs in the tail window
                     (a legitimately-quiet flow), or no on-disk obs to anchor from
    transient_unit() timeout/5xx/429/network drop, OR a 200 whose body is not valid XML
                     (retry next run) -> the WHOLE run returns 'partial'
    structural_unit()200 whose body parses as XML but is NOT an SDMX data message (the
                     expected StructureSpecificData/GenericData root is gone), i.e. a
                     real structural break -> DefinitiveError in finalize()
  finalize() => 'ok'/'no_change' only when no sub-unit transient/structural-failed;
  'partial' on any transient (orchestrator does NOT stamp success, re-runs next tick);
  DefinitiveError on a structural break or a large all-empty/error window. merge always
  preserves existing data (never shrinks).
"""
from __future__ import annotations
import datetime as dt
import os
import re
import time
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize, TransientStreak

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/xml"}
BASE = "https://api.insee.fr/series/BDM/V1"
SOURCE = "insee_bdm"
DEDUP = ("idbank", "obs_date")
TIMEOUT = 120
MAX_ATTEMPTS = 4
RATE = 0.3  # polite pause between flows

# SDMX message roots that mean "this is a valid data document" (possibly with 0 Obs).
_SDMX_DATA_ROOTS = {"StructureSpecificData", "GenericData", "Data", "MessageGroup"}


# --------------------------------------------------------------------------- #
# parse helpers — mirror jobs/ingest_insee_bdm.py exactly
# --------------------------------------------------------------------------- #
def _parse_period(s):
    """SDMX TIME_PERIOD -> date. Identical mapping to the ingester."""
    s = (s or "").strip()
    try:
        if re.match(r'\d{4}-Q\d', s):
            y, q = s.split("-Q")
            return dt.date(int(y), (int(q) - 1) * 3 + 1, 1)
        if re.match(r'\d{4}-\d{2}$', s):
            return dt.date(int(s[:4]), int(s[5:7]), 1)
        if re.match(r'\d{4}$', s):
            return dt.date(int(s), 12, 31)
        if re.match(r'\d{4}-\d{2}-\d{2}', s):
            return dt.date.fromisoformat(s[:10])
    except (ValueError, KeyError):
        pass
    return None


def _local(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


# A 404 body containing any of these phrases means the BDM catalog no longer knows this
# dataflow/DSD — a permanent WITHDRAWAL/renumber (a real structural break for an on-disk
# flow), not a transient hiccup or an empty tail.
_WITHDRAWAL_404 = ("could not find dataflow", "could not find dsd",
                   "no dataflow", "no structure found", "unknown dataflow")


def _is_withdrawal_404(body: str) -> bool:
    """True if a 404 body indicates a catalog withdrawal (structural), not NoRecordsFound."""
    low = (body or "").lower()
    return any(phrase in low for phrase in _WITHDRAWAL_404)


def _get_xml(sess, url):
    """GET an SDMX document.

    Returns:
      ET.Element                 -> a parsed SDMX data message (root in _SDMX_DATA_ROOTS)
      ("STRUCTURAL", root_tag)   -> 200, parsed as XML, but NOT an SDMX data message
                                    (expected structure is gone) — caller -> structural
      ("NOT_FOUND", body)        -> HTTP 404; the (truncated) response body is returned so
                                    the caller can distinguish a catalog WITHDRAWAL
                                    ("Could not find Dataflow"/"DSD") on a previously-
                                    populated flow (structural) from a transient/empty 404.
                                    A 404 is NOT raised here: an on-disk flow that 404s for
                                    a routine NSO renumber/withdrawal must not pin the whole
                                    source at 'partial' forever (transient-retried each run).
    Raises:
      TransientError  -> timeout/5xx/429/network drop after the retry budget, OR a 200
                         whose body is not valid XML (retry next run)
      DefinitiveError -> hard 4xx other than 404/429
    """
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, headers=UA, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"insee_bdm GET {url[-70:]}: {last}")
            time.sleep(min(5 * (attempt + 1), 30))
            continue
        if r.status_code == 200:
            try:
                root = ET.fromstring(r.content)
            except ET.ParseError as e:
                # 200 but unparseable body — could be a transient gateway HTML page;
                # retry within budget, then surface as transient (not silent no_change).
                last = f"unparseable 200 body: {e}"
                if attempt == MAX_ATTEMPTS - 1:
                    raise TransientError(f"insee_bdm GET {url[-70:]}: {last}")
                time.sleep(min(5 * (attempt + 1), 30))
                continue
            if _local(root.tag) in _SDMX_DATA_ROOTS:
                return root
            # Parsed as XML but the expected SDMX data envelope is gone -> structural.
            return ("STRUCTURAL", _local(root.tag))
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"insee_bdm GET {url[-70:]}: {last}")
            time.sleep(min(5 * (attempt + 1), 30))
            continue
        if r.status_code == 404:
            # Return the body (truncated) so the caller can classify the 404 instead of
            # blindly retrying forever. A permanent catalog withdrawal/renumber on an
            # on-disk flow ("Could not find Dataflow"/"DSD") is a structural break that
            # must surface for human attention; a NoRecordsFound 404 is a quiet/empty
            # tail. Either way existing data is left untouched (never data loss).
            try:
                body = (r.text or "")[:500]
            except Exception:
                body = ""
            return ("NOT_FOUND", body)
        raise DefinitiveError(f"insee_bdm GET {url[-70:]}: HTTP {r.status_code}")
    raise TransientError(f"insee_bdm GET {url[-70:]}: {last}")


def _parse_obs(root, flow_id):
    """Extract (idbank, obs_date, value) triples from an SDMX data message.
    Returns (idbanks, dates, values, n_series). Same field handling as the ingester."""
    idbanks, dates, values = [], [], []
    n_series = 0
    for elem in root.iter():
        if _local(elem.tag) == "Series":
            idbank = elem.get("IDBANK", "")
            if not idbank:
                continue
            n_series += 1
            for obs in elem:
                if _local(obs.tag) == "Obs":
                    d = _parse_period(obs.get("TIME_PERIOD", ""))
                    ov = obs.get("OBS_VALUE", "")
                    if d and ov not in ("", None):
                        try:
                            v = float(ov)
                        except (ValueError, TypeError):
                            continue
                        idbanks.append(idbank)
                        dates.append(d)
                        values.append(v)
    return idbanks, dates, values, n_series


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)

    # Sub-units = the dataflow parquet files we maintain (one per flow). This reuses the
    # ingester's enumeration result on disk; the 42 BDM flows that legitimately carry no
    # observations have no file and so nothing to refresh. Enumeration is blob-routed so
    # the flow set is visible under AQUEDUCT_BACKEND=r2.
    pfiles = blob.list_parquets(out_dir)
    if not pfiles:
        raise DefinitiveError(f"no insee_bdm parquet files under {out_dir}")

    sess = requests.Session()
    tally = Tally()
    total = 0
    maxd = None
    frontier = None                # max on-disk boundary across flows (last_obs on a fully-quiet run)
    changed: dict[str, str] = {}   # IDBANK -> max obs_date, ONLY for flows that gained net-new rows.
    # Keyed by idbank, NOT flow_id: the catalog series_id is 'insee_bdm:<idbank>', so a flow-keyed
    # cursor never maps and the CSV-coherence gate demotes every run to partial (101,768 series >
    # the 5,000 derive-all cap, so derive-all can't rescue it). (verified: csv_coherence diag)

    # Stop early if INSEE is refusing everything. On 2026-07-31 all 201 flows
    # transient-failed and this loop ground through every one with its own retry budget:
    # 104.5 min to conclude the upstream was throttling, versus 11.1 min for a healthy run.
    # Probed afterwards the API answered 200 in 0.6s, so it was temporary — the case where
    # spending an hour proving it is worst, and where continuing to hammer a rate limit is
    # actively harmful.
    streak = TransientStreak()

    for fn in pfiles:
        flow_id = fn[:-len(".parquet")]
        if streak.tripped:
            # Not attempted, not "failed on its own merits" — recorded transient so the run
            # is partial, the vintage does not advance, and it retries.
            tally.transient_unit(f"{flow_id} not attempted (upstream refusing)")
            total += blob.row_count(os.path.join(out_dir, fn))
            continue
        path = os.path.join(out_dir, fn)
        before = blob.row_count(path)

        # Learn this flow's frontier from its own parquet.
        existing_max = None
        try:
            od = blob.read_table(path, columns=["obs_date"]).column("obs_date")
            mx = pc.max(od).as_py() if od.length() else None
            if isinstance(mx, dt.datetime):
                mx = mx.date()
            existing_max = mx
        except Exception:
            existing_max = None
        if existing_max is not None:
            iso = existing_max.isoformat()
            if frontier is None or iso > frontier:
                frontier = iso

        # Year-tail startPeriod: re-fetch from January of the boundary year forward
        # (uniform across M/Q/A; merge dedups the overlap and captures revisions).
        if existing_max is not None:
            start_year = existing_max.year
        elif since:
            try:
                start_year = dt.date.fromisoformat(since).year
            except ValueError:
                start_year = None
        else:
            start_year = None  # no anchor -> request whole flow (first-time backfill)

        url = f"{BASE}/data/{flow_id}"
        if start_year is not None:
            url += f"?startPeriod={start_year}"

        try:
            root = _get_xml(sess, url)
        except TransientError:
            # Leave this flow's existing data untouched; record & keep going so one flaky
            # flow can't strand the other 200. -> run becomes 'partial'.
            tally.transient_unit()
            total += before
            if streak.fail():
                print(f"[insee_bdm] {streak.limit} flows transient-failed in a row — the "
                      f"upstream is refusing; not attempting the rest this run", flush=True)
            time.sleep(RATE)
            continue
        streak.ok()

        if isinstance(root, tuple) and root[0] == "STRUCTURAL":
            # 200 parsed as XML but the SDMX data envelope is gone (root=<root[1]>).
            tally.structural_unit()
            total += before
            time.sleep(RATE)
            continue

        if isinstance(root, tuple) and root[0] == "NOT_FOUND":
            body = root[1]
            if existing_max is not None and _is_withdrawal_404(body):
                # A flow that previously carried data but is now WITHDRAWN/renumbered in
                # the BDM catalog ("Could not find Dataflow"/"DSD") is a real structural
                # break — surface it for human attention (finalize -> DefinitiveError),
                # not a transient retried forever. Existing on-disk data is kept.
                tally.structural_unit()
            else:
                # A NoRecordsFound 404 (or a 404 on a flow with no on-disk anchor) is a
                # quiet/empty tail, not a break: record empty so the source can still
                # resolve to ok/no_change. Never data loss (file is untouched).
                tally.empty_unit()
            total += before
            time.sleep(RATE)
            continue

        idbanks, dates, values, n_series = _parse_obs(root, flow_id)

        if not idbanks:
            # No Obs parsed from the tail window. Distinguish two cases (pattern B2):
            #  - n_series > 0 on an ON-DISK flow: the 200 envelope CARRIED <Series>
            #    elements but they yielded 0 observations. startPeriod is INCLUSIVE, so a
            #    healthy active flow MUST re-return >=1 boundary Obs; 0 parsed from a
            #    non-empty envelope is a real parser/schema break -> structural.
            #  - otherwise (n_series == 0, or no on-disk anchor): a genuinely empty SDMX
            #    envelope — a legitimately quiet flow, not a break -> empty.
            if n_series > 0 and existing_max is not None:
                tally.structural_unit()
            else:
                tally.empty_unit()
            total += before
            time.sleep(RATE)
            continue

        new_tbl = pa.table({
            "idbank": pa.array(idbanks, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(values, pa.float64()),
            "dataflow": pa.array([flow_id] * len(idbanks), pa.string()),
        })

        n, md = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
        total += n
        # A flow that returned real Obs from a 200 is a SUCCESSFUL sub-unit (data
        # flowed), even when the year-tail overlaps fully and the merge nets zero new
        # rows. Mark it added_unit(len(rows)) so it does NOT feed the all-empty
        # structural floor — otherwise a healthy quiet day (every active flow re-returns
        # its boundary year, zero net-new) would have empty==attempted and wrongly raise
        # DefinitiveError. The real net-new delta is reflected in `obs` / the note.
        tally.added_unit(len(idbanks))
        if md:
            if maxd is None or md > maxd:
                maxd = md
            if n > 0:
                # Net-new rows landed -> this flow's series changed. Report each returned
                # series by IDBANK (dict assignment dedups) so the coherence gate maps them
                # to 'insee_bdm:<idbank>' and re-derives their CSVs. Bounded to the flows that
                # actually gained data (n>0), not every active flow, so the changed-set stays
                # small; pure value revisions with no new row are picked up on the next run.
                for ib in idbanks:
                    changed[ib] = md
        time.sleep(RATE)

    if maxd is None:
        # Fully-quiet run (no flow merged): report the max on-disk boundary as last_obs.
        maxd = frontier

    # empty_window_floor = (#sub-units) - 1, per the contract: only a near-total
    # all-empty window trips the wholesale-break floor; precise structural breaks are
    # already caught per-flow via tally.structural_unit().
    return finalize(tally, total, maxd, source=SOURCE, series_cursors=changed,
                    empty_window_floor=len(pfiles) - 1)
