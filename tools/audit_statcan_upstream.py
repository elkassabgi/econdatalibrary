"""Is a statcan cube's STORE behind its PUBLISHER?  A read-only, cube-level detector.

WHY THIS EXISTS (content audit, 2026-11-19). Two classes of statcan cube sat behind StatCan with
nothing alarming:

  * the six CIMT cubes 12100147-12100152. StatCan's full-table zip for them has not been rebuilt
    since 2023-11-09 (HEAD Last-Modified) while getCubeMetadata says the cube runs to 2026-09-01.
    The lane downloads the zip, parses it, merges a delta of 0 and logs "merged" - a true log line
    about a file that is three years old;
  * cubes released BEFORE the lane's floor (2026-07-29). The lane never owes them, so a store that
    ends one period before the publisher's is invisible to it.

Both are the same question asked without the lane's assumptions: does the period the store reaches
equal the period the publisher's cube list says the cube reaches? The release date is deliberately
NOT an input - the floor is what hid the second class.

WHAT IT READS (and nothing else; it writes only its own TSV):
  1. ONE GET of WDS getAllCubesListLite (productId, cubeEndDate, releaseTime, frequencyCode,
     archived), or --cubes-json for a saved copy of that response;
  2. the catalogued statcan cube ids, from --catalog (opened read-only through
     core.catalog_path.connect_path) or --pids;
  3. for each cube, ONLY the parquet footer of clean_full/statcan/<pid>.parquet, through
     updater.blob._r2_footer_buffer (ranged tail GETs - the reader PR #305 added; no new R2
     reader here). The store's last date is the max of the obs_date row-group statistics.
  4. with --probe-zip, for BEHIND cubes only: WDS getFullTableDownloadCSV (the zip URL) and a HEAD
     of that URL, for its Last-Modified.
It never imports jobs/statcan_lane.py, never takes the lane's lock, and has no write path to R2, D1,
the updater state or the catalogue. On R2 a botocore before-call hook, registered first on the client
this tool uses, refuses every operation except GetObject and HeadObject.

SCOPE: CUBE LEVEL ONLY. "current" here means the cube's newest stored date reaches the publisher's
cube end period. It cannot see members inside a cube that stopped early (24100057 holds members
ending 2018-10 in a cube that runs to 2026-10), and the row-group max is an upper bound: one stored
date beyond the publisher's end hides a BEHIND. That second case is reported as AHEAD, which is an
anomaly to look at, not a pass.

THE PERIOD COMPARISON. Measured on the 2026-11-19 cube list: for every month-step frequency
(monthly, quarterly, annual, occasional, every-N-years ...) cubeEndDate is the FIRST day of the last
period (annual 2025-01-01 = the year 2025); weekly cubes are dated by the week's end date. The store
dates some periods by their end (annual 2024 = 2024-12-31) and some by their start. Either way a
store date lies INSIDE the period it stands for, so:

    BEHIND  <=>  store_max < publisher_end          (the store's last period starts before the
                                                      publisher's last period)

For month-step frequencies a cubeEndDate not on day 01 breaks that reading and is UNMEASURED. lag is
reported for information: the number of frequency steps (calendar steps - an "occasional" cube that
misses one release can show more than 1).

EXIT CODES. An inability to measure is a failure, never a pass:
  0  every catalogued cube was measured (and with --probe-zip every probe answered);
  2  some cubes (or probes) could not be measured - the summary says "at least N" and lists them;
  1  nothing usable: the cube list or the catalogue could not be read, the cube list fails its sanity
     check, no cube is catalogued, or no cube at all could be measured.
rc 0 does NOT mean "nothing behind": BEHIND cubes are a measured finding, not a failure to measure.
Anything that schedules this tool must alarm on the behind count in the summary line (or the TSV),
never on rc alone - an rc-only alarm would stay silent about exactly what this tool exists to show.

WEEKLY CUBES. The predicate assumes the store and the publisher date a week the same way. Measured
2026-11-19 on the 18 non-archived weekly cubes: all 13 current ones hold store_max == cubeEndDate
exactly, and the 5 behind ones are 9 to 18 weeks behind - no one-week artefact of a start-dated store.

Usage:
  python tools/audit_statcan_upstream.py --catalog F:/copy/catalog.db --out F:/x/statcan_upstream.tsv
  python tools/audit_statcan_upstream.py --pids 12100147,35100182 --probe-zip --out x.tsv
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

WDS = "https://www150.statcan.gc.ca/t1/wds/rest"
UA = {"User-Agent": "econdatalibrary-audit/1 (read-only)"}
KEY_FMT = "clean_full/statcan/{pid}.parquet"
PID_RE = re.compile(r"^\d{8}$")

# WDS getCodeSets "frequency" (fetched 2026-11-19). Month-step families: months per period.
MONTH_STEP = {6: 1, 20: 1, 7: 2, 9: 3, 19: 3, 11: 6, 12: 12, 18: 12, 13: 24, 14: 36, 15: 48, 16: 60, 17: 120}
# Day-step families: days per period.
DAY_STEP = {1: 1, 21: 1, 2: 7, 4: 14}

ZIP_SLACK = dt.timedelta(hours=1)        # a fresh zip is written 1-2 min after the release clock
OVERDUE_AFTER = dt.timedelta(hours=48)   # a release older than this should have reached the store
MIN_LIST_FRACTION = 0.95                 # share of catalogued pids the cube list must contain
ALLOWED_OPERATIONS = frozenset({"GetObject", "HeadObject"})


class Unmeasurable(Exception):
    """A cube (or probe) this run could not measure. The message is the reason in the TSV."""


class Fatal(Exception):
    """Nothing usable was measured; exit 1."""


# ---------------------------------------------------------------- the period comparison

def _ym(d: dt.date) -> int:
    return d.year * 12 + (d.month - 1)


def compare(store_max: dt.date, pub_end: dt.date, freq) -> tuple[str, int]:
    """(status, lag) for one cube: status is current | behind | ahead. Raises Unmeasurable for a
    frequency this tool does not know, or a month-step cubeEndDate that is not a period start."""
    if freq in MONTH_STEP:
        step = MONTH_STEP[freq]
        if pub_end.day != 1:
            raise Unmeasurable(f"cubeEndDate {pub_end} is not a period start for frequency {freq}")
        months = _ym(pub_end) - _ym(store_max)
        if store_max < pub_end:
            return "behind", max(1, -(-months // step))
        if -months >= step:
            return "ahead", 0
        return "current", 0
    if freq in DAY_STEP:
        step = DAY_STEP[freq]
        days = (pub_end - store_max).days
        if store_max < pub_end:
            return "behind", max(1, -(-days // step))
        if -days >= step:
            return "ahead", 0
        return "current", 0
    raise Unmeasurable(f"unknown frequencyCode {freq!r}")


def _date_part(s) -> dt.date:
    if not isinstance(s, str) or len(s) < 10:
        raise Unmeasurable(f"unparseable publisher date {s!r}")
    try:
        return dt.date.fromisoformat(s[:10])
    except ValueError:
        raise Unmeasurable(f"unparseable publisher date {s!r}") from None


def _instant(s) -> dt.datetime:
    if not isinstance(s, str):
        raise Unmeasurable(f"unparseable instant {s!r}")
    try:
        v = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        raise Unmeasurable(f"unparseable instant {s!r}") from None
    if v.tzinfo is None:
        raise Unmeasurable(f"instant without a time zone {s!r}")
    return v


# ---------------------------------------------------------------- the store side

def store_max_date(metadata) -> dt.date:
    """Max obs_date over the row-group statistics of one parquet FileMetaData. Every row group must
    carry min/max statistics for obs_date, and obs_date must be a date32 column (all 8,257 statcan
    stores are, 2026-11-19); anything else is Unmeasurable - a partial max is not a max."""
    if metadata.num_rows == 0 or metadata.num_row_groups == 0:
        raise Unmeasurable("store has 0 rows")
    names = metadata.schema.names
    if "obs_date" not in names:
        raise Unmeasurable("store has no obs_date column")
    i = names.index("obs_date")
    lt = str(metadata.schema.column(i).logical_type)
    if "Date" not in lt:
        raise Unmeasurable(f"obs_date is not a date column ({lt})")
    best = None
    for g in range(metadata.num_row_groups):
        st = metadata.row_group(g).column(i).statistics
        if st is None or not st.has_min_max:
            raise Unmeasurable(f"no obs_date statistics in row group {g}")
        v = st.max
        if not isinstance(v, dt.date):
            raise Unmeasurable(f"obs_date statistic is {type(v).__name__}, not a date")
        best = v if best is None or v > best else best
    return best


def _refuse_other_operations(model=None, **_kwargs):
    name = getattr(model, "name", None)
    if name not in ALLOWED_OPERATIONS:
        raise PermissionError(f"audit_statcan_upstream is read-only: refused S3 operation {name!r}")


def make_store(backend: str, client=None):
    """The blob this tool reads footers through. r2: R2Blob with the READ key, plus the allow-list hook
    installed FIRST on the very client R2Blob will use. selfhost: SelfhostBlob (after T0 the r2 client
    refuses every call; the self-hosted store has the same keys). `client`: an S3 client already built
    by core.r2_util.client(write=False) elsewhere (a desktop launcher); it gets the same hook."""
    from updater import blob
    if backend == "selfhost":
        return blob.SelfhostBlob()
    if backend != "r2":
        raise Fatal(f"unknown backend {backend!r}")
    store = blob.R2Blob(write=False, pool=32)
    if client is not None:
        store._client = client
    try:
        events = store.client.meta.events        # builds the lazy client now, so the hook is on it
    except Exception as e:
        raise Fatal(f"R2 read client unavailable ({type(e).__name__})") from None
    events.register_first("before-call.*.*", _refuse_other_operations, unique_id="statcan-upstream-ro-all")
    events.register_first("before-call.s3", _refuse_other_operations, unique_id="statcan-upstream-ro-s3")
    return store


def read_store_max(store, pid: str) -> dt.date:
    import pyarrow.parquet as pq
    from updater import blob
    key = KEY_FMT.format(pid=pid)
    try:
        buf = blob._r2_footer_buffer(store, key)
    except Exception as e:                       # message text can carry an endpoint: class name only
        raise Unmeasurable(f"footer read failed ({type(e).__name__})") from None
    if buf is None:
        raise Unmeasurable("store object absent")
    try:
        md = pq.read_metadata(io.BytesIO(buf))
    except Exception as e:
        raise Unmeasurable(f"footer unreadable ({type(e).__name__})") from None
    return store_max_date(md)


# ---------------------------------------------------------------- inputs

def load_cube_list(cubes_json: str | None, http_get=None) -> dict:
    """{pid: cube dict} from getAllCubesListLite. Anything but a JSON array of cube objects is Fatal:
    WDS answers failures with an object ({"status": "FAILED", ...}), never an empty list."""
    if cubes_json:
        with open(cubes_json, encoding="utf-8") as f:
            data = json.load(f)
    else:
        http_get = http_get or _http_get
        try:
            r = http_get(f"{WDS}/getAllCubesListLite")
            if r.status_code != 200:
                raise Fatal(f"getAllCubesListLite answered HTTP {r.status_code}")
            data = r.json()
        except Fatal:
            raise
        except Exception as e:
            raise Fatal(f"getAllCubesListLite failed ({type(e).__name__})") from None
    if not isinstance(data, list) or not data:
        raise Fatal("getAllCubesListLite did not return a non-empty JSON array")
    out = {}
    for c in data:
        if not isinstance(c, dict) or "productId" not in c:
            raise Fatal("getAllCubesListLite element without productId")
        out[str(c["productId"])] = c
    return out


def catalogued_pids(catalog: str, allow_live: bool) -> tuple[list[str], int]:
    """(sorted distinct 8-digit pids, count of statcan series ids that do not yield one)."""
    from core import catalog_path
    real = os.path.normcase(os.path.realpath(catalog))
    live = {os.path.normcase(os.path.realpath(p)) for p in (catalog_path.BUILD_PATH, catalog_path.CHECKOUT_PATH)}
    if real in live and not allow_live:
        raise Fatal(f"refused: {catalog} is a live catalogue; pass a copy (or --live-catalogue for the "
                    "system's own scheduled run)")
    try:
        con = catalog_path.connect_path(catalog, write=False)
        try:
            rows = con.execute("SELECT series_id FROM series WHERE source_id = 'statcan'").fetchall()
        finally:
            con.close()
    except Fatal:
        raise
    except Exception as e:
        raise Fatal(f"catalogue unreadable ({type(e).__name__}: {e})") from None
    pids, bad = set(), 0
    for (sid,) in rows:
        pid = sid[len("statcan:"):].split("#", 1)[0] if isinstance(sid, str) and sid.startswith("statcan:") else ""
        if PID_RE.match(pid):
            pids.add(pid)
        else:
            bad += 1
    return sorted(pids), bad


def _http_get(url):
    import requests
    return requests.get(url, headers=UA, timeout=300)


def _http_head(url):
    import requests
    return requests.head(url, headers=UA, timeout=120, allow_redirects=True)


def probe_zip(pid: str, http_get=None, http_head=None) -> dt.datetime:
    """Last-Modified of the cube's full-table zip (URL from getFullTableDownloadCSV)."""
    from email.utils import parsedate_to_datetime
    http_get, http_head = http_get or _http_get, http_head or _http_head
    try:
        r = http_get(f"{WDS}/getFullTableDownloadCSV/{pid}/en")
        j = r.json()
        url = j.get("object") if isinstance(j, dict) and j.get("status") == "SUCCESS" else None
        if not isinstance(url, str) or not url.startswith("https://"):
            raise Unmeasurable("getFullTableDownloadCSV gave no zip URL")
        h = http_head(url)
        if h.status_code != 200:
            raise Unmeasurable(f"zip HEAD HTTP {h.status_code}")
        lm = h.headers.get("Last-Modified")
        if not lm:
            raise Unmeasurable("zip HEAD has no Last-Modified")
        v = parsedate_to_datetime(lm)
        if v.tzinfo is None:
            raise Unmeasurable("zip Last-Modified without a time zone")
        return v
    except Unmeasurable:
        raise
    except Exception as e:
        raise Unmeasurable(f"zip probe failed ({type(e).__name__})") from None


# ---------------------------------------------------------------- the audit

COLUMNS = ["pid", "frequency_code", "archived", "release_time", "release_age_h", "publisher_end",
           "store_max", "lag_periods", "status", "reason", "zip_last_modified", "zip_class"]


def audit(pids, cubes, store, *, now: dt.datetime, probe=False, http_get=None, http_head=None,
          threads=16, pause=0.5, sleep=time.sleep) -> list[dict]:
    def one(pid):
        row = dict.fromkeys(COLUMNS, "")
        row["pid"], row["zip_class"] = pid, "not_probed"
        c = cubes.get(pid)
        if c is None:
            row["status"], row["reason"] = "withdrawn", "not in the publisher cube list"
            return row
        row["frequency_code"] = str(c.get("frequencyCode", ""))
        row["archived"] = str(c.get("archived", ""))
        row["release_time"] = str(c.get("releaseTime", ""))
        row["publisher_end"] = str(c.get("cubeEndDate", ""))[:10]
        try:
            rel = _instant(c.get("releaseTime"))
            row["release_age_h"] = f"{(now - rel).total_seconds() / 3600:.1f}"
            pub_end = _date_part(c.get("cubeEndDate"))
            smax = read_store_max(store, pid)
            row["store_max"] = smax.isoformat()
            row["status"], lag = compare(smax, pub_end, c.get("frequencyCode"))
            row["lag_periods"] = str(lag)
        except Unmeasurable as e:
            row["status"], row["reason"] = "unmeasured", str(e)
        return row

    with ThreadPoolExecutor(max_workers=max(1, threads)) as ex:
        rows = list(ex.map(one, pids))

    if probe:
        for row in rows:
            if row["status"] != "behind":
                continue
            try:
                lm = probe_zip(row["pid"], http_get, http_head)
                row["zip_last_modified"] = lm.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
                stale = lm < _instant(row["release_time"]) - ZIP_SLACK
                row["zip_class"] = "stale" if stale else "fresh"
            except Unmeasurable as e:
                row["zip_class"], row["reason"] = "probe_error", str(e)
            sleep(pause)
    return rows


def summarise(rows, *, catalogued: int, probe: bool, bad_ids: int = 0) -> tuple[str, int]:
    """(summary line, exit code)."""
    by = {}
    for r in rows:
        by.setdefault(r["status"], []).append(r)
    behind, unmeasured = by.get("behind", []), by.get("unmeasured", [])
    withdrawn, ahead = by.get("withdrawn", []), by.get("ahead", [])
    measured = len(rows) - len(unmeasured) - len(withdrawn)
    probe_err = [r for r in behind if r["zip_class"] == "probe_error"]
    overdue = [r for r in behind if r["release_age_h"] and float(r["release_age_h"]) > OVERDUE_AFTER.total_seconds() / 3600]
    archived = [r for r in behind if r["archived"] == "1"]
    if catalogued == 0:
        return "statcan upstream: NOT MEASURED - no catalogued statcan cube", 1
    if measured == 0:
        return (f"statcan upstream: NOT MEASURED - 0 of {catalogued} catalogued cubes could be measured "
                f"({len(unmeasured)} unmeasured, {len(withdrawn)} withdrawn)"), 1
    n = f"at least {len(behind)}" if unmeasured else str(len(behind))
    if probe:
        k = len([r for r in behind if r["zip_class"] == "stale"])
        zip_part = f"{'at least ' if probe_err else ''}{k} zip-stale"
    else:
        zip_part = "zip not probed"
    line = (f"statcan upstream: {n} cubes behind publisher ({zip_part}); "
            f"overdue {len(overdue)} (release >{int(OVERDUE_AFTER.total_seconds() // 3600)}h), "
            f"recent {len(behind) - len(overdue)}; archived among behind {len(archived)}; "
            f"ahead {len(ahead)}; withdrawn {len(withdrawn)}; measured {measured} of {catalogued} catalogued; "
            f"unmeasured {len(unmeasured)}")
    if probe:
        line += f"; zip probe errors {len(probe_err)}"
    if bad_ids:
        line += f"; malformed statcan series ids {bad_ids}"
    line += " [cube level only]"
    return line, (2 if unmeasured or probe_err else 0)


def write_tsv(path: str, rows) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write("\t".join(COLUMNS) + "\n")
        for r in sorted(rows, key=lambda r: (r["status"], r["pid"])):
            f.write("\t".join(str(r[c]).replace("\t", " ").replace("\n", " ") for c in COLUMNS) + "\n")
    os.replace(tmp, path)


def main(argv=None, *, store=None, client=None, http_get=None, http_head=None, now=None,
         sleep=time.sleep) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--catalog", help="a COPY of the catalogue (read-only open)")
    src.add_argument("--pids", help="comma-separated 8-digit product ids")
    ap.add_argument("--live-catalogue", action="store_true", help="allow the live catalogue path (scheduled run only)")
    ap.add_argument("--cubes-json", help="a saved getAllCubesListLite response instead of the GET")
    ap.add_argument("--backend", choices=("r2", "selfhost"), default="r2")
    ap.add_argument("--probe-zip", action="store_true", help="HEAD the full-table zip of BEHIND cubes")
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--out", required=True, help="TSV path")
    a = ap.parse_args(argv)
    now = now or dt.datetime.now(dt.timezone.utc)
    try:
        bad = 0
        if a.pids:
            pids = sorted({p.strip() for p in a.pids.split(",") if p.strip()})
            wrong = [p for p in pids if not PID_RE.match(p)]
            if wrong:
                raise Fatal(f"not 8-digit product ids: {wrong[:5]}")
        else:
            pids, bad = catalogued_pids(a.catalog, a.live_catalogue)
        cubes = load_cube_list(a.cubes_json, http_get)
        if pids:
            found = sum(1 for p in pids if p in cubes)
            if found / len(pids) < MIN_LIST_FRACTION:
                raise Fatal(f"cube list holds only {found} of {len(pids)} catalogued pids - not a full list")
        if store is None and pids:
            store = make_store(a.backend, client)
        rows = audit(pids, cubes, store, now=now, probe=a.probe_zip, http_get=http_get,
                     http_head=http_head, threads=a.threads, sleep=sleep)
    except Fatal as e:
        print(f"statcan upstream: NOT MEASURED - {e}")
        return 1
    write_tsv(a.out, rows)
    line, rc = summarise(rows, catalogued=len(pids), probe=a.probe_zip, bad_ids=bad)
    print(line)
    skipped = [r for r in rows if r["status"] == "unmeasured" or r["zip_class"] == "probe_error"]
    for r in skipped[:20]:
        print(f"  skipped {r['pid']}: {r['reason']}")
    if len(skipped) > 20:
        print(f"  ... and {len(skipped) - 20} more (see {a.out})")
    print(f"tsv: {a.out}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
