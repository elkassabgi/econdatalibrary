"""S1 bulk fetcher — Swiss National Bank data portal (data.snb.ch, no key).

12 cubes, 762 series. There was no ingest script and no registry entry for this source, so it
had never had an updater path at all — the orchestrator only iterates registered units, so an
unregistered source is not "failing", it is invisible.

THE VINTAGE IS PUBLISHER-SUPPLIED, PER CUBE, which is the best case available. Every cube CSV
opens with its own metadata preamble:

    "CubeId";"devkum"
    "PublishingDate";"2026-07-01 14:30"

    "Date";"D0";"D1";"Value"

so the change signal is SNB's own statement of when it last published that cube — nothing to
infer from headers, nothing that can flap (contrast fed_board's per-request Last-Modified and
bis's replica ETags). Measured across all 12: dates range from 2025-09-01 (rendoblid/rendoblim)
to 2026-07-27 (snbgwdmigirow/snbgwdzid), i.e. they genuinely differ per cube, so a per-cube gate
does real work.

KEYS COME FROM THE CSV'S OWN COLUMNS, NOT FROM THE DIMENSIONS ENDPOINT. Our stored key is
`SNB:<cube>:<D0>:<D1>[:<D2>]`, and the CSV's dimension columns carry exactly those codes
(`M0`, `EUR1`). The `/dimensions/en` endpoint exposes a DIFFERENT id space — item ids like
`D0_0`, `D0_1` — and reconstructing keys from a cartesian product of those reproduced only
191 of 762 (25%). That number was an artifact of comparing two id spaces that cannot match
(ledger R141), not a real gap: parsing the CSV reproduces **762 of 762 (100.00%)**.

COLUMN ORDER IS READ, NOT ASSUMED. `devwkilandga` lists its dimensions as D1 then D0. The key is
built by walking the header's non-(Date, Value) columns in the order the CSV gives them, which
is what makes that cube reproduce exactly.

SCOPE. Cubes are taken from the parquets already in the store, so the set self-updates if one is
ever added, and no second list can go stale (R159).

NEW SERIES ARE PUBLISHED, AND SAID SO OUT LOUD. A cube is a snapshot: when SNB adds a series to
one, our copy of that cube should contain it, so the merge writes whatever the CSV holds. The
first run did exactly that — 764 cursors against 762 previously-known series, because snbiprogq
gained 2. That is correct for the DATA and incomplete for DISCOVERY: a series in the parquet with
no catalog row is hosted and invisible. So every new key is counted and logged by name, and the
orchestrator's derived-id recording is what carries them toward the catalog. An earlier draft of
this docstring claimed new series were "deliberately not published"; the code never did that, and
a comment that disagrees with its code is worse than no comment.

HONEST-STATUS: a cube that fails to download or parse -> transient_unit for that cube only. A
cube that downloads but yields ZERO rows -> structural_unit with its PublishingDate NOT recorded,
so it resurfaces next run instead of being sealed in behind a green status.
"""
from __future__ import annotations
import hashlib
import json
import os

import pyarrow as pa
import requests

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Deadline, Tally, finalize

SOURCE = "snb"
SIDECAR = "_cube_publishdates.json"
DEDUP = ("series_key", "obs_date")
BASE = "https://data.snb.ch/api/cube"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
BUDGET_MIN = 15


def _session():
    s = requests.Session()
    s.headers.update(UA)
    return s


def _cubes(out_dir) -> list:
    """Cube ids from the parquets we already publish — one file per cube, `<cube>.parquet`."""
    return sorted(os.path.splitext(f)[0] for f in blob.list_parquets(out_dir)
                  if not f.startswith("_"))


def _fetch(sess, cube):
    """(publishing_date, [(series_key, 'YYYY-MM'|'YYYY-MM-DD', float)]) for one cube."""
    r = sess.get(f"{BASE}/{cube}/data/csv/en", timeout=300)
    r.raise_for_status()
    lines = r.content.decode("utf-8-sig").replace("\r\n", "\n").split("\n")

    pub, hdr = None, None
    for i, line in enumerate(lines):
        if line.startswith('"PublishingDate"'):
            parts = line.split(";")
            if len(parts) > 1:
                pub = parts[1].strip('"').strip()
        if line.lower().startswith('"date"'):
            hdr = i
            break
    if hdr is None:
        raise ValueError(f"{cube}: no Date header row")

    cols = [c.strip('"') for c in lines[hdr].split(";")]
    dims = [c for c in cols if c not in ("Date", "Value")]
    rows = []
    for line in lines[hdr + 1:]:
        if not line.strip():
            continue
        parts = [c.strip('"') for c in line.split(";")]
        if len(parts) != len(cols):
            continue
        rec = dict(zip(cols, parts))
        raw = rec.get("Value", "")
        if raw in ("", None):
            continue                                         # SNB leaves gaps blank
        try:
            val = float(raw)
        except ValueError:
            continue
        key = "SNB:" + cube + ":" + ":".join(rec[d] for d in dims)
        rows.append((key, rec.get("Date", ""), val))
    return pub, rows


def _to_date(s: str):
    """SNB Date -> the date convention THIS STORE already uses. Derived from the data, not
    assumed, because the convention is not uniform across frequencies:

        "1987"        annual    -> 1987-12-31   (period END)
        "1914-01"     monthly   -> 1914-01-01   (period START)
        "2001-Q1"     quarterly -> 2001-01-01   (period START)
        "1989-01-02"  daily     -> as-is

    Mixing these up is not a cosmetic error: writing monthly rows as 1914-01-31 beside the
    stored 1914-01-01 would mint a PARALLEL date space for every monthly series and double the
    data instead of extending it. Verified by full (series_key, obs_date) set equality against
    the existing store, per cube.
    """
    import datetime as dt
    s = (s or "").strip()
    try:
        if len(s) == 4:
            return dt.date(int(s), 12, 31)
        if "-Q" in s.upper():
            y, q = s.upper().split("-Q")
            return dt.date(int(y), [1, 4, 7, 10][int(q) - 1], 1)
        if len(s) == 7:
            return dt.date(int(s[:4]), int(s[5:7]), 1)
        if len(s) == 10:
            return dt.date.fromisoformat(s)
    except (ValueError, IndexError):
        return None
    return None


def current_vintage(unit):
    """Hash of every cube's own PublishingDate. Cheap: the CSVs are small (tens of KB)."""
    out_dir = config.source_dir(SOURCE)
    cubes = _cubes(out_dir)
    if not cubes:
        return None
    sess = _session()
    h = hashlib.sha256()
    seen = 0
    for cube in cubes:
        try:
            pub, _ = _fetch(sess, cube)
        except Exception:                                    # noqa: BLE001
            pub = None
        if pub:
            seen += 1
        h.update(f"{cube}={pub or '?'};".encode())
    if not seen:
        return None
    return f"snb:{len(cubes)}:{h.hexdigest()[:16]}"


def _load(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    cubes = _cubes(out_dir)
    if not cubes:
        raise TransientError("snb: no cube parquets in the store to refresh")

    sess = _session()
    sidecar = _load(out_dir)
    tally = Tally()
    published = 0
    unchanged = 0
    cursors: dict = {}
    maxd = None
    dl = Deadline(minutes=BUDGET_MIN)

    for cube in cubes:
        path = os.path.join(out_dir, f"{cube}.parquet")
        if dl.spent():
            tally.transient_unit(f"{cube} deferred (budget)")
            continue
        try:
            pub, rows = _fetch(sess, cube)
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(cube)
            continue

        if pub and sidecar.get(cube) == pub and blob.exists(path):
            unchanged += 1
            continue
        if not rows:
            # Downloaded fine, parsed to nothing — a real break. PublishingDate NOT recorded.
            tally.structural_unit(f"{cube}: CSV parsed to 0 observations")
            continue

        keys, dates, vals = [], [], []
        for k, ds, v in rows:
            d = _to_date(ds)
            if d is None:
                continue
            keys.append(k)
            dates.append(d)
            vals.append(v)
        if not keys:
            tally.structural_unit(f"{cube}: no parseable dates")
            continue

        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        before = blob.row_count(path) if blob.exists(path) else 0
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(cube)                       # one cube must not sink the source
            continue
        published += n
        tally.added_unit(max(0, n - before), cube)
        if md and (maxd is None or str(md) > str(maxd)):
            maxd = md
        known = set()
        if before:
            known = set(blob.read_table(path, columns=["series_key"])
                        .column("series_key").to_pylist())
        fresh = set()
        for k, d in zip(keys, dates):
            iso = d.isoformat()
            if cursors.get(k, "") < iso:
                cursors[k] = iso
            if known and k not in known:
                fresh.add(k)
        if fresh:
            shown = ", ".join(sorted(fresh)[:6])
            print(f"[snb] {cube}: {len(fresh)} NEW series published — {shown}"
                  f"{' …' if len(fresh) > 6 else ''}. Hosted now; they need catalog rows to "
                  f"become discoverable.", flush=True)
        if pub:
            sidecar[cube] = pub                              # record ONLY after publishing

    if unchanged:
        print(f"[snb] {unchanged}/{len(cubes)} cube(s) unchanged by PublishingDate — skipped",
              flush=True)
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(sidecar, indent=2, sort_keys=True).encode("utf-8"))
    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))
    return finalize(tally, published, maxd or (since or None), source=SOURCE,
                    series_cursors=cursors or None)
