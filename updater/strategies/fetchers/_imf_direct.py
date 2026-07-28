"""Shared implementation for the IMF direct fetchers (api.imf.org SDMX 2.1).

One module per source is required — the registry resolves
`fetchers/<source_id>.py` — so each imf_<flow>_direct source is a three-line module
that calls into here. That keeps every dataset its OWN unit: its own out_dir, its
own state row, its own CSV-coherence mapping, and a failure in one that cannot sink
the other six. A single module looping all seven would have shared one out_dir and
broken the catalog-id mapping outright.

CHANGE SIGNAL: the dataflow's published version. IMF republishes whole datasets and
ships dated vintages (BOP_2026_MAY_VINTAGE), so a date-tail probe would miss a
back-revision that rewrites history without extending it. The version string moves
whenever they republish, which is exactly the event we care about.

WHY THESE ARE NEW SOURCE IDS: see jobs/ingest_imf_direct.py. IMF retired IFS and
re-keyed these datasets, and our DBnomics-era crosswalk is uneven (FDI 95.3%,
APDREO 100%, WHDREO 56%, FAS/WORLD/COFER ~0%). Overwriting the existing imf_<flow>
sources would break thousands of live series ids to buy freshness. These add
first-hand auto-updating data alongside them instead.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

import pyarrow.compute as pc
import pyarrow.parquet as pq

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize
from jobs import ingest_imf_direct as ing

DEDUP = ("series_key", "obs_date")
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}


def _flow_version(flow: str) -> "str | None":
    """Published version of one dataflow, or None if the catalogue is unreachable."""
    req = urllib.request.Request(f"{ing.BASE}/dataflow", headers=UA)
    with urllib.request.urlopen(req, timeout=180) as r:
        root = ET.fromstring(r.read())
    for e in root.iter():
        if e.tag.split("}")[-1] == "Dataflow" and (e.get("id") or "").upper() == flow:
            return e.get("version") or ""
    return None


def vintage(flow: str) -> "str | None":
    """Never raises: an undeterminable vintage must not fail the run (update() still
    does the real work and reports honestly)."""
    try:
        v = _flow_version(flow)
    except Exception:                                        # noqa: BLE001
        return None
    return f"{flow}:{v}" if v is not None else None


def run(flow: str, agency: str, source_id: str) -> Result:
    tally = Tally()
    out_dir = config.source_dir(source_id)
    path = os.path.join(out_dir, f"{source_id}.parquet")
    before = blob.row_count(path) if blob.exists(path) else 0

    try:
        ver = _flow_version(flow)
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"{flow}: IMF dataflow catalogue unreachable: {e!r}") from e

    sidecar = os.path.join(out_dir, "_version.json")
    seen = None
    try:
        if os.path.exists(sidecar):
            seen = json.load(open(sidecar, encoding="utf-8")).get("version")
    except Exception:                                        # noqa: BLE001
        seen = None

    if ver and seen == ver and before:
        # Unchanged upstream. Report the rows we hold so `obs` describes the whole
        # source rather than implying it emptied.
        tally.empty_unit(flow)
        return finalize(tally, before, _max_date(path), source=source_id,
                        series_cursors={})

    try:
        stage = os.path.join(out_dir, f"_staging_{source_id}.parquet")
        n = ing.pull(flow, agency, source_id, out_path=stage)
    except urllib.error.HTTPError as e:
        if e.code in (400, 404):
            # Flow id or agency moved. STRUCTURAL — existing rows are kept and the
            # run says so, rather than serving stale data indefinitely in silence.
            tally.structural_unit(f"{flow} HTTP {e.code}")
            return finalize(tally, before, _max_date(path), source=source_id)
        raise TransientError(f"{flow}: HTTP {e.code}") from e
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"{flow}: {e!r}") from e

    if not n:
        tally.structural_unit(flow)          # 200 but zero parsed — see the ingester
        return finalize(tally, before, _max_date(path), source=source_id)

    # PUBLISH THROUGH merge/blob, never trust the ingester's local write.
    # jobs/ingest_imf_direct.py writes a plain local parquet with pq.write_table
    # because it is also a standalone CLI. Under AQUEDUCT_BACKEND=r2 that file never
    # reaches R2, so a CI run would report rows merged and publish NOTHING — green,
    # and empty. Re-read what the ingester produced and republish it via
    # merge.merge_and_write, which is atomic, dedups, and is never-shrink.
    fresh = pq.read_table(stage)
    n_rows, _ = merge.merge_and_write(path, fresh, mode="merge", dedup_keys=DEDUP)
    tbl = blob.read_table(path)
    tally.added_unit(max(0, tbl.num_rows - before), flow)

    cursors = {}
    for k, d in zip(tbl.column("series_key").to_pylist(),
                    tbl.column("obs_date").to_pylist()):
        if d:
            iso = d.isoformat()
            if k not in cursors or iso > cursors[k]:
                cursors[k] = iso

    try:
        os.makedirs(out_dir, exist_ok=True)
        with open(sidecar, "w", encoding="utf-8") as fh:
            json.dump({"version": ver, "flow": flow, "agency": agency}, fh, indent=1)
    except Exception:                                        # noqa: BLE001
        pass

    try:
        os.remove(stage)          # staging file is scratch, never a second copy
    except OSError:
        pass

    mx = pc.max(tbl.column("obs_date")).as_py()
    return finalize(tally, tbl.num_rows, mx, source=source_id, series_cursors=cursors)


def _max_date(path: str):
    try:
        if blob.exists(path):
            return pc.max(blob.read_table(path, columns=["obs_date"])
                          .column("obs_date")).as_py()
    except Exception:                                        # noqa: BLE001
        pass
    return None
