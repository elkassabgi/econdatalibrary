"""Shared implementation for the IMF direct fetchers (api.imf.org SDMX 2.1).

One module per source is required — the registry resolves
`fetchers/<source_id>.py` — so each imf_<flow>_direct source is a three-line module
that calls into here. That keeps every dataset its OWN unit: its own out_dir, its
own state row, its own CSV-coherence mapping, and a failure in one that cannot sink
the other six. A single module looping all seven would have shared one out_dir and
broken the catalog-id mapping outright.

CHANGE SIGNAL - CORRECTED 2026-09-23: there is none; every run pulls the whole flow. This said
"the dataflow's published version moves whenever they republish". MEASURED FALSE: EER read
6.0.0 on 2026-08-05 and on 2026-09-23 while 2026-M07 and M08 were appended in between. The
version is still read (it keys the sliced-pull resume, and current_vintage() reports it), but
nothing may SKIP a pull on it: run() never does (see there), and the strategy's
detect_change() cannot either, because finalize() stamps new_vintage "date-tail", which never
equals a flow version (tests/test_imf_direct_pulls_on_unchanged_version.py pins both). Whole
flows are small enough for this: EER is 14.4 MB and ~10 s.

WHY THESE ARE NEW SOURCE IDS: see jobs/ingest_imf_direct.py. IMF retired IFS and
re-keyed these datasets, and our relay-era crosswalk is uneven (FDI 95.3%,
APDREO 100%, WHDREO 56%, FAS/WORLD/COFER ~0%). Overwriting the existing imf_<flow>
sources would break thousands of live series ids to buy freshness. These add
first-hand auto-updating data alongside them instead.
"""
from __future__ import annotations

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
        # The version only keys the sliced-pull resume now (no change signal - see below), so an
        # unreachable dataflow catalogue need not stop the pull: without a token the ingester
        # starts the slices afresh instead of resuming (jobs/ingest_imf_direct.py `reusable`).
        print(f"[imf_direct] {flow}: dataflow catalogue unreachable ({e!r}) - pulling without a "
              f"resume token", flush=True)
        ver = None

    # NO "UNCHANGED VERSION -> SKIP THE PULL" GATE (removed 2026-09-23). IMF does NOT move a
    # dataflow's version when it appends months: EER read 6.0.0 on 2026-08-05 and again on
    # 2026-09-23, while 2026-M07 and M08 had been added (179,736 -> 179,956 obs, NUMBERS.md
    # 2026-09-23). The gate compared against a _version.json written with a plain open() - on a
    # CI runner that file is gone every run, so the 32 heavy-matrix and 15 daily IMF sources
    # always pulled. On the DESKTOP it persists, and it FROZE imf_imts_direct (run_location:
    # local): _version.json 1.0.0 was written 2026-08-18, the 2026-09-14 pass finished the unit
    # in 13 s with no GET, and IMF had meanwhile published 2026-M05 (UPDATE_DATE 2026-08-31,
    # version still 1.0.0) while we hold 2026-04-30 (ledger R1101).
    # The version still keys the sliced-pull resume below; it is no longer a change signal.
    try:
        stage = os.path.join(out_dir, f"_staging_{source_id}.parquet")
        # Floor the pull at half of what is already published. IMF can return a
        # well-formed document carrying ~5% of the data (see the completeness gate
        # in the ingester); merge is never-shrink so such a pull cannot destroy
        # anything, but it WOULD be reported as a successful no-op run — the same
        # class of lie as a relay-derived "no change". Half is loose enough to
        # survive legitimate revisions and withdrawals, tight enough that a
        # collapse becomes a loud structural failure instead of a quiet success.
        # resume_token = the flow version, so a sliced pull interrupted by the unit
        # deadline resumes from the slices it already finished instead of restarting.
        # It does NOT separate releases: the version stays put when months are appended
        # (see above), so a resumed pull can mix slices fetched before and after an IMF
        # append. That is harmless to the store - every slice is a full-history read and
        # the merge dedups (series_key, obs_date) with the newer value winning - but an
        # older slice may miss the newest month until the next pull.
        n = ing.pull(flow, agency, source_id, out_path=stage,
                     min_obs=before // 2, resume_token=ver)
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

    _carry_dims_sidecar(stage, path)

    try:
        os.remove(stage)          # staging file is scratch, never a second copy
    except OSError:
        pass

    mx = pc.max(tbl.column("obs_date")).as_py()
    return finalize(tally, tbl.num_rows, mx, source=source_id, series_cursors=cursors)


def _carry_dims_sidecar(stage: str, path: str) -> bool:
    """Move the key-order sidecar over the staging boundary. The ingester records
    <stage>.dims.json next to the STAGING parquet it was told to write; the publish
    merges rows into the canonical path and deletes the staging parquet, and until
    cycle 11 nothing carried the sidecar — so every _direct source's key order sat
    stranded under a _staging_ name no reader looks at (imf_sdg_direct made it
    visible; DIP/IMTS/PIP/NA_MAIN all had the same strand). Best-effort like the
    write itself: a sidecar problem must never sink a good publish."""
    try:
        data = blob.read_bytes(stage + ".dims.json")
        if data is not None:
            blob.write_bytes_atomic(path + ".dims.json", data)
            return True
    except Exception:                                        # noqa: BLE001
        pass
    return False


def _max_date(path: str):
    try:
        if blob.exists(path):
            return pc.max(blob.read_table(path, columns=["obs_date"])
                          .column("obs_date")).as_py()
    except Exception:                                        # noqa: BLE001
        pass
    return None
