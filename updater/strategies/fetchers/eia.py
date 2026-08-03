"""S1 bulk fetcher — U.S. Energy Information Administration bulk datasets (api.eia.gov, no key).

29 bulk datasets (AEO vintages, STEO, EBA, ELEC, INTL, NG, PET, …), one zip each, published with
a manifest. The store already holds 52 parquets and the first alone carries 10.7M rows.

THIS SOURCE WAS SCHEDULED AND COULD NOT RUN. eia sits in the updater-heavy matrix and in the
registry with NO fetcher, so the orchestrator printed "PENDING eia — no adapter built" and
skipped it — the 05:50 heavy run on 2026-07-30 showed exactly that, all four matrix jobs
reporting "0 unit(s) processed" and exiting 0. Same state cepii_gravity was in, and the reason
tools/audit_schedule_coverage.py now demands an adapter before counting anything as scheduled.

THE VINTAGE IS PUBLISHER-SUPPLIED, PER DATASET — the best case there is. api.eia.gov/bulk/
manifest.txt gives every dataset an accessURL and a `last_updated`, populated for 29 of 29
(measured). That is EIA's own statement of when each bulk file changed: nothing to infer from
headers, nothing that can flap, and none of the defect class that bit fed_board (Last-Modified
regenerated per request), bis (ETags flapping across replicas) or whr (a CDN reporting its
cache-FILL time). One small text file per run, then work only where a stamp moved.

REUSED WHOLESALE from jobs.ingest_eia: fetch_manifest, download (5-try backoff, and it only
short-circuits when an EXPECTED size matches, so it is not a staleness bomb) and write_dataset,
which already streams the zip line-by-line and flushes in BATCH row-groups — so the 10M-row
datasets never materialise. main() is NOT reused: it has no last_updated gate and would
re-download all 29 every run.

CURSORS KEY ON series_id, NOT series_key. EIA's data parquet is
[series_id, obs_date, value, period, freq]. Passing the default column name would silently
yield no cursors, and a fetcher that merges rows while reporting none is demoted to partial with
its vintage withheld — it would republish forever with stale CSVs (R174).

HONEST-STATUS: manifest unreachable -> TransientError (partial, retried, data kept). A
per-dataset download or parse failure -> transient_unit for that dataset only, so one bad zip
cannot stop the other 28. A dataset that downloads but writes ZERO rows -> structural_unit with
its last_updated NOT recorded, so a parser break resurfaces next run instead of being sealed in.
"""
from __future__ import annotations
import hashlib
import json
import os

from ... import blob, config
from ...errors import TransientError
from ..base import Result
from ._common import CURSOR_CAP, Deadline, Tally, finalize, merge_cursors
from jobs import ingest_eia as ig     # manifest + the production downloader and parser

SOURCE = "eia"
SIDECAR = "_manifest_updated.json"
BUDGET_MIN = 25


def _manifest():
    try:
        return ig.fetch_manifest() or {}
    except Exception:                                        # noqa: BLE001
        return {}


def current_vintage(unit):
    """Hash of every dataset's own last_updated stamp. One small manifest fetch."""
    ds = _manifest()
    if not ds:
        return None
    h = hashlib.sha256()
    for k in sorted(ds):
        h.update(f"{k}={(ds[k] or {}).get('last_updated')};".encode())
    return f"eia:{len(ds)}:{h.hexdigest()[:16]}"


def _load(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save(out_dir, data) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))


def _safe(key: str) -> str:
    return key.replace("/", "_").replace(":", "_")


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(ig.RAW, exist_ok=True)
    os.makedirs(ig.META, exist_ok=True)

    ds = _manifest()
    if not ds:
        raise TransientError("eia: bulk manifest unreachable")

    sidecar = _load(out_dir)
    tally = Tally()
    published = 0
    unchanged = 0
    cursors: dict = {}
    dl = Deadline(minutes=BUDGET_MIN)

    for key in sorted(ds):
        meta = ds[key] or {}
        stamp = meta.get("last_updated") or ""
        safe = _safe(key)
        data_path = os.path.join(out_dir, safe + ".parquet")

        if stamp and sidecar.get(key) == stamp and blob.exists(data_path):
            unchanged += 1
            continue
        if dl.spent():
            tally.deferred_unit(f"{key} deferred (budget {BUDGET_MIN} min)")
            continue

        url = meta.get("accessURL")
        if not url:
            tally.transient_unit(f"{key}: manifest entry has no accessURL")
            continue

        zip_path = os.path.join(ig.RAW, safe + ".zip")
        try:
            if not ig.download(url, zip_path):
                tally.transient_unit(key)
                continue
            stats = ig.write_dataset(key, zip_path)
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(key)
            continue
        finally:
            try:
                os.remove(zip_path)                          # never let a stale zip satisfy a later run
            except OSError:
                pass

        # write_dataset returns n_obs / n_series / n_categories — READ from its actual return,
        # not guessed. A wrong key here is not cosmetic: n_rows 0 means tally.added stays 0,
        # finalize reports no_change, and orchestrate._derive_changed_csvs only runs on "ok" —
        # so a dataset would publish 119,105 rows and its CSVs would never be derived. That is
        # R174 arriving from a different direction, and the first smoke run hit it exactly.
        n_rows = int((stats or {}).get("n_obs") or 0)
        if not n_rows and not os.path.exists(data_path):
            # Downloaded fine, wrote nothing — a real break. Stamp NOT recorded.
            tally.structural_unit(f"{key}: bulk zip parsed to 0 rows")
            continue

        for p in (data_path, os.path.join(ig.META, safe + ".parquet")):
            if os.path.exists(p):
                blob.publish_file(p)
        merge_cursors(cursors, data_path, key_col="series_id")   # EIA keys on series_id
        published += n_rows
        tally.added_unit(n_rows, key)
        if stamp:
            sidecar[key] = stamp                             # record ONLY after publishing

    if unchanged:
        print(f"[eia] {unchanged}/{len(ds)} dataset(s) unchanged by manifest last_updated — "
              f"skipped", flush=True)
    if len(cursors) >= CURSOR_CAP:
        print(f"[eia] cursor set hit the {CURSOR_CAP:,} cap — further changed series are not "
              f"individually reported", flush=True)
    _save(out_dir, sidecar)
    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))
    return finalize(tally, published, since or None, source=SOURCE,
                    series_cursors=cursors or None)
