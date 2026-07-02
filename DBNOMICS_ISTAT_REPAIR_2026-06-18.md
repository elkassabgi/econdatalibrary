# DBnomics ISTAT — completeness repair (2026-06-18)

## The bug (same class as the GUS one)
`jobs/_dbnomics_pull.py` could silently drop whole datasets:
- `pull_provider` caught **any** dataset error (incl. a 6×-timed-out fetch),
  logged it, and `continue`d — then wrote `_DONE` regardless. So a timed-out
  dataset was dropped and the provider was marked complete.
- A timeout was indistinguishable from "no data": both surfaced the same way.
- Trigger seen live: the `164_164_DF_DCIS_RICPOPRES2011_*` census family timing
  out on the old 120 s read timeout.

This data is **not redundant**: the direct ISTAT pull (`data/clean_full/istat`,
1.1 GB / 755 files) does **not** contain the `164_164` family, so the DBnomics
slice adds unique datasets.

## The fix (code)
- `get()` now **raises `TransientError`** on transient failure (timeout / 5xx /
  429 / network) after retries; returns `None` only for 404; lets definitive 4xx
  (e.g. the 100k-series cap) raise `requests.HTTPError`. Timeout 120→**300 s**,
  tries 6→**8**.
- Output switched to **one file per dataset** (`ds-<code>.parquet`), written
  atomically (tmp+rename) and **only on full success**. A half-pulled dataset
  leaves no file ⇒ it is retried; it can never be silently truncated into a
  shared shard. (Reboot-safe: the file IS the unit of completeness.)
- Per-dataset resume via `_done_datasets.json`; on restart, completed datasets
  are skipped and only the rest are pulled.
- `TransientError` ⇒ record in `_failed_datasets.json`, do NOT write `_DONE`.
  `requests.HTTPError` (100k cap) ⇒ keep the max-obtainable slice, mark done.
- Writes `logs/dbnomics_istat.DONE` only when **every** dataset is whole, so the
  watchdog stops relaunching only when genuinely finished.

## The repair (data)
The old run's 51 `part-NNN.parquet` shared shards held partial data for the
timed-out datasets and used the old naming, so they were **deleted** and ISTAT is
**re-pulling clean** with the per-dataset design (PID was 28228, log
`logs/dbnomics_istat_fixed_0618_1320.log`). Dataset-list cache was not present,
so it enumerates the ISTAT catalog first, then pulls.

## Verify when done
`data/clean_full/dbnomics/ISTAT/_DONE` exists and `logs/dbnomics_istat.DONE`
appears; `_failed_datasets.json` absent. Then recount before the grand total.

See also [GUS_REPAIR_2026-06-18.md](GUS_REPAIR_2026-06-18.md) (same bug class).
