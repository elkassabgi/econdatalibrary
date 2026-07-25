#!/usr/bin/env python3
"""Download FULL observations for selected DBnomics providers into GROUPED Parquet.

Usage:
  python jobs/_dbnomics_pull.py <PROVIDER1> <PROVIDER2> ...   # pull these providers fully
  python jobs/_dbnomics_pull.py --from-classification          # pull every UNIQUE provider
                                                               #   up to --max-series cap

Grain / anti-bloat:
  One Parquet per provider-SHARD under data/clean_full/dbnomics/<PROVIDER>/part-NNN.parquet.
  Each shard packs MANY datasets+series (columns: provider, dataset, series_key,
  obs_date, value, license_id). A new shard starts when the current one exceeds
  ROWS_PER_SHARD rows, so a provider with thousands of tiny datasets still yields a
  handful of files, not thousands. Result for the whole source = low-hundreds of files.

Source: DBnomics. license_id = per-provider passthrough (provider's own terms;
source-level license stays dbnomics-passthrough). Polite UA, retry/backoff, conc<=6.
Resumable: a provider is skipped if data/clean_full/dbnomics/<PROVIDER>/_DONE exists.
"""
from __future__ import annotations
import concurrent.futures as cf
import json
import os
import sys
import time

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
RAW = os.path.join(ROOT, "data", "raw", "dbnomics")
OUT = os.path.join(ROOT, "data", "clean_full", "dbnomics")
API = "https://api.db.nomics.world/v22"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

ROWS_PER_SHARD = 4_000_000   # ~ keeps each Parquet file in the tens-of-MB range
SERIES_PAGE = 1000           # series-with-observations per API call (max)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})

LICENSE = {}   # provider -> license_id, filled from _classification.json


class TransientError(Exception):
    """A request kept failing transiently (timeout / 5xx / 429 / network) after
    every retry. Callers MUST NOT treat this as 'no data' — the dataset has to be
    retried on a later run so coverage stays complete. Definitive client errors
    (4xx other than 404/429, e.g. the 100k-series cap) raise requests.HTTPError
    instead and are handled separately."""


def get(url, tries=8):
    last = None
    for a in range(tries):
        try:
            r = SESSION.get(url, timeout=300)
        except requests.RequestException as e:
            last = e; time.sleep(2.0 * (a + 1)); continue
        if r.status_code == 429 or 500 <= r.status_code < 600:
            last = RuntimeError(f"HTTP {r.status_code}"); time.sleep(3.0 * (a + 1)); continue
        if r.status_code == 404:
            return None
        r.raise_for_status()   # definitive 4xx (incl. the 100k cap) -> HTTPError
        return r.json()
    raise TransientError(f"{tries} failed attempts ({last}): {url}")


def list_datasets(provider):
    ck = os.path.join(RAW, "_ckpt_datasets", f"{provider}.json")
    if os.path.exists(ck):
        return json.load(open(ck, encoding="utf-8"))
    rows, offset = [], 0
    while True:
        j = get(f"{API}/datasets/{provider}?limit=100&offset={offset}")
        if not j:
            break
        ds = j.get("datasets", {})
        docs = ds.get("docs", [])
        if not docs:
            break
        rows.extend({"dataset_code": d.get("code"), "nb_series": d.get("nb_series")} for d in docs)
        offset += len(docs)
        if offset >= (ds.get("num_found") or 0):
            break
    return rows


def iter_dataset_series(provider, dataset):
    """Yield (series_code, dates[], values[]) for a whole dataset, paginated."""
    offset = 0
    while True:
        j = get(f"{API}/series/{provider}/{dataset}?observations=1&limit={SERIES_PAGE}&offset={offset}")
        if not j:
            return
        s = j.get("series", {})
        docs = s.get("docs", [])
        if not docs:
            return
        for d in docs:
            yield (d.get("series_code"), d.get("period_start_day") or [], d.get("value") or [])
        offset += len(docs)
        if offset >= (s.get("num_found") or 0):
            return
        time.sleep(0.02)


class ShardWriter:
    """Accumulates rows and flushes to part-NNN.parquet every ROWS_PER_SHARD."""
    def __init__(self, provider):
        self.provider = provider
        self.dir = os.path.join(OUT, provider)
        os.makedirs(self.dir, exist_ok=True)
        self.lic = LICENSE.get(provider, "provider-terms")
        self._reset()
        self.part = 0
        self.total = 0

    def _reset(self):
        self.ds, self.sk, self.od, self.val = [], [], [], []

    def add(self, dataset, series_key, date_str, value):
        self.ds.append(dataset); self.sk.append(series_key)
        self.od.append(date_str); self.val.append(value)
        if len(self.sk) >= ROWS_PER_SHARD:
            self.flush()

    def flush(self):
        if not self.sk:
            return
        tbl = pa.table({
            "provider": pa.array([self.provider] * len(self.sk)),
            "dataset": self.ds,
            "series_key": self.sk,
            "obs_date": pa.array(self.od, type=pa.date32()),
            "value": pa.array(self.val, type=pa.float64()),
            "license_id": pa.array([self.lic] * len(self.sk)),
        })
        path = os.path.join(self.dir, f"part-{self.part:03d}.parquet")
        pq.write_table(tbl, path, compression="zstd")
        self.total += len(self.sk)
        self.part += 1
        self._reset()


def parse_date(s):
    if not s or len(s) < 10:
        return None
    try:
        import datetime as dt
        return dt.date.fromisoformat(s[:10])
    except (TypeError, ValueError):
        return None


def num(v):
    if v is None or isinstance(v, str):
        return None
    if isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


def _safe(dcode):
    return "".join(c if (c.isalnum() or c in "._-") else "_" for c in dcode)


def write_dataset(provider, dcode, rows, lic):
    """Atomically write one dataset's rows to ds-<code>.parquet. The file is the
    unit of completeness: it exists only if the dataset was pulled in full (or to
    its definitive 100k-series cap), so a half-pulled dataset never leaves a file."""
    out_dir = os.path.join(OUT, provider)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"ds-{_safe(dcode)}.parquet")
    tbl = pa.table({
        "provider":   pa.array([provider] * len(rows)),
        "dataset":    pa.array([r[0] for r in rows]),
        "series_key": pa.array([r[1] for r in rows]),
        "obs_date":   pa.array([r[2] for r in rows], type=pa.date32()),
        "value":      pa.array([r[3] for r in rows], type=pa.float64()),
        "license_id": pa.array([lic] * len(rows)),
    })
    tmp = path + ".tmp"
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, path)
    return len(rows)


def _done_path(provider):
    return os.path.join(OUT, provider, "_done_datasets.json")


def _persist_done(provider, done):
    p = _done_path(provider)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(sorted(done), f)
    os.replace(tmp, p)


def load_done(provider):
    done = set()
    p = _done_path(provider)
    if os.path.exists(p):
        try:
            done |= set(json.load(open(p, encoding="utf-8")))
        except Exception:
            pass
    # Belt-and-suspenders: an existing ds-<code>.parquet IS proof the dataset was
    # pulled in full (atomic write), so treat it as done even when the JSON set
    # was not yet persisted (it persists every 20 datasets). Trust the filename
    # only when _safe() round-trips it exactly, so the code is recovered losslessly.
    d = os.path.join(OUT, provider)
    if os.path.isdir(d):
        for fn in os.listdir(d):
            if fn.startswith("ds-") and fn.endswith(".parquet"):
                code = fn[3:-len(".parquet")]
                if _safe(code) == code:
                    done.add(code)
    return done


def pull_provider(provider):
    out_dir = os.path.join(OUT, provider)
    os.makedirs(out_dir, exist_ok=True)
    done_marker = os.path.join(out_dir, "_DONE")
    if os.path.exists(done_marker):
        return provider, "skip", 0, 0

    lic = LICENSE.get(provider, "provider-terms")
    datasets = list_datasets(provider)
    done = load_done(provider)            # dataset codes already pulled in full
    n_done_start = len(done)
    failed, capped, total_obs, processed = [], [], 0, 0

    for d in datasets:
        dcode = d.get("dataset_code")
        if not dcode or dcode in done:
            continue
        rows = []
        try:
            for series_code, dates, values in iter_dataset_series(provider, dcode):
                for ds_str, v in zip(dates, values):
                    od = parse_date(ds_str)
                    fv = num(v)
                    if od is None or fv is None:
                        continue
                    rows.append((dcode, series_code, od, fv))
        except TransientError as e:
            # timeout / 5xx / network: discard partial rows, retry on next run.
            print(f"  [{provider}] dataset {dcode} TRANSIENT (retry next run): {e}", flush=True)
            failed.append(dcode)
            continue
        except requests.HTTPError as e:
            # definitive client error (e.g. the 100k-series cap): keep the max
            # obtainable slice already gathered and mark done — retry can't help.
            print(f"  [{provider}] dataset {dcode} CAPPED/definitive ({e}); keeping {len(rows):,} obs", flush=True)
            capped.append(dcode)
        except Exception as e:  # noqa: BLE001 — unknown: treat as transient, retry
            print(f"  [{provider}] dataset {dcode} ERROR (retry next run): {e}", flush=True)
            failed.append(dcode)
            continue

        if rows:
            total_obs += write_dataset(provider, dcode, rows, lic)
        done.add(dcode)
        processed += 1
        if processed % 20 == 0:
            _persist_done(provider, done)
    _persist_done(provider, done)

    if failed:
        # Do NOT mark the provider done — leave the failed datasets for the next
        # run (completed ds-*.parquet are skipped, only failures are retried).
        try:
            with open(os.path.join(out_dir, "_failed_datasets.json"), "w", encoding="utf-8") as f:
                json.dump(sorted(failed), f)
        except OSError:
            pass
        print(f"  [{provider}] INCOMPLETE: {len(failed)} of {len(datasets)} datasets "
              f"failed transiently; {len(done)} done. Re-run to retry.", flush=True)
        return provider, "partial", total_obs, len(done) - n_done_start

    # every dataset is whole (capped ones are at their unbypassable maximum)
    with open(done_marker, "w") as f:
        f.write(json.dumps({"datasets": len(done), "obs_this_run": total_obs, "capped": capped}))
    try:
        sentinel = os.path.join(ROOT, "logs", f"dbnomics_{provider.lower()}.DONE")
        os.makedirs(os.path.dirname(sentinel), exist_ok=True)
        with open(sentinel, "w", encoding="utf-8") as f:
            f.write(f"{len(done)} datasets, capped={len(capped)}\n")
    except OSError:
        pass
    return provider, "done", total_obs, len(done)


def main():
    os.makedirs(OUT, exist_ok=True)
    cls = None
    cls_path = os.path.join(RAW, "_classification.json")
    if os.path.exists(cls_path):
        cls = json.load(open(cls_path, encoding="utf-8"))
        for r in cls["rows"]:
            LICENSE[r["provider"]] = r["license_id"]
    else:
        # fall back to the static per-provider license map (catalog still enumerating)
        sys.path.insert(0, os.path.dirname(__file__))
        from _dbnomics_classify import PROVIDER_LICENSE  # noqa: E402
        LICENSE.update(PROVIDER_LICENSE)

    args = sys.argv[1:]
    if "--from-classification" in args:
        if cls is None:
            raise SystemExit("--from-classification needs _classification.json (run _dbnomics_classify.py first)")
        cap = None
        if "--max-series" in args:
            cap = int(args[args.index("--max-series") + 1])
        provs = [r["provider"] for r in cls["rows"]
                 if not r["duplicate"] and (r["series"] or 0) > 0
                 and (cap is None or (r["series"] or 0) <= cap)]
    else:
        provs = [a for a in args if not a.startswith("--")]

    workers = 4
    if "--workers" in args:
        workers = int(args[args.index("--workers") + 1])
    print(f"PULLING {len(provs)} providers (conc={workers}): {provs}", flush=True)
    grand_obs = 0
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(pull_provider, p): p for p in provs}
        for fut in cf.as_completed(futs):
            p = futs[fut]
            try:
                prov, status, obs, nds = fut.result()
            except Exception as e:  # noqa: BLE001
                print(f"  {p} FAILED: {e}", flush=True)
                continue
            grand_obs += obs
            print(f"  {prov:14} {status:5} obs={obs:>14,} datasets={nds:>5,}", flush=True)
    print(f"PULL DONE. grand_obs={grand_obs:,}", flush=True)


if __name__ == "__main__":
    main()
