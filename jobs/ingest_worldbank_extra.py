#!/usr/bin/env python3
"""World Bank extra catalog ingestion — databases beyond WDI+ESG+Pink.

Pulls remaining CC BY 4.0 World Bank databases. Uses longer timeouts and
per-indicator pagination to handle slow API responses.

Resumable: checkpoint JSON tracks completed indicators so restarts skip ahead.

Run: python jobs/ingest_worldbank_extra.py
"""
import datetime as dt, json, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "worldbank_extra")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://api.worldbank.org/v2"

# (source_id, short_name)
SOURCES = [
    (2,  "hnp"),        # Health Nutrition and Population           ~27k obs
    (6,  "ids"),        # International Debt Statistics
    (12, "gfdd"),       # Global Financial Development (8450 inds)
    (14, "gender"),     # Gender Statistics (1392 inds)
    (15, "gem"),        # Global Economic Monitor (36 inds)
    (29, "edstats"),    # Education Statistics (3588 inds)
    (37, "poverty"),    # Poverty and Equity (211 inds)
    (40, "jobs"),       # Jobs (187 inds)
    (41, "doing_biz"),  # Doing Business — historical (185 inds)
]

CHECKPOINT_EVERY = 50   # write checkpoint parquet every N indicators


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_api(url, params, timeout=60):
    for attempt in range(4):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log(f"    retry {attempt+1}: {e}")
            time.sleep(8 * (attempt + 1))
    return None


def get_indicators(source_id: int) -> list[str]:
    indicators = []
    page = 1
    while True:
        j = get_api(f"{BASE}/source/{source_id}/indicator",
                    {"format": "json", "per_page": 500, "page": page})
        if not j or len(j) < 2 or not j[1]:
            break
        indicators.extend(item["id"] for item in j[1] if item.get("id"))
        total = int(j[0].get("total", 0))
        if len(indicators) >= total:
            break
        page += 1
        time.sleep(0.3)
    return indicators


def fetch_indicator(ind_code: str) -> list[tuple[str, dt.date, float]]:
    rows = []
    page = 1
    while True:
        j = get_api(f"{BASE}/country/all/indicator/{ind_code}",
                    {"format": "json", "per_page": 1000, "page": page})
        if not j or len(j) < 2 or not j[1]:
            break
        for rec in j[1]:
            if rec.get("value") is None:
                continue
            try:
                val = float(rec["value"])
            except (ValueError, TypeError):
                continue
            yr = (rec.get("date") or "")[:4]
            if not yr.isdigit():
                continue
            rows.append((
                rec.get("countryiso3code") or "",
                dt.date(int(yr), 12, 31),
                val,
            ))
        meta = j[0]
        total = int(meta.get("total", 0))
        done = (page - 1) * 1000 + len(j[1])
        if done >= total:
            break
        page += 1
        time.sleep(0.1)
    return rows


def checkpoint_path(name: str) -> str:
    return os.path.join(OUT, f"_{name}_ckpt.parquet")


def done_set_path(name: str) -> str:
    return os.path.join(OUT, f"_{name}_done.json")


def load_checkpoint(name: str) -> tuple[set, pa.Table | None]:
    """Return (set_of_done_indicator_ids, partial_parquet_table_or_None)."""
    done_ids: set[str] = set()
    ckpt_tbl = None
    dp = done_set_path(name)
    cp = checkpoint_path(name)
    if os.path.exists(dp):
        with open(dp) as f:
            done_ids = set(json.load(f))
    if os.path.exists(cp) and done_ids:
        try:
            ckpt_tbl = pq.read_table(cp)
        except Exception:
            ckpt_tbl = None
    return done_ids, ckpt_tbl


def save_checkpoint(name: str, done_ids: set[str],
                    all_ind, all_ctry, all_date, all_val,
                    partial_tbl: pa.Table | None) -> pa.Table:
    """Append current batch to checkpoint parquet and update done set."""
    if all_val:
        new_tbl = pa.table({
            "indicator": pa.array(all_ind, pa.string()),
            "country":   pa.array(all_ctry, pa.string()),
            "obs_date":  pa.array(all_date, pa.date32()),
            "value":     pa.array(all_val,  pa.float64()),
        })
        if partial_tbl is not None:
            import pyarrow as pa2
            merged = pa2.concat_tables([partial_tbl, new_tbl])
        else:
            merged = new_tbl
        pq.write_table(merged, checkpoint_path(name), compression="zstd")
    else:
        merged = partial_tbl

    with open(done_set_path(name), "w") as f:
        json.dump(sorted(done_ids), f)

    return merged


def ingest_source(source_id: int, name: str) -> int:
    out_path = os.path.join(OUT, f"{name}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  {name}: already {n:,} rows")
        return n

    log(f"  {name} (source={source_id}): loading indicators list...")
    indicators = get_indicators(source_id)
    log(f"  {name}: {len(indicators)} indicators to process")
    if not indicators:
        return 0

    # Load checkpoint if available
    done_ids, partial_tbl = load_checkpoint(name)
    if done_ids:
        log(f"  {name}: resuming from checkpoint ({len(done_ids)}/{len(indicators)} done)")

    all_ind, all_ctry, all_date, all_val = [], [], [], []
    batch_count = 0

    for i, ind in enumerate(indicators):
        if ind in done_ids:
            continue  # already fetched in a previous run

        rows = fetch_indicator(ind)
        for ctry, d, v in rows:
            all_ind.append(ind)
            all_ctry.append(ctry)
            all_date.append(d)
            all_val.append(v)
        done_ids.add(ind)
        batch_count += 1

        if batch_count % CHECKPOINT_EVERY == 0:
            partial_tbl = save_checkpoint(name, done_ids,
                                          all_ind, all_ctry, all_date, all_val,
                                          partial_tbl)
            all_ind, all_ctry, all_date, all_val = [], [], [], []
            pct = 100 * len(done_ids) / len(indicators)
            obs = pq.read_metadata(checkpoint_path(name)).num_rows if os.path.exists(checkpoint_path(name)) else 0
            log(f"    {name}: {len(done_ids)}/{len(indicators)} ({pct:.1f}%) indicators, {obs:,} obs so far")

        time.sleep(0.1)

    # Final flush — merge checkpoint + remaining batch → final parquet
    partial_tbl = save_checkpoint(name, done_ids,
                                  all_ind, all_ctry, all_date, all_val,
                                  partial_tbl)

    final_tbl = partial_tbl
    if final_tbl is None or final_tbl.num_rows == 0:
        log(f"  {name}: 0 obs")
        return 0

    pq.write_table(final_tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  {name}: DONE {n:,} obs")

    # Clean up checkpoint files
    for fp in [checkpoint_path(name), done_set_path(name)]:
        try:
            os.remove(fp)
        except OSError:
            pass
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    total = 0
    for sid, name in SOURCES:
        log(f"=== {name} (source {sid}) ===")
        total += ingest_source(sid, name)
    log(f"GRAND TOTAL: {total:,} World Bank extra observations")


if __name__ == "__main__":
    main()
