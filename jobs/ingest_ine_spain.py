#!/usr/bin/env python3
"""INE Spain (Instituto Nacional de Estadística) full ingest.

License: Spanish public sector information, freely reusable (RD 1495/2011 / OD-BY-NC →
         INE explicitly permits free redistribution for non-commercial + commercial uses
         under their open data policy: https://www.ine.es/ss/Satellite?c=Page&cid=...
Source: https://servicios.ine.es/wstempus/js/
No API key required.

Strategy:
  * List all operations → /OPERACIONES_DISPONIBLES
  * Per operation: list all tables → /TABLAS_OPERACION/{op_id}
  * Per table: fetch all data → /DATOS_TABLA/{table_id}
  * One Parquet per operation; fully resumable.

Run: python jobs/ingest_ine_spain.py
     python jobs/ingest_ine_spain.py --only 30,25   # operation IDs
"""
from __future__ import annotations
import datetime as dt, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "ine_spain")
BASE = "https://servicios.ine.es/wstempus/js/EN"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.3   # INE allows moderate traffic


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_json(url: str, retries: int = 4) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                log(f"  429 throttle, sleeping 60s"); time.sleep(60); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_ine_period(rec: dict) -> dt.date | None:
    """Parse INE period from record fields.
    Nested obs dicts have: {"Fecha": unix_ms, "Anyo": yr, "FK_Periodo": period_code, ...}
    """
    # Primary: use Fecha (Unix ms timestamp) — covers all frequencies correctly
    fecha = rec.get("Fecha")
    if fecha is not None:
        try:
            return dt.date.fromtimestamp(int(fecha) / 1000)
        except (ValueError, TypeError, OSError):
            pass

    # Fallback: legacy flat format with Anyo / Mes / T3_Periodo
    anyo = str(rec.get("Anyo", "") or "")
    mes  = rec.get("Mes")
    periodo = str(rec.get("T3_Periodo", "") or "").lower()
    if not anyo.isdigit():
        return None
    yr = int(anyo)
    try:
        if mes and str(mes).isdigit():
            m = int(mes)
            if 1 <= m <= 12:
                return dt.date(yr, m, 1)
        if "trim" in periodo or "q" in periodo:
            for c in str(rec.get("FK_Periodo", "") or ""):
                if c.isdigit():
                    q = int(c)
                    if 1 <= q <= 4:
                        return dt.date(yr, (q-1)*3+1, 1)
                    break
        if "semest" in periodo:
            s = str(rec.get("FK_Periodo", "1"))
            return dt.date(yr, 1 if "1" in s else 7, 1)
        return dt.date(yr, 12, 31)
    except Exception:
        return None


def get_operations() -> list[dict]:
    data = get_json(f"{BASE}/OPERACIONES_DISPONIBLES")
    return data if isinstance(data, list) else []


def get_tables_for_op(op_id: int) -> list[dict]:
    data = get_json(f"{BASE}/TABLAS_OPERACION/{op_id}")
    return data if isinstance(data, list) else []


def ingest_operation(op_id: int, op_name: str, out_dir: str) -> int:
    """Download all tables for one INE operation. Returns total obs count."""
    out_path = os.path.join(out_dir, f"op_{op_id}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  skip op {op_id} ({n:,} rows)")
        return n

    tables = get_tables_for_op(op_id)
    if not tables:
        return 0
    log(f"  op {op_id}: {len(tables)} tables")

    all_keys, all_dates, all_vals = [], [], []
    for t in tables:
        table_id = t.get("Id") or t.get("Codigo", "")
        if not table_id:
            continue
        data = get_json(f"{BASE}/DATOS_TABLA/{table_id}")
        if not data or not isinstance(data, list):
            time.sleep(RATE); continue

        # API returns list of series objects, each with a nested Data array
        # Format: [{"COD": "...", "Nombre": "...", "Data": [{"Fecha": ms, "Anyo": yr, "FK_Periodo": p, "Valor": v}, ...]}, ...]
        for series_rec in data:
            cod  = str(series_rec.get("COD", "") or "")
            nombre = str(series_rec.get("Nombre", "") or "")[:60]
            obs_list = series_rec.get("Data", [])
            if not obs_list:
                # Fallback: top-level record has Valor directly (old format)
                obs_list = [series_rec]
            for rec in obs_list:
                raw_v = rec.get("Valor")
                if raw_v is None:
                    continue
                try:
                    v = float(str(raw_v).replace(",", "."))
                except (TypeError, ValueError):
                    continue
                d = parse_ine_period(rec)
                if d is None:
                    continue
                key_parts = [f"table={table_id}", f"cod={cod}"]
                if nombre:
                    key_parts.append(f"nombre={nombre}")
                all_keys.append(":".join(key_parts))
                all_dates.append(d)
                all_vals.append(v)
        time.sleep(RATE)

    if not all_vals:
        return 0

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  op {op_id} ({op_name[:40]}): DONE {n:,} obs")
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    only_ids: set[int] = set()
    for a in sys.argv[1:]:
        if a.startswith("--only"):
            raw = a.split("=", 1)[-1] if "=" in a else ""
            only_ids = {int(x) for x in raw.split(",") if x.isdigit()}
        elif a.isdigit():
            only_ids.add(int(a))

    log("Fetching INE Spain operations catalog...")
    ops = get_operations()
    log(f"Found {len(ops)} operations")

    if only_ids:
        ops = [o for o in ops if int(o.get("Id", 0)) in only_ids]
        log(f"Filtered to {len(ops)} operations")

    # Deferred ops (env INE_SKIP_OPS="353,..."): oversized operations that choke
    # the whole-op-in-memory pull (e.g. op 353 Atlas de renta, 540 tables) — they
    # block every op behind them. Skipped here, tracked on the gap list, NOT
    # dropped; collected later with a per-table-checkpointing pass.
    skip_ops = {int(x) for x in os.environ.get("INE_SKIP_OPS", "").split(",") if x.strip().isdigit()}
    if skip_ops:
        log(f"Deferring ops via INE_SKIP_OPS: {sorted(skip_ops)}")

    total = 0
    for i, op in enumerate(ops, 1):
        op_id   = int(op.get("Id", 0))
        op_name = op.get("Nombre", str(op_id))
        if not op_id:
            continue
        if op_id in skip_ops:
            log(f"[{i}/{len(ops)}] op {op_id}: DEFERRED ({op_name[:50]})")
            continue
        log(f"[{i}/{len(ops)}] op {op_id}: {op_name[:60]}")
        total += ingest_operation(op_id, op_name, OUT)

    log(f"DONE: {total:,} total INE Spain observations")


if __name__ == "__main__":
    main()
