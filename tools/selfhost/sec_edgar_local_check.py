"""Does the local catalogue say what the local sec_edgar store holds? READ-ONLY. The plan's local "--audit".

After T0 the local refresher is the only writer of sec_edgar's catalogue rows, and core.sec_edgar_local
publishes their MAX(end_date) as the source's data_through. That is honest only if every row's span is the
span of the company's own stored facts. This tool measures it: for every catalogue row of the source it reads
the store file the refresher would write for that company, computes the span with the refresher's own rule
(tools/refresh_sec_edgar.coverage_span) and compares.

    python tools/selfhost/sec_edgar_local_check.py [--catalogue <path>] [--store <dir>] [--receipt <path>]

Five counts, each must be 0 for exit 0:
  differing        the catalogue span is not the store file's span
  store_only       a store file no catalogue row names (hosted, not listed: the worker answers 404 for it)
  catalogue_only   a catalogue row whose store file is missing
  forward          a store span that ends after today UTC (core.sec_edgar_local would refuse the copy)
  unreadable       a store file that could not be read or holds a NULL obs_date; or a catalogue row that
                   shares its store file with an earlier row (two ids, one file: the later one cannot be
                   compared)

It writes a RECEIPT (JSON): the counts, every row of every count (the repair list), a sha256 over the sorted
(series_id, start_date, end_date) rows it compared, and a fingerprint of the store listing (name, size, mtime). tools/selfhost/t0_ready.py recomputes
both and refuses READY when either moved, so a receipt cannot outlive the state it certified. The receipt goes
OUTSIDE data/_aqueduct, which the 6b delta sync mirrors.

WHAT IT NEVER DOES (review AR-194): it has no --apply. It opens the catalogue mode=ro, reads store files, and
writes one receipt file. It makes no network call and touches no D1, R2 or state.db. A repair is the
refresher's job (--ciks <cik> --apply), or a reviewed respan writer that does not exist yet.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import importlib.util
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from core import sec_edgar_local  # noqa: E402

STORE = os.path.join(ROOT, "data", "clean_grouped", "sec_edgar")
RECEIPT = os.path.join(ROOT, "data", "_selfhost_receipts", "sec_edgar_local_check.json")
PREFIX = sec_edgar_local.SOURCE + ":"
EXAMPLES = 20


def _refresher():
    """tools/refresh_sec_edgar.py, loaded by path (tools/ is not a package): its coverage_span is THE rule."""
    spec = importlib.util.spec_from_file_location("_refresh_sec_edgar_for_check",
                                                  os.path.join(ROOT, "tools", "refresh_sec_edgar.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def catalogue_rows(con) -> list[tuple[str, str | None, str | None]]:
    """(series_id, start_date, end_date) of every sec_edgar row, by primary-key range, sorted."""
    return [(r[0], r[1], r[2]) for r in con.execute(
        "SELECT series_id, start_date, end_date FROM series "
        "WHERE series_id >= 'sec_edgar:' AND series_id < 'sec_edgar;' ORDER BY series_id")]


def rows_sha256(rows) -> str:
    h = hashlib.sha256()
    for sid, lo, hi in sorted(rows):
        h.update(f"{sid}\t{lo}\t{hi}\n".encode("utf-8"))
    return h.hexdigest()


def store_listing(store: str) -> dict[str, tuple[int, int]]:
    """{file name: (size, mtime_ns)} of the store's parquet files. Names and stats only; no file is opened."""
    out = {}
    with os.scandir(store) as it:
        for e in it:
            if e.is_file() and e.name.endswith(".parquet"):
                st = e.stat()
                out[e.name] = (st.st_size, st.st_mtime_ns)
    return out


def listing_sha256(listing: dict) -> str:
    h = hashlib.sha256()
    for name in sorted(listing):
        size, mtime = listing[name]
        h.update(f"{name}\t{size}\t{mtime}\n".encode("utf-8"))
    return h.hexdigest()


def file_span(path: str, coverage_span):
    """(start, end) of one store file by the refresher's rule; raises on a file it cannot stand behind."""
    import pyarrow.parquet as pq                                       # noqa: PLC0415
    t = pq.read_table(path, columns=["obs_date", "vintage_date"])
    odate = t.column("obs_date").to_pylist()
    vint = t.column("vintage_date").to_pylist()
    if any(d is None for d in odate):
        raise ValueError("NULL obs_date")
    return coverage_span(odate, vint)


def compare(rows, store: str, coverage_span, today: str, workers: int = 8) -> dict:
    """The five lists. `rows` are catalogue rows; `store` is read through file_span."""
    listing = store_listing(store)
    named = {}
    catalogue_only, unreadable = [], []
    for sid, lo, hi in rows:
        name = sec_edgar_local.store_name(sid[len(PREFIX):]) + ".parquet"
        if name in named:
            # two ids, one file ("A/B" and "A_B"): the second would replace the first and that row would
            # never be compared, while every count stayed 0 (AR-195 N1)
            unreadable.append((sid, f"shares the store file {name} with {named[name][0]}"))
        elif name in listing:
            named[name] = (sid, lo, hi)
        else:
            catalogue_only.append(sid)
    store_only = sorted(n for n in listing if n not in named)

    def one(name):
        try:
            return name, file_span(os.path.join(store, name), coverage_span), None
        except Exception as e:                                         # noqa: BLE001 - counted, never skipped
            return name, None, f"{type(e).__name__}: {e}"

    differing, forward = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for name, span, err in ex.map(one, sorted(named)):
            sid, lo, hi = named[name]
            if err is not None:
                unreadable.append((sid, err))
                continue
            s_lo, s_hi = (str(span[0]) if span[0] is not None else None), (str(span[1]) if span[1] is not None else None)
            if s_hi is None or s_hi > today:
                forward.append((sid, s_hi))
            if (s_lo, s_hi) != (lo, hi):
                differing.append((sid, [lo, hi], [s_lo, s_hi]))
    return {"listing": listing, "differing": differing, "store_only": store_only,
            "catalogue_only": catalogue_only, "forward": forward, "unreadable": unreadable}


COUNTS = ("differing", "store_only", "catalogue_only", "forward", "unreadable")


def run(catalogue: str | None, store: str, receipt: str, today: str | None = None, workers: int = 8) -> dict:
    from core import catalog_path                                      # noqa: PLC0415
    today = today or sec_edgar_local.today_utc()
    path = catalogue or catalog_path.catalog_path()
    con = catalog_path.connect_path(path, write=False)                 # mode=ro; after T0 only the build opens
    try:
        rows = catalogue_rows(con)
    finally:
        con.close()
    started = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    res = compare(rows, store, _refresher().coverage_span, today, workers)
    # the store must not have moved while it was read: a file replaced mid-run makes every count a mixture
    after = store_listing(store)
    stable = listing_sha256(after) == listing_sha256(res["listing"])
    out = {
        "tool": "sec_edgar_local_check",
        "started_utc": started,
        "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "today_utc": today,
        "catalogue_path": os.path.normcase(os.path.realpath(path)),
        "store_path": os.path.normcase(os.path.realpath(store)),
        "catalogue_rows": len(rows),
        "store_files": len(res["listing"]),
        "catalogue_sha256": rows_sha256(rows),
        "store_fingerprint": listing_sha256(res["listing"]),
        "store_stable_during_read": stable,
        "counts": {k: len(res[k]) for k in COUNTS},
        "examples": {k: res[k][:EXAMPLES] for k in COUNTS},
        "rows": {k: res[k] for k in COUNTS},              # every row of every count, for the repair list
    }
    out["clean"] = stable and len(rows) > 0 and not any(out["counts"].values())
    os.makedirs(os.path.dirname(os.path.abspath(receipt)), exist_ok=True)
    tmp = receipt + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1, default=str)
    os.replace(tmp, receipt)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--catalogue", help="a catalogue to READ (mode=ro); default: this checkout's, or the build after T0")
    ap.add_argument("--store", default=STORE)
    ap.add_argument("--receipt", default=RECEIPT)
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args(argv)
    under = os.path.normcase(os.path.realpath(os.path.join(ROOT, "data", "_aqueduct")))
    if os.path.normcase(os.path.realpath(a.receipt)).startswith(under + os.sep):
        ap.error("--receipt must not be under data/_aqueduct (that prefix is mirrored by the delta sync)")
    out = run(a.catalogue, a.store, a.receipt, workers=a.workers)
    print(f"catalogue rows {out['catalogue_rows']:,}   store files {out['store_files']:,}   today {out['today_utc']} UTC")
    for k in COUNTS:
        n = out["counts"][k]
        print(f"  {k:15s} {n:>7,}" + (f"   e.g. {out['examples'][k][:3]}" if n else ""))
    if not out["store_stable_during_read"]:
        print("  THE STORE CHANGED DURING THE READ - the counts are a mixture; run again when no writer is active")
    print(f"receipt: {a.receipt}")
    print("CLEAN" if out["clean"] else "NOT CLEAN")
    return 0 if out["clean"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
