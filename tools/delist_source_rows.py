"""Delist a source: delete its catalogue + D1 rows WITHOUT touching any stored object.

AUTHORIZED by Ahmed 2026-08-06 ("yes, remove hf, owids"): removes catalogue/D1 listings for
sources whose rows are unservable or unserveable-by-licence, while deliberately leaving every
stored object (store parquets AND series CSVs) alone. That is the difference from
tools/retire_source.py, which purges objects — a gated source's gated store must survive its
delisting, so the two operations must never share a code path.

Targets this was built for:
  * hf_equities — 1,391 metadata-only listings econ cannot serve (R29: no metadata-only, ever);
    0 series CSVs and 0 store objects exist, so row deletion IS the whole cleanup.

After running: remove the id from util.ts SUPPORTED_SOURCES, deploy, verify live absence
(with a known-present control, R338), and refresh_r2_catalog --allow-shrink <src>.

WHERE each step acts is core/licence_targets.py's job (docs/ECON_SELF_HOSTING_PLAN.md, change 5): R2, the
checkout's catalogue and D1 before T0, exactly as always; the self-hosted blob store and the one catalogue
build (under the single-writer lock) after T0, with D1 left frozen.

Usage:
  python tools/delist_source_rows.py hf_equities          # dry run: counts only
  python tools/delist_source_rows.py hf_equities --apply
  python tools/delist_source_rows.py whr --purge-csv-prefix "WHR:" --apply
      # EXCLUSIVE CSV-only mode: deletes series/<urlenc(source:PREFIX)>* CSV objects and
      # touches NOTHING else (no catalogue, no D1, no store). Built for provenance residue:
      # whr's 178 older-provenance CSVs (R364), Ahmed-authorized.
"""
from __future__ import annotations

import argparse
import contextlib
import os
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core.licence_targets import Targets  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--purge-csv-prefix", default=None, metavar="NATIVE_PREFIX",
                    help="EXCLUSIVE mode: delete only series CSVs whose native key starts "
                         "with this prefix (terminated at the encoded delimiter); catalogue/D1 "
                         "untouched. Empty string = every CSV of the source.")
    a = ap.parse_args(argv)
    t = Targets()
    if t.selfhosted and a.apply:                    # a dry run only reads, so it takes no lock (AR-153)
        from core.catalog_path import writer_lock                           # noqa: PLC0415
        lock = writer_lock()
    else:
        lock = contextlib.nullcontext()
    with lock:
        if a.purge_csv_prefix is not None:
            return _purge_csvs(a.source, a.purge_csv_prefix, a.apply, t)
        return _delist(a.source, a.apply, t)


def _purge_csvs(src: str, native_prefix: str, apply: bool, t: Targets) -> int:
    pfx = "series/" + urllib.parse.quote(f"{src}:{native_prefix}", safe="")
    keys = [k for k, _ in t.list(pfx)]
    print(f"{src}: {len(keys):,} CSV object(s) under terminated prefix {pfx}")
    if not apply:
        print("(dry run - pass --apply to purge)")
        return 0
    t.delete(keys, (pfx,))
    left = t.list(pfx)
    print(f"  purged; residual objects: {len(left)} (must be 0)")
    if not left:
        t.record_removal("delist_source_rows", src, f"series CSVs under {pfx}")
    return 0 if not left else 1


def _delist(src: str, apply: bool, t: Targets) -> int:
    con = t.catalogue(write=apply)
    n_series = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (src,)).fetchone()[0]
    n_source = con.execute("SELECT COUNT(*) FROM source WHERE source_id=?", (src,)).fetchone()[0]
    print(f"{src}: catalog series={n_series:,} source_row={n_source} (stored objects untouched by design)")

    if not apply:
        print("(dry run - pass --apply to delist)")
        return 0

    left = t.remove_source_rows(con, src)
    print(f"  catalogue: deleted; residual rows={left} (must be 0)")
    if left:
        return 1

    # D1: every table that names the source (Targets.d1_statements - source_counts is R709, and the
    # freshness projection is what /v1/last-updates serves with no join to `source`: measured 2026-09-21,
    # 15 such ids were live, 11 of them gated).
    if not t.skip_d1() and not t.d1_execute(src):
        return 1
    t.record_removal("delist_source_rows", src, "delisted: catalogue rows")

    print(f"{src}: DELISTED (catalogue{'' if t.selfhosted else ' + D1'}). Now: util.ts removal + deploy + "
          f"live absence check + refresh_r2_catalog --allow-shrink {src}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
