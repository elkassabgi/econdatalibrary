#!/usr/bin/env python3
"""Which gus_dbw refresh parts a rebuild would EXCLUDE under the current-sweep rule - a read-only report.

jobs/gus_dbw_refresh.select_tail_parts merges only the parts the area's CURRENT sweep recorded in its
manifest (parts/_refresh/area_<id>/_sweep.jsonl). This tool says, for every area dir under a refresh
root, what the rule would leave out of the next rebuild.

STAT-ONLY BY DEFAULT. On Windows an open handle on a part or on the manifest makes the crawler's
os.replace fail (WinError 5), which turns a healthy sweep into a failed section. So the default mode
opens NO file in the tree: it lists names, sizes and mtimes. That is enough to classify every area
that has no manifest (every part there is excluded: it belongs to no sweep). An area WITH a manifest
needs the manifest and the footers to classify; the default mode reports it as `needs --read`.
`--read` runs the real selector (reads the manifest and the footers it names); use it only with the
crawler stopped, and say so with `--crawler-stopped`.

It writes nothing, deletes nothing, and never opens state.db or catalog.db.

    python tools/gus_dbw_refresh_exclusions.py [--root DIR] [--tail-start YEAR] [--list]
    python tools/gus_dbw_refresh_exclusions.py --read --crawler-stopped
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from jobs import gus_dbw_refresh as R      # noqa: E402

DEFAULT_ROOT = os.path.join(R.OUT, "parts", "_refresh")
PART_RE = re.compile(r"v(\d+)_s(\d+)_y(\d+)\.parquet")


def _utc(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stat_area(area_dir: str, tail_start: int) -> dict:
    """Stat-only facts for one area dir: no file in it is opened."""
    parts, manifest = [], None
    for e in os.scandir(area_dir):
        if not e.is_file():
            continue
        st = e.stat()
        if e.name == R.SWEEP_FILE:
            manifest = {"size": st.st_size, "mtime": _utc(st.st_mtime)}
        elif e.name.endswith(".parquet"):
            m = PART_RE.fullmatch(e.name)
            parts.append({"file": e.name, "size": st.st_size, "mtime": _utc(st.st_mtime),
                          "year": int(m.group(3)) if m else None})
    parts.sort(key=lambda p: p["file"])
    rec = {"area": os.path.basename(area_dir), "parts": len(parts),
           "bytes": sum(p["size"] for p in parts), "manifest": manifest,
           "oldest_mtime": min((p["mtime"] for p in parts), default=None),
           "newest_mtime": max((p["mtime"] for p in parts), default=None),
           "below_window": sum(1 for p in parts if p["year"] is not None and p["year"] < tail_start),
           "by_year": {}, "files": parts}
    for p in parts:
        rec["by_year"][str(p["year"])] = rec["by_year"].get(str(p["year"]), 0) + 1
    if manifest is None:
        rec["verdict"] = "no sweep manifest"
        rec["would_exclude"] = len(parts)
        rec["reason"] = "no sweep manifest: not attributable to any sweep (written before sweep identities)"
    else:
        rec["verdict"] = "needs --read"
        rec["would_exclude"] = None
        rec["reason"] = "a manifest is present; classifying needs it and the footers (--read, crawler stopped)"
    return rec


def read_area(root: str, area_name: str, tail_start: int) -> dict:
    """The real selector. Opens the manifest and the footers it names: crawler stopped only."""
    aid = area_name[len("area_"):]
    sel = R.select_tail_parts(aid, root, tail_start)
    return {"area": area_name, "sweep": sel["sweep"], "included": len(sel["included"]),
            "would_exclude": len(sel["excluded"]), "excluded": sel["excluded"],
            "superseded_by_zero": sel["superseded_by_zero"], "problems": sel["problems"]}


def report(root: str, tail_start: int, read: bool = False) -> list:
    out = []
    try:
        areas = sorted((e.name for e in os.scandir(root) if e.is_dir() and e.name.startswith("area_")),
                       key=lambda n: (len(n), n))
    except FileNotFoundError:
        return out
    for name in areas:
        rec = stat_area(os.path.join(root, name), tail_start)
        if read and rec["manifest"] is not None:
            rec.update(read_area(root, name, tail_start))
            rec["verdict"] = "read"
        out.append(rec)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=DEFAULT_ROOT)
    ap.add_argument("--tail-start", type=int, default=R.tail_start_year())
    ap.add_argument("--list", action="store_true", help="print every part (name, size, mtime)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--read", action="store_true", help="read manifests and footers (crawler stopped)")
    ap.add_argument("--crawler-stopped", action="store_true")
    a = ap.parse_args(argv)
    if a.read and not a.crawler_stopped:
        print("REFUSED: --read opens files the crawler replaces; stop the crawler and pass --crawler-stopped")
        return 2
    recs = report(a.root, a.tail_start, read=a.read)
    if a.json:
        print(json.dumps(recs, indent=1, sort_keys=True, default=str))
        return 0
    print(f"root {a.root}  tail from {a.tail_start}  mode {'read' if a.read else 'stat-only'}  "
          f"measured {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    tot_parts = tot_excl = unknown = 0
    for r in recs:
        tot_parts += r["parts"]
        if r["would_exclude"] is None:
            unknown += 1
        else:
            tot_excl += r["would_exclude"]
        print(f"{r['area']:>10}  parts {r['parts']:>5}  bytes {r['bytes']:>12,}  manifest "
              f"{'yes' if r['manifest'] else 'no ':>3}  would_exclude "
              f"{'?' if r['would_exclude'] is None else r['would_exclude']:>5}  below_window "
              f"{r['below_window']:>3}  mtimes {r['oldest_mtime'] or '-'} .. {r['newest_mtime'] or '-'}  "
              f"by_year {r['by_year']}  [{r['verdict']}]")
        if a.list:
            for f in r["files"]:
                print(f"            {f['file']:<32} {f['size']:>12,}  {f['mtime']}")
    print(f"TOTAL: {len(recs)} area dir(s), {tot_parts} part(s), {tot_excl} would be excluded, "
          f"{unknown} area(s) need --read to classify")
    return 0


if __name__ == "__main__":
    sys.exit(main())
