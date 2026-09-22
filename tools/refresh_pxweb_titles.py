"""Keep the PxWeb sources' `_catalog.json` title caches current, one table at a time.

WHY THIS EXISTS. `tools/catalog_pxweb_flowgrain.py` titles each flow-grain row from the source's
cached `_catalog.json` and falls back to the bare table id when the cache has no entry - honest,
but a poor discovery surface. Those caches go stale in three different ways and nothing fixed any
of them:

  * `bfs` and `dst` -- NO code in jobs/, tools/, core/ or updater/ writes their `_catalog.json` at
    all. They are files nothing maintains.
  * `statfin` -- `crawl_catalog()` in jobs/ingest_statfin.py returns the cached file whenever it
    exists and crawls only when it is absent, so the cache is written once and never again. Its
    copy had not moved since 2026-06-09.

Measured 2026-09-22, before this tool: 85 tables across the three would have been catalogued under
a bare id (statfin 24, bfs 30, dst 31).

WHY PER TABLE AND NOT A RE-CRAWL. A full BFS crawl of StatFin was throttled to HTTP 429 every ~74 s
and returned 1,533 tables against the cache's 1,555 - a truncated answer that would have DELETED 22
good titles had its guard not refused and restored the file. Asking for the ~59 tables actually
missing costs 59 requests instead of ~600 folder nodes and cannot truncate anything.

STRICTLY ADDITIVE. Entries are appended only for ids the cache does not already have. No existing
entry is edited or removed, so a bad or partial response cannot degrade a good file. The cache is
still backed up before any write.

IT SEPARATES THE TWO REASONS A TITLE IS MISSING, because they need different answers:
  * the publisher still lists the table -> fetch the title;
  * the publisher no longer lists it    -> no title EXISTS. For bfs that was 26 of 30 tables, i.e.
    we hold data the publisher has withdrawn. That is a catalogue question, not a title question,
    and this tool reports it rather than inventing a title.

A CONTROL GUARDS THE 'DELISTED' VERDICT. Before trusting any 404, three tables we ALREADY have
titles for must resolve upstream. If they do not, the URL shape is wrong and every "delisted" would
be this tool's bug, so it refuses that source instead of reporting a false withdrawal.

  python tools/refresh_pxweb_titles.py                       # all three, report only
  python tools/refresh_pxweb_titles.py --source dst --apply  # write dst's cache
"""
from __future__ import annotations

import argparse
import importlib.util
import io
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

UA = {"User-Agent": "econdatalibrary/title-refresh (contact: aelkassabgi@uca.edu)"}
PAUSE = 1.5          # the 0.3 s used by the full crawl drew sustained 429s
CONTROLS = 3


def _http(url: str) -> dict:
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=90) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def _statfin(tid: str, path: str) -> str:
    """StatFin addresses a table by its folder path, which the store prefix carries."""
    u = f"https://pxdata.stat.fi/PxWeb/api/v1/en/StatFin/{path}"
    return (_http(u).get("title") or "").strip()


def _bfs(tid: str, path: str) -> str:
    """BFS publishes each table as its own 'database': /<id>/<id>.px."""
    return (_http(f"https://www.pxweb.bfs.admin.ch/api/v1/en/{tid}/{tid}.px").get("title") or "").strip()


def _dst(tid: str, path: str) -> str:
    """Statbank is not PxWeb; lang=en matters or the title comes back in Danish."""
    u = f"https://api.statbank.dk/v1/tableinfo?id={tid}&format=JSON&lang=en"
    return (_http(u).get("text") or "").strip()


FETCHERS = {"statfin": _statfin, "bfs": _bfs, "dst": _dst}


def merge_additive(old: list, got: list) -> list:
    """Append entries for ids `old` does not have. Never edit, never drop.

    Losing a title here is silent and permanent - the cataloguer would simply fall back to the bare
    id and nothing would report it - so the one invariant worth asserting is that every id present
    before is present after.
    """
    old_ids = {str(t.get("id")) for t in old if isinstance(t, dict)}
    new = old + [g for g in got if str(g.get("id")) not in old_ids]
    if not {str(t.get("id")) for t in new if isinstance(t, dict)} >= old_ids:
        raise SystemExit("REFUSE: the merge would drop an existing id")
    return new


def _cataloguer():
    spec = importlib.util.spec_from_file_location(
        "catalog_pxweb_flowgrain", os.path.join(ROOT, "tools", "catalog_pxweb_flowgrain.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def missing_tables(cat, src: str):
    """(table_id, store_path) for every table the cataloguer would title with a bare id.

    Uses the cataloguer's OWN title map and prefix scan, so this cannot drift from the tool whose
    output it exists to improve.
    """
    tmap, agg = cat.build_title_map(src), cat.scan_prefixes(src)
    out = {}
    for pref in agg:
        tid = cat._last_seg(pref)
        if not (tmap.get(tid) or tmap.get(tid.lower())):
            out[tid] = pref.replace(":", "/")
    titled = [cat._last_seg(p) for p in agg if tmap.get(cat._last_seg(p))]
    return sorted(out.items()), titled


def refresh(src: str, apply: bool) -> int:
    cat = _cataloguer()
    bare, titled = missing_tables(cat, src)
    fetch = FETCHERS[src]
    print(f"{src}: {len(bare)} table(s) would be catalogued under a bare id")
    if not bare:
        return 0

    ok = 0
    for tid in titled[:CONTROLS]:
        try:
            if fetch(tid, ""):
                ok += 1
        except Exception:                                     # noqa: BLE001
            pass
        time.sleep(PAUSE)
    print(f"  control: {ok}/{min(CONTROLS, len(titled))} already-titled tables resolve upstream")
    if titled and ok == 0:
        print("  REFUSING: the control does not resolve, so every 'no longer published' below "
              "would be this tool's bug, not the publisher's doing", file=sys.stderr)
        return 2

    got, gone, errs = [], [], []
    for i, (tid, path) in enumerate(bare, 1):
        try:
            t = fetch(tid, path)
            (got.append({"id": tid, "path": path, "text": t}) if t
             else errs.append((tid, "empty title")))
            if t:
                print(f"  [{i}/{len(bare)}] {tid} -> {t[:64]}")
        except urllib.error.HTTPError as e:
            (gone if e.code in (400, 404) else errs).append((tid, f"HTTP {e.code}"))
        except Exception as exc:                              # noqa: BLE001
            errs.append((tid, type(exc).__name__))
        time.sleep(PAUSE)

    print(f"  fetched {len(got)} | no longer published {len(gone)} | other errors {len(errs)}")
    for tid, why in gone:
        print(f"     withdrawn upstream: {tid} ({why})")
    for tid, why in errs:
        print(f"     could not read: {tid} ({why})")

    if not apply:
        print("  (report only - pass --apply to write)")
        return 0
    if not got:
        print("  nothing to write")
        return 0

    path = os.path.join(ROOT, "data", "clean_full", src, "_catalog.json")
    old = json.load(io.open(path, encoding="utf-8"))
    shutil.copy2(path, f"{path}.bak-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}")
    new = merge_additive(old, got)
    io.open(path, "w", encoding="utf-8").write(json.dumps(new))
    print(f"  cache {len(old)} -> {len(new)} entries")

    left, _ = missing_tables(_cataloguer(), src)
    print(f"  bare-id titles a re-catalogue would now write: {len(left)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append", choices=sorted(FETCHERS),
                    help="repeatable; default is all three")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    rc = 0
    for src in (a.source or sorted(FETCHERS)):
        rc = refresh(src, a.apply) or rc
    return rc


if __name__ == "__main__":
    sys.exit(main())
