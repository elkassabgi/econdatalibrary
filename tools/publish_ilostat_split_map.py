"""Publish the desktop's ilostat _split_map.json to the store on R2 (review R1131).

WHY. The resolver reads `_split_map.json` from the local store directory for every
`ilostat:<stem>#<part>` id (1,470 of the 3,305 catalogued ids). tools/derive_ilostat_indicators.py
writes it on the DESKTOP only, so it never reached R2, and on a runner every part id failed to resolve.
The fetcher now copies the store's map onto the runner before the CSV phase (fetchers/ilostat.py
_fetch_split_map); this tool puts the map there. Run it after every non-dry run of the derive tool.

WHAT IT DOES. Reads data/clean_full/ilostat/_split_map.json from THIS checkout, checks it parses to a
non-empty {stem: ...} map, and compares it with the R2 object clean_full/ilostat/_split_map.json:
  identical      -> nothing to do
  absent on R2   -> PUT (with --apply), then read back byte-for-byte
  different      -> refused unless --replace (a map describes how CSV parts were cut; replacing a
                    newer map with an older desktop copy would break the ids it no longer names)
ALWAYS REFUSED, --replace or not (review R1137): a map that lacks any stem R2's map holds, or any stem
the catalogue serves as '#part' ids. A non-dry `--limit` run of the derive tool writes a truncated map,
and publishing it would make every part id of the missing stems unresolvable.
Dry run by default. Needs the R2_* credentials in the environment.

Usage:
  py tools/publish_ilostat_split_map.py            # compare only
  py tools/publish_ilostat_split_map.py --apply    # upload when absent
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
LOCAL = os.path.join(ROOT, "data", "clean_full", "ilostat", "_split_map.json")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--local", default=LOCAL)
    ap.add_argument("--catalog", default=os.environ.get("ECONDL_CATALOG")
                    or os.path.join(ROOT, "data", "catalog.db"))
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--replace", action="store_true")
    a = ap.parse_args(argv)
    os.environ["AQUEDUCT_BACKEND"] = "r2"
    from updater import blob                                  # noqa: PLC0415 - backend set first
    r2 = blob._r2_routed()
    if r2 is None:
        print("REFUSED: the r2 backend is not available (R2_* credentials?)")
        return 2
    body = open(a.local, "rb").read()
    smap = json.loads(body.decode("utf-8"))
    if not isinstance(smap, dict) or not smap:
        print(f"REFUSED: {a.local} is not a non-empty map")
        return 2
    key = blob._path_to_key(a.local)
    have = r2.get(key)
    sha = lambda b: hashlib.sha256(b).hexdigest()[:16]    # noqa: E731
    print(f"local {a.local}: {len(smap):,} indicator(s), {len(body):,} bytes, sha256 {sha(body)}")
    print(f"R2 {key}: " + ("absent" if have is None else f"{len(have):,} bytes, sha256 {sha(have)}"))
    if have == body:
        print("identical - nothing to do")
        return 0
    need = set()
    if have is not None:
        try:
            need |= set(json.loads(have.decode("utf-8")))
        except (ValueError, UnicodeDecodeError, TypeError):
            print("REFUSED: R2's map cannot be parsed, so what it covers cannot be checked")
            return 2
    try:
        import sqlite3                                            # noqa: PLC0415
        con = sqlite3.connect(f"file:{a.catalog}?mode=ro", uri=True)
        need |= {sid.split(":", 1)[1].split("#", 1)[0] for (sid,) in con.execute(
            "SELECT series_id FROM series WHERE series_id >= ? AND series_id < ?", ("ilostat:", "ilostat;"))
            if "#" in sid}
        con.close()
    except Exception as e:                                         # noqa: BLE001
        print(f"REFUSED: the catalogue ({a.catalog}) cannot be read to check the map's coverage "
              f"({type(e).__name__})")
        return 2
    missing = sorted(need - set(smap))
    print(f"stems the map must cover (R2 map + catalogued '#part' ids): {len(need):,}; missing: {len(missing):,}")
    if missing:
        print(f"REFUSED: the local map lacks {len(missing):,} required stem(s), e.g. {missing[:5]}")
        return 2
    if have is not None and not a.replace:
        print("REFUSED: R2 holds a DIFFERENT map (--replace to overwrite it)")
        return 2
    if not a.apply:
        print("dry run - nothing written")
        return 0
    r2.put_atomic(key, body)
    back = r2.get(key)
    ok = back == body
    print(f"PUT {key}; read back {'IDENTICAL' if ok else 'DIFFERENT'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
