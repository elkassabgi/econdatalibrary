"""Publish the desktop's _split_map.json (ilostat, or census with --source census) to the store on R2.

WHY (ilostat, review R1131). The resolver reads `_split_map.json` from the local store directory for every
`ilostat:<stem>#<part>` id (1,470 of the 3,305 catalogued ids). tools/derive_ilostat_indicators.py
writes it on the DESKTOP only, so it never reached R2, and on a runner every part id failed to resolve.
The fetcher now copies the store's map onto the runner before the CSV phase (fetchers/ilostat.py
_fetch_split_map); this tool puts the map there. Run it after every non-dry run of the derive tool.

WHAT IT DOES (ilostat). Reads data/clean_full/ilostat/_split_map.json from THIS checkout, checks it parses
to a non-empty {stem: ...} map, and compares it with the R2 object clean_full/ilostat/_split_map.json:
  identical      -> nothing to do
  absent on R2   -> PUT (with --apply), then read back byte-for-byte
  different      -> refused unless --replace (a map describes how CSV parts were cut; replacing a
                    newer map with an older desktop copy would break the ids it no longer names)
ALWAYS REFUSED, --replace or not (review R1137): a map that lacks any stem R2's map holds, or any stem
the catalogue serves as '#part' ids. A non-dry `--limit` run of the derive tool writes a truncated map,
and publishing it would make every part id of the missing stems unresolvable.

CENSUS (--source census, review AR-408). `census:<table>#<part>` ids (2,909 of the 2,993 catalogued) are
cut by data/clean_full/census/_split_map.json, which tools/derive_census_tables.py wrote on the desktop
only. Nothing that serves users reads it from R2 (the resolver reads the LOCAL file under ECONDL_DATA;
the worker never reads a split map); the one R2 reader is the `core.derive_csv --only` gate, which
compares every file a '#part' id reads - the map included - with R2's object and refuses while R2 has
none. So for census the map is published ONCE, and the rules are stricter than ilostat's:
  * ABSENT-ONLY. R2 already holding a different map is refused, and --replace is refused for census
    (a changed census map moves part ids, which is a catalogue change, not a publish).
  * the PUT is ONE create-only conditional write (If-None-Match: *, R2Blob.put_if), so a map another
    writer created after the check makes R2 refuse it - the GET before it is only the report;
  * the key is FIXED as clean_full/census/_split_map.json (never derived from --local), and --local must
    end in data/clean_full/census/_split_map.json;
  * every entry names its split dimension(s) as NAME or NAME:<n> (the resolver refuses an entry without
    one), a non-empty sep, and an integer part count;
  * COVERAGE, both directions, per table: the set of tables in the map equals the set of tables the
    catalogue serves as '#part' ids, and each table's `parts` equals the number of catalogued
    `census:<table>#...` ids. The map is keyed by the table name, i.e. the text between `census:` and
    `#` in the id (econdl._resolve._resolve_census_table);
  * --expect-md5 is REQUIRED and refuses unless the local file's md5 is the one the review measured. The
    coverage check counts parts per table, so it cannot see a changed CUT with the same counts (a new sep,
    another dimension, another truncation); only the md5 pins the bytes the review read;
  * every R2 error is printed as a code only (botocore messages carry the endpoint URL);
  * read back byte for byte after the PUT; refused after T0 (core.cutover), like ilostat.
All local checks run BEFORE the R2 client is built. The client is updater.blob's R2Blob, which holds the
WRITE credentials; what limits this tool to one object is its code path, not the key pair.

Dry run by default. Needs the R2_* credentials in the environment.

Usage:
  py tools/publish_ilostat_split_map.py            # compare only
  py tools/publish_ilostat_split_map.py --apply    # upload when absent
  py tools/publish_ilostat_split_map.py --source census --local <map> --catalog <catalog.db> \\
      --expect-md5 <md5>                           # census: every check, nothing written
  ... the same with --apply                        # census: upload when absent, then read back
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
LOCAL = os.path.join(ROOT, "data", "clean_full", "ilostat", "_split_map.json")
CENSUS_LOCAL = os.path.join(ROOT, "data", "clean_full", "census", "_split_map.json")
CENSUS_KEY = "clean_full/census/_split_map.json"
SOURCES = ("ilostat", "census")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--source", choices=SOURCES, default="ilostat",
                    help="whose map to publish (default ilostat, the tool's original behaviour)")
    ap.add_argument("--local", default=None,
                    help="the map file (default: data/clean_full/<source>/_split_map.json in this checkout)")
    ap.add_argument("--catalog", default=None,
                    help="a catalogue file to check coverage against (default: the one core.catalog_path resolves)")
    ap.add_argument("--expect-md5", default=None,
                    help="refuse unless the local map's md5 is this value (the md5 a review measured); "
                         "REQUIRED with --source census")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--replace", action="store_true")
    a = ap.parse_args(argv)
    if a.source == "census":
        return _main_census(a)
    if a.local is None:
        a.local = LOCAL
    return _main_ilostat(a)


def _main_ilostat(a) -> int:
    """The original ilostat path, unchanged (review R1131/R1137); --expect-md5 is checked only when given."""
    # AFTER T0 R2 is a frozen copy nobody is served from: publishing to it is meaningless (plan step 6d, like the
    # other R2-copy tools). The map then lives in the self-hosted store the derive writes.
    from core import cutover                                  # noqa: PLC0415
    cutover.refuse_if_cut_over("publish_ilostat_split_map - it PUTs to R2, a frozen copy after T0")
    os.environ["AQUEDUCT_BACKEND"] = "r2"
    from updater import blob                                  # noqa: PLC0415 - backend set first
    r2 = blob._r2_routed()
    if r2 is None:
        print("REFUSED: the r2 backend is not available (R2_* credentials?)")
        return 2
    body = open(a.local, "rb").read()
    if a.expect_md5 is not None and hashlib.md5(body).hexdigest() != a.expect_md5.strip().lower():
        print(f"REFUSED: {a.local} has md5 {hashlib.md5(body).hexdigest()}, not the expected {a.expect_md5}")
        return 2
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
        from core import catalog_path                             # noqa: PLC0415 - the one resolver (plan step 1)
        con = (catalog_path.connect_path(a.catalog, write=False) if a.catalog else catalog_path.connect())
        need |= {sid.split(":", 1)[1].split("#", 1)[0] for (sid,) in con.execute(
            "SELECT series_id FROM series WHERE series_id >= ? AND series_id < ?", ("ilostat:", "ilostat;"))
            if "#" in sid}
        con.close()
    except Exception as e:                                         # noqa: BLE001
        print(f"REFUSED: the catalogue ({a.catalog or 'resolved by core.catalog_path'}) cannot be read to "
              f"check the map's coverage ({type(e).__name__})")
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


# ---- census ---------------------------------------------------------------------------------------------
# A dimension as _resolve_census_table reads it: "NAME" or "NAME:n" (a truncation to n >= 1 characters).
# NAME goes into the regex f"{name}=(?P<v>[^|]*)", so only a plain column name is accepted.
_DIM_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(:[1-9][0-9]*)?$")


def catalogued_parts(con, source: str) -> dict:
    """{table: number of catalogued `<source>:<table>#<part>` ids}, over the primary-key range of the source
    (an index range, never `WHERE source_id = ?`). Ids without '#' (whole tables, census's legacy series-grain
    ids) are not split and are not counted."""
    out: dict = {}
    for (sid,) in con.execute("SELECT series_id FROM series WHERE series_id >= ? AND series_id < ?",
                              (f"{source}:", f"{source};")):
        if "#" in sid:
            table = sid.split(":", 1)[1].split("#", 1)[0]
            out[table] = out.get(table, 0) + 1
    return out


def census_map_problems(smap, parts: dict) -> list:
    """Every reason the census map must not be published against the catalogue's '#part' counts; [] = none.

    An entry must be a dict naming `dims` (a non-empty list of non-empty strings) or a legacy `dim`, because
    econdl._resolve._resolve_census_table refuses an entry that names no dimension, and an integer `parts`.
    Coverage is checked in BOTH directions, per table, as equality of the part count."""
    if not isinstance(smap, dict) or not smap:
        return ["the map is not a non-empty JSON object"]
    if not parts:
        return ["the catalogue holds no census '#part' id, so coverage cannot be checked (wrong catalogue?)"]
    problems = []
    for table in sorted(smap):
        e = smap[table]
        if not isinstance(e, dict):
            problems.append(f"{table}: the entry is not an object")
            continue
        dims = e.get("dims") or ([e["dim"]] if e.get("dim") else [])
        if not isinstance(dims, list) or not dims or not all(isinstance(d, str) and d for d in dims):
            problems.append(f"{table}: the entry names no split dimension")
        else:
            bad = [d for d in dims if not _DIM_RE.match(d)]
            if bad:
                problems.append(f"{table}: dimension(s) {bad[:3]} are not NAME or NAME:<n> with n >= 1")
        sep = e.get("sep", "~")
        if not isinstance(sep, str) or not sep:
            problems.append(f"{table}: the entry's sep is not a non-empty string")
        n = e.get("parts")
        if not isinstance(n, int) or isinstance(n, bool) or n < 1:
            problems.append(f"{table}: the entry has no integer part count")
    for table in sorted(set(parts) - set(smap)):
        problems.append(f"{table}: {parts[table]:,} catalogued '#part' id(s), and the map has no entry")
    for table in sorted(set(smap) - set(parts)):
        problems.append(f"{table}: in the map, and the catalogue serves no '#part' id of it")
    for table in sorted(set(smap) & set(parts)):
        e = smap[table]
        n = e.get("parts") if isinstance(e, dict) else None
        if isinstance(n, int) and not isinstance(n, bool) and n != parts[table]:
            problems.append(f"{table}: the map says {n:,} part(s), the catalogue serves {parts[table]:,}")
    return problems


def _main_census(a) -> int:
    from core import cutover                                  # noqa: PLC0415
    cutover.refuse_if_cut_over("publish_ilostat_split_map --source census - it PUTs to R2, a frozen copy after T0")
    if a.replace:
        print("REFUSED: census is absent-only - --replace is not offered (a changed census map moves part ids, "
              "which is a catalogue change)")
        return 2
    local = a.local or CENSUS_LOCAL
    try:
        body = open(local, "rb").read()
    except OSError as e:
        print(f"REFUSED: {local} cannot be read ({type(e).__name__})")
        return 2
    md5 = hashlib.md5(body).hexdigest()
    print(f"local {local}: {len(body):,} bytes, md5 {md5}")
    # REQUIRED for census (review AR-409 round 2): the coverage check counts parts per table, so it cannot see
    # a changed CUT with the same counts (sep '~' -> '|', NAICS -> STATE, E_COMMODITY:3 -> :4 all read 0
    # problems on the real map, and each moves every part of its table). Only the reviewed bytes may go up.
    if not a.expect_md5:
        print("REFUSED: --source census needs --expect-md5 <the md5 a review measured for this map>")
        return 2
    if md5 != a.expect_md5.strip().lower():
        print(f"REFUSED: the local map's md5 is {md5}, not the expected {a.expect_md5}")
        return 2
    try:
        smap = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        print(f"REFUSED: {local} is not JSON")
        return 2
    # The key is FIXED, never derived from the path (review AR-409 P9: blob._path_to_key takes the text after
    # the LAST "/data/", so an odd --local could name another object). --local must still end in the store
    # layout, so a map from another source's directory cannot be sent to census's key.
    key = CENSUS_KEY
    if not os.path.abspath(local).replace("\\", "/").endswith("/data/" + CENSUS_KEY):
        print(f"REFUSED: {local} does not end in data/{CENSUS_KEY}")
        return 2
    try:
        from core import catalog_path                             # noqa: PLC0415 - the one resolver (plan step 1)
        con = (catalog_path.connect_path(a.catalog, write=False) if a.catalog else catalog_path.connect())
        try:
            parts = catalogued_parts(con, "census")
        finally:
            con.close()
    except Exception as e:                                         # noqa: BLE001
        print(f"REFUSED: the catalogue ({a.catalog or 'resolved by core.catalog_path'}) cannot be read to "
              f"check the map's coverage ({type(e).__name__})")
        return 2
    problems = census_map_problems(smap, parts)
    print(f"tables with catalogued '#part' ids: {len(parts):,} ({sum(parts.values()):,} ids); "
          f"map entries: {len(smap) if isinstance(smap, dict) else 0:,}; problems: {len(problems):,}")
    if problems:
        for p in problems[:20]:
            print(f"  {p}")
        print(f"REFUSED: the map does not match the catalogue ({len(problems):,} problem(s))")
        return 2
    # Every R2 call below reduces an error to a CODE (tools/r2_object_census.error_code, ledger K2): botocore's
    # connection and timeout errors carry the endpoint URL in their message, so neither str(error) nor a
    # traceback may reach the output.
    from tools.r2_object_census import error_code             # noqa: PLC0415
    os.environ["AQUEDUCT_BACKEND"] = "r2"
    try:
        from updater import blob                              # noqa: PLC0415 - backend set first
        r2 = blob._r2_routed()
    except Exception as e:                                     # noqa: BLE001 - a code only, never the message
        print(f"REFUSED: the r2 backend could not be built ({error_code(e)})")
        return 2
    if r2 is None:
        print("REFUSED: the r2 backend is not available (R2_* credentials?)")
        return 2
    try:
        have = r2.get(key)
    except Exception as e:                                     # noqa: BLE001
        print(f"REFUSED: the GET of {key} failed ({error_code(e)}); nothing written")
        return 2
    print(f"R2 {key}: " + ("absent" if have is None else
                           f"{len(have):,} bytes, md5 {hashlib.md5(have).hexdigest()}"))
    if have == body:
        print("identical - nothing to do")
        return 0
    if have is not None:
        print("REFUSED: R2 already holds a DIFFERENT census map; census is absent-only")
        return 2
    if not a.apply:
        print("dry run - nothing written")
        return 0
    # ONE conditional PUT (If-None-Match: *), evaluated by R2 atomically with the write (review AR-409 P3):
    # the GET above is only the report; a map another writer created since then makes R2 refuse this PUT.
    try:
        etag = r2.put_if(key, body, if_none_match=True)
    except Exception as e:                                     # noqa: BLE001
        print(f"FAILED: the create-only PUT of {key} raised ({error_code(e)}); whether it landed is UNKNOWN - "
              "run the dry run again to read R2's state")
        return 1
    if etag is None:
        print(f"REFUSED: R2 refused the create-only PUT - {key} appeared after the check; nothing written")
        return 2
    try:
        back = r2.get(key)
    except Exception as e:                                     # noqa: BLE001
        print(f"FAILED: the PUT returned etag {etag or '(none)'}, and the read-back GET failed "
              f"({error_code(e)}) - run the dry run again to compare")
        return 1
    ok = back == body and (not etag or etag == md5)
    print(f"PUT {key} (create-only), etag {etag or '(none)'}; read back "
          + ("IDENTICAL" if back == body else "DIFFERENT")
          + ("" if back is None else f" (md5 {hashlib.md5(back).hexdigest()})")
          + ("" if not etag or etag == md5 else f"; the PUT's etag is not the local md5 {md5}"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
