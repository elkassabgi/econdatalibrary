"""The WITHHELD list (api/worker/src/withheld.ts): `_aqueduct/withheld.json` in the store the worker serves from
(R2 before T0, the self-hosted blob store after - updater.blob.csv_store()). Owner decision, 2026-10-17: an
incomplete table is hidden from users until it is complete. Every write here is ONE conditional write, read back.

    python tools/withheld_list.py show
    python tools/withheld_list.py create                 # dry run: what would be created from api/worker/withheld.json
    python tools/withheld_list.py create --apply         # If-None-Match: * (refused if the object exists)
    python tools/withheld_list.py withhold ID [ID ...] --why "..." [--until complete|operator] [--apply]
    python tools/withheld_list.py release  ID [ID ...] [--apply]    # take ids off the list (they are listed again)

Rules:
  - every id must pass the worker's own rule (tools/gen_withheld_floor.py validate: [a-z0-9_]:[A-Za-z0-9_.-], at
    most 500, until complete|operator); a list the worker would reject is never written;
  - `withhold` / `release` replace the object only while its ETag is the one read (If-Match); a change in between
    refuses (exit 4) and nothing is written;
  - `withhold` warns for an id the LOCAL catalogue (core.catalog_path, read-only) does not hold: such an id is
    hidden from nothing today, and only guards a later catalogue sync;
  - the object is never deleted: `release` of every id leaves `"ids": {}`, which the worker reads as "withhold
    nothing" (an absent or unreadable object means the compiled-in floor);
  - `withhold` REFUSES an id of a source the committed floor (api/worker/withheld.json) names no id of: while
    the list is unreadable the worker withholds the floor's sources WHOLE, and that is what keeps an id withheld
    after the floor was committed hidden (AR-341 finding 2). A new source goes into withheld.json first, then
    tools/gen_withheld_floor.py, commit, worker deploy - then `withhold`;
  - after a change, tools/gen_withheld_floor.py --from-store refreshes the committed floor (a stale floor no
    longer opens anything: it decides only which sources close whole, and which ids are named in it).
Exit: 0 ok or dry run, 2 refused (validation, exists), 4 the condition failed, 5 read-back mismatch."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))
import gen_withheld_floor as G  # noqa: E402

KEY = G.KEY


def encode(obj: dict) -> bytes:
    G.validate(obj)
    return (json.dumps(obj, indent=1, sort_keys=True) + "\n").encode("utf-8")


def catalogued(ids: list[str]) -> set[str]:
    """The ids the LOCAL catalogue holds (read-only, through core.catalog_path - after T0 the build itself)."""
    from core import catalog_path                                         # noqa: PLC0415
    try:
        con = catalog_path.connect(write=False)
    except FileNotFoundError:
        return set(ids)                     # no local catalogue to check against: say nothing
    try:
        q = ",".join("?" * len(ids))
        return {r[0] for r in con.execute(f"SELECT series_id FROM series WHERE series_id IN ({q})", ids)}
    finally:
        con.close()


def _store():
    from updater import blob                                              # noqa: PLC0415
    store = blob.csv_store()
    if not hasattr(store, "put_if"):
        raise SystemExit(f"{type(store).__name__} has no conditional write - refused")
    return store


def _write(store, body: bytes, *, if_match: str | None) -> int:
    got = store.put_if(KEY, body, if_none_match=True) if if_match is None else store.put_if(KEY, body, if_match=if_match)
    if got is None:
        print(f"REFUSED: the condition failed ({'If-None-Match: *' if if_match is None else 'If-Match ' + if_match}) - "
              f"{KEY} changed in between; nothing written")
        return 4
    back = store.get(KEY)
    if back != body:
        print(f"READ-BACK MISMATCH: {len(back or b'')} bytes - inspect {KEY}")
        return 5
    print(f"written and read back identical: {len(body)} bytes, md5 {hashlib.md5(body).hexdigest()}, etag {got}")
    print("next: python tools/gen_withheld_floor.py --from-store, then commit, so the floor follows the list")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=("show", "create", "withhold", "release"))
    ap.add_argument("ids", nargs="*")
    ap.add_argument("--why", default="")
    ap.add_argument("--until", choices=("complete", "operator"), default="complete")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    store = _store()
    raw = store.get(KEY)
    etag = store.etag(KEY) if raw is not None else None
    if raw is not None:
        # The GET and the HEAD are two requests: go on only when the ETag is the MD5 of the bytes read (R2
        # single-part and the self-hosted store both report that), so an If-Match built on `raw` cannot overwrite
        # a write that landed in between (AR-341 finding 1).
        if (etag or "").strip('"') != hashlib.md5(raw).hexdigest():
            print(f"REFUSED: {KEY} changed between its read and its ETag (etag {etag}, md5 of the bytes read "
                  f"{hashlib.md5(raw).hexdigest()}) - nothing written; run again")
            return 4
        etag = etag.strip('"')
    if a.action == "show":
        if raw is None:
            print(f"{KEY}: ABSENT - the worker applies its compiled-in floor")
            return 0
        obj = json.loads(raw)
        print(f"{KEY}: etag {etag}, {len(obj.get('ids') or {})} ids")
        for k, v in sorted((obj.get("ids") or {}).items()):
            print(f"  {k}  until={v.get('until')}  since={v.get('since')}  {str(v.get('why'))[:90]}")
        return 0
    if a.action == "create":
        if raw is not None:
            print(f"REFUSED: {KEY} already exists (etag {etag}); use withhold / release")
            return 2
        obj = json.load(open(G.JSON_PATH, encoding="utf-8"))
        body = encode(obj)
        print(f"create {KEY} from api/worker/withheld.json: {len(obj['ids'])} ids, {len(body)} bytes, "
              f"md5 {hashlib.md5(body).hexdigest()}")
        missing = sorted(set(obj["ids"]) - catalogued(sorted(obj["ids"])))
        if missing:
            print(f"  NOTE: {len(missing)} id(s) the local catalogue does not hold (guards a later sync): {missing}")
        if not a.apply:
            print("DRY RUN - nothing written")
            return 0
        return _write(store, body, if_match=None)
    if not a.ids:
        ap.error("give at least one id")
    if raw is None:
        print(f"REFUSED: {KEY} is absent - create it first")
        return 2
    obj = json.loads(raw)
    ids = dict(obj.get("ids") or {})
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    if a.action == "withhold":
        if not a.why.strip():
            ap.error("--why is required: the reason is part of the record")
        floor_sources = {k.split(":", 1)[0] for k in json.load(open(G.JSON_PATH, encoding="utf-8"))["ids"]}
        uncovered = sorted(i for i in a.ids if i.split(":", 1)[0] not in floor_sources)
        if uncovered:
            print(f"REFUSED: {uncovered}: their source has no id in the committed floor (api/worker/withheld.json), "
                  f"so a worker that cannot read the list would SHOW them. Add one to withheld.json, run "
                  f"tools/gen_withheld_floor.py, commit and deploy the worker first")
            return 2
        for i in a.ids:
            ids[i] = {"since": today, "why": a.why.strip(), "until": a.until}
        missing = sorted(set(a.ids) - catalogued(list(a.ids)))
        if missing:
            print(f"  NOTE: not in the local catalogue (hidden from nothing today): {missing}")
    else:
        absent = [i for i in a.ids if i not in ids]
        if absent:
            print(f"REFUSED: not on the list: {absent}")
            return 2
        for i in a.ids:
            ids.pop(i)
    new = {**obj, "ids": ids}
    try:
        body = encode(new)
    except SystemExit as e:
        print(f"REFUSED: {e}")
        return 2
    print(f"{a.action} {len(a.ids)} id(s): the list goes {len(obj.get('ids') or {})} -> {len(ids)} ids "
          f"(If-Match {etag})")
    if not a.apply:
        print("DRY RUN - nothing written")
        return 0
    return _write(store, body, if_match=etag)


if __name__ == "__main__":
    raise SystemExit(main())
