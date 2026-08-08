"""Apply a value-verified element re-code to a frozen fao_* source — the #19
restructure's re-key half (Ahmed approved 2026-08-07: "that tail is mis-keyed
today anyway").

WHAT IT DOES, for each accepted (old_suffix -> new_suffix) mapping:
  store    : series_key '<PREFIX>:<old>' -> '<PREFIX>:<new>' (parquet rewrite)
  catalog  : series_id + title updated (element name modernised via --title-sub)
  R2 CSVs  : new-id CSV derived by the normal derive path AFTERWARDS (the tool
             deletes the old-id CSV objects and prints the derive command); the
             fetcher's next run then extends the re-keyed series in place.
  D1       : old rows deleted + new rows upserted via the normal sync path
             (pending-file), printed as a follow-up command.

SAFETY — mappings are re-filtered here, independently of the crosswalk emitter:
  * item and area segments must be UNCHANGED (a value-coincidence across items —
    Forestry matching Fishery — is refused even if it scored 100%);
  * the element pair must appear in --allow (the globally-coherent re-codes,
    e.g. 6109=6224,6183=6225), each named explicitly by a human reading the
    decomposition — never inferred here;
  * the target id must NOT already exist in the catalog (collision refusal);
  * one old id -> one new id and vice versa (bijection check).
Anything refused is reported and left frozen. --dry-run prints the plan only.

    python tools/rekey_fao_series.py --source fao_ic \
        --crosswalk updater/strategies/fetchers/_faostat_maps/fao_ic.crosswalk.json \
        --order element,area,item --allow 6109=6224,6183=6225 \
        --title-sub "Value Local Currency=Value Standard Local Currency" --dry-run
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import sqlite3
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--crosswalk", required=True)
    ap.add_argument("--order", required=True,
                    help="what each dot-segment means, e.g. element,area,item")
    ap.add_argument("--allow", required=True,
                    help="comma list of old=new ELEMENT pairs, e.g. 6109=6224,6183=6225; "
                         "'same' entries allow an unchanged element")
    ap.add_argument("--allow-items", default="",
                    help="comma list of old=new ITEM pairs explicitly permitted to "
                         "change (e.g. 6646=6751 for GF's Forest land->Forestland "
                         "re-code); items not listed must be unchanged")
    ap.add_argument("--title-sub", action="append", default=[],
                    help="old=new substring substitution applied to titles (repeatable)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    order = [w.strip() for w in a.order.split(",")]
    assert sorted(order) == ["area", "element", "item"], order
    ep, ip, arp = order.index("element"), order.index("item"), order.index("area")
    allow_same = False
    allow = {}
    for tok in a.allow.split(","):
        if tok.strip() == "same":
            allow_same = True
        else:
            k, v = tok.split("=")
            allow[k] = v
    allow_items = dict(p.split("=") for p in a.allow_items.split(",")) if a.allow_items else {}
    subs = [t.split("=", 1) for t in a.title_sub]

    cw = json.load(open(a.crosswalk, encoding="utf-8"))
    assert cw["source_id"] == a.source
    raw = cw["crosswalk"]

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))
    existing = {r[0] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id=?", (a.source,))}
    prefix = None
    for sid in existing:
        parts = sid.split(":", 2)
        if len(parts) == 3:
            prefix = parts[1]
            break
    assert prefix, "could not infer key prefix from catalog ids"

    accepted, refused = {}, collections.Counter()
    for old, new in raw.items():
        op, np_ = old.split("."), new.split(".")
        if op[arp] != np_[arp]:
            refused["area_changed"] += 1
            continue
        if op[ip] != np_[ip] and allow_items.get(op[ip]) != np_[ip]:
            refused["item_pair_not_allowed"] += 1
            continue
        el_ok = (op[ep] == np_[ep] and allow_same) or allow.get(op[ep]) == np_[ep]
        if not el_ok:
            refused["element_pair_not_allowed"] += 1
            continue
        tgt = f"{a.source}:{prefix}:{new}"
        if tgt in existing:
            refused["target_id_already_exists"] += 1
            continue
        accepted[old] = new
    by_target = collections.defaultdict(list)
    for o, n in accepted.items():
        by_target[n].append(o)
    for n, olds in by_target.items():
        if len(olds) > 1:
            # two old series claim one successor — at most one can be right;
            # refuse ALL claimants rather than guess (they stay frozen).
            for o in olds:
                del accepted[o]
            refused["ambiguous_shared_target"] += len(olds)
    print(f"{a.source}: {len(raw):,} crosswalk entries -> {len(accepted):,} accepted, "
          f"refused {dict(refused)}")
    if not accepted:
        return 0
    if a.dry_run:
        for old, new in list(accepted.items())[:5]:
            print(f"  {old} -> {new}")
        print("--dry-run: nothing written.")
        return 0

    # 1) STORE rewrite
    spath = os.path.join(ROOT, "data", "clean_full", a.source, a.source + ".parquet")
    t = pq.read_table(spath)
    keymap = {f"{prefix}:{o}": f"{prefix}:{n}" for o, n in accepted.items()}
    keys = t.column("series_key").to_pylist()
    hit = sum(1 for k in keys if k in keymap)
    new_keys = [keymap.get(k, k) for k in keys]
    t = t.set_column(t.schema.get_field_index("series_key"), "series_key",
                     pa.array(new_keys, pa.string()))
    tmp = spath + ".rekey.tmp"
    pq.write_table(t, tmp, compression="zstd")
    os.replace(tmp, spath)
    print(f"store: {hit:,} rows re-keyed across {len(accepted):,} series")

    # 2) CATALOG rewrite (+ pending-file rows for the D1 delta sync)
    pending = os.path.join(ROOT, "data", "_aqueduct", "pending_catalog_sync.txt")
    os.makedirs(os.path.dirname(pending), exist_ok=True)
    deleted_ids = []
    with open(pending, "a", encoding="utf-8") as pf_:
        for old, new in accepted.items():
            osid = f"{a.source}:{prefix}:{old}"
            nsid = f"{a.source}:{prefix}:{new}"
            row = con.execute("SELECT title FROM series WHERE series_id=?",
                              (osid,)).fetchone()
            title = row[0] if row else None
            if title:
                for so, sn in subs:
                    title = title.replace(so, sn)
            con.execute("UPDATE series SET series_id=?, title=? WHERE series_id=?",
                        (nsid, title, osid))
            deleted_ids.append(osid)
            pf_.write(nsid + "\n")
    con.commit()
    print(f"catalog: {len(accepted):,} ids re-keyed; {len(accepted):,} new ids "
          f"queued in pending_catalog_sync.txt")

    # 3) old-id D1 deletes + old CSV deletes: emit exact lists for the operator
    outd = os.path.join(ROOT, "data", "_aqueduct")
    with open(os.path.join(outd, f"rekey_{a.source}_old_ids.txt"), "w",
              encoding="utf-8") as f:
        f.write("\n".join(deleted_ids) + "\n")
    print(f"wrote {outd}\\rekey_{a.source}_old_ids.txt ({len(deleted_ids):,} ids) — "
          "delete these from D1 and R2 series/ CSVs, then:")
    print(f"  python -m core.derive_csv --source {a.source} --bucket econ-data "
          "--skip-existing  (derives the new-id CSVs)")
    print(f"  python -m core.sync_catalog_d1  (upserts the new ids from the pending file)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
